package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"cluster/config"
	"cluster/events"
	"cluster/pubsub"
	"cluster/types"

	"github.com/redis/go-redis/v9"
)

type SessionProvider interface {
	GetOrCreate(ctx context.Context) (string, error)
}

type Manager struct {
	redis       *redis.Client
	cfg         *config.Config
	events      *events.EventEmitter
	pubsub      *pubsub.PubSub
	registry    *registry
	distributor *distributor
	session     SessionProvider
	webhook     *webhook

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu                    sync.Mutex
	running               bool
	shardsDistributed     bool
	distributedIDs        map[string]struct{}
	knownIDs              map[string]struct{}
	shardMap              map[int]string
	distributing          bool
	pendingRedistribution bool
	startTime             time.Time
	fallbackCancel        context.CancelFunc
}

func NewManager(rdb *redis.Client, sess SessionProvider, cfg *config.Config) *Manager {
	em := events.New()
	ps := pubsub.New(rdb, em)
	return &Manager{
		redis:          rdb,
		cfg:            cfg,
		events:         em,
		pubsub:         ps,
		registry:       newRegistry(rdb),
		distributor:    newDistributor(cfg.TotalShards, cfg.ShardsPerInstance),
		session:        sess,
		webhook:        newWebhook(),
		distributedIDs: make(map[string]struct{}),
		knownIDs:       make(map[string]struct{}),
		shardMap:       make(map[int]string),
	}
}

func (m *Manager) Registry() *registry          { return m.registry }
func (m *Manager) Events() *events.EventEmitter { return m.events }

func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return nil
	}
	m.running = true
	m.startTime = time.Now()
	m.mu.Unlock()

	m.ctx, m.cancel = context.WithCancel(ctx)

	if err := m.clearOldInstances(m.ctx); err != nil {
		slog.Warn("clearOldInstances error", "err", err)
	}

	m.mu.Lock()
	m.distributedIDs = make(map[string]struct{})
	m.shardMap = make(map[int]string)
	m.knownIDs = make(map[string]struct{})
	m.shardsDistributed = false
	m.mu.Unlock()

	if err := m.pubsub.Start(m.ctx); err != nil {
		return fmt.Errorf("pubsub start: %w", err)
	}

	m.pubsub.On("instance_ready", m.onInstanceReady)
	m.pubsub.On("heartbeat", m.onHeartbeat)

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.monitorLoop(m.ctx)
	}()

	slog.Info("Cluster Manager Started")
	_ = m.pubsub.Broadcast(m.ctx, "cluster_restart", map[string]interface{}{})
	go m.webhook.ClusterStarted(m.ctx, m.cfg.TotalShards, m.cfg.ExpectedInstances)
	return nil
}

func (m *Manager) Stop(ctx context.Context) {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return
	}
	m.running = false
	m.mu.Unlock()

	m.cancel()
	m.wg.Wait()

	go m.webhook.ClusterStopped(ctx)
	m.pubsub.Stop()
	slog.Info("Cluster Manager Stopped")
}

// ---- pub/sub message handlers ----

func (m *Manager) onInstanceReady(msg *types.ClusterMessage) {
	instanceID, _ := msg.M["instance_id"].(string)
	if instanceID == "" {
		return
	}

	infoRaw, ok := msg.M["info"]
	if !ok {
		return
	}
	raw, err := json.Marshal(infoRaw)
	if err != nil {
		slog.Error("onInstanceReady marshal", "err", err)
		return
	}
	var info types.InstanceInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		slog.Error("onInstanceReady unmarshal", "err", err)
		return
	}

	ctx := m.ctx
	if err := m.registry.Register(ctx, instanceID, &info); err != nil {
		slog.Error("registry.Register", "err", err)
		return
	}

	m.mu.Lock()
	m.knownIDs[instanceID] = struct{}{}
	m.mu.Unlock()

	switch info.Status {
	case "connecting":
		m.handleStartup(ctx, instanceID)
		go m.webhook.InstanceConnected(ctx, instanceID, nil, "connecting")

	case "ready":
		if len(info.ShardIDs) > 0 {
			m.mu.Lock()
			var conflicts []int
			for _, sid := range info.ShardIDs {
				if ownerID, exists := m.shardMap[sid]; exists && ownerID != instanceID {
					conflicts = append(conflicts, sid)
				}
			}
			if len(conflicts) > 0 {
				slog.Warn("instance claims conflicting shards; clearing",
					"instance", instanceID, "conflicts", conflicts)
				info.ShardIDs = nil
				info.ShardCount = 0
				m.mu.Unlock()
				_ = m.registry.Register(ctx, instanceID, &info)
			} else {
				m.distributedIDs[instanceID] = struct{}{}
				for _, sid := range info.ShardIDs {
					m.shardMap[sid] = instanceID
				}
				if !m.shardsDistributed && len(m.knownIDs) >= m.cfg.ExpectedInstances {
					m.shardsDistributed = true
					slog.Info("all instances reconnected after restart", "count", len(m.knownIDs))
				}
				m.mu.Unlock()
			}
		}
		go m.webhook.InstanceReconnected(ctx, instanceID, info.ShardIDs)
	}

	m.events.Emit("instance_registered", instanceID, &info)
	slog.Info("instance registered", "instance_id", instanceID)
}

func (m *Manager) onHeartbeat(msg *types.ClusterMessage) {
	instanceID, _ := msg.M["instance_id"].(string)
	if instanceID == "" {
		return
	}

	m.mu.Lock()
	_, known := m.knownIDs[instanceID]
	m.mu.Unlock()
	if !known {
		return
	}

	ctx := m.ctx
	if _, hasInfo := msg.M["info"]; hasInfo {
		raw, err := json.Marshal(msg.M["info"])
		if err != nil {
			return
		}
		var info types.InstanceInfo
		if err := json.Unmarshal(raw, &info); err != nil {
			return
		}
		// Use manager-assigned shards, not instance-reported ones.
		m.mu.Lock()
		var assigned []int
		for sid, iid := range m.shardMap {
			if iid == instanceID {
				assigned = append(assigned, sid)
			}
		}
		m.mu.Unlock()
		sort.Ints(assigned)
		info.ShardIDs = assigned
		info.ShardCount = len(assigned)
		_ = m.registry.Register(ctx, instanceID, &info)
	} else {
		_ = m.registry.UpdateHeartbeat(ctx, instanceID)
	}
}

// ---- startup and shard distribution ----

func (m *Manager) handleStartup(ctx context.Context, instanceID string) {
	_ = m.pubsub.SendToInstance(ctx, instanceID, "startup_config", map[string]interface{}{
		"instance_id":        instanceID,
		"heartbeat_interval": m.cfg.HeartbeatInterval,
	})

	m.mu.Lock()
	_, alreadyDistributed := m.distributedIDs[instanceID]
	if alreadyDistributed {
		m.mu.Unlock()
		return
	}

	if m.distributing {
		slog.Info("instance arrived during distribution, queuing redistribution", "instance", instanceID)
		m.pendingRedistribution = true
		m.mu.Unlock()
		return
	}

	var connectingCount int
	for iid := range m.knownIDs {
		if _, done := m.distributedIDs[iid]; !done {
			connectingCount++
		}
	}

	shardsDistributed := m.shardsDistributed
	totalShardsLen := len(m.shardMap)
	m.mu.Unlock()

	if connectingCount >= m.cfg.ExpectedInstances {
		m.mu.Lock()
		if m.fallbackCancel != nil {
			m.fallbackCancel()
			m.fallbackCancel = nil
		}
		m.shardsDistributed = false
		m.mu.Unlock()
		slog.Info("all expected instances registered, distributing shards", "count", connectingCount)
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.runDistribution(ctx)
		}()
		return
	}

	if shardsDistributed {
		if totalShardsLen >= m.cfg.TotalShards {
			slog.Info("instance arrived after distribution; all shards covered, sending empty config", "instance", instanceID)
			_ = m.pubsub.SendToInstance(ctx, instanceID, "shard_config", map[string]interface{}{
				"shard_ids":   []int{},
				"shard_count": m.cfg.TotalShards,
				"instance_id": instanceID,
			})
		} else {
			slog.Info("instance arrived after distribution; expanding to cover unassigned shards", "instance", instanceID)
			m.wg.Add(1)
			go func() {
				defer m.wg.Done()
				m.runExpandDistribution(ctx)
			}()
		}
		return
	}

	m.mu.Lock()
	hasFallback := m.fallbackCancel != nil
	if !hasFallback {
		fallbackCtx, cancel := context.WithCancel(m.ctx)
		m.fallbackCancel = cancel
		slog.Info("waiting for remaining instances", "registered", connectingCount, "expected", m.cfg.ExpectedInstances)
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			defer func() {
				m.mu.Lock()
				m.fallbackCancel = nil
				m.mu.Unlock()
			}()
			m.fallbackDistribute(fallbackCtx)
		}()
	}
	m.mu.Unlock()
}

func (m *Manager) runDistribution(ctx context.Context) {
	if err := m.distributeShards(ctx); err != nil {
		slog.Error("distribution task failed", "err", err)
		return
	}
	m.mu.Lock()
	m.shardsDistributed = true
	m.mu.Unlock()
}

func (m *Manager) runExpandDistribution(ctx context.Context) {
	if err := m.expandDistribution(ctx); err != nil {
		slog.Error("expand distribution task failed", "err", err)
	}
}

func (m *Manager) fallbackDistribute(ctx context.Context) {
	select {
	case <-time.After(120 * time.Second):
	case <-ctx.Done():
		return
	}
	m.mu.Lock()
	already := m.shardsDistributed
	m.mu.Unlock()
	if already {
		return
	}
	m.mu.Lock()
	ready := len(m.knownIDs)
	m.mu.Unlock()
	slog.Warn("fallback: distributing with partial instances", "ready", ready, "expected", m.cfg.ExpectedInstances)
	if err := m.distributeShards(ctx); err != nil {
		slog.Error("fallback distribution failed", "err", err)
		return
	}
	m.mu.Lock()
	m.shardsDistributed = true
	m.mu.Unlock()
}

func (m *Manager) distributeShards(ctx context.Context) error {
	m.mu.Lock()
	if m.distributing {
		m.mu.Unlock()
		slog.Warn("distribution already in progress, skipping")
		return nil
	}
	m.distributing = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.distributing = false
		pending := m.pendingRedistribution
		if pending {
			m.pendingRedistribution = false
			m.shardsDistributed = false
		}
		m.mu.Unlock()

		if pending {
			slog.Info("processing pending redistribution for late-arriving instances")
			_ = m.expandDistribution(ctx)
			m.mu.Lock()
			m.shardsDistributed = true
			m.mu.Unlock()
		}
	}()

	instances, err := m.registry.ListAll(ctx)
	if err != nil {
		return err
	}

	m.mu.Lock()
	knownCopy := make(map[string]struct{}, len(m.knownIDs))
	for k, v := range m.knownIDs {
		knownCopy[k] = v
	}
	m.mu.Unlock()

	var active, stale []*types.InstanceInfo
	for _, inst := range instances {
		if _, ok := knownCopy[inst.ID]; ok {
			active = append(active, inst)
		} else {
			stale = append(stale, inst)
		}
	}
	if len(stale) > 0 {
		staleIDs := make([]string, len(stale))
		for i, s := range stale {
			staleIDs[i] = s.ID
		}
		slog.Info("removing stale instances", "count", len(stale), "ids", staleIDs)
		for _, inst := range stale {
			_ = m.registry.Unregister(ctx, inst.ID)
		}
	}
	if len(active) == 0 {
		return nil
	}
	sortInstancesByID(active)

	slog.Info("distributing shards", "instances", len(active))
	configs := m.distributor.distribute(len(active))

	m.mu.Lock()
	m.distributedIDs = make(map[string]struct{})
	m.shardMap = make(map[int]string)
	m.mu.Unlock()

	for i, cfg := range configs {
		if i >= len(active) {
			break
		}
		inst := active[i]
		cfg.InstanceID = inst.ID

		if err := m.pubsub.SendToInstance(ctx, inst.ID, "shard_config", map[string]interface{}{
			"shard_ids":   cfg.ShardIDs,
			"shard_count": cfg.ShardCount,
			"instance_id": cfg.InstanceID,
		}); err != nil {
			slog.Error("send shard_config failed", "instance", inst.ID, "err", err)
		}

		m.mu.Lock()
		m.distributedIDs[inst.ID] = struct{}{}
		for _, sid := range cfg.ShardIDs {
			m.shardMap[sid] = inst.ID
		}
		m.mu.Unlock()

		inst.ShardIDs = cfg.ShardIDs
		inst.LastHeartbeat = float64(time.Now().UnixNano()) / 1e9
		_ = m.registry.Register(ctx, inst.ID, inst)

		slog.Info("distributed shards", "shards", cfg.ShardIDs, "instance", inst.ID)

		if i < len(configs)-1 {
			delay := time.Duration(len(cfg.ShardIDs)*6) * time.Second
			slog.Info("waiting before next instance", "delay", delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	// Send empty config to surplus instances.
	for j := len(configs); j < len(active); j++ {
		inst := active[j]
		_ = m.pubsub.SendToInstance(ctx, inst.ID, "shard_config", map[string]interface{}{
			"shard_ids":   []int{},
			"shard_count": m.cfg.TotalShards,
			"instance_id": inst.ID,
		})
	}

	// Refresh heartbeat timestamps for all distributed instances.
	now := float64(time.Now().UnixNano()) / 1e9
	m.mu.Lock()
	dids := make([]string, 0, len(m.distributedIDs))
	for id := range m.distributedIDs {
		dids = append(dids, id)
	}
	m.mu.Unlock()
	for _, iid := range dids {
		if inst, _ := m.registry.Get(ctx, iid); inst != nil {
			inst.LastHeartbeat = now
			_ = m.registry.Register(ctx, iid, inst)
		}
	}
	return nil
}

func (m *Manager) expandDistribution(ctx context.Context) error {
	instances, err := m.registry.ListAll(ctx)
	if err != nil || len(instances) == 0 {
		return err
	}
	sortInstancesByID(instances)

	m.mu.Lock()
	var assignedShards []int
	for sid, iid := range m.shardMap {
		if _, ok := m.distributedIDs[iid]; ok {
			assignedShards = append(assignedShards, sid)
		}
	}
	assignedSet := make(map[int]struct{}, len(assignedShards))
	for _, s := range assignedShards {
		assignedSet[s] = struct{}{}
	}
	distributedCopy := make(map[string]struct{}, len(m.distributedIDs))
	for k, v := range m.distributedIDs {
		distributedCopy[k] = v
	}
	m.mu.Unlock()

	var unassigned []*types.InstanceInfo
	for _, inst := range instances {
		if _, ok := distributedCopy[inst.ID]; !ok {
			unassigned = append(unassigned, inst)
		}
	}
	if len(unassigned) == 0 {
		return nil
	}

	var remaining []int
	for s := 0; s < m.cfg.TotalShards; s++ {
		if _, ok := assignedSet[s]; !ok {
			remaining = append(remaining, s)
		}
	}
	if len(remaining) == 0 {
		slog.Info("no unassigned shards to distribute")
		for _, inst := range unassigned {
			_ = m.pubsub.SendToInstance(ctx, inst.ID, "shard_config", map[string]interface{}{
				"shard_ids":   []int{},
				"shard_count": m.cfg.TotalShards,
				"instance_id": inst.ID,
			})
		}
		return nil
	}

	perInstance := len(remaining) / len(unassigned)
	if perInstance < 1 {
		perInstance = 1
	}
	offset := 0
	for i, inst := range unassigned {
		var chunk []int
		if i == len(unassigned)-1 {
			chunk = remaining[offset:]
		} else {
			chunk = remaining[offset : offset+perInstance]
		}
		offset += perInstance

		_ = m.pubsub.SendToInstance(ctx, inst.ID, "shard_config", map[string]interface{}{
			"shard_ids":   chunk,
			"shard_count": m.cfg.TotalShards,
			"instance_id": inst.ID,
		})

		m.mu.Lock()
		m.distributedIDs[inst.ID] = struct{}{}
		for _, sid := range chunk {
			m.shardMap[sid] = inst.ID
		}
		m.mu.Unlock()

		inst.ShardIDs = chunk
		inst.LastHeartbeat = float64(time.Now().UnixNano()) / 1e9
		_ = m.registry.Register(ctx, inst.ID, inst)

		slog.Info("expanded distribution", "shards", chunk, "instance", inst.ID)

		if i < len(unassigned)-1 {
			delay := time.Duration(len(chunk)*6) * time.Second
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

// ---- monitor loop ----

func (m *Manager) monitorLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(m.cfg.HeartbeatInterval) * time.Second)
	defer ticker.Stop()

	guildCounter := 0
	startupGrace := true

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if startupGrace {
				instances, _ := m.registry.ListAll(ctx)
				m.mu.Lock()
				distributed := m.shardsDistributed
				m.mu.Unlock()
				if len(instances) >= m.cfg.ExpectedInstances || distributed {
					startupGrace = false
				}
				continue
			}

			m.mu.Lock()
			distributing := m.distributing
			m.mu.Unlock()
			if distributing {
				continue
			}

			removed, err := m.reapDeadInstances(ctx)
			if err != nil {
				slog.Error("monitor reap error", "err", err)
				continue
			}

			m.mu.Lock()
			distributed := m.shardsDistributed
			startupWindow := time.Since(m.startTime) < 300*time.Second
			m.mu.Unlock()

			if len(removed) > 0 && distributed && !startupWindow {
				m.wg.Add(1)
				go func() {
					defer m.wg.Done()
					m.tryRecovery(ctx, removed)
				}()
			}

			guildCounter++
			if guildCounter >= 6 {
				_ = m.RequestGuildUpdate(ctx, "")
				stats, _ := m.GuildStats(ctx)
				if stats["instances"].(int) > 0 {
					instances, _ := m.registry.ListAll(ctx)
					var latencies []float64
					for _, inst := range instances {
						for _, s := range inst.Shards {
							if s.Latency > 0 {
								latencies = append(latencies, s.Latency)
							}
						}
					}
					var avgLatency float64
					if len(latencies) > 0 {
						sum := 0.0
						for _, l := range latencies {
							sum += l
						}
						avgLatency = sum / float64(len(latencies))
					}
					go m.webhook.ClusterStats(ctx,
						stats["total_guilds"].(int),
						stats["total_users"].(int),
						stats["instances"].(int),
						m.cfg.TotalShards,
						avgLatency,
					)
				}
				guildCounter = 0
			}
		}
	}
}

func (m *Manager) reapDeadInstances(ctx context.Context) ([]*types.InstanceInfo, error) {
	instances, err := m.registry.ListAll(ctx)
	if err != nil {
		return nil, err
	}

	now := float64(time.Now().UnixNano()) / 1e9
	timeout := float64(m.cfg.HeartbeatTimeout)
	var removed []*types.InstanceInfo

	m.mu.Lock()
	for _, inst := range instances {
		if _, known := m.knownIDs[inst.ID]; !known {
			m.mu.Unlock()
			_ = m.registry.Unregister(ctx, inst.ID)
			m.mu.Lock()
			continue
		}
		if now-inst.LastHeartbeat > timeout {
			delete(m.distributedIDs, inst.ID)
			delete(m.knownIDs, inst.ID)
			m.mu.Unlock()
			_ = m.registry.Unregister(ctx, inst.ID)
			m.mu.Lock()
			removed = append(removed, inst)
			m.events.Emit("instance_removed", inst.ID)
			slog.Warn("removed dead instance", "instance", inst.ID)
			if time.Since(m.startTime) > 120*time.Second {
				go m.webhook.InstanceDisconnected(ctx, inst.ID, inst.ShardIDs)
			}
		}
	}
	m.mu.Unlock()

	if len(removed) > 0 {
		alive, _ := m.registry.ListAll(ctx)
		m.mu.Lock()
		m.shardMap = make(map[int]string)
		for _, inst := range alive {
			for _, sid := range inst.ShardIDs {
				m.shardMap[sid] = inst.ID
			}
		}
		m.mu.Unlock()
	}
	return removed, nil
}

func (m *Manager) tryRecovery(ctx context.Context, dead []*types.InstanceInfo) {
	var orphaned []int
	for _, inst := range dead {
		orphaned = append(orphaned, inst.ShardIDs...)
	}
	if len(orphaned) == 0 {
		return
	}

	alive, err := m.registry.ListAll(ctx)
	if err != nil || len(alive) == 0 {
		slog.Error("no alive instances for shard recovery")
		return
	}

	deadIDs := make([]string, len(dead))
	for i, d := range dead {
		deadIDs[i] = d.ID
	}
	slog.Info("recovering orphaned shards", "shards", orphaned, "alive", len(alive))
	go m.webhook.ShardRecovery(ctx, deadIDs, orphaned, len(alive))

	target := alive[0]
	for _, inst := range alive[1:] {
		if len(inst.ShardIDs) < len(target.ShardIDs) {
			target = inst
		}
	}

	m.mu.Lock()
	for _, sid := range orphaned {
		m.shardMap[sid] = target.ID
	}
	m.mu.Unlock()

	newSet := make(map[int]struct{}, len(target.ShardIDs)+len(orphaned))
	for _, s := range target.ShardIDs {
		newSet[s] = struct{}{}
	}
	for _, s := range orphaned {
		newSet[s] = struct{}{}
	}
	merged := make([]int, 0, len(newSet))
	for s := range newSet {
		merged = append(merged, s)
	}
	sort.Ints(merged)
	target.ShardIDs = merged
	_ = m.registry.Register(ctx, target.ID, target)

	sort.Ints(orphaned)
	_ = m.pubsub.SendToInstance(ctx, target.ID, "add_shards", map[string]interface{}{
		"shard_ids":   orphaned,
		"shard_count": m.cfg.TotalShards,
	})
	slog.Info("assigned recovery shards", "shards", orphaned, "instance", target.ID)
}

// ---- routing helpers ----

func (m *Manager) shardForGuild(guildID int64) int {
	return int((guildID >> 22) % int64(m.cfg.TotalShards))
}

func (m *Manager) instanceForShard(ctx context.Context, shardID int) string {
	m.mu.Lock()
	if iid, ok := m.shardMap[shardID]; ok {
		m.mu.Unlock()
		return iid
	}
	m.mu.Unlock()

	instances, _ := m.registry.ListAll(ctx)
	m.mu.Lock()
	for _, inst := range instances {
		for _, sid := range inst.ShardIDs {
			m.shardMap[sid] = inst.ID
		}
	}
	iid := m.shardMap[shardID]
	m.mu.Unlock()
	return iid
}

// ---- data lookup methods ----

func parseRedis(v interface{}) interface{} {
	switch val := v.(type) {
	case string:
		var out interface{}
		if err := json.Unmarshal([]byte(val), &out); err == nil {
			return out
		}
	case []byte:
		var out interface{}
		if err := json.Unmarshal(val, &out); err == nil {
			return out
		}
	}
	return v
}

func sortInstancesByID(instances []*types.InstanceInfo) {
	sort.Slice(instances, func(i, j int) bool {
		aPrefix, aNum, aHasNum := splitTrailingNumber(instances[i].ID)
		bPrefix, bNum, bHasNum := splitTrailingNumber(instances[j].ID)

		if aPrefix != bPrefix {
			return aPrefix < bPrefix
		}
		if aHasNum && bHasNum && aNum != bNum {
			return aNum < bNum
		}
		if aHasNum != bHasNum {
			return aHasNum
		}
		return instances[i].ID < instances[j].ID
	})
}

func splitTrailingNumber(id string) (string, int, bool) {
	end := len(id) - 1
	for end >= 0 && id[end] >= '0' && id[end] <= '9' {
		end--
	}
	if end == len(id)-1 {
		return id, 0, false
	}
	n, err := strconv.Atoi(id[end+1:])
	if err != nil {
		return id, 0, false
	}
	return id[:end+1], n, true
}

// FindGuild finds a guild by ID, checking cache then the owning shard, then broadcasting.
func (m *Manager) FindGuild(ctx context.Context, guildID int64) (map[string]interface{}, error) {
	cacheKey := fmt.Sprintf("cache:guild:%d", guildID)
	if cached, _ := m.redis.Get(ctx, cacheKey).Result(); cached != "" {
		if d, ok := parseRedis(cached).(map[string]interface{}); ok && d["error"] != "not_found" {
			return d, nil
		}
	}

	responseKey := fmt.Sprintf("cluster:guild_response:%d", guildID)
	shardID := m.shardForGuild(guildID)
	targetID := m.instanceForShard(ctx, shardID)

	if targetID != "" {
		_ = m.pubsub.SendToInstance(ctx, targetID, "request_guild", map[string]interface{}{
			"guild_id":     guildID,
			"response_key": responseKey,
		})
		result, err := m.redis.BLPop(ctx, 2*time.Second, responseKey).Result()
		if err == nil && len(result) > 1 {
			if d, ok := parseRedis(result[1]).(map[string]interface{}); ok && d["error"] != "not_found" {
				data, _ := json.Marshal(d)
				_ = m.redis.SetEx(ctx, cacheKey, data, 60*time.Second).Err()
				return d, nil
			}
		}
	}

	instances, _ := m.registry.ListAll(ctx)
	if len(instances) == 0 {
		return nil, nil
	}

	_ = m.pubsub.Broadcast(ctx, "request_guild", map[string]interface{}{
		"guild_id":     guildID,
		"response_key": responseKey,
	})
	for range instances {
		result, err := m.redis.BLPop(ctx, 1*time.Second, responseKey).Result()
		if err != nil {
			break
		}
		if d, ok := parseRedis(result[1]).(map[string]interface{}); ok && d["error"] != "not_found" {
			data, _ := json.Marshal(d)
			_ = m.redis.SetEx(ctx, cacheKey, data, 60*time.Second).Err()
			return d, nil
		}
	}
	_ = m.redis.Del(ctx, responseKey)
	return map[string]interface{}{"guild_id": fmt.Sprintf("%d", guildID), "error": "not_found"}, nil
}

// FindUser finds a user by ID, checking cache first then broadcasting.
func (m *Manager) FindUser(ctx context.Context, userID int64) (map[string]interface{}, error) {
	cacheKey := fmt.Sprintf("cache:user:%d", userID)
	if cached, _ := m.redis.Get(ctx, cacheKey).Result(); cached != "" {
		if d, ok := parseRedis(cached).(map[string]interface{}); ok {
			return d, nil
		}
	}

	instances, _ := m.registry.ListAll(ctx)
	if len(instances) == 0 {
		return nil, nil
	}

	responseKey := fmt.Sprintf("cluster:user_response:%d", userID)
	_ = m.pubsub.Broadcast(ctx, "request_user", map[string]interface{}{
		"user_id":      userID,
		"response_key": responseKey,
	})

	for range instances {
		result, err := m.redis.BLPop(ctx, 1*time.Second, responseKey).Result()
		if err != nil {
			break
		}
		if d, ok := parseRedis(result[1]).(map[string]interface{}); ok && d["status"] != "not_found" {
			data, _ := json.Marshal(d)
			_ = m.redis.SetEx(ctx, cacheKey, data, 30*time.Second).Err()
			return d, nil
		}
	}
	_ = m.redis.Del(ctx, responseKey)
	return nil, nil
}

// FindUsers finds multiple users, using cached results where available.
func (m *Manager) FindUsers(ctx context.Context, userIDs []int64) (map[int64]map[string]interface{}, error) {
	results := make(map[int64]map[string]interface{})
	if len(userIDs) == 0 {
		return results, nil
	}

	var uncached []int64
	pipe := m.redis.Pipeline()
	cmds := make([]*redis.StringCmd, len(userIDs))
	for i, uid := range userIDs {
		cmds[i] = pipe.Get(ctx, fmt.Sprintf("cache:user:%d", uid))
	}
	_, _ = pipe.Exec(ctx)

	for i, uid := range userIDs {
		val, err := cmds[i].Result()
		if err == nil {
			if d, ok := parseRedis(val).(map[string]interface{}); ok && d["status"] != "not_found" {
				results[uid] = d
				continue
			}
		}
		uncached = append(uncached, uid)
	}

	if len(uncached) == 0 {
		return results, nil
	}

	instances, _ := m.registry.ListAll(ctx)
	if len(instances) == 0 {
		return results, nil
	}

	responseKey := fmt.Sprintf("cluster:bulk_user_response:%d", time.Now().UnixNano())
	_ = m.redis.Del(ctx, responseKey)
	_ = m.pubsub.Broadcast(ctx, "request_users_bulk", map[string]interface{}{
		"user_ids":     uncached,
		"response_key": responseKey,
	})

	for range instances {
		result, err := m.redis.BLPop(ctx, 2*time.Second, responseKey).Result()
		if err != nil {
			break
		}
		if d, ok := parseRedis(result[1]).(map[string]interface{}); ok {
			if users, ok := d["users"].([]interface{}); ok {
				cachePipe := m.redis.Pipeline()
				for _, u := range users {
					if umap, ok := u.(map[string]interface{}); ok {
						uidRaw := umap["user_id"]
						uid := toInt64(uidRaw)
						if uid != 0 {
							results[uid] = umap
							data, _ := json.Marshal(umap)
							cachePipe.SetEx(ctx, fmt.Sprintf("cache:user:%d", uid), data, 30*time.Second)
						}
					}
				}
				_, _ = cachePipe.Exec(ctx)
			}
		}
		if len(results) >= len(userIDs) {
			break
		}
	}
	_ = m.redis.Del(ctx, responseKey)
	return results, nil
}

// FindMutualGuilds returns all guilds a user shares with the bot.
func (m *Manager) FindMutualGuilds(ctx context.Context, userID int64) ([]map[string]interface{}, error) {
	cacheKey := fmt.Sprintf("cache:mutuals:%d", userID)
	if cached, _ := m.redis.Get(ctx, cacheKey).Result(); cached != "" {
		if d, ok := parseRedis(cached).([]interface{}); ok {
			out := make([]map[string]interface{}, 0, len(d))
			for _, item := range d {
				if m, ok := item.(map[string]interface{}); ok {
					out = append(out, m)
				}
			}
			return out, nil
		}
	}

	instances, _ := m.registry.ListAll(ctx)
	if len(instances) == 0 {
		return nil, nil
	}

	responseKey := fmt.Sprintf("cluster:mutuals_response:%d", userID)
	_ = m.redis.Del(ctx, responseKey)
	_ = m.pubsub.Broadcast(ctx, "request_mutuals", map[string]interface{}{
		"user_id":      userID,
		"response_key": responseKey,
	})

	var mutuals []map[string]interface{}
	for range instances {
		result, err := m.redis.BLPop(ctx, 2*time.Second, responseKey).Result()
		if err != nil {
			break
		}
		if d, ok := parseRedis(result[1]).([]interface{}); ok {
			for _, item := range d {
				if gm, ok := item.(map[string]interface{}); ok {
					mutuals = append(mutuals, gm)
				}
			}
		}
	}
	_ = m.redis.Del(ctx, responseKey)
	if len(mutuals) > 0 {
		data, _ := json.Marshal(mutuals)
		_ = m.redis.SetEx(ctx, cacheKey, data, 15*time.Second).Err()
	}
	return mutuals, nil
}

// FindGuildChannels returns all channels for a guild.
func (m *Manager) FindGuildChannels(ctx context.Context, guildID int64) ([]map[string]interface{}, error) {
	cacheKey := fmt.Sprintf("cache:channels:%d", guildID)
	if cached, _ := m.redis.Get(ctx, cacheKey).Result(); cached != "" {
		if d, ok := parseRedis(cached).([]interface{}); ok {
			return toMapSlice(d), nil
		}
	}

	responseKey := fmt.Sprintf("cluster:channels_response:%d", guildID)
	shardID := m.shardForGuild(guildID)
	targetID := m.instanceForShard(ctx, shardID)

	if targetID != "" {
		_ = m.pubsub.SendToInstance(ctx, targetID, "request_channels", map[string]interface{}{
			"guild_id":     guildID,
			"response_key": responseKey,
		})
		result, err := m.redis.BLPop(ctx, 2*time.Second, responseKey).Result()
		if err == nil && len(result) > 1 {
			if d, ok := parseRedis(result[1]).([]interface{}); ok {
				channels := toMapSlice(d)
				data, _ := json.Marshal(channels)
				_ = m.redis.SetEx(ctx, cacheKey, data, 30*time.Second).Err()
				return channels, nil
			}
		}
	}

	instances, _ := m.registry.ListAll(ctx)
	if len(instances) == 0 {
		return nil, nil
	}

	_ = m.pubsub.Broadcast(ctx, "request_channels", map[string]interface{}{
		"guild_id":     guildID,
		"response_key": responseKey,
	})
	for range instances {
		result, err := m.redis.BLPop(ctx, 1*time.Second, responseKey).Result()
		if err != nil {
			break
		}
		if d, ok := parseRedis(result[1]).([]interface{}); ok {
			channels := toMapSlice(d)
			data, _ := json.Marshal(channels)
			_ = m.redis.SetEx(ctx, cacheKey, data, 30*time.Second).Err()
			return channels, nil
		}
	}
	_ = m.redis.Del(ctx, responseKey)
	return nil, nil
}

// FindGuildChannel returns a specific channel within a guild.
func (m *Manager) FindGuildChannel(ctx context.Context, guildID, channelID int64) (map[string]interface{}, error) {
	cacheKey := fmt.Sprintf("cache:channel:%d:%d", guildID, channelID)
	if cached, _ := m.redis.Get(ctx, cacheKey).Result(); cached != "" {
		if d, ok := parseRedis(cached).(map[string]interface{}); ok && d["error"] != "not_found" {
			return d, nil
		}
	}

	responseKey := fmt.Sprintf("cluster:channel_response:%d:%d", guildID, channelID)
	shardID := m.shardForGuild(guildID)
	targetID := m.instanceForShard(ctx, shardID)

	if targetID != "" {
		_ = m.pubsub.SendToInstance(ctx, targetID, "request_channel", map[string]interface{}{
			"guild_id":     guildID,
			"channel_id":   channelID,
			"response_key": responseKey,
		})
		result, err := m.redis.BLPop(ctx, 2*time.Second, responseKey).Result()
		if err == nil && len(result) > 1 {
			if d, ok := parseRedis(result[1]).(map[string]interface{}); ok && d["error"] != "not_found" {
				data, _ := json.Marshal(d)
				_ = m.redis.SetEx(ctx, cacheKey, data, 30*time.Second).Err()
				return d, nil
			}
		}
	}

	instances, _ := m.registry.ListAll(ctx)
	if len(instances) == 0 {
		return nil, nil
	}

	_ = m.pubsub.Broadcast(ctx, "request_channel", map[string]interface{}{
		"guild_id":     guildID,
		"channel_id":   channelID,
		"response_key": responseKey,
	})
	for range instances {
		result, err := m.redis.BLPop(ctx, 1*time.Second, responseKey).Result()
		if err != nil {
			break
		}
		if d, ok := parseRedis(result[1]).(map[string]interface{}); ok && d["error"] != "not_found" {
			data, _ := json.Marshal(d)
			_ = m.redis.SetEx(ctx, cacheKey, data, 30*time.Second).Err()
			return d, nil
		}
	}
	_ = m.redis.Del(ctx, responseKey)
	return map[string]interface{}{
		"guild_id":   fmt.Sprintf("%d", guildID),
		"channel_id": fmt.Sprintf("%d", channelID),
		"error":      "not_found",
	}, nil
}

// ListCommands requests the command list from the first available instance.
func (m *Manager) ListCommands(ctx context.Context) ([]map[string]interface{}, error) {
	instances, _ := m.registry.ListAll(ctx)
	if len(instances) == 0 {
		return nil, nil
	}

	instanceID := instances[0].ID
	responseKey := fmt.Sprintf("cluster:commands_response:%s", instanceID)
	_ = m.redis.Del(ctx, responseKey)
	_ = m.pubsub.SendToInstance(ctx, instanceID, "request_commands", map[string]interface{}{})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		val, err := m.redis.Get(ctx, responseKey).Result()
		if err == nil {
			_ = m.redis.Del(ctx, responseKey)
			if d, ok := parseRedis(val).([]interface{}); ok {
				return toMapSlice(d), nil
			}
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, nil
}

// State returns a snapshot of the current cluster state.
func (m *Manager) State(ctx context.Context) (*types.ClusterState, error) {
	instances, err := m.registry.ListAll(ctx)
	if err != nil {
		return nil, err
	}
	state := &types.ClusterState{
		Instances:   make(map[string]*types.InstanceInfo, len(instances)),
		TotalShards: m.cfg.TotalShards,
	}
	for _, inst := range instances {
		state.Instances[inst.ID] = inst
		state.ActiveShards += inst.ShardCount
		state.TotalGuilds += inst.Guilds
	}
	return state, nil
}

// BroadcastGlobalEvent sends an event to all instances.
func (m *Manager) BroadcastGlobalEvent(ctx context.Context, event string, data map[string]interface{}) error {
	return m.pubsub.BroadcastGlobalEvent(ctx, event, data)
}

// RequestGuildUpdate asks one or all instances to resend their guild data.
func (m *Manager) RequestGuildUpdate(ctx context.Context, instanceID string) error {
	if instanceID != "" {
		return m.pubsub.SendToInstance(ctx, instanceID, "request_guild_data", map[string]interface{}{})
	}
	return m.pubsub.Broadcast(ctx, "request_guild_data", map[string]interface{}{})
}

// AllGuilds aggregates guild data stored by all instances.
func (m *Manager) AllGuilds(ctx context.Context) ([]map[string]interface{}, error) {
	instances, err := m.registry.ListAll(ctx)
	if err != nil {
		return nil, err
	}
	var guilds []map[string]interface{}
	for _, inst := range instances {
		data, _ := m.registry.GetGuildData(ctx, inst.ID)
		guilds = append(guilds, data...)
	}
	return guilds, nil
}

// GuildStats returns aggregated guild and user counts.
func (m *Manager) GuildStats(ctx context.Context) (map[string]interface{}, error) {
	instances, err := m.registry.ListAll(ctx)
	if err != nil {
		return nil, err
	}
	distribution := make(map[string]interface{}, len(instances))
	var totalGuilds, totalUsers int
	for _, inst := range instances {
		totalGuilds += inst.Guilds
		totalUsers += inst.Users
		distribution[inst.ID] = map[string]interface{}{
			"guilds": inst.Guilds,
			"users":  inst.Users,
		}
	}
	return map[string]interface{}{
		"total_guilds": totalGuilds,
		"total_users":  totalUsers,
		"instances":    len(instances),
		"distribution": distribution,
	}, nil
}

func (m *Manager) clearOldInstances(ctx context.Context) error {
	instances, err := m.registry.ListAll(ctx)
	if err != nil {
		return err
	}
	for _, inst := range instances {
		_ = m.registry.Unregister(ctx, inst.ID)
	}
	if len(instances) > 0 {
		slog.Info("cleared stale instances", "count", len(instances))
	}
	return nil
}

// Config exposes the cluster configuration (used by API handlers).
func (m *Manager) Config() *config.Config { return m.cfg }

// ---- small helpers ----

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case float64:
		return int64(val)
	case int64:
		return val
	case int:
		return int64(val)
	}
	return 0
}

func toMapSlice(in []interface{}) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(in))
	for _, item := range in {
		if m, ok := item.(map[string]interface{}); ok {
			out = append(out, m)
		}
	}
	return out
}
