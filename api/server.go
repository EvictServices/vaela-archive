package api

import (
	"encoding/json"
	"log/slog"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"time"

	"cluster/core"
	"cluster/types"

	"github.com/shirou/gopsutil/v3/cpu"
	gopsnet "github.com/shirou/gopsutil/v3/net"
	"github.com/shirou/gopsutil/v3/mem"
)

// Server is the HTTP handler for the cluster management API.
type Server struct {
	manager *core.Manager
	mux     *http.ServeMux
}

// NewServer wires all routes and returns the Server.
func NewServer(m *core.Manager) *Server {
	s := &Server{manager: m, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cors(s.mux).ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /health", s.healthCheck)
	s.mux.HandleFunc("GET /commands", s.listCommands)
	s.mux.HandleFunc("GET /status", s.getStatus)
	s.mux.HandleFunc("GET /instances", internalOnly(s.listInstances))
	s.mux.HandleFunc("GET /guilds", internalOnly(s.getGuilds))
	s.mux.HandleFunc("GET /guilds/stats", internalOnly(s.getGuildStats))
	s.mux.HandleFunc("GET /guilds/refresh", s.refreshGuildData)
	s.mux.HandleFunc("POST /user/find", internalOnly(s.findUser))
	s.mux.HandleFunc("POST /users/find", internalOnly(s.findUsers))
	s.mux.HandleFunc("GET /guild/find", internalOnly(s.findGuild))
	s.mux.HandleFunc("GET /user/{user_id}/mutuals", internalOnly(s.getUserMutuals))
	s.mux.HandleFunc("GET /guild/{guild_id}/channels", internalOnly(s.getGuildChannels))
	s.mux.HandleFunc("GET /guild/{guild_id}/channels/{channel_id}", internalOnly(s.getGuildChannel))
}

// ---- middleware ----

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func internalOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		if host != "127.0.0.1" && host != "::1" {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

// ---- helpers ----

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("writeJSON encode error", "err", err)
	}
}

func readBody(r *http.Request) (map[string]interface{}, error) {
	var body map[string]interface{}
	if r.ContentLength == 0 {
		return body, nil
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return nil, err
	}
	return body, nil
}

func systemMetrics() map[string]interface{} {
	cpuPct, _ := cpu.Percent(0, false)
	cpuCount, _ := cpu.Counts(true)
	vmStat, _ := mem.VirtualMemory()
	netStat, _ := gopsnet.IOCounters(false)

	cpuPercent := 0.0
	if len(cpuPct) > 0 {
		cpuPercent = cpuPct[0]
	}

	memMetrics := map[string]interface{}{
		"total":     uint64(0),
		"used":      uint64(0),
		"available": uint64(0),
		"percent":   float64(0),
	}
	if vmStat != nil {
		memMetrics = map[string]interface{}{
			"total":     vmStat.Total,
			"used":      vmStat.Used,
			"available": vmStat.Available,
			"percent":   vmStat.UsedPercent,
		}
	}

	netMetrics := map[string]interface{}{
		"bytes_sent":   uint64(0),
		"bytes_recv":   uint64(0),
		"packets_sent": uint64(0),
		"packets_recv": uint64(0),
	}
	if len(netStat) > 0 {
		netMetrics = map[string]interface{}{
			"bytes_sent":   netStat[0].BytesSent,
			"bytes_recv":   netStat[0].BytesRecv,
			"packets_sent": netStat[0].PacketsSent,
			"packets_recv": netStat[0].PacketsRecv,
		}
	}

	return map[string]interface{}{
		"cpu": map[string]interface{}{
			"percent": cpuPercent,
			"count":   cpuCount,
		},
		"memory":  memMetrics,
		"network": netMetrics,
	}
}

// ---- route handlers ----

func (s *Server) healthCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	instances, err := s.manager.Registry().ListAll(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	sort.Slice(instances, func(i, j int) bool {
		minA := minShardID(instances[i].ShardIDs)
		minB := minShardID(instances[j].ShardIDs)
		return minA < minB
	})

	now := float64(time.Now().UnixNano()) / 1e9
	timeout := float64(s.manager.Config().HeartbeatTimeout)
	clusters := make(map[string]interface{}, len(instances))

	for _, inst := range instances {
		var avgLatency float64
		if len(inst.Shards) > 0 {
			sum := 0.0
			for _, sh := range inst.Shards {
				sum += sh.Latency
			}
			avgLatency = sum / float64(len(inst.Shards))
		}

		shardStart, shardEnd := 0, 0
		if len(inst.ShardIDs) > 0 {
			shardStart = inst.ShardIDs[0]
			shardEnd = inst.ShardIDs[len(inst.ShardIDs)-1]
		}

		shards := make([]map[string]interface{}, len(inst.Shards))
		for i, sh := range inst.Shards {
			shards[i] = map[string]interface{}{
				"id":      sh.ID,
				"latency": sh.Latency,
				"users":   sh.Users,
				"guilds":  sh.Guilds,
				"uptime":  sh.Uptime,
				"seen":    sh.Seen,
			}
		}

		clusters[inst.ID] = map[string]interface{}{
			"config": map[string]interface{}{
				"shard": map[string]interface{}{
					"start": shardStart,
					"end":   shardEnd,
				},
			},
			"last_heartbeat":  inst.LastHeartbeat,
			"uptime":          inst.Uptime,
			"alive":           now-inst.LastHeartbeat < timeout,
			"users":           inst.Users,
			"guilds":          inst.Guilds,
			"average_latency": avgLatency,
			"shards":          shards,
		}
	}

	var allLatencies []float64
	for _, inst := range instances {
		for _, sh := range inst.Shards {
			allLatencies = append(allLatencies, sh.Latency)
		}
	}
	avgLatency := 0.0
	if len(allLatencies) > 0 {
		sum := 0.0
		for _, l := range allLatencies {
			sum += l
		}
		avgLatency = sum / float64(len(allLatencies))
	}

	sys := systemMetrics()
	sys["latency"] = avgLatency

	var totalUsers, totalGuilds int
	for _, inst := range instances {
		totalUsers += inst.Users
		totalGuilds += inst.Guilds
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"users":    totalUsers,
		"guilds":   totalGuilds,
		"system":   sys,
		"clusters": clusters,
	})
}

func (s *Server) listCommands(w http.ResponseWriter, r *http.Request) {
	cmds, err := s.manager.ListCommands(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if cmds == nil {
		cmds = []map[string]interface{}{}
	}
	writeJSON(w, http.StatusOK, cmds)
}

func (s *Server) getStatus(w http.ResponseWriter, r *http.Request) {
	state, err := s.manager.State(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *Server) listInstances(w http.ResponseWriter, r *http.Request) {
	instances, err := s.manager.Registry().ListAll(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	out := make([]*types.InstanceInfo, 0, len(instances))
	out = append(out, instances...)
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) getGuilds(w http.ResponseWriter, r *http.Request) {
	guilds, err := s.manager.AllGuilds(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if guilds == nil {
		guilds = []map[string]interface{}{}
	}
	writeJSON(w, http.StatusOK, guilds)
}

func (s *Server) getGuildStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.manager.GuildStats(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) refreshGuildData(w http.ResponseWriter, r *http.Request) {
	if err := s.manager.RequestGuildUpdate(r.Context(), ""); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "requested"})
}

func (s *Server) findUser(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "Invalid JSON body"})
		return
	}
	rawID, ok := body["user_id"]
	if !ok || rawID == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "Missing user_id"})
		return
	}
	userID := toInt64(rawID)
	result, err := s.manager.FindUser(r.Context(), userID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if result == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": "User not found"})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) findUsers(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "Invalid JSON body"})
		return
	}
	rawIDs, ok := body["user_ids"].([]interface{})
	if !ok || len(rawIDs) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "Missing user_ids list"})
		return
	}
	if len(rawIDs) > 100 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "Max 100 user IDs"})
		return
	}
	userIDs := make([]int64, len(rawIDs))
	for i, v := range rawIDs {
		userIDs[i] = toInt64(v)
	}
	results, err := s.manager.FindUsers(r.Context(), userIDs)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	out := make([]map[string]interface{}, 0, len(results))
	for _, v := range results {
		out = append(out, v)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) findGuild(w http.ResponseWriter, r *http.Request) {
	idxStr := r.URL.Query().Get("idx")
	if idxStr == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "Missing idx parameter"})
		return
	}
	guildID, err := strconv.ParseInt(idxStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "Invalid idx parameter"})
		return
	}
	result, err := s.manager.FindGuild(r.Context(), guildID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if result == nil || result["error"] == "not_found" {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": "Guild not found"})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) getUserMutuals(w http.ResponseWriter, r *http.Request) {
	userIDStr := r.PathValue("user_id")
	userID, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "Invalid user_id"})
		return
	}
	result, err := s.manager.FindMutualGuilds(r.Context(), userID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if result == nil {
		result = []map[string]interface{}{}
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) getGuildChannels(w http.ResponseWriter, r *http.Request) {
	guildIDStr := r.PathValue("guild_id")
	guildID, err := strconv.ParseInt(guildIDStr, 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "Invalid guild_id"})
		return
	}
	result, err := s.manager.FindGuildChannels(r.Context(), guildID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if result == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": "Guild not found"})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) getGuildChannel(w http.ResponseWriter, r *http.Request) {
	guildID, err1 := strconv.ParseInt(r.PathValue("guild_id"), 10, 64)
	channelID, err2 := strconv.ParseInt(r.PathValue("channel_id"), 10, 64)
	if err1 != nil || err2 != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "Invalid path parameters"})
		return
	}
	result, err := s.manager.FindGuildChannel(r.Context(), guildID, channelID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if result == nil || result["error"] == "not_found" {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": "Channel not found"})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// ---- small helpers ----

func minShardID(ids []int) int {
	if len(ids) == 0 {
		return math.MaxInt
	}
	m := ids[0]
	for _, v := range ids[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case float64:
		return int64(val)
	case int64:
		return val
	case int:
		return int64(val)
	case string:
		n, _ := strconv.ParseInt(val, 10, 64)
		return n
	}
	return 0
}


