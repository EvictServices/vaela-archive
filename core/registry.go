package core

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"cluster/types"

	"github.com/redis/go-redis/v9"
)

const registryTTL = 300 * time.Second
const cacheFresh = 5 * time.Second

type instanceEntry struct {
	info *types.InstanceInfo
	ts   time.Time
}

type registry struct {
	redis *redis.Client
	mu    sync.RWMutex
	cache map[string]instanceEntry
}

func newRegistry(rdb *redis.Client) *registry {
	return &registry{
		redis: rdb,
		cache: make(map[string]instanceEntry),
	}
}

func (r *registry) Register(ctx context.Context, instanceID string, info *types.InstanceInfo) error {
	key := "cluster:instances:" + instanceID
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	pipe := r.redis.Pipeline()
	pipe.Set(ctx, key, data, registryTTL)
	pipe.SAdd(ctx, "cluster:instances", instanceID)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	r.cache[instanceID] = instanceEntry{info: info, ts: time.Now()}
	r.mu.Unlock()
	return nil
}

func (r *registry) Unregister(ctx context.Context, instanceID string) error {
	key := "cluster:instances:" + instanceID
	pipe := r.redis.Pipeline()
	pipe.Del(ctx, key)
	pipe.SRem(ctx, "cluster:instances", instanceID)
	_, err := pipe.Exec(ctx)
	r.mu.Lock()
	delete(r.cache, instanceID)
	r.mu.Unlock()
	return err
}

func (r *registry) Get(ctx context.Context, instanceID string) (*types.InstanceInfo, error) {
	r.mu.RLock()
	entry, ok := r.cache[instanceID]
	r.mu.RUnlock()
	if ok && time.Since(entry.ts) < cacheFresh {
		return entry.info, nil
	}

	data, err := r.redis.Get(ctx, "cluster:instances:"+instanceID).Bytes()
	if err == redis.Nil {
		r.mu.Lock()
		delete(r.cache, instanceID)
		r.mu.Unlock()
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var info types.InstanceInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.cache[instanceID] = instanceEntry{info: &info, ts: time.Now()}
	r.mu.Unlock()
	return &info, nil
}

func (r *registry) ListAll(ctx context.Context) ([]*types.InstanceInfo, error) {
	rawIDs, err := r.redis.SMembers(ctx, "cluster:instances").Result()
	if err != nil {
		return nil, err
	}
	if len(rawIDs) == 0 {
		r.mu.Lock()
		r.cache = make(map[string]instanceEntry)
		r.mu.Unlock()
		return nil, nil
	}

	keys := make([]string, len(rawIDs))
	for i, id := range rawIDs {
		keys[i] = "cluster:instances:" + id
	}

	vals, err := r.redis.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	instances := make([]*types.InstanceInfo, 0, len(rawIDs))
	r.mu.Lock()
	for i, val := range vals {
		if val == nil {
			delete(r.cache, rawIDs[i])
			continue
		}
		raw, _ := val.(string)
		var info types.InstanceInfo
		if err := json.Unmarshal([]byte(raw), &info); err != nil {
			continue
		}
		r.cache[rawIDs[i]] = instanceEntry{info: &info, ts: now}
		instances = append(instances, &info)
	}
	r.mu.Unlock()
	return instances, nil
}

func (r *registry) UpdateHeartbeat(ctx context.Context, instanceID string) error {
	info, err := r.Get(ctx, instanceID)
	if err != nil || info == nil {
		return err
	}
	info.LastHeartbeat = float64(time.Now().UnixNano()) / 1e9
	return r.Register(ctx, instanceID, info)
}

func (r *registry) GetGuildData(ctx context.Context, instanceID string) ([]map[string]interface{}, error) {
	raw, err := r.redis.Get(ctx, "cluster:guilds:"+instanceID).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var guilds []map[string]interface{}
	if err := json.Unmarshal(raw, &guilds); err != nil {
		return nil, err
	}
	return guilds, nil
}
