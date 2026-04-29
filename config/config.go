package config

import (
	"os"
	"strconv"
)

type Config struct {
	TotalShards       int
	ShardsPerInstance int
	ExpectedInstances int
	HeartbeatInterval int
	HeartbeatTimeout  int
	RedisURL          string
	ManagerHost       string
	ManagerPort       int
}

func Load() *Config {
	totalShards := envInt("CLUSTER_TOTAL_SHARDS", 25)
	shardsPerInstance := envInt("CLUSTER_SHARDS_PER_INSTANCE", 5)
	heartbeatInterval := envInt("CLUSTER_HEARTBEAT_INTERVAL", 30)
	return &Config{
		TotalShards:       totalShards,
		ShardsPerInstance: shardsPerInstance,
		ExpectedInstances: envInt("CLUSTER_EXPECTED_INSTANCES", 5),
		HeartbeatInterval: heartbeatInterval,
		HeartbeatTimeout:  heartbeatInterval * 3,
		RedisURL:          envStr("CLUSTER_REDIS_URL", "redis://localhost:6379"),
		ManagerHost:       envStr("CLUSTER_MANAGER_HOST", "localhost"),
		ManagerPort:       envInt("CLUSTER_MANAGER_PORT", 8000),
	}
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
