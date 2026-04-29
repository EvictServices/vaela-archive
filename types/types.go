package types

// ShardInfo holds per-shard statistics reported by an instance.
type ShardInfo struct {
	ID      int     `json:"id"`
	Latency float64 `json:"latency"`
	Users   int     `json:"users"`
	Guilds  int     `json:"guilds"`
	Uptime  float64 `json:"uptime"`
	Seen    float64 `json:"seen"`
}

// InstanceInfo holds the state for a connected cluster instance.
type InstanceInfo struct {
	ID            string      `json:"id"`
	Status        string      `json:"status"`
	ShardIDs      []int       `json:"shard_ids"`
	ShardCount    int         `json:"shard_count"`
	LastHeartbeat float64     `json:"last_heartbeat"`
	Uptime        float64     `json:"uptime"`
	Users         int         `json:"users"`
	Guilds        int         `json:"guilds"`
	Shards        []ShardInfo `json:"shards"`
}

// ShardConfig is a shard assignment delivered to an instance.
type ShardConfig struct {
	ShardIDs   []int  `json:"shard_ids"`
	ShardCount int    `json:"shard_count"`
	InstanceID string `json:"instance_id"`
}

// ClusterMessage is the pub/sub message envelope.
type ClusterMessage struct {
	T float64                `json:"t"`
	M map[string]interface{} `json:"m"`
	W string                 `json:"w"`
}

// ClusterState is a point-in-time snapshot of the cluster.
type ClusterState struct {
	Instances    map[string]*InstanceInfo `json:"instances"`
	TotalShards  int                      `json:"total_shards"`
	ActiveShards int                      `json:"active_shards"`
	TotalGuilds  int                      `json:"total_guilds"`
}
