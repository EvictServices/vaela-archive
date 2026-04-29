package core

import (
	"fmt"

	"cluster/types"
)

type distributor struct {
	totalShards       int
	shardsPerInstance int
}

func newDistributor(totalShards, shardsPerInstance int) *distributor {
	if totalShards < 1 {
		totalShards = 1
	}
	if shardsPerInstance < 1 {
		shardsPerInstance = 1
	}
	return &distributor{totalShards: totalShards, shardsPerInstance: shardsPerInstance}
}

func (d *distributor) maxInstances() int {
	m := d.totalShards / d.shardsPerInstance
	if m < 1 {
		return 1
	}
	return m
}

func (d *distributor) distribute(instanceCount int) []types.ShardConfig {
	if instanceCount < 1 {
		return nil
	}
	active := instanceCount
	if active > d.maxInstances() {
		active = d.maxInstances()
	}
	base := d.totalShards / active
	extra := d.totalShards % active

	configs := make([]types.ShardConfig, 0, active)
	current := 0
	for i := 0; i < active; i++ {
		count := base
		if i < extra {
			count++
		}
		ids := make([]int, count)
		for j := range count {
			ids[j] = current + j
		}
		configs = append(configs, types.ShardConfig{
			ShardIDs:   ids,
			ShardCount: d.totalShards,
			InstanceID: fmt.Sprintf("instance_%d", i),
		})
		current += count
	}
	return configs
}
