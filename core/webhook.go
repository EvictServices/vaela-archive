package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

type webhook struct {
	url    string
	client *http.Client
}

func newWebhook() *webhook {
	url := os.Getenv("CLUSTER_WEBHOOK_URL")
	return &webhook{
		url:    url,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

func (w *webhook) send(ctx context.Context, embed map[string]interface{}) {
	if w.url == "" {
		return
	}
	payload, err := json.Marshal(map[string]interface{}{"embeds": []interface{}{embed}})
	if err != nil {
		slog.Error("webhook marshal error", "err", err)
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(payload))
	if err != nil {
		slog.Error("webhook request error", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		slog.Error("webhook send error", "err", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		slog.Warn("webhook unexpected status", "status", resp.StatusCode)
	}
}

func (w *webhook) timestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func (w *webhook) ClusterStarted(ctx context.Context, totalShards, expectedInstances int) {
	w.send(ctx, map[string]interface{}{
		"title": "Cluster Started",
		"description": fmt.Sprintf(
			"The cluster manager has started.\n**Shards:** `%d`\n**Expected Instances:** `%d`",
			totalShards, expectedInstances,
		),
		"color":     0x2ECC71,
		"timestamp": w.timestamp(),
	})
}

func (w *webhook) ClusterStopped(ctx context.Context) {
	w.send(ctx, map[string]interface{}{
		"title":       "Cluster Stopped",
		"description": "The cluster manager has shut down.",
		"color":       0xE74C3C,
		"timestamp":   w.timestamp(),
	})
}

func (w *webhook) InstanceConnected(ctx context.Context, instanceID string, shardIDs []int, status string) {
	w.send(ctx, map[string]interface{}{
		"title": "Instance Connected",
		"description": fmt.Sprintf(
			"**Instance:** `%s`\n**Status:** `%s`\n**Shards:** `%v`",
			instanceID, status, shardIDs,
		),
		"color":     0x3498DB,
		"timestamp": w.timestamp(),
	})
}

func (w *webhook) InstanceDisconnected(ctx context.Context, instanceID string, shardIDs []int) {
	w.send(ctx, map[string]interface{}{
		"title": "Instance Disconnected",
		"description": fmt.Sprintf(
			"**Instance:** `%s`\n**Lost Shards:** `%v`",
			instanceID, shardIDs,
		),
		"color":     0xE67E22,
		"timestamp": w.timestamp(),
	})
}

func (w *webhook) InstanceReconnected(ctx context.Context, instanceID string, shardIDs []int) {
	w.send(ctx, map[string]interface{}{
		"title": "Instance Reconnected",
		"description": fmt.Sprintf(
			"**Instance:** `%s`\n**Shards:** `%v`",
			instanceID, shardIDs,
		),
		"color":     0x9B59B6,
		"timestamp": w.timestamp(),
	})
}

func (w *webhook) ShardRecovery(ctx context.Context, deadInstances []string, orphanedShards []int, aliveCount int) {
	deadList := strings.Join(func() []string {
		out := make([]string, len(deadInstances))
		for i, id := range deadInstances {
			out[i] = "`" + id + "`"
		}
		return out
	}(), ", ")
	if deadList == "" {
		deadList = "None"
	}
	w.send(ctx, map[string]interface{}{
		"title": "Shard Auto-Recovery",
		"description": fmt.Sprintf(
			"**Dead Instances:** %s\n**Orphaned Shards:** `%v`\n**Alive Instances:** `%d`\nRedistributing shards across surviving instances.",
			deadList, orphanedShards, aliveCount,
		),
		"color":     0xF39C12,
		"timestamp": w.timestamp(),
	})
}

func (w *webhook) ClusterStats(ctx context.Context, totalGuilds, totalUsers, instances, shards int, latency float64) {
	latencyMs := int(latency * 1000)
	w.send(ctx, map[string]interface{}{
		"title": "Cluster Stats",
		"description": fmt.Sprintf(
			"**Guilds:** `%d`\n**Users:** `%d`\n**Instances:** `%d`\n**Shards:** `%d`\n**Avg Latency:** `%dms`",
			totalGuilds, totalUsers, instances, shards, latencyMs,
		),
		"color":     0x1ABC9C,
		"timestamp": w.timestamp(),
	})
}
