# cluster

A Discord bot cluster manager written in Go. Coordinates multiple bot instances over Redis pub/sub, distributes shards across them, monitors heartbeats, and exposes an HTTP API for health and data lookups.

## How it works

1. Bot instances connect and announce themselves via the `instance_ready` pub/sub message.
2. The manager waits until the expected number of instances are online, then distributes shard ranges evenly across them.
3. Each instance sends periodic `heartbeat` messages. The manager reaps dead instances and redistributes their shards automatically.
4. A distributed lock in Redis (`cluster:manager_lock:<id>`) prevents multiple managers from running simultaneously.
5. Lifecycle events (start, stop, instance connect/disconnect, recovery) are sent to a Discord webhook.

## Requirements

- Go 1.22+
- Redis

## Configuration

All settings are read from environment variables.

| Variable | Default | Description |
|---|---|---|
| `CLUSTER_REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `CLUSTER_TOTAL_SHARDS` | `25` | Total number of Discord shards |
| `CLUSTER_SHARDS_PER_INSTANCE` | `5` | Shards assigned to each instance |
| `CLUSTER_EXPECTED_INSTANCES` | `5` | Number of instances to wait for before distributing |
| `CLUSTER_HEARTBEAT_INTERVAL` | `30` | Seconds between heartbeats |
| `CLUSTER_MANAGER_HOST` | `localhost` | HTTP server bind host |
| `CLUSTER_MANAGER_PORT` | `8000` | HTTP server bind port |
| `CLUSTER_WEBHOOK_URL` | _(none)_ | Discord webhook URL for lifecycle notifications |

## Build & run

```sh
go mod tidy
go build -o cluster .
./cluster
```

## API

Endpoints marked **internal** only accept requests from `127.0.0.1` / `::1`.

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/health` | public | Per-instance shard, latency, and system metrics |
| `GET` | `/status` | public | Full cluster state snapshot |
| `GET` | `/commands` | public | Command list from the first available instance |
| `GET` | `/instances` | internal | All registered instances |
| `GET` | `/guilds` | internal | Aggregated guild list |
| `GET` | `/guilds/stats` | internal | Guild and user counts per instance |
| `GET` | `/guilds/refresh` | internal | Ask all instances to resend guild data |
| `POST` | `/user/find` | internal | Find a user by ID (`{"user_id": 123}`) |
| `POST` | `/users/find` | internal | Find multiple users (`{"user_ids": [...]}`, max 100) |
| `GET` | `/guild/find?idx=<id>` | internal | Find a guild by ID |
| `GET` | `/user/{user_id}/mutuals` | internal | Guilds shared between a user and the bot |
| `GET` | `/guild/{guild_id}/channels` | internal | All channels in a guild |
| `GET` | `/guild/{guild_id}/channels/{channel_id}` | internal | A specific channel |

## Project structure

```
main.go                 – entry point, service lifecycle
config/config.go        – environment variable loading
session/session.go      – filesystem-backed cluster ID persistence
events/events.go        – in-process event emitter
pubsub/pubsub.go        – Redis pub/sub with automatic reconnection
types/types.go          – shared data types
core/
  distributor.go        – even shard distribution algorithm
  registry.go           – Redis-backed instance registry with local cache
  manager.go            – central coordinator (distribution, monitoring, lookups)
  webhook.go            – Discord webhook notifications
api/server.go           – HTTP API handlers
```
