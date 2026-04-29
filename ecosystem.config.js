module.exports = {
  apps: [
    {
      name: "vaela",
      script: "./cluster",
      interpreter: "none",
      autorestart: true,
      watch: false,
      max_restarts: 10,
      restart_delay: 3000,
      env: {
        CLUSTER_REDIS_URL: "redis://localhost:6379",
        CLUSTER_TOTAL_SHARDS: "25",
        CLUSTER_SHARDS_PER_INSTANCE: "5",
        CLUSTER_EXPECTED_INSTANCES: "5",
        CLUSTER_HEARTBEAT_INTERVAL: "30",
        CLUSTER_MANAGER_HOST: "localhost",
        CLUSTER_MANAGER_PORT: "8000",
        CLUSTER_WEBHOOK_URL: "",
      },
    },
  ],
};
