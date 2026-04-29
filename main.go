package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"cluster/api"
	"cluster/config"
	"cluster/core"
	"cluster/session"

	"github.com/redis/go-redis/v9"
	"github.com/joho/godotenv"
)

const (
	colorReset  = "\033[0m"
	colorCyan   = "\033[36m"
	colorYellow = "\033[33m"
	colorRed    = "\033[31m"
	colorMagenta = "\033[95m"
	colorBlue   = "\033[34m"
	colorDim    = "\033[90m"
)

type prettyHandler struct {
	w io.Writer
}

func (h *prettyHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *prettyHandler) WithAttrs(_ []slog.Attr) slog.Handler          { return h }
func (h *prettyHandler) WithGroup(_ string) slog.Handler               { return h }

func (h *prettyHandler) Handle(_ context.Context, r slog.Record) error {
	levelColor := colorCyan
	switch r.Level {
	case slog.LevelWarn:
		levelColor = colorYellow
	case slog.LevelError:
		levelColor = colorRed
	}

	ts := r.Time.Format("2006-01-02 15:04:05")
	short := r.Time.Format("15:04:05")

	var attrs bytes.Buffer
	r.Attrs(func(a slog.Attr) bool {
		fmt.Fprintf(&attrs, " %s%s=%v%s", colorDim, a.Key, a.Value, colorReset)
		return true
	})

	fmt.Fprintf(h.w, "%s%s%s — %s%s%s (%s%s%s): %s%s%s%s\n",
		levelColor, strings.ToUpper(r.Level.String()), colorReset,
		colorMagenta, ts, colorReset,
		colorMagenta, short, colorReset,
		colorDim, r.Message, attrs.String(), colorReset,
	)
	return nil
}

func tokenURLSafe(nbytes int) string {
	b := make([]byte, nbytes)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

type ClusterService struct {
	cfg     *config.Config
	redis   *redis.Client
	manager *core.Manager
	session *session.Manager
}

func newClusterService() *ClusterService {
	loadDotEnv()

	return &ClusterService{
		cfg:     config.Load(),
		session: session.New("/tmp/elara/cluster"),
	}
}

func loadDotEnv() {
	paths := []string{".env"}
	if exePath, err := os.Executable(); err == nil {
		paths = append(paths, filepath.Join(filepath.Dir(exePath), ".env"))
	}

	if err := godotenv.Load(paths...); err != nil {
		slog.Warn(".env not loaded", "paths", paths, "err", err)
		return
	}
	slog.Info("environment loaded", "paths", paths)
}

func (s *ClusterService) run(ctx context.Context) error {
	opts, err := redis.ParseURL(s.cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("Invalid Redis URL: %w", err)
	}
	s.redis = redis.NewClient(opts)
	if err := s.redis.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("Redis Ping Failed: %w", err)
	}
	slog.Info("Connected to Redis")

	discordStart := time.Now()
	resp, err := http.Get("https://discord.com/api/v10/gateway")
	if err != nil {
		slog.Warn("Discord RTT check failed", "err", err)
	} else {
		_ = resp.Body.Close()
		slog.Info("Discord RTT", "ping", time.Since(discordStart).Round(time.Millisecond).String())
	}

	clusterID, err := s.session.GetOrCreate(ctx)
	if err != nil {
		return fmt.Errorf("Session Error: %w", err)
	}

	lockKey := "cluster:manager_lock:" + clusterID
	lockToken := tokenURLSafe(8)
	if err := s.redis.Set(ctx, lockKey, lockToken, 300*time.Second).Err(); err != nil {
		return fmt.Errorf("Lock Error: %w", err)
	}
	defer func() {
		_ = s.redis.Del(context.Background(), lockKey)
	}()

	s.manager = core.NewManager(s.redis, s.session, s.cfg)
	if err := s.manager.Start(ctx); err != nil {
		return fmt.Errorf("Manager Start: %w", err)
	}

	slog.Info("Cluster Ready",
		"shards", s.cfg.TotalShards,
		"per_instance", s.cfg.ShardsPerInstance,
	)

	handler := api.NewServer(s.manager)
	srv := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", s.cfg.ManagerHost, s.cfg.ManagerPort),
		Handler: handler,
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("HTTP Server Listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)

	s.manager.Stop(shutdownCtx)
	_ = s.redis.Close()
	slog.Info("Cluster Service Stopped")
	return nil
}

func main() {
	slog.SetDefault(slog.New(&prettyHandler{w: os.Stdout}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := newClusterService().run(ctx); err != nil {
		slog.Error("Cluster Service Error", "err", err)
		os.Exit(1)
	}
}
