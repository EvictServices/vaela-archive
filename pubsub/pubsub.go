package pubsub

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"cluster/events"
	"cluster/types"

	"github.com/redis/go-redis/v9"
)

var channels = []string{"cluster:broadcast", "cluster:instances", "cluster:global"}

type MessageHandler func(msg *types.ClusterMessage)

type PubSub struct {
	redis   *redis.Client
	events  *events.EventEmitter
	mu      sync.RWMutex
	handlers map[string][]MessageHandler

	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	ps      *redis.PubSub
}

func New(rdb *redis.Client, em *events.EventEmitter) *PubSub {
	return &PubSub{
		redis:    rdb,
		events:   em,
		handlers: make(map[string][]MessageHandler),
	}
}

func (p *PubSub) On(msgType string, fn MessageHandler) {
	p.mu.Lock()
	p.handlers[msgType] = append(p.handlers[msgType], fn)
	p.mu.Unlock()
}

func (p *PubSub) Start(ctx context.Context) error {
	if p.running {
		return nil
	}
	p.running = true
	listenCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.ps = p.redis.Subscribe(listenCtx, channels...)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.listen(listenCtx)
	}()
	return nil
}

func (p *PubSub) Stop() {
	if !p.running {
		return
	}
	p.running = false
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
	if p.ps != nil {
		_ = p.ps.Close()
	}
}

func (p *PubSub) listen(ctx context.Context) {
	ch := p.ps.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				if !p.running {
					return
				}
				if p.reconnect(ctx) {
					ch = p.ps.Channel()
					slog.Info("PubSub recovered, resuming listen loop")
				} else {
					slog.Error("PubSub reconnect exhausted, stopping listener")
					return
				}
				continue
			}
			p.dispatch(msg.Payload)
		}
	}
}

func (p *PubSub) reconnect(ctx context.Context) bool {
	for attempt := 1; attempt <= 10; attempt++ {
		if !p.running {
			return false
		}
		delay := time.Duration(min(attempt*2, 30)) * time.Second
		if attempt == 1 {
			delay = 500 * time.Millisecond
		}
		slog.Info("PubSub reconnect attempt", "attempt", attempt, "delay", delay)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(delay):
		}
		_ = p.ps.Close()
		p.ps = p.redis.Subscribe(ctx, channels...)
		if err := p.ps.Ping(ctx); err != nil {
			slog.Error("PubSub reconnect failed", "attempt", attempt, "err", err)
			continue
		}
		slog.Info("PubSub reconnected and verified")
		return true
	}
	return false
}

func (p *PubSub) dispatch(payload string) {
	var msg types.ClusterMessage
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		slog.Error("PubSub dispatch parse error", "err", err)
		return
	}

	p.mu.RLock()
	hs := make([]MessageHandler, len(p.handlers[msg.W]))
	copy(hs, p.handlers[msg.W])
	p.mu.RUnlock()

	for _, h := range hs {
		h(&msg)
	}

	if p.events != nil {
		p.events.Emit(msg.W, &msg)
	}
}

func (p *PubSub) newMsg(msgType string, data map[string]interface{}) *types.ClusterMessage {
	return &types.ClusterMessage{
		T: float64(time.Now().UnixNano()) / 1e9,
		M: data,
		W: msgType,
	}
}

func (p *PubSub) publish(ctx context.Context, channel string, msg *types.ClusterMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return p.redis.Publish(ctx, channel, data).Err()
}

func (p *PubSub) Broadcast(ctx context.Context, msgType string, data map[string]interface{}) error {
	return p.publish(ctx, "cluster:broadcast", p.newMsg(msgType, data))
}

func (p *PubSub) SendToInstance(ctx context.Context, instanceID, msgType string, data map[string]interface{}) error {
	return p.publish(ctx, "cluster:instance:"+instanceID, p.newMsg(msgType, data))
}

func (p *PubSub) BroadcastGlobalEvent(ctx context.Context, event string, data map[string]interface{}) error {
	return p.publish(ctx, "cluster:global", p.newMsg("global_event", map[string]interface{}{
		"event": event,
		"data":  data,
	}))
}
