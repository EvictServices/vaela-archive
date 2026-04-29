package events

import "sync"

type Handler func(args ...interface{})

type EventEmitter struct {
	mu       sync.RWMutex
	handlers map[string][]Handler
}

func New() *EventEmitter {
	return &EventEmitter{handlers: make(map[string][]Handler)}
}

func (e *EventEmitter) On(event string, fn Handler) {
	e.mu.Lock()
	e.handlers[event] = append(e.handlers[event], fn)
	e.mu.Unlock()
}

func (e *EventEmitter) Emit(event string, args ...interface{}) {
	e.mu.RLock()
	hs := make([]Handler, len(e.handlers[event]))
	copy(hs, e.handlers[event])
	e.mu.RUnlock()
	for _, h := range hs {
		h(args...)
	}
}

func (e *EventEmitter) Events() map[string]int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make(map[string]int, len(e.handlers))
	for k, v := range e.handlers {
		out[k] = len(v)
	}
	return out
}
