package session

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

type Manager struct {
	path      string
	clusterID string
}

func New(path string) *Manager {
	return &Manager{path: path}
}

func tokenURLSafe(nbytes int) string {
	b := make([]byte, nbytes)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func (s *Manager) GetOrCreate(_ context.Context) (string, error) {
	if s.clusterID != "" {
		return s.clusterID, nil
	}

	if err := os.MkdirAll(s.path, 0o755); err != nil {
		return "", err
	}

	idFile := filepath.Join(s.path, "_c.id")
	data, err := os.ReadFile(idFile)
	if err == nil {
		id := strings.TrimSpace(string(data))
		if id != "" {
			s.clusterID = id
			return id, nil
		}
	}

	id := tokenURLSafe(8)
	if err := os.WriteFile(idFile, []byte(id), 0o644); err != nil {
		return "", err
	}
	s.clusterID = id
	return id, nil
}

func (s *Manager) SaveState(_ context.Context, state map[string]interface{}) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.path, "_s.json"), data, 0o644)
}

func (s *Manager) LoadState(_ context.Context) (map[string]interface{}, error) {
	data, err := os.ReadFile(filepath.Join(s.path, "_s.json"))
	if err != nil {
		return nil, nil
	}
	var state map[string]interface{}
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, nil
	}
	return state, nil
}

func (s *Manager) Cleanup(_ context.Context) error {
	return os.RemoveAll(s.path)
}
