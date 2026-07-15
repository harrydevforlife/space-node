package storage

import (
	"sort"
	"sync"

	"github.com/space-node/space-node/internal/types"
)

// MemoryViewStore is a small thread-safe store for materialized-view rows.
type MemoryViewStore struct {
	mu   sync.RWMutex
	rows map[string]types.Row
}

// NewMemoryViewStore creates an empty in-memory materialized view store.
func NewMemoryViewStore() *MemoryViewStore {
	return &MemoryViewStore{rows: map[string]types.Row{}}
}

// Put stores a clone of row under key.
func (s *MemoryViewStore) Put(key string, row types.Row) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[key] = row.Clone()
}

// Get returns a clone of the row for key.
func (s *MemoryViewStore) Get(key string) (types.Row, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	row, ok := s.rows[key]
	if !ok {
		return nil, false
	}
	return row.Clone(), true
}

// All returns all rows sorted by key for deterministic tests and output.
func (s *MemoryViewStore) All() []types.Row {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.rows))
	for key := range s.rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]types.Row, 0, len(keys))
	for _, key := range keys {
		out = append(out, s.rows[key].Clone())
	}
	return out
}
