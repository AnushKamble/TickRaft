package raft

import (
	"sync"
	raftpb "tickraft/proto/raft"
)

type MemoryStorage struct {
	mu       sync.RWMutex
	term     uint64
	votedFor string
	entries  []*raftpb.LogEntry
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		entries: make([]*raftpb.LogEntry, 0),
	}
}

func (m *MemoryStorage) SaveHardState(term uint64, votedFor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.term = term
	m.votedFor = votedFor
	return nil
}

func (m *MemoryStorage) LoadHardState() (uint64, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.term, m.votedFor, nil
}

func (m *MemoryStorage) AppendEntries(entries []*raftpb.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range entries {
		// Replace if exists, or append
		found := false
		for i, existing := range m.entries {
			if existing.Index == e.Index {
				m.entries[i] = e
				found = true
				break
			}
		}
		if !found {
			m.entries = append(m.entries, e)
		}
	}
	return nil
}

func (m *MemoryStorage) TruncateSuffix(fromIndex uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cut := -1
	for i, e := range m.entries {
		if e.Index >= fromIndex {
			cut = i
			break
		}
	}
	if cut != -1 {
		m.entries = m.entries[:cut]
	}
	return nil
}

func (m *MemoryStorage) LoadEntries() ([]*raftpb.LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	copied := make([]*raftpb.LogEntry, len(m.entries))
	copy(copied, m.entries)
	return copied, nil
}

func (m *MemoryStorage) Close() error {
	return nil
}
