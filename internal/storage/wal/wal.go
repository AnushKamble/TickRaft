package wal

import (
	"encoding/binary"
	"fmt"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"sync"
	"tickraft/internal/storage/hardstate"
	raftpb "tickraft/proto/raft"
)

type DurableWALStorage struct {
	mu          sync.RWMutex
	dirPath     string
	walPath     string
	file        *os.File
	hardState   *hardstate.HardStateManager
	cachedLog   []*raftpb.LogEntry
	writeOffset int64
}

func NewDurableWALStorage(dirPath string) (*DurableWALStorage, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage dir: %w", err)
	}

	hs, err := hardstate.NewHardStateManager(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to init hardstate manager: %w", err)
	}

	walPath := filepath.Join(dirPath, "raft.wal")

	// Perform recovery pass on open
	recovery, err := Recover(walPath)
	if err != nil {
		return nil, fmt.Errorf("wal recovery failed: %w", err)
	}

	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file: %w", err)
	}

	return &DurableWALStorage{
		dirPath:     dirPath,
		walPath:     walPath,
		file:        f,
		hardState:   hs,
		cachedLog:   recovery.Entries,
		writeOffset: recovery.CleanOffset,
	}, nil
}

func (s *DurableWALStorage) SaveHardState(term uint64, votedFor string) error {
	return s.hardState.Save(term, votedFor)
}

func (s *DurableWALStorage) LoadHardState() (uint64, string, error) {
	return s.hardState.Load()
}

func (s *DurableWALStorage) AppendEntries(entries []*raftpb.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entry := range entries {
		data, err := proto.Marshal(entry)
		if err != nil {
			return fmt.Errorf("failed to marshal entry for wal: %w", err)
		}

		encoded, err := EncodeRecord(RecordTypeEntry, data)
		if err != nil {
			return fmt.Errorf("failed to encode wal record: %w", err)
		}

		n, err := s.file.Write(encoded)
		if err != nil {
			return fmt.Errorf("failed to write wal record: %w", err)
		}
		s.writeOffset += int64(n)

		// Update in-memory cache
		found := false
		for idx, existing := range s.cachedLog {
			if existing.Index == entry.Index {
				s.cachedLog[idx] = entry
				found = true
				break
			}
		}
		if !found {
			s.cachedLog = append(s.cachedLog, entry)
		}
	}

	// Raft requirement: durable before acknowledging replication!
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("failed to fsync wal entries: %w", err)
	}

	return nil
}

func (s *DurableWALStorage) TruncateSuffix(fromIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Encode truncation marker
	var payload [8]byte
	binary.BigEndian.PutUint64(payload[:], fromIndex)

	encoded, err := EncodeRecord(RecordTypeTrunc, payload[:])
	if err != nil {
		return fmt.Errorf("failed to encode truncation record: %w", err)
	}

	n, err := s.file.Write(encoded)
	if err != nil {
		return fmt.Errorf("failed to write truncation marker to wal: %w", err)
	}
	s.writeOffset += int64(n)

	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("failed to fsync truncation marker: %w", err)
	}

	// Truncate cached log
	cut := -1
	for i, e := range s.cachedLog {
		if e.Index >= fromIndex {
			cut = i
			break
		}
	}
	if cut != -1 {
		s.cachedLog = s.cachedLog[:cut]
	}

	return nil
}

func (s *DurableWALStorage) LoadEntries() ([]*raftpb.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	copied := make([]*raftpb.LogEntry, len(s.cachedLog))
	copy(copied, s.cachedLog)
	return copied, nil
}

func (s *DurableWALStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		_ = s.file.Sync()
		err := s.file.Close()
		s.file = nil
		return err
	}
	return nil
}
