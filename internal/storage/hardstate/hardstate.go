package hardstate

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type HardStateData struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

type HardStateManager struct {
	mu       sync.RWMutex
	filePath string
}

func NewHardStateManager(dirPath string) (*HardStateManager, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create hardstate dir: %w", err)
	}
	return &HardStateManager{
		filePath: filepath.Join(dirPath, "hardstate.json"),
	}, nil
}

func (m *HardStateManager) Save(term uint64, votedFor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data := HardStateData{
		CurrentTerm: term,
		VotedFor:    votedFor,
	}

	bytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal hard state: %w", err)
	}

	// Write atomically using temporary file with fsync
	tmpPath := fmt.Sprintf("%s.tmp.%d", m.filePath, os.Getpid())
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open temp hardstate file: %w", err)
	}

	if _, err := f.Write(bytes); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to write temp hardstate: %w", err)
	}

	// Flush to physical disk
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to fsync temp hardstate: %w", err)
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to close temp hardstate: %w", err)
	}

	// Atomic rename/replace (handles Windows replacement)
	if err := os.Rename(tmpPath, m.filePath); err != nil {
		_ = os.Remove(m.filePath)
		if err2 := os.Rename(tmpPath, m.filePath); err2 != nil {
			_ = os.Remove(tmpPath)
			return fmt.Errorf("failed to atomic replace hardstate file: %w", err2)
		}
	}

	return nil
}

func (m *HardStateManager) Load() (uint64, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if _, err := os.Stat(m.filePath); os.IsNotExist(err) {
		return 0, "", nil // Default initial hard-state
	}

	bytes, err := os.ReadFile(m.filePath)
	if err != nil {
		return 0, "", fmt.Errorf("failed to read hardstate file: %w", err)
	}

	var data HardStateData
	if err := json.Unmarshal(bytes, &data); err != nil {
		return 0, "", fmt.Errorf("failed to unmarshal hardstate: %w", err)
	}

	return data.CurrentTerm, data.VotedFor, nil
}
