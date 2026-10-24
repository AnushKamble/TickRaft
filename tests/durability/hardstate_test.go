package durability_test

import (
	"os"
	"path/filepath"
	"testing"
	"tickraft/internal/storage/hardstate"
)

func TestHardStatePersistence(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "hardstate_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	mgr, err := hardstate.NewHardStateManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create hardstate manager: %v", err)
	}

	// Initial load should be 0, ""
	term, votedFor, err := mgr.Load()
	if err != nil {
		t.Fatalf("unexpected load error: %v", err)
	}
	if term != 0 || votedFor != "" {
		t.Fatalf("expected term 0, empty vote; got term=%d, votedFor=%s", term, votedFor)
	}

	// Save term 5 and votedFor "node-2"
	if err := mgr.Save(5, "node-2"); err != nil {
		t.Fatalf("failed to save hard state: %v", err)
	}

	// Simulate restart by creating new manager on same directory
	mgr2, err := hardstate.NewHardStateManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create second manager: %v", err)
	}

	term2, votedFor2, err := mgr2.Load()
	if err != nil {
		t.Fatalf("failed to reload hard state: %v", err)
	}
	if term2 != 5 || votedFor2 != "node-2" {
		t.Fatalf("expected term 5, votedFor node-2; got term=%d, votedFor=%s", term2, votedFor2)
	}
}

func TestVoteDurability(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "hardstate_durability_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	mgr, _ := hardstate.NewHardStateManager(tempDir)
	_ = mgr.Save(10, "node-3")

	// Verify file exists on disk
	filePath := filepath.Join(tempDir, "hardstate.json")
	info, err := os.Stat(filePath)
	if err != nil || info.Size() == 0 {
		t.Fatalf("hardstate file should exist with non-zero size on disk")
	}
}
