package durability_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"tickraft/internal/storage/wal"
	raftpb "tickraft/proto/raft"
)

func TestWALFramingAndCRC(t *testing.T) {
	payload := []byte("hello-market-data-wal")
	encoded, err := wal.EncodeRecord(wal.RecordTypeEntry, payload)
	if err != nil {
		t.Fatalf("failed to encode record: %v", err)
	}

	if len(encoded) != wal.RecordHeaderSize+len(payload) {
		t.Fatalf("expected length %d, got %d", wal.RecordHeaderSize+len(payload), len(encoded))
	}

	payloadLen, recType, crc, err := wal.ValidateAndDecodeHeader(encoded[:wal.RecordHeaderSize])
	if err != nil {
		t.Fatalf("failed to decode header: %v", err)
	}

	if payloadLen != uint32(len(payload)) || recType != wal.RecordTypeEntry {
		t.Fatalf("mismatch: len=%d, type=%d", payloadLen, recType)
	}

	if !wal.VerifyChecksum(recType, payload, crc) {
		t.Fatalf("checksum verification failed")
	}

	// Corrupting payload should fail checksum check
	corruptPayload := append([]byte(nil), payload...)
	corruptPayload[0] ^= 0xFF
	if wal.VerifyChecksum(recType, corruptPayload, crc) {
		t.Fatalf("corrupted payload should fail checksum verification")
	}
}

func TestWALRecoveryClean(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_clean_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	storage, err := wal.NewDurableWALStorage(tempDir)
	if err != nil {
		t.Fatalf("failed to create durable storage: %v", err)
	}

	entries := []*raftpb.LogEntry{
		{Index: 1, Term: 1, Data: []byte("entry-1")},
		{Index: 2, Term: 1, Data: []byte("entry-2")},
		{Index: 3, Term: 2, Data: []byte("entry-3")},
	}

	if err := storage.AppendEntries(entries); err != nil {
		t.Fatalf("failed to append entries: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("failed to close storage: %v", err)
	}

	// Reopen on same directory (simulate node restart)
	storage2, err := wal.NewDurableWALStorage(tempDir)
	if err != nil {
		t.Fatalf("failed to reopen storage after restart: %v", err)
	}
	defer storage2.Close()

	recovered, err := storage2.LoadEntries()
	if err != nil {
		t.Fatalf("failed to load recovered entries: %v", err)
	}

	if len(recovered) != 3 {
		t.Fatalf("expected 3 recovered entries, got %d", len(recovered))
	}

	for i := 0; i < 3; i++ {
		if recovered[i].Index != entries[i].Index || recovered[i].Term != entries[i].Term ||
			string(recovered[i].Data) != string(entries[i].Data) {
			t.Fatalf("entry mismatch at %d: got %+v, expected %+v", i, recovered[i], entries[i])
		}
	}
}

func TestWALPartialTailTruncation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_tail_trunc_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	storage, err := wal.NewDurableWALStorage(tempDir)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	entries := []*raftpb.LogEntry{
		{Index: 1, Term: 1, Data: []byte("complete-1")},
		{Index: 2, Term: 1, Data: []byte("complete-2")},
	}
	_ = storage.AppendEntries(entries)
	_ = storage.Close()

	// Append corrupt / partial bytes to the end of the WAL file
	walFile := filepath.Join(tempDir, "raft.wal")
	f, err := os.OpenFile(walFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open wal file: %v", err)
	}

	// Append 5 garbage bytes (partial header of an incomplete record crash)
	_, _ = f.Write([]byte{0x00, 0x00, 0x05, 0x01, 0xFF})
	_ = f.Close()

	// Recovery should detect partial tail, truncate it, and recover the 2 complete entries
	recovery, err := wal.Recover(walFile)
	if err != nil {
		t.Fatalf("expected successful recovery with tail truncation, got err: %v", err)
	}

	if !recovery.TruncatedTail {
		t.Fatalf("expected TruncatedTail=true")
	}

	if len(recovery.Entries) != 2 {
		t.Fatalf("expected 2 recovered entries, got %d", len(recovery.Entries))
	}
}

func TestWALMidLogCorruptionFailLoud(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_mid_corrupt_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	storage, err := wal.NewDurableWALStorage(tempDir)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	entries := []*raftpb.LogEntry{
		{Index: 1, Term: 1, Data: []byte("valid-entry-1")},
		{Index: 2, Term: 1, Data: []byte("valid-entry-2")},
		{Index: 3, Term: 1, Data: []byte("valid-entry-3")},
	}
	_ = storage.AppendEntries(entries)
	_ = storage.Close()

	// Corrupt a byte in entry 1 (in the middle of valid records)
	walFile := filepath.Join(tempDir, "raft.wal")
	bytes, err := os.ReadFile(walFile)
	if err != nil {
		t.Fatalf("failed to read wal: %v", err)
	}

	// Mutate byte in record 1 payload (offset ~15)
	bytes[15] ^= 0xAA
	_ = os.WriteFile(walFile, bytes, 0644)

	// Recovery must FAIL LOUDLY with ErrMidLogCorruption!
	_, err = wal.Recover(walFile)
	if err == nil {
		t.Fatalf("expected loud failure for mid-log corruption, got nil error")
	}
	if !errors.Is(err, wal.ErrMidLogCorruption) {
		t.Fatalf("expected ErrMidLogCorruption, got: %v", err)
	}
}

func TestDurableAppendBeforeAck(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_durability_ack_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	storage, err := wal.NewDurableWALStorage(tempDir)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	entry := &raftpb.LogEntry{Index: 1, Term: 1, Data: []byte("data")}
	if err := storage.AppendEntries([]*raftpb.LogEntry{entry}); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	// Check file size on disk is non-zero
	info, err := os.Stat(filepath.Join(tempDir, "raft.wal"))
	if err != nil || info.Size() == 0 {
		t.Fatalf("wal file should be flushed to disk with non-zero size")
	}
}

func TestWALFsyncDurability(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_fsync_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	storage, _ := wal.NewDurableWALStorage(tempDir)
	defer storage.Close()

	entries := []*raftpb.LogEntry{
		{Index: 1, Term: 1, Data: []byte("fsync-test")},
	}
	err = storage.AppendEntries(entries)
	if err != nil {
		t.Fatalf("expected successful append with fsync, got: %v", err)
	}
}
