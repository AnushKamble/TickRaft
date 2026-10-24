package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"google.golang.org/protobuf/proto"
	raftpb "tickraft/proto/raft"
)

var (
	ErrMidLogCorruption = errors.New("unrecoverable mid-log corruption detected in WAL")
)

type RecoveryResult struct {
	Entries       []*raftpb.LogEntry
	TruncatedTail bool
	CleanOffset   int64
}

// Recover reads and validates a WAL file from start to finish.
// It reconstructs the sequence of LogEntries, applying uncommitted truncations.
// If an incomplete tail record exists at EOF (due to a power failure or crash during write),
// it repairs the tail by truncating the file to the last valid clean record boundary.
// If corruption occurs in the middle of valid records, it fails loudly with ErrMidLogCorruption.
func Recover(walPath string) (*RecoveryResult, error) {
	fileInfo, err := os.Stat(walPath)
	if os.IsNotExist(err) {
		return &RecoveryResult{Entries: nil, CleanOffset: 0}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to stat wal file: %w", err)
	}

	totalSize := fileInfo.Size()
	if totalSize == 0 {
		return &RecoveryResult{Entries: nil, CleanOffset: 0}, nil
	}

	f, err := os.OpenFile(walPath, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file for recovery: %w", err)
	}
	defer f.Close()

	var entries []*raftpb.LogEntry
	var offset int64 = 0
	truncatedTail := false

	hdrBuf := make([]byte, RecordHeaderSize)

	for offset < totalSize {
		bytesLeft := totalSize - offset
		if bytesLeft < RecordHeaderSize {
			// Partial header at tail -> crash during header write!
			truncatedTail = true
			break
		}

		n, err := io.ReadFull(f, hdrBuf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				truncatedTail = true
				break
			}
			return nil, fmt.Errorf("read error at offset %d: %w", offset, err)
		}

		payloadLen, recType, expectedCRC, err := ValidateAndDecodeHeader(hdrBuf[:n])
		if err != nil {
			// Malformed header
			if offset+int64(RecordHeaderSize) >= totalSize {
				// At tail
				truncatedTail = true
				break
			}
			return nil, fmt.Errorf("%w: invalid header at offset %d", ErrMidLogCorruption, offset)
		}

		recordTotalLen := int64(RecordHeaderSize) + int64(payloadLen)
		if offset+recordTotalLen > totalSize {
			// Partial payload at tail -> crash during payload write!
			truncatedTail = true
			break
		}

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(f, payload); err != nil {
			truncatedTail = true
			break
		}

		if !VerifyChecksum(recType, payload, expectedCRC) {
			// Checksum mismatch
			if offset+recordTotalLen >= totalSize {
				// Corrupted final record at tail
				truncatedTail = true
				break
			}
			// Middle of log corrupted!
			return nil, fmt.Errorf("%w: checksum mismatch at offset %d", ErrMidLogCorruption, offset)
		}

		// Valid record
		switch recType {
		case RecordTypeEntry:
			entry := &raftpb.LogEntry{}
			if err := proto.Unmarshal(payload, entry); err != nil {
				if offset+recordTotalLen >= totalSize {
					truncatedTail = true
					break
				}
				return nil, fmt.Errorf("%w: failed to unmarshal log entry at offset %d: %v", ErrMidLogCorruption, offset, err)
			}
			// Handle replacement or append
			found := false
			for idx, existing := range entries {
				if existing.Index == entry.Index {
					entries[idx] = entry
					found = true
					break
				}
			}
			if !found {
				entries = append(entries, entry)
			}

		case RecordTypeTrunc:
			if len(payload) >= 8 {
				fromIndex := binary.BigEndian.Uint64(payload[0:8])
				cut := -1
				for i, e := range entries {
					if e.Index >= fromIndex {
						cut = i
						break
					}
				}
				if cut != -1 {
					entries = entries[:cut]
				}
			}
		}

		offset += recordTotalLen
	}

	if truncatedTail {
		// Truncate file to last known clean record offset
		if err := f.Truncate(offset); err != nil {
			return nil, fmt.Errorf("failed to truncate corrupt tail at offset %d: %w", offset, err)
		}
		if err := f.Sync(); err != nil {
			return nil, fmt.Errorf("failed to fsync truncated wal: %w", err)
		}
	}

	return &RecoveryResult{
		Entries:       entries,
		TruncatedTail: truncatedTail,
		CleanOffset:   offset,
	}, nil
}
