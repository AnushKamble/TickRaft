package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	RecordHeaderSize = 10 // 4B Length + 2B Type + 4B CRC32
	MaxRecordPayload = 16 * 1024 * 1024 // 16 MB bounded maximum
	RecordTypeEntry  = uint16(1)
	RecordTypeTrunc  = uint16(2)
)

var (
	ErrOversizedRecord  = errors.New("wal record exceeds maximum allowed payload size")
	ErrChecksumMismatch = errors.New("wal record checksum mismatch")
	ErrInvalidHeader    = errors.New("wal invalid record header")
)

type Record struct {
	Type    uint16
	Payload []byte
}

func EncodeRecord(recType uint16, payload []byte) ([]byte, error) {
	payloadLen := len(payload)
	if payloadLen > MaxRecordPayload {
		return nil, ErrOversizedRecord
	}

	buf := make([]byte, RecordHeaderSize+payloadLen)

	// 1. Length (4B)
	binary.BigEndian.PutUint32(buf[0:4], uint32(payloadLen))
	// 2. Type (2B)
	binary.BigEndian.PutUint16(buf[4:6], recType)

	// Copy payload
	copy(buf[RecordHeaderSize:], payload)

	// 3. CRC32 computed over Type + Payload
	h := crc32.NewIEEE()
	h.Write(buf[4:6])       // Type
	h.Write(payload)        // Payload
	checksum := h.Sum32()

	binary.BigEndian.PutUint32(buf[6:10], checksum)

	return buf, nil
}

func ValidateAndDecodeHeader(hdrBytes []byte) (payloadLen uint32, recType uint16, expectedCRC uint32, err error) {
	if len(hdrBytes) < RecordHeaderSize {
		return 0, 0, 0, ErrInvalidHeader
	}
	payloadLen = binary.BigEndian.Uint32(hdrBytes[0:4])
	if payloadLen > MaxRecordPayload {
		return 0, 0, 0, fmt.Errorf("%w: length=%d", ErrOversizedRecord, payloadLen)
	}
	recType = binary.BigEndian.Uint16(hdrBytes[4:6])
	expectedCRC = binary.BigEndian.Uint32(hdrBytes[6:10])
	return payloadLen, recType, expectedCRC, nil
}

func VerifyChecksum(recType uint16, payload []byte, expectedCRC uint32) bool {
	h := crc32.NewIEEE()
	var typeBytes [2]byte
	binary.BigEndian.PutUint16(typeBytes[:], recType)
	h.Write(typeBytes[:])
	h.Write(payload)
	return h.Sum32() == expectedCRC
}
