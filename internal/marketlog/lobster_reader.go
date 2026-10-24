package marketlog

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	marketlogpb "tickraft/proto/marketlog"
)

func ParseLOBSTERFiles(msgPath, bookPath string, levels int) ([]*marketlogpb.MarketEvent, string, string, error) {
	msgFile, err := os.Open(msgPath)
	if err != nil {
		return nil, "", "", fmt.Errorf("failed to open message file: %w", err)
	}
	defer msgFile.Close()

	bookFile, err := os.Open(bookPath)
	if err != nil {
		return nil, "", "", fmt.Errorf("failed to open book file: %w", err)
	}
	defer bookFile.Close()

	// Compute hashes
	hMsg := sha256.New()
	msgReader := io.TeeReader(msgFile, hMsg)

	hBook := sha256.New()
	bookReader := io.TeeReader(bookFile, hBook)

	msgScanner := bufio.NewScanner(msgReader)
	bookScanner := bufio.NewScanner(bookReader)

	var events []*marketlogpb.MarketEvent
	var seq uint64 = 1

	for msgScanner.Scan() {
		msgLine := strings.TrimSpace(msgScanner.Text())
		if msgLine == "" {
			continue
		}

		if !bookScanner.Scan() {
			return nil, "", "", fmt.Errorf("book file has fewer rows than message file")
		}
		bookLine := strings.TrimSpace(bookScanner.Text())

		msgParts := strings.Split(msgLine, ",")
		if len(msgParts) != 6 {
			return nil, "", "", fmt.Errorf("invalid message columns: expected 6, got %d", len(msgParts))
		}

		// Exact nanoseconds from seconds string
		timeSecStr := msgParts[0]
		tsNs, err := ParseDecimalSecondsToNano(timeSecStr)
		if err != nil {
			return nil, "", "", fmt.Errorf("invalid time '%s': %w", timeSecStr, err)
		}

		eventType, _ := strconv.ParseUint(msgParts[1], 10, 32)
		orderID, _ := strconv.ParseUint(msgParts[2], 10, 64)
		size, _ := strconv.ParseUint(msgParts[3], 10, 64)
		price, _ := strconv.ParseInt(msgParts[4], 10, 64)
		direction, _ := strconv.ParseInt(msgParts[5], 10, 32)

		bookParts := strings.Split(bookLine, ",")
		expectedBookCols := 4 * levels
		if len(bookParts) != expectedBookCols {
			return nil, "", "", fmt.Errorf("invalid book columns: expected %d, got %d", expectedBookCols, len(bookParts))
		}

		asks := make([]*marketlogpb.OrderBookLevel, levels)
		bids := make([]*marketlogpb.OrderBookLevel, levels)

		for i := 0; i < levels; i++ {
			base := i * 4
			aPrice, _ := strconv.ParseInt(bookParts[base], 10, 64)
			aSize, _ := strconv.ParseUint(bookParts[base+1], 10, 64)
			bPrice, _ := strconv.ParseInt(bookParts[base+2], 10, 64)
			bSize, _ := strconv.ParseUint(bookParts[base+3], 10, 64)

			asks[i] = &marketlogpb.OrderBookLevel{Price: aPrice, Size: aSize}
			bids[i] = &marketlogpb.OrderBookLevel{Price: bPrice, Size: bSize}
		}

		event := &marketlogpb.MarketEvent{
			SourceSeq:   seq,
			TimestampNs: tsNs,
			EventType:   uint32(eventType),
			OrderId:     orderID,
			Size:        size,
			Price:       price,
			Direction:   int32(direction),
			Book: &marketlogpb.OrderBookSnapshot{
				Asks: asks,
				Bids: bids,
			},
		}

		events = append(events, event)
		seq++
	}

	if bookScanner.Scan() {
		return nil, "", "", fmt.Errorf("message file has fewer rows than book file")
	}

	msgHash := hex.EncodeToString(hMsg.Sum(nil))
	bookHash := hex.EncodeToString(hBook.Sum(nil))

	return events, msgHash, bookHash, nil
}

func ParseDecimalSecondsToNano(s string) (int64, error) {
	parts := strings.Split(s, ".")
	sec, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, err
	}

	var nano int64 = 0
	if len(parts) > 1 {
		frac := parts[1]
		if len(frac) > 9 {
			frac = frac[:9]
		} else {
			frac = frac + strings.Repeat("0", 9-len(frac))
		}
		nano, err = strconv.ParseInt(frac, 10, 64)
		if err != nil {
			return 0, err
		}
	}

	return sec*1_000_000_000 + nano, nil
}
