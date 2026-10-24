# TickRaft Data Source Policy & Licensing Terms

## 1. LOBSTER Data Terms & Access Plan

LOBSTER (Limit Order Book System - The Efficient Reconstructor) provides high-frequency reconstruction of NASDAQ limit order books.
Reference URLs:
- Samples: https://php.lobsterdata.com/info/DataSamples.php
- Data Structure: https://data.lobsterdata.com/info/DataStructure.php
- Access Options: https://data.lobsterdata.com/info/AccessOptions.php

### Legal & Redistribution Policy:
1. LOBSTER data is proprietary and governed by academic and commercial subscription licenses.
2. Under no circumstances should licensed raw market data, user credentials, or subscription keys be committed to this repository.
3. This repository includes ONLY synthetic, author-created fixtures (`data/fixtures/`) specifically constructed for unit testing, protocol validation, order-book invariant checks, and deterministic reproducible demonstrations.

## 2. Synthetic Fixture Contract

The repository ships with self-contained synthetic fixtures:
- `data/fixtures/synthetic_message.csv`: Synthesized 6-column LOBSTER message format with pre-computed events (submissions, cancellations, deletions, executions).
- `data/fixtures/synthetic_orderbook.csv`: Synthesized 20-column LOBSTER book format ($L=5$ levels) aligned row-for-row with the message file.
- `data/fixtures/expected_features.json`: Hand-calculated expected outputs for Level-1 OFI, TVI, depth-aware vectors, and mid-price targets.

## 3. Real Data Ingestion Procedure

When a researcher possesses authorized LOBSTER files:
1. Place `[SYMBOL]_[DATE]_[START]_[END]_message_[LEVELS].csv` and `[SYMBOL]_[DATE]_[START]_[END]_orderbook_[LEVELS].csv` in a secure local directory outside git tracking (or configure `configs/research.example.yaml`).
2. Run `tickraft-import` pointing to the file path. The importer computes source SHA-256 hashes, normalizes prices and timestamps, validates row counts and book integrity, and submits batches to the Raft cluster.
3. The committed export produces a verified manifest and Parquet file.
4. If authorized data is not present on the host, the empirical pipeline will mark results `NOT RUN — real data unavailable` rather than fabricating statistical conclusions.
