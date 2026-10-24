package marketlog

type DatasetManifest struct {
	DatasetID          string `json:"dataset_id"`
	Symbol             string `json:"symbol"`
	SessionDate        string `json:"session_date"`
	ExpectedEvents     uint64 `json:"expected_events"`
	AppliedEvents      uint64 `json:"applied_events"`
	SeqStart           uint64 `json:"seq_start"`
	SeqEnd             uint64 `json:"seq_end"`
	SourceManifestHash string `json:"source_manifest_hash"`
	RollingDigest      string `json:"rolling_digest"`
	IsFinalized        bool   `json:"is_finalized"`
}
