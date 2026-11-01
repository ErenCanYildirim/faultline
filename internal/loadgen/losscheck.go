package loadgen

// LossCheck is what was sent against what was stored, for one run.
type LossCheck struct {
	Generated    uint64 `json:"generated"`
	Acknowledged uint64 `json:"acknowledged"`
	DroppedBySDK uint64 `json:"dropped_by_sdk"`
	FailedInSDK  uint64 `json:"failed_in_sdk"`

	Stored        uint64 `json:"stored"` // distinct sequence numbers
	Rows          uint64 `json:"rows"`
	DuplicateRows uint64 `json:"duplicate_rows"` // before ClickHouse merges them

	Missing int64 `json:"missing"`
	// Explained: missing events the SDK knew it didn't deliver. Capped at
	// Missing; the SDK can give up on a batch that ingest had in fact
	// written (a timeout after the write), which StoredDespiteFailed counts.
	Explained           int64    `json:"explained"`
	StoredDespiteFailed int64    `json:"stored_despite_failed"`
	Unexplained         int64    `json:"unexplained"`
	MissingSample       []uint64 `json:"missing_sample,omitempty"`
}

// Check compares a run's report with what storage holds for it.
func Check(rep Report, st StoreResult) LossCheck {
	c := LossCheck{
		Generated:     rep.Generation.Generated,
		Acknowledged:  rep.SDK.Sent,
		DroppedBySDK:  rep.SDK.Dropped,
		FailedInSDK:   rep.SDK.Failed,
		Stored:        st.Seqs,
		Rows:          st.Rows,
		DuplicateRows: st.Duplicates,
		MissingSample: st.MissingSeqs,
	}
	c.Missing = int64(c.Generated) - int64(c.Stored)
	sdk := int64(c.DroppedBySDK + c.FailedInSDK)
	c.Explained = min(c.Missing, sdk)
	c.StoredDespiteFailed = max(sdk-c.Missing, 0)
	c.Unexplained = max(c.Missing-sdk, 0)
	return c
}