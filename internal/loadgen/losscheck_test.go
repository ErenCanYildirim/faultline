package loadgen

import "testing"

func TestCheck(t *testing.T) {
	rep := Report{Generation: Generation{Generated: 1000}, SDK: SDKTotals{Sent: 950, Dropped: 30, Failed: 20}}

	// 50 events the SDK never delivered are missing, plus 3 lost after
	// ingest acknowledged them. 7 rows are duplicates.
	c := Check(rep, StoreResult{Seqs: 947, Rows: 954, Duplicates: 7, MissingSeqs: []uint64{4, 9}})
	if c.Missing != 53 || c.Explained != 50 || c.Unexplained != 3 || c.DuplicateRows != 7 || c.StoredDespiteFailed != 0 {
		t.Errorf("check = %+v", c)
	}

	// The SDK gave up on 20 that ingest had in fact stored: no loss.
	c = Check(rep, StoreResult{Seqs: 970, Rows: 970})
	if c.Missing != 30 || c.Unexplained != 0 || c.StoredDespiteFailed != 20 {
		t.Errorf("check = %+v", c)
	}

	// Everything arrived.
	c = Check(Report{Generation: Generation{Generated: 10}, SDK: SDKTotals{Sent: 10}}, StoreResult{Seqs: 10, Rows: 12, Duplicates: 2})
	if c.Missing != 0 || c.Unexplained != 0 || c.Explained != 0 {
		t.Errorf("check = %+v", c)
	}
}
