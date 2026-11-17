package main

import (
	"testing"
	"time"
)

func withSqueueOutput(t *testing.T, out string) {
	t.Helper()
	old := slurmCache
	slurmCache = &SlurmCache{entries: map[string]*CacheEntry{
		"squeue_users_basic": {Output: []byte(out)},
	}}
	t.Cleanup(func() { slurmCache = old })
}

// Regression test: when nothing is running (drain, maintenance), the cache must
// drop its entries instead of reporting the last GPU allocations forever.
func TestRefreshEvictsEverythingWhenNothingIsRunning(t *testing.T) {
	withSqueueOutput(t, "") // squeue -h with no jobs prints nothing
	c := NewJobStaticCache(time.Hour)
	c.entries[38150822] = &JobStaticInfo{JobID: 38150822, User: "alice",
		GPUs: []JobGPUAssignment{{User: "alice", Hostname: "gpu-d-1", GPU: "1"}}}
	c.entries[38150900] = &JobStaticInfo{JobID: 38150900, User: "bob"}
	c.lastDiff = time.Now().Add(-2 * time.Hour) // diff interval elapsed

	c.Refresh()

	if n := len(c.Snapshot()); n != 0 {
		t.Fatalf("expected the cache to be emptied, still has %d jobs", n)
	}
}

// Jobs that are still running are kept; ended ones are evicted (no scontrol
// call is needed when there are no new jobs).
func TestRefreshKeepsRunningAndEvictsEnded(t *testing.T) {
	withSqueueOutput(t, "38150822|alice|RUNNING|8\n38150999|carol|PENDING|4\n")
	c := NewJobStaticCache(time.Hour)
	c.entries[38150822] = &JobStaticInfo{JobID: 38150822, User: "alice"}
	c.entries[38150900] = &JobStaticInfo{JobID: 38150900, User: "bob"} // ended
	c.lastDiff = time.Now().Add(-2 * time.Hour)

	c.Refresh()

	snap := c.Snapshot()
	if len(snap) != 1 || snap[0].JobID != 38150822 {
		t.Fatalf("expected only job 38150822 to remain, got %+v", snap)
	}
}
