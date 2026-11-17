package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func withStateDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	oldH, oldC := userHistoryFile, userCountsFile
	userHistoryFile = filepath.Join(dir, "slurm_exporter_job_history.json")
	userCountsFile = filepath.Join(dir, "slurm_exporter_job_counts.json")
	t.Cleanup(func() { userHistoryFile, userCountsFile = oldH, oldC })
	return dir
}

func TestWriteFileAtomicReplacesAndLeavesNoTemp(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "state.json")
	if err := os.WriteFile(p, []byte(`{"old":1}`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(p, []byte(`{"new":2}`), 0644); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(p)
	if string(got) != `{"new":2}` {
		t.Errorf("content = %s", got)
	}
	if m, _ := filepath.Glob(p + ".tmp-*"); len(m) != 0 {
		t.Errorf("temp files left behind: %v", m)
	}
}

func TestLoadStateFileMovesCorruptFileAside(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "slurm_exporter_job_counts.json")
	os.WriteFile(p, []byte(`{"alice":{"completed":3`), 0644) // truncated, as if cut off mid-write
	var v map[string]map[string]float64
	if loadStateFile(p, &v) {
		t.Fatal("truncated file loaded as valid")
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Errorf("corrupt file still in place")
	}
	if m, _ := filepath.Glob(p + ".corrupt-*"); len(m) != 1 {
		t.Errorf("expected one .corrupt-* file, got %v", m)
	}
}

func TestLoadStateFileMissingIsNotAnError(t *testing.T) {
	var v map[string]string
	if loadStateFile(filepath.Join(t.TempDir(), "absent.json"), &v) {
		t.Fatal("missing file reported as loaded")
	}
}

func TestSaveUserStateRoundTripAndConsistency(t *testing.T) {
	dir := withStateDir(t)
	userJobHistoryMux.Lock()
	countsMux.Lock()
	oldHist, oldCounts := userJobHistory, userJobCounts
	userJobHistory = map[string]*JobHistoryEntry{"123_4": {JobID: "123_4", User: "alice", State: "completed", LastSeen: time.Now()}}
	userJobCounts = map[string]map[string]float64{"alice": {"completed": 1}}
	countsMux.Unlock()
	userJobHistoryMux.Unlock()
	t.Cleanup(func() { userJobHistory, userJobCounts = oldHist, oldCounts })

	if !saveUserState() {
		t.Fatal("save skipped")
	}
	var h map[string]*JobHistoryEntry
	var c map[string]map[string]float64
	if !loadStateFile(userHistoryFile, &h) || !loadStateFile(userCountsFile, &c) {
		t.Fatal("saved files did not load")
	}
	if h["123_4"].State != "completed" || c["alice"]["completed"] != 1 {
		t.Errorf("round trip mismatch: %+v %+v", h["123_4"], c)
	}
	// A leftover temp file from a killed save is cleaned up.
	os.WriteFile(userHistoryFile+".tmp-999", []byte("partial"), 0644)
	removeStaleTempFiles(userHistoryFile, userCountsFile)
	if m, _ := filepath.Glob(filepath.Join(dir, "*.tmp-*")); len(m) != 0 {
		t.Errorf("stale temp not removed: %v", m)
	}
}

func TestSaveUserStateIsSingleFlight(t *testing.T) {
	withStateDir(t)
	stateSaving = 1 // pretend a save is running
	defer func() { stateSaving = 0 }()
	if saveUserState() {
		t.Fatal("second concurrent save was not skipped")
	}
}

func TestStateFilesAreValidJSONAfterSave(t *testing.T) {
	withStateDir(t)
	if !saveUserState() {
		t.Fatal("save skipped")
	}
	for _, p := range []string{userHistoryFile, userCountsFile} {
		b, _ := os.ReadFile(p)
		if !json.Valid(b) || strings.TrimSpace(string(b)) == "" {
			t.Errorf("%s is not valid JSON: %q", p, b)
		}
	}
}
