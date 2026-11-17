/* Copyright 2026 Chan Zuckerberg Biohub

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

// Persistence of the per-user job history and job counts across restarts.
//
// The two files are one logical state: when a job leaves the queue it is
// marked "completed" in the history AND added to the counts. They are
// therefore always saved together from a single snapshot. If the counts were
// newer than the history, a restart would see the job as running-then-gone
// and count it a second time.
//
// Writes are atomic (temp file in the same directory, fsync, rename), so a
// crash or restart mid-save leaves the previous complete file in place. A
// file that still fails to parse is moved aside rather than silently
// replaced with an empty state.

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	stateSaving   int32 // 1 while a save is in flight
	lastStateSave time.Time
	lastSaveMux   sync.Mutex
	// How often the periodic refresh saves the state. Saves on shutdown are
	// unconditional. The history file is large (tens of MB), so every refresh
	// (30 s) is wasteful, especially over NFS.
	stateSaveInterval = time.Duration(envInt("SLURM_EXPORTER_STATE_SAVE_SECONDS", 300)) * time.Second
)

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return def
}

// writeFileAtomic replaces path with data so that readers (and a restart) see
// either the old or the new content, never a partial file.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return err
	}
	return nil
}

// loadStateFile unmarshals path into v. A missing file is normal on first
// start. A file that exists but does not parse is renamed to
// "<path>.corrupt-<time>" (kept for inspection) and reported loudly; the
// caller then starts from an empty state.
func loadStateFile(path string, v interface{}) bool {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		log.Printf("No existing state file %s; starting empty", path)
		return false
	}
	if err != nil {
		log.Printf("ERROR: cannot read state file %s: %v; starting empty", path, err)
		return false
	}
	if err := json.Unmarshal(data, v); err != nil {
		aside := fmt.Sprintf("%s.corrupt-%s", path, time.Now().Format("20060102T150405"))
		if rerr := os.Rename(path, aside); rerr != nil {
			log.Printf("ERROR: state file %s is corrupt (%v) and could not be moved aside: %v; starting empty", path, err, rerr)
		} else {
			log.Printf("ERROR: state file %s is corrupt (%v, %d bytes); moved to %s and starting empty. "+
				"Totals derived from it will restart from zero.", path, err, len(data), aside)
		}
		return false
	}
	return true
}

// removeStaleTempFiles deletes temp files left by a save that was killed
// before its rename.
func removeStaleTempFiles(paths ...string) {
	for _, p := range paths {
		matches, _ := filepath.Glob(p + ".tmp-*")
		for _, m := range matches {
			if err := os.Remove(m); err == nil {
				log.Printf("Removed stale temp file %s", m)
			}
		}
	}
}

// saveUserState writes the job history and job counts from one consistent
// snapshot. Returns false without doing anything if a save is already running.
func saveUserState() bool {
	if !atomic.CompareAndSwapInt32(&stateSaving, 0, 1) {
		return false
	}
	defer atomic.StoreInt32(&stateSaving, 0)

	start := time.Now()
	// Same lock order as the collector (history, then counts).
	userJobHistoryMux.RLock()
	countsMux.RLock()
	history, herr := json.Marshal(userJobHistory)
	counts, cerr := json.Marshal(userJobCounts)
	nJobs, nUsers := len(userJobHistory), len(userJobCounts)
	countsMux.RUnlock()
	userJobHistoryMux.RUnlock()
	if herr != nil || cerr != nil {
		log.Printf("ERROR: cannot marshal state (history: %v, counts: %v); not saved", herr, cerr)
		return true
	}

	// History first: if we die between the two renames, the counts are the
	// older ones, which at worst misses a few completions rather than
	// double-counting them.
	if err := writeFileAtomic(userHistoryFile, history, 0644); err != nil {
		log.Printf("ERROR: failed to save job history: %v", err)
		return true
	}
	if err := writeFileAtomic(userCountsFile, counts, 0644); err != nil {
		log.Printf("ERROR: failed to save job counts: %v", err)
		return true
	}
	lastSaveMux.Lock()
	lastStateSave = time.Now()
	lastSaveMux.Unlock()
	if d := time.Since(start); d > 5*time.Second {
		log.Printf("Saved state (%d jobs, %d users, %d+%d bytes) in %v", nJobs, nUsers, len(history), len(counts), d.Round(time.Millisecond))
	}
	return true
}

// maybeSaveUserState is called after every refresh; it starts a background
// save at most once per stateSaveInterval.
func maybeSaveUserState() {
	lastSaveMux.Lock()
	due := time.Since(lastStateSave) >= stateSaveInterval
	if due {
		// Claim this slot now so the next refresh doesn't start another save
		// while this one is still running.
		lastStateSave = time.Now()
	}
	lastSaveMux.Unlock()
	if due {
		go saveUserState()
	}
}

// shutdownSaveUserState waits (bounded) for an in-flight save, then saves
// once more so a restart loses nothing.
func shutdownSaveUserState(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for !saveUserState() {
		if time.Now().After(deadline) {
			log.Printf("ERROR: in-flight state save did not finish within %v; exiting without a final save", timeout)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Printf("Saved job history and counts for shutdown")
}

// stateFilesSummary is used in log messages.
func stateFilesSummary() string {
	return strings.Join([]string{userHistoryFile, userCountsFile}, ", ")
}
