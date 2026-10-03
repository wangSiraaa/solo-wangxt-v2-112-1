package backup_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

// gcEngine returns an engine over a fresh tree, plus helpers to make a big
// file big enough that content-defined chunking yields multiple chunks.
func gcSetup(t *testing.T) (*backup.Engine, string) {
	t.Helper()
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	big := strings.Repeat("abcdefgh0123456789ABCDEFGHIJ\n", 7000) // ~190KiB
	must(t, os.WriteFile(filepath.Join(src, "big.bin"), []byte(big), 0o644))
	return e, src
}

func blobExists(t *testing.T, e *backup.Engine, digest []byte) bool {
	t.Helper()
	p, err := e.Store.Path(digest)
	if err != nil {
		t.Fatal(err)
	}
	_, err = os.Lstat(p)
	return !os.IsNotExist(err)
}

// Acceptance 1: deleting the older of two content-sharing snapshots keeps
// every shared blob and the newer snapshot restores byte-complete.
func TestGCOlderSnapshotSharedBlobsSurvive(t *testing.T) {
	e, src := gcSetup(t)

	r1, err := e.CreateSnapshot(src, "v1-old", true)
	if err != nil {
		t.Fatal(err)
	}
	chunks1, err := e.Manifest.ReferencedChunks(r1.SnapshotID)
	if err != nil || len(chunks1) < 3 {
		t.Fatalf("need several chunks, got %d: %v", len(chunks1), err)
	}

	// Small middle edit -> one new chunk, everything else shared.
	p := filepath.Join(src, "big.bin")
	buf, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	copy(buf[90*1024:90*1024+10], []byte("PATCHED!!"))
	must(t, os.WriteFile(p, buf, 0o644))
	r2, err := e.CreateSnapshot(src, "v2-new", true)
	if err != nil {
		t.Fatal(err)
	}
	if r2.NewChunks != 1 {
		t.Fatalf("want exactly 1 new chunk, got %d", r2.NewChunks)
	}
	chunks2, _ := e.Manifest.ReferencedChunks(r2.SnapshotID)
	chunks1After, _ := e.Manifest.ReferencedChunks(r1.SnapshotID)

	// Retain only the newest snapshot.
	if _, err := e.Manifest.AddRetentionRule(1, 0, "newest wins"); err != nil {
		t.Fatal(err)
	}
	res, err := e.RunGC(time.Now().UTC())
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if res.Status != repo.GCStatusSucceeded {
		t.Fatalf("gc status = %s: %s", res.Status, res.Error)
	}
	if res.TargetCount != 1 || res.TargetsDone != 1 {
		t.Fatalf("targets = %+v", res)
	}
	// The old snapshot's unique chunk must be gone; every chunk still used by
	// snapshot 2 must remain.
	shared := map[string]repo.ChunkRef{}
	for _, c := range chunks2 {
		shared[fmt.Sprintf("%x", c.Digest)] = c
	}
	oldOnly := 0
	for _, c := range chunks1After {
		if _, keep := shared[fmt.Sprintf("%x", c.Digest)]; keep {
			if !blobExists(t, e, c.Digest) {
				t.Fatalf("shared blob %x was deleted", c.Digest)
			}
		} else {
			oldOnly++
			if blobExists(t, e, c.Digest) {
				t.Fatalf("exclusive old blob %x should have been deleted", c.Digest)
			}
		}
	}
	if oldOnly != 1 {
		t.Fatalf("expected 1 old-only blob deleted, got %d", oldOnly)
	}
	for _, c := range chunks2 {
		if !blobExists(t, e, c.Digest) {
			t.Fatalf("newer snapshot blob %x missing after GC", c.Digest)
		}
	}

	// Old snapshot is a tombstone and cannot be restored; diagnostics of any
	// pre-existing failure are untouched (none here).
	s1, _ := e.Manifest.GetSnapshot(r1.SnapshotID)
	if s1.Status != repo.StatusReclaimed {
		t.Fatalf("old snapshot status = %s", s1.Status)
	}
	if _, err := e.Restore(r1.SnapshotID, filepath.Join(t.TempDir(), "old")); err == nil {
		t.Fatal("restoring a reclaimed snapshot must fail")
	}

	// Newer snapshot restores fully and byte-identical to the source.
	target := filepath.Join(t.TempDir(), "new")
	rr, err := e.Restore(r2.SnapshotID, target)
	if err != nil {
		t.Fatalf("restore newer: %v", err)
	}
	got, n := fileSHA(t, filepath.Join(target, "big.bin"))
	want, wn := fileSHA(t, p)
	if got != want || n != wn {
		t.Fatalf("restored content mismatch %s/%d vs %s/%d", got, n, want, wn)
	}
	_ = rr
}

// Acceptance 2: hold skips snapshot in both preview and execution; releasing
// the hold makes it reclaimable.
func TestGCHoldSkipsPreviewAndExecution(t *testing.T) {
	e, src := gcSetup(t)
	r1, err := e.CreateSnapshot(src, "old", true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Manifest.AddHold(r1.SnapshotID, "compliance W9"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Manifest.AddRetentionRule(0, 1, "one day"); err != nil {
		t.Fatal(err)
	}
	// The rule is age-based; evaluate a month in the future so snapshots made
	// "now" are expired while keeping the frozen boundary deterministic.
	future := time.Now().UTC().Add(31 * 24 * time.Hour)

	pv, err := e.PreviewRetention(future)
	if err != nil {
		t.Fatal(err)
	}
	if len(pv.Candidates) != 0 || len(pv.SkippedHeld) != 1 || pv.SkippedHeld[0] != r1.SnapshotID {
		t.Fatalf("preview must skip held snapshot: %+v", pv)
	}

	res, err := e.RunGC(future)
	if err != nil {
		t.Fatal(err)
	}
	if res.TargetCount != 0 {
		t.Fatalf("no targets expected while held, got %d", res.TargetCount)
	}
	s1, _ := e.Manifest.GetSnapshot(r1.SnapshotID)
	if s1.Status != repo.StatusCommitted {
		t.Fatalf("held snapshot status = %s", s1.Status)
	}
	chunks, _ := e.Manifest.ReferencedChunks(r1.SnapshotID)
	for _, c := range chunks {
		if !blobExists(t, e, c.Digest) {
			t.Fatal("held snapshot blob deleted")
		}
	}

	// Release: the very next job reclaims it.
	if err := e.Manifest.ReleaseHold(r1.SnapshotID); err != nil {
		t.Fatal(err)
	}
	pv2, _ := e.PreviewRetention(future)
	if len(pv2.Candidates) != 1 || pv2.Candidates[0].SnapshotID != r1.SnapshotID {
		t.Fatalf("released snapshot must reappear as candidate: %+v", pv2)
	}
	res2, err := e.RunGC(future)
	if err != nil {
		t.Fatal(err)
	}
	if res2.TargetsDone != 1 {
		t.Fatalf("want 1 reclaimed after release, got %+v", res2)
	}
	s1b, _ := e.Manifest.GetSnapshot(r1.SnapshotID)
	if s1b.Status != repo.StatusReclaimed {
		t.Fatalf("status = %s", s1b.Status)
	}
}

// Acceptance 3: interrupt after references are deleted but before blob
// cleanup; restart resumes and only removes still-unreferenced blobs,
// producing the same job id and final result.
func TestGCCrashAfterPhase1ResumeSameJob(t *testing.T) {
	e, src := gcSetup(t)
	r1, err := e.CreateSnapshot(src, "old", true)
	if err != nil {
		t.Fatal(err)
	}
	chunks, _ := e.Manifest.ReferencedChunks(r1.SnapshotID)
	if _, err := e.Manifest.AddRetentionRule(0, 1, ""); err != nil {
		t.Fatal(err)
	}
	future := time.Now().UTC().Add(31 * 24 * time.Hour)

	res, err := e.RunGCOpts(future, backup.GCFailpoints{CrashAfterPhase1: true})
	if !errors.Is(err, backup.ErrGCSimulatedCrash) {
		t.Fatalf("want simulated crash, got res=%+v err=%v", res, err)
	}
	jobID := res.JobID

	// Mid-crash state: snapshot references gone, all blob files present.
	s1, _ := e.Manifest.GetSnapshot(r1.SnapshotID)
	if s1.Status != repo.StatusReclaimed {
		t.Fatalf("status after crash = %s", s1.Status)
	}
	for _, c := range chunks {
		if !blobExists(t, e, c.Digest) {
			t.Fatal("blob must survive a phase-1 crash")
		}
	}
	j, _ := e.Manifest.GetGCJob(jobID)
	if j.Status != repo.GCStatusFailed {
		t.Fatalf("job status after crash = %s", j.Status)
	}

	// "Restart": resume the same job id.
	res2, err := e.ResumeGC(jobID)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !res2.Resumed || res2.JobID != jobID {
		t.Fatalf("resume must continue same job: %+v", res2)
	}
	if res2.Status != repo.GCStatusSucceeded {
		t.Fatalf("resumed status = %s: %s", res2.Status, res2.Error)
	}
	if res2.BlobsDeleted != int64(len(chunks)) {
		t.Fatalf("deleted=%d want %d", res2.BlobsDeleted, len(chunks))
	}
	for _, c := range chunks {
		if blobExists(t, e, c.Digest) {
			t.Fatal("blob should be cleaned on resume")
		}
	}

	// Resuming a succeeded job never re-executes: same stored result.
	res3, err := e.ResumeGC(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if !res3.AlreadyDone || res3.BlobsDeleted != res2.BlobsDeleted ||
		res3.BytesFreed != res2.BytesFreed {
		t.Fatalf("succeeded job must be returned unchanged: %+v vs %+v", res2, res3)
	}
	// Running GC again creates a new, empty job and does not touch job 1.
	res4, err := e.RunGC(time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if res4.JobID == jobID || res4.TargetCount != 0 || res4.BlobCount != 0 ||
		res4.BlobsDeleted != 0 {
		t.Fatalf("post-completion GC must be a fresh empty job: %+v", res4)
	}
}

// Crash in the row_removed window (chunks row gone, file still on disk):
// resume unlinks only that orphaned file and finishes with identical counts.
func TestGCCrashAfterRowRemovedResume(t *testing.T) {
	e, src := gcSetup(t)
	r1, _ := e.CreateSnapshot(src, "old", true)
	chunks, _ := e.Manifest.ReferencedChunks(r1.SnapshotID)
	_, _ = e.Manifest.AddRetentionRule(0, 1, "")
	future := time.Now().UTC().Add(31 * 24 * time.Hour)

	res, err := e.RunGCOpts(future, backup.GCFailpoints{CrashAfterRowRemoved: true})
	if !errors.Is(err, backup.ErrGCSimulatedCrash) {
		t.Fatalf("want crash, got %v", err)
	}
	jobID := res.JobID

	detail, _ := e.Manifest.GetGCJob(jobID)
	blobs, _ := e.Manifest.ListGCBlobs(jobID)
	rowRemoved := 0
	for _, b := range blobs {
		if b.State == repo.GCBlobRowRemoved {
			rowRemoved++
			if !blobExists(t, e, b.Digest) {
				t.Fatal("row_removed file must still be on disk at crash")
			}
		}
	}
	if rowRemoved != 1 {
		t.Fatalf("want exactly 1 row_removed blob, got %d", rowRemoved)
	}
	_ = detail

	res2, err := e.ResumeGC(jobID)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if res2.Status != repo.GCStatusSucceeded || res2.BlobsDeleted != int64(len(chunks)) {
		t.Fatalf("resume result = %+v", res2)
	}
	for _, c := range chunks {
		if blobExists(t, e, c.Digest) {
			t.Fatalf("blob %x left after resume", c.Digest)
		}
	}
}

// Acceptance 4: a snapshot racing with GC never loses a referenced blob;
// pending/failed snapshots and their /errors,/missing diagnostics survive.
func TestGCConcurrentSnapshotKeepsItsBlobs(t *testing.T) {
	e, src := gcSetup(t)
	r1, err := e.CreateSnapshot(src, "old", true)
	if err != nil {
		t.Fatal(err)
	}
	oldChunks, _ := e.Manifest.ReferencedChunks(r1.SnapshotID)
	_, _ = e.Manifest.AddRetentionRule(0, 1, "")
	future := time.Now().UTC().Add(31 * 24 * time.Hour)

	// The seam is between phase 1 (target references deleted) and the phase-2
	// zero-reference gates. A snapshot committed in that window re-references
	// every frozen blob candidate, so the gates must keep all the files.
	started := make(chan struct{})
	release := make(chan struct{})
	fail := backup.GCFailpoints{
		BeforePhase2: func(jobID int64) {
			close(started)
			<-release
		},
	}

	gcDone := make(chan *backup.GCResult, 1)
	gcErr := make(chan error, 1)
	go func() {
		res, err := e.RunGCOpts(future, fail)
		gcDone <- res
		gcErr <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("GC never reached phase 2 seam")
	}

	// Concurrent snapshot of the identical tree: it reuses all the same
	// content-addressed chunks the frozen job is about to delete.
	r2, err := e.CreateSnapshot(src, "racer", true)
	if err != nil {
		t.Fatalf("concurrent snapshot: %v", err)
	}
	close(release)
	res := <-gcDone
	if err := <-gcErr; err != nil {
		t.Fatalf("gc: %v", err)
	}
	if res.Status != repo.GCStatusSucceeded {
		t.Fatalf("gc = %+v", res)
	}

	// Every frozen candidate is still referenced by the racing snapshot, so
	// none may be deleted.
	if res.BlobsDeleted != 0 {
		t.Fatalf("GC deleted %d blobs a concurrent snapshot references", res.BlobsDeleted)
	}
	if res.BlobsKept != int64(len(oldChunks)) {
		t.Fatalf("kept=%d want %d", res.BlobsKept, len(oldChunks))
	}
	newChunks, _ := e.Manifest.ReferencedChunks(r2.SnapshotID)
	if len(newChunks) != len(oldChunks) {
		t.Fatalf("racing snapshot chunk count = %d want %d", len(newChunks), len(oldChunks))
	}
	for _, c := range newChunks {
		if !blobExists(t, e, c.Digest) {
			t.Fatalf("racing snapshot lost blob %x", c.Digest)
		}
	}
	target := filepath.Join(t.TempDir(), "racer-out")
	if _, err := e.Restore(r2.SnapshotID, target); err != nil {
		t.Fatalf("restore racing snapshot: %v", err)
	}
	got, n := fileSHA(t, filepath.Join(target, "big.bin"))
	want, wn := fileSHA(t, filepath.Join(src, "big.bin"))
	if got != want || n != wn {
		t.Fatal("racing snapshot restored wrong bytes")
	}

	// With no concurrent snapshot, a follow-up job actually frees the blobs.
	j3, err := e.RunGC(future.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if j3.BlobsDeleted != int64(len(oldChunks)) {
		t.Fatalf("second job should free now-exclusive blobs: %+v", j3)
	}
}

// GC must never touch pending/failed snapshots, and their errors/missing
// diagnostics remain queryable afterwards.
func TestGCNeverRemovesPendingFailedAndDiagnostics(t *testing.T) {
	e, src := gcSetup(t)
	dir := filepath.Dir(src)

	// An eligible committed snapshot of src. Later the pending snapshot of the
	// same tree shares every one of its chunks.
	ro, err := e.CreateSnapshot(src, "old-committed", true)
	if err != nil {
		t.Fatal(err)
	}
	oldChunks, _ := e.Manifest.ReferencedChunks(ro.SnapshotID)
	if len(oldChunks) == 0 {
		t.Fatal("need chunks in the eligible snapshot")
	}

	// A failed snapshot on a SEPARATE root: its lost singleton chunk is unique
	// to that tree, so no later scan can recreate the blob — the missing
	// diagnosis must remain exactly 1.
	src2 := filepath.Join(dir, "src2")
	must(t, os.MkdirAll(src2, 0o755))
	must(t, os.WriteFile(filepath.Join(src2, "unique.log"),
		[]byte(strings.Repeat("zzzz-unique-to-failed-tree\n", 600)), 0o644))
	e.Fail.LoseChunkCount = 1
	rf, err := e.CreateSnapshot(src2, "failed", true)
	e.Fail.LoseChunkCount = 0
	if !errors.As(err, new(*backup.ErrRejected)) {
		t.Fatalf("want failed snapshot, got %v", err)
	}
	failedID := rf.SnapshotID
	failedErrs, err := e.Manifest.ListErrors(failedID)
	if err != nil || len(failedErrs) == 0 {
		t.Fatalf("failed diagnostics missing: %d err=%v", len(failedErrs), err)
	}
	failedMissing, err := e.Manifest.FindMissingChunks(failedID,
		func(d []byte, l int64) (bool, error) { return e.Store.Has(d, l) })
	if err != nil || len(failedMissing) != 1 {
		t.Fatalf("missing diag = %d: %v", len(failedMissing), err)
	}

	// A pending snapshot on the first tree.
	rp, err := e.CreateSnapshot(src, "pending", false)
	if err != nil || rp.Status != repo.StatusPending {
		t.Fatalf("pending snapshot: %+v %v", rp, err)
	}

	// Age-based rule evaluated a month out: the committed snapshot is expired,
	// while pending/failed rows are never eligible regardless of age.
	_, _ = e.Manifest.AddRetentionRule(0, 1, "")
	future := time.Now().UTC().Add(31 * 24 * time.Hour)
	res, err := e.RunGC(future)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != repo.GCStatusSucceeded {
		t.Fatalf("gc = %+v", res)
	}
	if res.TargetCount != 1 || res.TargetsDone != 1 {
		t.Fatalf("only the one committed snapshot may be targeted: %+v", res)
	}

	sf, _ := e.Manifest.GetSnapshot(failedID)
	if sf.Status != repo.StatusFailed {
		t.Fatalf("failed snapshot changed to %s", sf.Status)
	}
	sp, _ := e.Manifest.GetSnapshot(rp.SnapshotID)
	if sp.Status != repo.StatusPending {
		t.Fatalf("pending snapshot changed to %s", sp.Status)
	}
	so, _ := e.Manifest.GetSnapshot(ro.SnapshotID)
	if so.Status != repo.StatusReclaimed {
		t.Fatalf("eligible committed snapshot status = %s", so.Status)
	}

	// The eligible snapshot's blobs are all referenced by the pending snapshot
	// at freeze time, so they are never even frozen as GC candidates: GC
	// reclaims the snapshot row and deletes zero blob files.
	if res.BlobCount != 0 || res.BlobsDeleted != 0 {
		t.Fatalf("pending-referenced blobs must not enter the frozen blob set: %+v", res)
	}
	for _, c := range oldChunks {
		if !blobExists(t, e, c.Digest) {
			t.Fatalf("pending-referenced blob %x was deleted", c.Digest)
		}
	}

	// Diagnostics survive GC untouched.
	errs2, _ := e.Manifest.ListErrors(failedID)
	if len(errs2) != len(failedErrs) {
		t.Fatalf("errors changed: %d -> %d", len(failedErrs), len(errs2))
	}
	missing2, _ := e.Manifest.FindMissingChunks(failedID,
		func(d []byte, l int64) (bool, error) { return e.Store.Has(d, l) })
	if len(missing2) != len(failedMissing) {
		t.Fatalf("missing diagnostics changed: %d -> %d", len(failedMissing), len(missing2))
	}

	// Startup recovery still turns the pending snapshot into a verdict and its
	// retained blobs make it fully restorable.
	rec, err := e.RecoverPending()
	if err != nil || len(rec) != 1 || rec[0].Status != repo.StatusCommitted {
		t.Fatalf("pending recovery = %+v err=%v", rec, err)
	}
	target := filepath.Join(t.TempDir(), "recovered-pending")
	if _, err := e.Restore(rp.SnapshotID, target); err != nil {
		t.Fatalf("recovered pending snapshot must restore: %v", err)
	}
}

// GC accounting is exact even when the job runs twice over a repo where a
// second batch becomes eligible: no double counting across jobs.
func TestGCRepeatedJobsAccountIndependently(t *testing.T) {
	e, src := gcSetup(t)
	r1, _ := e.CreateSnapshot(src, "one", true)
	c1, _ := e.Manifest.ReferencedChunks(r1.SnapshotID)
	_, _ = e.Manifest.AddRetentionRule(0, 1, "")
	future := time.Now().UTC().Add(31 * 24 * time.Hour)

	j1, err := e.RunGC(future)
	if err != nil {
		t.Fatal(err)
	}
	if j1.BlobsDeleted != int64(len(c1)) || j1.BytesFreed <= 0 {
		t.Fatalf("job1 accounting = %+v", j1)
	}
	// Nothing left: a new job freezes zero work.
	j2, err := e.RunGC(future)
	if err != nil {
		t.Fatal(err)
	}
	if j2.JobID == j1.JobID || j2.TargetCount != 0 || j2.BlobCount != 0 || j2.BytesFreed != 0 {
		t.Fatalf("job2 should be empty: %+v", j2)
	}
}
