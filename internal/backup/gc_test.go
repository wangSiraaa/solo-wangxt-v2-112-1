package backup_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

// writeBigVersion writes a ~192KiB file whose content-defined chunks are all
// identical across versions except for one 8-byte patch in the middle: every
// version shares the same base chunks and adds exactly one new middle chunk.
func writeBigVersion(t *testing.T, path string, version int) []byte {
	t.Helper()
	big := []byte(strings.Repeat("0123456789ABCDEF\n", 12000))
	copy(big[90*1024:], []byte(fmt.Sprintf("V%06d!", version)))
	must(t, os.WriteFile(path, big, 0o644))
	return big
}

func digestSet(t *testing.T, e *backup.Engine, id int64) map[string]repo.ChunkRef {
	t.Helper()
	refs, err := e.Manifest.ReferencedChunks(id)
	must(t, err)
	out := map[string]repo.ChunkRef{}
	for _, c := range refs {
		out[string(c.Digest)] = c
	}
	return out
}

func mustHaveBlobs(t *testing.T, e *backup.Engine, refs map[string]repo.ChunkRef, want bool) {
	t.Helper()
	for _, c := range refs {
		ok, err := e.Store.Has(c.Digest, c.Length)
		must(t, err)
		if ok != want {
			t.Fatalf("blob %x present=%v, want %v", c.Digest, ok, want)
		}
	}
}

func restoreAndCheck(t *testing.T, e *backup.Engine, id int64, target, rel string, want []byte) {
	t.Helper()
	rr, err := e.Restore(id, target)
	must(t, err)
	for _, rep := range rr.Verified {
		if rep.RelPath == rel {
			if rep.Size != int64(len(want)) || rep.Digest != sha256Hex(want) {
				t.Fatalf("restored %s mismatch: %d/%s", rel, rep.Size, rep.Digest)
			}
			return
		}
	}
	t.Fatalf("restore report missing %s", rel)
}

// ① Deleting the older of two snapshots that share content blocks must keep
// the shared blobs and leave the newer snapshot fully restorable.
func TestGCReclaimsOlderSharedSnapshot(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	f := filepath.Join(src, "big.bin")

	writeBigVersion(t, f, 1)
	ra, err := e.CreateSnapshot(src, "v1", true)
	must(t, err)
	want2 := writeBigVersion(t, f, 2)
	rb, err := e.CreateSnapshot(src, "v2", true)
	must(t, err)

	setA := digestSet(t, e, ra.SnapshotID)
	setB := digestSet(t, e, rb.SnapshotID)
	shared, uniqueA := map[string]repo.ChunkRef{}, map[string]repo.ChunkRef{}
	for k, c := range setA {
		if _, ok := setB[k]; ok {
			shared[k] = c
		} else {
			uniqueA[k] = c
		}
	}
	if len(uniqueA) != 1 || len(shared) != len(setA)-1 {
		t.Fatalf("chunk layout: shared=%d uniqueA=%d", len(shared), len(uniqueA))
	}

	if _, err := e.Manifest.PutRetentionRule("default", 1, 0); err != nil {
		t.Fatal(err)
	}
	prev, err := e.PreviewGC(backup.GCSelector{RuleName: "default"})
	must(t, err)
	if len(prev.Targets) != 1 || prev.Targets[0].SnapshotID != ra.SnapshotID {
		t.Fatalf("preview targets = %+v", prev.Targets)
	}
	if prev.ReclaimableBlobs != 1 || prev.CandidateBlobs != len(setA) {
		t.Fatalf("preview blobs: candidate=%d reclaimable=%d",
			prev.CandidateBlobs, prev.ReclaimableBlobs)
	}
	if len(prev.KeptByRule) != 1 || prev.KeptByRule[0] != rb.SnapshotID {
		t.Fatalf("kept = %v", prev.KeptByRule)
	}

	jobID, err := e.CreateGCJob(backup.GCSelector{RuleName: "default"})
	must(t, err)
	job, err := e.RunGCJob(jobID, false)
	must(t, err)
	if job.Status != repo.GCJobCompleted || job.SnapshotsDone != 1 || job.BlobsDeleted != 1 {
		t.Fatalf("job = %+v", job)
	}
	var uniqueLen int64
	for _, c := range uniqueA {
		uniqueLen = c.Length
	}
	if job.BytesFreed != uniqueLen {
		t.Fatalf("bytes_freed=%d want %d", job.BytesFreed, uniqueLen)
	}
	if _, err := e.Manifest.GetSnapshot(ra.SnapshotID); !errors.Is(err, repo.ErrNotFound) {
		t.Fatalf("reclaimed snapshot still present: %v", err)
	}
	mustHaveBlobs(t, e, shared, true)   // shared blocks survive
	mustHaveBlobs(t, e, uniqueA, false) // old-only blocks are gone
	restoreAndCheck(t, e, rb.SnapshotID, filepath.Join(dir, "out-b"), "big.bin", want2)
}

// ② A protected target is skipped by preview and execution; only after
// unprotecting does it become reclaimable.
func TestGCProtectSkipsTarget(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	f := filepath.Join(src, "big.bin")

	writeBigVersion(t, f, 1)
	ra, err := e.CreateSnapshot(src, "v1", true)
	must(t, err)
	want2 := writeBigVersion(t, f, 2)
	rb, err := e.CreateSnapshot(src, "v2", true)
	must(t, err)
	want3 := writeBigVersion(t, f, 3)
	rc, err := e.CreateSnapshot(src, "v3", true)
	must(t, err)

	if _, err := e.Manifest.PutRetentionRule("default", 1, 0); err != nil {
		t.Fatal(err)
	}
	must(t, e.Manifest.ProtectSnapshot(rb.SnapshotID, "audit hold"))

	prev, err := e.PreviewGC(backup.GCSelector{RuleName: "default"})
	must(t, err)
	if len(prev.Targets) != 1 || prev.Targets[0].SnapshotID != ra.SnapshotID {
		t.Fatalf("targets = %+v", prev.Targets)
	}
	if len(prev.SkippedProtected) != 1 ||
		prev.SkippedProtected[0].SnapshotID != rb.SnapshotID ||
		prev.SkippedProtected[0].Reason != "audit hold" {
		t.Fatalf("skipped_protected = %+v", prev.SkippedProtected)
	}

	jobID, err := e.CreateGCJob(backup.GCSelector{RuleName: "default"})
	must(t, err)
	job, err := e.RunGCJob(jobID, false)
	must(t, err)
	if job.SnapshotsDone != 1 { // only A; protected B was never frozen as a target
		t.Fatalf("snapshots_done=%d", job.SnapshotsDone)
	}
	if si, err := e.Manifest.GetSnapshot(rb.SnapshotID); err != nil || si.Status != repo.StatusCommitted {
		t.Fatalf("protected snapshot touched: %v %+v", err, si)
	}
	restoreAndCheck(t, e, rb.SnapshotID, filepath.Join(dir, "out-b"), "big.bin", want2)

	must(t, e.Manifest.UnprotectSnapshot(rb.SnapshotID))
	prev, err = e.PreviewGC(backup.GCSelector{RuleName: "default"})
	must(t, err)
	if len(prev.Targets) != 1 || prev.Targets[0].SnapshotID != rb.SnapshotID {
		t.Fatalf("after unprotect targets = %+v", prev.Targets)
	}
	jobID, err = e.CreateGCJob(backup.GCSelector{RuleName: "default"})
	must(t, err)
	if _, err := e.RunGCJob(jobID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Manifest.GetSnapshot(rb.SnapshotID); !errors.Is(err, repo.ErrNotFound) {
		t.Fatalf("unprotected snapshot not reclaimed: %v", err)
	}
	restoreAndCheck(t, e, rc.SnapshotID, filepath.Join(dir, "out-c"), "big.bin", want3)
}

// ③ Interrupt after manifest references are deleted but before blobs are
// swept: after a restart the same job resumes, deletes only unreferenced
// blobs, and reports one consistent result without double accounting.
func TestGCInterruptResumeSameJob(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	f := filepath.Join(src, "big.bin")

	open := func() *backup.Engine {
		m, err := repo.OpenManifest(filepath.Join(dir, "manifest.sqlite"))
		must(t, err)
		t.Cleanup(func() { m.Close() })
		s, err := repo.NewContentStore(filepath.Join(dir, "chunks"))
		must(t, err)
		e, err := backup.NewEngine(m, s)
		must(t, err)
		return e
	}

	e := open()
	writeBigVersion(t, f, 1)
	ra, err := e.CreateSnapshot(src, "v1", true)
	must(t, err)
	want2 := writeBigVersion(t, f, 2)
	rb, err := e.CreateSnapshot(src, "v2", true)
	must(t, err)
	setA := digestSet(t, e, ra.SnapshotID)
	setB := digestSet(t, e, rb.SnapshotID)
	shared, uniqueA := map[string]repo.ChunkRef{}, map[string]repo.ChunkRef{}
	for k, c := range setA {
		if _, ok := setB[k]; ok {
			shared[k] = c
		} else {
			uniqueA[k] = c
		}
	}

	if _, err := e.Manifest.PutRetentionRule("default", 1, 0); err != nil {
		t.Fatal(err)
	}
	jobID, err := e.CreateGCJob(backup.GCSelector{RuleName: "default"})
	must(t, err)

	// Crash point: phase 1 done (references gone), phase 2 not started.
	job, err := e.RunGCJob(jobID, true)
	must(t, err)
	if job.Status != repo.GCJobRefsDeleted || job.BlobsDeleted != 0 {
		t.Fatalf("interrupted job = %+v", job)
	}
	if _, err := e.Manifest.GetSnapshot(ra.SnapshotID); !errors.Is(err, repo.ErrNotFound) {
		t.Fatalf("references should be deleted: %v", err)
	}
	mustHaveBlobs(t, e, uniqueA, true) // orphaned blob not yet swept
	e.Manifest.Close()

	// Restart: the same job resumes and finishes; nothing is re-executed.
	e2 := open()
	resumed, err := e2.ResumeGCJobs()
	must(t, err)
	if len(resumed) != 1 || resumed[0] != jobID {
		t.Fatalf("resumed = %v", resumed)
	}
	job2, err := e2.Manifest.GetGCJob(jobID)
	must(t, err)
	if job2.Status != repo.GCJobCompleted || job2.BlobsDeleted != 1 || job2.SnapshotsDone != 1 {
		t.Fatalf("resumed job = %+v", job2)
	}
	mustHaveBlobs(t, e2, uniqueA, false) // only unreferenced blobs swept
	mustHaveBlobs(t, e2, shared, true)
	restoreAndCheck(t, e2, rb.SnapshotID, filepath.Join(dir, "out-b"), "big.bin", want2)

	// Re-running a completed job is a no-op: counters must not move.
	job3, err := e2.RunGCJob(jobID, false)
	must(t, err)
	if job3.BlobsDeleted != job2.BlobsDeleted || job3.BytesFreed != job2.BytesFreed ||
		job3.RefsDeleted != job2.RefsDeleted {
		t.Fatalf("completed job re-executed: %+v vs %+v", job3, job2)
	}
	if again, err := e2.ResumeGCJobs(); err != nil || len(again) != 0 {
		t.Fatalf("nothing should remain resumable: %v %v", again, err)
	}

	events, err := e2.Manifest.ListGCEvents(jobID)
	must(t, err)
	actions := map[string]int{}
	for _, ev := range events {
		actions[ev.Action]++
	}
	for _, want := range []string{"job_created", "job_started", "snapshot_refs_deleted",
		"job_resumed", "blob_deleted", "job_completed"} {
		if actions[want] == 0 {
			t.Fatalf("audit trail missing %q: %v", want, actions)
		}
	}
	if actions["blob_deleted"] != 1 {
		t.Fatalf("blob_deleted recorded %d times (double accounting?)", actions["blob_deleted"])
	}
}

// ④ A snapshot created concurrently with GC never loses its chunks, and
// pending/failed snapshots plus their diagnostics survive collection.
func TestGCConcurrentSnapshotKeepsLiveChunks(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	f := filepath.Join(src, "big.bin")

	writeBigVersion(t, f, 1)
	ra, err := e.CreateSnapshot(src, "v1", true)
	must(t, err)
	want2 := writeBigVersion(t, f, 2)
	rb, err := e.CreateSnapshot(src, "v2", true)
	must(t, err)

	// A pending snapshot (crash-before-commit shape) and a failed snapshot
	// with diagnostics must both be off-limits for GC.
	rp, err := e.CreateSnapshot(src, "pending", false)
	must(t, err)
	if rp.Status != repo.StatusPending {
		t.Fatalf("status=%s", rp.Status)
	}
	writeBigVersion(t, f, 3)
	e.Fail.LoseChunkCount = 1
	rf, err := e.CreateSnapshot(src, "doomed", true)
	e.Fail.LoseChunkCount = 0
	var rej *backup.ErrRejected
	if !errors.As(err, &rej) || rf.Status != repo.StatusFailed {
		t.Fatalf("want failed snapshot, got %v %+v", err, rf)
	}
	has := func(d []byte, l int64) (bool, error) { return e.Store.Has(d, l) }
	missBefore, err := e.Manifest.FindMissingChunks(rf.SnapshotID, has)
	must(t, err)
	if len(missBefore) != 1 {
		t.Fatalf("missing before = %d", len(missBefore))
	}

	if _, err := e.Manifest.PutRetentionRule("default", 1, 0); err != nil {
		t.Fatal(err)
	}
	jobID, err := e.CreateGCJob(backup.GCSelector{RuleName: "default"})
	must(t, err)

	// GC and a brand-new snapshot race; the engine serializes them, and
	// either order must leave the new snapshot's chunks intact.
	want4 := writeBigVersion(t, f, 4)
	gcDone := make(chan error, 1)
	go func() {
		_, err := e.RunGCJob(jobID, false)
		gcDone <- err
	}()
	rq, err := e.CreateSnapshot(src, "concurrent", false)
	must(t, err)
	must(t, <-gcDone)

	targets, err := e.Manifest.GCJobTargets(jobID)
	must(t, err)
	if len(targets) != 1 || targets[0].SnapshotID != ra.SnapshotID {
		t.Fatalf("job targets = %+v", targets)
	}
	job, err := e.Manifest.GetGCJob(jobID)
	must(t, err)
	if job.Status != repo.GCJobCompleted || job.BlobsDeleted != 1 {
		t.Fatalf("job = %+v", job)
	}

	// pending/failed snapshots and their diagnostics are untouched.
	if si, err := e.Manifest.GetSnapshot(rp.SnapshotID); err != nil || si.Status != repo.StatusPending {
		t.Fatalf("pending snapshot touched: %v %+v", err, si)
	}
	if si, err := e.Manifest.GetSnapshot(rf.SnapshotID); err != nil || si.Status != repo.StatusFailed {
		t.Fatalf("failed snapshot touched: %v %+v", err, si)
	}
	if errs, _ := e.Manifest.ListErrors(rf.SnapshotID); len(errs) == 0 {
		t.Fatal("failed snapshot diagnostics lost")
	}
	missAfter, err := e.Manifest.FindMissingChunks(rf.SnapshotID, has)
	must(t, err)
	if len(missAfter) != 1 || missAfter[0].RelPath != missBefore[0].RelPath {
		t.Fatalf("missing diagnostics changed: %v vs %v", missAfter, missBefore)
	}

	// The concurrent snapshot verifies and restores completely.
	res, err := e.VerifyAndFinalize(rq.SnapshotID)
	must(t, err)
	if res.Status != repo.StatusCommitted {
		t.Fatalf("concurrent snapshot = %s", res.Status)
	}
	restoreAndCheck(t, e, rq.SnapshotID, filepath.Join(dir, "out-q"), "big.bin", want4)
	restoreAndCheck(t, e, rb.SnapshotID, filepath.Join(dir, "out-b"), "big.bin", want2)
}

// A job runs with the rule version frozen at creation time, even if the rule
// is changed before the job executes.
func TestGCJobFreezesRuleVersion(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	f := filepath.Join(src, "big.bin")

	writeBigVersion(t, f, 1)
	ra, err := e.CreateSnapshot(src, "v1", true)
	must(t, err)
	writeBigVersion(t, f, 2)
	if _, err := e.CreateSnapshot(src, "v2", true); err != nil {
		t.Fatal(err)
	}

	if _, err := e.Manifest.PutRetentionRule("default", 1, 0); err != nil {
		t.Fatal(err)
	}
	jobID, err := e.CreateGCJob(backup.GCSelector{RuleName: "default"})
	must(t, err)
	// Rule is replaced by one that would keep everything; the frozen job
	// must still reclaim exactly its frozen target.
	if _, err := e.Manifest.PutRetentionRule("default", 100, 0); err != nil {
		t.Fatal(err)
	}
	job, err := e.RunGCJob(jobID, false)
	must(t, err)
	if job.RuleVersion != 1 || job.SnapshotsDone != 1 {
		t.Fatalf("job = %+v", job)
	}
	if _, err := e.Manifest.GetSnapshot(ra.SnapshotID); !errors.Is(err, repo.ErrNotFound) {
		t.Fatalf("frozen target not reclaimed: %v", err)
	}
}

func TestGCRuleValidation(t *testing.T) {
	e, _ := openEngine(t)
	if _, err := e.PreviewGC(backup.GCSelector{}); err == nil ||
		!strings.Contains(err.Error(), "keeps nothing") {
		t.Fatalf("empty rule accepted: %v", err)
	}
	if _, err := e.CreateGCJob(backup.GCSelector{KeepLast: -1}); err == nil {
		t.Fatal("negative keep_last accepted")
	}
	if _, err := e.PreviewGC(backup.GCSelector{RuleName: "nope"}); !errors.Is(err, repo.ErrNotFound) {
		t.Fatalf("unknown rule: %v", err)
	}
	// keep_days alone is a valid rule and keeps everything fresh.
	if _, err := e.Manifest.PutRetentionRule("fresh", 0, 30); err != nil {
		t.Fatal(err)
	}
	prev, err := e.PreviewGC(backup.GCSelector{RuleName: "fresh"})
	must(t, err)
	if len(prev.Targets) != 0 {
		t.Fatalf("fresh rule targets = %+v", prev.Targets)
	}
}
