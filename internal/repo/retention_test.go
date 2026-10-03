package repo_test

import (
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"incbackup/internal/repo"
)

func openManifestStore(t *testing.T) (*repo.Manifest, func()) {
	t.Helper()
	dir := t.TempDir()
	m, err := repo.OpenManifest(dir + "/manifest.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	return m, func() { m.Close() }
}

// minimalSnapshot inserts a committed snapshot directly at the catalog level
// with a controlled commit time and a set of chunk digests.
func seedCommitted(t *testing.T, m *repo.Manifest, committedAt time.Time, digests [][]byte) int64 {
	t.Helper()
	now := committedAt.UTC().Format(time.RFC3339Nano)
	res, err := m.DB().Exec(`INSERT INTO snapshots
		(root_path, status, polynomial, created_at, committed_at)
		VALUES (?,?,?,?,?)`, "/data", repo.StatusCommitted, 1, now, now)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	if len(digests) > 0 {
		if _, err := m.DB().Exec(`INSERT INTO entries (snapshot_id, rel_path, kind, mode, mod_time_ns, entry_order)
			VALUES (?,?,?,?,?,?)`, id, "f", repo.KindFile, 0o644, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	for i, d := range digests {
		if _, err := m.DB().Exec(`INSERT OR IGNORE INTO chunks (digest, length, created_at) VALUES (?,?,?)`,
			d, int64(len(d)), now); err != nil {
			t.Fatal(err)
		}
		if _, err := m.DB().Exec(`INSERT INTO entry_chunks (snapshot_id, rel_path, chunk_digest, seq)
			VALUES (?,?,?,?)`, id, "f", d, i); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

func digest(b byte) []byte {
	d := make([]byte, 32)
	for i := range d {
		d[i] = b
	}
	return d
}

func TestRetentionRuleVersioningImmutability(t *testing.T) {
	m, cleanup := openManifestStore(t)
	defer cleanup()

	if _, err := m.LatestRetentionRule(); !errors.Is(err, repo.ErrNoRetentionRule) {
		t.Fatalf("want ErrNoRetentionRule, got %v", err)
	}
	if _, err := m.AddRetentionRule(0, 0, ""); !errors.Is(err, repo.ErrInvalidRule) {
		t.Fatalf("want ErrInvalidRule for keep-nothing, got %v", err)
	}
	r1, err := m.AddRetentionRule(1, 0, "keep one")
	if err != nil {
		t.Fatal(err)
	}
	r2, err := m.AddRetentionRule(0, 7, "a week")
	if err != nil {
		t.Fatal(err)
	}
	if r2.Version != r1.Version+1 {
		t.Fatalf("rule versions not increasing: %d %d", r1.Version, r2.Version)
	}
	got, err := m.GetRetentionRule(r1.Version)
	if err != nil || got.KeepLastN != 1 || got.Note != "keep one" {
		t.Fatalf("old rule version mutated: %+v err=%v", got, err)
	}
	latest, _ := m.LatestRetentionRule()
	if latest.Version != r2.Version {
		t.Fatalf("latest = %d want %d", latest.Version, r2.Version)
	}
}

func TestEvaluateRetentionEligibilityAndHold(t *testing.T) {
	m, cleanup := openManifestStore(t)
	defer cleanup()
	now := time.Now().UTC()

	dOld := digest(0xAA) // exclusive to oldest
	dShared := digest(0xBB)
	dNew := digest(0xCC)

	old := seedCommitted(t, m, now.Add(-30*24*time.Hour), [][]byte{dOld, dShared})
	mid := seedCommitted(t, m, now.Add(-20*24*time.Hour), [][]byte{dShared})
	newest := seedCommitted(t, m, now.Add(-1*time.Hour), [][]byte{dShared, dNew})

	rule, err := m.AddRetentionRule(2, 0, "keep newest 2")
	if err != nil {
		t.Fatal(err)
	}
	pv, err := m.EvaluateRetention(rule, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(pv.Candidates) != 1 || pv.Candidates[0].SnapshotID != old {
		t.Fatalf("candidates = %+v", pv.Candidates)
	}
	if pv.ProtectedCommitted != 2 {
		t.Fatalf("protected = %d", pv.ProtectedCommitted)
	}
	// dShared is referenced by protected snapshots; only dOld is exclusive.
	if len(pv.ExclusiveChunks) != 1 {
		t.Fatalf("exclusive chunks = %d (%x)", len(pv.ExclusiveChunks), pv.ExclusiveChunks)
	}
	if hex.EncodeToString(pv.ExclusiveChunks[0].Digest) != hex.EncodeToString(dOld) {
		t.Fatalf("wrong exclusive chunk: %x", pv.ExclusiveChunks[0].Digest)
	}

	// Hold the old snapshot: it drops out of candidates, nothing exclusive left.
	if _, err := m.AddHold(old, "legal"); err != nil {
		t.Fatal(err)
	}
	pv, err = m.EvaluateRetention(rule, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(pv.Candidates) != 0 || len(pv.SkippedHeld) != 1 || pv.SkippedHeld[0] != old {
		t.Fatalf("held snapshot not skipped: %+v", pv)
	}
	if len(pv.ExclusiveChunks) != 0 {
		t.Fatalf("held snapshot's chunks must not look reclaimable: %d", len(pv.ExclusiveChunks))
	}
	// A second active hold on top of an active one must be rejected by the
	// partial unique index.
	if _, err := m.AddHold(old, "again"); err == nil {
		t.Fatal("duplicate active hold should fail")
	}
	if err := m.ReleaseHold(old); err != nil {
		t.Fatal(err)
	}
	pv, _ = m.EvaluateRetention(rule, now)
	if len(pv.Candidates) != 1 {
		t.Fatalf("released hold should make it eligible again: %+v", pv.Candidates)
	}
	_ = mid
	_ = newest
}

func TestEvaluateRetentionPendingFailedNeverEligible(t *testing.T) {
	m, cleanup := openManifestStore(t)
	defer cleanup()
	now := time.Now().UTC()

	d := digest(1)
	old := seedCommitted(t, m, now.Add(-90*24*time.Hour), [][]byte{d})

	// pending and failed snapshots referencing the same (otherwise exclusive)
	// chunk keep it protected.
	for _, status := range []string{repo.StatusPending, repo.StatusFailed} {
		ts := now.Add(-90 * 24 * time.Hour).Format(time.RFC3339Nano)
		res, _ := m.DB().Exec(`INSERT INTO snapshots
			(root_path, status, polynomial, created_at, committed_at) VALUES (?,?,?,?,?)`,
			"/data", status, 1, ts, ts)
		id, _ := res.LastInsertId()
		m.DB().Exec(`INSERT INTO entries (snapshot_id, rel_path, kind, mode, mod_time_ns, entry_order)
			VALUES (?,?,?,?,?,?)`, id, "p", repo.KindFile, 0o644, 0, 0)
		m.DB().Exec(`INSERT INTO entry_chunks (snapshot_id, rel_path, chunk_digest, seq)
			VALUES (?,?,?,?)`, id, "p", d, 0)
	}

	rule, _ := m.AddRetentionRule(0, 1, "one day")
	pv, err := m.EvaluateRetention(rule, now)
	if err != nil {
		t.Fatal(err)
	}
	if pv.Pending != 1 || pv.Failed != 1 {
		t.Fatalf("pending=%d failed=%d", pv.Pending, pv.Failed)
	}
	if len(pv.Candidates) != 1 || pv.Candidates[0].SnapshotID != old {
		t.Fatalf("candidates = %+v", pv.Candidates)
	}
	// d must NOT be exclusive: pending/failed snapshots still reference it.
	if len(pv.ExclusiveChunks) != 0 {
		t.Fatalf("chunk referenced by pending/failed must be protected, got %d", len(pv.ExclusiveChunks))
	}
}

func TestCreateGCJobFreezesTargetsRuleAndBlobs(t *testing.T) {
	m, cleanup := openManifestStore(t)
	defer cleanup()
	now := time.Now().UTC()

	d1, d2 := digest(1), digest(2)
	dShared := digest(3)
	old1 := seedCommitted(t, m, now.Add(-30*24*time.Hour), [][]byte{d1, dShared})
	old2 := seedCommitted(t, m, now.Add(-20*24*time.Hour), [][]byte{d2, dShared})
	keep := seedCommitted(t, m, now.Add(-time.Minute), [][]byte{dShared})

	rule, _ := m.AddRetentionRule(1, 0, "newest one")
	job, err := m.CreateGCJob(now)
	if err != nil {
		t.Fatal(err)
	}
	if job.RuleVersion != rule.Version || job.Status != repo.GCStatusQueued {
		t.Fatalf("job = %+v", job)
	}
	targets, err := m.ListGCTargets(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	gotIDs := map[int64]bool{}
	for _, tg := range targets {
		gotIDs[tg.SnapshotID] = true
		if tg.State != repo.GCTargetPending {
			t.Fatalf("fresh target state = %s", tg.State)
		}
	}
	if !gotIDs[old1] || !gotIDs[old2] || gotIDs[keep] || len(targets) != 2 {
		t.Fatalf("frozen targets = %v want %d,%d (not %d)", gotIDs, old1, old2, keep)
	}
	blobs, err := m.ListGCBlobs(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs) != 2 {
		t.Fatalf("frozen blobs = %d want 2 (shared chunk must not be listed)", len(blobs))
	}
	for _, b := range blobs {
		if hex.EncodeToString(b.Digest) == hex.EncodeToString(dShared) {
			t.Fatal("shared chunk leaked into frozen blob set")
		}
	}

	// Adding a newer rule after the freeze must not change the frozen job.
	if _, err := m.AddRetentionRule(100, 100, "everything protected later"); err != nil {
		t.Fatal(err)
	}
	job2, _ := m.GetGCJob(job.ID)
	if job2.RuleVersion != rule.Version {
		t.Fatalf("frozen rule version moved: %d -> %d", rule.Version, job2.RuleVersion)
	}
}

func TestTwoPhaseDeletionIdempotentAndShareSafe(t *testing.T) {
	m, cleanup := openManifestStore(t)
	defer cleanup()
	now := time.Now().UTC()

	dExclusive := digest(7)
	dBecomesShared := digest(8)
	// At freeze time both chunks are exclusive to the old snapshot.
	target := seedCommitted(t, m, now.Add(-30*24*time.Hour), [][]byte{dExclusive, dBecomesShared})

	_, _ = m.AddRetentionRule(0, 1, "")
	job, _ := m.CreateGCJob(now)

	// A surviving snapshot taken AFTER the freeze references one of the
	// frozen candidates: phase 2 must keep that blob even though it was
	// frozen as exclusive. (FK ordering: chunk rows still exist pre-gate.)
	keeper := seedCommitted(t, m, now.Add(-time.Minute), [][]byte{dBecomesShared})

	if err := m.StartGCJob(job.ID); err != nil {
		t.Fatal(err)
	}
	// Phase 1 only reclaims the frozen target; the keeper is unrelated.
	state, err := m.ReclaimSnapshotTarget(job.ID, target)
	if err != nil || state != repo.GCTargetDone {
		t.Fatalf("phase 1: state=%s err=%v", state, err)
	}
	// Idempotent: re-running phase 1 for the same target changes nothing.
	state2, err := m.ReclaimSnapshotTarget(job.ID, target)
	if err != nil || state2 != repo.GCTargetDone {
		t.Fatalf("phase 1 re-run: state=%s err=%v", state2, err)
	}
	snap, _ := m.GetSnapshot(target)
	if snap.Status != repo.StatusReclaimed {
		t.Fatalf("target status = %s", snap.Status)
	}
	// keeper untouched
	k, _ := m.GetSnapshot(keeper)
	if k.Status != repo.StatusCommitted {
		t.Fatalf("keeper status = %s", k.Status)
	}

	// Phase 2 gates
	st, _, err := m.ApproveBlobDeletion(job.ID, dExclusive)
	if err != nil || st != repo.GCBlobRowRemoved {
		t.Fatalf("exclusive: %s %v", st, err)
	}
	st, _, err = m.ApproveBlobDeletion(job.ID, dBecomesShared)
	if err != nil || st != repo.GCBlobKeptShared {
		t.Fatalf("shared blob must be kept: %s %v", st, err)
	}
	// Re-gating an already-settled blob is a no-op.
	st, _, _ = m.ApproveBlobDeletion(job.ID, dBecomesShared)
	if st != repo.GCBlobKeptShared {
		t.Fatalf("kept blob re-evaluated: %s", st)
	}
	st, _, _ = m.ApproveBlobDeletion(job.ID, dExclusive)
	if st != repo.GCBlobRowRemoved {
		t.Fatalf("row_removed blob gate not stable: %s", st)
	}

	// Exclusive chunks row must be gone, shared row stays.
	if exists, _ := m.ChunkRowExists(dExclusive); exists {
		t.Fatal("exclusive chunks row should be deleted at the gate")
	}
	if exists, _ := m.ChunkRowExists(dBecomesShared); !exists {
		t.Fatal("shared chunks row must remain")
	}

	if err := m.FinalizeBlobDeletion(job.ID, dExclusive); err != nil {
		t.Fatal(err)
	}
	// Double finalize must not error or double-account (state stays deleted).
	if err := m.FinalizeBlobDeletion(job.ID, dExclusive); err != nil {
		t.Fatal(err)
	}

	done, err := m.FinishGCJob(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.BlobsDeleted != 1 || done.BlobsKeptShared != 1 || done.BytesFreed != 32 {
		t.Fatalf("counters = %+v", done)
	}
	// Finishing twice cannot re-execute: counters identical, and StartGCJob
	// on a succeeded job is refused by Finish's WHERE.
	done2, err := m.FinishGCJob(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done2.BlobsDeleted != 1 || done2.BytesFreed != 32 {
		t.Fatalf("double finish changed counters: %+v", done2)
	}
}

func TestResumeAfterRowRemovedRechecksReferences(t *testing.T) {
	m, cleanup := openManifestStore(t)
	defer cleanup()
	now := time.Now().UTC()

	d := digest(9)
	target := seedCommitted(t, m, now.Add(-30*24*time.Hour), [][]byte{d})
	_, _ = m.AddRetentionRule(0, 1, "")
	job, _ := m.CreateGCJob(now)
	_ = m.StartGCJob(job.ID)
	if _, err := m.ReclaimSnapshotTarget(job.ID, target); err != nil {
		t.Fatal(err)
	}
	// Gate deletes the chunks row -> row_removed, file still "on disk".
	st, _, err := m.ApproveBlobDeletion(job.ID, d)
	if err != nil || st != repo.GCBlobRowRemoved {
		t.Fatalf("gate: %s %v", st, err)
	}
	// Simulate a concurrent snapshot committed during the crash window: it
	// re-creates the chunk row and references it.
	ts := now.Format(time.RFC3339Nano)
	res, _ := m.DB().Exec(`INSERT INTO snapshots
		(root_path, status, polynomial, created_at, committed_at) VALUES (?,?,?,?,?)`,
		"/data", repo.StatusCommitted, 1, ts, ts)
	newID, _ := res.LastInsertId()
	m.DB().Exec(`INSERT INTO entries (snapshot_id, rel_path, kind, mode, mod_time_ns, entry_order)
		VALUES (?,?,?,?,?,?)`, newID, "f", repo.KindFile, 0o644, 0, 0)
	m.DB().Exec(`INSERT INTO chunks (digest, length, created_at) VALUES (?,?,?)`, d, 32, ts)
	m.DB().Exec(`INSERT INTO entry_chunks (snapshot_id, rel_path, chunk_digest, seq)
		VALUES (?,?,?,?)`, newID, "f", d, 0)

	// ApproveBlobDeletion on a row_removed item returns its state unchanged;
	// the engine is responsible for the re-reference check. Emulate it:
	pending, err := m.PendingGCBlobs(job.ID)
	if err != nil || len(pending) != 1 || pending[0].State != repo.GCBlobRowRemoved {
		t.Fatalf("pending blobs = %+v err=%v", pending, err)
	}
	var refs int
	if err := m.DB().QueryRow(`SELECT count(*) FROM entry_chunks WHERE chunk_digest = ?`, d).Scan(&refs); err != nil {
		t.Fatal(err)
	}
	if refs == 0 {
		t.Fatal("expected re-created reference after interruption")
	}
	// The resume path must keep the file; mark it kept_shared as the engine
	// would after seeing refs>0.
	if _, err := m.DB().Exec(`UPDATE gc_job_blobs SET state = ? WHERE job_id = ? AND digest = ? AND state = ?`,
		repo.GCBlobKeptShared, job.ID, d, repo.GCBlobRowRemoved); err != nil {
		t.Fatal(err)
	}
	done, err := m.FinishGCJob(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.BlobsDeleted != 0 || done.BlobsKeptShared != 1 {
		t.Fatalf("resume counters = %+v", done)
	}
	if exists, _ := m.ChunkRowExists(d); !exists {
		t.Fatal("chunk row re-created by concurrent snapshot must survive resume")
	}
}

func TestHoldAppearingAfterFreezeSkipsTarget(t *testing.T) {
	m, cleanup := openManifestStore(t)
	defer cleanup()
	now := time.Now().UTC()
	d := digest(4)
	target := seedCommitted(t, m, now.Add(-30*24*time.Hour), [][]byte{d})
	_, _ = m.AddRetentionRule(0, 1, "")
	job, _ := m.CreateGCJob(now)
	_ = m.StartGCJob(job.ID)

	// Freeze said "candidate"; hold arrives before phase 1 executes.
	if _, err := m.AddHold(target, "late legal hold"); err != nil {
		t.Fatal(err)
	}
	state, err := m.ReclaimSnapshotTarget(job.ID, target)
	if err != nil || state != repo.GCTargetSkippedHold {
		t.Fatalf("state=%s err=%v", state, err)
	}
	snap, _ := m.GetSnapshot(target)
	if snap.Status != repo.StatusCommitted {
		t.Fatalf("held snapshot must stay committed, got %s", snap.Status)
	}
	var refs int
	if err := m.DB().QueryRow(`SELECT count(*) FROM entry_chunks WHERE snapshot_id = ?`, target).Scan(&refs); err != nil {
		t.Fatal(err)
	}
	if refs == 0 {
		t.Fatal("references of hold-skipped snapshot must not be deleted")
	}
	done, err := m.FinishGCJob(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.TargetsDone != 0 || done.TargetsSkipped != 1 {
		t.Fatalf("counters = %+v", done)
	}
	// The chunk is still referenced by the skipped snapshot: not frozen as
	// exclusive at freeze time it WAS, but the blob gate would keep it; the
	// job simply had no blobs because freeze saw it as exclusive — verify the
	// gate keeps it.
	pending, _ := m.PendingGCBlobs(job.ID)
	for _, b := range pending {
		st, _, err := m.ApproveBlobDeletion(job.ID, b.Digest)
		if err != nil {
			t.Fatal(err)
		}
		if st != repo.GCBlobKeptShared {
			t.Fatalf("chunk of hold-skipped snapshot must be kept at gate, got %s", st)
		}
	}
}

func TestActiveGCJobNeverRerunsSucceeded(t *testing.T) {
	m, cleanup := openManifestStore(t)
	defer cleanup()
	now := time.Now().UTC()
	d := digest(5)
	target := seedCommitted(t, m, now.Add(-30*24*time.Hour), [][]byte{d})
	_, _ = m.AddRetentionRule(0, 1, "")
	job, _ := m.CreateGCJob(now)

	active, err := m.ActiveGCJob()
	if err != nil || active == nil || active.ID != job.ID {
		t.Fatalf("active = %+v err=%v", active, err)
	}
	_ = m.StartGCJob(job.ID)
	if _, err := m.ReclaimSnapshotTarget(job.ID, target); err != nil {
		t.Fatal(err)
	}
	st, _, _ := m.ApproveBlobDeletion(job.ID, d)
	if st != repo.GCBlobRowRemoved {
		t.Fatalf("st=%s", st)
	}
	_ = m.FinalizeBlobDeletion(job.ID, d)
	if _, err := m.FinishGCJob(job.ID); err != nil {
		t.Fatal(err)
	}

	active, err = m.ActiveGCJob()
	if err != nil {
		t.Fatal(err)
	}
	if active != nil {
		t.Fatalf("no active job expected after success, got %d/%s", active.ID, active.Status)
	}
	// A second CreateGCJob freezes a fresh, empty job (nothing left to do);
	// the old one stays succeeded with identical counters.
	j2, err := m.CreateGCJob(now)
	if err != nil {
		t.Fatal(err)
	}
	if j2.ID == job.ID || j2.TargetCount != 0 || j2.BlobCount != 0 {
		t.Fatalf("new job should be empty: %+v", j2)
	}
	var n int
	err = m.DB().QueryRow(`SELECT count(*) FROM gc_audit WHERE job_id = ? AND event = 'job_succeeded'`,
		job.ID).Scan(&n)
	if err != nil || n != 1 {
		t.Fatalf("success audit events = %d err=%v", n, err)
	}
}
