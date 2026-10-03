package backup

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"incbackup/internal/repo"
)

// GCFailpoints inject maintenance interruptions for tests and the demo.
type GCFailpoints struct {
	// CrashAfterPhase1 stops the job after all target snapshot references
	// have been deleted but before any blob is cleaned.
	CrashAfterPhase1 bool
	// CrashAfterRowRemoved stops phase 2 after the first blob's chunks row was
	// deleted while its file is still on disk (row_removed window).
	CrashAfterRowRemoved bool
	// BeforePhase2 runs after every target's references have been deleted and
	// before the phase-2 zero-reference gates. It is the real concurrency
	// seam: a snapshot committed here re-references a frozen blob candidate,
	// which the gate must then keep.
	BeforePhase2 func(jobID int64)
	// BeforeBlobUnlink runs while the engine mutex is held, between the
	// "zero refs" gate and the file unlink. It lets tests inspect the
	// row_removed window.
	BeforeBlobUnlink func(digest []byte)
}

// ErrGCSimulatedCrash marks an injected GC interruption. The job persists as
// failed/resumable; the error exists so callers can tell the failpoint from a
// real failure.
var ErrGCSimulatedCrash = errors.New("garbage collection interrupted (simulated crash)")

// gcMu serializes GC jobs against each other. It is deliberately separate
// from Engine.mu: freezing and phase 1 must not block snapshot listing, while
// the destructive per-blob steps take Engine.mu one blob at a time.
func (e *Engine) initGC() {
	e.gcMu = &sync.Mutex{}
}

// GCResult is the outcome of starting, resuming or discovering a GC job.
type GCResult struct {
	JobID          int64      `json:"job_id"`
	Status         string     `json:"status"`
	Resumed        bool       `json:"resumed"`
	AlreadyDone    bool       `json:"already_done"` // resume of a succeeded job: nothing re-ran
	Interrupted    bool       `json:"interrupted,omitempty"`
	RuleVersion    int64      `json:"rule_version"`
	TargetCount    int64      `json:"target_count"`
	BlobCount      int64      `json:"blob_count"`
	TargetsDone    int64      `json:"targets_done"`
	TargetsSkipped int64      `json:"targets_skipped_held"`
	BlobsDeleted   int64      `json:"blobs_deleted"`
	BlobsKept      int64      `json:"blobs_kept_shared"`
	BytesFreed     int64      `json:"bytes_freed"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	Error          string     `json:"error,omitempty"`
}

func gcResultOf(j repo.GCJob, resumed bool) *GCResult {
	return &GCResult{
		JobID:          j.ID,
		Status:         j.Status,
		Resumed:        resumed,
		RuleVersion:    j.RuleVersion,
		TargetCount:    j.TargetCount,
		BlobCount:      j.BlobCount,
		TargetsDone:    j.TargetsDone,
		TargetsSkipped: j.TargetsSkipped,
		BlobsDeleted:   j.BlobsDeleted,
		BlobsKept:      j.BlobsKeptShared,
		BytesFreed:     j.BytesFreed,
		StartedAt:      j.StartedAt,
		FinishedAt:     j.FinishedAt,
		Error:          j.Error,
	}
}

// PreviewRetention evaluates the active rule without freezing or deleting.
func (e *Engine) PreviewRetention(now time.Time) (repo.RetentionPreview, error) {
	rule, err := e.Manifest.LatestRetentionRule()
	if err != nil {
		return repo.RetentionPreview{}, err
	}
	return e.Manifest.EvaluateRetention(rule, now)
}

// RunGC starts a garbage-collection job. It first freezes the target set and
// rule version in one transaction, then executes the two-phase deletion. If an
// unfinished (queued/running/failed) job already exists it is resumed in place
// instead of creating a competing job. A finished job is never re-executed; a
// post returns a freshly frozen new job.
func (e *Engine) RunGC(now time.Time) (*GCResult, error) {
	return e.runGC(now, nil)
}

// RunGCOpts is RunGC with per-call fault injection (tests/demo). The options
// apply only to a freshly frozen job; resuming an interrupted job always runs
// cleanly so an injected crash can never be latched on.
func (e *Engine) RunGCOpts(now time.Time, fail GCFailpoints) (*GCResult, error) {
	return e.runGC(now, &fail)
}

func (e *Engine) runGC(now time.Time, fail *GCFailpoints) (*GCResult, error) {
	if e.gcMu == nil {
		e.initGC()
	}
	e.gcMu.Lock()
	defer e.gcMu.Unlock()

	active, err := e.Manifest.ActiveGCJob()
	if err != nil {
		return nil, err
	}
	if active != nil {
		return e.executeJob(active.ID, true, nil)
	}
	job, err := e.Manifest.CreateGCJob(now)
	if err != nil {
		return nil, err
	}
	return e.executeJob(job.ID, false, fail)
}

// ResumeGC continues a previously interrupted job by id. Resuming a succeeded
// job reports the stored result without touching anything.
func (e *Engine) ResumeGC(jobID int64) (*GCResult, error) {
	if e.gcMu == nil {
		e.initGC()
	}
	e.gcMu.Lock()
	defer e.gcMu.Unlock()

	job, err := e.Manifest.GetGCJob(jobID)
	if err != nil {
		return nil, err
	}
	if job.Status == repo.GCStatusSucceeded {
		r := gcResultOf(job, true)
		r.AlreadyDone = true
		return r, nil
	}
	return e.executeJob(jobID, true, nil)
}

// executeJob drives both phases. It is idempotent: every step is conditional
// on the persisted state, so stopping at any point and re-entering here
// produces the same end result without double deletes or double accounting.
func (e *Engine) executeJob(jobID int64, resumed bool, fail *GCFailpoints) (*GCResult, error) {
	if fail == nil {
		fail = &GCFailpoints{}
	}
	if err := e.Manifest.StartGCJob(jobID); err != nil {
		return nil, err
	}

	// ---- Phase 1: delete snapshot references ------------------------------
	// The engine mutex makes reference deletion atomic with respect to
	// snapshot creation and restore: a restore can never observe a half-
	// reclaimed snapshot, and a pending snapshot keeps its refs until commit.
	e.mu.Lock()
	targets, err := e.Manifest.PendingGCTargets(jobID)
	if err != nil {
		e.mu.Unlock()
		return nil, e.failGC(jobID, err)
	}
	for _, sid := range targets {
		state, err := e.Manifest.ReclaimSnapshotTarget(jobID, sid)
		if err != nil {
			e.mu.Unlock()
			return nil, e.failGC(jobID, fmt.Errorf("phase 1 snapshot %d: %w", sid, err))
		}
		if state == repo.GCTargetSkippedHold {
			// Hold appeared between freeze and execution (or snapshot is no
			// longer committed). Its references stay intact.
			continue
		}
	}
	e.mu.Unlock()

	if fail.CrashAfterPhase1 {
		_ = e.Manifest.AddAudit(&jobID, "job_interrupted", 0, nil,
			"simulated crash after phase 1: references gone, blobs untouched")
		if err := e.Manifest.FailGCJob(jobID, ErrGCSimulatedCrash.Error()+": after phase 1"); err != nil {
			return nil, err
		}
		j, _ := e.Manifest.GetGCJob(jobID)
		r := gcResultOf(j, resumed)
		r.Interrupted = true
		return r, ErrGCSimulatedCrash
	}

	// ---- Phase 2: delete only provably unreferenced blobs ------------------
	// The candidate list is frozen, but each blob is re-gated at deletion
	// time: any entry_chunks reference still present — from an older
	// surviving snapshot, a pending/failed snapshot, or a brand-new snapshot
	// taken concurrently — keeps the file.
	if hook := fail.BeforePhase2; hook != nil {
		hook(jobID)
	}
	blobs, err := e.Manifest.PendingGCBlobs(jobID)
	if err != nil {
		return nil, e.failGC(jobID, err)
	}
	crashOnce := fail.CrashAfterRowRemoved
	for _, b := range blobs {
		if crashOnce && b.State == repo.GCBlobPending {
			// Inject the crash window for exactly one pending blob: run the
			// zero-reference gate in its own transaction, then stop before
			// the file unlink. The blob is now row_removed (catalog row gone,
			// file still on disk) — the state resume must reconcile.
			e.mu.Lock()
			state, _, gerr := e.Manifest.ApproveBlobDeletion(jobID, b.Digest)
			e.mu.Unlock()
			if gerr != nil {
				return nil, e.failGC(jobID, gerr)
			}
			if state == repo.GCBlobRowRemoved {
				crashOnce = false
				_ = e.Manifest.AddAudit(&jobID, "job_interrupted", 0, nil,
					"simulated crash in row_removed window: catalog row gone, file present")
				if ferr := e.Manifest.FailGCJob(jobID, ErrGCSimulatedCrash.Error()+": after blob row removal"); ferr != nil {
					return nil, ferr
				}
				j, _ := e.Manifest.GetGCJob(jobID)
				rr := gcResultOf(j, resumed)
				rr.Interrupted = true
				return rr, ErrGCSimulatedCrash
			}
			// Gate kept it (shared); fall through bookkeeping and continue.
			continue
		}
		r, err := e.processBlob(jobID, b, fail.BeforeBlobUnlink)
		if err != nil {
			return nil, e.failGC(jobID, err)
		}
		_ = r
	}

	job, err := e.Manifest.FinishGCJob(jobID)
	if err != nil {
		return nil, err
	}
	return gcResultOf(job, resumed), nil
}

// processBlob handles one frozen blob candidate. The destructive gate-to-
// unlink sequence runs under e.mu so a snapshot being created concurrently
// has either fully committed its references before the gate (blob kept) or
// starts only after the unlink, in which case ContentStore.Put re-creates the
// blob from scratch — never a dangling reference.
func (e *Engine) processBlob(jobID int64, b repo.GCBlob, beforeUnlink func([]byte)) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if b.State == repo.GCBlobRowRemoved {
		// Resume from the crash window: a concurrent new snapshot may have
		// re-created the chunk row and references since.
		var refs int
		if err := e.Manifest.DB().QueryRow(
			`SELECT count(*) FROM entry_chunks WHERE chunk_digest = ?`, b.Digest).Scan(&refs); err != nil {
			return "", err
		}
		if refs > 0 {
			if err := e.reattachBlob(jobID, b); err != nil {
				return "", err
			}
			return repo.GCBlobKeptShared, nil
		}
		if beforeUnlink != nil {
			beforeUnlink(b.Digest)
		}
		if err := e.Store.Remove(b.Digest); err != nil {
			return "", fmt.Errorf("remove blob %x: %w", b.Digest, err)
		}
		if err := e.Manifest.FinalizeBlobDeletion(jobID, b.Digest); err != nil {
			return "", err
		}
		return repo.GCBlobDeleted, nil
	}

	state, _, err := e.Manifest.ApproveBlobDeletion(jobID, b.Digest)
	if err != nil {
		return state, err
	}
	switch state {
	case repo.GCBlobKeptShared, repo.GCBlobDeleted:
		return state, nil // settled by an earlier attempt
	case repo.GCBlobPending:
		return state, nil // should not happen; nothing was done
	}

	// state == row_removed: zero refs confirmed in this transaction; the
	// chunks catalog row is already gone. Finish with the file unlink.
	if beforeUnlink != nil {
		beforeUnlink(b.Digest)
	}
	if err := e.Store.Remove(b.Digest); err != nil {
		return "", fmt.Errorf("remove blob %x: %w", b.Digest, err)
	}
	if err := e.Manifest.FinalizeBlobDeletion(jobID, b.Digest); err != nil {
		return "", err
	}
	return repo.GCBlobDeleted, nil
}

// reattachBlob reverses a row_removed blob that turned out to be referenced
// again: the file was never deleted, and the chunks row was re-created by the
// concurrent snapshot; mark the frozen item kept_shared.
func (e *Engine) reattachBlob(jobID int64, b repo.GCBlob) error {
	_, err := e.Manifest.DB().Exec(`UPDATE gc_job_blobs SET state = ?
		WHERE job_id = ? AND digest = ? AND state = ?`,
		repo.GCBlobKeptShared, jobID, b.Digest, repo.GCBlobRowRemoved)
	if err != nil {
		return err
	}
	jid := jobID
	return e.Manifest.AddAudit(&jid, "blob_kept_shared", 0, b.Digest,
		"row re-created and referenced after interruption; file kept")
}

func (e *Engine) failGC(jobID int64, cause error) error {
	_ = e.Manifest.FailGCJob(jobID, cause.Error())
	return cause
}
