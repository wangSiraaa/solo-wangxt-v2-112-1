package backup

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"incbackup/internal/repo"
)

// GCSelector picks the retention rule for a preview or an execution: either a
// stored rule by name (its current version is frozen into the job) or inline
// keep_last/keep_days parameters.
type GCSelector struct {
	RuleName string
	KeepLast int
	KeepDays int
}

// GCTargetInfo describes one snapshot a rule would reclaim.
type GCTargetInfo struct {
	SnapshotID  int64
	RootPath    string
	BytesTotal  int64
	ChunksRef   int64
	CommittedAt *time.Time
}

// GCProtectedInfo is a would-be target spared by a protect flag.
type GCProtectedInfo struct {
	SnapshotID int64
	Reason     string
}

// GCNotCommitted is a snapshot GC never touches, listed for transparency.
type GCNotCommitted struct {
	SnapshotID int64
	Status     string
}

// GCPreview is the dry-run result of a retention rule: exactly what an
// execution of the same rule would freeze, before anything is deleted.
type GCPreview struct {
	Rule             repo.RetentionRule
	Targets          []GCTargetInfo
	SkippedProtected []GCProtectedInfo
	KeptByRule       []int64
	NotCommitted     []GCNotCommitted
	CandidateBlobs   int
	CandidateBytes   int64
	ReclaimableBlobs int
	ReclaimableBytes int64
}

// resolveRule turns a selector into a concrete rule. Inline rules get
// version 0; named rules are loaded at their current version.
func (e *Engine) resolveRule(sel GCSelector) (repo.RetentionRule, error) {
	if sel.RuleName != "" {
		rule, err := e.Manifest.GetRetentionRule(sel.RuleName)
		if err != nil {
			return repo.RetentionRule{}, err
		}
		return *rule, nil
	}
	if sel.KeepLast < 0 || sel.KeepDays < 0 {
		return repo.RetentionRule{}, fmt.Errorf("keep_last and keep_days must be >= 0")
	}
	if sel.KeepLast == 0 && sel.KeepDays == 0 {
		return repo.RetentionRule{}, fmt.Errorf("retention rule keeps nothing: keep_last or keep_days must be > 0")
	}
	return repo.RetentionRule{KeepLast: sel.KeepLast, KeepDays: sel.KeepDays}, nil
}

// retentionEval is the outcome of applying a rule to the current catalog.
type retentionEval struct {
	targets          []repo.SnapshotInfo // committed, not kept, not protected; oldest first
	skippedProtected []GCProtectedInfo   // committed, not kept, but protected
	kept             []int64             // committed and retained by the rule
	notCommitted     []GCNotCommitted    // pending/failed: never GC candidates
}

// evalRetention applies a rule to the catalog. Only committed snapshots are
// eligible; pending and failed snapshots (and their diagnostics) are never
// collected. keep_last counts the newest committed snapshots; keep_days keeps
// committed snapshots younger than the cutoff.
func (e *Engine) evalRetention(rule repo.RetentionRule) (*retentionEval, error) {
	snaps, err := e.Manifest.ListSnapshots()
	if err != nil {
		return nil, err
	}
	protected, err := e.Manifest.ProtectedIDs()
	if err != nil {
		return nil, err
	}
	eval := &retentionEval{}
	var committed []repo.SnapshotInfo
	for _, s := range snaps {
		if s.Status == repo.StatusCommitted {
			committed = append(committed, s) // ListSnapshots is newest-first
		} else {
			eval.notCommitted = append(eval.notCommitted, GCNotCommitted{SnapshotID: s.ID, Status: s.Status})
		}
	}
	kept := map[int64]bool{}
	if rule.KeepLast > 0 {
		for i := 0; i < rule.KeepLast && i < len(committed); i++ {
			kept[committed[i].ID] = true // committed[0] is the newest
		}
	}
	if rule.KeepDays > 0 {
		cutoff := time.Now().Add(-time.Duration(rule.KeepDays) * 24 * time.Hour)
		for _, s := range committed {
			at := s.CreatedAt
			if s.CommittedAt != nil {
				at = *s.CommittedAt
			}
			if at.After(cutoff) {
				kept[s.ID] = true
			}
		}
	}
	for _, s := range committed {
		if kept[s.ID] {
			eval.kept = append(eval.kept, s.ID)
			continue
		}
		if reason, ok := protected[s.ID]; ok {
			eval.skippedProtected = append(eval.skippedProtected,
				GCProtectedInfo{SnapshotID: s.ID, Reason: reason})
			continue
		}
		eval.targets = append(eval.targets, s)
	}
	// Process oldest first; kept ids ascending for a stable report.
	for i, j := 0, len(eval.targets)-1; i < j; i, j = i+1, j-1 {
		eval.targets[i], eval.targets[j] = eval.targets[j], eval.targets[i]
	}
	sort.Slice(eval.kept, func(i, j int) bool { return eval.kept[i] < eval.kept[j] })
	return eval, nil
}

// candidateChunks collects the distinct chunks referenced by the targets.
func (e *Engine) candidateChunks(targets []repo.SnapshotInfo) ([]repo.ChunkRef, error) {
	seen := map[string]bool{}
	var out []repo.ChunkRef
	for _, t := range targets {
		refs, err := e.Manifest.ReferencedChunks(t.ID)
		if err != nil {
			return nil, err
		}
		for _, c := range refs {
			key := string(c.Digest)
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, c)
		}
	}
	return out, nil
}

// PreviewGC evaluates a rule without changing anything: the exact target set
// an execution would freeze, plus how many of the candidate blobs are shared
// with snapshots outside the target set (and would therefore be kept).
func (e *Engine) PreviewGC(sel GCSelector) (*GCPreview, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	rule, err := e.resolveRule(sel)
	if err != nil {
		return nil, err
	}
	eval, err := e.evalRetention(rule)
	if err != nil {
		return nil, err
	}
	p := &GCPreview{
		Rule:             rule,
		SkippedProtected: eval.skippedProtected,
		KeptByRule:       eval.kept,
		NotCommitted:     eval.notCommitted,
	}
	inTarget := map[int64]bool{}
	for _, t := range eval.targets {
		inTarget[t.ID] = true
		p.Targets = append(p.Targets, GCTargetInfo{
			SnapshotID:  t.ID,
			RootPath:    t.RootPath,
			BytesTotal:  t.BytesTotal,
			ChunksRef:   t.ChunksRef,
			CommittedAt: t.CommittedAt,
		})
	}
	chunks, err := e.candidateChunks(eval.targets)
	if err != nil {
		return nil, err
	}
	p.CandidateBlobs = len(chunks)
	for _, c := range chunks {
		p.CandidateBytes += c.Length
		refs, err := e.Manifest.ChunkReferencers(c.Digest)
		if err != nil {
			return nil, err
		}
		shared := false
		for _, id := range refs {
			if !inTarget[id] {
				shared = true
				break
			}
		}
		if !shared {
			p.ReclaimableBlobs++
			p.ReclaimableBytes += c.Length
		}
	}
	return p, nil
}

// CreateGCJob freezes a new GC job: the rule version, the target set and the
// candidate chunk set are persisted before anything is deleted. Running the
// job is a separate step (RunGCJob), so an interrupted job can be resumed
// against exactly this frozen state.
func (e *Engine) CreateGCJob(sel GCSelector) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	rule, err := e.resolveRule(sel)
	if err != nil {
		return 0, err
	}
	eval, err := e.evalRetention(rule)
	if err != nil {
		return 0, err
	}
	params, err := json.Marshal(map[string]any{
		"name": rule.Name, "version": rule.Version,
		"keep_last": rule.KeepLast, "keep_days": rule.KeepDays,
	})
	if err != nil {
		return 0, err
	}
	var targets []repo.GCJobTarget
	for _, t := range eval.targets {
		committed := ""
		if t.CommittedAt != nil {
			committed = t.CommittedAt.UTC().Format(time.RFC3339Nano)
		}
		targets = append(targets, repo.GCJobTarget{
			SnapshotID:  t.ID,
			RootPath:    t.RootPath,
			BytesTotal:  t.BytesTotal,
			CommittedAt: committed,
		})
	}
	refs, err := e.candidateChunks(eval.targets)
	if err != nil {
		return 0, err
	}
	chunks := make([]repo.GCJobChunk, 0, len(refs))
	for _, c := range refs {
		chunks = append(chunks, repo.GCJobChunk{Digest: c.Digest, Length: c.Length})
	}
	return e.Manifest.CreateGCJob(rule, string(params), targets, chunks)
}

// RunGCJob executes (or resumes) a frozen job. It is idempotent: every step
// checks persisted per-target/per-chunk state, so calling it again after an
// interruption continues where the job stopped, and calling it on a completed
// job is a no-op. The whole run holds the engine mutex, which serializes it
// against snapshot creation — a concurrent snapshot's references are either
// fully visible to the re-confirmation check or not yet written, and its
// blobs can never be swept from under it.
//
// Phase 1 deletes each target snapshot's manifest references in its own
// transaction. Phase 2 re-confirms, for every frozen candidate chunk, that no
// entry_chunks row references it anymore, and only then deletes the blob.
// stopAfterRefs is a failpoint that halts the job between the phases,
// simulating a crash with references gone but blobs not yet swept.
func (e *Engine) RunGCJob(jobID int64, stopAfterRefs bool) (*repo.GCJob, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	job, err := e.Manifest.GetGCJob(jobID)
	if err != nil {
		return nil, err
	}
	switch job.Status {
	case repo.GCJobCompleted:
		return job, nil // terminal: never re-execute finished work
	case repo.GCJobFailed:
		// Manual resume of a failed job; per-step state makes it idempotent.
		if err := e.Manifest.ReopenGCJob(jobID); err != nil {
			return nil, err
		}
	}

	resumed := job.SnapshotsDone > 0 || job.BlobsDeleted > 0 || job.Status == repo.GCJobRefsDeleted
	action := "job_started"
	if resumed {
		action = "job_resumed"
	}
	_ = e.Manifest.AddGCEvent(jobID, nil, nil, action,
		fmt.Sprintf("status=%s snapshots_done=%d/%d", job.Status, job.SnapshotsDone, job.SnapshotsTotal))

	fail := func(err error) (*repo.GCJob, error) {
		_ = e.Manifest.FailGCJob(jobID, err.Error())
		return nil, err
	}

	// Phase 1: drop the frozen targets' snapshot references.
	targets, err := e.Manifest.PendingGCJobTargets(jobID)
	if err != nil {
		return fail(err)
	}
	for _, t := range targets {
		if _, err := e.Manifest.DeleteSnapshotRefs(jobID, t.SnapshotID); err != nil {
			return fail(err)
		}
	}
	if err := e.Manifest.MarkGCJobRefsDeleted(jobID); err != nil {
		return fail(err)
	}

	if stopAfterRefs {
		_ = e.Manifest.AddGCEvent(jobID, nil, nil, "job_interrupted",
			"failpoint: stopped after reference deletion, blobs not yet swept")
		return e.Manifest.GetGCJob(jobID)
	}

	// Phase 2: sweep candidate blobs, but only after re-confirming that no
	// entry_chunks row anywhere still references the chunk. Chunks shared
	// with live (or pending, or failed) snapshots are kept.
	chunks, err := e.Manifest.PendingGCJobChunks(jobID)
	if err != nil {
		return fail(err)
	}
	for _, c := range chunks {
		n, err := e.Manifest.GCChunkRefcount(c.Digest)
		if err != nil {
			return fail(err)
		}
		if n > 0 {
			if err := e.Manifest.MarkGCChunkKept(jobID, c.Digest); err != nil {
				return fail(err)
			}
			continue
		}
		if err := e.Store.Remove(c.Digest); err != nil { // idempotent
			return fail(err)
		}
		if err := e.Manifest.MarkGCChunkDeleted(jobID, c.Digest, c.Length); err != nil {
			return fail(err)
		}
	}
	if err := e.Manifest.CompleteGCJob(jobID); err != nil {
		return fail(err)
	}
	return e.Manifest.GetGCJob(jobID)
}

// ResumeGCJobs finishes jobs that were interrupted between phases (process
// restart). Failed jobs are left for an explicit manual resume.
func (e *Engine) ResumeGCJobs() ([]int64, error) {
	jobs, err := e.Manifest.UnfinishedGCJobs()
	if err != nil {
		return nil, err
	}
	var done []int64
	for _, j := range jobs {
		if _, err := e.RunGCJob(j.ID, false); err != nil {
			return done, fmt.Errorf("resume gc job %d: %w", j.ID, err)
		}
		done = append(done, j.ID)
	}
	return done, nil
}

// IsGCNotFound reports whether err is a missing rule/job lookup.
func IsGCNotFound(err error) bool { return errors.Is(err, repo.ErrNotFound) }
