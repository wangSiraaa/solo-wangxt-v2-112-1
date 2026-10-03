package repo

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// GC job lifecycle states.
const (
	GCStatusQueued    = "queued"    // frozen, never executed
	GCStatusRunning   = "running"   // phase 1/2 in progress
	GCStatusSucceeded = "succeeded" // finished; must never run again
	GCStatusFailed    = "failed"    // interrupted; resumable
)

// GC target (snapshot) states.
const (
	GCTargetPending     = "pending"
	GCTargetDone        = "done"
	GCTargetSkippedHold = "skipped_held"
)

// GC blob states track the two-phase deletion:
//
//	pending -> row_removed -> deleted
//	pending -> kept_shared   (still referenced at delete time)
//
// row_removed is the crash window: catalog row gone, file still on disk.
const (
	GCBlobPending    = "pending"
	GCBlobRowRemoved = "row_removed"
	GCBlobDeleted    = "deleted"
	GCBlobKeptShared = "kept_shared"
)

// GCJob is the persisted status of one garbage-collection run.
type GCJob struct {
	ID              int64
	RuleVersion     int64
	Status          string
	FrozenAt        time.Time
	StartedAt       *time.Time
	FinishedAt      *time.Time
	Error           string
	TargetCount     int64
	BlobCount       int64
	BlobsDeleted    int64
	BlobsKeptShared int64
	BytesFreed      int64
	TargetsDone     int64
	TargetsSkipped  int64
}

// GCTarget is a frozen job target.
type GCTarget struct {
	SnapshotID  int64
	State       string
	Reason      string
	CommittedAt string
}

// GCBlob is a frozen blob candidate with its current processing state.
type GCBlob struct {
	Digest []byte
	Length int64
	State  string
}

// GCAuditEvent is one row of the append-only GC audit trail.
type GCAuditEvent struct {
	ID         int64
	JobID      *int64
	Event      string
	SnapshotID *int64
	Digest     []byte
	Detail     string
	CreatedAt  time.Time
}

type querier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

// CreateGCJob freezes a job atomically: it snapshots the active retention-rule
// version, the exact target snapshot set and the exact exclusive-blob set in
// one transaction. Everything the run later does is driven by these frozen
// rows, so rule changes or new holds after this point cannot move the goal
// posts.
func (m *Manifest) CreateGCJob(now time.Time) (GCJob, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return GCJob{}, err
	}
	defer tx.Rollback()

	rule, err := latestRuleTx(tx)
	if err != nil {
		return GCJob{}, err
	}
	sel, err := selectRetentionCandidates(tx, rule, now)
	if err != nil {
		return GCJob{}, err
	}
	ids := candidateIDs(sel.candidates)
	blobs, err := exclusiveChunksQ(tx, ids)
	if err != nil {
		return GCJob{}, err
	}
	var blobBytes int64
	for _, b := range blobs {
		blobBytes += b.Length
	}

	res, err := tx.Exec(`INSERT INTO gc_jobs
		(rule_version, status, frozen_at, target_count, blob_count)
		VALUES (?,?,?,?,?)`,
		rule.Version, GCStatusQueued, now.UTC().Format(time.RFC3339Nano),
		len(ids), len(blobs))
	if err != nil {
		return GCJob{}, fmt.Errorf("create gc job: %w", err)
	}
	jobID, _ := res.LastInsertId()

	for _, c := range sel.candidates {
		if _, err := tx.Exec(`INSERT INTO gc_job_targets
			(job_id, snapshot_id, state, committed_at) VALUES (?,?,?,?)`,
			jobID, c.SnapshotID, GCTargetPending,
			c.CommittedAt.UTC().Format(time.RFC3339Nano)); err != nil {
			return GCJob{}, err
		}
	}
	for _, b := range blobs {
		if _, err := tx.Exec(`INSERT INTO gc_job_blobs (job_id, digest, length, state)
			VALUES (?,?,?,?)`, jobID, b.Digest, b.Length, GCBlobPending); err != nil {
			return GCJob{}, err
		}
	}
	if err := addAuditTx(tx, &jobID, "job_created", 0, nil,
		fmt.Sprintf("rule_version=%d targets=%d exclusive_blobs=%d exclusive_bytes=%d held_snapshots_skipped=%d",
			rule.Version, len(ids), len(blobs), blobBytes, len(sel.skippedHeld))); err != nil {
		return GCJob{}, err
	}
	if err := tx.Commit(); err != nil {
		return GCJob{}, err
	}
	return m.GetGCJob(jobID)
}

func latestRuleTx(tx *sql.Tx) (RetentionRule, error) {
	row := tx.QueryRow(`SELECT version, keep_last_n, max_age_days, created_at, note
		FROM retention_rules ORDER BY version DESC LIMIT 1`)
	r, err := scanRetentionRule(row)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNoRetentionRule
	}
	return r, err
}

// ActiveGCJob returns the newest job that never succeeded (queued/running/
// failed). A failed/queued job is resumed instead of replaced; a succeeded job
// is never re-executed.
func (m *Manifest) ActiveGCJob() (*GCJob, error) {
	row := m.db.QueryRow(`SELECT `+gcJobCols+` FROM gc_jobs
		WHERE status != ? ORDER BY id DESC LIMIT 1`, GCStatusSucceeded)
	j, err := scanGCJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &j, nil
}

const gcJobCols = `id, rule_version, status, frozen_at, started_at, finished_at,
	error, target_count, blob_count, blobs_deleted, blobs_kept_shared, bytes_freed`

func scanGCJob(row interface {
	Scan(...any) error
}) (GCJob, error) {
	var j GCJob
	var frozen, started, finished sql.NullString
	if err := row.Scan(&j.ID, &j.RuleVersion, &j.Status, &frozen, &started, &finished,
		&j.Error, &j.TargetCount, &j.BlobCount, &j.BlobsDeleted, &j.BlobsKeptShared,
		&j.BytesFreed); err != nil {
		return j, err
	}
	j.FrozenAt, _ = time.Parse(time.RFC3339Nano, frozen.String)
	if started.Valid {
		t, _ := time.Parse(time.RFC3339Nano, started.String)
		j.StartedAt = &t
	}
	if finished.Valid {
		t, _ := time.Parse(time.RFC3339Nano, finished.String)
		j.FinishedAt = &t
	}
	return j, nil
}

// GetGCJob fetches one job with progress counters recomputed from the frozen
// child tables, so counters can never double-count after a resume.
func (m *Manifest) GetGCJob(id int64) (GCJob, error) {
	row := m.db.QueryRow(`SELECT `+gcJobCols+` FROM gc_jobs WHERE id = ?`, id)
	j, err := scanGCJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return j, ErrNotFound
	}
	if err != nil {
		return j, err
	}
	if err := m.fillGCStats(&j); err != nil {
		return j, err
	}
	return j, nil
}

// ListGCJobs returns recent jobs, newest first.
func (m *Manifest) ListGCJobs(limit int) ([]GCJob, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := m.db.Query(`SELECT `+gcJobCols+` FROM gc_jobs ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	var out []GCJob
	for rows.Next() {
		j, err := scanGCJob(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, j)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Stats run separate queries: only after the listing connection is freed
	// (SetMaxOpenConns(1)) to avoid a self-deadlock.
	for i := range out {
		if err := m.fillGCStats(&out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// fillGCStats recomputes progress from gc_job_targets/gc_job_blobs. The
// gc_jobs counter columns are only written once, at finalization.
func (m *Manifest) fillGCStats(j *GCJob) error {
	err := m.db.QueryRow(`SELECT count(*) FROM gc_job_targets WHERE job_id = ? AND state = ?`,
		j.ID, GCTargetDone).Scan(&j.TargetsDone)
	if err != nil {
		return err
	}
	err = m.db.QueryRow(`SELECT count(*) FROM gc_job_targets WHERE job_id = ? AND state = ?`,
		j.ID, GCTargetSkippedHold).Scan(&j.TargetsSkipped)
	if err != nil {
		return err
	}
	err = m.db.QueryRow(`SELECT count(*) FROM gc_job_blobs WHERE job_id = ? AND state = ?`,
		j.ID, GCBlobDeleted).Scan(&j.BlobsDeleted)
	if err != nil {
		return err
	}
	err = m.db.QueryRow(`SELECT count(*) FROM gc_job_blobs WHERE job_id = ? AND state = ?`,
		j.ID, GCBlobKeptShared).Scan(&j.BlobsKeptShared)
	if err != nil {
		return err
	}
	return m.db.QueryRow(`SELECT coalesce(sum(length),0) FROM gc_job_blobs
		WHERE job_id = ? AND state = ?`, j.ID, GCBlobDeleted).Scan(&j.BytesFreed)
}

// ListGCTargets returns the frozen targets of a job.
func (m *Manifest) ListGCTargets(jobID int64) ([]GCTarget, error) {
	rows, err := m.db.Query(`SELECT snapshot_id, state, reason, committed_at
		FROM gc_job_targets WHERE job_id = ? ORDER BY snapshot_id`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GCTarget
	for rows.Next() {
		var t GCTarget
		if err := rows.Scan(&t.SnapshotID, &t.State, &t.Reason, &t.CommittedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ListGCBlobs returns frozen blob candidates with states (tests/audit).
func (m *Manifest) ListGCBlobs(jobID int64) ([]GCBlob, error) {
	rows, err := m.db.Query(`SELECT digest, length, state FROM gc_job_blobs
		WHERE job_id = ? ORDER BY digest`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GCBlob
	for rows.Next() {
		var b GCBlob
		if err := rows.Scan(&b.Digest, &b.Length, &b.State); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// PendingGCTargets returns targets not processed yet, oldest snapshot first.
func (m *Manifest) PendingGCTargets(jobID int64) ([]int64, error) {
	rows, err := m.db.Query(`SELECT snapshot_id FROM gc_job_targets
		WHERE job_id = ? AND state = ? ORDER BY snapshot_id`, jobID, GCTargetPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// PendingGCBlobs returns (digest,length,state) of blobs still needing work:
// "pending" candidates and "row_removed" stragglers from a crash between
// catalog delete and file unlink.
func (m *Manifest) PendingGCBlobs(jobID int64) ([]GCBlob, error) {
	rows, err := m.db.Query(`SELECT digest, length, state FROM gc_job_blobs
		WHERE job_id = ? AND state IN (?,?) ORDER BY digest`,
		jobID, GCBlobPending, GCBlobRowRemoved)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GCBlob
	for rows.Next() {
		var b GCBlob
		if err := rows.Scan(&b.Digest, &b.Length, &b.State); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// StartGCJob flips queued/failed -> running exactly once per transition. A
// succeeded job cannot be restarted.
func (m *Manifest) StartGCJob(jobID int64) error {
	res, err := m.db.Exec(`UPDATE gc_jobs SET status = ?, started_at = COALESCE(started_at, ?),
		error = '' WHERE id = ? AND status != ?`,
		GCStatusRunning, nowText(), jobID, GCStatusSucceeded)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		var status string
		_ = m.db.QueryRow(`SELECT status FROM gc_jobs WHERE id = ?`, jobID).Scan(&status)
		if status == GCStatusSucceeded {
			return fmt.Errorf("gc job %d already succeeded and must not be re-run", jobID)
		}
	}
	return nil
}

// FailGCJob marks a non-succeeded job failed with the interruption reason.
func (m *Manifest) FailGCJob(jobID int64, cause string) error {
	_, err := m.db.Exec(`UPDATE gc_jobs SET status = ?, error = ?, finished_at = ?
		WHERE id = ? AND status != ?`,
		GCStatusFailed, cause, nowText(), jobID, GCStatusSucceeded)
	return err
}

// FinishGCJob transitions a running job to succeeded and writes the result
// counters once, derived solely from the frozen child tables. The conditional
// WHERE makes a duplicate finish a no-op.
func (m *Manifest) FinishGCJob(jobID int64) (GCJob, error) {
	_, err := m.db.Exec(`UPDATE gc_jobs SET
		status = ?,
		finished_at = ?,
		blobs_deleted = (SELECT count(*) FROM gc_job_blobs WHERE job_id = ? AND state = ?),
		blobs_kept_shared = (SELECT count(*) FROM gc_job_blobs WHERE job_id = ? AND state = ?),
		bytes_freed = (SELECT coalesce(sum(length),0) FROM gc_job_blobs WHERE job_id = ? AND state = ?)
		WHERE id = ? AND status = ?`,
		GCStatusSucceeded, nowText(),
		jobID, GCBlobDeleted,
		jobID, GCBlobKeptShared,
		jobID, GCBlobDeleted,
		jobID, GCStatusRunning)
	if err != nil {
		return GCJob{}, err
	}
	j, err := m.GetGCJob(jobID)
	if err != nil {
		return j, err
	}
	_ = m.AddAudit(&jobID, "job_succeeded", 0, nil,
		fmt.Sprintf("targets_done=%d targets_skipped_held=%d blobs_deleted=%d blobs_kept_shared=%d bytes_freed=%d",
			j.TargetsDone, j.TargetsSkipped, j.BlobsDeleted, j.BlobsKeptShared, j.BytesFreed))
	return j, nil
}

// ReclaimSnapshotTarget executes phase 1 for one frozen target in a single
// transaction. It re-confirms immediately before deleting that:
//   - the target row is still "pending" (resume skips processed rows),
//   - no hold was added after the freeze (target becomes "skipped_held"),
//   - the snapshot is still committed (pending/failed are never reclaimed).
//
// Then its entry_chunks and entries are deleted and the snapshot becomes a
// "reclaimed" tombstone. snapshot_errors rows are deliberately left intact.
func (m *Manifest) ReclaimSnapshotTarget(jobID, snapshotID int64) (string, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	var state string
	if err := tx.QueryRow(`SELECT state FROM gc_job_targets
		WHERE job_id = ? AND snapshot_id = ?`, jobID, snapshotID).Scan(&state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	if state != GCTargetPending {
		return state, nil // already processed: idempotent resume
	}

	var holdN int
	if err := tx.QueryRow(`SELECT count(*) FROM snapshot_holds
		WHERE snapshot_id = ? AND released_at IS NULL`, snapshotID).Scan(&holdN); err != nil {
		return "", err
	}
	if holdN > 0 {
		if _, err := tx.Exec(`UPDATE gc_job_targets SET state = ?, reason = ?
			WHERE job_id = ? AND snapshot_id = ?`,
			GCTargetSkippedHold, "hold added after job freeze", jobID, snapshotID); err != nil {
			return "", err
		}
		if err := addAuditTx(tx, &jobID, "target_skipped_held", snapshotID, nil,
			"active hold present at phase 1"); err != nil {
			return "", err
		}
		return GCTargetSkippedHold, tx.Commit()
	}

	var snapStatus string
	if err := tx.QueryRow(`SELECT status FROM snapshots WHERE id = ?`, snapshotID).Scan(&snapStatus); err != nil {
		return "", err
	}
	if snapStatus != StatusCommitted {
		// Defensive: the freeze only selected committed, un-held snapshots.
		if _, err := tx.Exec(`UPDATE gc_job_targets SET state = ?, reason = ?
			WHERE job_id = ? AND snapshot_id = ?`,
			GCTargetSkippedHold, "snapshot status is "+snapStatus, jobID, snapshotID); err != nil {
			return "", err
		}
		if err := addAuditTx(tx, &jobID, "target_skipped_held", snapshotID, nil,
			"snapshot no longer committed at phase 1"); err != nil {
			return "", err
		}
		return GCTargetSkippedHold, tx.Commit()
	}

	if _, err := tx.Exec(`DELETE FROM entry_chunks WHERE snapshot_id = ?`, snapshotID); err != nil {
		return "", err
	}
	if _, err := tx.Exec(`DELETE FROM entries WHERE snapshot_id = ?`, snapshotID); err != nil {
		return "", err
	}
	if _, err := tx.Exec(`UPDATE snapshots SET status = ? WHERE id = ? AND status = ?`,
		StatusReclaimed, snapshotID, StatusCommitted); err != nil {
		return "", err
	}
	if _, err := tx.Exec(`UPDATE gc_job_targets SET state = ?
		WHERE job_id = ? AND snapshot_id = ?`,
		GCTargetDone, jobID, snapshotID); err != nil {
		return "", err
	}
	if err := addAuditTx(tx, &jobID, "target_reclaimed", snapshotID, nil,
		"snapshot references deleted (phase 1)"); err != nil {
		return "", err
	}
	return GCTargetDone, tx.Commit()
}

// ApproveBlobDeletion is the phase-2 gate for one frozen blob candidate.
// Inside one transaction it re-checks that NO entry_chunks row of any snapshot
// (committed, pending or failed) still references the digest.
//
//   - unreferenced: the chunks catalog row is deleted, item -> row_removed.
//     The caller must unlink the file, then call FinalizeBlobDeletion.
//   - still referenced (shared with a surviving/new snapshot): item ->
//     kept_shared, nothing is deleted.
//   - already processed: its stored state is returned unchanged.
func (m *Manifest) ApproveBlobDeletion(jobID int64, digest []byte) (string, int64, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return "", 0, err
	}
	defer tx.Rollback()

	var state string
	var length int64
	if err := tx.QueryRow(`SELECT state, length FROM gc_job_blobs
		WHERE job_id = ? AND digest = ?`, jobID, digest).Scan(&state, &length); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", 0, ErrNotFound
		}
		return "", 0, err
	}
	if state != GCBlobPending {
		return state, length, nil // row_removed on resume, deleted/kept_shared otherwise
	}

	var refs int
	if err := tx.QueryRow(`SELECT count(*) FROM entry_chunks WHERE chunk_digest = ?`,
		digest).Scan(&refs); err != nil {
		return "", 0, err
	}
	if refs > 0 {
		if _, err := tx.Exec(`UPDATE gc_job_blobs SET state = ?
			WHERE job_id = ? AND digest = ?`, GCBlobKeptShared, jobID, digest); err != nil {
			return "", 0, err
		}
		if err := addAuditTx(tx, &jobID, "blob_kept_shared", 0, digest,
			fmt.Sprintf("still referenced by %d entry row(s); file kept", refs)); err != nil {
			return "", 0, err
		}
		return GCBlobKeptShared, length, tx.Commit()
	}

	if _, err := tx.Exec(`DELETE FROM chunks WHERE digest = ?`, digest); err != nil {
		return "", 0, err
	}
	if _, err := tx.Exec(`UPDATE gc_job_blobs SET state = ?
		WHERE job_id = ? AND digest = ?`, GCBlobRowRemoved, jobID, digest); err != nil {
		return "", 0, err
	}
	return GCBlobRowRemoved, length, tx.Commit()
}

// FinalizeBlobDeletion confirms the file unlink by moving row_removed ->
// deleted. Counters are derived at job finalization, so this is safe to repeat
// and cannot double-count.
func (m *Manifest) FinalizeBlobDeletion(jobID int64, digest []byte) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`UPDATE gc_job_blobs SET state = ?
		WHERE job_id = ? AND digest = ? AND state = ?`,
		GCBlobDeleted, jobID, digest, GCBlobRowRemoved)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		var length int64
		if err := tx.QueryRow(`SELECT length FROM gc_job_blobs WHERE job_id = ? AND digest = ?`,
			jobID, digest).Scan(&length); err != nil {
			return err
		}
		if err := addAuditTx(tx, &jobID, "blob_deleted", 0, digest,
			fmt.Sprintf("catalog row and blob file removed, %d bytes", length)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// AddAudit appends an event. jobID may be nil; snapshotID 0 means NULL.
func (m *Manifest) AddAudit(jobID *int64, event string, snapshotID int64, digest []byte, detail string) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := addAuditTx(tx, jobID, event, snapshotID, digest, detail); err != nil {
		return err
	}
	return tx.Commit()
}

func addAuditTx(tx *sql.Tx, jobID *int64, event string, snapshotID int64, digest []byte, detail string) error {
	var sid any
	if snapshotID != 0 {
		sid = snapshotID
	}
	_, err := tx.Exec(`INSERT INTO gc_audit
		(job_id, event, snapshot_id, digest, detail, created_at)
		VALUES (?,?,?,?,?,?)`, jobID, event, sid, digest, detail, nowText())
	return err
}

// ListAudit returns audit events, newest last (append order). When jobID > 0
// the list is restricted to that job; otherwise all events are returned.
func (m *Manifest) ListAudit(jobID int64, limit int) ([]GCAuditEvent, error) {
	if limit <= 0 {
		limit = 1000
	}
	var rows *sql.Rows
	var err error
	if jobID > 0 {
		rows, err = m.db.Query(`SELECT id, job_id, event, snapshot_id, digest, detail, created_at
			FROM gc_audit WHERE job_id = ? ORDER BY id LIMIT ?`, jobID, limit)
	} else {
		rows, err = m.db.Query(`SELECT id, job_id, event, snapshot_id, digest, detail, created_at
			FROM gc_audit ORDER BY id LIMIT ?`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GCAuditEvent
	for rows.Next() {
		var ev GCAuditEvent
		var job, snap sql.NullInt64
		var created string
		if err := rows.Scan(&ev.ID, &job, &ev.Event, &snap, &ev.Digest, &ev.Detail, &created); err != nil {
			return nil, err
		}
		if job.Valid {
			v := job.Int64
			ev.JobID = &v
		}
		if snap.Valid {
			v := snap.Int64
			ev.SnapshotID = &v
		}
		ev.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		out = append(out, ev)
	}
	return out, rows.Err()
}
