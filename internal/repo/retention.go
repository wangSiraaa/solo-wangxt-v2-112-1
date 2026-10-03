package repo

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// GC job status values. A job freezes its rule version, target set and
// candidate chunk set before deleting anything, then runs two phases:
// planned (phase 1: dropping snapshot references) -> refs_deleted (phase 2:
// sweeping now-unreferenced blobs) -> completed. failed jobs keep their
// per-step state so a manual resume is idempotent.
const (
	GCJobPlanned     = "planned"
	GCJobRefsDeleted = "refs_deleted"
	GCJobCompleted   = "completed"
	GCJobFailed      = "failed"
)

// GC target/chunk row statuses.
const (
	GCTargetPending          = "pending"
	GCTargetRefsDeleted      = "refs_deleted"
	GCTargetSkippedProtected = "skipped_protected"

	GCChunkPending = "pending"
	GCChunkDeleted = "deleted"
	GCChunkKept    = "kept"
)

func nowUTC() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// ---------------------------------------------------------------------------
// Retention rules (versioned, immutable history)
// ---------------------------------------------------------------------------

// RetentionRule is one version of a named retention rule. keep_last keeps the
// N newest committed snapshots; keep_days keeps committed snapshots younger
// than N days. A snapshot kept by either clause is retained.
type RetentionRule struct {
	ID        int64
	Name      string
	Version   int64
	KeepLast  int
	KeepDays  int
	CreatedAt time.Time
}

// PutRetentionRule stores a new version of the named rule and returns it.
// Old versions are kept so finished jobs remain auditable against the exact
// rule they froze.
func (m *Manifest) PutRetentionRule(name string, keepLast, keepDays int) (*RetentionRule, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var version int64
	if err := tx.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM retention_rules WHERE name = ?`,
		name).Scan(&version); err != nil {
		return nil, err
	}
	version++
	created := nowUTC()
	res, err := tx.Exec(`INSERT INTO retention_rules (name, version, keep_last, keep_days, created_at)
		VALUES (?,?,?,?,?)`, name, version, keepLast, keepDays, created)
	if err != nil {
		return nil, err
	}
	id, _ := res.LastInsertId()
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	t, _ := time.Parse(time.RFC3339Nano, created)
	return &RetentionRule{ID: id, Name: name, Version: version,
		KeepLast: keepLast, KeepDays: keepDays, CreatedAt: t}, nil
}

func scanRule(row interface{ Scan(...any) error }) (RetentionRule, error) {
	var r RetentionRule
	var created string
	if err := row.Scan(&r.ID, &r.Name, &r.Version, &r.KeepLast, &r.KeepDays, &created); err != nil {
		return r, err
	}
	r.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	return r, nil
}

// GetRetentionRule returns the newest version of a named rule.
func (m *Manifest) GetRetentionRule(name string) (*RetentionRule, error) {
	r, err := scanRule(m.db.QueryRow(`SELECT id, name, version, keep_last, keep_days, created_at
		FROM retention_rules WHERE name = ? ORDER BY version DESC LIMIT 1`, name))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// ListRetentionRules returns the newest version of every rule, by name.
func (m *Manifest) ListRetentionRules() ([]RetentionRule, error) {
	rows, err := m.db.Query(`SELECT id, name, version, keep_last, keep_days, created_at
		FROM retention_rules r
		WHERE version = (SELECT MAX(version) FROM retention_rules WHERE name = r.name)
		ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RetentionRule
	for rows.Next() {
		r, err := scanRule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Snapshot protect flags
// ---------------------------------------------------------------------------

// ProtectSnapshot marks a snapshot as off-limits for garbage collection.
// Re-protecting updates the reason.
func (m *Manifest) ProtectSnapshot(id int64, reason string) error {
	if _, err := m.GetSnapshot(id); err != nil {
		return err
	}
	_, err := m.db.Exec(`INSERT INTO snapshot_protections (snapshot_id, reason, created_at)
		VALUES (?,?,?)
		ON CONFLICT(snapshot_id) DO UPDATE SET reason = excluded.reason, created_at = excluded.created_at`,
		id, reason, nowUTC())
	return err
}

// UnprotectSnapshot removes the protect flag.
func (m *Manifest) UnprotectSnapshot(id int64) error {
	if _, err := m.GetSnapshot(id); err != nil {
		return err
	}
	_, err := m.db.Exec(`DELETE FROM snapshot_protections WHERE snapshot_id = ?`, id)
	return err
}

// Protection reports whether a snapshot is protected, and why.
func (m *Manifest) Protection(id int64) (string, bool, error) {
	var reason string
	err := m.db.QueryRow(`SELECT reason FROM snapshot_protections WHERE snapshot_id = ?`, id).Scan(&reason)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return reason, true, nil
}

// ProtectedIDs returns every protected snapshot id mapped to its reason.
func (m *Manifest) ProtectedIDs() (map[int64]string, error) {
	rows, err := m.db.Query(`SELECT snapshot_id, reason FROM snapshot_protections`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var reason string
		if err := rows.Scan(&id, &reason); err != nil {
			return nil, err
		}
		out[id] = reason
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// GC jobs
// ---------------------------------------------------------------------------

// GCJob is the persistent state of one garbage-collection run.
type GCJob struct {
	ID             int64
	Status         string
	RuleName       string
	RuleVersion    int64
	RuleParams     string // frozen JSON copy of the rule at freeze time
	SnapshotsTotal int
	SnapshotsDone  int
	RefsDeleted    int64
	BlobsDeleted   int64
	BytesFreed     int64
	Error          string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	FinishedAt     *time.Time
}

// GCJobTarget is one frozen target snapshot of a job.
type GCJobTarget struct {
	JobID       int64
	SnapshotID  int64
	Status      string
	RootPath    string
	BytesTotal  int64
	CommittedAt string
}

// GCJobChunk is one frozen candidate blob of a job.
type GCJobChunk struct {
	JobID  int64
	Digest []byte
	Length int64
	Status string
}

// GCEvent is one audit-trail entry.
type GCEvent struct {
	ID          int64
	JobID       int64
	SnapshotID  *int64
	ChunkDigest []byte
	Action      string
	Detail      string
	CreatedAt   time.Time
}

func scanGCJob(row interface{ Scan(...any) error }) (GCJob, error) {
	var j GCJob
	var created, updated string
	var finished sql.NullString
	if err := row.Scan(&j.ID, &j.Status, &j.RuleName, &j.RuleVersion, &j.RuleParams,
		&j.SnapshotsTotal, &j.SnapshotsDone, &j.RefsDeleted, &j.BlobsDeleted,
		&j.BytesFreed, &j.Error, &created, &updated, &finished); err != nil {
		return j, err
	}
	j.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	j.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
	if finished.Valid {
		if t, err := time.Parse(time.RFC3339Nano, finished.String); err == nil {
			j.FinishedAt = &t
		}
	}
	return j, nil
}

const gcJobCols = `id, status, rule_name, rule_version, rule_params,
	snapshots_total, snapshots_done, refs_deleted, blobs_deleted, bytes_freed,
	error, created_at, updated_at, finished_at`

// CreateGCJob freezes a new job: the rule version, the full target set and
// the full candidate chunk set are written in one transaction before any
// deletion happens.
func (m *Manifest) CreateGCJob(rule RetentionRule, ruleParams string,
	targets []GCJobTarget, chunks []GCJobChunk) (int64, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	now := nowUTC()
	res, err := tx.Exec(`INSERT INTO gc_jobs
		(status, rule_name, rule_version, rule_params, snapshots_total, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?)`,
		GCJobPlanned, rule.Name, rule.Version, ruleParams, len(targets), now, now)
	if err != nil {
		return 0, err
	}
	jobID, _ := res.LastInsertId()
	for _, t := range targets {
		if _, err := tx.Exec(`INSERT INTO gc_job_targets
			(job_id, snapshot_id, status, root_path, bytes_total, committed_at)
			VALUES (?,?,?,?,?,?)`,
			jobID, t.SnapshotID, GCTargetPending, t.RootPath, t.BytesTotal, t.CommittedAt); err != nil {
			return 0, fmt.Errorf("freeze target %d: %w", t.SnapshotID, err)
		}
	}
	for _, c := range chunks {
		if _, err := tx.Exec(`INSERT INTO gc_job_chunks (job_id, digest, length, status)
			VALUES (?,?,?,?)`, jobID, c.Digest, c.Length, GCChunkPending); err != nil {
			return 0, fmt.Errorf("freeze chunk: %w", err)
		}
	}
	if err := addGCEventTx(tx, jobID, nil, nil, "job_created",
		fmt.Sprintf("rule=%s v%d, targets=%d, candidate_blobs=%d",
			rule.Name, rule.Version, len(targets), len(chunks))); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return jobID, nil
}

// GetGCJob fetches one job.
func (m *Manifest) GetGCJob(id int64) (*GCJob, error) {
	j, err := scanGCJob(m.db.QueryRow(`SELECT `+gcJobCols+` FROM gc_jobs WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// ListGCJobs returns all jobs, newest first.
func (m *Manifest) ListGCJobs() ([]GCJob, error) {
	rows, err := m.db.Query(`SELECT ` + gcJobCols + ` FROM gc_jobs ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GCJob
	for rows.Next() {
		j, err := scanGCJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// UnfinishedGCJobs returns jobs interrupted between phases (crash/restart):
// planned or refs_deleted, oldest first. Failed jobs need a manual resume.
func (m *Manifest) UnfinishedGCJobs() ([]GCJob, error) {
	rows, err := m.db.Query(`SELECT `+gcJobCols+` FROM gc_jobs
		WHERE status IN (?, ?) ORDER BY id`, GCJobPlanned, GCJobRefsDeleted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GCJob
	for rows.Next() {
		j, err := scanGCJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// GCJobTargets returns the frozen target set of a job, oldest snapshot first.
func (m *Manifest) GCJobTargets(jobID int64) ([]GCJobTarget, error) {
	rows, err := m.db.Query(`SELECT job_id, snapshot_id, status, root_path, bytes_total, committed_at
		FROM gc_job_targets WHERE job_id = ? ORDER BY snapshot_id`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GCJobTarget
	for rows.Next() {
		var t GCJobTarget
		if err := rows.Scan(&t.JobID, &t.SnapshotID, &t.Status, &t.RootPath,
			&t.BytesTotal, &t.CommittedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// PendingGCJobTargets returns frozen targets whose references are not yet
// deleted — this is what makes phase 1 re-entrant after a crash.
func (m *Manifest) PendingGCJobTargets(jobID int64) ([]GCJobTarget, error) {
	all, err := m.GCJobTargets(jobID)
	if err != nil {
		return nil, err
	}
	var out []GCJobTarget
	for _, t := range all {
		if t.Status == GCTargetPending {
			out = append(out, t)
		}
	}
	return out, nil
}

// PendingGCJobChunks returns candidate blobs not yet swept or confirmed kept.
func (m *Manifest) PendingGCJobChunks(jobID int64) ([]GCJobChunk, error) {
	rows, err := m.db.Query(`SELECT job_id, digest, length, status FROM gc_job_chunks
		WHERE job_id = ? AND status = ? ORDER BY digest`, jobID, GCChunkPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GCJobChunk
	for rows.Next() {
		var c GCJobChunk
		if err := rows.Scan(&c.JobID, &c.Digest, &c.Length, &c.Status); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// GCJobChunkStats counts candidate blobs by status for progress reporting.
func (m *Manifest) GCJobChunkStats(jobID int64) (map[string]int, error) {
	rows, err := m.db.Query(`SELECT status, count(*) FROM gc_job_chunks
		WHERE job_id = ? GROUP BY status`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{GCChunkPending: 0, GCChunkDeleted: 0, GCChunkKept: 0}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}

// DeleteSnapshotRefs runs phase 1 for one frozen target, in one transaction:
// the snapshot's entry_chunks/entries/row are removed and the target row,
// job counters and audit event are updated atomically with the deletion.
// A snapshot that became protected since the freeze is skipped instead.
// Returns the number of entry_chunks references removed.
func (m *Manifest) DeleteSnapshotRefs(jobID, snapshotID int64) (int64, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var targetStatus string
	err = tx.QueryRow(`SELECT status FROM gc_job_targets
		WHERE job_id = ? AND snapshot_id = ?`, jobID, snapshotID).Scan(&targetStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("snapshot %d is not a frozen target of job %d", snapshotID, jobID)
	}
	if err != nil {
		return 0, err
	}
	if targetStatus != GCTargetPending {
		return 0, nil // already processed (retry after interruption)
	}

	// A protect flag added after the target set was frozen still wins.
	var reason string
	err = tx.QueryRow(`SELECT reason FROM snapshot_protections WHERE snapshot_id = ?`,
		snapshotID).Scan(&reason)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if err == nil {
		if _, err := tx.Exec(`UPDATE gc_job_targets SET status = ?
			WHERE job_id = ? AND snapshot_id = ?`, GCTargetSkippedProtected, jobID, snapshotID); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`UPDATE gc_jobs SET snapshots_done = snapshots_done + 1,
			updated_at = ? WHERE id = ?`, nowUTC(), jobID); err != nil {
			return 0, err
		}
		if err := addGCEventTx(tx, jobID, &snapshotID, nil, "snapshot_skipped_protected",
			"protected after freeze: "+reason); err != nil {
			return 0, err
		}
		return 0, tx.Commit()
	}

	var snapStatus string
	err = tx.QueryRow(`SELECT status FROM snapshots WHERE id = ?`, snapshotID).Scan(&snapStatus)
	if errors.Is(err, sql.ErrNoRows) {
		// Snapshot row already gone; only possible if a previous attempt
		// committed the deletion but not the target update. Converge.
		if _, err := tx.Exec(`UPDATE gc_job_targets SET status = ?
			WHERE job_id = ? AND snapshot_id = ?`, GCTargetRefsDeleted, jobID, snapshotID); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`UPDATE gc_jobs SET snapshots_done = snapshots_done + 1,
			updated_at = ? WHERE id = ?`, nowUTC(), jobID); err != nil {
			return 0, err
		}
		if err := addGCEventTx(tx, jobID, &snapshotID, nil, "snapshot_refs_deleted",
			"snapshot row already removed"); err != nil {
			return 0, err
		}
		return 0, tx.Commit()
	}
	if err != nil {
		return 0, err
	}
	if snapStatus != StatusCommitted {
		// Hard rule: GC only ever touches committed snapshots. Pending and
		// failed snapshots (and their diagnostics) are never collected.
		return 0, fmt.Errorf("refusing to collect snapshot %d in status %q", snapshotID, snapStatus)
	}

	res, err := tx.Exec(`DELETE FROM entry_chunks WHERE snapshot_id = ?`, snapshotID)
	if err != nil {
		return 0, err
	}
	refs, _ := res.RowsAffected()
	if _, err := tx.Exec(`DELETE FROM entries WHERE snapshot_id = ?`, snapshotID); err != nil {
		return 0, err
	}
	res, err = tx.Exec(`DELETE FROM snapshots WHERE id = ? AND status = ?`, snapshotID, StatusCommitted)
	if err != nil {
		return 0, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return 0, fmt.Errorf("snapshot %d changed state during collection", snapshotID)
	}
	if _, err := tx.Exec(`UPDATE gc_job_targets SET status = ?
		WHERE job_id = ? AND snapshot_id = ?`, GCTargetRefsDeleted, jobID, snapshotID); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(`UPDATE gc_jobs SET snapshots_done = snapshots_done + 1,
		refs_deleted = refs_deleted + ?, updated_at = ? WHERE id = ?`,
		refs, nowUTC(), jobID); err != nil {
		return 0, err
	}
	if err := addGCEventTx(tx, jobID, &snapshotID, nil, "snapshot_refs_deleted",
		fmt.Sprintf("removed %d chunk references", refs)); err != nil {
		return 0, err
	}
	return refs, tx.Commit()
}

// MarkGCJobRefsDeleted records the phase boundary: all target references are
// gone, blob sweeping may begin.
func (m *Manifest) MarkGCJobRefsDeleted(jobID int64) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`UPDATE gc_jobs SET status = ?, updated_at = ?
		WHERE id = ? AND status = ?`, GCJobRefsDeleted, nowUTC(), jobID, GCJobPlanned)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		if err := addGCEventTx(tx, jobID, nil, nil, "phase_refs_deleted",
			"all target references removed; sweeping unreferenced blobs"); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// GCChunkRefcount counts live entry_chunks references to a chunk. This is the
// re-confirmation gate: a blob may only be deleted when this returns 0.
func (m *Manifest) GCChunkRefcount(digest []byte) (int, error) {
	var n int
	err := m.db.QueryRow(`SELECT count(*) FROM entry_chunks WHERE chunk_digest = ?`, digest).Scan(&n)
	return n, err
}

// ChunkReferencers lists the distinct snapshots still referencing a chunk.
func (m *Manifest) ChunkReferencers(digest []byte) ([]int64, error) {
	rows, err := m.db.Query(`SELECT DISTINCT snapshot_id FROM entry_chunks
		WHERE chunk_digest = ? ORDER BY snapshot_id`, digest)
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

// MarkGCChunkDeleted finishes the deletion of one swept blob: the catalog
// chunk row, the candidate status, the job counters and the audit event flip
// in one transaction, so a crash between the (idempotent) blob-file removal
// and this call can never double-account bytes.
func (m *Manifest) MarkGCChunkDeleted(jobID int64, digest []byte, length int64) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`UPDATE gc_job_chunks SET status = ?
		WHERE job_id = ? AND digest = ? AND status = ?`, GCChunkDeleted, jobID, digest, GCChunkPending)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil // already accounted by a previous attempt
	}
	if _, err := tx.Exec(`DELETE FROM chunks WHERE digest = ?`, digest); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE gc_jobs SET blobs_deleted = blobs_deleted + 1,
		bytes_freed = bytes_freed + ?, updated_at = ? WHERE id = ?`,
		length, nowUTC(), jobID); err != nil {
		return err
	}
	if err := addGCEventTx(tx, jobID, nil, digest, "blob_deleted",
		fmt.Sprintf("length=%d", length)); err != nil {
		return err
	}
	return tx.Commit()
}

// MarkGCChunkKept records that a candidate blob is still referenced and must
// not be deleted.
func (m *Manifest) MarkGCChunkKept(jobID int64, digest []byte) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`UPDATE gc_job_chunks SET status = ?
		WHERE job_id = ? AND digest = ? AND status = ?`, GCChunkKept, jobID, digest, GCChunkPending)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		if err := addGCEventTx(tx, jobID, nil, digest, "blob_kept_referenced",
			"still referenced by a live snapshot"); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// CompleteGCJob marks a job finished. A completed job is terminal: resume
// calls return its stored state without re-executing anything.
func (m *Manifest) CompleteGCJob(jobID int64) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := nowUTC()
	res, err := tx.Exec(`UPDATE gc_jobs SET status = ?, finished_at = ?, updated_at = ?
		WHERE id = ? AND status IN (?, ?)`,
		GCJobCompleted, now, now, jobID, GCJobPlanned, GCJobRefsDeleted)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		var snaps, blobs, bytes int64
		if err := tx.QueryRow(`SELECT snapshots_done, blobs_deleted, bytes_freed
			FROM gc_jobs WHERE id = ?`, jobID).Scan(&snaps, &blobs, &bytes); err != nil {
			return err
		}
		if err := addGCEventTx(tx, jobID, nil, nil, "job_completed",
			fmt.Sprintf("snapshots=%d blobs_deleted=%d bytes_freed=%d", snaps, blobs, bytes)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// FailGCJob records a job failure. Per-target/per-chunk state is kept, so a
// later resume continues where the job stopped.
func (m *Manifest) FailGCJob(jobID int64, cause string) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.Exec(`UPDATE gc_jobs SET status = ?, error = ?, updated_at = ?
		WHERE id = ? AND status != ?`, GCJobFailed, cause, nowUTC(), jobID, GCJobCompleted)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		if err := addGCEventTx(tx, jobID, nil, nil, "job_failed", cause); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ReopenGCJob moves a failed job back to planned so it can be resumed.
func (m *Manifest) ReopenGCJob(jobID int64) error {
	_, err := m.db.Exec(`UPDATE gc_jobs SET status = ?, error = '', updated_at = ?
		WHERE id = ? AND status = ?`, GCJobPlanned, nowUTC(), jobID, GCJobFailed)
	return err
}

// AddGCEvent appends one audit-trail entry.
func (m *Manifest) AddGCEvent(jobID int64, snapshotID *int64, digest []byte, action, detail string) error {
	return addGCEvent(func(query string, args ...any) (sql.Result, error) {
		return m.db.Exec(query, args...)
	}, jobID, snapshotID, digest, action, detail)
}

func addGCEventTx(tx *sql.Tx, jobID int64, snapshotID *int64, digest []byte, action, detail string) error {
	return addGCEvent(func(query string, args ...any) (sql.Result, error) {
		return tx.Exec(query, args...)
	}, jobID, snapshotID, digest, action, detail)
}

func addGCEvent(exec func(string, ...any) (sql.Result, error),
	jobID int64, snapshotID *int64, digest []byte, action, detail string) error {
	var sid any
	if snapshotID != nil {
		sid = *snapshotID
	}
	_, err := exec(`INSERT INTO gc_events (job_id, snapshot_id, chunk_digest, action, detail, created_at)
		VALUES (?,?,?,?,?,?)`, jobID, sid, digest, action, detail, nowUTC())
	return err
}

// ListGCEvents returns the audit trail of a job, oldest first.
func (m *Manifest) ListGCEvents(jobID int64) ([]GCEvent, error) {
	rows, err := m.db.Query(`SELECT id, job_id, snapshot_id, chunk_digest, action, detail, created_at
		FROM gc_events WHERE job_id = ? ORDER BY id`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GCEvent
	for rows.Next() {
		var e GCEvent
		var sid sql.NullInt64
		var created string
		if err := rows.Scan(&e.ID, &e.JobID, &sid, &e.ChunkDigest, &e.Action, &e.Detail, &created); err != nil {
			return nil, err
		}
		if sid.Valid {
			e.SnapshotID = &sid.Int64
		}
		e.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		out = append(out, e)
	}
	return out, rows.Err()
}
