package repo

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// RetentionRule is one immutable versioned retention configuration.
type RetentionRule struct {
	Version    int64
	KeepLastN  int64 // keep the N newest eligible snapshots; 0 disables
	MaxAgeDays int64 // keep snapshots committed within the last N days; 0 disables
	CreatedAt  time.Time
	Note       string
}

// ErrNoRetentionRule is returned when GC is requested without any rule in
// place: deleting without a frozen policy is not allowed.
var ErrNoRetentionRule = errors.New("no retention rule configured")

// ErrInvalidRule rejects a rule that keeps nothing (would expire everything
// immediately) or nothing-but-junk.
var ErrInvalidRule = errors.New("invalid retention rule")

// AddRetentionRule stores a new immutable version. At least one selector must
// be positive, otherwise the rule would expire every committed snapshot.
func (m *Manifest) AddRetentionRule(keepLastN, maxAgeDays int64, note string) (RetentionRule, error) {
	if keepLastN < 0 || maxAgeDays < 0 {
		return RetentionRule{}, fmt.Errorf("%w: values must be >= 0", ErrInvalidRule)
	}
	if keepLastN == 0 && maxAgeDays == 0 {
		return RetentionRule{}, fmt.Errorf("%w: set keep_last_n or max_age_days", ErrInvalidRule)
	}
	now := nowText()
	res, err := m.db.Exec(`INSERT INTO retention_rules
		(keep_last_n, max_age_days, created_at, note) VALUES (?,?,?,?)`,
		keepLastN, maxAgeDays, now, note)
	if err != nil {
		return RetentionRule{}, fmt.Errorf("add retention rule: %w", err)
	}
	ver, _ := res.LastInsertId()
	_ = m.AddAudit(nil, "retention_rule_created", 0, nil,
		fmt.Sprintf("version=%d keep_last_n=%d max_age_days=%d", ver, keepLastN, maxAgeDays))
	return m.GetRetentionRule(ver)
}

// GetRetentionRule fetches one rule version.
func (m *Manifest) GetRetentionRule(version int64) (RetentionRule, error) {
	row := m.db.QueryRow(`SELECT version, keep_last_n, max_age_days, created_at, note
		FROM retention_rules WHERE version = ?`, version)
	return scanRetentionRule(row)
}

// LatestRetentionRule returns the active (highest-versioned) rule.
func (m *Manifest) LatestRetentionRule() (RetentionRule, error) {
	row := m.db.QueryRow(`SELECT version, keep_last_n, max_age_days, created_at, note
		FROM retention_rules ORDER BY version DESC LIMIT 1`)
	r, err := scanRetentionRule(row)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNoRetentionRule
	}
	return r, err
}

func scanRetentionRule(row interface {
	Scan(...any) error
}) (RetentionRule, error) {
	var r RetentionRule
	var created string
	if err := row.Scan(&r.Version, &r.KeepLastN, &r.MaxAgeDays, &created, &r.Note); err != nil {
		return r, err
	}
	r.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	return r, nil
}

// Hold is a preservation pin on a snapshot.
type Hold struct {
	ID         int64
	SnapshotID int64
	Reason     string
	CreatedAt  time.Time
	ReleasedAt *time.Time
}

// ErrHoldExists reports an already-active hold on the snapshot.
var ErrHoldExists = errors.New("snapshot already has an active hold")

// AddHold pins a snapshot against garbage collection.
func (m *Manifest) AddHold(snapshotID int64, reason string) (Hold, error) {
	if _, err := m.GetSnapshot(snapshotID); err != nil {
		return Hold{}, err
	}
	now := nowText()
	res, err := m.db.Exec(`INSERT INTO snapshot_holds (snapshot_id, reason, created_at)
		VALUES (?,?,?)`, snapshotID, reason, now)
	if err != nil {
		return Hold{}, fmt.Errorf("add hold for snapshot %d: %w", snapshotID, err)
	}
	id, _ := res.LastInsertId()
	_ = m.AddAudit(nil, "hold_added", snapshotID, nil, reason)
	return m.GetHold(id)
}

// ReleaseHold removes the active hold of a snapshot. ErrNotFound when there
// is none.
func (m *Manifest) ReleaseHold(snapshotID int64) error {
	res, err := m.db.Exec(`UPDATE snapshot_holds SET released_at = ?
		WHERE snapshot_id = ? AND released_at IS NULL`, nowText(), snapshotID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("hold for snapshot %d: %w", snapshotID, ErrNotFound)
	}
	return m.AddAudit(nil, "hold_released", snapshotID, nil, "")
}

// GetHold fetches a hold row.
func (m *Manifest) GetHold(id int64) (Hold, error) {
	row := m.db.QueryRow(`SELECT id, snapshot_id, reason, created_at, released_at
		FROM snapshot_holds WHERE id = ?`, id)
	var h Hold
	var created, released sql.NullString
	if err := row.Scan(&h.ID, &h.SnapshotID, &h.Reason, &created, &released); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return h, ErrNotFound
		}
		return h, err
	}
	h.CreatedAt, _ = time.Parse(time.RFC3339Nano, created.String)
	if released.Valid {
		t, _ := time.Parse(time.RFC3339Nano, released.String)
		h.ReleasedAt = &t
	}
	return h, nil
}

// HoldsOf returns active holds of one snapshot (normally zero or one).
func (m *Manifest) HoldsOf(snapshotID int64) ([]Hold, error) {
	rows, err := m.db.Query(`SELECT id, snapshot_id, reason, created_at, released_at
		FROM snapshot_holds WHERE snapshot_id = ? AND released_at IS NULL ORDER BY id`, snapshotID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Hold
	for rows.Next() {
		var h Hold
		var created string
		var released sql.NullString
		if err := rows.Scan(&h.ID, &h.SnapshotID, &h.Reason, &created, &released); err != nil {
			return nil, err
		}
		h.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		out = append(out, h)
	}
	return out, rows.Err()
}

// HeldSnapshotIDs returns the set of snapshot ids with an active hold.
func (m *Manifest) HeldSnapshotIDs() (map[int64]bool, error) {
	rows, err := m.db.Query(`SELECT DISTINCT snapshot_id FROM snapshot_holds
		WHERE released_at IS NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

// RetentionCandidate is one committed snapshot a rule considers expired.
type RetentionCandidate struct {
	SnapshotID  int64
	CommittedAt time.Time
	Message     string
}

// RetentionPreview is the live (non-frozen) view used by the preview API.
type RetentionPreview struct {
	RuleVersion        int64
	Rule               RetentionRule
	Candidates         []RetentionCandidate // committed, un-held, expired by rule
	SkippedHeld        []int64              // expired but pinned by a hold
	ProtectedCommitted int64                // committed snapshots still inside the rule window
	Pending            int64                // never eligible
	Failed             int64                // never eligible; diagnostics kept
	// ExclusiveChunks are chunks referenced only by candidate snapshots: these
	// are the bytes GC could actually free; chunks shared with a surviving
	// snapshot are not listed.
	ExclusiveChunks []ChunkRef
}

// EvaluateRetention computes the live expiration view under rule. It does not
// freeze anything — that happens atomically when a GC job is created.
func (m *Manifest) EvaluateRetention(rule RetentionRule, now time.Time) (RetentionPreview, error) {
	sel, err := selectRetentionCandidates(m.db, rule, now)
	if err != nil {
		return RetentionPreview{}, err
	}
	pv := RetentionPreview{
		Rule:               rule,
		RuleVersion:        rule.Version,
		Candidates:         sel.candidates,
		SkippedHeld:        sel.skippedHeld,
		ProtectedCommitted: sel.protectedCommitted,
		Pending:            sel.pending,
		Failed:             sel.failed,
	}
	chunks, err := exclusiveChunksQ(m.db, candidateIDs(sel.candidates))
	if err != nil {
		return pv, err
	}
	pv.ExclusiveChunks = chunks
	return pv, nil
}

type retentionSelection struct {
	candidates         []RetentionCandidate
	skippedHeld        []int64
	protectedCommitted int64
	pending            int64
	failed             int64
}

// selectRetentionCandidates evaluates the rule against the current snapshot
// table through q (a *sql.DB for the live preview, a *sql.Tx when freezing a
// job). Held snapshots never appear in the candidate list even when expired;
// pending/failed snapshots are counted but are never eligible.
func selectRetentionCandidates(q querier, rule RetentionRule, now time.Time) (retentionSelection, error) {
	var sel retentionSelection
	held, err := heldSnapshotIDsQ(q)
	if err != nil {
		return sel, err
	}
	rows, err := q.Query(`SELECT id, status, committed_at, message
		FROM snapshots ORDER BY committed_at DESC, id DESC`)
	if err != nil {
		return sel, err
	}
	type snapRow struct {
		id          int64
		status      string
		committedAt string
		message     string
	}
	var committed []snapRow
	for rows.Next() {
		var sr snapRow
		var ctNull sql.NullString
		if err := rows.Scan(&sr.id, &sr.status, &ctNull, &sr.message); err != nil {
			rows.Close()
			return sel, err
		}
		sr.committedAt = ctNull.String
		switch sr.status {
		case StatusPending:
			sel.pending++
		case StatusFailed:
			sel.failed++
		case StatusCommitted:
			committed = append(committed, sr)
		case StatusReclaimed:
			// tombstones of earlier GC runs: nothing to expire, nothing pending
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return sel, err
	}

	cutoff := now.Add(-time.Duration(rule.MaxAgeDays) * 24 * time.Hour)
	for i, sr := range committed {
		ct, perr := time.Parse(time.RFC3339Nano, sr.committedAt)
		if perr != nil {
			continue
		}
		youngEnough := rule.MaxAgeDays > 0 && ct.After(cutoff)
		newestN := rule.KeepLastN > 0 && int64(i) < rule.KeepLastN
		if youngEnough || newestN {
			sel.protectedCommitted++
			continue
		}
		if held[sr.id] {
			sel.skippedHeld = append(sel.skippedHeld, sr.id)
			continue
		}
		sel.candidates = append(sel.candidates,
			RetentionCandidate{SnapshotID: sr.id, CommittedAt: ct, Message: sr.message})
	}
	return sel, nil
}

func heldSnapshotIDsQ(q querier) (map[int64]bool, error) {
	rows, err := q.Query(`SELECT DISTINCT snapshot_id FROM snapshot_holds
		WHERE released_at IS NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

func candidateIDs(cs []RetentionCandidate) []int64 {
	ids := make([]int64, len(cs))
	for i, c := range cs {
		ids[i] = c.SnapshotID
	}
	return ids
}

// exclusiveChunks returns chunks referenced only by the given snapshot set:
// no other snapshot's entry_chunks (of ANY status — pending/failed included)
// still points at them. These are the only blobs GC may later delete.
func (m *Manifest) exclusiveChunks(snapshotIDs []int64) ([]ChunkRef, error) {
	return exclusiveChunksQ(m.db, snapshotIDs)
}

func exclusiveChunksQ(q querier, snapshotIDs []int64) ([]ChunkRef, error) {
	if len(snapshotIDs) == 0 {
		return nil, nil
	}
	ph, args := buildExclusionQuery(snapshotIDs)
	rows, err := q.Query(`SELECT c.digest, c.length
		FROM entry_chunks ec JOIN chunks c ON c.digest = ec.chunk_digest
		WHERE ec.snapshot_id IN (`+ph+`)
		AND c.digest NOT IN (
			SELECT chunk_digest FROM entry_chunks WHERE snapshot_id NOT IN (`+ph+`)
		)
		GROUP BY c.digest ORDER BY c.digest`, append(append([]any{}, args...), args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChunkRef
	for rows.Next() {
		var c ChunkRef
		if err := rows.Scan(&c.Digest, &c.Length); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func buildExclusionQuery(ids []int64) (string, []any) {
	placeholders := ""
	args := make([]any, len(ids))
	for i, id := range ids {
		if i > 0 {
			placeholders += ","
		}
		placeholders += "?"
		args[i] = id
	}
	return placeholders, args
}

func nowText() string { return time.Now().UTC().Format(time.RFC3339Nano) }
