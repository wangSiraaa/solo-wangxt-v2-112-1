package api

import (
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

type ruleReq struct {
	KeepLastN  int64  `json:"keep_last_n"`
	MaxAgeDays int64  `json:"max_age_days"`
	Note       string `json:"note"`
}

type ruleResp struct {
	Version    int64     `json:"version"`
	KeepLastN  int64     `json:"keep_last_n"`
	MaxAgeDays int64     `json:"max_age_days"`
	CreatedAt  time.Time `json:"created_at"`
	Note       string    `json:"note"`
}

func toRuleResp(r repo.RetentionRule) ruleResp {
	return ruleResp{
		Version:    r.Version,
		KeepLastN:  r.KeepLastN,
		MaxAgeDays: r.MaxAgeDays,
		CreatedAt:  r.CreatedAt,
		Note:       r.Note,
	}
}

func (s *Server) createRule(w http.ResponseWriter, r *http.Request) {
	var req ruleReq
	if r.Body != nil {
		if err := decodeJSON(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
			return
		}
	}
	if req.KeepLastN == 0 && req.MaxAgeDays == 0 {
		writeErr(w, http.StatusBadRequest, "bad_request",
			"keep_last_n and max_age_days cannot both be zero (would expire every committed snapshot)", nil)
		return
	}
	rule, err := s.Engine.Manifest.AddRetentionRule(req.KeepLastN, req.MaxAgeDays, req.Note)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusCreated, toRuleResp(rule))
}

func (s *Server) latestRule(w http.ResponseWriter, r *http.Request) {
	rule, err := s.Engine.Manifest.LatestRetentionRule()
	if errors.Is(err, repo.ErrNoRetentionRule) {
		writeErr(w, http.StatusNotFound, "no_rule", "no retention rule configured yet", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, toRuleResp(rule))
}

type holdReq struct {
	Reason string `json:"reason"`
}

func (s *Server) addHold(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	var req holdReq
	if r.Body != nil {
		_ = decodeJSON(r, &req) // empty body is allowed
	}
	h, err := s.Engine.Manifest.AddHold(id, strings.TrimSpace(req.Reason))
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "snapshot does not exist", nil)
		return
	}
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") || strings.Contains(err.Error(), "constraint") {
			writeErr(w, http.StatusConflict, "hold_exists",
				"snapshot already has an active hold", nil)
			return
		}
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"hold_id":     h.ID,
		"snapshot_id": h.SnapshotID,
		"reason":      h.Reason,
		"created_at":  h.CreatedAt,
	})
}

func (s *Server) releaseHold(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	if err := s.Engine.Manifest.ReleaseHold(id); err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "no_hold", "snapshot has no active hold", nil)
			return
		}
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot_id": id,
		"released":    true,
	})
}

func (s *Server) listHolds(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	holds, err := s.Engine.Manifest.HoldsOf(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	out := make([]map[string]any, 0, len(holds))
	for _, h := range holds {
		out = append(out, map[string]any{
			"hold_id":     h.ID,
			"snapshot_id": h.SnapshotID,
			"reason":      h.Reason,
			"created_at":  h.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshot_id": id, "holds": out})
}

type candidateResp struct {
	SnapshotID  int64     `json:"snapshot_id"`
	CommittedAt time.Time `json:"committed_at"`
	Message     string    `json:"message"`
}

type previewResp struct {
	RuleVersion        int64           `json:"rule_version"`
	Rule               ruleResp        `json:"rule"`
	Candidates         []candidateResp `json:"candidates"`
	SkippedHeld        []int64         `json:"skipped_held_snapshots"`
	ProtectedCommitted int64           `json:"protected_committed"`
	Pending            int64           `json:"pending_snapshots"`
	Failed             int64           `json:"failed_snapshots"`
	ExclusiveBlobs     []blobItemResp  `json:"exclusive_blobs"`
	ExclusiveBytes     int64           `json:"exclusive_bytes"`
}

type blobItemResp struct {
	Digest string `json:"digest"`
	Length int64  `json:"length"`
}

func (s *Server) previewRetention(w http.ResponseWriter, r *http.Request) {
	rule, err := s.Engine.Manifest.LatestRetentionRule()
	if errors.Is(err, repo.ErrNoRetentionRule) {
		writeErr(w, http.StatusNotFound, "no_rule", "no retention rule configured yet", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	now := time.Now().UTC()
	if v := r.URL.Query().Get("now"); v != "" {
		if parsed, perr := time.Parse(time.RFC3339Nano, v); perr == nil {
			now = parsed
		}
	}
	pv, err := s.Engine.Manifest.EvaluateRetention(rule, now)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	cands := make([]candidateResp, 0, len(pv.Candidates))
	var bytes int64
	for _, c := range pv.Candidates {
		cands = append(cands, candidateResp{
			SnapshotID:  c.SnapshotID,
			CommittedAt: c.CommittedAt,
			Message:     c.Message,
		})
	}
	blobs := make([]blobItemResp, 0, len(pv.ExclusiveChunks))
	for _, c := range pv.ExclusiveChunks {
		blobs = append(blobs, blobItemResp{Digest: hex.EncodeToString(c.Digest), Length: c.Length})
		bytes += c.Length
	}
	writeJSON(w, http.StatusOK, previewResp{
		RuleVersion:        pv.RuleVersion,
		Rule:               toRuleResp(pv.Rule),
		Candidates:         cands,
		SkippedHeld:        pv.SkippedHeld,
		ProtectedCommitted: pv.ProtectedCommitted,
		Pending:            pv.Pending,
		Failed:             pv.Failed,
		ExclusiveBlobs:     blobs,
		ExclusiveBytes:     bytes,
	})
}

func (s *Server) runGC(w http.ResponseWriter, r *http.Request) {
	// Optional fault-injection query params (demo/test only):
	//   crash_after_phase1=1      stop after references are deleted
	//   crash_after_row_removed=1 stop one blob into the row_removed window
	//   now=RFC3339               freeze-time override (demo/test only)
	var fail backup.GCFailpoints
	fail.CrashAfterPhase1 = r.URL.Query().Get("crash_after_phase1") == "1"
	fail.CrashAfterRowRemoved = r.URL.Query().Get("crash_after_row_removed") == "1"
	now := time.Now().UTC()
	if v := r.URL.Query().Get("now"); v != "" {
		parsed, perr := time.Parse(time.RFC3339Nano, v)
		if perr != nil {
			writeErr(w, http.StatusBadRequest, "bad_now", "now must be RFC3339 time", nil)
			return
		}
		now = parsed
	}
	res, err := s.Engine.RunGCOpts(now, fail)
	if err != nil {
		if errors.Is(err, repo.ErrNoRetentionRule) {
			writeErr(w, http.StatusConflict, "no_rule", "create a retention rule before running GC", nil)
			return
		}
		if errors.Is(err, backup.ErrGCSimulatedCrash) {
			writeJSON(w, http.StatusAccepted, res) // interrupted; job is persisted and resumable
			return
		}
		writeErr(w, http.StatusInternalServerError, "gc_failed", err.Error(), res)
		return
	}
	// A resumed-but-finished job and a fresh success both report the stored
	// result; nothing is ever executed twice.
	writeJSON(w, http.StatusOK, res)
}

func parseGID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("gid"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_id", "gc job id must be an integer", nil)
		return 0, false
	}
	return id, true
}

func (s *Server) getGC(w http.ResponseWriter, r *http.Request) {
	id, ok := parseGID(w, r)
	if !ok {
		return
	}
	job, err := s.Engine.Manifest.GetGCJob(id)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "gc job does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	targets, _ := s.Engine.Manifest.ListGCTargets(id)
	blobs, _ := s.Engine.Manifest.ListGCBlobs(id)
	writeJSON(w, http.StatusOK, jobDetailRespOf(job, targets, blobs))
}

func (s *Server) listGC(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.Engine.Manifest.ListGCJobs(100)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	out := make([]backup.GCResult, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, toGCResult(j))
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
}

func (s *Server) resumeGC(w http.ResponseWriter, r *http.Request) {
	id, ok := parseGID(w, r)
	if !ok {
		return
	}
	res, err := s.Engine.ResumeGC(id)
	if err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "gc job does not exist", nil)
			return
		}
		if errors.Is(err, backup.ErrGCSimulatedCrash) {
			writeJSON(w, http.StatusAccepted, res)
			return
		}
		writeErr(w, http.StatusInternalServerError, "gc_failed", err.Error(), res)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

type jobTargetResp struct {
	SnapshotID  int64  `json:"snapshot_id"`
	State       string `json:"state"`
	Reason      string `json:"reason,omitempty"`
	CommittedAt string `json:"committed_at"`
}

type jobBlobResp struct {
	Digest string `json:"digest"`
	Length int64  `json:"length"`
	State  string `json:"state"`
}

type jobDetailResp struct {
	Job             backup.GCResult `json:"job"`
	Phase           string          `json:"phase"`
	PendingTargets  int64           `json:"pending_targets"`
	PendingBlobs    int64           `json:"pending_blobs"`
	RowRemovedBlobs int64           `json:"row_removed_blobs"`
	DeletedBlobs    int64           `json:"deleted_blobs"`
	KeptSharedBlobs int64           `json:"kept_shared_blobs"`
	Targets         []jobTargetResp `json:"targets"`
	Blobs           []jobBlobResp   `json:"blobs"`
}

// jobDetailRespOf derives the whole progress view from frozen child rows;
// counters are never read from mutable job columns.
func jobDetailRespOf(job repo.GCJob, targets []repo.GCTarget, blobs []repo.GCBlob) jobDetailResp {
	res := toGCResult(job)
	trs := make([]jobTargetResp, 0, len(targets))
	var pendingTargets int64
	for _, t := range targets {
		trs = append(trs, jobTargetResp{
			SnapshotID:  t.SnapshotID,
			State:       t.State,
			Reason:      t.Reason,
			CommittedAt: t.CommittedAt,
		})
		if t.State == repo.GCTargetPending {
			pendingTargets++
		}
	}
	brs := make([]jobBlobResp, 0, len(blobs))
	var pendingBlobs, rowRemoved int64
	for _, b := range blobs {
		brs = append(brs, jobBlobResp{
			Digest: hex.EncodeToString(b.Digest),
			Length: b.Length,
			State:  b.State,
		})
		switch b.State {
		case repo.GCBlobPending:
			pendingBlobs++
		case repo.GCBlobRowRemoved:
			rowRemoved++
		}
	}
	phase := "done"
	switch {
	case job.Status == repo.GCStatusFailed || job.Status == repo.GCStatusQueued:
		if pendingTargets > 0 {
			phase = "phase1"
		} else {
			phase = "phase2"
		}
	case job.Status == repo.GCStatusRunning:
		if pendingTargets > 0 {
			phase = "phase1"
		} else {
			phase = "phase2"
		}
	}
	return jobDetailResp{
		Job:             res,
		Phase:           phase,
		PendingTargets:  pendingTargets,
		PendingBlobs:    pendingBlobs,
		RowRemovedBlobs: rowRemoved,
		DeletedBlobs:    job.BlobsDeleted,
		KeptSharedBlobs: job.BlobsKeptShared,
		Targets:         trs,
		Blobs:           brs,
	}
}

func toGCResult(j repo.GCJob) backup.GCResult {
	return backup.GCResult{
		JobID:          j.ID,
		Status:         j.Status,
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

type auditResp struct {
	ID         int64     `json:"id"`
	JobID      *int64    `json:"job_id,omitempty"`
	Event      string    `json:"event"`
	SnapshotID *int64    `json:"snapshot_id,omitempty"`
	Digest     string    `json:"digest,omitempty"`
	Detail     string    `json:"detail"`
	CreatedAt  time.Time `json:"created_at"`
}

func toAuditResp(ev repo.GCAuditEvent) auditResp {
	a := auditResp{
		ID:         ev.ID,
		JobID:      ev.JobID,
		Event:      ev.Event,
		SnapshotID: ev.SnapshotID,
		Detail:     ev.Detail,
		CreatedAt:  ev.CreatedAt,
	}
	if len(ev.Digest) > 0 {
		a.Digest = hex.EncodeToString(ev.Digest)
	}
	return a
}

func writeAudit(w http.ResponseWriter, events []repo.GCAuditEvent, scope map[string]any) {
	out := make([]auditResp, 0, len(events))
	for _, ev := range events {
		out = append(out, toAuditResp(ev))
	}
	resp := map[string]any{"events": out}
	for k, v := range scope {
		resp[k] = v
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) jobAudit(w http.ResponseWriter, r *http.Request) {
	id, ok := parseGID(w, r)
	if !ok {
		return
	}
	if _, err := s.Engine.Manifest.GetGCJob(id); errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "gc job does not exist", nil)
		return
	}
	events, err := s.Engine.Manifest.ListAudit(id, 10000)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeAudit(w, events, map[string]any{"job_id": id})
}

func (s *Server) allAudit(w http.ResponseWriter, r *http.Request) {
	events, err := s.Engine.Manifest.ListAudit(0, 10000)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeAudit(w, events, nil)
}
