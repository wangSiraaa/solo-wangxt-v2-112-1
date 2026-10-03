// Package api exposes the backup engine over a small local HTTP API.
package api

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

// Server wires the engine to HTTP.
type Server struct {
	Engine *backup.Engine
}

// NewRouter builds the mux.
func (s *Server) NewRouter() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /v1/recover", s.recover)
	mux.HandleFunc("GET /v1/snapshots", s.list)
	mux.HandleFunc("POST /v1/snapshots", s.create)
	mux.HandleFunc("GET /v1/snapshots/{id}", s.get)
	mux.HandleFunc("POST /v1/snapshots/{id}/verify", s.verify)
	mux.HandleFunc("GET /v1/snapshots/{id}/errors", s.listErrors)
	mux.HandleFunc("GET /v1/snapshots/{id}/missing", s.missing)
	mux.HandleFunc("POST /v1/snapshots/{id}/restore", s.restore)
	mux.HandleFunc("PUT /v1/snapshots/{id}/protect", s.protect)
	mux.HandleFunc("DELETE /v1/snapshots/{id}/protect", s.unprotect)
	mux.HandleFunc("PUT /v1/retention/rules", s.putRule)
	mux.HandleFunc("GET /v1/retention/rules", s.listRules)
	mux.HandleFunc("GET /v1/retention/rules/{name}", s.getRule)
	mux.HandleFunc("POST /v1/gc/preview", s.gcPreview)
	mux.HandleFunc("POST /v1/gc/jobs", s.gcExecute)
	mux.HandleFunc("GET /v1/gc/jobs", s.gcListJobs)
	mux.HandleFunc("GET /v1/gc/jobs/{id}", s.gcGetJob)
	mux.HandleFunc("POST /v1/gc/jobs/{id}/resume", s.gcResume)
	mux.HandleFunc("GET /v1/gc/jobs/{id}/events", s.gcEvents)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string, details any) {
	writeJSON(w, status, map[string]any{
		"error":   code,
		"message": msg,
		"details": details,
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339)})
}

type snapshotResp struct {
	ID            int64      `json:"id"`
	RootPath      string     `json:"root_path"`
	Status        string     `json:"status"`
	FileCount     int64      `json:"file_count"`
	DirCount      int64      `json:"dir_count"`
	BytesTotal    int64      `json:"bytes_total"`
	ChunksNew     int64      `json:"chunks_new"`
	ChunksRef     int64      `json:"chunks_referenced"`
	Polynomial    string     `json:"polynomial"`
	CreatedAt     time.Time  `json:"created_at"`
	CommittedAt   *time.Time `json:"committed_at,omitempty"`
	Message       string     `json:"message"`
	Protected     bool       `json:"protected"`
	ProtectReason string     `json:"protect_reason,omitempty"`
}

func toSnapshotResp(si repo.SnapshotInfo) snapshotResp {
	return snapshotResp{
		ID:          si.ID,
		RootPath:    si.RootPath,
		Status:      si.Status,
		FileCount:   si.FileCount,
		DirCount:    si.DirCount,
		BytesTotal:  si.BytesTotal,
		ChunksNew:   si.ChunksNew,
		ChunksRef:   si.ChunksRef,
		Polynomial:  "0x" + strconv.FormatUint(si.Polynomial, 16),
		CreatedAt:   si.CreatedAt,
		CommittedAt: si.CommittedAt,
		Message:     si.Message,
	}
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	all, err := s.Engine.Manifest.ListSnapshots()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	protected, err := s.Engine.Manifest.ProtectedIDs()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	out := make([]snapshotResp, 0, len(all))
	for _, si := range all {
		sr := toSnapshotResp(si)
		if reason, ok := protected[si.ID]; ok {
			sr.Protected, sr.ProtectReason = true, reason
		}
		out = append(out, sr)
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshots": out})
}

type createReq struct {
	Root          string `json:"root"`
	Message       string `json:"message"`
	Finish        *bool  `json:"finish"`      // default true; false = die before commit (demo)
	LoseChunks    int    `json:"lose_chunks"` // failpoint: delete N blobs pre-verify
	UnstableRetry int    `json:"-"`
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var req createReq
	if err := decodeBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
		return
	}
	if strings.TrimSpace(req.Root) == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "root is required", nil)
		return
	}
	finish := true
	if req.Finish != nil {
		finish = *req.Finish
	}
	s.Engine.Fail.LoseChunkCount = req.LoseChunks
	defer func() { s.Engine.Fail.LoseChunkCount = 0 }()

	res, err := s.Engine.CreateSnapshot(req.Root, req.Message, finish)
	if err != nil {
		var rej *backup.ErrRejected
		if errors.As(err, &rej) {
			writeJSON(w, http.StatusConflict, map[string]any{
				"snapshot_id": rej.SnapshotID,
				"status":      repo.StatusFailed,
				"error":       "snapshot_rejected",
				"reasons":     rej.Reasons,
				"hint":        "GET /v1/snapshots/" + strconv.FormatInt(rej.SnapshotID, 10) + "/missing",
			})
			return
		}
		status := http.StatusInternalServerError
		if res == nil {
			writeErr(w, status, "snapshot_failed", err.Error(), nil)
			return
		}
		writeJSON(w, status, map[string]any{
			"snapshot_id": res.SnapshotID,
			"status":      res.Status,
			"error":       err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"snapshot_id":       res.SnapshotID,
		"status":            res.Status,
		"chunks_new":        res.NewChunks,
		"chunks_referenced": res.RefChunks,
	})
}

func parseID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_id", "id must be an integer", nil)
		return 0, false
	}
	return id, true
}

// decodeBody parses an optional JSON request body; an empty body is not an
// error and leaves req untouched.
func decodeBody(r *http.Request, req any) error {
	if r.Body == nil {
		return nil
	}
	err := json.NewDecoder(r.Body).Decode(req)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	si, err := s.Engine.Manifest.GetSnapshot(id)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "snapshot does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	sr := toSnapshotResp(si)
	if reason, ok, perr := s.Engine.Manifest.Protection(id); perr == nil && ok {
		sr.Protected, sr.ProtectReason = true, reason
	}
	writeJSON(w, http.StatusOK, sr)
}

func (s *Server) verify(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	res, err := s.Engine.VerifyAndFinalize(id)
	if err != nil {
		var rej *backup.ErrRejected
		if errors.As(err, &rej) {
			writeJSON(w, http.StatusConflict, map[string]any{
				"snapshot_id": id,
				"status":      repo.StatusFailed,
				"error":       "verification_failed",
				"missing":     rej.Reasons,
			})
			return
		}
		writeErr(w, http.StatusInternalServerError, "verify_failed", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot_id": id,
		"status":      res.Status,
	})
}

type missingResp struct {
	SnapshotID int64             `json:"snapshot_id"`
	Status     string            `json:"status"`
	Missing    []missingItemResp `json:"missing"`
}

type missingItemResp struct {
	RelPath  string `json:"rel_path"`
	Digest   string `json:"chunk_digest"`
	Length   int64  `json:"declared_length"`
	BlobPath string `json:"expected_blob_path"`
	Reason   string `json:"reason"`
}

func (s *Server) missing(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	si, err := s.Engine.Manifest.GetSnapshot(id)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "snapshot does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	found, err := s.Engine.Manifest.FindMissingChunks(id, func(digest []byte, length int64) (bool, error) {
		return s.Engine.Store.Has(digest, length)
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	items := make([]missingItemResp, 0, len(found))
	for _, mc := range found {
		item := missingItemResp{
			RelPath: mc.RelPath,
			Digest:  hex.EncodeToString(mc.Digest),
			Length:  mc.Length,
			Reason:  mc.Reason,
		}
		if p, err := s.Engine.Store.Path(mc.Digest); err == nil {
			item.BlobPath = p
		}
		items = append(items, item)
	}
	writeJSON(w, http.StatusOK, missingResp{SnapshotID: id, Status: si.Status, Missing: items})
}

func (s *Server) listErrors(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	errs, err := s.Engine.Manifest.ListErrors(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	type item struct {
		Stage       string    `json:"stage"`
		RelPath     string    `json:"rel_path"`
		ChunkDigest string    `json:"chunk_digest,omitempty"`
		Message     string    `json:"message"`
		CreatedAt   time.Time `json:"created_at"`
	}
	out := make([]item, 0, len(errs))
	for _, e := range errs {
		it := item{Stage: e.Stage, RelPath: e.RelPath, Message: e.Message, CreatedAt: e.CreatedAt}
		if len(e.ChunkDigest) > 0 {
			it.ChunkDigest = hex.EncodeToString(e.ChunkDigest)
		}
		out = append(out, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshot_id": id, "errors": out})
}

type restoreReq struct {
	Target string `json:"target"`
}

func (s *Server) restore(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	var req restoreReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
		return
	}
	if strings.TrimSpace(req.Target) == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "target is required", nil)
		return
	}
	res, err := s.Engine.Restore(id, req.Target)
	if err != nil {
		if errors.Is(err, backup.ErrTargetExists) {
			writeErr(w, http.StatusConflict, "target_exists", err.Error(), nil)
			return
		}
		if strings.Contains(err.Error(), "only committed snapshots") {
			writeErr(w, http.StatusConflict, "snapshot_not_committed", err.Error(), nil)
			return
		}
		if strings.Contains(err.Error(), "escapes restore root") {
			writeErr(w, http.StatusUnprocessableEntity, "unsafe_symlink", err.Error(), nil)
			return
		}
		writeErr(w, http.StatusInternalServerError, "restore_failed", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"snapshot_id": res.SnapshotID,
		"target":      res.Target,
		"files":       res.Files,
		"directories": res.Dirs,
		"symlinks":    res.Symlinks,
		"bytes":       res.Bytes,
		"verified":    res.Verified,
	})
}

func (s *Server) recover(w http.ResponseWriter, r *http.Request) {
	out, err := s.Engine.RecoverPending()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "recover_failed", err.Error(), nil)
		return
	}
	ids := make([]map[string]any, 0, len(out))
	for _, r := range out {
		ids = append(ids, map[string]any{"snapshot_id": r.SnapshotID, "status": r.Status})
	}
	writeJSON(w, http.StatusOK, map[string]any{"recovered": ids})
}

// ---------------------------------------------------------------------------
// Protect flags
// ---------------------------------------------------------------------------

type protectReq struct {
	Reason string `json:"reason"`
}

func (s *Server) protect(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	var req protectReq
	if err := decodeBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
		return
	}
	if err := s.Engine.Manifest.ProtectSnapshot(id, req.Reason); err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "snapshot does not exist", nil)
			return
		}
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot_id": id, "protected": true, "reason": req.Reason,
	})
}

func (s *Server) unprotect(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	if err := s.Engine.Manifest.UnprotectSnapshot(id); err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "snapshot does not exist", nil)
			return
		}
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshot_id": id, "protected": false})
}

// ---------------------------------------------------------------------------
// Retention rules
// ---------------------------------------------------------------------------

type ruleReq struct {
	Name     string `json:"name"`
	KeepLast int    `json:"keep_last"`
	KeepDays int    `json:"keep_days"`
}

type ruleResp struct {
	Name     string    `json:"name"`
	Version  int64     `json:"version"`
	KeepLast int       `json:"keep_last"`
	KeepDays int       `json:"keep_days"`
	Created  time.Time `json:"created_at"`
}

func toRuleResp(r repo.RetentionRule) ruleResp {
	return ruleResp{Name: r.Name, Version: r.Version, KeepLast: r.KeepLast,
		KeepDays: r.KeepDays, Created: r.CreatedAt}
}

func validRuleParams(keepLast, keepDays int) string {
	if keepLast < 0 || keepDays < 0 {
		return "keep_last and keep_days must be >= 0"
	}
	if keepLast == 0 && keepDays == 0 {
		return "rule keeps nothing: keep_last or keep_days must be > 0"
	}
	return ""
}

func (s *Server) putRule(w http.ResponseWriter, r *http.Request) {
	var req ruleReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "name is required", nil)
		return
	}
	if msg := validRuleParams(req.KeepLast, req.KeepDays); msg != "" {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_rule", msg, nil)
		return
	}
	rule, err := s.Engine.Manifest.PutRetentionRule(req.Name, req.KeepLast, req.KeepDays)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusCreated, toRuleResp(*rule))
}

func (s *Server) listRules(w http.ResponseWriter, r *http.Request) {
	rules, err := s.Engine.Manifest.ListRetentionRules()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	out := make([]ruleResp, 0, len(rules))
	for _, rule := range rules {
		out = append(out, toRuleResp(rule))
	}
	writeJSON(w, http.StatusOK, map[string]any{"rules": out})
}

func (s *Server) getRule(w http.ResponseWriter, r *http.Request) {
	rule, err := s.Engine.Manifest.GetRetentionRule(r.PathValue("name"))
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "retention rule does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, toRuleResp(*rule))
}

// ---------------------------------------------------------------------------
// Garbage collection: preview, execute, progress, audit
// ---------------------------------------------------------------------------

type gcSelectorReq struct {
	Rule     string `json:"rule"`      // name of a stored rule
	KeepLast int    `json:"keep_last"` // inline rule when rule is empty
	KeepDays int    `json:"keep_days"`
}

func (req gcSelectorReq) selector() backup.GCSelector {
	return backup.GCSelector{RuleName: req.Rule, KeepLast: req.KeepLast, KeepDays: req.KeepDays}
}

func writeGCErr(w http.ResponseWriter, err error) {
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", err.Error(), nil)
		return
	}
	msg := err.Error()
	if strings.Contains(msg, "keep_last") || strings.Contains(msg, "keeps nothing") {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_rule", msg, nil)
		return
	}
	writeErr(w, http.StatusInternalServerError, "gc_failed", msg, nil)
}

func (s *Server) gcPreview(w http.ResponseWriter, r *http.Request) {
	var req gcSelectorReq
	if err := decodeBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
		return
	}
	p, err := s.Engine.PreviewGC(req.selector())
	if err != nil {
		writeGCErr(w, err)
		return
	}
	targets := make([]map[string]any, 0, len(p.Targets))
	for _, t := range p.Targets {
		targets = append(targets, map[string]any{
			"snapshot_id": t.SnapshotID, "root_path": t.RootPath,
			"bytes_total": t.BytesTotal, "chunks_referenced": t.ChunksRef,
			"committed_at": t.CommittedAt,
		})
	}
	skipped := make([]map[string]any, 0, len(p.SkippedProtected))
	for _, sp := range p.SkippedProtected {
		skipped = append(skipped, map[string]any{"snapshot_id": sp.SnapshotID, "reason": sp.Reason})
	}
	notCommitted := make([]map[string]any, 0, len(p.NotCommitted))
	for _, nc := range p.NotCommitted {
		notCommitted = append(notCommitted, map[string]any{"snapshot_id": nc.SnapshotID, "status": nc.Status})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"rule":                  toRuleResp(p.Rule),
		"targets":               targets,
		"skipped_protected":     skipped,
		"kept_by_rule":          p.KeptByRule,
		"skipped_not_committed": notCommitted,
		"candidate_blobs":       p.CandidateBlobs,
		"candidate_bytes":       p.CandidateBytes,
		"reclaimable_blobs":     p.ReclaimableBlobs,
		"reclaimable_bytes":     p.ReclaimableBytes,
	})
}

type gcExecuteReq struct {
	gcSelectorReq
	Wait          bool `json:"wait"`            // run synchronously and return the final state
	StopAfterRefs bool `json:"stop_after_refs"` // failpoint: halt between phase 1 and 2
}

func (s *Server) gcExecute(w http.ResponseWriter, r *http.Request) {
	var req gcExecuteReq
	if err := decodeBody(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
		return
	}
	// The job (rule version + target set + candidate chunks) is frozen before
	// anything runs; execution is a separate, resumable step.
	jobID, err := s.Engine.CreateGCJob(req.selector())
	if err != nil {
		writeGCErr(w, err)
		return
	}
	if !req.Wait {
		go func() { _, _ = s.Engine.RunGCJob(jobID, req.StopAfterRefs) }()
	} else if _, err := s.Engine.RunGCJob(jobID, req.StopAfterRefs); err != nil {
		writeErr(w, http.StatusInternalServerError, "gc_failed",
			err.Error(), map[string]any{"job_id": jobID})
		return
	}
	s.writeJob(w, r, http.StatusCreated, jobID)
}

func (s *Server) gcListJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.Engine.Manifest.ListGCJobs()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	out := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, gcJobJSON(j, nil, nil))
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out})
}

func (s *Server) gcGetJob(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	s.writeJob(w, r, http.StatusOK, id)
}

func (s *Server) gcResume(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	// Idempotent: a completed job returns its stored result unchanged.
	if _, err := s.Engine.RunGCJob(id, false); err != nil {
		if errors.Is(err, repo.ErrNotFound) {
			writeErr(w, http.StatusNotFound, "not_found", "gc job does not exist", nil)
			return
		}
		writeErr(w, http.StatusInternalServerError, "gc_failed", err.Error(), map[string]any{"job_id": id})
		return
	}
	s.writeJob(w, r, http.StatusOK, id)
}

// writeJob renders a job with its frozen targets and chunk-sweep progress.
func (s *Server) writeJob(w http.ResponseWriter, r *http.Request, status int, jobID int64) {
	job, err := s.Engine.Manifest.GetGCJob(jobID)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "gc job does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	targets, err := s.Engine.Manifest.GCJobTargets(jobID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	stats, err := s.Engine.Manifest.GCJobChunkStats(jobID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, status, gcJobJSON(*job, targets, stats))
}

func gcJobJSON(j repo.GCJob, targets []repo.GCJobTarget, chunkStats map[string]int) map[string]any {
	out := map[string]any{
		"id":              j.ID,
		"status":          j.Status,
		"rule":            json.RawMessage(j.RuleParams),
		"snapshots_total": j.SnapshotsTotal,
		"snapshots_done":  j.SnapshotsDone,
		"refs_deleted":    j.RefsDeleted,
		"blobs_deleted":   j.BlobsDeleted,
		"bytes_freed":     j.BytesFreed,
		"created_at":      j.CreatedAt,
		"updated_at":      j.UpdatedAt,
	}
	if j.FinishedAt != nil {
		out["finished_at"] = j.FinishedAt
	}
	if j.Error != "" {
		out["error"] = j.Error
	}
	if targets != nil {
		ts := make([]map[string]any, 0, len(targets))
		for _, t := range targets {
			ts = append(ts, map[string]any{
				"snapshot_id": t.SnapshotID, "status": t.Status,
				"root_path": t.RootPath, "bytes_total": t.BytesTotal,
				"committed_at": t.CommittedAt,
			})
		}
		out["targets"] = ts
	}
	if chunkStats != nil {
		out["chunks"] = chunkStats
	}
	return out
}

func (s *Server) gcEvents(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	if _, err := s.Engine.Manifest.GetGCJob(id); errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "gc job does not exist", nil)
		return
	}
	events, err := s.Engine.Manifest.ListGCEvents(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	out := make([]map[string]any, 0, len(events))
	for _, e := range events {
		item := map[string]any{
			"id":         e.ID,
			"action":     e.Action,
			"detail":     e.Detail,
			"created_at": e.CreatedAt,
		}
		if e.SnapshotID != nil {
			item["snapshot_id"] = *e.SnapshotID
		}
		if len(e.ChunkDigest) > 0 {
			item["chunk_digest"] = hex.EncodeToString(e.ChunkDigest)
		}
		out = append(out, item)
	}
	writeJSON(w, http.StatusOK, map[string]any{"job_id": id, "events": out})
}
