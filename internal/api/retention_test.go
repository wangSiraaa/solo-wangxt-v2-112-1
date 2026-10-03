package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"incbackup/internal/api"
	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

type testEnv struct {
	srv *httptest.Server
	e   *backup.Engine
	dir string
	src string
}

func newEnv(t *testing.T) *testEnv {
	t.Helper()
	dir := t.TempDir()
	m, err := repo.OpenManifest(filepath.Join(dir, "manifest.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	s, err := repo.NewContentStore(filepath.Join(dir, "chunks"))
	if err != nil {
		t.Fatal(err)
	}
	e, err := backup.NewEngine(m, s)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer((&api.Server{Engine: e}).NewRouter())
	t.Cleanup(srv.Close)

	src := filepath.Join(dir, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	big := strings.Repeat("abcdefgh0123456789\n", 9000) // ~170KiB -> several chunks
	if err := os.WriteFile(filepath.Join(src, "big.bin"), []byte(big), 0o644); err != nil {
		t.Fatal(err)
	}
	return &testEnv{srv: srv, e: e, dir: dir, src: src}
}

func (env *testEnv) do(t *testing.T, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, env.srv.URL+path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if len(data) > 0 {
		if err := json.Unmarshal(data, &out); err != nil {
			t.Fatalf("bad json %q: %v", data, err)
		}
	}
	if out == nil {
		out = map[string]any{}
	}
	return resp.StatusCode, out
}

func (env *testEnv) snapshot(t *testing.T, msg string, extra map[string]any) int64 {
	t.Helper()
	body := map[string]any{"root": env.src, "message": msg}
	for k, v := range extra {
		body[k] = v
	}
	code, b := env.do(t, "POST", "/v1/snapshots", body)
	if code != http.StatusCreated && code != http.StatusConflict {
		t.Fatalf("snapshot %s: HTTP %d %v", msg, code, b)
	}
	return int64(b["snapshot_id"].(float64))
}

// Full HTTP lifecycle: rule, preview, hold, skip, release, execute, progress,
// audit and restore-after-GC.
func TestHTTPRetentionAndGCLifecycle(t *testing.T) {
	env := newEnv(t)

	id1 := env.snapshot(t, "old", nil)
	// Middle edit: second snapshot shares most chunks.
	p := filepath.Join(env.src, "big.bin")
	buf, _ := os.ReadFile(p)
	copy(buf[80*1024:80*1024+8], []byte("PATCHED!"))
	if err := os.WriteFile(p, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	id2 := env.snapshot(t, "new", nil)

	// GC without a rule -> 409.
	code, b := env.do(t, "POST", "/v1/retention/gc", nil)
	if code != http.StatusConflict || b["error"] != "no_rule" {
		t.Fatalf("gc before rule: %d %v", code, b)
	}

	// Rule: keep only newest snapshot.
	code, b = env.do(t, "POST", "/v1/retention/rules",
		map[string]any{"keep_last_n": 1, "note": "latest only"})
	if code != http.StatusCreated {
		t.Fatalf("create rule: %d %v", code, b)
	}
	ruleVer := int64(b["version"].(float64))

	code, b = env.do(t, "GET", "/v1/retention/rules/latest", nil)
	if code != 200 || int64(b["version"].(float64)) != ruleVer {
		t.Fatalf("latest rule: %d %v", code, b)
	}

	// Preview selects the old snapshot as a candidate.
	code, b = env.do(t, "GET", "/v1/retention/preview", nil)
	if code != 200 {
		t.Fatalf("preview: %d %v", code, b)
	}
	cands := b["candidates"].([]any)
	if len(cands) != 1 || int64(cands[0].(map[string]any)["snapshot_id"].(float64)) != id1 {
		t.Fatalf("preview candidates = %v", cands)
	}
	if n := int64(b["exclusive_blobs"].([]any)[0].(map[string]any)["length"].(float64)); n <= 0 {
		t.Fatal("preview must list exclusive blob lengths")
	}

	// Hold the old snapshot: preview and execution skip it.
	code, b = env.do(t, "PUT", fmt.Sprintf("/v1/snapshots/%d/hold", id1),
		map[string]any{"reason": "legal W9"})
	if code != http.StatusCreated {
		t.Fatalf("hold: %d %v", code, b)
	}
	// Duplicate hold -> 409.
	code, _ = env.do(t, "PUT", fmt.Sprintf("/v1/snapshots/%d/hold", id1), nil)
	if code != http.StatusConflict {
		t.Fatalf("duplicate hold = %d want 409", code)
	}
	code, b = env.do(t, "GET", "/v1/retention/preview", nil)
	if len(b["candidates"].([]any)) != 0 ||
		len(b["skipped_held_snapshots"].([]any)) != 1 {
		t.Fatalf("preview with hold: %v", b)
	}
	code, b = env.do(t, "POST", "/v1/retention/gc", nil)
	if code != 200 || int64(b["target_count"].(float64)) != 0 {
		t.Fatalf("gc with hold: %d %v", code, b)
	}
	// GET snapshot now reports held=true.
	code, b = env.do(t, "GET", fmt.Sprintf("/v1/snapshots/%d", id1), nil)
	if code != 200 || b["held"] != true {
		t.Fatalf("held flag: %d %v", code, b)
	}

	// Release and execute.
	code, _ = env.do(t, "DELETE", fmt.Sprintf("/v1/snapshots/%d/hold", id1), nil)
	if code != 200 {
		t.Fatalf("release hold: %d", code)
	}
	code, b = env.do(t, "POST", "/v1/retention/gc", nil)
	if code != 200 || b["status"] != "succeeded" {
		t.Fatalf("execute gc: %d %v", code, b)
	}
	jobID := int64(b["job_id"].(float64))
	if b["rule_version"].(float64) != float64(ruleVer) {
		t.Fatal("job must freeze the rule version")
	}
	deleted := int64(b["blobs_deleted"].(float64))
	if deleted != 1 {
		t.Fatalf("only the one old-only blob should be deleted, got %d", deleted)
	}
	// Re-executing returns the stored job, not a rerun: the next POST makes a
	// fresh empty job.
	code, b2 := env.do(t, "POST", "/v1/retention/gc", nil)
	if code != 200 {
		t.Fatalf("second gc: %d", code)
	}
	if int64(b2["job_id"].(float64)) == jobID ||
		int64(b2["target_count"].(float64)) != 0 ||
		int64(b2["blobs_deleted"].(float64)) != 0 {
		t.Fatalf("second gc must be a fresh empty job: %v", b2)
	}

	// Progress / detail of the original job.
	code, b = env.do(t, "GET", fmt.Sprintf("/v1/retention/gc/%d", jobID), nil)
	if code != 200 {
		t.Fatalf("job detail: %d %v", code, b)
	}
	jb := b["job"].(map[string]any)
	if jb["status"] != "succeeded" || int64(jb["targets_done"].(float64)) != 1 {
		t.Fatalf("job detail = %v", jb)
	}
	targets := b["targets"].([]any)
	if len(targets) != 1 || targets[0].(map[string]any)["state"] != "done" {
		t.Fatalf("target states = %v", targets)
	}

	// Audit trail exists and contains both job and hold events.
	code, b = env.do(t, "GET", fmt.Sprintf("/v1/retention/gc/%d/audit", jobID), nil)
	if code != 200 {
		t.Fatalf("job audit: %d", code)
	}
	events := b["events"].([]any)
	wantEvents := map[string]bool{"job_created": false, "target_reclaimed": false,
		"blob_deleted": false, "job_succeeded": false}
	for _, ev := range events {
		wantEvents[ev.(map[string]any)["event"].(string)] = true
	}
	for ev, seen := range wantEvents {
		if !seen {
			t.Fatalf("audit missing event %q in %v", ev, events)
		}
	}
	code, b = env.do(t, "GET", "/v1/retention/audit", nil)
	if code != 200 {
		t.Fatalf("global audit: %d", code)
	}
	var sawHold bool
	for _, ev := range b["events"].([]any) {
		if ev.(map[string]any)["event"] == "hold_added" {
			sawHold = true
		}
	}
	if !sawHold {
		t.Fatal("global audit should contain hold_added")
	}

	// Old snapshot is a tombstone; newer restores byte-complete.
	code, _ = env.do(t, "POST", fmt.Sprintf("/v1/snapshots/%d/restore", id1),
		map[string]any{"target": filepath.Join(env.dir, "old-out")})
	if code < 400 {
		t.Fatalf("restore reclaimed snapshot = %d, want error", code)
	}
	target := filepath.Join(env.dir, "new-out")
	code, b = env.do(t, "POST", fmt.Sprintf("/v1/snapshots/%d/restore", id2),
		map[string]any{"target": target})
	if code != http.StatusCreated {
		t.Fatalf("restore newer: %d %v", code, b)
	}
	got, _ := os.ReadFile(filepath.Join(target, "big.bin"))
	want, _ := os.ReadFile(p)
	if !bytes.Equal(got, want) {
		t.Fatal("newer snapshot did not restore identical bytes after GC")
	}
}

// Crash after phase 1 over HTTP: job fails, then resume via the same job id
// completes and cleans only unreferenced blobs.
func TestHTTPGCCrashAndResume(t *testing.T) {
	env := newEnv(t)
	id1 := env.snapshot(t, "old", nil)

	future := time.Now().UTC().Add(31 * 24 * time.Hour).Format(time.RFC3339Nano)
	if code, rb := env.do(t, "POST", "/v1/retention/rules",
		map[string]any{"max_age_days": 1}); code != http.StatusCreated {
		t.Fatalf("rule: %d %v", code, rb)
	}
	code, b := env.do(t, "POST",
		"/v1/retention/gc?crash_after_phase1=1&now="+future, nil)
	if code != http.StatusAccepted {
		t.Fatalf("crash injection: %d %v", code, b)
	}
	if b["status"] != "failed" {
		t.Fatalf("status = %v want failed", b["status"])
	}
	jobID := int64(b["job_id"].(float64))

	// Old snapshot reclaimed but its blob files still present.
	code, _ = env.do(t, "GET", fmt.Sprintf("/v1/snapshots/%d", id1), nil)
	if code != 200 {
		t.Fatalf("get old snapshot: %d", code)
	}
	chunks, err := env.e.Manifest.ReferencedChunks(id1)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 0 {
		t.Fatalf("references should be gone after phase 1, got %d", len(chunks))
	}

	// Job list shows the failed job.
	code, b = env.do(t, "GET", "/v1/retention/gc", nil)
	if code != 200 || len(b["jobs"].([]any)) < 1 {
		t.Fatalf("job list: %d %v", code, b)
	}

	// Resume the same job id.
	code, b = env.do(t, "POST", fmt.Sprintf("/v1/retention/gc/%d/resume", jobID), nil)
	if code != 200 || b["status"] != "succeeded" {
		t.Fatalf("resume: %d %v", code, b)
	}
	if !b["resumed"].(bool) || int64(b["job_id"].(float64)) != jobID {
		t.Fatalf("resume must reuse job id: %v", b)
	}
	if int64(b["blobs_deleted"].(float64)) <= 0 {
		t.Fatal("resume should delete the now-orphan blobs")
	}

	// Resuming a succeeded job reports the same result and never re-runs.
	code, b2 := env.do(t, "POST", fmt.Sprintf("/v1/retention/gc/%d/resume", jobID), nil)
	if code != 200 || !b2["already_done"].(bool) {
		t.Fatalf("idempotent resume: %d %v", code, b2)
	}
	if b2["blobs_deleted"] != b["blobs_deleted"] {
		t.Fatal("succeeded job counters changed on re-resume")
	}
}
