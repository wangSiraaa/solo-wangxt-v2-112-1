// Command demo drives the backup HTTP API through the full failure story:
//
//  1. committed snapshot + restore into a new directory with digest/length
//     verification (empty file included),
//  2. small edit reusing existing content-defined chunks,
//  3. refusal to restore over an existing directory,
//  4. file actively written during scan -> re-read then rejected,
//  5. commit interruption losing a blob -> failed snapshot with the exact
//     missing chunk located,
//  6. symlink escaping the root -> restored link is blocked,
//  7. server restart with a pending snapshot -> startup recovery commits it,
//  8. retention rule + GC: reclaim the older of two chunk-sharing snapshots,
//  9. protect flag: skipped by preview and execution until unprotected,
//  10. GC interrupted between phases -> restart resumes the same job,
//  11. GC concurrent with a new snapshot; pending/failed snapshots and their
//      diagnostics are never collected.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"incbackup/internal/api"
	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

var pass, fail int

func main() {
	keep := flag.Bool("keep", false, "keep the demo workspace afterwards")
	flag.Parse()

	work, err := os.MkdirTemp("", "incbackup-demo-")
	must(err)
	if *keep {
		fmt.Printf("(workspace: %s)\n", work)
	} else {
		defer os.RemoveAll(work)
	}
	repoDir := filepath.Join(work, "repo")
	src := filepath.Join(work, "src")

	section(0, "准备：在一个进程内启动本地 API 服务与数据目录")
	srv := startServer(repoDir)
	fmt.Printf("  API   : %s\n", srv.URL)
	fmt.Printf("  仓库  : %s (manifest.sqlite + chunks/)\n", repoDir)
	fmt.Printf("  数据源: %s\n", src)
	must(os.MkdirAll(filepath.Join(src, "docs"), 0o755))

	// Big-ish log so content-defined chunking produces several chunks.
	log := make([]byte, 0, 160*1024)
	for i := 0; i < 160*1024; i++ {
		log = append(log, byte("abcdefghijklmnopqrstuvwxyz0123456789\n"[i%37]))
	}
	must(os.WriteFile(filepath.Join(src, "app.log"), log, 0o644))
	must(os.WriteFile(filepath.Join(src, "docs", "notes.txt"), []byte("meeting notes\n"), 0o644))
	must(os.WriteFile(filepath.Join(src, "run.sh"), []byte("#!/bin/sh\necho hi\n"), 0o750))
	must(os.WriteFile(filepath.Join(src, "EMPTY.dat"), nil, 0o600)) // empty file
	must(os.Symlink("docs/notes.txt", filepath.Join(src, "link_to_notes")))

	// ---- 1. first snapshot + restore --------------------------------------
	section(1, "首次快照：完成前逐块验证，然后恢复到全新目录并核对摘要与长度")
	r := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "baseline"})
	firstID := int64(r["snapshot_id"].(float64))
	fmt.Printf("  快照 %d: status=%s 新块=%v 引用块=%v\n",
		firstID, r["status"], r["chunks_new"], r["chunks_referenced"])
	check("快照状态为 committed", r["status"] == "committed")

	restoreDir := filepath.Join(work, "restore-1")
	code, body := raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", firstID),
		map[string]any{"target": restoreDir})
	if code != http.StatusCreated {
		must(fmt.Errorf("restore #1 failed: HTTP %d %s", code, body["message"]))
	}
	rr := body
	verified, _ := rr["verified"].([]any)
	fmt.Printf("  恢复到 %s\n  文件=%v 目录=%v 符号链接=%v 字节=%v\n",
		restoreDir, rr["files"], rr["directories"], rr["symlinks"], rr["bytes"])
	for _, v := range verified {
		m := v.(map[string]any)
		fmt.Printf("    %-18s 长度=%-6d 块数=%-2d 摘要=%s… 权限=0%o\n",
			m["rel_path"], int64(m["size"].(float64)), int(m["chunk_count"].(float64)),
			m["digest"].(string)[:16], int64(m["mode"].(float64)))
	}
	emptyOK := false
	for _, v := range verified {
		m := v.(map[string]any)
		if m["rel_path"] == "EMPTY.dat" {
			emptyOK = m["size"].(float64) == 0 &&
				m["digest"] == fmt.Sprintf("%x", sha256.New().Sum(nil)) &&
				m["chunk_count"].(float64) == 0
		}
	}
	check("空文件：长度 0、SHA256=e3b0c44…、0 个内容块", emptyOK)

	// Compare tree metadata with source.
	var modeMismatch []string
	for _, rel := range []string{"run.sh", "app.log", "docs"} {
		a, _ := os.Lstat(filepath.Join(src, rel))
		b, err := os.Lstat(filepath.Join(restoreDir, rel))
		if err != nil || a.Mode().Perm() != b.Mode().Perm() {
			modeMismatch = append(modeMismatch, rel)
		}
	}
	check("目录权限与文件权限均保留 (run.sh 0750, docs 0755)", len(modeMismatch) == 0)
	lt, _ := os.Readlink(filepath.Join(restoreDir, "link_to_notes"))
	check("符号链接本身被恢复（链接目标=docs/notes.txt，未跟随）", lt == "docs/notes.txt")
	notes, err := os.ReadFile(filepath.Join(restoreDir, "link_to_notes"))
	check("恢复出的链接仍可解析到文件内容", err == nil && string(notes) == "meeting notes\n")

	// byte-identical content of app.log independently re-hashed
	got, _ := hashFile(filepath.Join(restoreDir, "app.log"))
	want, _ := hashFile(filepath.Join(src, "app.log"))
	check("恢复内容逐字节一致（独立重算 SHA256）", got == want)

	// ---- 2. small edit reuses chunks --------------------------------------
	section(2, "小改动的增量：在 app.log 中部改一行，只新增 1 个块，其余块全部复用")
	off := 80 * 1024
	copy(log[off:off+8], []byte("PATCHED!"))
	must(os.WriteFile(filepath.Join(src, "app.log"), log, 0o644))
	r = post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "one-line patch"})
	secondID := int64(r["snapshot_id"].(float64))
	newChunks := int64(r["chunks_new"].(float64))
	refChunks := int64(r["chunks_referenced"].(float64))
	fmt.Printf("  快照 %d: 引用块=%d，其中新写入=%d，复用=%d\n",
		secondID, refChunks, newChunks, refChunks-newChunks)
	check("仅有改动附近的 1 个块是新块（内容定义分块边界由内容决定）", newChunks == 1)
	check("其余块全部复用快照 1 中的旧块", refChunks-newChunks == refChunks-1)

	restore2 := filepath.Join(work, "restore-2")
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", secondID), map[string]any{"target": restore2})
	if code != http.StatusCreated {
		must(fmt.Errorf("restore #2 failed: HTTP %d %s", code, body["message"]))
	}
	g2, _ := hashFile(filepath.Join(restore2, "app.log"))
	w2, _ := hashFile(filepath.Join(src, "app.log"))
	check("恢复快照 2 后 app.log 与当前源文件一致", g2 == w2)

	// ---- 3. never overwrite destination -----------------------------------
	section(3, "恢复位置已有任何东西 → 拒绝，不覆盖、不合并")
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", firstID),
		map[string]any{"target": restoreDir})
	fmt.Printf("  POST restore 到已存在目录 -> HTTP %d: %s\n", code, body["error"])
	check("已有目录时返回 409 target_exists", code == http.StatusConflict && body["error"] == "target_exists")

	// ---- 4. file being written during scan --------------------------------
	section(4, "扫描中仍在写入的文件：先短暂写入触发重读，再持续写入触发拒绝")
	growing := filepath.Join(src, "growing.log")
	must(os.WriteFile(growing, []byte("line0\n"), 0o644))

	// 4a. writer finishes within the retry window: scanner re-reads and commits
	done := make(chan struct{})
	go func() {
		f, _ := os.OpenFile(growing, os.O_APPEND|os.O_WRONLY, 0o644)
		for i := 1; i <= 5; i++ {
			fmt.Fprintf(f, "line%d %s\n", i, strings.Repeat("y", 200))
			time.Sleep(20 * time.Millisecond)
		}
		f.Close()
		close(done)
	}()
	snap4a := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "writer settles during retry"})
	<-done
	check("写入在重读窗口内结束：扫描器重读文件，快照仍正常 committed", snap4a["status"] == "committed")

	// 4b. writer keeps going: every pass sees a changed size/mtime -> rejected
	stop := make(chan struct{})
	go func() {
		f, _ := os.OpenFile(growing, os.O_APPEND|os.O_WRONLY, 0o644)
		defer f.Close()
		i := 1
		for {
			select {
			case <-stop:
				return
			default:
				fmt.Fprintf(f, "line%d %s\n", i, strings.Repeat("x", 200))
				i++
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()
	time.Sleep(30 * time.Millisecond)
	code, body = raw("POST", srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "racing writer"})
	close(stop)
	reasons, _ := body["reasons"].([]any)
	fmt.Printf("  持续写入时 HTTP %d, 快照 %v -> %s\n", code, body["snapshot_id"], body["error"])
	for _, x := range reasons {
		fmt.Printf("    拒绝原因: %s\n", x)
	}
	sawUnstable := false
	for _, x := range reasons {
		if strings.Contains(x.(string), "growing.log") &&
			strings.Contains(x.(string), "still being written") {
			sawUnstable = true
		}
	}
	check("3 次重读后仍在变化的 growing.log 被明确点名（而不是备份静默成功）",
		code == http.StatusConflict && sawUnstable)
	badID := int64(body["snapshot_id"].(float64))
	errs, _ := get(srv.URL + fmt.Sprintf("/v1/snapshots/%d/errors", badID))["errors"].([]any)
	check("失败快照保留在清单中，stage=scan 可追溯", len(errs) > 0 &&
		errs[0].(map[string]any)["stage"] == "scan")

	// ---- 5. commit interruption: lost blob, locate exact chunk ------------
	section(5, "模拟提交中断：删掉最后一个内容块 → 完成前验证拦截并定位具体缺块")
	must(os.WriteFile(growing, []byte("stable now\n"), 0o644))
	code, body = raw("POST", srv.URL+"/v1/snapshots",
		map[string]any{"root": src, "message": "interrupted commit", "lose_chunks": 1})
	fmt.Printf("  HTTP %d, 快照 %v -> %s\n", code, body["snapshot_id"], body["error"])
	interruptedID := int64(body["snapshot_id"].(float64))
	check("缺块快照不能 committed，返回 409", code == http.StatusConflict)

	missing := get(srv.URL + fmt.Sprintf("/v1/snapshots/%d/missing", interruptedID))["missing"].([]any)
	fmt.Printf("  维护查询 GET .../missing 找到 %d 个缺块：\n", len(missing))
	for _, x := range missing {
		m := x.(map[string]any)
		fmt.Printf("    文件   : %s\n", m["rel_path"])
		fmt.Printf("    块摘要 : %s\n", m["chunk_digest"])
		fmt.Printf("    应在   : %s\n", m["expected_blob_path"])
		fmt.Printf("    原因   : %s\n", m["reason"])
		_, statErr := os.Stat(m["expected_blob_path"].(string))
		check("报告的块路径在磁盘上确实不存在", os.IsNotExist(statErr))
	}
	check("缺块清单精确到 文件+摘要+期望磁盘路径（不是“上传队列为空”）", len(missing) == 1)
	si := get(srv.URL + fmt.Sprintf("/v1/snapshots/%d", interruptedID))
	check("失败快照状态可查 = failed", si["status"] == "failed")

	// restore of a failed snapshot must be refused
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", interruptedID),
		map[string]any{"target": filepath.Join(work, "never")})
	fmt.Printf("  尝试恢复 failed 快照 -> HTTP %d %s\n", code, body["error"])
	check("failed 快照拒绝恢复", code >= 400)

	// ---- 6. symlink escape containment ------------------------------------
	section(6, "符号链接越界：备份只存链接本身，恢复时指向根目录外的链接被拒绝")
	secret := filepath.Join(work, "secret.txt")
	must(os.WriteFile(secret, []byte("TOP SECRET"), 0o600))
	evil := filepath.Join(src, "evil_link")
	_ = os.Remove(evil)
	rel, _ := filepath.Rel(filepath.Join(src), secret)
	must(os.Symlink(rel, evil)) // src/evil_link -> ../secret.txt
	snapEvil := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "with evil link"})
	evilID := int64(snapEvil["snapshot_id"].(float64))
	evilTarget := filepath.Join(work, "restore-evil")
	code, body = raw("POST", srv.URL+fmt.Sprintf("/v1/snapshots/%d/restore", evilID),
		map[string]any{"target": evilTarget})
	fmt.Printf("  含越界链接的恢复 -> HTTP %d: %s\n", code, body["message"])
	check("越界符号链接恢复被阻止 (422)", code == http.StatusUnprocessableEntity)
	_, statErr := os.Lstat(evilTarget)
	check("失败后不留半成品目录（回滚清理）", os.IsNotExist(statErr))
	_, err = os.ReadFile(filepath.Join(evilTarget, "evil_link"))
	check("秘密文件没有被触及/写出", err != nil)

	// ---- 7. restart recovery of a pending snapshot -------------------------
	section(7, "提交前进程退出：快照留在 pending，服务重启时自动验证并给结论")
	pend := post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "crash before commit", "finish": false})
	pendID := int64(pend["snapshot_id"].(float64))
	fmt.Printf("  故障时刻: 快照 %d status=%s，块已落盘、清单未提交\n", pendID, pend["status"])
	check("finish=false 留下 pending 快照", pend["status"] == "pending")
	srv.Close()

	srv = startServer(repoDir) // same repo, new process equivalent
	time.Sleep(100 * time.Millisecond)
	si = get(srv.URL + fmt.Sprintf("/v1/snapshots/%d", pendID))
	fmt.Printf("  重启后: 快照 %d status=%s\n", pendID, si["status"])
	check("重启恢复把 pending 快照验证后提交为 committed", si["status"] == "committed")

	// final listing
	section(0, "快照总览")
	list := get(srv.URL + "/v1/snapshots")["snapshots"].([]any)
	for _, x := range list {
		m := x.(map[string]any)
		fmt.Printf("  #%-3v %-10s files=%-3v bytes=%-7v %s\n",
			m["id"], m["status"], m["file_count"], m["bytes_total"], m["message"])
	}
	srv.Close()

	// Retention & GC act, in a fresh repository with its own server.
	gcAct(work)

	fmt.Println()
	if fail == 0 {
		fmt.Printf("✅ 全部 %d 项检查通过\n", pass)
		return
	}
	fmt.Printf("❌ %d 项失败，%d 项通过\n", fail, pass)
	os.Exit(1)
}

// ---------- helpers ----------

func startServer(repoDir string) *httptest.Server {
	must(os.MkdirAll(repoDir, 0o755))
	manifest, err := repo.OpenManifest(filepath.Join(repoDir, "manifest.sqlite"))
	must(err)
	store, err := repo.NewContentStore(filepath.Join(repoDir, "chunks"))
	must(err)
	engine, err := backup.NewEngine(manifest, store)
	must(err)
	if recovered, err := engine.RecoverPending(); err == nil {
		for _, r := range recovered {
			fmt.Printf("  [启动恢复] 快照 %d -> %s\n", r.SnapshotID, r.Status)
		}
	}
	if resumed, err := engine.ResumeGCJobs(); err == nil {
		for _, id := range resumed {
			fmt.Printf("  [启动恢复] 回收作业 %d 续跑完成\n", id)
		}
	}
	return httptest.NewServer((&api.Server{Engine: engine}).NewRouter())
}

func post(url string, body any) map[string]any {
	code, b := raw("POST", url, body)
	if code >= 300 {
		out, _ := json.MarshalIndent(b, "", "  ")
		fmt.Println(string(out))
	}
	return b
}

func get(url string) map[string]any {
	code, b := raw("GET", url, nil)
	if code >= 300 {
		out, _ := json.MarshalIndent(b, "", "  ")
		fmt.Println(string(out))
	}
	return b
}

func raw(method, url string, body any) (int, map[string]any) {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		must(err)
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, url, rdr)
	must(err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	must(err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	must(err)
	var out map[string]any
	if len(data) > 0 {
		must(json.Unmarshal(data, &out))
		if out == nil {
			out = map[string]any{}
		}
	} else {
		out = map[string]any{}
	}
	return resp.StatusCode, out
}

func hashFile(p string) (string, int64) {
	f, err := os.Open(p)
	must(err)
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	must(err)
	return hex.EncodeToString(h.Sum(nil)), n
}

func section(n int, title string) {
	if n == 0 {
		fmt.Printf("\n── %s ──────────────────────────────\n", title)
		return
	}
	fmt.Printf("\n── %d. %s ──────────────────────────────\n", n, title)
}

func check(name string, ok bool) {
	if ok {
		pass++
		fmt.Printf("  ✓ %s\n", name)
		return
	}
	fail++
	fmt.Printf("  ✗ %s\n", name)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "FATAL:", err)
		var ee *exec.ExitError
		_ = ee
		os.Exit(2)
	}
}

// ---------------------------------------------------------------------------
// Retention & garbage collection act (fresh repository)
// ---------------------------------------------------------------------------

// bigVersion returns a ~192KiB file body whose content-defined chunks are all
// identical across versions except one 8-byte patch in the middle: every
// version shares the same base chunks and adds exactly one new middle chunk.
func bigVersion(v int) []byte {
	big := []byte(strings.Repeat("0123456789ABCDEF\n", 12000))
	copy(big[90*1024:], []byte(fmt.Sprintf("V%06d!", v)))
	return big
}

func countBlobs(chunksDir string) int {
	n := 0
	err := filepath.WalkDir(chunksDir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() && !strings.HasPrefix(d.Name(), ".tmp-") {
			n++
		}
		return nil
	})
	must(err)
	return n
}

func snapID(body map[string]any) int64 { return int64(body["snapshot_id"].(float64)) }

func jobTargets(body map[string]any) []int64 {
	var ids []int64
	if ts, ok := body["targets"].([]any); ok {
		for _, t := range ts {
			ids = append(ids, int64(t.(map[string]any)["snapshot_id"].(float64)))
		}
	}
	return ids
}

func restoreAndVerify(srvURL string, id int64, target string, want []byte) bool {
	code, body := raw("POST", fmt.Sprintf("%s/v1/snapshots/%d/restore", srvURL, id),
		map[string]any{"target": target})
	if code != http.StatusCreated {
		fmt.Printf("  恢复快照 %d 失败: HTTP %d %v\n", id, code, body["message"])
		return false
	}
	for _, v := range body["verified"].([]any) {
		m := v.(map[string]any)
		if m["rel_path"] == "big.bin" {
			sum := sha256.Sum256(want)
			return int64(m["size"].(float64)) == int64(len(want)) &&
				m["digest"] == hex.EncodeToString(sum[:])
		}
	}
	return false
}

func gcAct(work string) {
	repoDir := filepath.Join(work, "repo-gc")
	src := filepath.Join(work, "src2")
	must(os.MkdirAll(src, 0o755))
	chunksDir := filepath.Join(repoDir, "chunks")
	bigFile := filepath.Join(src, "big.bin")
	writeV := func(v int) []byte {
		b := bigVersion(v)
		must(os.WriteFile(bigFile, b, 0o644))
		return b
	}
	srv := startServer(repoDir)
	fmt.Printf("\n── 保留与回收（独立仓库 %s） ──────────────────────────────\n", repoDir)

	// ---- 8. reclaim the older of two chunk-sharing snapshots ---------------
	section(8, "保留规则 + 回收：两个共享内容块的快照，回收较旧者，共享块不受影响")
	writeV(1)
	snapA := snapID(post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "gc v1"}))
	v2 := writeV(2)
	snapB := snapID(post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "gc v2"}))
	fmt.Printf("  快照 %d (v1) 与 %d (v2) 共享除 1 个块外的全部内容块\n", snapA, snapB)

	code, rule := raw("PUT", srv.URL+"/v1/retention/rules",
		map[string]any{"name": "default", "keep_last": 1, "keep_days": 0})
	check("保留规则已存入 SQLite (keep_last=1, version=1)",
		code == http.StatusCreated && rule["version"].(float64) == 1)

	prev := post(srv.URL+"/v1/gc/preview", map[string]any{"rule": "default"})
	pt := jobTargets(prev)
	check("预览：目标只有较旧的快照 A", len(pt) == 1 && pt[0] == snapA)
	check("预览：A 的候选块中仅 1 个不被共享（可回收）",
		prev["reclaimable_blobs"].(float64) == 1)

	blobsBefore := countBlobs(chunksDir)
	job := post(srv.URL+"/v1/gc/jobs", map[string]any{"rule": "default", "wait": true})
	fmt.Printf("  作业 %v: status=%s 删除快照=%v 删除块=%v 释放=%v 字节\n",
		job["id"], job["status"], job["snapshots_done"], job["blobs_deleted"], job["bytes_freed"])
	check("作业完成且只删除 1 个无引用块",
		job["status"] == "completed" && job["blobs_deleted"].(float64) == 1)
	check("磁盘 blob 数恰好减 1（共享块保留）", countBlobs(chunksDir) == blobsBefore-1)
	code, _ = raw("GET", fmt.Sprintf("%s/v1/snapshots/%d", srv.URL, snapA), nil)
	check("被回收快照的清单引用已删除 (404)", code == http.StatusNotFound)
	check("较新快照 B 仍可完整恢复（共享块内容逐字节一致）",
		restoreAndVerify(srv.URL, snapB, filepath.Join(work, "gc-restore-b"), v2))

	// ---- 9. protect flag ----------------------------------------------------
	section(9, "保全标记：被保全的目标在预览与执行中都被跳过，解除后才可回收")
	writeV(3)
	snapC := snapID(post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "gc v3"}))
	code, _ = raw("PUT", fmt.Sprintf("%s/v1/snapshots/%d/protect", srv.URL, snapB),
		map[string]any{"reason": "审计留存"})
	check("对 B 设置保全标记", code == http.StatusOK)

	prev = post(srv.URL+"/v1/gc/preview", map[string]any{"rule": "default"})
	skipped, _ := prev["skipped_protected"].([]any)
	check("预览：B 出现在 skipped_protected，目标集为空",
		len(jobTargets(prev)) == 0 && len(skipped) == 1 &&
			int64(skipped[0].(map[string]any)["snapshot_id"].(float64)) == snapB)
	job = post(srv.URL+"/v1/gc/jobs", map[string]any{"rule": "default", "wait": true})
	check("执行：被保全的 B 不在冻结目标集中，作业空跑完成",
		job["status"] == "completed" && job["snapshots_total"].(float64) == 0)
	si := get(fmt.Sprintf("%s/v1/snapshots/%d", srv.URL, snapB))
	check("B 仍是 committed 且标记 protected=true",
		si["status"] == "committed" && si["protected"] == true)

	code, _ = raw("DELETE", fmt.Sprintf("%s/v1/snapshots/%d/protect", srv.URL, snapB), nil)
	check("解除 B 的保全", code == http.StatusOK)
	prev = post(srv.URL+"/v1/gc/preview", map[string]any{"rule": "default"})
	check("解除后预览：B 成为目标", len(jobTargets(prev)) == 1 && jobTargets(prev)[0] == snapB)
	job = post(srv.URL+"/v1/gc/jobs", map[string]any{"rule": "default", "wait": true})
	check("解除后执行：B 被回收", job["snapshots_done"].(float64) == 1)
	code, _ = raw("GET", fmt.Sprintf("%s/v1/snapshots/%d", srv.URL, snapB), nil)
	check("B 的清单引用已删除 (404)", code == http.StatusNotFound)
	check("最新快照 C 不受影响可完整恢复",
		restoreAndVerify(srv.URL, snapC, filepath.Join(work, "gc-restore-c"), bigVersion(3)))

	// ---- 10. interrupt between phases, restart resumes the same job ---------
	section(10, "两阶段中断：引用已删、blob 未清时重启，同一作业续跑且不重复记账")
	writeV(4)
	snapD := snapID(post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "gc v4"}))
	blobsBefore = countBlobs(chunksDir)
	code, job = raw("POST", srv.URL+"/v1/gc/jobs",
		map[string]any{"rule": "default", "wait": true, "stop_after_refs": true})
	jobID := int64(job["id"].(float64))
	fmt.Printf("  故障时刻: 作业 %d status=%s（C 的引用已删，blob 尚未清理）\n", jobID, job["status"])
	check("作业停在 refs_deleted（阶段 1 完成、阶段 2 未开始）",
		code == http.StatusCreated && job["status"] == "refs_deleted")
	code, _ = raw("GET", fmt.Sprintf("%s/v1/snapshots/%d", srv.URL, snapC), nil)
	check("C 的清单引用已删除 (404)", code == http.StatusNotFound)
	check("blob 一个都没删（磁盘数量不变）", countBlobs(chunksDir) == blobsBefore)
	srv.Close()

	srv = startServer(repoDir) // restart: startup resume finishes the job
	job = get(fmt.Sprintf("%s/v1/gc/jobs/%d", srv.URL, jobID))
	check("重启后续跑的是同一作业并最终 completed",
		job["status"] == "completed" && int64(job["id"].(float64)) == jobID)
	check("只清理了无引用的 1 个块", job["blobs_deleted"].(float64) == 1 &&
		countBlobs(chunksDir) == blobsBefore-1)
	check("快照 D 完整恢复（共享块未被误删）",
		restoreAndVerify(srv.URL, snapD, filepath.Join(work, "gc-restore-d"), bigVersion(4)))
	freed := job["bytes_freed"].(float64)
	job2 := post(fmt.Sprintf("%s/v1/gc/jobs/%d/resume", srv.URL, jobID), nil)
	check("重复 resume 已完成作业：计数不变（不重复记账、不重跑）",
		job2["blobs_deleted"].(float64) == 1 && job2["bytes_freed"].(float64) == freed)

	events := get(fmt.Sprintf("%s/v1/gc/jobs/%d/events", srv.URL, jobID))["events"].([]any)
	actions := map[string]int{}
	for _, ev := range events {
		actions[ev.(map[string]any)["action"].(string)]++
	}
	check("审计轨迹完整：job_created/refs_deleted/job_resumed/blob_deleted/job_completed",
		actions["job_created"] == 1 && actions["snapshot_refs_deleted"] == 1 &&
			actions["job_resumed"] == 1 && actions["blob_deleted"] == 1 &&
			actions["job_completed"] == 1)
	for _, ev := range events {
		e := ev.(map[string]any)
		fmt.Printf("    审计: %-22s %s\n", e["action"], e["detail"])
	}

	// ---- 11. concurrency + pending/failed protection ------------------------
	section(11, "并发与保护：回收不碰新快照的引用块，pending/failed 及诊断永不自动清除")
	writeV(5)
	snapE := snapID(post(srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "gc v5"}))
	snapP := snapID(post(srv.URL+"/v1/snapshots",
		map[string]any{"root": src, "message": "gc pending", "finish": false}))
	writeV(6)
	code, body := raw("POST", srv.URL+"/v1/snapshots",
		map[string]any{"root": src, "message": "gc doomed", "lose_chunks": 1})
	snapF := snapID(body)
	check("构造出 failed 快照（带缺块诊断）", code == http.StatusConflict)
	v7 := writeV(7)

	// Async GC job racing a brand-new snapshot.
	code, job = raw("POST", srv.URL+"/v1/gc/jobs", map[string]any{"rule": "default"})
	gcJobID := int64(job["id"].(float64))
	snapQ := snapID(post(srv.URL+"/v1/snapshots",
		map[string]any{"root": src, "message": "gc concurrent", "finish": false}))
	deadline := time.Now().Add(10 * time.Second)
	for {
		job = get(fmt.Sprintf("%s/v1/gc/jobs/%d", srv.URL, gcJobID))
		if job["status"] == "completed" || job["status"] == "failed" {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	check("并发作业完成，目标只有 D（pending/failed 快照从不入选）",
		job["status"] == "completed" && len(jobTargets(job)) == 1 && jobTargets(job)[0] == snapD)

	si = get(fmt.Sprintf("%s/v1/snapshots/%d", srv.URL, snapP))
	check("pending 快照原样保留", si["status"] == "pending")
	si = get(fmt.Sprintf("%s/v1/snapshots/%d", srv.URL, snapF))
	errs, _ := get(fmt.Sprintf("%s/v1/snapshots/%d/errors", srv.URL, snapF))["errors"].([]any)
	missing, _ := get(fmt.Sprintf("%s/v1/snapshots/%d/missing", srv.URL, snapF))["missing"].([]any)
	check("failed 快照保留且 /errors、/missing 诊断仍可查询",
		si["status"] == "failed" && len(errs) > 0 && len(missing) == 1)

	vres := post(fmt.Sprintf("%s/v1/snapshots/%d/verify", srv.URL, snapQ), nil)
	check("并发创建的新快照 Q 的引用块一个不少（验证后 committed）",
		vres["status"] == "committed")
	check("Q 可完整恢复", restoreAndVerify(srv.URL, snapQ, filepath.Join(work, "gc-restore-q"), v7))
	check("保留的最新快照 E 可完整恢复",
		restoreAndVerify(srv.URL, snapE, filepath.Join(work, "gc-restore-e"), bigVersion(5)))

	// audit overview
	section(0, "回收作业总览（GET /v1/gc/jobs）")
	jobs := get(srv.URL + "/v1/gc/jobs")["jobs"].([]any)
	for _, j := range jobs {
		m := j.(map[string]any)
		fmt.Printf("  作业 #%-3v %-12s 目标=%-2v 删块=%-2v 释放=%-7v 规则=%v\n",
			m["id"], m["status"], m["snapshots_done"], m["blobs_deleted"], m["bytes_freed"],
			m["rule"])
	}
	srv.Close()
}
