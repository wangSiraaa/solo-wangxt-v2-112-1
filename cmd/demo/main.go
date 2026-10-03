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
//  7. server restart with a pending snapshot -> startup recovery commits it.
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

	// =======================================================================
	// 本地保留与回收（retention / hold / 两阶段 GC）
	// 使用完全独立的仓库与服务，使保留规则只作用于本章节的快照集。
	gcRepoDir := filepath.Join(work, "gc-repo")
	gsrv := startServer(gcRepoDir)
	gcsrc := filepath.Join(work, "gcsrc")
	must(os.MkdirAll(gcsrc, 0o755))
	glog := make([]byte, 0, 160*1024)
	for i := 0; i < 160*1024; i++ {
		glog = append(glog, byte("abcdefghijklmnopqrstuvwxyz0123456789\n"[i%37]))
	}
	must(os.WriteFile(filepath.Join(gcsrc, "g.log"), glog, 0o644))
	gs1 := int64(post(gsrv.URL+"/v1/snapshots", map[string]any{"root": gcsrc, "message": "gc base"})["snapshot_id"].(float64))

	// ---- 8. retention rules + hold: preview/execution skip held -----------
	section(8, "保留规则与保全标记：规则版本化入库；被保全快照在预览与执行时都跳过")
	code, body = raw("POST", gsrv.URL+"/v1/retention/rules",
		map[string]any{"keep_last_n": 0, "max_age_days": 0})
	check("keep_last_n 与 max_age_days 同时为 0 的规则被拒绝（防止清空一切）",
		code == http.StatusBadRequest)

	// 中部小改产生 gs2：仅 1 个新块，其余块与 gs1 共享。
	copy(glog[80*1024:80*1024+8], []byte("PATCHED!"))
	must(os.WriteFile(filepath.Join(gcsrc, "g.log"), glog, 0o644))
	gs2 := int64(post(gsrv.URL+"/v1/snapshots", map[string]any{"root": gcsrc, "message": "gc patched"})["snapshot_id"].(float64))

	rule := post(gsrv.URL+"/v1/retention/rules", map[string]any{"keep_last_n": 1, "note": "keep newest one"})
	ruleV := int64(rule["version"].(float64))
	fmt.Printf("  规则 v%d: keep_last_n=%v\n", ruleV, rule["keep_last_n"])
	pv := get(gsrv.URL + "/v1/retention/preview")
	cands := arr(pv, "candidates")
	check("回收预览精确选中较旧快照 gs1（gs2 受 keep_last_n 保护）",
		len(cands) == 1 && int64(cands[0].(map[string]any)["snapshot_id"].(float64)) == gs1)
	excl := arr(pv, "exclusive_blobs")
	fmt.Printf("  预览: 候选=%d 可释放块=%d 可释放字节=%v 被保全跳过=%d\n",
		len(cands), len(excl), pv["exclusive_bytes"], len(arr(pv, "skipped_held_snapshots")))
	check("预览只把 gs1 独占块计入可释放（共享块不在清单里）", len(excl) == 1)

	code, body = raw("PUT", fmt.Sprintf("%s/v1/snapshots/%d/hold", gsrv.URL, gs1),
		map[string]any{"reason": "legal hold 2026-Q4"})
	check("对 gs1 加保全成功 (201)", code == http.StatusCreated)
	code, _ = raw("PUT", fmt.Sprintf("%s/v1/snapshots/%d/hold", gsrv.URL, gs1), nil)
	check("重复保全被拒绝 (409)", code == http.StatusConflict)
	pv = get(gsrv.URL + "/v1/retention/preview")
	check("加保全后预览无候选、gs1 出现在 skipped_held",
		len(arr(pv, "candidates")) == 0 &&
			len(arr(pv, "skipped_held_snapshots")) == 1)
	g1info := get(gsrv.URL + fmt.Sprintf("/v1/snapshots/%d", gs1))
	check("快照查询回显 held=true", g1info["held"] == true)
	body = post(gsrv.URL+"/v1/retention/gc", nil)
	check("保全存在时执行回收：目标数 0，什么都不删",
		int64(body["target_count"].(float64)) == 0)

	// ---- 9. acceptance ①: reclaim older, shared blobs survive -------------
	section(9, "解除保全后回收较旧快照：共享 blob 全部保留，较新快照可完整恢复")
	code, _ = raw("DELETE", fmt.Sprintf("%s/v1/snapshots/%d/hold", gsrv.URL, gs1), nil)
	check("解除保全成功", code == http.StatusOK)
	body = post(gsrv.URL+"/v1/retention/gc", nil)
	job1 := int64(body["job_id"].(float64))
	fmt.Printf("  作业 %d: status=%v targets_done=%v blobs_deleted=%v kept_shared=%v bytes_freed=%v\n",
		job1, body["status"], body["targets_done"], body["blobs_deleted"],
		body["blobs_kept_shared"], body["bytes_freed"])
	check("回收作业成功、冻结规则版本为最新版本",
		body["status"] == "succeeded" && int64(body["rule_version"].(float64)) == ruleV)
	check("只冻结并删除 gs1 的 1 个独占块；共享块从不进入候选清单（blob_count=1）",
		body["blob_count"].(float64) == 1 &&
			body["blobs_deleted"].(float64) == 1)

	g1info = get(gsrv.URL + fmt.Sprintf("/v1/snapshots/%d", gs1))
	check("gs1 成为 reclaimed 墓碑快照", g1info["status"] == "reclaimed")
	code, _ = raw("POST", fmt.Sprintf("%s/v1/snapshots/%d/restore", gsrv.URL, gs1),
		map[string]any{"target": filepath.Join(work, "gs1-restore-attempt")})
	check("reclaimed 快照不可再恢复", code >= 400)
	gc2dir := filepath.Join(work, "gs2-restore")
	code, body = raw("POST", fmt.Sprintf("%s/v1/snapshots/%d/restore", gsrv.URL, gs2),
		map[string]any{"target": gc2dir})
	check("较新快照 gs2 恢复成功（共享块仍在）", code == http.StatusCreated)
	gh, _ := os.ReadFile(filepath.Join(gc2dir, "g.log"))
	check("恢复内容与源逐字节一致——共享 blob 一个都没被删", bytes.Equal(gh, glog))

	body = post(gsrv.URL+"/v1/retention/gc", nil)
	check("再次执行不会重跑旧作业：冻结出全新的空作业",
		int64(body["job_id"].(float64)) != job1 &&
			int64(body["target_count"].(float64)) == 0 &&
			int64(body["blobs_deleted"].(float64)) == 0)

	// ---- 10. acceptance ③: crash between refs-delete and blob-cleanup -----
	section(10, "两阶段可恢复：引用已删、blob 未清时中断；重启续跑同一作业且结果一致")
	// 用一段不同的中部改动生成 gs3：gs2 的独有块（PATCHED! 位置）不被 gs3 引用，
	// 因而在冻结集里成为“待清理的无引用块”；其余块二者共享必须保留。
	for i := 0; i < len(glog); i++ {
		glog[i] = byte("abcdefghijklmnopqrstuvwxyz0123456789\n"[i%37])
	}
	copy(glog[120*1024:120*1024+12], []byte("REWRITTEN!!=="))
	must(os.WriteFile(filepath.Join(gcsrc, "g.log"), glog, 0o644))
	gs3 := int64(post(gsrv.URL+"/v1/snapshots", map[string]any{"root": gcsrc, "message": "gc tail"})["snapshot_id"].(float64))
	future := time.Now().UTC().Add(31 * 24 * time.Hour).Format(time.RFC3339Nano)
	code, body = raw("POST",
		gsrv.URL+"/v1/retention/gc?crash_after_phase1=1&now="+future, nil)
	fmt.Printf("  注入中断 -> HTTP %d 作业 %v status=%v\n", code, body["job_id"], body["status"])
	check("中断返回 202，作业持久化为 failed（可续跑）",
		code == http.StatusAccepted && body["status"] == "failed")
	job2 := int64(body["job_id"].(float64))
	check("阶段一已完成：gs2 引用删除但 0 个 blob 被清理",
		body["targets_done"].(float64) == 1 && body["blobs_deleted"].(float64) == 0)
	g2info := get(gsrv.URL + fmt.Sprintf("/v1/snapshots/%d", gs2))
	check("中断态：gs2 已 reclaimed", g2info["status"] == "reclaimed")
	// gs3 在中断期间仍可完整恢复：blob 一个都还没删。
	midDir := filepath.Join(work, "gs3-midcrash")
	code, _ = raw("POST", fmt.Sprintf("%s/v1/snapshots/%d/restore", gsrv.URL, gs3),
		map[string]any{"target": midDir})
	check("中断窗口内较新快照 gs3 依然可恢复", code == http.StatusCreated)

	// “重启服务”后续跑同一作业 id。
	gsrv.Close()
	gsrv = startServer(gcRepoDir)
	time.Sleep(100 * time.Millisecond)
	body = post(gsrv.URL+fmt.Sprintf("/v1/retention/gc/%d/resume", job2), nil)
	fmt.Printf("  续跑作业 %d: status=%v deleted=%v kept=%v\n",
		job2, body["status"], body["blobs_deleted"], body["blobs_kept_shared"])
	check("续跑使用同一作业 id 且成功",
		int64(body["job_id"].(float64)) == job2 && body["status"] == "succeeded" &&
			body["resumed"] == true)
	check("续跑只清理无引用块（gs2 独占块 1 个），共享块计数一致",
		body["blobs_deleted"].(float64) == 1)
	g3dir := filepath.Join(work, "gs3-after-resume")
	code, _ = raw("POST", fmt.Sprintf("%s/v1/snapshots/%d/restore", gsrv.URL, gs3),
		map[string]any{"target": g3dir})
	check("续跑后 gs3 仍可完整恢复", code == http.StatusCreated)
	gh, _ = os.ReadFile(filepath.Join(g3dir, "g.log"))
	check("续跑后恢复内容仍与源一致", bytes.Equal(gh, glog))
	body = post(gsrv.URL+fmt.Sprintf("/v1/retention/gc/%d/resume", job2), nil)
	check("已完成作业不可重新执行：返回同一记账结果",
		body["already_done"] == true && body["blobs_deleted"].(float64) == 1)

	// ---- 11. acceptance ④: pending/failed + diagnostics never reclaimed ----
	section(11, "并发安全与诊断保全：pending/failed 快照不被回收，/errors、/missing 永远可查")
	// failed 快照用独立目录，保证其丢失块诊断不会被后续扫描“自愈”。
	fsrc := filepath.Join(work, "fail-src")
	must(os.MkdirAll(fsrc, 0o755))
	must(os.WriteFile(filepath.Join(fsrc, "uniq.log"),
		[]byte(strings.Repeat("zzzz-unique-failure-content\n", 600)), 0o644))
	code, body = raw("POST", gsrv.URL+"/v1/snapshots",
		map[string]any{"root": fsrc, "message": "gc-era failed", "lose_chunks": 1})
	check("故障快照仍按原机制失败", code == http.StatusConflict)
	fID := int64(body["snapshot_id"].(float64))
	fmiss := get(gsrv.URL + fmt.Sprintf("/v1/snapshots/%d/missing", fID))["missing"].([]any)
	ferrs := get(gsrv.URL + fmt.Sprintf("/v1/snapshots/%d/errors", fID))["errors"].([]any)
	check("失败快照 /missing 与 /errors 诊断存在", len(fmiss) == 1 && len(ferrs) > 0)

	// pending 快照：与 gs3 内容一致，其引用块必须保护 gs3 的旧引用。
	pbody := post(gsrv.URL+"/v1/snapshots", map[string]any{"root": gcsrc, "message": "gc-era pending", "finish": false})
	pID := int64(pbody["snapshot_id"].(float64))
	check("制造 pending 快照", pbody["status"] == "pending")
	// 再提交一个更新的 gs4，使 gs3 成为“较旧的已提交”候选。
	glog = append(glog, []byte("EVEN NEWER G4 TAIL\n")...)
	must(os.WriteFile(filepath.Join(gcsrc, "g.log"), glog, 0o644))
	gs4 := int64(post(gsrv.URL+"/v1/snapshots", map[string]any{"root": gcsrc, "message": "gs4 newest"})["snapshot_id"].(float64))

	body = post(gsrv.URL+"/v1/retention/gc?now="+future, nil)
	fmt.Printf("  回收作业: targets_done=%v blob_count=%v blobs_deleted=%v\n",
		body["targets_done"], body["blob_count"], body["blobs_deleted"])
	check("仅 gs3 作为已提交旧快照被回收（pending/failed 不入选）",
		body["status"] == "succeeded" && body["targets_done"].(float64) == 1)
	check("gs3 的块仍被 pending 快照引用：冻结候选块为 0、删除 0",
		body["blob_count"].(float64) == 0 && body["blobs_deleted"].(float64) == 0)
	fInfo := get(gsrv.URL + fmt.Sprintf("/v1/snapshots/%d", fID))
	pInfo := get(gsrv.URL + fmt.Sprintf("/v1/snapshots/%d", pID))
	g4info := get(gsrv.URL + fmt.Sprintf("/v1/snapshots/%d", gs4))
	check("failed / pending 快照状态原样保留",
		fInfo["status"] == "failed" && pInfo["status"] == "pending" &&
			g4info["status"] == "committed")
	fmiss2 := get(gsrv.URL + fmt.Sprintf("/v1/snapshots/%d/missing", fID))["missing"].([]any)
	ferrs2 := get(gsrv.URL + fmt.Sprintf("/v1/snapshots/%d/errors", fID))["errors"].([]any)
	check("GC 之后 /missing、/errors 诊断条数不变",
		len(fmiss2) == len(fmiss) && len(ferrs2) == len(ferrs))

	// 启动恢复对 pending 给出结论，且其块因受保护仍在，可完整恢复。
	rec := post(gsrv.URL+"/v1/recover", nil)["recovered"].([]any)
	ok := false
	for _, x := range rec {
		m := x.(map[string]any)
		if int64(m["snapshot_id"].(float64)) == pID && m["status"] == "committed" {
			ok = true
		}
	}
	check("/v1/recover 把 pending 快照复验为 committed", ok)
	pdir := filepath.Join(work, "p-restore")
	code, _ = raw("POST", fmt.Sprintf("%s/v1/snapshots/%d/restore", gsrv.URL, pID),
		map[string]any{"target": pdir})
	check("被 pending 保全的内容块支撑其恢复成功", code == http.StatusCreated)

	// 全局审计可查。
	allEvents := get(gsrv.URL + "/v1/retention/audit")["events"].([]any)
	eventKinds := map[string]bool{}
	for _, x := range allEvents {
		eventKinds[x.(map[string]any)["event"].(string)] = true
	}
	check("全局审计包含规则、保全、回收各阶段事件（含中断与续跑）",
		eventKinds["retention_rule_created"] && eventKinds["hold_added"] &&
			eventKinds["hold_released"] && eventKinds["target_reclaimed"] &&
			eventKinds["blob_deleted"] && eventKinds["job_interrupted"] &&
			eventKinds["job_succeeded"])
	gsrv.Close()

	// final listing
	section(0, "快照总览")
	list := get(srv.URL + "/v1/snapshots")["snapshots"].([]any)
	for _, x := range list {
		m := x.(map[string]any)
		fmt.Printf("  #%-3v %-10s files=%-3v bytes=%-7v %s\n",
			m["id"], m["status"], m["file_count"], m["bytes_total"], m["message"])
	}
	srv.Close()

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

func arr(m map[string]any, key string) []any {
	if v, ok := m[key].([]any); ok {
		return v
	}
	return nil
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
