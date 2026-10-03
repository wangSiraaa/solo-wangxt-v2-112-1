# incbackup — 可验证的本地增量备份服务（仅 API）

“备份任务显示成功，恢复时却缺了一段文件”——本服务用一个硬规则拦住它：

> **快照在提交之前，必须逐个核对清单引用的每个内容块在内容仓中真实存在且长度正确；
> 恢复时再对流过的每个块和每个整文件做 SHA-256 与长度核对。**

任何一块对不上，快照就是 `failed`（或崩溃留下的 `pending`，重启后自动复验），
永远不会出现“成功”的快照恢复出残缺目录。

## 技术栈

| 组件 | 选择 | 用途 |
|---|---|---|
| 分块 | `github.com/restic/chunker`（Rabin 指纹内容定义分块） | 小改动只产生 1 个新块，其余块哈希相同直接复用 |
| 清单 | SQLite（`modernc.org/sqlite`，纯 Go，无 CGO） | 快照、条目、块索引、错误记录 |
| 内容仓 | 独立目录 `<repo>/chunks/ab/cdef…` | SHA-256 内容寻址、去重、只读不可变 blob |
| 接口 | 本地 HTTP API（默认 `127.0.0.1:8090`） | 无 UI、无鉴权，设计为只监听本地 |

分块多项式持久化在 `meta` 表中，跨快照/跨重启保持一致——否则边界漂移会让增量失效。

## 目录结构

```
cmd/backupd/main.go          HTTP 服务（启动时自动复验 pending 快照）
cmd/demo/main.go             端到端演示（走真实 HTTP API，含 58 项断言）
internal/repo/
  contentstore.go            内容寻址块仓（原子写、读时校验摘要、分片目录）
  manifest.go                SQLite schema 与快照状态机（pending/committed/failed/reclaimed）
  manifest_write.go          条目/块写入、缺块诊断查询
  retention.go               版本化保留规则、保全标记、候选评估
  gc.go                      GC 作业冻结、两阶段删除状态机、审计
  meta.go                    分块多项式持久化
internal/backup/
  scan.go                    不跟随链接的目录扫描、分块、整文件摘要、写入中重读
  engine.go                  快照编排、提交前逐块验证、恢复与全部安全约束
  gc.go                      回收作业执行（冻结→阶段一→阶段二）、断点续跑、并发互斥
  util_linux.go              O_EXCL|O_NOFOLLOW 建文件（阻止沿预置符号链接写出）
internal/api/
  server.go                  快照相关 HTTP 路由
  retention_handlers.go      保留/保全/回收/审计 HTTP 路由
```

## 快速开始

```bash
go run ./cmd/demo            # 端到端演示（临时目录，自动清理）
go test ./...                # 单元测试
go run ./cmd/backupd --repo ./backup-repo --addr 127.0.0.1:8090
```

## HTTP API

| 方法与路径 | 说明 |
|---|---|
| `POST /v1/snapshots` | 扫描 `root` → 落块 → **逐块验证** → 提交。`finish:false`、`lose_chunks:N` 为故障演练参数 |
| `GET  /v1/snapshots` | 列出全部快照（含 failed，失败记录不删除） |
| `GET  /v1/snapshots/{id}` | 单个快照状态 |
| `GET  /v1/snapshots/{id}/missing` | **维护入口**：列出每个缺块的文件路径、SHA-256、期望磁盘路径与原因 |
| `GET  /v1/snapshots/{id}/errors` | 扫描/验证阶段的逐条错误（stage、rel_path、chunk_digest） |
| `POST /v1/snapshots/{id}/verify` | 对 pending 快照重新执行逐块验证并提交/判失败 |
| `POST /v1/snapshots/{id}/restore` | 恢复到**全新**目录，返回逐文件长度+摘要+块数报告 |
| `POST /v1/recover` | 复验所有 pending 快照（服务启动时也会自动执行） |
| `PUT  /v1/snapshots/{id}/hold` | 对快照加**保全标记**（法律保留/取证），回收永不再选中它 |
| `DELETE /v1/snapshots/{id}/hold` | 解除保全（解除后才可被回收） |
| `GET  /v1/snapshots/{id}/holds` | 查询某快照当前生效的保全 |
| `POST /v1/retention/rules` | 新增**版本化保留规则**（不可变）：`keep_last_n` / `max_age_days`，至少一个 >0 |
| `GET  /v1/retention/rules/latest` | 当前生效规则版本 |
| `GET  /v1/retention/preview` | **回收预览**：候选墓碑、被保全跳过者、pending/failed 计数、仅候选独占的 blob 清单与字节数 |
| `POST /v1/retention/gc` | 冻结目标集+规则版本并执行两阶段回收（无规则返回 409）；支持续跑已有失败作业 |
| `GET  /v1/retention/gc` | 作业列表与状态 |
| `GET  /v1/retention/gc/{jid}` | 单作业进度：阶段、各 target/blob 状态、删除/保留/字节计数 |
| `POST /v1/retention/gc/{jid}/resume` | 续跑中断作业；对已成功作业只回显同一结果，绝不重新执行 |
| `GET  /v1/retention/gc/{jid}/audit` | 单作业审计事件流 |
| `GET  /v1/retention/audit` | 全局审计（规则、保全、回收、每个 blob 的删除/保留决定） |

### 典型请求

```bash
curl -s -XPOST localhost:8090/v1/snapshots \
  -d '{"root":"/srv/data","message":"nightly"}'
# 201 {"snapshot_id":7,"status":"committed","chunks_new":1,"chunks_referenced":5}

curl -s localhost:8090/v1/snapshots/7/missing
# {"snapshot_id":7,"status":"failed","missing":[
#   {"rel_path":"app.log",
#    "chunk_digest":"d2a8d66b…",
#    "expected_blob_path":"/…/chunks/d2/a8d66b…",
#    "reason":"chunk blob missing or length mismatch in content store"}]}

curl -s -XPOST localhost:8090/v1/snapshots/7/restore \
  -d '{"target":"/restore/2026-09-29"}'

# 保留：保留最新 7 个或 30 天内的已提交快照
curl -s -XPOST localhost:8090/v1/retention/rules \
  -d '{"keep_last_n":7,"max_age_days":30,"note":"nightly policy"}'
# 201 {"version":3,"keep_last_n":7,"max_age_days":30,...}

curl -s localhost:8090/v1/retention/preview
# {"rule_version":3,"candidates":[{"snapshot_id":2,...}],
#  "skipped_held_snapshots":[],"pending_snapshots":0,"failed_snapshots":1,
#  "exclusive_blobs":[{"digest":"ab12…","length":65536}],"exclusive_bytes":65536}

# 对取证快照加保全 → 预览与执行都跳过；解除后才可回收
curl -s -XPUT  localhost:8090/v1/snapshots/2/hold -d '{"reason":"legal W9"}'
curl -s -XDELETE localhost:8090/v1/snapshots/2/hold

curl -s -XPOST localhost:8090/v1/retention/gc
# 200 {"job_id":4,"status":"succeeded","resumed":false,"targets_done":1,
#      "blob_count":1,"blobs_deleted":1,"bytes_freed":65536,...}
curl -s localhost:8090/v1/retention/gc/4/audit   # 每个 blob 的删除/保留决定
```

## 关键正确性保证

1. **完成前验证所有内容块**：`snapshots.status` 只有 pending→committed/failed。
   提交前 `FindMissingChunks` 同时检查（a）清单里是否有块行、（b）blob 是否存在且长度一致；
   每块在恢复读取时再做流式 SHA-256 校验，每个文件组装后比对整文件摘要与总长度。
2. **扫描中正在写入的文件**：读取前后比对 size+mtime，并增加读后置静窗口
   （防止小文件恰好在两次 append 之间被整文件读完）。检测到变化→整块重读（最多 3 次）；
   仍在变→快照 `failed`，错误明确点名文件，绝不猜测版本。
3. **权限与符号链接本身保留**：保存并恢复目录/文件权限位、属主（root 时）、mtime；
   符号链接存的是链接本身与目标字符串，扫描与恢复均不跟随。
4. **不越界**：恢复前校验清单路径无绝对路径/`..`；符号链接目标按词法解析，解析后必须仍在恢复根内；
   任何现存祖先目录是符号链接一律拒绝；Linux 下用 `O_EXCL|O_NOFOLLOW` 建文件。
5. **不覆盖**：恢复目标已存在（任何类型）直接 `409`；恢复中途失败自动删除半成品目录。
6. **空文件**：长度 0、整文件摘要 `e3b0c442…`、0 个内容块，正常备份与恢复。
7. **失败可定位**：failed/pending 快照永久保留，`/missing` 直接给出“哪个文件的哪个块该在哪个路径”，
   而不是只看到队列空了。

## 本地保留与回收（retention / hold / GC）

长期保留快照会持续占空间，维护作业负责释放过期内容，但有一条硬底线：

> **blob 只有在冻结、阶段一删引用之后，再次确认已无任何 `entry_chunks` 引用时才会被删除；
> pending/failed 快照与加了保全标记的快照永远不进目标集，其诊断（`/errors`、`/missing`）永不被清除。**

机制：

1. **保留规则版本化且不可变**：`retention_rules` 只追加新版本，每个 GC 作业冻结所依据的
   `rule_version`；事后改规则不能移动已冻结作业的目标。选择器为 `keep_last_n`（保留最新 N 个）
   与 `max_age_days`（保留 N 天内），二者必须至少一个为正，拒绝“一条规则清空一切”。
2. **保全（legal hold）**：`snapshot_holds` 带部分唯一索引，同一快照同时只有一个生效保全。
   保全中的快照即使已过期也只出现在预览的 `skipped_held_snapshots`，执行时跳过；解除后才可回收。
3. **先冻结再执行**：`gc_jobs` 创建时在**一个事务**里冻结规则版本、目标快照集（`gc_job_targets`）
   与仅候选独占的 blob 集（`gc_job_blobs`，digest+length 快照，无外键）。后续所有动作只驱动这些冻结行。
4. **只处理无保全的已提交快照**：pending/failed 在候选评估与阶段一事务里双重排除；
   冻结后才加的保全也会在阶段一被重新检查，该目标转为 `skipped_held`，引用原样保留。
5. **可恢复的两阶段删除**：
   - **阶段一**（每个目标一个事务，持引擎互斥锁）：删 `entry_chunks`/`entries`，
     快照变为 `reclaimed` 墓碑（`snapshot_errors` 故意保留），target→`done`。
   - **阶段二**（逐 blob）：`ApproveBlobDeletion` 在事务里再次 `SELECT count(*) FROM entry_chunks
     WHERE chunk_digest=?`——为 0 才删 `chunks` 行（blob→`row_removed`），随后 unlink 文件并置
     `deleted`；仍有引用（含并发新快照、pending/failed）则置 `kept_shared`，文件保留。
   - 崩溃停在 `row_removed`（目录行已删、文件还在）时，续跑会复查引用：被新快照重新引用则
     回退为 `kept_shared`，否则只删这一个孤儿文件。
6. **中断/重试不犯错**：每个步骤都以持久化状态为条件，天然幂等——不重复删块、不重复记账
   （计数在成功时由冻结子表一次性派生）；已成功作业不可重跑（续跑只回显同一结果），
   存在未完成作业时再次执行只会续跑同一作业。
7. **并发安全**：阶段一门控与快照提交共用引擎互斥锁；新快照与回收并发时，其引用要么在门控
   前完整提交（blob 被判 `kept_shared`），要么在 unlink 之后才开始（`Put` 重新落盘），
   绝不会留下悬空引用。
8. **全程审计**：`gc_audit` 追加记录规则变更、保全加/解、每个目标的回收/跳过、每个 blob 的
   删除/保留原因及字节数。

## 演示会依次证明

1. 基线快照 → 恢复到新目录，逐文件核对摘要与长度（含空文件、0750 脚本、符号链接）；
2. 在 160KB 文件中部改 8 字节：**新块=1，复用旧块=4**，恢复结果与源一致；
3. 恢复到已存在目录 → `409 target_exists`；
4. 写入在重读窗口内停止 → 重读后成功；持续写入 → 3 次重读后拒绝并点名；
5. `lose_chunks:1` 模拟提交中断 → `failed` + `/missing` 给出精确缺块，旧快照仍可恢复；
6. 指向根目录外的符号链接 → 恢复 `422`，半成品目录回滚，外部文件不被触及；
7. `finish:false` 制造 pending → 重启服务后自动复验为 committed；
8. 版本化规则 + 保全：预览与执行都跳过被保全快照，重复保全 `409`，解除后才出现在候选中；
9. **删除两个共享内容块快照中的较旧者**：仅其 1 个独占块被删，共享 blob 全部存活，较新快照可完整恢复；
10. **引用已删、blob 未清时模拟中断**：重启续跑同一作业 id，只清理无引用块，结果一致、不重复执行；
11. **并发与诊断保全**：回收不删新/pending/failed 快照仍引用的块，`/errors`、`/missing` 条数不变。
