# incbackup — 可验证的本地增量备份服务（仅 API）

“备份任务显示成功，恢复时却缺了一段文件”——本服务用一个硬规则拦住它：

> **快照在提交之前，必须逐个核对清单引用的每个内容块在内容仓中真实存在且长度正确；
> 恢复时再对流过的每个块和每个整文件做 SHA-256 与长度核对。**

任何一块对不上，快照就是 `failed`（或崩溃留下的 `pending`，重启后自动复验），
永远不会出现“成功”的快照恢复出残缺目录。

仓库长期保留快照会持续增长，因此服务内置**保留规则 + 两阶段回收**：
回收只处理无保全标记的已提交快照，先冻结目标集与规则版本，
删完清单引用后再次确认没有任何 `entry_chunks` 引用才删 blob——
共享内容块、pending/failed 快照及其故障诊断都不会被误清。

## 技术栈

| 组件 | 选择 | 用途 |
|---|---|---|
| 分块 | `github.com/restic/chunker`（Rabin 指纹内容定义分块） | 小改动只产生 1 个新块，其余块哈希相同直接复用 |
| 清单 | SQLite（`modernc.org/sqlite`，纯 Go，无 CGO） | 快照、条目、块索引、错误记录、保留规则、保全标记、回收作业与审计 |
| 内容仓 | 独立目录 `<repo>/chunks/ab/cdef…` | SHA-256 内容寻址、去重、只读不可变 blob |
| 接口 | 本地 HTTP API（默认 `127.0.0.1:8090`） | 无 UI、无鉴权，设计为只监听本地 |

分块多项式持久化在 `meta` 表中，跨快照/跨重启保持一致——否则边界漂移会让增量失效。

## 目录结构

```
cmd/backupd/main.go          HTTP 服务（启动时自动复验 pending 快照、续跑中断的回收作业）
cmd/demo/main.go             端到端演示（走真实 HTTP API，含 54 项断言）
internal/repo/
  contentstore.go            内容寻址块仓（原子写、读时校验摘要、分片目录）
  manifest.go                SQLite schema 与快照状态机（pending/committed/failed）
  manifest_write.go          条目/块写入、缺块诊断查询
  retention.go               保留规则（版本化）、保全标记、回收作业状态与审计事件
  meta.go                    分块多项式持久化
internal/backup/
  scan.go                    不跟随链接的目录扫描、分块、整文件摘要、写入中重读
  engine.go                  快照编排、提交前逐块验证、恢复与全部安全约束
  gc.go                      回收预览、作业冻结、可恢复两阶段执行、重启续跑
  util_linux.go              O_EXCL|O_NOFOLLOW 建文件（阻止沿预置符号链接写出）
internal/api/server.go       HTTP 路由
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
| `PUT  /v1/snapshots/{id}/protect` | 给快照加保全标记（`{"reason":"…"}`），回收一律跳过 |
| `DELETE /v1/snapshots/{id}/protect` | 解除保全标记 |
| `PUT  /v1/retention/rules` | 新建/更新保留规则（`name`+`keep_last`/`keep_days`，每次更新产生新版本） |
| `GET  /v1/retention/rules`、`/{name}` | 查询当前规则（含版本号） |
| `POST /v1/gc/preview` | 回收预览：按 `{"rule":"name"}` 或内联 `keep_last/keep_days` 给出目标集、被保全跳过项、可回收块数/字节 |
| `POST /v1/gc/jobs` | 执行回收：先冻结目标集与规则版本再跑两阶段；`wait:true` 同步返回，`stop_after_refs:true` 为中断演练参数 |
| `GET  /v1/gc/jobs`、`/{id}` | 作业进度：状态、计数器、冻结目标集、块清扫进度 |
| `POST /v1/gc/jobs/{id}/resume` | 续跑未完成作业（已完成作业直接返回原结果，绝不重跑） |
| `GET  /v1/gc/jobs/{id}/events` | 审计轨迹：冻结、删引用、逐块删除/保留、续跑、完成 |

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

# 保留与回收
curl -s -XPUT localhost:8090/v1/retention/rules \
  -d '{"name":"default","keep_last":7,"keep_days":30}'
curl -s -XPUT localhost:8090/v1/snapshots/3/protect -d '{"reason":"审计留存"}'
curl -s -XPOST localhost:8090/v1/gc/preview -d '{"rule":"default"}'
curl -s -XPOST localhost:8090/v1/gc/jobs -d '{"rule":"default","wait":true}'
curl -s localhost:8090/v1/gc/jobs/2/events
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
8. **回收不误删**：回收作业只处理**无保全标记的已提交快照**；执行前先在一个事务里冻结
   目标集、候选块集与规则版本（`gc_jobs`/`gc_job_targets`/`gc_job_chunks`），
   阶段 1 逐快照事务化删除清单引用，阶段 2 对每个候选块**重新确认**没有任何
   `entry_chunks` 引用后才删 blob。整个作业与快照创建共用同一把引擎锁，
   并发新快照的引用块永远不会被清扫；作业中断后按持久化状态幂等续跑，
   不重复记账、不重跑已完成作业，全程写入 `gc_events` 审计轨迹。

## 演示会依次证明

1. 基线快照 → 恢复到新目录，逐文件核对摘要与长度（含空文件、0750 脚本、符号链接）；
2. 在 160KB 文件中部改 8 字节：**新块=1，复用旧块=4**，恢复结果与源一致；
3. 恢复到已存在目录 → `409 target_exists`；
4. 写入在重读窗口内停止 → 重读后成功；持续写入 → 3 次重读后拒绝并点名；
5. `lose_chunks:1` 模拟提交中断 → `failed` + `/missing` 给出精确缺块，旧快照仍可恢复；
6. 指向根目录外的符号链接 → 恢复 `422`，半成品目录回滚，外部文件不被触及；
7. `finish:false` 制造 pending → 重启服务后自动复验为 committed；
8. 两个共享内容块的快照中回收较旧者：共享 blob 保留，较新快照完整恢复；
9. 对目标加保全 → 预览与执行都跳过；解除保全后才被回收；
10. `stop_after_refs` 在“引用已删、blob 未清”处中断 → 重启后同一作业续跑，
    只清无引用块、计数不重复；
11. 回收与新快照并发：新快照的引用块一个不少，pending/failed 快照及
    `/errors`、`/missing` 诊断全部保留可查。
