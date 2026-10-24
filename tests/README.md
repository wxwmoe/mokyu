# 验证

构建和测试在隔离 Docker 环境中执行。每组数据库契约使用独立空库或全新 SQLite 文件；测试账户、S3 桶、data 目录和故障注入目录也必须专用，不能指向已有部署。

## Rust 检查

构建容器须安装 `protoc`，在源码目录运行：

```sh
cargo fmt --all -- --check
cargo test --locked --no-default-features --all-targets
cargo clippy --locked --all-targets --features fault-injection
cargo build --locked
```

数据库契约默认 ignored。设置 `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only`，再按后端提供 `MOKYU_TEST_DATABASE_URL_FILE`（PostgreSQL，保存 DSN 的文件）、`MOKYU_TEST_MYSQL_CONFIG` 或 `MOKYU_TEST_SQLITE_CONFIG`。配置文件包含测试数据库连接、固定部署密钥和独立 data 目录；各测试头部注明额外环境变量。例如：

```sh
MOKYU_TEST_SQLITE_CONFIG=/config/sqlite-accounts.toml \
  cargo test --locked --no-default-features sqlite_account -- --ignored --nocapture
```

每个原生契约覆盖对应接口、事务和故障恢复；原生 SQL 仅用于构造或检查本后端状态。不要用同一配置批量并发执行所有 ignored 测试，也不要把过滤后执行零个测试视为通过。曾经执行并写入数据的契约，应更换空库后重跑。

`postgres_node_sessions_recover_across_controls_and_fence_old_runs` 使用两个独立连接池检查身份代次、会话接管、并发同步与心跳、过期 fencing、退役 boot、历史上限、轮换和禁用。

`postgres_node_reads_close_dependencies_bound_windows_and_protect_sources` 验证读取依赖、材料复用、Range、超过首窗口的映射、双窗口背压、资源到期与节点隔离，以及区块、包、raw 和暂存来源在映射改写后的 GC 保护。

`rpc::client::transport_tests::reads::node_read_plans_reuse_materials_fetch_s3_and_survive_lost_replies` 使用下述双 control/TLS 夹具及独立 S3 存储。`MOKYU_TEST_READ_FIXTURE` 指向包含桶、读取凭据、预期正文路径和 worker 身份路径的 JSON；编排器增加 `change-read-profile`、`lose-read`、`resume-read`、`assert-no-reads` 动作，丢响应使用 `node-read-opened` 故障点。验证一次 RPC 后直接解密读取、材料和正文复用、分窗口读取与失去响应后的跨 control 重放。

`rpc::client::transport_tests::reads::node_read_resources_renew_reconnect_and_release_automatically` 复用读取夹具，增加 `record-read-expiry`、`assert-read-renewed` 和 `assert-live-read` 动作，比较实际到期时间与存续资源。验证冷/热 HEAD、正文多窗口预取、连续完整/Range 读取、自动续期、control 退出后的恢复及最后使用者退出后的释放。夹具可包含 `body_cases`（bucket、key、access_key、body_path、source），覆盖空对象、混合独立区块/Pack 和 raw；取消测试使用 `MOKYU_TEST_FAULT_DIR` 下的 `node-read-decode`，确认阻塞工作结束前保护不会提前释放。普通资源/材料/正文测试覆盖批量上限、迟到确认、缓存持有、容量/修订/部署校验、映射边界、返回切片计费，以及 raw 的摘要、长度和远端 ETag。

`rpc::client::transport_tests::reads::http::node_http_reads_use_one_rpc_and_verify_signatures` 使用同一读取夹具，在隔离网络的 19100/19101 端口提供节点 S3/Web 入口，并写入 `http-ports.json`。编排器响应 `http-signed`、`http-sources`、`http-public`、`http-proxy`、`http-compression-busy`、`http-rejected`、`http-pending`、`http-reconnected` 和 `http-fenced` 动作，分别发出真实 SDK、HTTP/1.1、h2c 及 TLS 代理请求。代理保持 Host 和签名路径，以 h2c 回源；用例检查实际入站协议、一次 OpenRead、签名错误优先级、无正文 I/O 的 HEAD/条件响应、压缩退让、来源释放及 control 恢复。`http-compression-finish` 故障点验证已开始压缩的响应在运行权失效时中断。夹具须包含空对象、Pack、raw、路径编码、公开域名（含端口）、CORS、目录页和错误页；pending 用例临时把该隔离 raw 来源设为其他节点尚未上传的来源，验证等待、就绪和桶修订变动后恢复。本用例验证读取适配层，不替代完整角色启动或实际 CDN 验收。

`rpc::client::transport_tests::reads::http::node_http_cache_validates_versions_and_recovers` 复用 HTTP 夹具并启用 16MiB 正文缓存。编排器的 `l1-*` 动作使用独立公开对象，检查三种 HTTP 链路的缓存命中、Range、identity 派生压缩、编码器繁忙时复用压缩体、ACL 拒绝、策略切换、对象替换和 control 恢复。Rust 断言后端 GET、命中计数与预算；`http-compression-finish` 暂停验证取消后不发布候选，`response-cache-body` 暂停验证无读取租约的缓存响应仍受运行权约束。对象和策略改动只作用于隔离夹具。

`rpc::client::transport_tests::reads::checks::node_checks_verify_bodies_and_report_owned_sources` 使用读取夹具及专用 worker，编排器处理 `checks-queued`、`checks-run-healthy`、`checks-background`、`checks-range-verified`、`checks-corrupt-raw`、`checks-quarantined-raw`、`checks-corrupt-chunk` 和 `checks-quarantined-chunk`。验证批量报告去重、读取/窗口/角色 fencing、重连重放、健康来源不隔离、后台 raw 摘要、Range 后端复核和加密区块损坏后的持久复核。`check-tasks.json` 提供待核对任务 ID；故障只能修改该夹具的后端对象。

`rpc::client::transport_tests::node_rpc_balances_tls_origins_and_recovers_sessions` 需两个真实 control、各自 TLS 源站和受限夹具编排器；设置 `MOKYU_TEST_NODE_STATE`、`MOKYU_TEST_CONTROL_URL`、`MOKYU_TEST_RPC_ACTIONS` 及测试 CA 的 `SSL_CERT_FILE`。测试在本机临时 UDP 端口提供可变 A 记录，两个地址为 `127.0.0.2/3`，`127.0.0.4` 提供错误域名证书。编排器按 actions 目录的 `change-settings`、`stop-first`、`start-first`、`lose-welcome`、`resume-welcome`、`rotate-node`、`disable-node` 文件执行动作，成功后写同名 `.done` 内容 `ok`；丢 Welcome 使用 `node-session-opened` 故障点。DNS 宽限到期用受控单调时钟推进，其他行为通过真实 DNS/TLS/gRPC 路径验证；不能作为实际 CDN 或跨洲验收结果。

`rpc::client::transport_tests::node_runtime_recovers_without_database_or_resurrecting_old_work` 使用同一隔离夹具，驱动不含数据库配置的节点上下文自动恢复；另需 `enable-maintenance`、`disable-maintenance`、`expire-run` 动作，最后一项仅把该测试节点的运行权设为已过期。覆盖 Welcome 丢失、DNS 迁移、源站退出、配置变更、维护、过期 fencing、排空重连、凭据轮换和取消后旧句柄失效。该契约验证节点上下文，不替代完整 data/worker 业务进程验收。

`sqlite_synchronous_contract` 验证默认 FULL、保存后重启生效、FULL/NORMAL/OFF 的写池与所有读连接、连接重建、事务回滚、单连接模式和错误设置拒绝。初始化/迁移在测试构建中断言 FULL。`sqlite_nonce_durability_contract` 检查 NORMAL 预留强制 FULL 后连接恢复、取消/提交边界、并发号段不重叠、OFF 保存与首个预留互斥、已有预留禁止 OFF 和启动校验；模拟进程退出不能代替断电持久性验证。

`python -m tests.recovery.sqlite_sync` 接管已初始化且停止的隔离应用，沿用恢复测试的配置、管理员密码、故障目录和本地存储根目录环境。SQLite 验证 API/CLI 保存、实际同步值、重启和 SIGKILL 读回；其他数据库验证仅提供公共 I/O 设置、不提供 SQLite 字段。四库都检查修订冲突返回 409。`web/tests/sqlite-sync.spec.ts` 使用实际应用检查三语同步/退出期限表单、取消不保存、键盘操作和重启提示。

`python -m tests.recovery.shutdown_flush` 使用相同的进程托管环境和独立 S3 后端，节点磁盘缓存至少64MiB、其中上传缓存至少32MiB；验证每次 flush 的数据库时间上界、CLI 等待超时不取消任务、SIGTERM 排空 raw/chunks、暂停 CDC 时原始来源仍可持久化、退出期限及重启恢复。四库分别运行，不能与同一实例上的其他测试并发。

`python -m tests.recovery.buffered_writes` 与 `python -m tests.recovery.buffered_chunks` 独占已初始化且停止的单容器服务，沿用上述配置、S3 后端与 fsync 注入环境。分别验证 raw 和 chunks 的提前确认、立即读取、可靠复制/复用屏障、缓存关闭/满额回退、文件与目录同步失败、强杀恢复、损坏隔离、flush 和关闭。raw 入口另验证首次文件写入前的持久性记录，以及来源创建后切换策略时的同步和状态一致性；区块入口另覆盖 Multipart 分片持久性、加密及后台组包竞态；四库分别运行，建议磁盘缓存128MiB、上传缓存64MiB。`web/tests/buffered-storage.spec.ts` 验证三语草稿取消/保存、键盘和窄屏提示。

`python -m tests.recovery.write_memory` 使用同样的独占服务和隔离 S3 夹具，为本次测试设置 8MiB 写内存和 64MiB 文件缓存，覆盖校验失败/断线释放、raw/chunks RAM 读取与 Range、最后读者计费、Copy/复用持久晋级、强杀后的失效隔离与重传、RAM/文件满额回退和 503、flush 失败保留、Multipart、最终 pack、后台 CDC、退出排空和 0B 回退。

`python -m tests.recovery.write_scheduling` 复用上述独占服务及隔离 S3 夹具，验证各后端合并等待、参数范围与秒数规范化、raw/chunks 强制排空、超龄拒绝/同步回退、后端隔离、满额提前排空、本地文件同步、强杀恢复和正常关闭，四库分别运行。

`python -m tests.recovery.cluster_shutdown` 使用双 data 的 `cluster_cache` 环境，编排器额外处理 `stop-peer`、`start-peer` 与 `.done` 标记。验证退出期间继续心跳、只排空当前节点的 raw/chunks/原生 Multipart、其他节点仍可读最终来源，以及只有 CDC 尾块可写共享暂存。

数据库改动需在 PostgreSQL 18、MySQL 8.0.19、MariaDB 10.6.3 的最低版本和当前维护版本验证。MariaDB 复用全部 MySQL 契约和 `MOKYU_TEST_MYSQL_CONFIG`；`mysql_dialect_and_lock_scope_contract` 额外检查 JSON 类型、大小写/尾空格比较和任务领取的锁范围。`migrations.py` 在 PostgreSQL 17 空库验证低版本拒绝且不写入表，在支持版本验证新装、当前库重启和迁移前检查。

`postgres_schema_bootstrap_contract`、`mysql_schema_bootstrap_contract` 对照当前快照与完整编号迁移的结构、初值和校验和，并验证迁移检查、重启、未知版本、历史损坏、新装失败与真实连接中断；拒绝前后结构、完整行数据和迁移历史保持一致。PostgreSQL 同时验证并发初始化。两者除上述连接配置外，测试用户还需有创建数据库的权限；会在同一隔离服务器创建 `mokyu_bootstrap_*` 测试库，验证后随专用测试实例一并清理。`sqlite_schema_bootstrap_contract` 使用捆绑 SQLite 内存库，属于普通 Rust 测试；磁盘/WAL/进程恢复由 `sqlite_runtime_and_migration_recovery` 覆盖。

增加编号迁移时同步更新对应 `migrations/schema/*.sql` 与迁移入口的快照版本。快照包含最终结构和必需初值，版本行及 `_sqlx_migrations` 由初始化器最后登记。保留原迁移内容与校验和；结构对照包含列、默认值、约束、索引、视图，以及 PostgreSQL 函数、触发器和序列。

`nonce_database_contract` 使用 `MOKYU_TEST_NONCE_CONFIG` 指向全新的隔离数据库，并要求 `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only`。通过 `cargo test --features fault-injection nonce_database_contract -- --ignored --nocapture` 验证密钥创建回滚、分域计数、并发分配、重启舍弃余号和密钥材料唯一性；四种数据库分别运行。

`python -m tests.recovery.nonce_ranges` 接管已初始化但停止的专用服务，沿用来源隔离 fixture 的配置、管理员密码、故障目录和 S3 后端环境。覆盖加密区块和 pack、直接去重不分配、提交前后强杀、缺失计数、耗尽、换 key 及未加密/raw 零分配。

`key_replacement_contract` 使用 `MOKYU_TEST_CONFIG` 指向全新隔离数据库，以 `--ignored` 运行，覆盖停用存储、关联范围及修订冲突、审计失败回滚、并发重复确认、目标校验、关闭重连和旧操作重试；四种数据库分别运行。

`python -m tests.recovery.key_replacement` 接管已初始化但停止的专用服务，沿用来源隔离 fixture 的配置，并设置 `MOKYU_TEST_FSYNC_ROOT`。覆盖提交前后强杀、响应丢失、旧加密区块及 pack 读取、去重复用、保存后的刷新失败与重试、API/CLI 身份隔离。

`python -m tests.recovery.key_replacement_cluster` 使用双 data 的 PostgreSQL/S3 fixture，设置 `MOKYU_TEST_PEER_ENDPOINT` 和两节点的 `MOKYU_TEST_SOURCE_FAULT_DIRS`。覆盖新密钥首次跨节点读取、在途写入保留旧密钥、通知阻塞后的周期刷新和离线节点重启；运行器收到配置目录下 `replacement-control/stop-peer` 或 `start-peer` 文件后，停止或启动第二个 data，并写入同名 `.done` 文件确认。两个 `key-capacity.spec.ts` / `key-replacement.spec.ts` 浏览器入口验证三语、明暗主题、320–1440px、阈值配色、键盘提示和预览恢复。

MySQL 连接验证还需冷重启数据库后直接启动应用，不能先由其他客户端登录预热认证缓存。关闭 TLS 时，`caching_sha2_password` 的首次认证依赖 SQLx 的 `mysql-rsa` 功能。

`cleanup_reacquires_returning_connection`、`cleanup_yields_to_busy_writer_and_recovers`、`cleanup_propagates_pool_errors`、`cleanup_cancels_busy_database_transaction` 属于普通 Rust 测试，使用独立 SQLite 临时库和单写连接覆盖清理连续领取、忙写者下有界退让和取消后恢复、外部写事务占用时取消 BEGIN、真实连接池错误。

## 公共接口与恢复

`storage_diagnostics_count_shared_failures_and_cache_fallback` 是带 `fault-injection` 的 Rust ignored 测试，使用全新 `MOKYU_TEST_SQLITE_CONFIG`、独立 `MOKYU_TEST_FAULT_DIR` 和 `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only`。覆盖 16 个共享读取者、来源缺失、错误正文、超长正文，以及无法清理的损坏缓存和缓存文件丢失；检查传输、内容与缓存计数互不混淆。

启动带 `fault-injection` 的隔离服务，配置专用 local/S3 存储、桶、区块密钥及管理员后，执行对应 Python 模块。依赖 `boto3`、`requests`；PostgreSQL 专用测试另需 `psycopg`。

生命周期测试会等待容量统计收敛；测试服务的 `metrics.refresh_interval` 应设为短周期（如 `2s`），保存设置后重启再运行。

- `integration.py`：S3 普通上传、条件请求、校验、Range 和分片协议。
- `s3/publication.py`：保留 boto3 默认 checksum 行为，验证文件流、transfer-manager multipart、同 key 普通 PUT/If-None-Match 竞争，以及正文、ETag、长度、类型和 writer 元数据属于同一代。默认 20 轮 × 8 写者；`MOKYU_TEST_ENDPOINTS` 可指定多 data 地址 JSON 数组，否则使用单个 `MOKYU_TEST_ENDPOINT`。记录预算拒绝 SlowDown，不把它算作成功。
- `s3/connections.py`：Expect 未发送正文、部分正文、完整正文遭拒后的 HTTP/1.1 连接边界；前两种关闭，完整正文允许安全关闭或复用；无正文请求复用，复用后的响应正文必须完整。仅直连隔离 HTTP 端口运行，每种 20 轮。
- `s3/protocols.py`：三个监听器的 HTTP/1.1 + h2c、特殊路径签名、HEAD/Range、大于流窗口的读写、并发、取消、早期拒绝和慢 preface。另需 Python `h2`；在普通 S3 凭据 fixture 上设置 `MOKYU_TEST_PUBLIC`、已绑定该桶的 `MOKYU_TEST_PUBLIC_HOST` 和管理入口 `MOKYU_TEST_WEB`，用 `python -m tests.s3.protocols` 执行。
- `recovery/truncated_stream.py`：PostgreSQL 专用数据库构造 513 个有 source offset 的 extent，删除最后完整 extent 后，已读过和绕过缓存的完整/尾部 Range 均不得静默成功；恢复后字节正确。使用 `MOKYU_TEST_DATABASE_FILE` 和与发布测试相同的 endpoint 设置。
- `s3/empty_ranges.py`：签名/匿名 S3、公共 Web 与面板下载的空对象、单字节、suffix/open/closed Range、HEAD 和条件请求。空对象无 Range 正常返回，合法但不可满足的 Range 返回 416 和 `Content-Range: bytes */0`；使用默认 SDK 校验配置，另验证空对象复制。面板验证读取 `MOKYU_TEST_PASSWORD_FILE`，公共 Web 使用 `MOKYU_TEST_PUBLIC_WEB`/`MOKYU_TEST_PUBLIC_HOST`。
- `control/database_capabilities.py`：管理 API/CLI、配置字段和群集能力边界。
- `storage/raw_objects.py`：raw 的 PUT/HEAD/Range/条件请求、Multipart、Copy 和 chunks 切换；远端设置 `MOKYU_TEST_STORAGE_TYPE=s3`。
- `storage/raw_lifecycle.py`：本地 raw 损坏隔离/复检、复用 Multipart 暂存、失败 Complete、用量和读取/GC 竞争。
- `storage/raw_deduplication.py`：四数据库的可选整文件去重、并发与跨分片边界、跨桶用量归属及坏来源排除；复用 source_quarantine 的 fixture，设置 `MOKYU_TEST_BACKEND` 可验证远端 raw。
- `recovery/file_identities.py`：接管已初始化但停止的隔离服务，验证各数据库持久 UUID、物理文件与缓存路径、pack/unpack 逻辑身份稳定、API ID 类型拒绝和重启读取；环境沿用 `source_quarantine`，需要独立 S3 后端。
- `recovery/cluster_identities.py`：两个真实 data 节点并发写入加密区块，检查 nonce 不重复、节点本地写缓存排空、跨节点读取，以及 pack/unpack 后 UUID、完整正文和 Range 保持一致。使用独立 PostgreSQL 库和 S3 后端，环境沿用 `source_quarantine`，另设 `MOKYU_TEST_PEER_ENDPOINT` 指向第二个 data。
- `storage/raw_cache.py`：独占已初始化但停止的隔离服务，使用 9MiB 文件缓存和关闭的响应缓存、独立 S3 fixture，验证并发冷读/Range、坏缓存回退、pin/淘汰、巡检绕过、隔离/恢复代次、重启和覆盖，以及成功 PUT 不等待可选区块缓存填充；环境沿用 `source_quarantine`。
- `storage/raw_migration.py`：raw 与加密 chunks、raw 与 raw 的跨存储 copy/move，支持独立远端 S3 fixture。
- `storage/raw_streaming.py`：raw 大分片、原始 ETag、完整摘要和跨分片 Range；`--part-mib`、`--repeat`、`--label` 记录 UploadPart/Complete 耗时和服务 RSS，`--order sequential|reverse` 选择上传顺序。默认远端需 `MOKYU_TEST_BACKEND`，本地使用 `--local` 且不设置该变量；`MOKYU_TEST_PID_FILE` 必须属于同一容器。不同构建或服务预算的数字不能直接比较。
- `storage/raw_replay.py`：暂停 Copy/Multipart 校验后的回放，修改远端 raw 或本地暂存来源，验证失败不会替换原对象或丢失已上传分片，恢复源数据后重试成功；需要 `MOKYU_TEST_BACKEND` 和故障注入构建。
- `recovery/raw_crashes.py`：独占启动/停止隔离服务，验证 raw 发布前后强杀、响应丢失、fsync EIO 和孤儿回收，使用与 `local_durability.py` 相同的 fsync 注入环境。
- `recovery/file_permissions.py`：接管已初始化但停止的隔离 SQLite 服务，验证严格/宽松继承 umask、普通载荷和暂存0644、目录0755、SQLite伴随文件0600、Multipart发布及重启；需Linux隔离容器内root以切换到UID65534验证实际读权限，环境沿用 `source_quarantine`，不设置 `MOKYU_TEST_BACKEND`。
- `recovery/local_multipart.py`：独占已初始化但停止的隔离服务，验证本地候选文件按需扩展、连续布局 inode 复用、乱序/替换/所选清单、上传和 Complete 强杀、两批积压上限、确认/映射/回收强杀、Abort、最后一批异步回收及 GC 后全部成功对象的长度/SHA-256 校验；环境沿用 `source_quarantine` fixture；加 `--s3-cache` 或 `MOKYU_TEST_MULTIPART_CACHE=1` 并提供 `MOKYU_TEST_BACKEND` 时覆盖 S3 raw 写缓存文件复用，缓存建议至少 256MiB、上传缓存 128MiB。配合 `recovery/fsync.c` 和 `LD_PRELOAD`，设置 `MOKYU_TEST_FSYNC_FAULT` / `MOKYU_TEST_FSYNC_TRACE` 覆盖文件与目录确认失败；S3 写缓存故障根目录设为 `<data_path>/raw`。另设 `MOKYU_TEST_SPACE_FAULT` 注入目标盘低空间，验证保留来源和重试。`--cross-filesystem` 要求 `MOKYU_TEST_FSYNC_ROOT` 与实例 Multipart 目录位于不同文件系统，用于验证复制回退。Rust 普通测试覆盖启动探测不阻塞、共享结果、探测失败不重复、未探测直接复制及匿名临时文件回收；专项 `real_reflink_preserves_sources_and_copy_on_write` 需独立且支持 reflink 的 `MOKYU_TEST_REFLINK_DIR`，显式 `--ignored` 运行并断言实际克隆及 COW 隔离。
- `recovery/storage_policy.py`：重启保留后端策略与修订、不同后端的 Multipart 空闲期限、查询不续期、写入续期、目标盘余量隔离，以及内存预算准入和重启生效。先用当前程序初始化隔离实例后停止，由测试持有服务进程；使用 `source_quarantine.py` 的环境参数。原生 `*_storage_contract`、`*_maintenance_contract` 与 `postgres_storage_policy_expiration` 补测事务内重新判断和活跃分片租约。
- `recovery/cluster_policy.py`：真实双 data 节点共享 S3 的后端独立 Multipart 期限、跨节点写入续期和较长期限上传继续完成；环境沿用 `raw_cluster.py`，通过维护入口触发回收。
- `recovery/multipart_completion.py`：四库及双 data 的 Complete 5 秒保活、HTTP 200 内 XML 错误、断线继续、相同清单加入、持久结果重放、进程重启及租约 fencing。服务以 fault-injection 构建，快速回归设置 `MOKYU_TEST_COMPLETE_TIMEOUT_SECONDS=12`，真实截止回归使用 `300`；群集服务与测试使用相同值；群集另验发布清理竞态及等待本地队列远端确认，使用原有节点 kill/start 标记。测试超时变量不属于运行配置。
- `s3/multipart_admission.py`：完成工作独立额度及满额 503、前台读写可用、校验失败释放额度、断线继续；需要独立 PostgreSQL 夹具与现有 integration 环境。
- `recovery/remote_multipart.py`：原生后端 MPU 的跨节点乱序及稀疏分片、仅本地暂存、本地提前确认与重启、迟到同编号覆盖重传、上游完成后强杀恢复、缓存满 reject/fallback、Abort、空对象、profile 修改、维护排空、低空间拒绝和空闲回收；使用 `raw_cluster.py` 的双 data 夹具及 `replacement-control` 节点 kill/start 标记。
- `recovery/raw_cluster.py`：两个实际 data 节点的原生远端 raw Multipart、跨节点 Complete/去重/读取，以及慢校验续租与 GC；另设 `MOKYU_TEST_PEER_ENDPOINT`，服务端 `read_idle_timeout=30s`，故障目录指向第一个 data 节点。`--cache` 额外验证两节点 raw 文件缓存及慢缓存命中续租；节点文件缓存至少 32MiB，并用 `MOKYU_TEST_CACHE_DIRS` 传入两个逗号分隔的 data/raw 路径。
- `storage/single_lifecycle.py`：提前本地确认、持久上传、pack、rewrite、unpack、巡检、容量和清理。
- `recovery/upload_cache.rs`：PostgreSQL 上传准备的双执行者竞争、取消回滚和 owner/pin/来源代次原子性；需要故障注入构建。
- `recovery/single_restart.py`：三个后端共用的 API 契约，覆盖 10 个崩溃点，包括上传准备提交前后的 owner/pin 原子恢复、待上传数据的密钥轮换及 local/S3 加密数据重启读取。
- `recovery/cluster_runtime.py`：PostgreSQL 多角色与节点生命周期验证。
- `recovery/cluster_scheduler.py`：双 data 和独立 worker 的上传归属、通知断连兜底、暂停隔离及空闲数据库工作上限。
- `recovery/maintenance_state.py`：双 control、双 data 和 worker 的维护状态收敛。按文件头部的 fixture 与阶段运行：`enable` 后重启一个 data，执行 `resume`、`failure`、`race`、`drain`。`failure` 在专用库中短暂锁表并取消各节点的状态查询，再释放锁；`drain` 写入停止请求标记后，测试编排器向首个 control 发送 SIGTERM，验证已有连接的迟到管理请求不能恢复写入，完成后重启该节点。
- `recovery/cluster_cache.py`：两个 data 的本地 raw/chunks 提前确认、节点所属关系、重启恢复、写缓存转读缓存、签名/匿名/Web GET 有界等待和 CORS、权限复查、全节点维护排空，以及仅依赖最终区块和共享 CDC 尾块的跨节点 Multipart 完成。环境沿用 `raw_cluster.py`，另设 `MOKYU_TEST_SOURCE_FAULT_DIRS` 为两个 data 的逗号分隔故障目录；节点缓存至少 256MiB。编排器处理测试目录 `replacement-control/` 下的 `kill-primary`、`start-primary` 标记，执行后写入同名 `.done` 文件。
- `s3/read_path.py`：分页 SQL、后台缓存故障和读名额耗尽时的 HEAD/304；可用 `MOKYU_TEST_PUBLIC_WEB`、`MOKYU_TEST_PUBLIC_HOST` 同时验证公共 Web。
- `recovery/read_leases.py`：两个 data 节点、按时间续约、元数据轻读和过期租约拒绝；要求读取空闲超时为 `30s`，测试进程独立于受限应用容器，至少预留 1 GiB 内存。
- `recovery/read_replicas.py`：真实 PostgreSQL 热备及独立只读账户，覆盖公共分页分流、签名排除、暂停回放、权限/部署身份错误、DDL 回放阻塞、连接与超时预算、过期主库租约和流中删除。需要 `MOKYU_TEST_REPLICA_DATABASE_FILE`（隔离从库的管理 DSN）、`MOKYU_TEST_REPLICA_USER`、data 的 `MOKYU_TEST_STATUS_URL` 和故障注入目录；读空闲超时为 `30s`，读并发至少 4，从库池为 2。测试会暂停回放并修改权限，仅用于独立测试主从。
- `web/web_assets.py`：内嵌资源与公开 schema 的三种 HTTP 编码、协商、ETag、HEAD、敏感响应排除；另需 Python `brotli`、`zstandard`，直连与反向代理均应执行。
- `s3/http_compression.py`：公共 Web/匿名 S3 的线上编码字节、签名排除、HEAD/304/Range、预估与学习；使用同样的解码依赖，可选 PostgreSQL DSN 检查 JSON。原生 `postgres_compression_contract`、`mysql_content_contract`、`sqlite_content_contract` 覆盖并发算法合并、策略切换和重复观测；迁移恢复契约同时检查新增字段。
- `s3/response_cache.py`：启用完整正文缓存后，验证跨入口/编码复用、真实长度、权限变更、覆盖删除和坏映射拒绝；PostgreSQL 配合 `pg_stat_statements` 验证稳定热命中不写来源租约，故障注入验证快照后删除不破坏已校验内存正文。群集测试通过 `MOKYU_TEST_STATUS_URL` 读取对应 data 的状态；`s3/read_path.py` 也支持此字段。`s3/response_cache_pressure.py` 在 `4MiB` 额度与故障注入下额外验证取消、迟到构建、淘汰后慢响应持有内存及正常上传仍可前进。
- `recovery/public_read_cache.py`：使用已初始化但停止的故障注入服务配置，验证公开快照的硬期限、四种路由别名、零 SQL 热读、签名隔离与即时本机撤权；无关 PUT/Copy/Complete/ACL/delete 不清空热点及 CORS 关联，保留快照仍能刷新，迟到回填与失败条件写不恢复旧对象。

各文件头部及 `MOKYU_TEST_*` 读取处定义所需环境变量。凭据使用文件传入；不能将测试密码或 S3 secret 放入日志。恢复测试会强杀并重启配置指定的网关进程，只能在专用测试容器运行。故障注入构建不用于部署。

`recovery/pack_memory.rs` 使用全新 `MOKYU_TEST_SQLITE_CONFIG`、独立 `MOKYU_TEST_FAULT_DIR` 和状态修改授权，运行 `cargo test --locked --no-default-features --features fault-injection pack_memory_follows_blocking_work_and_last_reader -- --ignored --nocapture`。它阻塞真实 pack 解码、完整巡检、编码和拆包编码，验证取消、超时、错误、panic、成功以及最后使用者释放后的内存/CPU 额度回收；测试数据库不能与运行中的服务共用。

`recovery/local_durability.py` 在每种单容器数据库验证 local 文件与目录的 fsync 失败：初始化不登记存储、覆盖失败保留原对象、rewrite 保留旧编码，恢复后可重试。在 Linux 构建容器编译 `cc -shared -fPIC -Wall -Wextra -Werror tests/recovery/fsync.c -ldl -o /test/fsync.so`，仅为被测服务设置 `LD_PRELOAD=/test/fsync.so`。服务和测试共享 `MOKYU_TEST_FSYNC_ROOT`（专用后端根目录，不带末尾斜线）、`MOKYU_TEST_FSYNC_FAULT` 和 `MOKYU_TEST_FSYNC_TRACE`；故障文件、trace 和数据库必须位于后端根目录以外。此检查注入真实 fsync 返回错误，不模拟断电。local 禁止上传缓存，S3 唯一暂存与 flush 由 `storage/single_lifecycle.py` 覆盖。

浏览器验证在包含 Playwright 浏览器的容器中执行 `web` 的 `npm run test:ui`。必须指定隔离服务地址 `MOKYU_TEST_WEB`、状态修改授权及测试账户密码文件；`frontend-auth.spec.ts` 覆盖真实会话刷新和重新打开页面。

`file-identities.spec.ts` 使用含有 ready 区块包的 PostgreSQL 实例，验证 UUID API、区块包列表、详情和关联文件，在 320–1440px 宽度下检查长 UUID 换行和页面溢出；沿用上述浏览器登录环境。

`http-compression.spec.ts` 使用实际 Chromium 缓存验证三种编码与 identity 切换、强弱 ETag 和 304 后的完整正文；设置 `MOKYU_TEST_HTTP_OBJECT` 可同时验证具有压缩收益的公开文本对象 URL。HTTPS 测试需信任专用测试证书，跳过证书错误可能导致浏览器禁用缓存，不能将全为 200 的结果当作重验证通过。

## 公共读基准

`python -m tests.benchmarks.public_reads /config/benchmark.json /results/reads.json` 在独立客户端 Docker 中运行，要求状态修改授权。服务必须来自已记录 revision 的 `cargo build --release --locked` 构建，不能使用 debug 或故障注入构建作性能结论。上传固定种子的公开样本后保存原始字节；基准本身只读对象，逐请求检查 HTTP 状态、线上编码、Content-Length 和解码后的完整正文。

先在 Linux cgroup v2 测试容器运行 `python -m tests.benchmarks.public_reads_test`，验证四种编码的实际字节计数、正文/HTTP 错误计数、全失败时的空百分位和预热失败退出。这个自检使用本机 HTTP fixture，不是 Mokyu 性能数据。

fixture 示例：

```json
{
  "label": "cache-on-round-1",
  "revision": "tested-git-revision",
  "build": "release",
  "database_version": "PostgreSQL 18",
  "cache_state": "warm; response cache enabled; chunk cache enabled",
  "seed": 719,
  "requests": 1000,
  "concurrency": 4,
  "warmup": 4,
  "database_file": "/config/postgres-dsn",
  "resources": {
    "client": {"cgroup": "/sys/fs/cgroup"},
    "data": {"cgroup": "/metrics/data", "status_url": "http://data:9002/status"},
    "database": {"cgroup": "/metrics/database"}
  },
  "cases": [{
    "name": "text-1MiB-zstd",
    "urls": ["http://data:9000/test-bucket/text"],
    "expected_file": "/fixtures/text.bin",
    "encoding": "zstd"
  }]
}
```

将本轮各容器自己的 cgroup v2 目录分别只读挂入客户端；资源列表应包含客户端、所有参与的 data/control/worker、数据库与共享后端。程序记录实际 CPU/内存上限、CPU 使用与限流增量、采样的 `memory.current` 峰值，节点状态包含前后 RSS、完整正文缓存、从库和后端请求/字节计数。`memory.current` 包含页缓存，不能当作进程 RSS；状态中的 peak RSS 是进程生命周期峰值。PostgreSQL 可选 `database_file` 需要预先启用 `pg_stat_statements`，记录主库分页、快照和租约 SQL 增量；其他数据库省略该项，输出 null，不能解释成零查询。

`cases` 可配置多个对象与入口，每组 `urls` 最多 4096 个，按请求序号轮流访问，用于区分单热点和超出缓存容量的热集合；`encoding` 支持 identity/gzip/br/zstd，后两者另需 Python `brotli`/`zstandard`。Range 场景通过 `headers.Range`、`status: 206` 和对应切片的 expected_file 表达。`successful_wire_body_bytes` 统计实际读取的编码正文，不含 HTTP 头和帧开销，不以解码长度回填。延迟从请求开始到正文读完，包含连接等待；TTFB 到第一个正文字节。RPS 包含客户端解码校验开销，必须检查客户端 CPU 是否饱和。

可选 `seconds` 为 1–600 秒，以实际完成的请求数计算速率，仍受 `requests` 上限约束。每个 case 的 `method` 支持 GET/HEAD/OPTIONS，`response_headers` 核对指定响应头；HEAD/OPTIONS 要求无正文。资源报告另含匿名页、文件页、脏页、回写页及逐设备 I/O 差值。

Linux 单并发 case 可指定 `evict_files`，仅列出该隔离测试拥有的文件。每次请求前用 `posix_fadvise(DONTNEED)` 驱逐这些文件的页缓存，并用 `mincore` 确认无驻留页；页面仍被使用时最多重试一秒，未成功驱逐即报错，不触碰全局缓存。驱逐耗时不计入请求延迟，但计入整个测量窗口，因此此类 RPS 不能和热读吞吐直接比较。该结果只证明文件页缓存冷读，不证明磁盘控制器、虚拟机宿主或远端介质的缓存状态。

预热失败不会启动并发屏障；请求或校验失败写入结果并非零退出，没有成功样本时百分位为 null。先运行少量请求验证入口，再做交错 off/on/on/off；冷测试必须明确记录外部清理或重启步骤并设 warmup=0，不能把重复同对象的全程称为冷读。固定总预算与新增总预算的比较分别报告，禁止把消除 SQL、缓存命中或小样本吞吐直接作为生产收益。

## 来源隔离回归

`source_quarantine_contract` 使用全新 `MOKYU_TEST_CONFIG` 和 `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only`，四种数据库共用。验证引用/发布拒绝、执行 token、授权、隔离代次、维护输入 ABA 与独立后台审计；pack publication 契约另验证健康替代来源。

`postgres_source_validation_allows_references` 使用指向全新隔离库的 `MOKYU_TEST_DATABASE_URL_FILE`，验证来源资格锁期间仍能建立普通引用，但物理隔离与映射变更必须等待；同时验证重复引用不再更新已清空的存活时间。来源检查还覆盖两种作用域的三阶段锁等待后的新状态、不可用结果后事务及锁仍然有效、空/fragment来源、4097区块和generic/custom计划。

`postgres_pending_release_preserves_only_copy` 验证仍被引用的唯一暂存副本不会提前释放，并覆盖 EOF 重排的 stream 范围、重复引用和已有任务 owner。

`python -m tests.storage.source_quarantine` 在独立故障注入服务验证引用前/发布前隔离、Copy/UploadPartCopy/Complete 拒绝、原对象保留、暖正文缓存/Range/304、CSRF、幂等、暂停复检及恢复。要求 `MOKYU_TEST_BINARY`、`MOKYU_TEST_CONFIG`、`MOKYU_TEST_ENDPOINT`、`MOKYU_TEST_WEB`、`MOKYU_TEST_FAULT_DIR`、`MOKYU_TEST_PASSWORD_FILE`，初始管理员用户名为 tester，启用响应缓存。local 使用专用 `MOKYU_TEST_FSYNC_ROOT`；模拟 S3 使用 `MOKYU_TEST_BACKEND` JSON 文件，字段为 endpoint、bucket、region、access_key、secret_key、prefix。测试自行创建独立 storage、桶及凭据，不使用真实桶。

群集另设 `MOKYU_TEST_SOURCE_FAULT_DIRS` 为逗号分隔的 data/worker 故障目录，`MOKYU_TEST_SECOND_CONTROL` 指向第二个 control，以验证并发执行同一预览。配置的 Web origin 须与 MOKYU_TEST_WEB 一致。`python -m tests.storage.pack_quarantine` 使用同一模拟 S3 fixture 验证整个 pack 的精确复检、成员不变、维护模式恢复及普通成员拒绝。两者会修改专用后端的物理测试字节，不用于已有部署。
同一 fixture 的 `python -m tests.storage.quarantine_gc` 在完整复检提交前删除逻辑对象并运行独立 GC，核对精确物理保护与退休后的 CAS 拒绝；`python -m tests.storage.quarantine_permissions` 在最终提交前撤销发起管理员，核对隔离不被解除。

`python -m tests.storage.quarantine_replica` 另需真实 PostgreSQL 备库、启用从库的数据节点，以及主/从库管理 DSN 文件 `MOKYU_TEST_DATABASE_FILE` / `MOKYU_TEST_REPLICA_DATABASE_FILE`。它改写专用测试对象的 extents 来产生分页，暂停 WAL 回放后隔离来源，验证旧快照不能授权 Copy 或冷回源；已开始的流只允许完整正确正文或截断失败。

Web 回归 `web/tests/source-quarantine.spec.ts` 使用真实管理 API 和专用已隔离来源 fixture，验证列表、确认词与完整复检；fixture 格式和环境变量见测试文件。

## 来源重传恢复回归

`source_recovery_contract` 使用全新 `MOKYU_TEST_CONFIG`，验证候选所有权、元数据、并发胜者、ABA、写代次、旧引用及未知候选回收。PostgreSQL 额外验证同 ID 节点重新注册后的凭据隔离、排空和写租约失效。MySQL bootstrap 契约覆盖新装中断后的拒绝、失败历史不续跑及触发器结构对比。

以下 Python 入口沿用来源隔离 fixture，自行创建测试 storage 和桶。需要访问同一隔离主库；PostgreSQL 安装 psycopg，MySQL/MariaDB 安装 PyMySQL，数据库连接读取 `MOKYU_TEST_CONFIG`。local 与模拟 S3 应分别运行。

- `tests.recovery.source_repair`：三种编码、坏候选完整回读、覆盖/profile 竞争、旧对象恢复、UploadPart/Web、条件请求、部分正文和错误校验和；恢复审计持久化、审计失败时同事务回滚
- `tests.recovery.pack_repair`：模拟 S3 上逐成员恢复、旧包布局保留、最后成员解除后回收
- `tests.recovery.repair_cluster`：两个真实 data 节点，另设 `MOKYU_TEST_SECOND_DATA` 和第一个 data 的 `MOKYU_TEST_NODE_STATE`；验证没有跨 I/O 的数据库 claim、双候选 CAS、独立 GC、撤权、租约过期和排空；只允许专用 PostgreSQL 测试库
- `tests.recovery.repair_node`：在第一个 data 的进程命名空间内运行，强杀并重启该节点，验证重启保留未过期候选、另一 data 恢复及失效候选清理。另设 `MOKYU_TEST_NODE_CONFIG`、`MOKYU_TEST_DATA`、`MOKYU_TEST_GATEWAY_PID_FILE`；fixture 的 CLI 须通过独立转发端口访问 control，不能把管理命令发给本机 data。排空用例之后先重启 data，再运行此项
- `tests.recovery.repair_lifecycle`：仅单容器，使用 `MOKYU_TEST_DATA` 和 `MOKYU_TEST_GATEWAY_PID_FILE` 强杀并重启服务，覆盖各恢复检查点与第一次删除后的有限迟到 PUT
- `tests.recovery.repair_fsync`：沿用 `local_durability.py` 的 LD_PRELOAD 与 fsync fixture，验证恢复不能确认文件或目录同步失败

## 来源写入基准

### 锁与连接池

`postgres_chunk_claims_release_connections_and_fence_stale_writers` 使用新隔离 PostgreSQL 库，以 `--ignored` 运行，覆盖小池与 LISTEN、短领取锁竞争、租约和任务 token 失效、原子准备失败回滚、未知来源记录及 SQL 等待取消。`python -m tests.recovery.claim_pool` 沿用双 data 来源恢复 fixture，设置 `MOKYU_TEST_SECOND_DATA`，两 data 的连接池均为 4；验证两个暂停上传不占住查询容量、同内容互斥、跨节点独立写、完整读回，以及分配/上传/完成前租约过期后拒绝旧写者。来源修复竞争和撤权由 `tests.recovery.repair_cluster` 覆盖；该夹具要求 GC 未暂停。

`python -m tests.recovery.cluster_scheduler` 还需要独立 worker，三个执行节点均配置 `instance.cpu_jobs = 1`，使用非零磁盘读缓存和上传缓存预算。沿用上述双 data fixture 的 `MOKYU_TEST_SECOND_DATA` 和 `MOKYU_TEST_SOURCE_FAULT_DIRS`，暂停本节点上传期间验证另一节点推进，主动断开当前隔离数据库的任务 LISTEN 会话，核对重连、暂停后的旧执行不能发布、恢复后跨节点完整读回，以及六秒空闲事务数量有界。

`postgres_source_validation_allows_references` 使用新的隔离 PostgreSQL 库及 `MOKYU_TEST_DATABASE_URL_FILE`，以 `--ignored` 运行。覆盖来源锁与活跃引用、隔离、打包映射竞争；等待期间从独立块切换为 pack 后仍须持物理锁，空/fragment 来源和 4097 块的 generic/custom plan 均须正确。

`python -m tests.recovery.publish_coordination` 沿用来源隔离的运行中单容器 fixture，覆盖 chunks/raw 的 PUT/Copy/Complete：发布后暂停封口时读取能推进、请求不能提前 ACK、删除及 GC 不能跨越活动写者的 pin。`raw_crashes` 另验证该边界强杀后的恢复。用于旧实现负对照时可设置 `MOKYU_TEST_EXPECT_PUBLISH_BLOCKED=1`，在旧的发布后检查点确认读取确实阻塞。

`python -m tests.recovery.received_publication` uses the same running-service fixture for each database. It checks raw/chunks PUT metadata, failed conditions, checksums, empty objects, old-version reads while publication is paused and write authorization revoked during upload. `raw_crashes` also kills the service before publication to verify recovery of fully received, unpublished content.

`tests/benchmarks/lock_tables.rs` 比较每次扫描与实际摊销清理实现，覆盖 1/64/4096 个活跃键、单线程/四线程、命中/换键，交替运行三轮并输出 CSV。使用同一 Rust 工具链的 release Tokio 产物，在隔离构建容器内执行：

```sh
deps="${CARGO_TARGET_DIR:-target}/release/deps"
tokio_rlib=$(ls -t "$deps"/libtokio-*.rlib | head -1)
rustc --edition=2024 -O tests/benchmarks/lock_tables.rs --extern "tokio=$tokio_rlib" -L "dependency=$deps" -o /tmp/mokyu-lock-tables
/tmp/mokyu-lock-tables
```

先完成 `cargo build --release --locked`；锁身份、取消等待和闲置容量回收由普通 Rust 测试覆盖。此入口只计锁表操作，不代表对象请求吞吐；端到端比较继续使用下述对象基准。

### 独立操作计时

`python -m tests.benchmarks.object_io /config/io.json /results/io.json` 分别计量 `put`、`get`、`head` 或 `multipart`，要求 release 构建与 `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only`。先运行 `python -m tests.benchmarks.object_io_test`。普通写入只计收到 ACK 的耗时，每个成功对象使用唯一 key，全部完整读回校验在计时及资源采样窗口之外；GET 计完整正文校验并另记首字节，Multipart 同时记录 Create、UploadPart、Complete 和完整生命周期。写入 ACK 不代表异步后台已排空，应另外记录排空时间与资源。

fixture 使用 `revision`、`build`、`database_version`、`cache_state`、`seed`、`credentials_file`、`prefix` 和可选 `resources`/PostgreSQL `database_file`，另设单个 `endpoint`、`operation`、`size`、`seconds`、`concurrency`、`max_operations`、`max_total_bytes`。正文大小 0–64MiB，并发 1–64，持续 1–600 秒，请求最多一百万；包括预热对象的 `(max_operations + 1) × size` 必须在字节预算内。每轮使用独占前缀，脚本不会删除数据；触及请求上限时按实际时间报告。

后台写入比较可设置 `raw_drain_seconds`（1–600）并提供对应隔离库的 `MOKYU_TEST_CONFIG`；该桶必须使用本轮专用 storage。预热及请求结束后等待该 storage 的 raw 去重、CDC、raw/chunks 待上传、文件同步及 pack_inputs 排空，包含已不再被当前对象引用的来源，完整读回放在排空后。积压按义务类别计数，不能相加当成唯一对象数。报告保留请求期积压历史、排空历史，以及 ACK 窗口和包括排空的时间、完成速率、CPU/磁盘成本与采样内存峰值；失败或超时不能计为稳态完成。排空和积压采样本身有数据库开销，比较双方须使用同样设置。

`hot_keys` 默认为 0，表示每次新 key；1–1024 时每个写者使用独立的有限 key 集合，避免不同写者竞争同一目标，计时后核对各 key 最后一次成功写入。`payload_mode` 可选 `tail`（兼容默认，只改尾16字节）、`full`（确定性全内容变化）、`compressible`（随序号变化的重复模式）和 `duplicate`（完全相同）。`repeat_every` 默认为 0，表示序号不复用；正数按周期复用序号，用于控制重复程度。tail 不能代表多区块全新内容。生成耗时/线程CPU与基础样本字节另外报告，生成在请求延迟计时之外，但仍占用吞吐窗口和客户端资源。ACK 数包含所有成功覆盖，读回数量只包含最终对象，两者不能混用。

`requests_per_second` 默认 0（不限速）；正数按总请求率安排发起时刻，最多一百万。工作线程数量固定，不建立无界待发队列；跟不上目标速率时记录调度延迟和实际成功速率，不声称达到目标负载。失败单独记录，保留响应的 Retry-After 分布，不纳入成功延迟分位数。Multipart 当前为单个 UploadPart，不能据此推导多片拼装性能。

报告保存各操作百分位、实际请求窗口吞吐、读回校验数量/错误、CPU、cgroup 内存（含匿名页/文件页/脏页）、逐设备 I/O 字节/次数及节点状态。逐设备计数不叠加，以免重复计算分区和底层设备。PostgreSQL SQL 差值是整个测量窗口的数据库总成本，包含后台工作，不是逐请求前台 SQL 计数；没有采集时为 null，不能解读成零。公共 Web/匿名 S3 的编码读取继续使用公共读基准；所有比较记录应用/页缓存状态，统一资源与确认策略，并发超出服务准入额度的 SlowDown 必须计为错误，不能只报告成功请求。

### 写入与立即读回

`python -m tests.benchmarks.source_writes /config/writes.json /results/writes.json` 使用独立 release 服务和可丢弃桶，要求 `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only`、boto3，以及启用 SQL 采样时的 psycopg。先运行 `python -m tests.benchmarks.source_writes_test`，确保错误正文不能计作成功。

fixture 沿用公共读基准的 `label`、`revision`、`build`、`database_version`、`cache_state`、`seed`、`resources` 和可选 `database_file`，增加：

| 字段 | 含义 |
| --- | --- |
| `round` | 本轮唯一非负整数，生成不同的新写入内容 |
| `credentials_file` | 含 access_key、secret_key、bucket 的受限 JSON 文件 |
| `endpoints` | 1–8 个 S3 地址，各客户端均匀分配 |
| `prefix` | 本轮独占对象前缀；脚本不会清理桶 |
| `seconds` | 每个场景持续秒数，默认 60，上限 600 |
| `concurrency` | 并发写者，默认 4，上限 32 |
| `max_operations` | 每场景请求上限，默认 100000，上限 1000000；提前触顶的结果不能声称达到指定持续时间 |
| `slots_per_worker` | 每个写者覆盖的有限目标 key 数，默认 4，上限 64 |
| `unique_keys` | 默认 false；true 时每次用新 key，必须设置 `max_total_bytes`，且 max_operations × size 不超过它 |
| `drain_seconds` | 默认 0；设置 1–600 时要求 PostgreSQL，预热后及计时后等待 pending_uploads/pack_inputs 清空，超时失败 |
| `size` | 普通场景原始正文大小，默认 32768，上限 16MiB |
| `cases` | `new`、`dedup`、`copy`、`multipart`、`many`、`mixed` 的数组 |
| `many_source` | `many` 所需的既有对象 key 与 expected_file，完整正文上限 64MiB；须另记录真实 extent 数及是否使用合成布局 |
| `repair_payloads` | `mixed` 所需的有限原文文件列表，最多 1024；每 32 次请求消耗一份，须事先确认对应来源已隔离 |

`new` 改变固定种子正文的尾部以产生新内容；`dedup` 和 `copy` 复用同一热门来源；`many` 复制预先准备的多 extent 对象；`multipart` 只计 Complete 调用延迟，完整周期还包括初始化、上传单片和读回。每次成功写入均完整 GET 并核对字节和长度，吞吐包含校验耗时，写延迟不含读回。请求或采样错误使脚本失败，空样本百分位为 null。

CPU/cgroup 内存、各节点状态、后端字节/操作均记录前后差值。PostgreSQL 以 userid/dbid/queryid/toplevel 区分语句，保存完整 SQL、执行/计划时间、行数、buffer 和 WAL；统计重置会报错。记录 track/track_planning/fsync/synchronous_commit 设置，track=all 的内外层耗时不能相加。每 100ms 按 application_name 采样等待类别、事件和阻塞关系，每秒采样上传积压；这些计数不能当作精确等待秒数，application_name 也不参与 SQL 统计分组。连接池等待未插桩，输出 null。

启用 drain 后分别保留请求窗口和含排空的 SQL/CPU/后端成本、待处理队列与 ready 编码数；不能把延后落盘当成提速。`mixed` 分别记录健康与恢复请求延迟，需结合恢复审计确认命中恢复。各比较版本须从等价历史/队列状态开始，统一预热、统计刷新和 ANALYZE 相位；至少三组交错配对，详细 tracing 只作独立归因。有限 key 不限制历史物理版本数量，运行前仍须安排总字节、保留期及清理预算。

- `python -m tests.recovery.storage_relocation`: owns a stopped isolated service, copies one complete local namespace to an isolated S3 fixture and back, verifies maintenance/upload/read guards and crash boundaries. Set `MOKYU_TEST_RELOCATION_BACKEND` to the fixture credential file and the usual `source_quarantine` environment, without `MOKYU_TEST_BACKEND`.
- `python -m tests.recovery.raw_processing`：自行启动已初始化但停止的专用服务，沿用来源隔离的环境变量。覆盖空闲 raw 后台不会吞掉任务通知、可靠 ACK 先于后台去重、接收策略保持、两处强杀恢复、读者占用、Multipart/Copy、删除和坏来源拒绝；local 与模拟 S3、四种数据库分别运行。
- `python -m tests.recovery.raw_processing_cluster`：沿用 `raw_cluster` 的双 data/共享 S3 fixture，将两节点故障目录写入 `MOKYU_TEST_SOURCE_FAULT_DIRS`；验证共享读者保护、处理租约过期接手、停用节点拒绝提交、无租约领取恢复和迟到执行者不能重复切换。数据库保留两个 LISTEN 连接后，用一个查询连接和行锁等待验证续租不会停止事务推进。

`raw_permit_order_cancellation_and_retry` uses a fresh `MOKYU_TEST_SQLITE_CONFIG`, an isolated `MOKYU_TEST_FAULT_DIR`, a disposable S3 `MOKYU_TEST_BACKEND` JSON file and `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only`. Run `cargo test --locked --no-default-features --features fault-injection raw_permit_order_cancellation_and_retry -- --ignored --nocapture`. It schedules actual verified raw reads, the deduplication worker, raw writes and native Multipart uploads with one CPU/read/backend slot and one or three saturated global upload slots. It checks cancellation at both upload gates, CPU-wait cancellation, persisted processing retries, invalid digests and completed Multipart contents. The database and fault directory must not be shared with another service; use `raw_processing`, `raw_uploads` and `raw_crashes` for process-restart coverage.

`python -m tests.recovery.raw_receiving` owns an isolated initialized service and remote S3 fixture to verify pre-upload raw reuse, bounded request workspace, protocol failures, corruption and crash cleanup. Run it with the `source_quarantine` fixture variables and a disposable primary database.

`python -m tests.recovery.raw_uploads` 接管已初始化但停止的隔离服务，沿用来源隔离 fixture 的配置、故障目录和 S3 后端变量；节点缓存至少 64MiB、上传缓存至少 32MiB。覆盖提前确认后的 GET/HEAD/Range、PUT/Copy/Multipart、关闭缓存与重启、配额回退、条件/校验失败、维护 flush 的时间上界、最终上传后崩溃和唯一缓存损坏。四种数据库分别运行。

`python -m tests.recovery.cached_encoding` 使用相同的隔离服务与 S3 夹具，覆盖新块在最终编码前的持久确认、零提前 nonce 消耗、读取与去重、重启恢复、压缩/组包/密钥设置变更隔离、旧密钥撤销保护、直接组包后的最终文件数量、独立块压缩和缓存满 reject/fallback。上传缓存至少 8MiB。设置 `MOKYU_TEST_PEER_ENDPOINT` 时复用已启动的双 data 夹具，故障目录列表使用 `MOKYU_TEST_SOURCE_FAULT_DIRS`；单容器入口自行启停服务，四库分别运行。

`python -m tests.recovery.background_cdc` 复用上述环境，验证原始表示提前确认、设置冻结、后台直接组包、健康复用、同内容并发、旧读者保护、覆盖/删除、空对象、候选上传后的强杀恢复、坏源拒绝及修复重试、显式缓存刷新和 Multipart。四库分别运行；`MOKYU_TEST_CDC_LOCAL=1` 使用本地后端，双 data 使用同样的 peer/fault 变量。故障点只可用于隔离构建。

`upload-overflow.spec.ts` 使用独立 S3 fixture（`MOKYU_TEST_BACKEND`），自行创建本地后端，验证三语明暗表单、键盘选择、320–1440px、草稿取消/保存及本地禁用。`recovery.raw_uploads` 同时验证 chunks/raw 的 fallback/reject、503 SlowDown、失败写入不覆盖旧对象、重启保存策略、非法选项与缺省字段。

`python -m tests.recovery.credential_activity` uses a stopped isolated service and the source-quarantine fixture variables to verify signed GET/HEAD while credential activity writes are paused, bounded timeout and retry, five-minute coalescing, invalid signature isolation, and restart loss of optional statistics. Run against each supported database.

`python -m tests.recovery.read_snapshots` uses the same stopped-service fixture and a local raw backend. It counts actual SQL by HTTP request ID for signed and anonymous GET/HEAD/Range, including Web domains, S3 path, virtual hosts and public aliases. It also checks CORS, encoded keys, empty objects, presigned URLs, failed/expired/disabled credentials, grants, conditional ACL/version races, pre-open GC pins, and current source quarantine. SQL traces remain in the isolated fault directory; run against each supported database.

`python -m tests.recovery.cors_cache` also verifies nonblocking paused fills, generation fencing, independent CORS background validation and hard expiry. It uses the same stopped-service fixture, a local backend and zero response-cache budget. It reuses the complete S3 CORS suite, then checks concurrent parsing, per-Origin/HEAD/OPTIONS behavior, CLI and both settings APIs, empty rules, domain reassignment, and bucket recreation. Run against each supported database; Rust tests additionally cover bounded allocation, retained readers, oversized documents and stale revisions.

`python -m tests.recovery.raw_responses` enables a 4 MiB response budget in the stopped-service fixture. It runs the response cache and pressure suites on raw objects, verifies one-query hot reads, signatures and current ACL, source quarantine/revisions, multipart, content changes during cache construction, and raw-to-chunks conversion, then repeats the chunk cache suite. Set `MOKYU_TEST_BACKEND` for an isolated S3 backend; run each supported database and retain the fault logs.

`python -m tests.recovery.cluster_responses` uses the two-data-node `cluster_cache` fixture with `response_cache_size = "4MiB"` on both data nodes. It verifies independent raw identity/compressed caches, signed and public S3/Web reuse, CORS, HEAD, Range, current ACL and credentials, overwrites and deletion.

`python -m tests.recovery.public_read_cache` uses the stopped-service fixture and tests zero body budget metadata, zero foreground SQL for hot public HEAD/GET/Range/OPTIONS, signed and conditional isolation, local mutations, failed refresh with hard expiry, domain-edit races and raw/chunks on each supported database. The test changes another node's ACL through the isolated database to measure the explicitly configured stale-permission window.

`python -m tests.recovery.source_read_cache` uses the same stopped-service fixture with no response body budget. It verifies zero-SQL raw reads and Range with explicit source snapshots, independent source expiry, strict and signed bypass, quarantine/recheck invalidation, pre-open pins through overwrite/GC, and bounded chunk refresh failures. Run each supported database locally and set `MOKYU_TEST_BACKEND` for remote raw/chunk/pack coverage. The optional source policy in `tests.recovery.cluster_public_cache` must retain real data-node read leases on body misses.

`python -m tests.recovery.cluster_public_cache` uses the same two-data-node fixture to verify per-node metadata/body reuse, immediate writer-node invalidation, bounded peer permission staleness, current signed credential revocation and convergence after overwrite/delete.

`tests/recovery/read_integrity.py` 在隔离四库/本地及远端 S3 上验证额外摘要策略、迟到缓存、后台队列与内存上限、Range/取消、实际坏缓存与真实后端的隔离边界；pack 与加密认证的强制检查还由 Rust 内容格式回归覆盖

`tests/recovery/io_presets.py` 验证三套服务端模板、API/CLI 只读预览、显式保存、版本冲突和无关配置不变，支持单容器与双 data 群集夹具。`web/tests/io-presets.spec.ts` 覆盖三语、明暗主题、键盘、小屏、预览取消、草稿载入和冲突保留；`web/tests/storage-folds.spec.ts` 验证预设常显、详细配置默认折叠、已修改项提示、折叠后的草稿与保存、原生校验定位和服务端校验失败展开；原有 `buffered-storage.spec.ts` 继续验证手动调整各选项。

`python -m tests.recovery.received_responses` verifies hot-path PUT reuse for raw/chunks, signed and anonymous S3/Web/Range, rejected or disconnected uploads, bounded optional admission, no-store and current permissions. With `MOKYU_TEST_BACKEND`, it also verifies shared RAM allocation and independent write-quota release after drain. Uses the isolated recovery fixture and a 4MiB response cache.

`python -m tests.recovery.file_delivery` uses the stopped-service source-quarantine fixture and a 4MiB response budget. It covers local profile validation, one-query cold and zero-query hot raw reads, per-storage body-cache retirement, restoration, file size and symlink checks. With `MOKYU_TEST_PROXY_RUN` set to the application container hostname, it writes `proxy/nginx.conf`, `proxy/apache.conf` and `proxy/raw-path` beside the application config, then waits for `proxy/ready`. The orchestrator mounts only that raw directory read-only at `/srv/raw`, starts Nginx and Apache with mod_xsendfile as `<hostname>-nginx` and `<hostname>-apache`, and creates the marker. HTTP fixtures on 9100/9101 pause the handoff for GC races. Both proxy configurations come directly from the deployment document; Nginx also enables the actual optional control-cache blocks. Run in a private Docker network with isolated data and explicit resource limits.

`python -m tests.recovery.local_deployment` reproduces the documented SQLite/raw CLI initialization. Set `MOKYU_TEST_BINARY`, `MOKYU_TEST_ROOT` to an empty owned directory, and `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only`. It owns ports 9000–9002 and starts/stops the service itself; use a fresh Linux container with 1GiB memory and UID/GID 10001 to check the documented file permissions.

`node tests/recovery/sdk_multipart.mjs /fixture.json` uses AWS SDK for JavaScript v3 and an isolated fault-injection service with `MOKYU_TEST_COMPLETE_TIMEOUT_SECONDS=12`. Install the SDK in the test environment, not the application. The private fixture contains `endpoint`, `bucket`, `access_key`, `secret_key` and the service's shared `fault_dir`; require `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only`. It checks full PUT/GET/HEAD/Range, two-part uploads, HTTP 200 Error XML rejection/retry, five-second whitespace keepalive under an eight-second socket timeout, and successful result replay. Run raw and chunks separately, including a real reverse proxy; the script leaves its uniquely prefixed objects in the disposable bucket.

`python -m tests.benchmarks.cors_cache` uses a started isolated fixture to compare 100 buckets sharing 100-rule CORS documents. It reports retained memory, parse counts and five HTTP HEAD rounds at concurrency 8; missing objects deliberately keep public object caching out of the measurement.

`python -m tests.recovery.cluster_cors_cache` requires two isolated PostgreSQL data nodes and the shared S3 fixture. It covers forced CORS caching with public snapshots, independent CORS-only confirmation and peer convergence. Set `MOKYU_TEST_CORS_RESTART_SIGNALS` when the harness can restart the peer after `restart-peer` appears to verify deployment-wide limits with independent per-node activation.
