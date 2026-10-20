# 验证

构建和测试在隔离 Docker 环境中执行。每组数据库契约使用独立空库或全新 SQLite 文件；测试账户、S3 桶、data 目录和故障注入目录也必须专用，不能指向已有部署。

## Rust 检查

在构建容器的源码目录运行：

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

数据库改动需在 PostgreSQL 18、MySQL 8.0.19、MariaDB 10.6.3 的最低版本和当前维护版本验证。MariaDB 复用全部 MySQL 契约和 `MOKYU_TEST_MYSQL_CONFIG`；`mysql_dialect_and_lock_scope_contract` 额外检查 JSON 类型、大小写/尾空格比较和任务领取的锁范围。`migrations.py` 在 PostgreSQL 17 空库验证低版本拒绝且不写入表，在支持版本验证新装、旧版本升级与恢复。

`postgres_schema_bootstrap_contract`、`mysql_schema_bootstrap_contract` 对照当前快照与完整编号迁移的结构、初值和校验和，并检查升级、重启、未知版本、历史损坏、新装失败与真实连接中断。PostgreSQL 同时验证并发初始化。两者除上述连接配置外，测试用户还需有创建数据库的权限；会在同一隔离服务器创建 `mokyu_bootstrap_*` 测试库，验证后随专用测试实例一并清理。`sqlite_schema_bootstrap_contract` 使用捆绑 SQLite 内存库，属于普通 Rust 测试；磁盘/WAL/进程恢复由 `sqlite_runtime_and_migration_recovery` 覆盖。

增加编号迁移时同步更新对应 `migrations/schema/*.sql` 与迁移入口的快照版本。快照包含最终结构和必需初值，版本行及 `_sqlx_migrations` 由初始化器最后登记。保留原迁移内容与校验和；结构对照包含列、默认值、约束、索引、视图，以及 PostgreSQL 函数、触发器和序列。

`nonce_database_contract` 使用 `MOKYU_TEST_NONCE_CONFIG` 指向全新的隔离数据库，并要求 `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only`。通过 `cargo test --features fault-injection nonce_database_contract -- --ignored --nocapture` 验证密钥创建回滚、分域计数、并发分配、重启舍弃余号和密钥材料唯一性；四种数据库分别运行。

`python -m tests.recovery.nonce_ranges` 接管已初始化但停止的专用服务，沿用来源隔离 fixture 的配置、管理员密码、故障目录和 S3 后端环境。覆盖加密区块和 pack、直接去重不分配、提交前后强杀、缺失计数、耗尽、换 key 及未加密/raw 零分配。旧格式由数据库迁移拒绝测试覆盖，不提供旧密文读取兼容。

MySQL 连接验证还需冷重启数据库后直接启动应用，不能先由其他客户端登录预热认证缓存。关闭 TLS 时，`caching_sha2_password` 的首次认证依赖 SQLx 的 `mysql-rsa` 功能。

`cleanup_reacquires_returning_connection`、`cleanup_yields_to_busy_writer_and_recovers`、`cleanup_propagates_pool_errors`、`cleanup_cancels_busy_database_transaction` 属于普通 Rust 测试，使用独立 SQLite 临时库和单写连接覆盖清理连续领取、忙写者下有界退让和取消后恢复、外部写事务占用时取消 BEGIN、真实连接池错误。

## 公共接口与恢复

`storage_diagnostics_count_shared_failures_and_cache_fallback` 是带 `fault-injection` 的 Rust ignored 测试，使用全新 `MOKYU_TEST_SQLITE_CONFIG`、独立 `MOKYU_TEST_FAULT_DIR` 和 `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only`。覆盖 16 个共享读取者、来源缺失、错误正文、超长正文，以及无法清理的损坏缓存和缓存文件丢失；检查传输、内容与缓存计数互不混淆。

启动带 `fault-injection` 的隔离服务，配置专用 local/S3 存储、桶、区块密钥及管理员后，执行对应 Python 模块。依赖 `boto3`、`requests`；PostgreSQL 专用测试另需 `psycopg`。

生命周期测试会等待容量统计收敛；测试服务的 `metrics.refresh_interval` 应设为短周期（如 `2s`），保存设置后重启再运行。

- `integration.py`：S3 普通上传、条件请求、校验、Range 和分片协议。
- `s3/publication.py`：保留 boto3 默认 checksum 行为，验证文件流、transfer-manager multipart、同 key 普通 PUT/If-None-Match 竞争，以及正文、ETag、长度、类型和 writer 元数据属于同一代。默认 20 轮 × 8 写者；`MOKYU_TEST_ENDPOINTS` 可指定多 data 地址 JSON 数组，否则使用单个 `MOKYU_TEST_ENDPOINT`。记录预算拒绝 SlowDown，不把它算作成功。
- `s3/connections.py`：Expect 未发送正文、部分正文、完整正文遭拒后的 HTTP/1.1 连接边界；前两种关闭，完整正文可复用，且后续响应正文完整。仅直连隔离 HTTP 端口运行，每种 20 轮。
- `recovery/truncated_stream.py`：PostgreSQL 专用数据库构造 513 个有 source offset 的 extent，删除最后完整 extent 后，已读过和绕过缓存的完整/尾部 Range 均不得静默成功；恢复后字节正确。使用 `MOKYU_TEST_DATABASE_FILE` 和与发布测试相同的 endpoint 设置。
- `s3/empty_ranges.py`：签名/匿名 S3、公共 Web 与面板下载的空对象、单字节、suffix/open/closed Range、HEAD 和条件请求。空对象无 Range 正常返回，合法但不可满足的 Range 返回 416 和 `Content-Range: bytes */0`；使用默认 SDK 校验配置，另验证空对象复制。面板验证读取 `MOKYU_TEST_PASSWORD_FILE`，公共 Web 使用 `MOKYU_TEST_PUBLIC_WEB`/`MOKYU_TEST_PUBLIC_HOST`。
- `control/database_capabilities.py`：管理 API/CLI、配置字段和群集能力边界。
- `storage/raw_objects.py`：raw 的 PUT/HEAD/Range/条件请求、Multipart、Copy 和 chunks 切换；远端设置 `MOKYU_TEST_STORAGE_TYPE=s3`。
- `storage/raw_lifecycle.py`：本地 raw 损坏隔离/复检、复用 Multipart 暂存、失败 Complete、用量和读取/GC 竞争。
- `storage/raw_deduplication.py`：四数据库的可选整文件去重、并发与跨分片边界、跨桶用量归属及坏来源排除；复用 source_quarantine 的 fixture，设置 `MOKYU_TEST_BACKEND` 可验证远端 raw。
- `recovery/file_identities.py`：接管已初始化但停止的隔离服务，验证各数据库持久 UUID、物理文件与缓存路径、pack/unpack 逻辑身份稳定、API ID 类型拒绝和重启读取；环境沿用 `source_quarantine`，需要独立 S3 后端。
- `recovery/cluster_identities.py`：两个真实 data 节点并发写入加密区块，检查 nonce 不重复、共享暂存排空、跨节点读取，以及 pack/unpack 后 UUID、完整正文和 Range 保持一致。使用独立 PostgreSQL 库和 S3 后端，环境沿用 `source_quarantine`，另设 `MOKYU_TEST_PEER_ENDPOINT` 指向第二个 data。
- `storage/raw_cache.py`：独占已初始化但停止的隔离服务，使用 9MiB 文件缓存和关闭的响应缓存、独立 S3 fixture，验证并发冷读/Range、坏缓存回退、pin/淘汰、巡检绕过、隔离/恢复代次、重启和覆盖，以及成功 PUT 不等待可选区块缓存填充；环境沿用 `source_quarantine`。
- `storage/raw_migration.py`：raw 与加密 chunks、raw 与 raw 的跨存储 copy/move，支持独立远端 S3 fixture。
- `storage/raw_streaming.py`：raw 大分片、原始 ETag、完整摘要和跨分片 Range；`--part-mib`、`--repeat`、`--label` 记录 UploadPart/Complete 耗时和服务 RSS，`--order sequential|reverse` 选择上传顺序。默认远端需 `MOKYU_TEST_BACKEND`，本地使用 `--local` 且不设置该变量；`MOKYU_TEST_PID_FILE` 必须属于同一容器。不同构建或服务预算的数字不能直接比较。
- `storage/raw_replay.py`：暂停 Copy/Multipart 校验后的回放，修改远端 raw 或本地暂存来源，验证失败不会替换原对象或丢失已上传分片，恢复源数据后重试成功；需要 `MOKYU_TEST_BACKEND` 和故障注入构建。
- `recovery/raw_crashes.py`：独占启动/停止隔离服务，验证 raw 发布前后强杀、响应丢失、fsync EIO 和孤儿回收，使用与 `local_durability.py` 相同的 fsync 注入环境。
- `recovery/file_permissions.py`：接管已初始化但停止的隔离 SQLite 服务，验证严格/宽松继承 umask、普通载荷和暂存0644、目录0755、SQLite伴随文件0600、Multipart发布及重启；需Linux隔离容器内root以切换到UID65534验证实际读权限，环境沿用 `source_quarantine`，不设置 `MOKYU_TEST_BACKEND`。
- `recovery/local_multipart.py`：独占已初始化但停止的隔离服务，验证本地候选文件按需扩展、连续布局 inode 复用、乱序/替换/所选清单、上传和 Complete 强杀、两批积压上限、确认/映射/回收强杀、Abort、最后一批异步回收及 GC 后全部成功对象的长度/SHA-256 校验；环境沿用 `source_quarantine` fixture。配合 `recovery/fsync.c` 和 `LD_PRELOAD`，设置 `MOKYU_TEST_FSYNC_FAULT` / `MOKYU_TEST_FSYNC_TRACE` 覆盖文件与目录确认失败；另设 `MOKYU_TEST_SPACE_FAULT` 注入目标盘低空间，验证保留来源和重试。`--cross-filesystem` 要求 `MOKYU_TEST_FSYNC_ROOT` 与实例 Multipart 目录位于不同文件系统，用于验证复制回退。Rust 专项 `real_reflink_preserves_sources_and_copy_on_write` 需独立且支持 reflink 的 `MOKYU_TEST_REFLINK_DIR`，显式 `--ignored` 运行并断言实际克隆及 COW 隔离。
- `recovery/storage_policy.py`：旧运行参数物化到各后端、监听前中断恢复、不同后端的 Multipart 空闲期限、查询不续期、写入续期、目标盘余量隔离，以及内存预算准入和重启生效。先用上一结构的程序初始化隔离实例后停止，由测试持有服务进程；使用 `source_quarantine.py` 的环境参数。原生 `*_storage_contract`、`*_maintenance_contract` 与 `postgres_storage_policy_expiration` 补测事务内重新判断和活跃分片租约。
- `recovery/cluster_policy.py`：真实双 data 节点共享 S3 的后端独立 Multipart 期限、跨节点写入续期和较长期限上传继续完成；环境沿用 `raw_cluster.py`，通过维护入口触发回收。
- `recovery/raw_cluster.py`：两个实际 data 节点的共享 Multipart 暂存、跨节点 Complete/去重/读取，以及慢校验续租与 GC；另设 `MOKYU_TEST_PEER_ENDPOINT`，服务端 `read_idle_timeout=30s`，故障目录指向第一个 data 节点。`--cache` 额外验证两节点 raw 文件缓存及慢缓存命中续租；节点文件缓存至少 32MiB，并用 `MOKYU_TEST_CACHE_DIRS` 传入两个逗号分隔的 data/raw 路径。
- `storage/single_lifecycle.py`：提前本地确认、持久上传、pack、rewrite、unpack、巡检、容量和清理。
- `recovery/upload_cache.rs`：PostgreSQL 上传准备的双执行者竞争、取消回滚和 owner/pin/来源代次原子性；需要故障注入构建。
- `recovery/single_restart.py`：三个后端共用的 API 契约，覆盖 10 个崩溃点，包括上传准备提交前后的 owner/pin 原子恢复、待上传数据的密钥轮换及 local/S3 加密数据重启读取。
- `recovery/cluster_runtime.py`、`cluster_staging.py`：PostgreSQL 多角色、节点和共享暂存验证。
- `recovery/maintenance_state.py`：双 control、双 data 和 worker 的维护状态收敛。按文件头部的 fixture 与阶段运行：`enable` 后重启一个 data，执行 `resume`、`failure`、`race`、`drain`。`failure` 在专用库中短暂锁表并取消各节点的状态查询，再释放锁；`drain` 写入停止请求标记后，测试编排器向首个 control 发送 SIGTERM，验证已有连接的迟到管理请求不能恢复写入，完成后重启该节点。
- `recovery/staged_reads.py`：两个 data 上验证共享暂存查找与后台发布/删除并发，公共及签名读重新选择已发布来源；唯一暂存丢失仍拒绝。另需 `MOKYU_TEST_UPLOAD_FAULT_DIRS`（所有执行上传的 data/worker 故障目录 JSON 数组）和首个 data 的 `MOKYU_TEST_FAULT_DIR`。
- `s3/read_path.py`：分页 SQL、后台缓存故障和读名额耗尽时的 HEAD/304；可用 `MOKYU_TEST_PUBLIC_WEB`、`MOKYU_TEST_PUBLIC_HOST` 同时验证公共 Web。
- `recovery/read_leases.py`：两个 data 节点、按时间续约、元数据轻读和过期租约拒绝；要求读取空闲超时为 `30s`，测试进程独立于受限应用容器，至少预留 1 GiB 内存。
- `recovery/read_replicas.py`：真实 PostgreSQL 热备及独立只读账户，覆盖公共分页分流、签名排除、暂停回放、权限/部署身份错误、DDL 回放阻塞、连接与超时预算、过期主库租约和流中删除。需要 `MOKYU_TEST_REPLICA_DATABASE_FILE`（隔离从库的管理 DSN）、`MOKYU_TEST_REPLICA_USER`、data 的 `MOKYU_TEST_STATUS_URL` 和故障注入目录；读空闲超时为 `30s`，读并发至少 4，从库池为 2。测试会暂停回放并修改权限，仅用于独立测试主从。
- `web/web_assets.py`：内嵌资源与公开 schema 的三种 HTTP 编码、协商、ETag、HEAD、敏感响应排除；另需 Python `brotli`、`zstandard`，直连与反向代理均应执行。
- `s3/http_compression.py`：公共 Web/匿名 S3 的线上编码字节、签名排除、HEAD/304/Range、预估与学习；使用同样的解码依赖，可选 PostgreSQL DSN 检查 JSON。原生 `postgres_compression_contract`、`mysql_content_contract`、`sqlite_content_contract` 覆盖并发算法合并、策略切换和重复观测；迁移恢复契约同时检查新增字段。
- `s3/response_cache.py`：启用完整正文缓存后，验证跨入口/编码复用、真实长度、权限变更、覆盖删除和坏映射拒绝；PostgreSQL 配合 `pg_stat_statements` 验证稳定热命中不写来源租约，故障注入验证快照后删除不破坏已校验内存正文。群集测试通过 `MOKYU_TEST_STATUS_URL` 读取对应 data 的状态；`s3/read_path.py` 也支持此字段。`s3/response_cache_pressure.py` 在 `4MiB` 额度与故障注入下额外验证取消、迟到构建、淘汰后慢响应持有内存及正常上传仍可前进。

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

`cases` 可配置多个对象与入口；`encoding` 支持 identity/gzip/br/zstd，后两者另需 Python `brotli`/`zstandard`。Range 场景通过 `headers.Range`、`status: 206` 和对应切片的 expected_file 表达。`successful_wire_body_bytes` 统计实际读取的编码正文，不含 HTTP 头和帧开销，不以解码长度回填。延迟从请求开始到正文读完，包含连接等待；TTFB 到第一个正文字节。RPS 包含客户端解码校验开销，必须检查客户端 CPU 是否饱和。

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

`source_recovery_contract` 使用全新 `MOKYU_TEST_CONFIG`，验证候选所有权、元数据、并发胜者、ABA、写代次、旧引用及未知候选回收。PostgreSQL 额外验证同 ID 节点重新注册后的凭据隔离、排空和写租约失效。MySQL bootstrap 契约覆盖恢复迁移的中断续跑及触发器结构对比。

以下 Python 入口沿用来源隔离 fixture，自行创建测试 storage 和桶。需要访问同一隔离主库；PostgreSQL 安装 psycopg，MySQL/MariaDB 安装 PyMySQL，数据库连接读取 `MOKYU_TEST_CONFIG`。local 与模拟 S3 应分别运行。

- `tests.recovery.source_repair`：三种编码、坏候选完整回读、覆盖/profile 竞争、旧对象恢复、UploadPart/Web、条件请求、部分正文和错误校验和；恢复审计持久化、审计失败时同事务回滚
- `tests.recovery.pack_repair`：模拟 S3 上逐成员恢复、旧包布局保留、最后成员解除后回收
- `tests.recovery.repair_cluster`：两个真实 data 节点，另设 `MOKYU_TEST_SECOND_DATA` 和第一个 data 的 `MOKYU_TEST_NODE_STATE`；故意终止唯一持有 chunk advisory lock 的数据库连接，验证双候选 CAS、独立 GC、撤权、租约过期和排空；只允许专用 PostgreSQL 测试库
- `tests.recovery.repair_node`：在第一个 data 的进程命名空间内运行，强杀并重启该节点，验证重启保留未过期候选、另一 data 恢复及失效候选清理。另设 `MOKYU_TEST_NODE_CONFIG`、`MOKYU_TEST_DATA`、`MOKYU_TEST_GATEWAY_PID_FILE`；fixture 的 CLI 须通过独立转发端口访问 control，不能把管理命令发给本机 data。排空用例之后先重启 data，再运行此项
- `tests.recovery.repair_lifecycle`：仅单容器，使用 `MOKYU_TEST_DATA` 和 `MOKYU_TEST_GATEWAY_PID_FILE` 强杀并重启服务，覆盖各恢复检查点与第一次删除后的有限迟到 PUT
- `tests.recovery.repair_fsync`：沿用 `local_durability.py` 的 LD_PRELOAD 与 fsync fixture，验证恢复不能确认文件或目录同步失败

## 来源写入基准

### 锁表清理

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

raw 后台去重比较可设置 `raw_drain_seconds`（1–600）并提供对应隔离库的 `MOKYU_TEST_CONFIG`；预热及请求结束后等待该桶的处理记录排空，完整读回放在排空后。报告分别保留 ACK 窗口和包括排空的时间、完成速率、CPU/磁盘成本；失败或超时不能计为稳态完成。排空轮询本身有数据库开销，比较双方须使用同样设置。

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
