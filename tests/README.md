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

MySQL 连接验证还需冷重启数据库后直接启动应用，不能先由其他客户端登录预热认证缓存。关闭 TLS 时，`caching_sha2_password` 的首次认证依赖 SQLx 的 `mysql-rsa` 功能。

`cleanup_reacquires_returning_connection`、`cleanup_yields_to_busy_writer_and_recovers`、`cleanup_propagates_pool_errors`、`cleanup_cancels_busy_database_transaction` 属于普通 Rust 测试，使用独立 SQLite 临时库和单写连接覆盖清理连续领取、忙写者下有界退让和取消后恢复、外部写事务占用时取消 BEGIN、真实连接池错误。

## 公共接口与恢复

启动带 `fault-injection` 的隔离服务，配置专用 local/S3 存储、桶、区块密钥及管理员后，执行对应 Python 模块。依赖 `boto3`、`requests`；PostgreSQL 专用测试另需 `psycopg`。

生命周期测试会等待容量统计收敛；测试服务的 `metrics.refresh_interval` 应设为短周期（如 `2s`），保存设置后重启再运行。

- `integration.py`：S3 普通上传、条件请求、校验、Range 和分片协议。
- `s3/empty_ranges.py`：签名/匿名 S3、公共 Web 与面板下载的空对象、单字节、suffix/open/closed Range、HEAD 和条件请求。空对象无 Range 正常返回，合法但不可满足的 Range 返回 416 和 `Content-Range: bytes */0`；使用默认 SDK 校验配置，另验证空对象复制。面板验证读取 `MOKYU_TEST_PASSWORD_FILE`，公共 Web 使用 `MOKYU_TEST_PUBLIC_WEB`/`MOKYU_TEST_PUBLIC_HOST`。
- `control/database_capabilities.py`：管理 API/CLI、配置字段和群集能力边界。
- `storage/single_lifecycle.py`：提前本地确认、持久上传、pack、rewrite、unpack、巡检、容量和清理。
- `recovery/single_restart.py`：三个后端共用的 API 契约，覆盖 8 个崩溃点、待上传数据的密钥轮换及 local/S3 加密数据重启读取。
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

`http-compression.spec.ts` 使用实际 Chromium 缓存验证三种编码与 identity 切换、强弱 ETag 和 304 后的完整正文；设置 `MOKYU_TEST_HTTP_OBJECT` 可同时验证具有压缩收益的公开文本对象 URL。HTTPS 测试需信任专用测试证书，跳过证书错误可能导致浏览器禁用缓存，不能将全为 200 的结果当作重验证通过。
