# 配置参考

主配置是 [`config.example.toml`](../config.example.toml)，默认路径 `/config/config.toml`。文件保存 `[database]`、`[instance]` 及可选的 `[replica_database]`；Web、请求、上传、统计、维护和 storage 策略通过 Web 专项表单或 `mokyu cli` 写入数据库。

未知字段、错误类型、超范围数值和无法满足本机内存预算的配置会拒绝启动。保存需要重启的字段时只显示 `restart_required`，新值不会阻塞请求，也不会在当前进程中假装已经生效。

## database

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `database.type` | `postgres` | 数据库类型：`postgres`、`mysql` 或 `sqlite` |
| `database.host` | 服务端数据库必填 | PostgreSQL/MySQL 主机或容器服务名；SQLite 不使用 |
| `database.port` | PostgreSQL `5432`、MySQL `3306` | 服务端数据库端口；SQLite 不使用 |
| `database.name` | 服务端数据库必填 | PostgreSQL/MySQL 数据库名；SQLite 不使用 |
| `database.user` | 服务端数据库必填 | PostgreSQL/MySQL 用户名；SQLite 不使用 |
| `database.password` / `database.password_file` | 服务端数据库二选一 | 直接密码或密码文件；SQLite 不使用 |
| `database.ssl_mode` | `prefer` | PostgreSQL/MySQL 的连接 SSL 模式；MySQL 支持 `disable`、`prefer`、`require`、`verify-ca`、`verify-full`，SQLite 不使用 |
| `database.path` | `instance.data_path/mokyu.db` | SQLite 数据库文件；其他类型不使用 |

| 数据库 | 单容器 | 群集 |
| --- | --- | --- |
| PostgreSQL | 支持 | 支持 |
| MySQL / MariaDB | 支持 | 不支持 |
| SQLite | 支持 | 不支持 |

最低支持 PostgreSQL 18、MySQL 8.0.19、MariaDB 10.6.3。MariaDB 使用 `type = "mysql"` 和相同连接字段，由程序自动识别。MySQL/MariaDB 使用 InnoDB，要求 `innodb_flush_log_at_trx_commit=1`。低于最低版本时，启动会在执行迁移前拒绝。SQLite 随程序编译，无需单独安装数据库服务。

最低兼容版本不代表推荐部署版本。新部署建议选择仍在维护的稳定版或 LTS，并安装该系列最新补丁。

群集模式下，每个 control、worker、data 都从自己的 `[database]` 连接 PostgreSQL。Mokyu 不负责 PostgreSQL 故障转移；需要高可用时使用 PostgreSQL 自己的主备或集群地址。

SQLite 使用 WAL、外键和默认 FULL 同步，数据库及其 `-wal`、`-shm` 文件必须位于可写的本地持久文件系统，不支持网络共享盘。默认文件是 `/data/mokyu.db`；新建数据库和锁文件权限为 `0600`，新建父目录为 `0755`。一个数据库文件只允许一个 Mokyu 实例，符号链接按实际路径识别，数据库硬链接被拒绝。文件被替换或运行锁丢失时进程退出。

SQLite 将 `instance.database_connections` 中的一个连接用于串行写入，其余用于只读查询；设置为 `1` 时读写共用一个连接。更高的连接数不增加同时写入的事务数。

SQLite 的运行设置 `io.sqlite_synchronous` 可选 `full`（默认）、`normal`、`off`，保存后重启生效。NORMAL 在断电或系统崩溃后可能丢失已提交事务，OFF 还可能损坏数据库；影响整个元数据库，包括配置、凭据和对象映射。初始化与迁移始终使用 FULL，完成后才按保存值创建运行连接池。NORMAL 下加密 nonce 的预留仍以 FULL 提交；已有任何加密预留的数据库不能设置 OFF，OFF 生效或等待生效期间拒绝新的加密预留。恢复加密写入需保存 FULL/NORMAL 并重启。面板的 `database.journal_mode` 和 `database.synchronous` 显示实际连接读取值；后端存储设置不会修改此选项，其他数据库不提供它。

## replica_database

可选的 PostgreSQL 物理从库，只用于群集 data 的公共读取后续分页。省略整节时不创建从库池或探测；single、MySQL、MariaDB、SQLite 拒绝此配置。control/worker 可以使用包含该节的共享配置，但不连接从库。

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `replica_database.host` | 配置该节时必填 | 从库入口，不继承主库主机 |
| `replica_database.port` | 主库端口 | PostgreSQL 端口 |
| `replica_database.name` | 主库数据库名 | 显式填写时必须与主库相同 |
| `replica_database.user` | 主库用户名 | 可使用独立只读账户 |
| `replica_database.password` / `replica_database.password_file` | 主库凭据 | 二选一；显式设置 user 时必须同时提供其中一项；可以只覆盖密码 |
| `replica_database.ssl_mode` | 主库 SSL 模式 | 沿用所选 TLS 模式；verify-full 验证从库主机名 |

此节不接受 `type`、`path` 或未知字段。密码文件相对于配置文件所在目录解析。只读账户需要连接数据库、使用 schema 和读取 `mokyu_meta`、`stream_leases`、`extents`、`chunks`、`chunk_encodings`；应用不需要复制管理或超级用户权限。主从初始化、复制和故障切换由部署环境管理。

最小配置为 `[replica_database]` 下设置 `host = "postgres-replica"`。从库故障或身份校验失败不阻断健康主库，读取自动回退；具体一致性边界见[群集部署](cluster-deployment.md#可选-postgresql-从库)。配置重启生效。

## instance

字段按“身份、监听、本机目录、上传缓存、连接池和资源预算”排列。`instance.name` 是节点注册名；单容器省略时使用 `single`。

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `instance.deployment_key` | 必填 | `mokyu tool keygen` 生成的 64 字符部署密钥；同一部署所有容器必须相同且永久不变 |
| `instance.name` | 单容器为 `single` | control、worker、data 必须填写部署内唯一名称 |
| `instance.listen_control` | `0.0.0.0:9002` | 管理 HTTP 监听；CLI 默认访问本机地址 |
| `instance.listen_web` | `0.0.0.0:9001` | 公共读 Web 监听 |
| `instance.listen_s3` | `0.0.0.0:9000` | S3 监听 |
| `instance.data_path` | `/data` | 本节点持久目录，保存 multipart、缓存和锁，群集角色另保存 node.json |
| `instance.min_free_space` | `1GiB` | 本地文件写入后必须保留的空间 |
| `instance.multipart_limit` | 自动 | 节点本地 Multipart 暂存上限，单容器和群集均适用 |
| `instance.cache_size` | 自动 | 本地 chunks/raw 文件缓存共用字节上限 |
| `instance.cache_entries` | 自动 | 本地缓存索引条目上限 |
| `instance.response_cache_size` | `0B` | 完整对象响应的内存额度；公共读取与已授权的签名 S3 共用，示例显式设为 `64MiB`，省略或 `0B` 关闭；启用至少 `4MiB`，需要重启 |
| `instance.write_memory_size` | `0B` | 单容器 S3 volatile 写缓存的共享内存额度；示例 `64MiB`，最少 `4KiB`，按 `4KiB` 向下取整，需重启 |
| `instance.upload_cache_size` | 自动 | 本地待上传暂存上限 |
| `instance.thumbnail_cache_size` | `256MiB` | 缩略图缓存上限 |
| `instance.request_connections` | 自动 | 本节点 HTTP/S3 请求连接池上限 |
| `instance.database_connections` | 自动 | 本节点数据库连接池上限；群集 control 至少 5，data/worker 至少 4；高并发需增加预算 |
| `instance.database_read_connections` | 启用从库时 `min(4, max(1, database_connections / 4))` | data 从库池的独立上限；正整数，需同时配置 replica_database；不扣减主库预算 |
| `instance.cpu_jobs` | 自动 | 本节点压缩、哈希和加解密等阻塞计算预算 |
| `instance.stream_memory_budget` | 自动 | 数据流临时内存预算，不是 RSS 硬限制 |
| `instance.storage_read_slots` | 自动 | 本节点所有 storage 共享的读槽位 |
| `instance.storage_upload_slots` | 自动 | 本节点所有 storage 共享的上传槽位 |
| `instance.storage_control_slots` | 自动 | 本节点所有 storage 共享的控制槽位 |
| `instance.compression_idle_timeout` | `30s` | 本节点压缩上下文空闲回收时间 |

省略 `request_connections` 和三个 `storage_*_slots` 时，服务按 CPU、内存、文件描述符和连接池自动计算保守但偏宽的本机预算。它们只限制本机；storage profile 则限制每个 storage 的总并发，单容器使用进程内队列，群集通过 PostgreSQL 共享许可。profile 热更新保留正在执行和排队的操作；降低上限后等待现有操作排空，提高上限则放行等待者。显式修改 instance 预算需要重启。

对象响应缓存显式启用后，从原有 pack 内存与工作区预算中划出，不额外增加总预算；control/worker 不启用正文缓存。额度不能超过进程内存的四分之一，并保留流式读写及默认 pack 所需空间。启用或增大前需确认剩余 pack 额度可容纳现有最大 pack 及各 storage profile；历史 pack 可能大于当前创建策略，因此默认不自动缩减它们的读取预算。每个正文的原始与编码长度均不超过总额度的 1/16 或 8 MiB，构建峰值最多占总额度的四分之一；索引、历史与构建元数据预留 1 MiB。淘汰后仍被发送响应引用的正文继续占额度，额度不足时不等待缓存空间，退回正常读取。

示例配置配合 Compose 的 Mokyu 容器 `1GiB` 内存限制，显式设置 `stream_memory_budget = "256MiB"`、`response_cache_size = "64MiB"` 和 `write_memory_size = "64MiB"`；PostgreSQL 容器内存另计。省略响应缓存或写入内存字段的配置仍关闭对应内存用途。默认压缩参数下，流式预算可提供 5 个数据槽位，实际预留约 `216MiB`，runtime/OS 预留 `256MiB`，pack 读取与工作区各自动分得约 `212MiB`。S3 storage 的默认 `pack_max_size = "32MiB"` 每个 pack 需约 `140MiB` 额度；local storage 不创建 pack。pack 大小在 storage profile 中配置，节点 pack 内存无需单独填写；更改容器限制、压缩参数或 pack 大小时应一起核对这些预算。

桶 CORS 解析规则使用 runtime 预留中的独立 `1MiB` 有界缓存，最多 `256` 个桶、每项最多 `128KiB`；正文缓存关闭时仍启用。规则按桶及配置修订复用，Origin、方法和预检头逐请求匹配；超出缓存额度的规则仍正常解析和执行，不增加 SQL 查询。

写入内存从同一总预算划出，最多占进程内存四分之一，并保留 stream、响应缓存及默认 pack 的额度。raw 在已知长度且能一次领取完整额度时进入 RAM，未知或过大的长度沿用有界文件暂存；chunks 按完整区块领取。待上传唯一副本不参与淘汰，最后一个读取者释放正文后才返还内存，空对象至少计 `4KiB`。local、群集和 durable/buffered 不使用 RAM 提前确认；写内存为 `0B` 不影响既有流式工作区。

共享 staging 设置只对 PostgreSQL 提供；MySQL、SQLite 不接受这两个设置。PostgreSQL 的 single 可以预先配置供群集切换使用。群集模式禁止 local storage。即使只有一个 data，也必须配置共享 S3/S3 兼容 storage 作为 `uploads.staging_storage_id`；仅未闭合的 CDC Multipart 尾块在保留前缀 `__mokyu/multipart/fragment/` 共享暂存；raw Multipart、raw/chunks 写缓存使用所属节点的 `instance.data_path`，最终载荷直接写目标 S3。单容器模式可使用 `/data/storage` 的 local storage。

## runtime 设置

runtime 设置保存在 `runtime_settings`。Web 使用固定表单，不提供任意 JSON 编辑器；CLI 可用 `settings get/set/reset` 完整管理。

| 分组 | 主要内容 | 作用范围 |
| --- | --- | --- |
| `web` | origin、cookie、session、Gravatar | 部署共享 |
| `requests` | 读写空闲超时、请求行为 | 部署共享，重启提示按字段显示 |
| `uploads` | staging storage、共享 staging 字节上限、客户端空闲超时与活动上传限制 | 部署共享；群集 storage 共享 |
| `metrics` | storage/runtime/access 采样间隔、保留期和查询超时 | 部署共享 |
| `maintenance` | GC、清理、任务保留、审计保留和批量大小 | 部署共享 |
| `io` | `shutdown_flush_timeout`；SQLite 另有 `sqlite_synchronous` | 部署共享，重启生效；SQLite 同步设置作用于整个元数据库 |

runtime section 使用 revision 乐观并发。冲突返回当前 revision；重置恢复当前版本默认值。runtime 设置在启动时加载；保存后按各进程实际加载的修订提示重启，不进行热更新。

`io.shutdown_flush_timeout` 默认 `30s`，接受 `0s` 至 `30m`。正常退出在同一期限内等待前台 HTTP 请求结束并排空已确认的写缓存及本地缓冲文件；群集节点只排空自己的缓存，期间保持上传和节点租约。`0s` 跳过等待，未完成来源留待重启恢复。超时日志记录待写条目、字节、最早创建时间和仍活跃的引用，诊断查询最多另用 1 秒。容器的停止宽限应大于此期限；强杀、断电和节点本地磁盘丢失不在正常退出保证内。

## storage profile

存储连接和所有后端行为都在 storage 创建/修改表单中配置。新建默认选择 S3；选择 local 或 S3 只切换未保存的草稿和差异预设，“载入预设”只修改草稿，取消不写数据库。保存已有 storage 时始终读取并核对 `meta.json`，空目标不能被编辑流程隐式初始化。 单容器维护状态支持完整搬迁后切换 local/S3 类型，要求上传、待写来源及后台任务已排空，并在保存后重启；步骤见[存储格式](storage-format.md#原始文件)。

同一 storage 的完整物理目录可由管理员搬迁；也可新建 storage，通过 Mokyu 复制/移动对象。物理搬迁必须保留根 `meta.json` 和全部路径，不需要 `raw/meta.json`。

profile 是类型化字段，不是任意 JSON。字段包括：

- `file_delivery` 默认 `application`；仅 local 可选 `x_accel_redirect` / `x_sendfile`，同时必须提供 `file_delivery_prefix`。前缀是代理内部 URI 或所见绝对目录，规范化为尾部 `/`，仅接受 3–512 字节的安全 ASCII 路径，拒绝根目录、点段、重复斜杠、编码字符及查询串。application 的前缀必须为 null；S3 不接受代理交付。开启后该存储不使用正文读缓存，回退请求仍流式读取，其他存储的缓存不受影响；预设不改变交付配置。条件、响应头、只读挂载与回收回退见[代理文件交付](local-nginx-deployment.md)
- `deduplication_enabled` 默认 false，仅控制 raw 的整文件去重；chunks 固定启用区块去重。更改仅影响新写入；关闭不会拆开已有共享文件；
- `write_durability` 仅单容器可选，默认 `durable`；`buffered` 允许 local chunks/raw 和 S3 chunks/raw 写缓存完整写入文件后响应，再由后台同步文件及目录；`volatile` 仅允许开启写缓存的 S3，接收完整内容到有界 RAM 后可响应，额度为零或不足时回退到 buffered 文件。buffered 在断电或系统崩溃时、volatile 在进程结束前尚未排空时，都可能丢失最近已确认的数据。Multipart 分片、复制、迁移和显式排空仍要求持久来源；无写缓存的 S3 请求及缓存满额 fallback 等待最终 S3 完成。重启核验未同步文件并隔离丢失的 RAM 来源，不返回旧对象代替已确认的新版本；这与 SQLite 的数据库同步策略分别生效。
- `public_cache_validation` 默认 `strict`，公开 Web 与匿名 S3 读取逐请求验证权限；设为 `background` 后可复用已验证的公开元数据，完整正文命中无需前台数据库查询。签名请求仍实时鉴权，条件请求、`Cache-Control: no-cache`、`Pragma: no-cache` 与校验和读取绕过权限快照。
- `public_cache_refresh_interval` 默认 `1s`，范围 `1s..60s`；`public_cache_max_age` 默认 `5s`，范围 `1s..5m`，刷新间隔不得超过最大有效期。期限从成功权威验证开始，访问、队列满和刷新失败均不能续期，过期必须前台验证。本机对象权限、覆盖删除、桶、域名和后端配置变更在确认前使快照失效；其他节点可能在硬期限内返回旧权限或旧版本，需立即撤销公开访问的存储应保留 `strict`。
- `read_source_validation` 默认 `strict`，来源选择仍查询数据库；`background` 允许普通读取复用健康来源快照，`source_validation_max_age` 默认 `5s`，范围 `1s..5m`。单容器 raw 同时启用公开快照后，正文未缓存也可在两个期限内直接取得文件保护并读取；chunks 来源缓存独立有界，后台刷新失败不能延长期限。本机隔离与配置变更使快照失效，群集仍取得数据库读租约，pack 来源选择保持同步。写入引用、迁移、修复及完整检查不使用来源快照，文件长度、解密和摘要验证不因该选项关闭。
- `read_integrity` 控制普通 S3/Web 读取的额外摘要复核，默认 `inline`；`background` 先响应，再使用最多 4 个作业和共 8MiB 的读取分配做复核（按原分配容量计费，已计费的待写 RAM 继续持有原预算），额度或 CPU 忙、读取取消时可跳过；`off` 不做额外摘要复核。两者都可能返回尚未发现损坏的内容，文件长度、解压上限、加密认证及映射范围仍必须通过。维护检查、迁移、重建和新引用保持原有强校验边界。
- 后台完整 raw 复核使用实际发送的字节，Range 则可另行检查完整来源；pack 普通后台复核检查请求涉及的完整 chunk，完整维护检查仍检查整个 pack。错误先拒绝坏缓存，再检查当前后端命名空间与完整内容，只有确认损坏才沿正常事务流程隔离。后台队列不持久化，重启不保证执行之前尚未完成的可选复核。
- 未完成核验的整文件响应缓存只能在原配置代次复用，后台失败或跳过时拒绝复用，切回 `inline` 不能把旧弱缓存当成已核验数据。已核验的不可变正文继续复用；普通后台 chunk 核验通过后可填充磁盘读缓存。raw 的 `background/off` 直接流式读取冷来源，不为填充磁盘读缓存预读整个文件，已有磁盘读缓存与写转读缓存仍可用。`status.read_checks` 提供通过、失败、跳过、关闭、在途及内存占用计数，8MiB 上限计入运行时预留空间
- 热门路径的完整小对象覆盖 PUT 可在接收时预留正文缓存额度，发布成功后复用同一份接收数据；只异步核对当前映射，不重新读取文件，也不延迟 PUT 确认。只写未读的新路径、未知长度、超出单条上限及额度不足时跳过。候选最多16个，2s内未接纳则释放，失败/取消/no-store 不进入正文缓存。S3 RAM 写缓存与响应缓存共享接收分配，但分别持有写入和读取额度；排空后写额度不因读缓存仍在而滞留
- 公开元数据使用 runtime 预留内的独立 `1MiB` 有界额度（最多 256 条、每条最多 `256KiB`），正文缓存关闭时仍可复用；热 `HEAD` 和 `OPTIONS` 无需读取文件。正文未命中仍取得来源保护，群集保留数据库读租约。状态 `public_read_cache` 提供条目、额度、命中、刷新失败和队列跳过计数，原始 `private`/`no-store` 元数据不准入。
- `write_flush_interval` 默认 `0s`，范围 `0s..60s`，控制文件同步和 S3 写缓存排空前的合并等待；后端积压超龄、缓存压力、显式 flush 和正常关闭提前推进，重启恢复不等待此间隔。
- `write_backlog_max_age` 默认 `5m`，范围 `1s..24h`，超过后优先处理旧来源，新的 S3 缓存写入遵守 `upload_cache_overflow` 的同步回退或 `503 SlowDown` 策略，本地缓冲写入改为等待持久化。年龄来自每秒更新的有界后端快照，刷新失败超过 5 秒时停止接收新的待排空积压；已有来源保留并重试，不按年龄删除。该期限是准入及调度阈值，不保证故障后端在期限内完成上传。
- `background_scheduling` 默认 `balanced`，可选 `foreground`。前台优先在每个节点上将后台对每个后端读、写和控制并发的占用限制为一半（向下取整且至少一个），有等待时为后台保留一个位置；单槽沿用老化调度。群集共享配额、CPU、进程总 I/O 和数据库预算仍共同约束，配置变化不取消在途工作。
- `write_processing` 默认 `inline`，开启 CDC 或 raw 去重时可选 `background`。后台 CDC 的普通 PUT 先保存可读 raw，再分块、查重和编码，新块直接生成最终区块或区块包；Multipart 继续按到达数据流式分块。S3 开启写缓存时确认本地文件与对象映射后响应，后台直接从缓存转换，不先上传完整 raw；没有写缓存时先等待完整原始文件可靠保存。本地 storage 不使用磁盘写缓存。raw 的 inline 去重先查重再上传，background 先保存独立文件再查重；已有工作保留接受时的设置和密钥，重启继续，切换表示保留对象元数据并等待已有读者释放保护；
- `cdc_enabled` 默认 true；local 和 S3 均可关闭。关闭时必须清除 block key，同时禁用压缩、pack 和 pack Range；Web 在草稿中联动这些值，保存后生效，重新开启不会恢复旧 key。已有对象保持可读；

- 连接超时、请求超时、重试次数、重试窗口、优先级老化、最低存储期限、未引用宽限和删除保留期；
- `multipart_idle_timeout`：未完成上传的空闲期限，默认 `24h`，可设置 `7d`；只有实际写入进展续期，查询列表不会延长期限；
- local 的 `min_free_space`：目标后端文件系统的最低可用空间，默认 `1GiB`；S3 不提供此项；
- cache 开关、大小和条目由 S3 的 chunks/raw 磁盘读缓存共用。S3 raw 可启用 `upload_cache`，文件同步到节点缓存、对象映射提交后响应，随后去重及上传；群集使用同样的节点本地缓存合同，跨节点未就绪读取短等后返回可重试错误。本地 storage 不使用磁盘读写缓存。实例待上传总额与后端局部限额同时生效。`upload_cache_overflow` 默认 `fallback`，缓存不可用时回退同步处理；设为 `reject` 则返回 `503 SlowDown`，请客户端稍后重试。关闭缓存时该选项不生效，关闭缓存或缩小额度不会丢弃既有待上传唯一副本；
- compression 的 `never`、`always`、`sample`、等级、采样等级和最小节省阈值；
- pack 开关、大小、刷新/复用/回收/合包/Range 参数；local storage 不创建新区块包，Web 会固定关闭该选项；
- `pack_concurrency`、`integrity_concurrency`、`integrity_rps`、`integrity_bandwidth`、`integrity_batch_timeout`；
- storage 级 read/upload/control、pack、integrity、maintenance、migration 并发和字节预算。

后端策略在 Web storage 页面及 `storage profile` CLI 中独立调整。并发、速率和字节预算保存后下一批任务使用新值；调大压缩工作区或 pack 大小若超出节点已分配的内存容量，会标记需要重启并暂停数据服务，重启后重新计算节点预算。预算本身不足的配置会被拒绝。单容器使用进程内并发队列和巡检速率账户，群集通过 PostgreSQL 共享这些额度。巡检同时检查 RPS 与字节预算，任一不足时均不扣减；大于一秒带宽预算的单批仍可前进。修改 profile 保留已消耗额度；单容器重启或重新启用 storage 后允许一次有界突发。

### 读写预设

Web 存储编辑中的“读写策略”提供三套服务端模板，先预览差异与确认点，再载入当前草稿，点击保存后才生效。预设只组合当前后端的选项，不保存独立模式；选项不再完全匹配时显示自定义。CDC、去重、压缩、加密、pack、容量及其他后端不随预设改变。

| 选项 | `reliable` 可靠性优先 | `cached` 缓存优先 | `throughput` 吞吐优先 |
| --- | --- | --- | --- |
| 公共权限快照 | 每次确认 | 后台刷新，最长 5s | 后台刷新，最长 30s |
| 来源健康快照 | 每次确认 | 每次确认 | 后台刷新，最长 30s |
| 额外摘要 | 请求内复核 | 请求内复核 | 有界后台复核 |
| 单容器持久性 | durable | durable | local 为 buffered，S3 写缓存为 volatile |
| 写入处理 / 合并等待 | inline / 0s | inline / 0s | background / 1s |
| 后台调度 | balanced | foreground | foreground |

三套模板的公共快照刷新间隔均为 1s，积压期限均为 300s；S3 开启写缓存且满额时 fallback，local 始终禁用写缓存。群集模板不包含 `write_durability`，节点本地写缓存仍需持久化。吞吐优先允许权限撤销延迟、未复核内容及崩溃丢失；S3 的 volatile 需要实例显式配置写入内存，预算为零时回退普通持久缓存。

预览显示实际 SQLite 同步模式和单容器内存额度，但不会修改它们。选择可靠性优先不会自动把 SQLite NORMAL/OFF 改回 FULL，也不会更改进程关闭等待。群集各数据节点的实际内存额度在节点运行配置查看。CLI 的预览和保存步骤见[CLI 参考](cli-reference.md#存储)。

旧部署升级时，运行设置中的上传空闲期限会一次写入已有后端，随后移除全局项；已有 local 后端的磁盘余量会在启动监听前一次写入。之后各后端独立保存，修改节点参数不会覆盖这些值。`instance.min_free_space` 继续保护节点数据目录中的 Multipart 暂存和缓存；同一文件系统上的并发写入共享预留账户，目标后端另按自己的余量限制检查。

## 加密、压缩与存量维护

block key 在数据库生成，创建后算法和材料不可更改。支持 AES-256-GCM、ChaCha20-Poly1305 和无加密，默认 AES-256-GCM；storage 只绑定已有 block key，未找到匹配算法的 key 时表单提供“在新标签页创建 key”的入口，不会在保存 storage 时隐式生成。

切换 storage 当前 key、压缩规则或区块包策略只影响新写入。`storage rewrite` 是显式、可预览、可确认、可暂停、可重试的低优先级任务，按 encoding/pack fingerprint 重写需要转换的存量来源；保存表单和“载入预设”都不会自动启动 rewrite。旧 key 在所有物理来源和任务释放后才可删除。单容器 rewrite 只创建一个任务，保存扫描上界和游标，按区块 ID 顺序推进；群集保留分片子任务。

## 模式与 CLI

入口命令决定角色；配置文件使用 `[database]`、`[instance]` 及可选 `[replica_database]`：

```sh
mokyu                 # 单容器模式
mokyu control         # control
mokyu worker          # worker
mokyu data            # data
mokyu cli status      # 本机 control HTTP
```

`mokyu cli` 访问 `127.0.0.1:instance.listen_control`。单容器使用部署密钥认证，群集 control 使用本机 `node.json` 凭据。worker/data 只提供 status。入口与数据库模式不匹配时仍监听管理/状态端口，data 返回 503、worker 和调度暂停；`cluster mode single|cluster` 可在该状态下修正数据库模式，成功后重启入口。

## 运行环境信息

运行配置沿用现有字段列表，以“运行环境 / 读取值”展示数据库程序、连接初始化时的版本和结构版本，以及程序版本、平台、CPU 和内存识别结果。环境行只读；`effective.*` 仍是高级详情中的最终预算。MySQL 配置连接 MariaDB 时，程序名称显示 MariaDB。

CPU 配额保留小数；启动时可用内存不代表当前剩余内存，也不等于容器上限。未限制、无法读取、尚未读取和不适用分别展示，零值仍是有效数值。从库版本只在按需连接成功后保存，浏览页面不会触发从库连接；读取时间不代表实时健康状态。密码、密码文件和部署密钥不回显，从库继承的非秘密配置标注为继承值。

群集的运行配置分为“部署设置”和“节点信息”。部署设置展示主库保存的目标值及相关节点的生效进度；节点信息固定选择 node id，展示实际加载的配置、修订、环境和有效预算，字段只读。当前接入的控制节点与选中的查看目标分别标明。单容器继续使用原页面结构，不显示群集范围切换或节点选择。
