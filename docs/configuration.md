# 配置参考

主配置是 [`config.example.toml`](../config.example.toml)，默认路径 `/config/config.toml`。文件只保存启动前必须可用的 `[database]` 和 `[instance]`；Web、请求、上传、统计、维护和 storage 策略通过 Web 专项表单或 `mokyu cli` 写入数据库。

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

SQLite 使用 WAL、外键和 FULL 同步，数据库及其 `-wal`、`-shm` 文件必须位于可写的本地持久文件系统，不支持网络共享盘。默认文件是 `/data/mokyu.db`；新建数据库和锁文件权限为 `0600`，新建父目录为 `0700`。一个数据库文件只允许一个 Mokyu 实例，符号链接按实际路径识别，数据库硬链接被拒绝。文件被替换或运行锁丢失时进程退出。

SQLite 将 `instance.database_connections` 中的一个连接用于串行写入，其余用于只读查询；设置为 `1` 时读写共用一个连接。更高的连接数不增加同时写入的事务数。

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
| `instance.multipart_limit` | 自动 | 单容器本地 multipart 暂存上限；群集使用共享 staging storage |
| `instance.cache_size` | 自动 | 本地区块缓存字节上限 |
| `instance.cache_entries` | 自动 | 本地缓存索引条目上限 |
| `instance.upload_cache_size` | 自动 | 本地待上传暂存上限 |
| `instance.thumbnail_cache_size` | `256MiB` | 缩略图缓存上限 |
| `instance.request_connections` | 自动 | 本节点 HTTP/S3 请求连接池上限 |
| `instance.database_connections` | 自动 | 本节点数据库连接池上限 |
| `instance.cpu_jobs` | 自动 | 本节点压缩、哈希和加解密等阻塞计算预算 |
| `instance.stream_memory_budget` | 自动 | 数据流临时内存预算，不是 RSS 硬限制 |
| `instance.storage_read_slots` | 自动 | 本节点所有 storage 共享的读槽位 |
| `instance.storage_upload_slots` | 自动 | 本节点所有 storage 共享的上传槽位 |
| `instance.storage_control_slots` | 自动 | 本节点所有 storage 共享的控制槽位 |
| `instance.compression_idle_timeout` | `30s` | 本节点压缩上下文空闲回收时间 |

省略 `request_connections` 和三个 `storage_*_slots` 时，服务按 CPU、内存、文件描述符和连接池自动计算保守但偏宽的本机预算。它们只限制本机；storage profile 则限制每个 storage 的总并发，单容器使用进程内队列，群集通过 PostgreSQL 共享许可。profile 热更新保留正在执行和排队的操作；降低上限后等待现有操作排空，提高上限则放行等待者。显式修改 instance 预算需要重启。

共享 staging 设置只对 PostgreSQL 提供；MySQL、SQLite 不接受这两个设置。PostgreSQL 的 single 可以预先配置供群集切换使用。群集模式禁止 local storage。即使只有一个 data，也必须配置共享 S3/S3 兼容 storage 作为 `uploads.staging_storage_id`；multipart 尾块和异步上传使用保留前缀 `__mokyu/multipart/`。单容器模式可使用 `/data/storage` 的 local storage。

## runtime 设置

runtime 设置保存在 `runtime_settings`，只有以下五组。Web 使用固定的可爱专项表单，不提供任意 JSON 编辑器；CLI 可用 `settings get/set/reset` 完整管理。

| 分组 | 主要内容 | 作用范围 |
| --- | --- | --- |
| `web` | origin、cookie、session、Gravatar | 部署共享 |
| `requests` | 读写空闲超时、请求行为 | 部署共享，重启提示按字段显示 |
| `uploads` | staging storage、共享 staging 字节上限、上传过期与活动上传限制 | 部署共享；群集 storage 共享 |
| `metrics` | storage/runtime/access 采样间隔、保留期和查询超时 | 部署共享 |
| `maintenance` | GC、清理、任务保留、审计保留和批量大小 | 部署共享 |

runtime section 使用 revision 乐观并发。冲突返回当前 revision；重置恢复当前版本默认值。设置改变后各节点在请求边界、任务领取边界和低频刷新时重新加载。

## storage profile

存储连接和所有后端行为都在 storage 创建/修改表单中配置。新建默认选择 S3；选择 local 或 S3 只切换未保存的草稿和差异预设，“载入预设”只修改草稿，取消不写数据库。保存已有 storage 时始终读取并核对 `meta.json`，空目标不能被编辑流程隐式初始化。

profile 是类型化字段，不是任意 JSON。字段包括：

- 连接超时、请求超时、重试次数、重试窗口、优先级老化、最低存储期限、未引用宽限和删除保留期；
- cache 开关、大小和条目、待上传缓存；
- compression 的 `never`、`always`、`sample`、等级、采样等级和最小节省阈值；
- pack 开关、大小、刷新/复用/回收/合包/Range 参数；local storage 不创建新区块包，Web 会固定关闭该选项；
- `pack_concurrency`、`integrity_concurrency`、`integrity_rps`、`integrity_bandwidth`、`integrity_batch_timeout`；
- storage 级 read/upload/control、pack、integrity、maintenance、migration 并发和字节预算。

这些并发、速率和字节预算在 Web storage 页面及 `storage profile` CLI 中调整，保存后下一批任务使用新值，不要求重启。单容器使用进程内并发队列和巡检速率账户，群集通过 PostgreSQL 共享这些额度。巡检同时检查 RPS 与字节预算，任一不足时均不扣减；大于一秒带宽预算的单批仍可前进。修改 profile 保留已消耗额度；单容器重启或重新启用 storage 后允许一次有界突发。

## 加密、压缩与存量维护

block key 在数据库生成，创建后算法和材料不可更改。支持 AES-256-GCM、ChaCha20-Poly1305 和无加密，默认 AES-256-GCM；storage 只绑定已有 block key，未找到匹配算法的 key 时表单提供“在新标签页创建 key”的入口，不会在保存 storage 时隐式生成。

切换 storage 当前 key、压缩规则或区块包策略只影响新写入。`storage rewrite` 是显式、可预览、可确认、可暂停、可重试的低优先级任务，按 encoding/pack fingerprint 重写需要转换的存量来源；保存表单、local/S3 切换和“载入预设”都不会自动启动 rewrite。旧 key 在所有物理来源和任务释放后才可删除。单容器 rewrite 只创建一个任务，保存扫描上界和游标，按区块 ID 顺序推进；群集保留分片子任务。

## 模式与 CLI

入口命令决定角色；配置文件只包含 `[database]` 和 `[instance]`：

```sh
mokyu                 # 单容器模式
mokyu control         # control
mokyu worker          # worker
mokyu data            # data
mokyu cli status      # 本机 control HTTP
```

`mokyu cli` 访问 `127.0.0.1:instance.listen_control`。单容器使用部署密钥认证，群集 control 使用本机 `node.json` 凭据。worker/data 只提供 status。入口与数据库模式不匹配时仍监听管理/状态端口，data 返回 503、worker 和调度暂停；`cluster mode single|cluster` 可在该状态下修正数据库模式，成功后重启入口。
