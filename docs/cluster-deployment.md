# 群集部署

Mokyu 的群集模式让多个进程共享一个 PostgreSQL 数据库和部署密钥。进程使用同一镜像，通过启动命令选择职责；配置文件只保存数据库、实例监听、资源预算、数据目录和部署密钥等启动字段。存储、账户、节点注册信息和其他运行设置保存在数据库中。

群集模式不代替 PostgreSQL 做故障转移。需要高可用数据库时，由部署环境提供稳定的 PostgreSQL 连接地址；所有 Mokyu 进程直接连接这个地址。Mokyu 负责节点注册、任务租约和按角色提供 HTTP 入口。

## 拓扑

下面的拓扑对应三台机器。控制节点和 worker 可以在同一台机器上，也可以分开；data 节点由部署管理员自行安排宿主机。

```text
                         admin.example.com
                               |
                         control-a
                               |
                    +----------+----------+
                    |                     |
                 data-b + worker-b    data-c + worker-c
                    |                     |
             shared PostgreSQL + staging S3

       s3.example.com    -> data-b, data-c
       media.example.com -> data-b, data-c
```

典型安排如下：

| 机器 | 容器 | 监听和职责 |
| --- | --- | --- |
| A | PostgreSQL、`mokyu control` | 提供数据库和管理 API、管理页面、控制 CLI；不提供 S3 或公共读 |
| B/C | `mokyu data` + `mokyu worker` | data 提供 S3/公共读，worker 领取后台任务；两者各自直接连接 PostgreSQL |

每个容器使用自己的配置文件和 `instance.data_path` 目录，不共享本地目录。所有容器必须使用同一个不可替换的 `instance.deployment_key`，并连接同一个数据库。data 节点和 worker 仍需要数据库地址，不从 control 读取数据库连接信息。

启动命令是镜像的角色开关：

```sh
mokyu                 # 未指定命令，单容器模式（single）
mokyu control         # 管理/control
mokyu worker          # 后台任务/worker
mokyu data            # S3 和公共读/data
mokyu cli ...         # 通过本机 control HTTP 入口执行管理命令
```

CLI 向本机 `instance.listen_control` 发送请求。单容器使用配置中的部署密钥认证，不注册节点或创建 `node.json`；群集 control 使用 `node.json` 中的节点凭据。data/worker 在模式不匹配或需要恢复时只提供 `/status`，不会接收业务流量或调度任务。

## 网络和 DNS

反向代理把管理域名转发到 `instance.listen_control`，把数据域名转发到 `instance.listen_s3` 或 `instance.listen_web`。control、worker 和 data 都直接连接 PostgreSQL；节点注册和心跳也写入数据库，不经过 control URL 或节点间转发。data 的对象读写不会经过 control。

数据面可以使用多 IP A 记录：

```text
s3.example.com    A  <machine-b-ip>
s3.example.com    A  <machine-c-ip>
media.example.com A  <machine-b-ip>
media.example.com A  <machine-c-ip>
```

DNS 轮询不提供健康检查；客户端应按 S3 重试规则处理节点不可用。管理域名也可以使用多 IP A 记录，但各 control 进程独立验证自己的节点凭据，不依赖主备租约或请求转发。

## 首次安装

1. 准备 PostgreSQL 数据库、每个容器独立的配置和 data 目录，生成一次部署密钥，并把同一字符串写入所有配置的 `instance.deployment_key`。
2. 启动一个临时单容器实例，让它创建数据库结构并完成初始管理设置；也可以在 control 容器上直接执行相同 CLI：

   ```sh
   docker exec mokyu mokyu cli status
   docker exec mokyu mokyu cli setup token
   ```

3. 在数据库中创建共享 S3/S3 兼容 staging storage 和至少一个普通 S3 storage，再创建桶。群集模式拒绝 local 存储；`instance.data_path` 仍可用于节点状态和本机临时文件，它不是共享存储。
4. 停止单容器，使用 control、worker、data 的独立配置启动对应进程。B/C 的每台机器同时启动一个 data 和一个 worker；每个进程首次启动时在 PostgreSQL 注册自身并把凭据写入各自 `instance.data_path/node.json`。

## 节点注册和管理

节点不使用 join token。control、worker 和 data 直接使用部署密钥保护的数据库注册流程：节点名称来自 `instance.name`，角色来自启动命令，凭据只写入本节点的 `node.json`。重启时复用该文件；文件丢失或数据库中已有同名节点时，进程进入 `recovery_required`，不会覆盖旧节点身份。

把每个节点的 `node.json` 纳入备份。恢复时先恢复对应文件；若文件不可恢复，管理员可在 control 上释放旧节点，再让该角色重新启动并注册。

节点管理命令：

```sh
docker exec mokyu-control mokyu cli cluster status
docker exec mokyu-control mokyu cli cluster nodes
docker exec mokyu-control mokyu cli cluster release <node-name> --confirm
```

`cluster status` 返回 deployment mode、enabled 节点数和 local storage 数；`cluster nodes` 返回节点 id、名称、角色、启用状态和最近心跳；`cluster release` 禁用节点、释放其 lease 并把未完成任务重新排队。所有管理命令都经由 control HTTP API 执行，并记录 CLI 审计来源。

## 模式切换和恢复

使用当前 single 或最后一个 control 的 CLI 切换数据库部署模式，也可由管理员通过管理 API 操作：

```sh
docker exec mokyu-control mokyu cli cluster mode cluster
docker exec mokyu-control mokyu cli cluster mode single
```

1. 暂停定时维护策略，完成 queued/running/paused 任务；完成或中止上传，并执行 `cache flush` 排空已确认的暂存数据。
2. 进入 cluster 前迁移并移除所有 local storage，配置启用的 S3 `uploads.staging_storage_id`。回到 single 前释放其他 enabled 节点；当前 control 会在成功切换时释放自己。
3. 停止其他所有进程，包括已经 release 但尚未退出的节点；只保留执行切换的 single/control。进入维护模式并等待活动读写、GC、清理与读取租约排空。仍被引用的 multipart 暂存或待上传数据会阻止切换。
4. 执行模式切换。服务在数据库独占运行权下更新 `deployment_mode_revision`，保留独占直到当前进程退出；旧进程停止接受数据操作和新任务，不能直接恢复服务。
5. 停止当前进程，再按新模式启动对应角色。核对 `status --ready` 后退出维护模式，恢复需要的定时策略。

同一模式的重复设置不重复切换。失败任务历史保留，但不能在另一模式恢复旧任务形态；不会自动转换任务树。切换请求中断或提交结果无法确认时，当前进程保持受限，应核对数据库模式后重启。

角色与数据库模式不匹配时，control/single 继续提供管理入口；worker/data 只提供 `/status` 并停止调度或数据面服务。`mokyu cli status --ready` 只有在模式匹配、没有待重启的切换且未处于 `recovery_required` 时才成功；群集角色还要求节点已注册。启动时读取的模式在当前运行权期间固定，请求无需反复查询数据库；修正数据库模式、节点文件或部署配置后须重启进程。

维护模式由任一 control 写入部署状态，各节点随 10 秒心跳刷新；数据库读取失败时保留上次状态。进入维护后，确认各节点工作状态已切换且在途操作排空，再执行要求停写的操作。退出维护后，各节点刷新状态即可恢复写入，无需重启；正在停机排空的进程不会重新接受写入。

## 存储限制与迁移

群集模式禁用 local 存储。单容器模式切换到群集模式时，Mokyu 会检查存储和运行状态，不会替部署管理员上传文件：

1. 选择迁移方式：新建 S3 storage 和目标桶，由 Web/CLI 的对象复制或移动任务迁移；或在单容器维护状态下，自行完整搬运原存储命名空间和根 `meta.json` 到 S3，再修改原 storage 的类型与连接参数。具体边界见[存储格式](storage-format.md#原始文件)。
2. 自行搬迁前完成或中止 Multipart、排空待写来源及后台任务，停用公共/S3 入口和后台清理，复制后核对完整性；修改类型后重启单容器。由 Mokyu 跨存储迁移时核对任务结果，再移除原 local 桶和 storage。桶的 storage 绑定不直接更改。
3. 配置共享 S3 暂存，确认没有 local storage 后再执行 `cluster mode cluster`。

桶绑定的 `storage_id` 永久不变；同一 storage 的桶共享去重，不跨 storage 去重。跨 storage 的对象迁移由 Mokyu 异步任务负责复制、权限、校验和发布一致性；部署管理员负责目标容量、凭据、网络和供应商策略。

## 可选 PostgreSQL 从库

先为公共入口启用合适的响应缓存，再按元数据分页负载考虑从库。data 可以通过 `[replica_database]` 指定一个 PostgreSQL 物理热备入口，连接字段及凭据继承规则见[配置参考](configuration.md#replica_database)。无需启用同步复制；不配置时全部读取继续走主库。

分流仅覆盖 Web 公共读和匿名 S3 公共读的后续正文页。路由、ACL、当前对象、完整响应缓存校验、首个页面、Range 起点、物理来源及其保护仍在主库；签名/预签名 S3、管理请求、后台任务、写入、迁移和通知不使用从库。每页最多 64 条映射；小对象和完整正文缓存命中通常没有从库查询。

每个来源读取在主库创建独立 UUID 读租约。后续页在从库同一 SQL 快照中确认该租约标记可见，再读取对应不可变 stream 的映射；看不到标记立即回主库，不等待 WAL 追平。从库中残留的旧租约不能用于新请求，也不能续活主库已过期的租约。主库续约、运行权及来源保护仍然决定请求能否继续。

从库连接检查 PostgreSQL 版本、hot standby 状态、部署 ID、部署密钥指纹和 schema 版本，页面查询再次检查身份及恢复状态。账户只需 SELECT 权限；从库不会执行初始化、迁移、锁定业务记录、心跳或租约写入。连接按需建立，失败时健康主库仍可用。

池忙直接回主库；新连接获取最多等待 200ms，页面查询最多 100ms，超时连接关闭。未追平后短暂退避 250ms，故障按 1–30 秒退避，并限制恢复探测。每页至多回主库一次，沿用原 stream 和尚未发送的起点；不重播已发字节、不拼接不同对象版本。主库也无法提供数据时中止响应流。

`instance.database_connections` 保留主库预算，`instance.database_read_connections` 独立限制从库池。主库池包括通知监听：control 至少 5 个连接，容纳通知、统计与维护协调、管理权限保护和查询；data/worker 至少 4 个，chunk claim 在取连接前排队，为两条通知监听和一个推进查询保留容量。这些是启动下限，高并发需增加预算。owner 连接在池外，仍需单独计入数据库总连接预算以及主从数据库的 CPU/内存。不要仅因配置了从库就缩减主库可靠写和故障回退容量。`status.read_replica` 返回 `disabled`、`not_applicable`、`idle`、`available`、`lagging` 或 `backoff`，并提供池大小、尝试、成功页、未追平、错误、忙碌、回退次数和下次尝试时间；可用状态只反映最近结果，每页仍需检查本次租约。

PostgreSQL 故障切换和旧主库隔离由部署环境负责。主库运行权连接丢失时 Mokyu 退出，切换后重启并为新请求创建新租约；旧时间线的从库无法提供这些标记，因此回退新主库。被提升的从库因不再处于 recovery 状态停止接受分流。此功能不把从库变成可脱离主库运行的数据面。

## 备份、升级和故障边界

PostgreSQL 数据库、WAL、部署密钥、每个容器的 data 目录（尤其是 `node.json`）和存储区块仍按[部署与恢复](deployment-and-recovery.md)的要求备份。

群集部署只改变进程职责，不改变 S3 兼容接口、存储格式或 PostgreSQL 的恢复责任。S3 服务、DNS 健康检查、TLS 终止和 PostgreSQL 主备切换由部署环境提供和维护。

正文读取在同一事务中固定对象版本和创建读取租约。续约间隔为租约 TTL 的三分之一，TTL 为读取空闲超时与 30 秒中较大值的两倍；续约失败或租约已经过期时停止取源。续约次数随读取时间增长，不随区块数量增长。HEAD/304 不获取来源租约；单容器继续使用本机 pin，不写群集租约。

完整响应缓存由各 data 节点独立配置和计费，control/worker 不分配正文缓存。普通 GET 有本机正文候选时，通过主库单个快照检查当前对象、权限、桶设置版本及最多 64 条逻辑映射；成功命中直接发送独立内存正文，不创建或释放来源租约。候选失效或缺少所需编码时重新取得来源保护并检查当前对象；没有候选的冷读沿用正常路径。权限生效不依赖节点间失效通知或 TTL；校验后已经开始的响应可以完成，后续请求按新权限处理。添加 data 节点可分摊传输和编码，但不会消除 PostgreSQL 元数据负载。

## 节点观测

群集的节点报告独立于心跳和请求执行，每约十秒保存一次本地资源、缓存、请求和队列快照。页面查询数据库中的报告，不逐台连接节点。联系状态（在线、失联、尚未报告、已释放）、工作状态、报告新鲜度和配置状态分别显示；最近心跳或报告超过三十秒即不作为当前生效证明。没有注册身份的失败容器只能通过本机状态和日志诊断。

节点报告绑定有效注册凭据、启动编号及递增序号，旧启动、乱序和已释放身份不能覆盖新报告。配置只包含实际加载的白名单值及修订，秘密在持久化前脱敏；静态配置与动态状态各有大小上限。报告失败保留旧时间，不影响心跳。节点详情最多返回二十项正在执行的任务。

部署共享设置保存后，按角色实际使用的字段比较每个节点的加载值。值不同表示需要重启；值相同但修订不同表示值一致，不强制重启；报告缺失、过期或格式未知表示状态未知。Web 设置只对 control 生效，S3 域名和区域只对 data 生效；control 的 Web 上传仍使用上传超时和并发限制。这里不提供跨节点修改配置文件、自动重启或自动释放节点。

管理控制台的群集服务状态默认显示概况和节点列表，可按名称、角色、联系或工作状态筛选。默认排除已释放节点，可单独筛选查看；详情按固定 node id 展示最近报告、缓存/队列和当前任务。节点未响应与工作受限分别呈现，没有独立 worker 不被视为故障。部署配置提示使用全体相关节点的报告，不以接入 control 的修订代替群集生效情况。

节点历史按 `metrics.runtime_interval` 记录，保留期沿用 `metrics.runtime_retention`。control 平等竞争短期汇总锁，每轮最多读取 4096 个有联系节点的最近两个采样；超过范围的节点仍计入预期数，并显示部分覆盖。汇总和清理受超时限制，清理每轮最多八批，每批最多一千项；失败保留已有结果，不在页面请求中补算历史。

请求和后端流量先按相同节点、启动编号与单调时间计算差分，再相加；公共请求统计只计 Web/S3 数据入口，不计 control 管理流量。缓存命中率使用有效窗口的命中数之和除以查询数之和，不平均各节点百分比。不汇总节点 p95/p99。重启后的首样本和缺失基线显示未知；共享待上传和任务只从主库统计一次。各节点缓存及 RSS 为有覆盖说明的副本总和，不等于部署唯一文件量。
