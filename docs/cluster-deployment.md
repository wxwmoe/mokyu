# 群集部署

Mokyu 群集由平等的 control 节点、提供 S3/公共读的 data 节点和执行后台工作的 worker 组成。只有 control 连接共享 PostgreSQL 并持有部署密钥。data/worker 通过 HTTPS gRPC 取得业务元数据和任务，直接访问后端对象存储；对象正文不经过 control。

## 拓扑与配置

```text
data / worker -> control.example.com -> control-a / control-b -> PostgreSQL
      |                                    |
      +---------- shared S3 storage -------+

S3 clients -> s3.example.com    -> data nodes
Public GET -> media.example.com -> data nodes
Management -> control.example.com -> control nodes
```

| 角色 | 启动配置 | 入口和职责 |
| --- | --- | --- |
| `mokyu control` | database、deployment_key、唯一 instance.name，可选 replica_database | 同一 listen_control 端口提供管理页面、管理 API、CLI 和节点 gRPC；处理数据库任务、授权、调度与租约 |
| `mokyu data` | control_url、本机预算和数据目录、预创建 node.json | 提供 S3/公共读、正文传输和可领取的后台工作；本机 status |
| `mokyu worker` | control_url、本机预算和数据目录、预创建 node.json | 执行需要后端 I/O 或计算的后台工作；本机 status |

每个进程使用独立配置和持久 data 目录。control 使用 [`config.example.toml`](../config.example.toml)，data/worker 使用 [`config.node.example.toml`](../config.node.example.toml)。节点禁止配置 database、replica_database、deployment_key 和数据库连接预算；名称由预创建身份确定。数据库只需允许 control 访问。后端 S3 应允许相应节点直接访问。

data/worker 主动连接 `control_url`；节点状态查询、CLI 和容器健康检查默认监听 `127.0.0.1:9002`，无需向 control 或公网开放。节点示例省略 `listen_control`；需要外部监控采集时，可显式设置监听地址。

control 执行 usage、桶 purge 和历史清理等数据库任务。普通共享任务可以由 worker 或 data 执行，本机唯一来源任务只交给所属节点；多个 control 通过持久任务租约协调。数据库高可用、主备切换及旧主库隔离由部署环境负责。

## 网络与多地址

`instance.control_url` 是 HTTPS 域名源地址，例如 `https://control.example.com`。域名可以配置多个 A 记录，各入口应提供同一域名的有效证书并转发到对应 control：

```text
control.example.com A <control-a-public-ip>
control.example.com A <control-b-public-ip>
```

客户端最多保留 32 个 A 地址，按 DNS TTL 在 1–300 秒内刷新并复用健康地址的连接；DNS 临时失败时，已知集合在原刷新时间之后最多继续使用 300 秒。Unary 在地址间轮转，NodeSession 使用独立持久连接；连接和安全重试共用总期限。TCP 连接目标是选中的 IP，TLS SNI、证书校验和 HTTP authority 始终使用原域名。删除地址会触发该地址会话恢复，业务运行权由 control 确认。

S3/公共读域名也可以有多个 data 地址；这是面向 S3 客户端的入口选择，客户端需处理不可用节点并遵守重试规则。

### Nginx 与 Cloudflare

[`nginx.example.conf`](../nginx.example.conf) 在公网终止 TLS：普通管理、S3 和公共读以 `proxy_http_version 2` 使用 h2c 回源；`/mokyu.node.` 命名空间以 `grpc_pass grpc://` 转发到同一个 control 端口。使用 Nginx 1.30.6+ 或 1.31.7+，启用 HTTP/2 和 gRPC 模块，并安装安全补丁。替换域名、证书和 upstream 后执行 `nginx -t`；control_url 的域名须与 control server_name 一致。[HTTP/2 回源](https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_http_version)、[gRPC 模块](https://nginx.org/en/docs/http/ngx_http_grpc_module.html)、[安全公告](https://nginx.org/en/security_advisories.html)

管理 HTTP 和 gRPC 使用各自的 upstream keepalive 池，两者仍指向同一个 `listen_control` 地址；不要把示例的 `mokyu_control` 和 `mokyu_control_rpc` 合并，以免不同代理模块复用不相容的连接状态。

代理保留原始 Host、URI、查询参数、gRPC metadata 和 trailers；节点路径禁止缓存、重写和交互式挑战。Unary 重试由节点的幂等协议处理，代理不自动重发。NodeSession 的应用心跳与 HTTP/2 PING 独立，代理读写超时须覆盖心跳间隔。

接入 Cloudflare 时使用代理记录，在 Network 中启用 gRPC；源站入口使用 443、TLS 和通过 ALPN 声明的 HTTP/2，SSL/TLS 使用 Full (strict)。客户端均衡的是 DNS 返回的边缘 IP，Cloudflare 到源站的选择由 Cloudflare 管理，二者不是同一层均衡。节点凭据继续由 Mokyu 验证，管理页面的访问策略不能阻断节点 RPC。[Cloudflare gRPC 要求及限制](https://developers.cloudflare.com/network/grpc-connections/)

## 安装与启动

1. 准备 PostgreSQL、单容器/control 的配置和固定部署密钥，先按[部署与恢复](deployment-and-recovery.md)完成管理员初始化、共享 S3 staging、普通 S3 storage 和桶配置。群集禁止 local storage；`uploads.staging_storage_id` 须指向启用的 S3 暂存存储。
2. 在单容器上进入维护模式并排空工作，执行 `mokyu cli cluster mode cluster`，随后停止该进程。使用有唯一名称的配置启动 `mokyu control`；其他 control 使用同一数据库和部署密钥、各自的名称与目录。
3. 配置 TLS 代理和 control 域名，在 control 上预创建 data/worker。命令只在创建或轮换时返回完整凭据，将输出直接保存到对应节点目录，不发布到日志或共享位置：

   ```sh
   umask 077
   mkdir -p data-1 worker-1
   docker exec mokyu-control mokyu cli cluster create data-1 data > data-1/node.json
   docker exec mokyu-control mokyu cli cluster create worker-1 worker > worker-1/node.json
   ```

4. 为每个节点准备独立目录和 `config.node.example.toml`，设置 control_url，安全传送对应 node.json，并给运行用户读写权限。官方镜像使用 UID/GID 10001，身份文件权限为 0600。以 `mokyu data` 或 `mokyu worker` 启动，角色须与文件一致。
5. 通过本机 `mokyu cli status` 和管理面检查会话、配置与节点状态，退出维护模式后检查 `status --ready`。data 完成同步后提供 S3/公共读，worker 开始领取工作。

未指定启动命令的 `mokyu` 是单容器模式。data/worker 本机 CLI 只支持 `status`；其他管理命令在单容器/control 执行。

## 节点身份与恢复

control 首次启动通过 PostgreSQL 注册，使用自己的 node.json 恢复。data/worker 必须事先取得身份，不会自行注册；缺文件或角色不符会拒绝启动。连接不可用时保留本机状态入口并有界重连；复制同一身份启动另一个活跃进程会产生所有权冲突，不能同时服务。

把每个节点的 node.json 和持久 data 目录纳入备份。重建时恢复对应文件并确保原进程已停止。data/worker 身份文件不可恢复时，在 control 执行 `cluster rotate NAME --confirm`，安全保存返回文件并替换后重启；该操作保留节点 UUID、递增凭据代次、隔离旧会话并撤销旧任务租约。它不能恢复已经丢失的本机唯一正文。control 文件丢失时先恢复备份；无法恢复则释放原 control 身份后重新注册。

| 命令 | 行为 |
| --- | --- |
| `cluster status` | 部署模式、启用节点数和 local storage 数 |
| `cluster nodes` | 节点身份、角色、启用状态及最近报告 |
| `cluster create NAME data/worker` | 预创建身份并返回 node.json |
| `cluster rotate NAME --confirm` | 更换 data/worker 凭据并返回 node.json |
| `cluster release NAME --confirm` | 禁用节点，释放租约并重新排队未完成任务 |

启用后端写缓存时，确认规则取决于 storage 的持久性档位；待上传唯一文件保留在所属节点。其他节点等待共享来源就绪，超时返回既有 `503 SlowDown`；HEAD 和列表不等待正文。释放或轮换前应在 control 完成 `cache flush`，不能把释放节点当成迁移本机文件。正常 SIGTERM 停止新准入并继续续租、任务结果确认和本机上传排空；超过退出期限会报告未完成项，后续依赖原目录恢复。

## 模式切换和恢复

在单容器或最后一个 control 上执行 `cluster mode single|cluster`，前置条件是维护模式和已排空工作：

1. 暂停定时维护，完成或中止上传与任务，使用 `cache flush` 排空唯一暂存。进入群集前迁移并移除 local storage、配置共享 S3 staging；回到单容器前释放其他启用节点。
2. 停止其他进程，包括已释放但仍运行的进程；只保留执行切换的单容器/control。执行 `maintenance enable`，等待活动读写、读取租约和后台工作排空。
3. 执行模式切换。服务在数据库独占运行权下更新部署模式，当前进程进入需要重启的状态；停止它，再按目标模式启动。
4. 核对状态和配置，执行 `maintenance disable`，恢复维护计划并检查 `status --ready`。

重复设置相同模式不重复切换。未完成 Multipart、唯一来源或任务会阻止切换；失败任务历史保留，不自动转换任务树。请求中断或结果未知时先查询部署模式再重启。节点收到模式、凭据或运行权失效后停止相应工作；配置同步与续租确认不能由缓存或仅 HTTP/2 PING 替代。

## 存储限制与迁移


群集模式禁用 local 存储。单容器模式切换到群集模式时，Mokyu 会检查存储和运行状态，不会替部署管理员上传文件：

1. 选择迁移方式：新建 S3 storage 和目标桶，由 Web/CLI 的对象复制或移动任务迁移；或在单容器维护状态下，自行完整搬运原存储命名空间和根 `meta.json` 到 S3，再修改原 storage 的类型与连接参数。具体边界见[存储格式](storage-format.md#原始文件)。
2. 自行搬迁前完成或中止 Multipart、排空待写来源及后台任务，停用公共/S3 入口和后台清理，复制后核对完整性；修改类型后重启单容器。由 Mokyu 跨存储迁移时核对任务结果，再移除原 local 桶和 storage。桶的 storage 绑定不直接更改。
3. 配置共享 S3 暂存，确认没有 local storage 后再执行 `cluster mode cluster`。

桶绑定的 `storage_id` 永久不变；同一 storage 的桶共享去重，不跨 storage 去重。跨 storage 的对象迁移由 Mokyu 异步任务负责复制、权限、校验和发布一致性；部署管理员负责目标容量、凭据、网络和供应商策略。

## 可选 PostgreSQL 从库

只在 control 配置 `[replica_database]`，字段和最小只读权限见[配置参考](configuration.md#replica_database)。从库仅用于合格匿名公共读的后续区块映射页；首个 OpenRead、当前对象、权限、Range 起点、物理来源、租约和写入仍在主库处理。签名读取和后台任务不使用从库。

control 检查从库的部署身份、结构版本、hot standby 状态，以及同一快照中的读标记和载荷修订。池忙、未追平、身份异常或查询超时均沿当前窗口回退主库，不增加节点 RPC。连接获取最多 200ms，分页查询最多 100ms；不等待 WAL 追平、不重播已发字节、不拼接对象版本。

control 主库池至少五个连接，另有池外运行权连接；从库预算独立，不能代替主库可靠写与回退容量。`status.read_replica` 提供状态、尝试、成功页、未追平、错误、忙碌和回退计数；每页仍重新检查可见性。主库故障转移和旧主隔离由部署环境处理，从库不能让节点脱离 control 独立授权。

## 备份与故障边界

备份 PostgreSQL/WAL、部署密钥、各节点持久目录及 S3 载荷，参见[部署与恢复](deployment-and-recovery.md)。元数据和正文必须属于一致的恢复边界，单独的对象存储副本不能恢复数据库、权限或密钥。

读取在 control 上固定对象版本和精确物理来源，NodeSession 批量续租和释放；丢失释放由 TTL 回收，超时租约不能复活。control 失联后只在已确认运行权及来源保护期限内继续已有工作，不能无限发布或读取。断线恢复重新同步权威配置、资源和任务；连接切换不直接等于业务失败或成功。

已初始化节点的正常冷读在一次 OpenRead 中得到首窗口及缺失依赖，随后直接访问 S3。`include` 选择本次需要的材料，`cached` 声明仍持有的修订；失效材料在同次响应补齐。正文缓存和公开快照按当前存储策略使用，签名、条件及强制刷新请求继续验证。具体消息、批次、租约与重试边界见[节点 RPC](node-rpc.md)。

## 节点观测


群集的节点报告独立于心跳和请求执行，每约十秒保存一次本地资源、缓存、请求和队列快照。页面查询数据库中的报告，不逐台连接节点。联系状态（在线、失联、尚未报告、已释放）、工作状态、报告新鲜度和配置状态分别显示；最近心跳或报告超过三十秒即不作为当前生效证明。没有注册身份的失败容器只能通过本机状态和日志诊断。

节点报告绑定有效注册凭据、启动编号及递增序号，旧启动、乱序和已释放身份不能覆盖新报告。配置只包含实际加载的白名单值及修订，秘密在持久化前脱敏；静态配置与动态状态各有大小上限。报告失败保留旧时间，不影响心跳。节点详情最多返回二十项正在执行的任务。

部署共享设置保存后，按角色实际使用的字段比较每个节点的加载值。值不同表示需要重启；值相同但修订不同表示值一致，不强制重启；报告缺失、过期或格式未知表示状态未知。Web 设置只对 control 生效，S3 域名和区域只对 data 生效；control 的 Web 上传仍使用上传超时和并发限制。这里不提供跨节点修改配置文件、自动重启或自动释放节点。

管理控制台的群集服务状态默认显示概况和节点列表，可按名称、角色、联系或工作状态筛选。默认排除已释放节点，可单独筛选查看；详情按固定 node id 展示最近报告、缓存/队列和当前任务。节点未响应与工作受限分别呈现，没有独立 worker 不被视为故障。部署配置提示使用全体相关节点的报告，不以接入 control 的修订代替群集生效情况。

节点历史按 `metrics.runtime_interval` 记录，保留期沿用 `metrics.runtime_retention`。control 平等竞争短期汇总锁，每轮最多读取 4096 个有联系节点的最近两个采样；超过范围的节点仍计入预期数，并显示部分覆盖。汇总和清理受超时限制，清理每轮最多八批，每批最多一千项；失败保留已有结果，不在页面请求中补算历史。

请求和后端流量先按相同节点、启动编号与单调时间计算差分，再相加；公共请求统计只计 Web/S3 数据入口，不计 control 管理流量。缓存命中率使用有效窗口的命中数之和除以查询数之和，不平均各节点百分比。不汇总节点 p95/p99。重启后的首样本和缺失基线显示未知；共享待上传和任务只从主库统计一次。各节点缓存及 RSS 为有覆盖说明的副本总和，不等于部署唯一文件量。

节点详情显示 control_url、当前连接 IP、control ID、会话阶段、最近错误和 RPC 延迟/尝试/重试计数。最近错误保留为诊断记录，不能仅凭该字段判断当前离线；当前工作以会话阶段和报告时间为准。未采样队列显示未知，不伪造零值。访问观测使用独立有界批次，缺报或丢批会重置统计覆盖起点，不阻塞前台读取。
