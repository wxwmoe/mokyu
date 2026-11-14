# 存储群集

`mokyu storage` 是由 control 登记的独立存储角色，仅限 `single_control` 和 `cluster`。storage 使用节点凭据注册，不连接元数据库，不配置部署 key。地区、地址和存储分组由 storage 的配置文件宣告；群集策略和成员归属保存在 control 的数据库中。

原生后端支持双副本 PUT、GET、HEAD、Range、跨节点 Multipart、元数据日志与恢复，不依赖远端 S3。启用后端时会实际写入探针并读回；只有验证通过才能承接业务。

## 从空部署开始

空数据库可以在没有 S3 地址或凭据时进入群集模式。先初始化管理员，再通过本机 CLI 开启维护并切换模式：

```sh
mokyu cli maintenance enable
mokyu cli cluster mode single_control
```

重启主容器，保留原数据库、部署 key 和 data 目录。多 control 使用 PostgreSQL，将目标模式改为 `cluster`，重启为 `mokyu control`。这项引导仅适用于没有桶和对象、也没有 local/S3 后端的部署。已有数据仍须按[群集部署](cluster-deployment.md)完成迁移和排空。

## 注册 storage

在 control 执行：

```sh
mokyu cli cluster create storage-1 storage
```

将返回的节点身份保存到 storage 的 `/data/node.json`，权限设为 `0600`。使用仓库根目录的 `config.storage.example.toml`，填写 `control_url`、`storage_url` 和 `storage_group`，按传输方式提供证书或代理配置，然后运行 `mokyu storage`。身份输出含通信凭据，不能公开分享。

`storage_url` 是其他节点能独立访问的 HTTPS origin，不含用户名、密码、路径、查询或片段。每次重启重新宣告配置。默认专用监听为 `0.0.0.0:9003`，使用原生 TLS；证书和私钥由 `storage_tls_cert`、`storage_tls_key` 提供。客户端使用系统信任根，私有 CA 需安装到信任库，或通过运行环境的 `SSL_CERT_FILE` 提供。

`storage_transport = "proxy"` 时不配置原生证书，`listen_storage` 必须显式绑定回环或内网 IP。代理对外仍提供 HTTPS，并以 HTTP/2 连接 storage。Nginx 使用 `grpc_pass`，同时设置 `client_max_body_size 0`，由 storage 的授权范围和配额限制流式载荷。不能将代理模式的明文监听暴露到公网。证书和拓扑配置的修改均在重启后生效。本机 `mokyu cli status` 使用独立的 `listen_control`，不通过对外 TLS 端口。

`instance.region` 可省略，适用于所有角色，与 S3 桶地区独立。`storage_group` 必填，表示部署者确认的独立故障域；不同地区使用相同组名仍算同一组。`storage_purpose` 默认为 `primary`，也可配置 `backup`。已入群成员改变用途或存储分组需要先完成迁移，不能仅靠修改配置改变已有布局。

`data_limit` 限制整个 data 目录，`min_free_space` 保留目标文件系统空间。额度包含身份、群集历史、正文和临时文件；两者同时设置时共同约束可写空间。

storage 按分片协议计算内存额度，不预留 data 的压缩、区块包和响应缓存。`stream_memory_budget` 同时约束连接与读、写、控制操作，最多使用容器有效内存的一半；`cpu_jobs` 限制实际文件 I/O 和校验。等待网络收发时释放磁盘额度，传输本身仍受各自的 `storage_read_slots`、`storage_upload_slots` 和 `storage_control_slots` 限制。超出有效内存的显式配置会在启动时拒绝。

## 创建群集和后端绑定

Web 管理区的“存储群集”只在群集模式显示。也可通过 CLI 操作：

```sh
mokyu cli storage-cluster create media
mokyu cli storage-cluster list
mokyu cli storage-cluster members CLUSTER_UUID --revision 1 MEDIA_UUID_1 MEDIA_UUID_2
mokyu cli storage-cluster enable CLUSTER_UUID --revision 2 MEDIA_UUID_1 MEDIA_UUID_2
mokyu cli storage create native mokyu --cluster-id CLUSTER_UUID --init
mokyu cli storage update native --enabled true
```

命令中的修订和 UUID 取自上一步实际结果。成员可按任意合法数量加入；启用组合才检查主盘数、成员用途、状态、故障域及逐盘空间。目前只开放数据分片 1、校验分片 0、额外副本 1 的组合。两块主存储必须属于不同 `storage_group`，不能用另一块盘的剩余空间代替其中一块。

群集策略分别保存，更新必须携带当前修订；并发编辑冲突时重新加载后再核对。`storage-cluster update UUID --revision N --name NAME --policy JSON` 更新名称和策略；`state UUID MEDIA_UUID --revision N paused` 暂停成员，`available` 恢复，尚未分配的主存储可设为 `spare`。人工状态与节点连接状态分开保存。

Mokyu 后端仅绑定群集 UUID，不绑定某个 storage 地址。管理 API 返回 `cluster_id` 与 `capabilities`。不同 data 可以上传、覆盖和完成同一会话的 Part。暂存默认使用存储桶的后端，也可选择独立的 S3 或 Mokyu 后端。参见[尾块上传方式](configuration.md#multipart-暂存)。

## 写入、读取与修复

正常写入确认档可选全部落盘、原套数据与校验落盘、仅原套数据落盘。双副本没有校验片，后两档均在原份落盘并提交元数据后返回，镜像由持久任务补齐。全部落盘档等待两份独立回执。提前确认期间只有一份数据，丢失该份后不能依靠尚未完成的镜像恢复。

每个群集可另选降级确认档，默认沿用正常档。暂停或离线只影响镜像位时，允许按较弱的降级档继续写入；主数据位不可用时拒绝新写入。已确认对象从健康的主副本读取，备份存储不参与普通读取。恢复节点后，后台任务补齐欠缺的镜像。

data 使用 HTTP/2 直接访问 storage，分范围并行读取并按对象顺序交付。写入目的节点位于同一地区而 data 位于其他地区时，可由首收 storage 向同区节点转发；同区直写、跨区转发共用布局和独立回执校验。未配置地区时不据此推断网络位置。

使用 `storage-cluster repair CLUSTER_UUID FRAGMENT_UUID` 核验并修复分片。健康片保持不变；确认损坏或缺失后，从健康主副本生成新片，完整校验后切换映射。读租约、共享引用、任务和元数据恢复点仍使用旧来源时不会回收它。网络错误不会被当作磁盘损坏。

## 独立备份

在“存储群集”里将用途为 `backup` 的成员加入群集，然后设置备份份数。每份备份使用不同的 `storage_group`；同组可以有多块盘，恢复这一份时需找回持有相应文件的成员。份数默认为 0。调高份数时后台也会补齐先前已发布的载荷。限速按单次传输生效。备份盘断线或已满时，主写仍按正常或降级确认档返回，欠账保留。

展开“恢复点”可分别查看后端在各组的正文缺口、元数据缺口和发布修订。只有已启用元数据日志，且所需正文、恢复载荷与发布证据都齐全时，才有可恢复的业务水位。未启用日志的桶即使正文已备份，也需要原控制面的数据库才能识别对象。怀疑盘上文件丢失或损坏时，点击“核验备份”；后台从主存储补回能校验的内容。也可使用：

```sh
mokyu cli storage-cluster backups CLUSTER_UUID
mokyu cli storage-cluster verify-backups CLUSTER_UUID --revision N
```

原控制面丢失时，在新 control 接管所选组的介质，再用 `mokyu cli storage recovery probe connection.json --namespace STORAGE_UUID --backup-group GROUP` 预览。需要旧部署 key 时通过 `--key-stdin` 输入。导入到另一个可写后端；若选 Mokyu 后端，须绑定另一个存储群集。此方式只从选定组读取，恢复前不会把备份盘用于业务流量。

## 身份和恢复边界

每个介质有稳定的 `media.json` UUID。已发布群集清单为 `cluster.json`，不可变历史放在 `cluster/history/<群集 UUID>/<记录 UUID>.json`。记录包含修订、记录 UUID 和前序摘要。同步先持久保存 `cluster.json.part`，逐批核对并保存历史后再原子发布清单；中断重启后继续同步。缺失或损坏的身份和清单不会被当成空盘重新初始化。

control 的物理映射与 storage 文件字段分别见[数据库结构](database-schema.md#存储群集)和[原生分片格式](storage-fragments.md)。

更换新部署签发的 `node.json` 后，storage 可以向新 control 登记，无需原部署 key。现存清单和历史保留并标为待核对，不会自动覆盖，也不能直接加入新的布局。停止旧群集后，在新 control 使用 `storage-cluster takeover NAME MEDIA_UUID...` 核对历史并接管所选介质；介质 UUID 与已存载荷保持不变。一个目录只能有一个进程持有运行锁，control 也限制同一身份的有效会话与介质绑定。

只有启用元数据日志的桶能从 `recovery/` 恢复业务目录。control 遍历恢复载荷、重建物理映射后使用现有后端恢复入口预览和导入；需要解锁时仅由 control 读取上层密钥，storage 不接触部署 key。恢复依赖与发布证据必须齐全，缺少正文、日志或快照块时报告缺口。异步尚未导出的变更不属于恢复点，关闭日志的桶不会被猜测还原。导入桶默认私有，不恢复原账号、权限、API key 或运行配置。

当前每群集最多 256 个成员，管理目录最多 256 个群集、1024 个已登记介质。历史按批次同步，每批最多 16 条、256 KiB，单清单最多 128 KiB。这些边界用于限制控制面和注册消息大小，不表示相应布局已完成性能验收。
