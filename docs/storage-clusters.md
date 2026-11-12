# 存储群集

`mokyu storage` 是由 control 登记的独立存储角色，仅限 `single_control` 和 `cluster`。storage 使用节点凭据注册，不连接元数据库，不配置部署 key。地区、地址和存储分组由 storage 的配置文件宣告；群集策略和成员归属保存在 control 的数据库中。

当前支持身份登记、群集策略、成员管理和后端绑定。启用组合后显示“等待存储验证”，后端保持停用；共享正文读写和跨节点暂存尚未开放。不能把此状态当成可用容量，也不能解除维护模式承接数据。

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

`storage_transport = "proxy"` 时不配置原生证书，`listen_storage` 必须显式绑定回环或内网 IP。代理对外仍提供 HTTPS，并以 HTTP/2 连接 storage。不能将代理模式的明文监听暴露到公网。证书和拓扑配置的修改均在重启后生效。本机 `mokyu cli status` 使用独立的 `listen_control`，不通过对外 TLS 端口。

`instance.region` 可省略，适用于所有角色，与 S3 桶地区独立。`storage_group` 必填，表示部署者确认的独立故障域；不同地区使用相同组名仍算同一组。`storage_purpose` 默认为 `primary`，也可配置 `backup`。已入群成员改变用途或存储分组需要先完成迁移，不能仅靠修改配置改变已有布局。

`data_limit` 限制整个 data 目录，`min_free_space` 保留目标文件系统空间。额度包含身份、群集历史、正文和临时文件；两者同时设置时共同约束可写空间。

## 创建群集和后端绑定

Web 管理区的“存储群集”只在群集模式显示。也可通过 CLI 操作：

```sh
mokyu cli storage-cluster create media
mokyu cli storage-cluster list
mokyu cli storage-cluster members CLUSTER_UUID --revision 1 MEDIA_UUID_1 MEDIA_UUID_2
mokyu cli storage-cluster enable CLUSTER_UUID --revision 2 MEDIA_UUID_1 MEDIA_UUID_2
mokyu cli storage create native mokyu --cluster-id CLUSTER_UUID --init
```

命令中的修订和 UUID 取自上一步实际结果。成员可按任意合法数量加入；启用组合才检查主盘数、成员用途、状态、故障域及逐盘空间。目前只开放数据分片 1、校验分片 0、额外副本 1 的组合。两块主存储必须属于不同 `storage_group`，不能用另一块盘的剩余空间代替其中一块。

群集策略分别保存，更新必须携带当前修订；并发编辑冲突时重新加载后再核对。`storage-cluster update UUID --revision N --name NAME --policy JSON` 更新名称和策略；`state UUID MEDIA_UUID --revision N paused` 暂停成员，`available` 恢复，尚未分配的主存储可设为 `spare`。人工状态与节点连接状态分开保存。

Mokyu 后端仅绑定群集 UUID，不绑定某个 storage 地址。管理 API 返回 `cluster_id` 与 `capabilities`，当前原生后端能力均为关闭，不能启用或用于桶。空部署在维护状态可以预选原生后端作为 `uploads.staging_storage_id`，通过能力验证前不能退出维护。

## 身份和恢复边界

每个介质有稳定的 `media.json` UUID。已发布群集清单为 `cluster.json`，不可变历史放在 `cluster/history/<群集 UUID>/<修订>`。同步先持久保存 `cluster.json.part`，逐批核对并保存历史后再原子发布清单；中断重启后继续同步。缺失或损坏的身份和清单不会被当成空盘重新初始化。

更换新部署签发的 `node.json` 后，storage 可以向新 control 登记，无需原部署 key。现存清单和历史保留并标为待核对，不会自动覆盖，也不能直接加入新的布局。完整接管和业务恢复尚未开放。一个目录只能有一个进程持有运行锁，control 也限制同一身份的有效会话与介质绑定。

当前每群集最多 256 个成员，管理目录最多 256 个群集、1024 个已登记介质。历史按批次同步，每批最多 16 条、256 KiB，单清单最多 128 KiB。这些边界用于限制控制面和注册消息大小，不表示相应布局已完成性能验收。
