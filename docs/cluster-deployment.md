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

## 存储限制与迁移

群集模式禁用 local 存储。单容器模式切换到群集模式时，Mokyu 会检查存储和运行状态，不会替部署管理员上传文件：

1. 手动把 local 目录中的规范 S3 文件结构迁移到目标 S3 前缀或供应商。
2. 在 Web 或 CLI 修改 storage 的连接信息。保存时核对远端 `meta.json`；文件缺失或内容不匹配时拒绝保存。
3. 确认所有 local storage 已改为 S3 后再执行 `cluster mode cluster`。

桶绑定的 `storage_id` 永久不变；同一 storage 的桶共享去重，不跨 storage 去重。跨 storage 的对象迁移由 Mokyu 异步任务负责复制、权限、校验和发布一致性；部署管理员负责目标容量、凭据、网络和供应商策略。

## 备份、升级和故障边界

PostgreSQL 数据库、WAL、部署密钥、每个容器的 data 目录（尤其是 `node.json`）和存储区块仍按[部署与恢复](deployment-and-recovery.md)的要求备份。

群集部署只改变进程职责，不改变 S3 兼容接口、存储格式或 PostgreSQL 的恢复责任。S3 服务、DNS 健康检查、TLS 终止和 PostgreSQL 主备切换由部署环境提供和维护。

正文读取在同一事务中固定对象版本和创建读取租约。续约间隔为租约 TTL 的三分之一，TTL 为读取空闲超时与 30 秒中较大值的两倍；续约失败或租约已经过期时停止取源。续约次数随读取时间增长，不随区块数量增长。HEAD/304 不获取来源租约；单容器继续使用本机 pin，不写群集租约。
