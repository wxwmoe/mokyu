# 群集部署

Mokyu 的群集模式让多个进程共享同一个 PostgreSQL 数据库和部署密钥。进程仍使用同一个镜像，通过启动命令选择职责；配置文件只保存数据库、资源限制、监听地址和部署密钥等必须在进程启动前可用的内容。存储目标、账户、节点注册信息和其他运行设置保存在数据库中。

群集模式不包含 PostgreSQL 故障转移。需要高可用数据库时，由部署管理员提供带有稳定连接地址的 PostgreSQL 自身群集；所有 Mokyu 进程直接使用配置文件中的数据库地址。Mokyu 只负责节点注册、任务租约和控制接口主备切换。

## 拓扑

下面的拓扑对应三台机器。控制节点和 worker 可以在同一台机器上，也可以分开；data 节点由部署管理员自行安排宿主机。

```text
                         admin.example.com
                       A -> control-a / control-b
                                  |
                    +-------------+-------------+
                    |                           |
             PostgreSQL HA endpoint       active control
                    |                           |
          +---------+---------+          standby forwards API
          |                   |
     worker-a             data-b, data-c
     (tasks)          (S3 + public read)
          |                   |
          +------ PostgreSQL -+

       s3.example.com A -> data-b, data-c
       media.example.com A -> data-b, data-c
```

典型安排如下：

| 机器 | 容器 | 监听和职责 |
| --- | --- | --- |
| A | `mokyu control` | 只提供管理 API、管理页面和本地 CLI socket；不提供 S3 或公共读 |
| A | `mokyu worker` | 领取并执行后台任务；不提供 HTTP 业务入口 |
| B/C | `mokyu data` | 提供 S3 和公共读；处理数据面请求并直接连接 PostgreSQL |

每个容器使用自己的配置文件和 `storage.data` 目录，不共享本地目录。所有容器必须使用同一个不可替换的 `encryption.deployment_key`，并连接同一个数据库。data 节点和 worker 仍需要数据库地址；不会从 control 读取数据库连接信息。

启动命令是镜像的唯一角色开关：

```sh
mokyu                 # 未指定命令，单容器模式
mokyu control         # 管理/control
mokyu worker          # 后台任务/worker
mokyu data            # S3 和公共读/data
mokyu cli ...         # 通过本地 Unix socket 执行管理命令
```

Linux 的 `htop` 会读取进程的 comm 名称，启动后应显示 `mokyu: single`、`mokyu: control`、`mokyu: worker` 或 `mokyu: data`。完整命令行仍可在详细视图中看到。

## 网络和 DNS

Mokyu 不判断 URL 是内网、远程、HTTP 还是 HTTPS，只要求配置值是绝对 URL。示例使用 HTTPS，是因为 TLS、证书和反向代理应由部署环境提供。

数据面可以直接使用多 IP A 记录：

```text
s3.example.com    A  <machine-b-ip>
s3.example.com    A  <machine-c-ip>
media.example.com A  <machine-b-ip>
media.example.com A  <machine-c-ip>
```

每台机器的 Nginx 或其他反向代理把这些域名分别转发到本机 data 容器的 S3/公共读端口。DNS 轮询不提供健康检查；客户端应按 S3 重试规则处理节点不可用。

控制面也可以使用多 IP A 记录，让浏览器和新节点随机命中任意 control：

```text
admin.example.com A <control-a-ip>
admin.example.com A <control-b-ip>
```

每个 control 的 `cluster.advertise_url` 必须是只指向该节点的可达 URL，例如 `https://control-a.example.com` 和 `https://control-b.example.com`。不要把所有 control 的 `advertise_url` 都写成同一个会随机解析的地址，否则 standby 转发可能再次落回 standby。已注册节点的 `cluster.control_url` 可以写共享的多 IP 管理地址；请求命中 standby 时，standby 会按租约转发到 active control。首次加入一个新的 control 时，建议临时把 `control_url` 写成可直达 active 的 URL，完成注册后再改为共享地址。

反向代理只需要把管理域名转发到 `listen.manage`，把数据域名转发到 `listen.s3` 或 `listen.web`。control、worker 和 data 之间的注册请求走 `cluster.control_url`；data 的对象读写不会经过 control。

## 首次安装

1. 准备 PostgreSQL 数据库、每个容器独立的配置和 data 目录，生成一次部署密钥，并把同一字符串写入所有配置的 `encryption.deployment_key`。
2. 用默认命令启动一个临时单容器实例，让它创建空数据库结构，然后执行：

   ```sh
   docker exec mokyu cli cluster mode cluster
   ```

   该命令会检查数据库中不能存在 local 存储目标。已有 local 目标时，先按[存储迁移](#存储限制与迁移)完成手动迁移。

3. 停止单容器，使用第一台 control 的配置启动：

   ```sh
   mokyu control
   ```

   第一台 control 不需要 `cluster.control_url` 或 `cluster.join_token`，但必须设置 `cluster.advertise_url`；未设置 `cluster.node_name` 时使用 `control-1`。它会在数据库注册自己并取得 control lease。首次管理员创建和存储目标配置都在 control 的管理 API、Web 或 CLI 上完成。
4. 在 control 上创建至少一个 S3 存储目标，再创建桶。群集模式拒绝 local 存储目标；`storage.data` 仍可用于 multipart、缓存和节点状态，它不是存储目标。

## 加入 control、worker 和 data

先在 active control 上为每个容器生成一次性令牌。令牌绑定节点名称和角色，只在输出中显示一次：

```sh
docker exec mokyu-control cli cluster token control-b control \
  --advertise-url https://control-b.example.com --expires-in 1h
docker exec mokyu-control cli cluster token worker-a worker --expires-in 1h
docker exec mokyu-control cli cluster token data-b data --expires-in 1h
docker exec mokyu-control cli cluster token data-c data --expires-in 1h
```

把对应输出中的 `token` 保存到新容器的配置文件：

```toml
[cluster]
control_url = "https://admin.example.com"
join_token = "<one-time-token>"
```

新容器第一次启动会调用 `/api/cluster/join`，得到节点凭据并写入自己的 `storage.data/node.json`。后续重启使用该凭据 heartbeat，不会重复消耗令牌；确认节点已加入后可以从配置文件删除 `join_token`。control token 还可以在配置中写 `advertise_url` 覆盖注册时的 URL，worker/data 不需要该字段。data/worker 可以直接使用 shared `control_url`，命中已注册 standby 时由 standby 转发；新的 control 首次 join 仍应确保该 URL 能到达 active。

节点管理命令：

```sh
docker exec mokyu-control cli cluster status
docker exec mokyu-control cli cluster nodes
docker exec mokyu-control cli cluster revoke <node-id>
```

撤销会立即禁止节点 heartbeat 和任务领取；重新加入需要生成新的节点令牌。数据库中的节点注册状态是控制面事实来源，配置文件不需要互相校验或保持相同文件内容。

## 控制主备和请求转发

每个 control 每 10 秒续租 PostgreSQL 中的 control lease，租期为 30 秒。持有租约的 control 执行管理 API；standby 收到 `/api/` 请求时会把请求原样转发到当前 active 的 `advertise_url`，包括尚未注册节点的 join/heartbeat 请求。没有有效租约时，control 返回 `503 ControlUnavailable`，等待某个已注册 control 取得租约。

这个机制只解决 Mokyu control 的主备和管理请求转发：

- PostgreSQL 连接中断时，所有依赖数据库的节点都会停止或重试，不会由 Mokyu 代替 PostgreSQL 做故障转移。
- control 不代理 S3、公共读或 data 节点之间的数据流量。
- worker 任务通过数据库租约领取，同一任务只由一个节点处理；可以增加 worker 数量。
- control、worker、data 都应配置同一个 PostgreSQL 稳定地址和部署密钥。

控制 API 的多 IP DNS 只提供入口冗余；真正的 active 选择由数据库 lease 决定。故障切换期间可能有短暂的 503，客户端应重试管理请求。

## 存储限制与迁移

群集模式禁用 local 存储目标。single 转 cluster 时，Mokyu 只检查是否仍存在 local 目标，不会替部署管理员上传文件：

1. 手动把 local 目录中的规范 S3 文件结构迁移到目标 S3 前缀或供应商。
2. 在 Web/CLI 修改该 storage target 的连接信息，保存时让 Mokyu 核对远端 meta；空 meta 会拒绝保存，避免把没有对应区块的目标接入。
3. 确认所有 local target 已改为 S3 后再执行 `cluster mode cluster`。

桶绑定的 `storage_target_id` 永久不变；同一 storage target 的桶共享去重，不跨 target 去重。storage target 自身可以更换连接信息，但实际文件迁移由部署管理员负责。已绑定不同 target 的 Copy/Move 会要求二次确认并排入 `storage_migration` 后台任务，Move 完成后才退休源对象。

## 返回单机

停止或撤销全部已注册节点，确认 `cluster nodes` 中没有 enabled 节点后，在 control CLI 执行：

```sh
docker exec mokyu-control cli cluster mode single
```

然后停止 split roles，使用默认命令启动单容器。切回 single 不会自动把 S3 文件下载成本地；如需 local target，先由管理员完成反向迁移并在 Web/CLI 更新 target 连接信息。

## 备份、升级和故障边界

PostgreSQL 数据库、WAL、部署密钥、每个容器的 data 目录和后端区块仍按[部署与恢复](deployment-and-recovery.md)的要求备份。0.0.4 不支持 0.0.3 原地升级；应使用全新数据库和目录，再通过 S3 接口或手动存储迁移导入数据。

群集部署只改变进程职责，不改变 S3 兼容接口、存储格式或 PostgreSQL 的恢复责任。真实云 S3、DNS 健康检查、TLS 终止和 PostgreSQL 主备切换均由部署环境验证和维护。
