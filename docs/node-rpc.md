# 节点 RPC

control 在 `instance.listen_control` 同一端口提供管理 HTTP 和 gRPC。协议定义为 [`proto/node.proto`](../proto/node.proto)，包名 `mokyu.node.v1`；节点 RPC 不经过管理 API 的 Cookie、JSON 或响应压缩中间件。反向代理必须保留 gRPC 路径、HTTP/2、请求元数据和响应 trailers，并禁止缓存。

## 身份与限制

节点凭据由管理员的 `cluster create` / `cluster rotate` 取得，见 [CLI](cli-reference.md)。每次 RPC 都携带下列 metadata，由 control 验证当前部署、启用状态、节点角色、凭据代次和凭据。

| Metadata | 值 |
| --- | --- |
| `x-mokyu-deployment-id` | 部署 UUID |
| `x-mokyu-node-id` | 节点 UUID |
| `x-mokyu-node-role` | `data` 或 `worker` |
| `x-mokyu-credential-generation` | 正整数凭据代次 |
| `authorization` | `Bearer <credential>` |

拒绝重复身份字段；全部 metadata 键和值合计最多 4 KiB。Unary 响应最多 4 MiB，SyncNode 请求最多 1 KiB，NodeSession 消息最多 64 KiB；在解码之前检查身份格式和独立并发额度，额度持续到响应结束或取消。开始请求的服务端期限为 15 秒。节点凭据不授予管理 API 权限。

## 方法

| 方法 | 请求与响应 |
| --- | --- |
| `NodeControl/SyncNode` | Unary；提交当前 `run` 和最多五个 `cached` 配置修订，返回运行状态、全部当前修订及未命中配置 |
| `NodeSessions/NodeSession` | 双向流；首条必须为 Hello，收到 Welcome 后发送 Pulse 并等待匹配序号的确认 |

配置分区为 `requests`、`uploads`、`metrics`、`maintenance`、`io`，不含管理 `web` 设置。`cached` 只表示已持有的配置修订，不替代任何身份或租约检查；缺失或修订不符时，在同一 SyncNode 响应内补全。Settings 的 JSON 字节使用对应运行设置的现有结构。

## 会话和重连

每次进程启动生成新的 `boot_id`，一次待确认接入使用固定 `connection_id`。Hello 包含协议号 1、boot ID、connection ID 和最后获知的 `previous_epoch`；初次为 0。Welcome 返回 `run={boot_id,fence}`、会话 epoch、剩余有效毫秒数和配置修订。

会话有效期为 90 秒，数据库时钟决定到期。Pulse 的正整数 sequence 必须严格递增，确认后续期；流上 30 秒没有新 Pulse 时关闭连接。`draining` 一旦设为 true，同一 boot 不可撤销。客户端用请求发出时的单调时钟扣除传输耗时及 2 秒余量，不能把到期时间直接换成本机墙上时钟。

每次成功接入都会推进 epoch，旧流不能继续心跳。相同 boot 在有效期内重连保留业务 fence；过期重连会更换 fence。Welcome 丢失时，用相同 connection ID 重试可接入其他 control；获知新 epoch 后再创建下一次接入。新的 boot 须等待前一个 boot 过期，已退役的 boot 不能再次成为当前实例。每代凭据的 boot 历史达到 4096 项时，由管理员轮换凭据。

会话存在于共享 PostgreSQL 中，各 control 使用相同规则接管。流消息不构成业务提交记录。SyncNode 不续期会话，凭据轮换或节点禁用立即阻止后续请求。

## 地址选择

客户端要求 HTTPS 域名 origin，不接受 IP、用户信息、路径或 query。解析完整 A 集合（最多 32 个），按 TTL 刷新，刷新间隔限制在 1～300 秒；DNS 失败时仅在上一次集合到期后的 300 秒内继续使用已知地址。

Unary 在可用地址间轮询，每个地址复用自己的 Channel；失败地址按退避暂停选择。NodeSession 使用独立 TCP/TLS 连接。连接实际 IP 时，仍使用原域名做 TLS SNI、证书验证与 HTTP authority。连接期限 3 秒，每次尝试最多 5 秒，整个操作最多 15 秒和 3 个地址尝试；只有 `Unavailable` / `DeadlineExceeded` 进入传输重试。

| gRPC 状态 | 处理 |
| --- | --- |
| `Unauthenticated` | 身份或凭据无效，停止使用并由管理员恢复 |
| `PermissionDenied` | 节点被禁用、角色不适用或部署不在群集模式 |
| `Aborted` | 其他 boot/会话已接管，或心跳序号过旧 |
| `FailedPrecondition` | 协议不支持、运行租约到期或 boot 已退役 |
| `ResourceExhausted` | 元数据、并发或 boot 历史超限 |
| `Unavailable` / `DeadlineExceeded` | 在有限期限和退避内尝试其他地址 |

消息长度超限也可能由 gRPC 解码器返回 `OutOfRange`。业务身份和状态错误不应触发地址轮换重试。

代理返回的 HTTP 429、502、503、504 转为 `Unavailable`，在解码正文前丢弃 HTML 错误页；HTTP 401/403 保留认证/权限失败语义，不轮换重试。
