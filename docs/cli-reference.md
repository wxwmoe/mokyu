# CLI 参考（0.0.2）

`docker exec media-gateway cli status` 返回运行计数及后台容量快照，与管理端 `GET /api/status` 使用相同字段；详见[管理 API 的状态字段](manage-api-reference.md#具体返回结构)。运行计数在重启后归零，容量快照异步采集，读取命令不会触发全表汇总。

默认配置 `/config/config.toml`，从中读取 socket 路径

可用 `docker exec media-gateway cli --socket /path/admin.sock ...` 覆写

需与容器 UID10001 或 root 相同权限访问 0600 socket，所有在线状态操作都通过常驻服务的 Unix socket 操作

成功向 stdout 输出 JSON，退出 0；执行/连接失败向 stderr 输出错误，非零退出（一般为 1），参数错误为 2。
没有成功 JSON 不能推断操作成功；任务创建成功仅表示已排队。管理帧最多 1 MiB，RPC 等待 60 秒。

## 完整命令树

| 命令（省略统一前缀） | 参数 / 默认 | 作用和返回 |
| --- | --- | --- |
| `status` | 无 | 版本、资源预算、进程内存、local_bytes、维护/GC标志、连接池、活跃流、HTTP/缓存/后端计数和容量快照 |
| `bucket list` | 无 | 桶数组：id/name/state/cors/website_enabled/index_document/error_document/created_at |
| `bucket create NAME` | NAME 必填 | 创建空逻辑桶；返回桶信息 |
| `bucket delete NAME` | NAME 必填 | 删除空桶；仍有对象、上传或待清理版本则拒绝；返回 deleted UUID |
| `bucket cors NAME FILE` | JSON文件必填 | 替换桶 CORS；`[]` 清空，返回实际规则 |
| `bucket purge NAME` | 默认预览 | 统计对象/活跃上传及桶 UUID，不执行删除 |
| `bucket purge NAME --execute` | 必须真实终端 | 二次确认后封桶、排队遍历删除，返回 task_id |
| `credential list` | 无 | access_key/enabled 数组，不返回 secret |
| `credential create BUCKET [--read-only]` | 默认读写 | 生成 access_key/secret_key，自动授权此桶；secret 只在此次返回 |
| `credential grant ACCESS_KEY BUCKET [--read-only]` | 默认读写 | 添加/替换该桶授权，返回 granted |
| `credential revoke ACCESS_KEY BUCKET` | 两参数必填 | 撤销桶授权，返回 revoked |
| `credential disable ACCESS_KEY` | 必填 | 禁用凭据，所有桶授权不可再用于认证 |
| `domain list` | 无 | 公共 Host 与逻辑桶名称映射 |
| `domain set HOST BUCKET` | 必填 | 新增/替换域名映射；HOST 转小写，可含非标准端口 |
| `domain delete HOST` | 必填 | 删除公共域名映射 |
| `user list` | 无 | Web 用户 id/username/enabled，不返回密码哈希 |
| `user create USERNAME [--password-stdin]` | 默认终端隐藏输入 | 创建 Web 管理员，无默认账户；密码12～1024字节 |
| `user password USERNAME [--password-stdin]` | 同上 | 更新密码并撤销用户全部会话 |
| `user disable USERNAME` | 必填 | 禁用用户并撤销全部会话 |
| `user delete USERNAME` | 必填 | 删除用户，数据库级联删除会话 |
| `gc status` | 无 | paused/maintenance/running、各状态区块数、宽限和间隔 |
| `gc run` | 无 | 执行一批本地清理与远端回收，返回计数；遵守暂停、维护和宽限 |
| `gc pause` | 无 | 持久化暂停后续远端 GC；已发出的单次请求可能完成 |
| `gc resume` | 无 | 解除暂停；需要立即一批可接着 gc run |
| `cleanup status` | 无 | 历史保留策略、是否运行、上轮起止时间/耗时/各表删除数/时限及错误状态 |
| `cleanup run` | 无 | 立即执行一轮数据库历史清理，遵守保留期、引用保护、批量和时间预算；已有清理运行时返回当前状态 |
| `maintenance enable` | 无 | 持久化禁止新写入与远端GC，返回 active_operations；等待已接纳操作收敛 |
| `maintenance disable` | 无 | 恢复写入；正在排队/运行的破坏性 sweep 必须先暂停/完成 |
| `task list` | 无 | 最近100个任务 |
| `task show UUID` | 必填 | 任务完整状态、游标、计数、detail/error |
| `task pause UUID` | queued/running | 当前批次可能结束，然后暂停 |
| `task resume UUID` | paused/failed | 从持久进度重排队，清除旧错误；破坏性 sweep 需维护模式，purge 需退出维护模式，只读 sweep 预览可在维护模式下继续 |
| `integrity check [--mode metadata/head/full] [--bucket NAME] [--key KEY]` | 默认 metadata；key 需 bucket | 创建只读巡检任务并返回 task_id；同一时间最多一个排队/运行中的巡检 |
| `integrity issues UUID [--after ID] [--limit N]` | 默认 after=0、limit=100；N为1～200 | 分页返回异常；next_after 为下一页游标，ID 使用十进制字符串 |
| `backend sweep [--older-than 48h]` | 默认仅预览 | 扫描部署 chunks 前缀，统计未被数据库任何状态索引的区块 |
| `backend sweep --execute --preview UUID [--older-than 48h]` | 终端、已完成预览、维护状态 | 使用相同前缀及年龄阈值重新扫描，每次删除前重查DB |

`--help` 可用于每层命令

## 初始化示例

```sh
docker exec media-gateway cli bucket create media
docker exec media-gateway cli credential create media
docker exec media-gateway cli domain set media.example.com media
docker exec -it media-gateway cli user create admin
docker exec media-gateway cli status
```

创建凭据的输出含秘密，保存到应用的私有配置，不要贴到公开日志

命令参数不接收明文密码，使用 `--password-stdin` 从 stdin 读密码，例如：

```sh
docker exec -i media-gateway cli user create admin --password-stdin < /secure/password-file
```

CORS文件示例：

```json
[{"origins":["https://app.example.com"],"methods":["GET","HEAD"],"headers":["range"],"expose":["ETag","Content-Range"],"max_age":3600}]
```

```sh
docker exec media-gateway cli bucket cors media /config/cors.json
```

规则最多100条，methods 支持 GET / HEAD / PUT / POST / DELETE / OPTIONS，origin / header 可用 `*`

## 危险操作与任务

```sh
docker exec media-gateway cli bucket purge media
docker exec -it media-gateway cli bucket purge media --execute
```

CLI 显示预览，要求输入**精确桶名**，再输入 **DELETE**

共享区块将在失去最后引用并满足全局 GC 宽限后回收，命令返回时后端空间不会立即释放

```sh
docker exec media-gateway cli maintenance enable
docker exec media-gateway cli backend sweep
docker exec media-gateway cli task show PREVIEW_UUID
docker exec -it media-gateway cli backend sweep --execute --preview PREVIEW_UUID
```

sweep 仅供维护使用，预览和执行必须相同 `--older-than`

执行前需已进入维护模式、完成预览、排空活跃操作、非空区块索引

## 完整性巡检

```sh
docker exec media-gateway cli integrity check --mode metadata
docker exec media-gateway cli integrity check --mode head --bucket media
docker exec media-gateway cli integrity check --mode full --bucket media --key path/file.mp4
docker exec media-gateway cli task show TASK_UUID
docker exec media-gateway cli integrity issues TASK_UUID
docker exec media-gateway cli task pause TASK_UUID
docker exec media-gateway cli task resume TASK_UUID
```

metadata 检查已发布对象的映射和区块元数据；head 额外检查后端存在性与编码长度；full 直接下载区块并校验认证、解压、原始长度及 BLAKE3。共享物理块一轮只检查一次。远端检查绕过缓存，也不填充缓存；任何模式均不修复或删除数据。维护模式允许巡检。

`completed` 表示已完成扫描，须同时查看 `detail.issues`；网络、权限及超时等执行故障使任务 `failed`，可从提交进度继续。暂停允许当前批次完成，重启自动恢复此前运行的任务；paused 保持暂停。CLI 返回英文协议字段，Web 提供双语说明、关联对象和完成报告导出。覆盖范围与返回结构见[管理 API](manage-api-reference.md#完整性巡检)。

## 离线入口

`docker run --rm wxwmoe/media-gateway:0.0.2 keygen` 生成 32 字节随机密钥，使用 64 位十六进制表示
`media-gateway serve --maintenance` 在监听前设置维护状态，用于数据库恢复
