# CLI 参考

在线命令通过常驻服务执行，统一前缀为 `docker exec mokyu mokyu cli`。命令和输出使用英语；每层命令均支持 `--help`，下表的 `[]` 表示可选参数。

默认读取 `/config/config.toml`，可用 `mokyu -c PATH cli ...` 或 `mokyu --config PATH cli ...` 指定配置。

成功向 stdout 输出 JSON、退出 0；执行或连接失败向 stderr 输出错误并非零退出，参数错误退出 2。管理帧最多 1 MiB，RPC 等待 60 秒。任务创建成功表示已排队，后续结果通过 `task show` 查看。

离线命令 `mokyu tool openapi` 输出管理 API 的 OpenAPI JSON，不访问配置、数据库或服务；`mokyu tool keygen` 生成部署 key。

## 首次初始化

| 命令 | 作用与返回 |
| --- | --- |
| `init USERNAME [--stdin] [--require-change]` | 在空数据库中创建首个管理员；终端内隐藏输入密码，`--stdin` 从标准输入读取一行 |
| `setup token` | 生成一次性 Web 首次管理员 token |

`init` 只允许在本机单容器或 control 管理端执行；数据库已有管理员时拒绝重复初始化。

## 桶与域名

| 命令 | 作用与返回 |
| --- | --- |
| `bucket list` | 返回[桶信息](control-api-reference.md#存储桶)数组 |
| `bucket create NAME [--project UUID]` | 创建空逻辑桶，默认内置项目，返回桶信息 |
| `bucket delete NAME` | 删除空桶，返回 deleted UUID；仍有对象、上传或待清理版本时拒绝 |
| `bucket cors NAME FILE` | 用 JSON 文件替换 CORS，返回实际规则；`[]` 关闭 |
| `bucket purge NAME [--execute]` | 默认预览对象、活跃上传和桶 UUID；确认执行后封桶并返回 task_id |
| `domain list` | 列出公共 Host 与桶名映射 |
| `domain set HOST BUCKET` | 绑定域名；Host 转小写，可含端口；拒绝占用其他桶的映射 |
| `domain delete HOST` | 删除域名映射 |

CORS 文件示例，字段规则见[CORS 设置](control-api-reference.md#cors-设置)：

```json
[{"origins":["https://app.example.com"],"methods":["GET","HEAD"],"headers":["range"],"expose":["ETag","Content-Range"],"max_age":3600}]
```

```sh
docker exec mokyu mokyu cli bucket cors media /config/cors.json
```

## 存储

| 命令 | 作用与返回 |
| --- | --- |
| `storage list` | 列出存储名称、类型、连接范围、当前区块 key、三项存储并发上限、启用状态和更新时间；不返回凭据 |
| `storage show NAME` | 显示一个存储的当前配置，不执行连接检查 |
| `storage create NAME local --init [--key KEY] [--path PATH] [--prefix PREFIX]` | 明确初始化空的本地存储；省略 `--path` 使用 `/data/storage`，返回存储信息 |
| `storage create NAME s3 --init [--key KEY] -e URL -b NAME -r REGION -a KEY -s SECRET [-p PREFIX]` | 明确初始化空的 S3 存储并写入 `meta.json`，返回存储信息 |
| `storage update NAME [--type local\|s3] [--key KEY] [--path PATH] [-e URL] [-b NAME] [-r REGION] [-p PREFIX] [-a KEY] [-s SECRET] [--read N\|--clear-read-concurrency] [--upload N\|--clear-upload-concurrency] [--control N\|--clear-control-concurrency] [--enabled true\|false]` | 修改存储连接信息、当前 block key、并发上限或启停；保存前核对已有 `meta.json`，不返回凭据 |
| `storage check NAME` | 读取并核对存储的 `meta.json`，返回检查结果 |
| `storage key set NAME KEY` | 将存储绑定到未撤销的 block key 名称或 UUID |
| `storage key clear NAME` | 解除存储的当前 block key，新写入区块不加密 |
| `storage profile show NAME` | 显示该 storage 的类型化连接、缓存、压缩、打包、巡检和群集共享预算 |
| `storage profile set NAME FILE` | 用完整 JSON 替换 storage profile；`FILE` 为 `-` 时从 stdin 读取，要求当前 `revision` |
| `storage rewrite NAME preview [--key KEY]` | 统计需要按目标 key 或无加密重写的物理区块和区块包，返回确认值 |
| `storage rewrite NAME start --confirm VALUE [--key KEY]` | 使用 preview 返回的确认值创建低优先级重写任务 |
| `storage rewrite NAME status` | 查看该存储最近的重写任务 |
| `storage rewrite NAME pause\|resume\|retry` | 暂停、继续或重试该存储最近的重写任务 |

存储名称为 1～128 字节且不能含 `/` 或控制字符；S3 的 endpoint 必须是没有查询串或片段的 HTTP(S) URL。`local` 只接受 path，`s3` 需要 endpoint、bucket、region、access key 和 secret key，并具备对应范围的 Get、Put、Delete、List 和条件创建权限；`prefix` 默认为空。存储创建前，目标命名空间必须为空并会写入身份标识；更新连接信息只接受与已有标识匹配的目标，不会初始化空目标。存储由数据库管理，创建桶时用 `bucket create NAME --storage UUID` 绑定；绑定后不可更改。

单容器模式下可以使用本地存储；群集模式必须使用 S3 存储。创建桶前至少要有一个启用的存储，存在多个时必须指定 `--storage`。相关 HTTP 接口见[存储](control-api-reference.md#存储)。

## 存储额度

| 命令 | 作用 |
| --- | --- |
| `quota get SCOPE ID` | 读取 project/bucket 额度及用量 |
| `quota set SCOPE ID FILE` | 用 JSON 完整替换上限，写入审计 |

文件示例：`{"logical_bytes_limit":"10737418240","inflight_bytes_limit":"2147483648","bucket_count_limit":"10"}`。null 不限，`"0"` 为零；bucket 仅设置 logical_bytes_limit，其余字段为 null。调低额度不会删除现有文件。

## 活动审计

`audit [--after ID] [--actor NAME] [--action PREFIX]` 返回 `{events,next}`，每页最多 50 条，不含密码或 secret。管理写命令以 Local CLI 记入同一审计记录。

## 项目

| 命令 | 作用 |
| --- | --- |
| `project list` | 列出项目 |
| `project create NAME` | 创建项目并开启项目管理 |
| `project update ID NAME [--description TEXT] [--allow-bucket-create]` | 更新项目设置 |
| `project delete ID` | 删除无桶、凭据和成员的非内置项目 |
| `project mode true/false` | 设置项目管理开关；关闭前需清理依赖 |

## 凭据与用户

| 命令 | 作用与返回 |
| --- | --- |
| `cred list [--after ACCESS_KEY]` | 返回最多 100 条元数据与授权，不含 secret；after 使用上页末项 access_key |
| `cred create BUCKET [--read-only] [--label TEXT] [--ttl 30d]` | 生成 access_key/secret_key 并授权该桶，默认读写；secret 仅返回一次 |
| `cred grant ACCESS_KEY BUCKET [--read-only]` | 添加或替换同项目桶授权，默认读写，返回 granted |
| `cred revoke ACCESS_KEY BUCKET` | 撤销桶授权，返回 revoked |
| `cred disable ACCESS_KEY` | 禁用凭据及其全部桶授权 |
| `cred enable ACCESS_KEY` | 重新启用密钥，保留到期时间 |
| `cred show ACCESS_KEY` | 查看安全元数据和授权 |
| `cred update ACCESS_KEY FILE` | JSON 设置，格式同管理 API PUT；FILE 为 `-` 时从 stdin 读取 |
| `cred permissions ACCESS_KEY FILE` | 用 JSON 数组替换完整授权；FILE 为 `-` 时从 stdin 读取 |
| `cred rotate ACCESS_KEY [--overlap 24h] [--ttl 30d]` | 创建替代密钥与重叠期，仅返回一次新 secret |
| `cred delete ACCESS_KEY` | 永久删除密钥 |
| `token list USERNAME [--after UUID]` | 返回最多 100 个 Token 安全记录 |
| `token create USERNAME FILE` | 按管理 API JSON 签发个人 Token，仅返回一次 secret |
| `token update ID FILE` | 替换自己名下 Token 的设置；CLI 可指定任意用户的 Token |
| `token revoke ID` | 永久撤销 Token |
| `user list` | 返回 id/username/enabled，不含密码哈希 |
| `user create USERNAME [--role admin/member] [--require-change] [--stdin]` | 创建账户，默认管理员；密码 12～1024 字节 |
| `user password USERNAME [--require-change] [--stdin]` | 更新密码并撤销全部会话 |
| `user enable USERNAME` | 重新允许登录 |
| `user role USERNAME admin/member` | 调整角色并退出已有会话 |
| `user membership USERNAME PROJECT_ID FILE` | 用 JSON 完整替换项目授权，格式见管理 API |
| `user leave USERNAME PROJECT_ID` | 移除项目授权 |
| `user disable USERNAME` | 禁用用户并撤销全部会话 |
| `user delete USERNAME` | 删除用户及其会话 |

最后一个启用的管理员不可被禁用、降级或删除。`--require-change` 要求下次登录先改密。`user list` 包含角色、启用状态和改密要求。

## 数据库密钥

| 命令 | 作用 |
| --- | --- |
| `key list` | 列出名称、用途、算法、指纹、存储使用状态和撤销状态，不返回密钥材料 |
| `key create NAME PURPOSE [-a aes-256-gcm\|chacha20-poly1305]` | 在数据库生成不可读的随机密钥；PURPOSE 为 access/signing/block，block 默认 AES-256-GCM |
| `key revoke UUID` | 撤销未被存储、物理编码、待上传或任务使用的密钥；历史区块仍使用数据库中的密钥材料读取 |

凭据/Token 设置文件上限 512 KiB。JSON 中的权限与有效期语义见[管理 API](control-api-reference.md#应用密钥与-api-token)。创建凭据的输出应保存到应用私有配置。密码默认在终端隐藏输入并再次确认，使用 `docker exec -it`；非终端须指定 `--stdin`。不接受明文命令参数，自动化时可从 stdin 读取一行：

```sh
docker exec -i mokyu mokyu cli user create admin --stdin < /secure/password-file
```

首次初始化流程见[部署与接入](deployment-and-recovery.md#初始化与接入)。

## 状态与维护

| 命令 | 作用与返回 |
| --- | --- |
| `status` | 返回[运行状态和容量快照](control-api-reference.md#运行状态)，读取不触发全表汇总 |
| `gc status` | 返回 paused/maintenance/running、区块状态计数、宽限、间隔和 min_storage_duration |
| `gc mark-unreferenced` | 在维护模式且活跃操作已排空时，为没有引用的 ready 区块补上回收时间标记 |
| `gc run` | 执行一批本地清理与远端回收，返回计数；遵守暂停、维护、宽限及最低存储期限 |
| `gc pause` | 持久暂停后续远端 GC，已发出的请求可能完成 |
| `gc resume` | 解除暂停；需要立即执行时再调用 `gc run` |
| `cleanup status` | 返回历史保留策略、运行状态和上轮结果 |
| `cleanup run` | 立即执行一轮历史清理，遵守保留期、引用和资源预算；已有清理运行时返回当前状态 |
| `maintenance enable` | 持久禁止新写入和远端 GC，暂停清桶、打包、拆包、存储迁移和重写任务，返回 active_operations；须等待已接纳操作排空 |
| `maintenance disable` | 恢复写入；须先暂停或完成破坏性 sweep |

`maintenance status` 返回七类定时策略、当前/最近任务、周期、下一次检查和新包生成状态。`maintenance pause TASK` / `resume TASK` 持久暂停或恢复对应策略，`maintenance run TASK` 发起单次任务；TASK 为 pack/reuse/reclaim/repack/range/gc/cleanup。暂停策略不丢失进度，当前批次可完成。

`maintenance pack-creation stop` 停止即时上传、异步上传及各维护路径发布新包；已有 preparing 包收尾后才允许全部拆包。`maintenance pack-creation resume` 恢复生成，但未完成的全部拆包任务会阻止恢复。配置 packing.enabled=false 时，运行开关不能越过配置启用。

## 控制设置

| 命令 | 作用与返回 |
| --- | --- |
| `settings get SECTION` | 显示数据库中一个 runtime settings 分组；不返回秘密 |
| `settings set SECTION FILE [--rev REVISION]` | 用 JSON 文件完整替换一个分组；省略 `--rev` 时由服务端在当前 revision 上保存，FILE 为 `-` 时从 stdin 读取 |
| `settings reset SECTION` | 将分组恢复为当前代码默认值 |
| `settings web show` | 显示 Web 设置兼容视图 |
| `settings web set [--origin URL] [--secure-cookie true/false] [--session-lifetime 12h] [--gravatar-base-url URL]` | 修改 Web 设置；未提供的字段保持不变 |

runtime settings 由所有角色共享。保存成功后响应会标记需要重启的字段；重启前继续使用旧值，不阻塞请求。

## 群集

| 命令 | 作用与返回 |
| --- | --- |
| `setup token` | 生成一次性 Web 首次管理员 setup token；只输出本次明文，保存哈希和一小时有效期 |
| `cluster status` | 返回 deployment mode、enabled 节点数和 local storage 数 |
| `cluster mode single/cluster` | 在维护模式且工作已排空时切换数据库部署模式，随后必须重启；准备步骤见[模式切换](cluster-deployment.md#模式切换和恢复) |
| `cluster nodes` | 列出节点 id、名称、角色、启用状态和最近 heartbeat |
| `cluster release NAME --confirm` | 禁用节点并将其持有的任务重新排队 |

单容器 CLI 使用部署密钥认证，不依赖 `node.json`。control、worker 和 data 首次启动时直接向 PostgreSQL 注册，凭据保存到 `instance.data_path/node.json`。节点文件丢失时进入 `recovery_required`，管理员先释放旧节点，再重启角色完成注册。控制节点不使用远程 URL 或主备转发；DNS 和角色安排见[群集部署](cluster-deployment.md)。

群集模式下，data/worker 容器的本地 CLI 只支持 `status`；其他管理命令在单容器模式或 control 容器执行。

## 后台任务

| 命令 | 作用与返回 |
| --- | --- |
| `task list` | 最近 100 个任务 |
| `task show UUID` | 完整任务状态、游标、计数、detail/error |
| `task pause UUID` | 暂停 queued/running 任务，允许当前批次结束 |
| `task resume UUID` | 从持久进度继续 paused/failed 任务，清除旧错误并刷新 storage profile 版本；保留游标和操作目标 |

重启后此前运行的任务自动继续，paused 保持暂停。维护模式下 purge/pack/unpack/upload/storage_migration/rewrite 会持久暂停，退出维护后需显式 `task resume`。completed 任务不能继续；继续破坏性 sweep 需要维护模式，purge/pack/unpack/upload/storage_migration/rewrite 需要退出维护模式，只读 sweep、巡检和显式 cache flush 可在维护模式下运行。

### 区块包维护

| 命令 | 作用 |
| --- | --- |
| `pack status` | 开关、大小上限，以及各状态的数量和字节 |
| `pack run [--mode MODE]` | MODE 为 `pack`（默认）、reuse、reclaim、range、repack；分别为创建、复用拆分、无引用成员回收、Range 回源优化、碎片合并 |
| `pack unpack ID [--execute]` | 默认预览；execute 创建将指定包拆成独立区块的任务 |
| `pack unpack --all [--execute]` | 拆除全部历史包；先 `maintenance pack-creation stop` 并等待 preparing 数量归零 |

手动维护仍遵守引用、冷却、收益、维护模式和资源限制；run 返回 task_id，可用 task 命令暂停、恢复、查看错误。拆包先写新来源再切换映射，旧包按 GC 宽限回收，临时占用会增加。关闭打包功能后 reuse/reclaim 仍可执行，但只输出独立块。

range 需要 range_enabled=true；packing.enabled=false 时可优化历史包，但不新建小包。task detail.last_range 给出候选、处理原因和预计净收益。重新合包受观测窗口及冷却约束，复用拆分不受这项冷却阻挡。

### 清空存储桶

```sh
docker exec mokyu mokyu cli bucket purge media
docker exec -it mokyu mokyu cli bucket purge media --execute
```

执行需要真实终端，依次输入精确桶名和 `DELETE`。任务遍历删除对象及上传；共享区块在失去最后引用并满足 GC 宽限后回收，返回 task_id 时空间不会立即释放。

### 存储清查

| 命令 | 作用 |
| --- | --- |
| `storage sweep [--storage UUID] [--age 48h]` | 默认预览；扫描指定存储（省略时按当前选择）chunks/packs 规范路径，统计未被数据库任何物理状态索引的载荷 |
| `storage sweep --execute --preview UUID [--age 48h]` | 按相同前缀和年龄阈值重新扫描，每次删除前重新查询数据库 |

预览和执行均按存储对象的修改时间计算年龄，取 `--age` 与该 storage profile 的 `min_storage_duration` 中较长者。最低期限改变后须重新预览；继续旧任务也不会绕过当前最低期限。

执行需要维护模式、已完成的预览、排空的活跃操作、非空区块索引及真实终端；按提示确认前缀，再输入 `DELETE`。预览过期后需重新生成。

```sh
docker exec mokyu mokyu cli maintenance enable
docker exec mokyu mokyu cli storage sweep
docker exec mokyu mokyu cli task show PREVIEW_UUID
docker exec -it mokyu mokyu cli storage sweep --execute --preview PREVIEW_UUID
```

## 完整性巡检

| 命令 | 参数与返回 |
| --- | --- |
| `integrity check [--mode MODE] [--bucket NAME] [--key KEY]` | 默认 metadata；key 需 bucket；返回 task_id，同一时间最多一个 queued/running 巡检 |
| `integrity issues UUID [--after ID] [--limit N]` | 默认 after=0、limit=100，N 为 1～200；返回 issues 和 next_after，ID 为十进制字符串 |

| 模式 | 检查内容 |
| --- | --- |
| `metadata` | 已发布对象映射、区块元数据和所需密钥 |
| `head` | metadata，加存储对象存在性及编码长度 |
| `full` | metadata，加远端下载、认证、解压、长度及 BLAKE3 校验 |

巡检只报告，不修复或删除数据；远端检查绕过且不填充缓存，共享物理块每轮只检查一次。没有远端来源的 pending 块在所有模式下校验本地唯一副本。`completed` 仍可能有异常，应查看 `detail.issues`；网络、权限及超时使任务 `failed`，可恢复后继续。覆盖边界、异常代码和 JSONL 导出见[巡检 API](control-api-reference.md#完整性巡检)，并发和限速见[配置](configuration.md#完整性巡检)。

```sh
docker exec mokyu mokyu cli integrity check --mode full --bucket media
docker exec mokyu mokyu cli task show TASK_UUID
docker exec mokyu mokyu cli integrity issues TASK_UUID
docker exec mokyu mokyu cli task pause TASK_UUID
docker exec mokyu mokyu cli task resume TASK_UUID
```

## 上传缓存

| 命令 | 返回与行为 |
| --- | --- |
| `cache status` | 配置及有效额度、待上传字节/条目/最早时间、最近错误、前 100 条 pin 归属；回退次数为本次进程累计 |
| `cache flush` | 返回 task_id，将创建时已有积压写入独立存储来源；可在维护模式显式执行，支持任务暂停/恢复 |

新请求仍可产生积压。备份排空需先阻止新写入并等待活跃请求，任务完成后确认 pending 为零。关闭上传缓存不删除已有待上传文件；恢复与备份要求见[部署说明](deployment-and-recovery.md#备份材料)。

## 离线入口

| 命令 | 作用 |
| --- | --- |
| `mokyu tool keygen` | 生成部署密钥；只输出一次，必须保存到配置文件 |
| `mokyu tool openapi` | 离线导出管理 OpenAPI，不读取配置或数据库 |
| `mokyu cli maintenance enable` | 通过管理 API 进入维护状态；恢复数据库时先停止写入，再用该命令确认维护状态 |

全部在线管理能力在 Web 有对应入口，见[管理指南](management.md)；部署密钥生成、配置文件安装、数据库备份恢复及停止状态的救援仍在部署环境执行。

`cluster nodes` 返回 `{items,next_cursor}`，默认 50 项，可用 `--limit`（1–200）、`--after`、`--name`、`--role`、`--contact`、`--work`、`--configuration`、`--section` 筛选并续页。游标只用于同一组筛选。`cluster status` 增加节点联系、工作和配置生效汇总；`status` 仍表示本进程状态。

## 来源隔离与复检

| 命令 | 说明 |
| --- | --- |
| `integrity quarantines --storage UUID --source chunk\|pack [--after ID] [--limit N]` | 按存储和类型分页，默认 50，最多 200 |
| `integrity quarantine --storage UUID --source chunk\|pack ID [--confirm FINGERPRINT]` | 无 confirm 时完整核查并预览；指定返回的 fingerprint 后再次核查并提交隔离任务 |
| `integrity recheck --storage UUID --source chunk\|pack ID [--confirm FINGERPRINT]` | 预览已隔离来源；确认后异步完整回源复检，成功才解除 |

ID 是独立 encoding ID 或 pack ID。CLI 与面板使用同一授权、来源代次和任务边界；无强制解除选项。隔离/复检允许维护模式，排空时拒绝。任务入队不代表成功，使用 `task show UUID` 核对状态与结果；普通巡检不会自动隔离，故障计数也不会触发改库。