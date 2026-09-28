# CLI 参考

在线命令通过常驻服务的 Unix socket 执行，统一前缀为 `docker exec media-gateway cli`。命令和输出使用英语；每层命令均支持 `--help`，下表的 `[]` 表示可选参数。

默认读取 `/config/config.toml`，可用 `media-gateway --config PATH cli ...` 指定配置，或用 `cli --socket PATH ...` 覆盖 socket 路径。socket 权限为 0600，需以容器 UID 10001 或 root 访问。

成功向 stdout 输出 JSON、退出 0；执行或连接失败向 stderr 输出错误并非零退出，参数错误退出 2。管理帧最多 1 MiB，RPC 等待 60 秒。任务创建成功表示已排队，后续结果通过 `task show` 查看。

## 桶与域名

| 命令 | 作用与返回 |
| --- | --- |
| `bucket list` | 返回[桶信息](manage-api-reference.md#存储桶)数组 |
| `bucket create NAME` | 创建空逻辑桶，返回桶信息 |
| `bucket delete NAME` | 删除空桶，返回 deleted UUID；仍有对象、上传或待清理版本时拒绝 |
| `bucket cors NAME FILE` | 用 JSON 文件替换 CORS，返回实际规则；`[]` 关闭 |
| `bucket purge NAME [--execute]` | 默认预览对象、活跃上传和桶 UUID；确认执行后封桶并返回 task_id |
| `domain list` | 列出公共 Host 与桶名映射 |
| `domain set HOST BUCKET` | 新增或替换映射；Host 转小写，可含端口 |
| `domain delete HOST` | 删除域名映射 |

CORS 文件示例，字段规则见[CORS 设置](manage-api-reference.md#cors-设置)：

```json
[{"origins":["https://app.example.com"],"methods":["GET","HEAD"],"headers":["range"],"expose":["ETag","Content-Range"],"max_age":3600}]
```

```sh
docker exec media-gateway cli bucket cors media /config/cors.json
```

## 凭据与用户

| 命令 | 作用与返回 |
| --- | --- |
| `credential list` | 返回 access_key/enabled 数组，不含 secret |
| `credential create BUCKET [--read-only]` | 生成 access_key/secret_key 并授权该桶，默认读写；secret 仅返回一次 |
| `credential grant ACCESS_KEY BUCKET [--read-only]` | 添加或替换桶授权，默认读写，返回 granted |
| `credential revoke ACCESS_KEY BUCKET` | 撤销桶授权，返回 revoked |
| `credential disable ACCESS_KEY` | 禁用凭据及其全部桶授权 |
| `user list` | 返回 id/username/enabled，不含密码哈希 |
| `user create USERNAME [--password-stdin]` | 创建 Web 管理员；无默认账户，密码 12～1024 字节 |
| `user password USERNAME [--password-stdin]` | 更新密码并撤销全部会话 |
| `user disable USERNAME` | 禁用用户并撤销全部会话 |
| `user delete USERNAME` | 删除用户及其会话 |

创建凭据的输出应保存到应用私有配置。密码默认在终端隐藏输入并再次确认，使用 `docker exec -it`；非终端须指定 `--password-stdin`。不接受明文命令参数，自动化时可从 stdin 读取一行：

```sh
docker exec -i media-gateway cli user create admin --password-stdin < /secure/password-file
```

首次初始化流程见[部署与接入](deployment-and-recovery.md#初始化与接入)。

## 状态与维护

| 命令 | 作用与返回 |
| --- | --- |
| `status` | 返回[运行状态和容量快照](manage-api-reference.md#运行状态)，读取不触发全表汇总 |
| `gc status` | 返回 paused/maintenance/running、区块状态计数、宽限、间隔和 min_storage_duration |
| `gc run` | 执行一批本地清理与远端回收，返回计数；遵守暂停、维护、宽限及后端最低存储期限 |
| `gc pause` | 持久暂停后续远端 GC，已发出的请求可能完成 |
| `gc resume` | 解除暂停；需要立即执行时再调用 `gc run` |
| `cleanup status` | 返回历史保留策略、运行状态和上轮结果 |
| `cleanup run` | 立即执行一轮历史清理，遵守保留期、引用和资源预算；已有清理运行时返回当前状态 |
| `maintenance enable` | 持久禁止新写入和远端 GC，暂停清桶、pack 和拆包任务，返回 active_operations；须等待已接纳操作排空 |
| `maintenance disable` | 恢复写入；须先暂停或完成破坏性 sweep |

## 后台任务

| 命令 | 作用与返回 |
| --- | --- |
| `task list` | 最近 100 个任务 |
| `task show UUID` | 完整任务状态、游标、计数、detail/error |
| `task pause UUID` | 暂停 queued/running 任务，允许当前批次结束 |
| `task resume UUID` | 从持久进度继续 paused/failed 任务，清除旧错误 |

重启后此前运行的任务自动继续，paused 保持暂停。维护模式下 purge/pack/unpack/upload 会持久暂停，退出维护后需显式 `task resume`。completed 任务不能继续；继续破坏性 sweep 需要维护模式，purge/pack/unpack/upload 需要退出维护模式，只读 sweep、巡检和显式 cache flush 可在维护模式下运行。

### Pack 维护

| 命令 | 作用 |
| --- | --- |
| `pack status` | 开关、大小上限，以及各状态的数量和字节 |
| `pack run [--kind KIND]` | KIND 为 pack（默认）、reuse、reclaim、range、repack；分别为创建、复用拆分、无引用成员回收、Range 回源优化、碎片合并 |
| `pack unpack ID [--execute]` | 默认预览；execute 创建将指定包拆成独立区块的任务 |
| `pack unpack --all [--execute]` | 拆除全部历史包；先关闭 pack.enabled 并重启 |

手动维护仍遵守引用、冷却、收益、维护模式和资源限制；run 返回 task_id，可用 task 命令暂停、恢复、查看错误。拆包先写新来源再切换映射，旧包按 GC 宽限回收，临时占用会增加。关闭 pack 后 reuse/reclaim 仍可执行，但只输出独立块。

range 需要 range_optimization=true；pack.enabled=false 时可优化历史包，但不新建小包。task detail.last_range 给出候选、处理原因和预计净收益。重新合包受观测窗口及冷却约束，复用拆分不受这项冷却阻挡。

### 清空存储桶

```sh
docker exec media-gateway cli bucket purge media
docker exec -it media-gateway cli bucket purge media --execute
```

执行需要真实终端，依次输入精确桶名和 `DELETE`。任务遍历删除对象及上传；共享区块在失去最后引用并满足 GC 宽限后回收，返回 task_id 时空间不会立即释放。

### 后端清查

| 命令 | 作用 |
| --- | --- |
| `backend sweep [--older-than 48h]` | 默认预览；扫描部署 chunks/packs 规范路径，统计未被数据库任何物理状态索引的载荷 |
| `backend sweep --execute --preview UUID [--older-than 48h]` | 按相同前缀和年龄阈值重新扫描，每次删除前重新查询数据库 |

预览和执行均以远端 `Last-Modified` 计算年龄，取 `--older-than` 与 `backend.min_storage_duration` 中较长者。最低期限改变后须重新预览；继续旧任务也不会绕过当前最低期限。

执行需要维护模式、已完成的预览、排空的活跃操作、非空区块索引及真实终端；按提示确认前缀，再输入 `DELETE`。预览过期后需重新生成。

旧 chunks-only 预览不能用于扩大范围后的清查；重新生成预览，不复用旧任务的确认范围。

```sh
docker exec media-gateway cli maintenance enable
docker exec media-gateway cli backend sweep
docker exec media-gateway cli task show PREVIEW_UUID
docker exec -it media-gateway cli backend sweep --execute --preview PREVIEW_UUID
```

## 完整性巡检

| 命令 | 参数与返回 |
| --- | --- |
| `integrity check [--mode MODE] [--bucket NAME] [--key KEY]` | 默认 metadata；key 需 bucket；返回 task_id，同一时间最多一个 queued/running 巡检 |
| `integrity issues UUID [--after ID] [--limit N]` | 默认 after=0、limit=100，N 为 1～200；返回 issues 和 next_after，ID 为十进制字符串 |

| 模式 | 检查内容 |
| --- | --- |
| `metadata` | 已发布对象映射、区块元数据和所需密钥 |
| `head` | metadata，加后端存在性及编码长度 |
| `full` | metadata，加远端下载、认证、解压、长度及 BLAKE3 校验 |

巡检只报告，不修复或删除数据；远端检查绕过且不填充缓存，共享物理块每轮只检查一次。没有远端来源的 pending 块在所有模式下校验本地唯一副本。`completed` 仍可能有异常，应查看 `detail.issues`；网络、权限及超时使任务 `failed`，可恢复后继续。覆盖边界、异常代码和 JSONL 导出见[巡检 API](manage-api-reference.md#完整性巡检)，并发和限速见[配置](configuration.md#完整性巡检)。

```sh
docker exec media-gateway cli integrity check --mode full --bucket media
docker exec media-gateway cli task show TASK_UUID
docker exec media-gateway cli integrity issues TASK_UUID
docker exec media-gateway cli task pause TASK_UUID
docker exec media-gateway cli task resume TASK_UUID
```

## 上传缓存

| 命令 | 返回与行为 |
| --- | --- |
| `cache status` | 配置及有效额度、待上传字节/条目/最早时间、最近错误、前 100 条 pin 归属；回退次数为本次进程累计 |
| `cache flush` | 返回 task_id，将创建时已有积压写入独立后端来源；可在维护模式显式执行，支持任务暂停/恢复 |

新请求仍可产生积压。备份排空需先阻止新写入并等待活跃请求，任务完成后确认 pending 为零。关闭上传缓存不删除已有待上传文件；恢复与备份要求见[部署说明](deployment-and-recovery.md#备份材料)。

## 离线入口

| 命令 | 作用 |
| --- | --- |
| `docker run --rm wxwmoe/media-gateway:latest keygen` | 生成 32 字节随机密钥，以 64 个十六进制字符表示 |
| `media-gateway serve --maintenance` | 在监听前设置维护状态，供[数据库恢复](deployment-and-recovery.md#恢复步骤)使用 |
