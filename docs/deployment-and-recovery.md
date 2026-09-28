# 部署、升级与恢复

每个部署运行一个网关实例，独占数据库、`data` 和后端存储范围。以下命令在源码根目录执行。

## 构建镜像

```sh
./build.sh
```

默认构建 Debian slim 镜像，标签为 `wxwmoe/mokyu:latest` 及项目版本号。可用 `./build.sh --alpine` 构建 Alpine 镜像，标签为 `:alpine` 及带 `-alpine` 后缀的版本号；`--no-cache` 禁用构建缓存。

下文使用默认镜像。[Compose 示例](../compose.yaml) 使用随源码维护的版本标签；选择 Alpine 时需相应调整服务的 `image`。

## 准备配置并启动

以下步骤用于首次部署；已有部署使用[升级流程](#升级与数据库迁移)。

1. 创建配置、密钥和数据目录：

   ```sh
   mkdir -p config/secrets config/keys data
   chmod 700 config config/secrets config/keys data
   cp config.example.toml config/config.toml
   cp keyring.example.toml config/keys/keyring.toml
   docker run --rm wxwmoe/mokyu:latest keygen > config/secrets/postgres-password
   docker run --rm wxwmoe/mokyu:latest keygen > config/secrets/credential-key
   docker run --rm wxwmoe/mokyu:latest keygen > config/keys/new-chunk-key
   ```

2. 编辑 `config/config.toml`：

   | 配置 | 填写内容 |
   | --- | --- |
   | `database` | 主机、端口、数据库和用户；删除示例 `password`，启用 `password_file = "secrets/postgres-password"`，与 Compose 共用密码文件 |
   | `backend` | endpoint、region、私有 bucket、可选 prefix，以及直接填写或通过文件提供的凭据；需具备该范围的 Get / Put / Delete / List 和条件创建权限 |
   | `manage` | 浏览器访问的完整 `origin`；通过 HTTP 访问时将 `secure_cookie` 设为 `false` |
   | `encryption` | 选择算法和 keyring 路径，确保与 keyring 的 active key 算法一致 |

   将 `new-chunk-key` 的内容填入 keyring 后移除临时文件。`credential-key` 必须与区块密钥不同。完整字段和资源限制见[配置参考](configuration.md)。

3. 设置权限并启动：

   ```sh
   chmod 600 config/config.toml config/secrets/* config/keys/*
   chown -R 10001:10001 config data
   docker compose up -d
   docker compose ps
   docker exec mokyu cli status
   ```

Compose 将配置只读挂载、数据目录可写挂载，CLI socket 使用 tmpfs；PostgreSQL 在独立容器中使用命名卷。网关以 UID/GID 10001 运行。

## 初始化与接入

```sh
docker exec mokyu cli bucket create media
docker exec mokyu cli credential create media
docker exec mokyu cli domain set media.example.com media
docker exec -it mokyu cli user create admin
```

凭据的 secret 仅在创建时返回，保存到应用私有配置。不同应用可使用独立逻辑桶、凭据和域名；区块跨桶去重，授权各自独立。

S3 客户端使用网关 endpoint、逻辑桶和生成的凭据，签名 region 与 `listen.region` 一致。寻址、ACL 和上传限制见[S3 兼容性](s3-compatibility.md)。反代配置见[Nginx 示例](../nginx.example.conf)，需替换域名、证书和上游。

## 日常维护

```sh
docker logs --tail 100 mokyu
docker exec mokyu cli status
docker exec mokyu cli gc status
docker exec mokyu cli cleanup status
docker exec mokyu cli task list
```

远端 GC、上传过期和数据库历史清理由服务调度，无需 crontab。管理页可查看容量、清理结果及到期积压；保留期见[清理配置](configuration.md#回收与历史清理)。历史记录保留期不延长远端区块保留或数据库恢复窗口。

保持 PostgreSQL autovacuum/ANALYZE 启用。普通 DELETE/VACUUM 释放的空间通常供数据库复用，不会立即缩小磁盘文件；项目不自动执行 `VACUUM FULL`。

需要验证数据时，手动运行[完整性巡检](cli-reference.md#完整性巡检)。后端未索引区块仅在管理需要时使用[清查命令](cli-reference.md#后端清查)。

### 补齐无引用区块的回收标记

若数据库中存在 `ready`、无引用且 `unreferenced_at` 为空的遗留区块，可先启用维护模式并停止网关，再用 PostgreSQL 客户端执行仓库内的脚本：

```sh
psql -X -h DB_HOST -U DB_USER -d DB_NAME -f scripts/mark-unreferenced-chunks.sql
```

[脚本](../scripts/mark-unreferenced-chunks.sql) 取得网关数据库独占锁，每批最多检查 1000 条记录并提交，可中断后重跑。它只补时间标记，不访问后端；重新启动并退出维护后，区块按正常 GC 宽限回收。有引用的区块和已有时间标记保持不变。无需日常执行，勿使用 `--single-transaction`。

## 备份材料

项目不备份后端 S3 区块或区块包。可用 pgBackRest 备份 PostgreSQL/WAL、restic 备份配置，亦可选用其他满足恢复要求的工具。

| 材料 | 恢复用途 |
| --- | --- |
| 数据库一致备份及所需 WAL | 对象索引、权限、引用和上传状态 |
| 配置、全部历史区块密钥、`credential-key` | 后端身份、解密区块及客户端凭据 |
| 仍存在的后端区块和区块包 | 实际对象内容 |
| `data/multipart` 的一致快照 | 恢复已确认但尚未 Complete 的分片上传 |
| `data/chunks` | 包含待上传唯一副本；有积压时必须与数据库一致保存 |

上传缓存默认开启，成功响应可能表示数据库和本地文件已持久，后端 S3 尚未收到。本地磁盘丢失可能丢失这些已确认数据。只读缓存可重建，待上传文件不能丢弃。

建立恢复点可选择：

1. 进入维护模式、等待活跃写入排空，显式运行 `cli cache flush` 并等待任务完成，确认 `cli cache status` 无积压；停止网关，再备份数据库、配置和必要的 multipart 数据。
2. 停止写入并取得数据库、配置、multipart 和 chunks 待上传数据的一致快照。

排空只保证该恢复点，不保证此前任意 WAL 时刻：旧数据库可能引用后来已淘汰的本地来源。任意 PITR 还需匹配的数据历史，或关闭提前本地确认并遵守后端 GC 保留窗口。仅备份配置不能恢复本地唯一数据。

**可恢复时间同时受数据库/WAL、后端 GC 和密钥保留限制。** 旧数据库引用的区块若已被回收，无法仅凭数据库或密钥重建。

## 恢复步骤

1. 停止所有会写同一数据库或后端范围的网关，保留现场，核对备份所属部署。
2. 恢复数据库、配置和全部历史密钥；一并恢复该恢复点依赖的 multipart、chunks 待上传快照。只有确认恢复点不存在待上传数据时，chunks 才可以为空。
3. **生成新的实际区块写密钥**，加入 keyring 并设为 active，保留旧密钥供读取。数据库 ID 可能回滚，只改 key ID 或日期不能防止 nonce 重用。`credential-key` 仍须与恢复的数据库匹配。
4. 在 Compose 中临时设置 `command: ["serve", "--maintenance"]` 后启动，使维护状态在监听前生效。
5. 确认 `cli status` 的 `maintenance=true`，核对后端身份、历史密钥、旧对象和 Range 读取；可运行巡检。清桶任务会保持暂停，须在核验并退出维护后显式恢复。缺失 multipart 尾部的未完成上传需重传，已发布对象不依赖该目录。
6. 如需清理恢复点之后遗留的未知区块，保持维护并排空活跃操作，先[预览后端清查](cli-reference.md#后端清查)，核对后再确认执行；并非每次恢复都需清查。
7. 移除 Compose 的临时启动参数，再用 `cli maintenance disable` 恢复写入；验证新上传和旧对象读取。切换 active 后仍应保留历史读密钥。

## 升级与数据库迁移

wxw-media-gateway 的旧区块和区块包格式不支持直接升级为 Mokyu。改名迁移仅允许后端尚未初始化、没有业务数据的数据库；已使用的旧实例会明确报错。请使用新的数据库、data 目录和空后端命名空间；需要保留文件时，先通过原服务导出，再通过 Mokyu 的 S3 接口重新上传。

1. 阅读目标 Release 的升级说明，保留当前镜像，并按上面的方式排空或一致备份待上传数据，同时保留数据库、配置、密钥及必要的 multipart 数据。
2. 停止网关，构建目标源码的镜像，保留现有数据库、data 和后端配置。
3. 启动新镜像。服务先取得独占锁、校验迁移文件并执行待应用 SQL，全部成功后才启动 HTTP、GC 和上传恢复。
4. 检查日志、`cli status`、旧对象及新上传。普通升级不回滚序列，无需换写密钥；恢复数据库时必须遵循上面的换密钥步骤。

迁移在事务中执行，失败或中断后可修正原因并重启；已成功的迁移不重复执行。不要修改已发布迁移或迁移历史。涉及索引构建时，应为大库预留启动时间和磁盘空间。

程序不自动降级数据库。回退需按恢复流程还原升级前的一致备份，并确认相关后端区块仍存在。实际 SQL 见[migrations](../migrations)。

支持区块包的服务将匹配身份的后端标识升级为格式 2；维护启动时延后至解除维护。物理来源表保留旧区块的身份和密文，升级不重编码数据。历史 chunks-only sweep 预览不再匹配当前范围，需要重新预览。旧 processing.backend_concurrency 可继续作为读写额度；改用 backend 三个方向的并发项时应删除旧项。

打包功能默认开启，维护改写期间新旧载荷并存，旧载荷在 GC 宽限后删除；为这段临时空间和 S3 请求预留预算。停用时设置 pack.enabled=false，历史数据仍可读；需要消除历史包时执行 `pack unpack --all` 的预览与确认操作。
