# 部署、升级与恢复

默认部署运行一个单容器模式实例，同时提供管理、S3 和公共读，独占一个数据库、data 目录和存储范围。需要拆分职责或多节点入口时，使用[群集部署](cluster-deployment.md)；群集的 control、worker、data 容器仍共享同一数据库和部署密钥，但各自使用独立 data 目录。以下单容器部署命令在源码根目录执行。

## 构建镜像

```sh
./build.sh
```

默认构建 Debian slim 镜像，标签为 `wxwmoe/mokyu:latest` 及项目版本号。可用 `./build.sh --alpine` 构建 Alpine 镜像，标签为 `:alpine` 及带 `-alpine` 后缀的版本号；`--no-cache` 禁用构建缓存。

构建会安装并编译 Vue 静态资源，再嵌入 Rust 二进制；运行不需要 Node.js。`--api-only` 省略内嵌前端，标签为 `:api` / 版本号加 `-api`；与 `--alpine` 组合使用 `:alpine-api` / 版本号加 `-alpine-api`。仓库不包含构建后的镜像。

独立前端使用 [web](../web/README.md) 的 `dist/`，将 `/api/v1/` 同源反代至管理端口，只有前端页面使用 SPA fallback。管理 Origin 必须是浏览器实际访问的地址；管理 API 不应缓存，也不使用桶的 CORS 规则。首次设置、头像与安全头要求与内嵌部署相同。

下文使用默认镜像。[Compose 示例](../compose.yaml) 使用随源码维护的版本标签；选择 Alpine 时需相应调整服务的 `image`。

## 准备配置并启动

以下步骤用于首次部署；已有部署使用[升级流程](#升级与数据库迁移)。

1. 创建配置、密钥和数据目录：

   ```sh
   mkdir -p config/secrets data
   chmod 700 config config/secrets data
   cp config.example.toml config/config.toml
   docker run --rm wxwmoe/mokyu:latest tool keygen > config/secrets/postgres-password
   docker run --rm wxwmoe/mokyu:latest tool keygen > config/secrets/deployment-key
   ```

2. 编辑 `config/config.toml`：

   | 配置 | 填写内容 |
   | --- | --- |
   | `database` | 主机、端口、数据库和用户；删除示例 `password`，启用 `password_file = "secrets/postgres-password"`，与 Compose 共用密码文件 |
   | `instance` | 监听地址、数据目录、资源预算和固定 `deployment_key`；所有角色使用同一部署密钥 |
   `instance.deployment_key` 必须在同一部署的所有容器中保持不变。首次启动后用 `cli key create NAME block` 创建区块密钥，并用 `cli storage key set STORE KEY` 绑定；密钥材料只在数据库中保存加密副本，CLI 和 Web 永不返回明文。完整字段和资源限制见[配置参考](configuration.md)。

3. 设置权限并启动：

   ```sh
   chmod 600 config/config.toml config/secrets/*
   chown -R 10001:10001 config data
   docker compose up -d
   docker compose ps
   docker exec mokyu mokyu cli status
   ```

Compose 将配置只读挂载、数据目录可写挂载；PostgreSQL 在独立容器中使用命名卷。网关以 UID/GID 10001 运行。CLI 通过本机 control HTTP 入口执行。

## 初始化与接入

首次打开管理页可创建管理员：先运行 `docker exec mokyu mokyu cli setup token`，把本次输出的 token 填入页面。token 只保存哈希并在一小时后失效；也可直接使用 CLI 创建管理员。账户设置支持语言、外观、可选 Gravatar、改密和会话撤销。

```sh
docker exec -it mokyu mokyu cli user create admin
docker exec -it mokyu mokyu cli key create main block
docker exec mokyu mokyu cli storage create primary local --init
docker exec -it mokyu mokyu cli storage key set primary main
docker exec mokyu mokyu cli bucket create media
docker exec mokyu mokyu cli cred create media
docker exec mokyu mokyu cli domain set media.example.com media
```

`storage create primary local` 使用默认路径 `/data/storage`。也可以在 Web 或 CLI 中创建 S3 存储，所需字段和核对规则见[存储](configuration.md#存储与运行参数)。创建桶前必须先创建存储；凭据的 secret 仅在创建时返回，保存到应用私有配置。不同应用可使用独立逻辑桶、凭据和域名；同一存储内跨桶去重，跨存储不去重。

上述在线操作也可在管理页面完成。默认无需启用项目管理；多个应用团队需要分权时，再创建项目与普通成员，并设置桶范围、应用密钥和逻辑额度，见[管理指南](management.md)。

S3 客户端使用网关 endpoint、逻辑桶和生成的凭据，签名 region 与 runtime `requests.s3_region` 一致。寻址、ACL 和上传限制见[S3 兼容性](s3-compatibility.md)。反代配置见[Nginx 示例](../nginx.example.conf)，需替换域名、证书和上游。

## 日常维护

```sh
docker logs --tail 100 mokyu
docker exec mokyu mokyu cli status
docker exec mokyu mokyu cli gc status
docker exec mokyu mokyu cli cleanup status
docker exec mokyu mokyu cli task list
```

远端 GC、上传过期和数据库历史清理由服务调度，无需 crontab。管理页可查看容量、清理结果及到期积压；保留期见[清理配置](configuration.md#回收与历史清理)。历史记录保留期不延长远端区块保留或数据库恢复窗口。

保持 PostgreSQL autovacuum/ANALYZE 启用。普通 DELETE/VACUUM 释放的空间通常供数据库复用，不会立即缩小磁盘文件；项目不自动执行 `VACUUM FULL`。

媒体目录需要 PostgreSQL 的 `pg_trgm` 扩展；数据库用户须能创建该扩展，或由数据库管理员提前安装。首次启动或升级后，服务会逐个并发创建目录索引，再分批回填摘要，重启可继续。构建期间仍可浏览目录、按路径前缀或完整路径查找；包含搜索、筛选和其他排序在就绪后开放。`cli status` 的 `catalog` 字段显示进度。为索引和构建临时文件预留数据库磁盘空间，长路径和大量对象会增加占用。

需要验证数据时，手动运行[完整性巡检](cli-reference.md#完整性巡检)。存储中未索引区块仅在管理需要时使用[清查命令](cli-reference.md#存储清查)。

### 补齐无引用区块的回收标记

若数据库中存在 `ready`、无引用且 `unreferenced_at` 为空的遗留区块，先启用维护模式并等待活跃操作排空，再运行 CLI：

```sh
docker exec mokyu mokyu cli maintenance enable
docker exec mokyu mokyu cli gc mark-unreferenced
```

命令取得数据库锁，只补时间标记，不访问存储；退出维护后，区块按正常 GC 宽限回收。有引用的区块和已有时间标记保持不变。

## 备份材料

项目不代替部署环境备份存储区块或区块包。可用 pgBackRest 备份 PostgreSQL/WAL、restic 备份配置，亦可选用其他满足恢复要求的工具。

| 材料 | 恢复用途 |
| --- | --- |
| 数据库一致备份及所需 WAL | 对象索引、权限、引用和上传状态 |
| 配置、`instance.deployment_key` | storage 身份、解密数据库中的历史区块密钥及客户端凭据 |
| 仍存在的存储区块和区块包（包括本地存储目录或 S3 命名空间） | 实际对象内容 |
| `instance.data_path/multipart` 的一致快照 | 恢复已确认但尚未 Complete 的分片上传 |
| `instance.data_path/chunks` | 包含待上传唯一副本；有积压时必须与数据库一致保存 |

上传缓存默认开启，成功响应可能表示数据库和本地文件已持久，远端存储尚未收到。本地磁盘丢失可能丢失这些已确认数据。只读缓存可重建，待上传文件不能丢弃。

建立恢复点可选择：

1. 进入维护模式、等待活跃写入排空，显式运行 `cli cache flush` 并等待任务完成，确认 `cli cache status` 无积压；停止网关，再备份数据库、配置和必要的 multipart 数据。
2. 停止写入并取得数据库、配置、multipart 和 chunks 待上传数据的一致快照。

排空只保证该恢复点，不保证此前任意 WAL 时刻：旧数据库可能引用后来已淘汰的本地来源。任意 PITR 还需匹配的数据历史，或关闭提前本地确认并遵守存储 GC 保留窗口。仅备份配置不能恢复本地唯一数据。

**可恢复时间同时受数据库/WAL、存储 GC 和密钥保留限制。** 旧数据库引用的区块若已被回收，无法仅凭数据库或密钥重建。

## 恢复步骤

1. 停止所有会写同一数据库或存储范围的网关，保留现场，核对备份所属部署。
2. 恢复数据库、配置和全部历史密钥；一并恢复该恢复点依赖的 multipart、chunks 待上传快照。只有确认恢复点不存在待上传数据时，chunks 才可以为空。
3. 保持恢复数据库对应的 `instance.deployment_key` 不变。启动 control 或单容器入口后，立即执行 `mokyu cli maintenance enable`，确认管理端口可用且数据面已停止写入。
4. **生成新的实际区块写密钥**：用 `cli key create NAME block` 创建，再对每个存储执行 `cli storage key set STORAGE NAME`，保留历史读密钥。需要把存量编码全部切换时，再显式运行 `storage rewrite`。数据库 ID 可能回滚；只改名称或日期不能防止 nonce 重用。完成这一步前不要运行会重新编码区块的维护任务或恢复写入。
5. 确认 `cli status` 的 `maintenance=true`，核对storage 身份、历史密钥、旧对象和 Range 读取；可运行巡检。清桶任务会保持暂停，须在核验并退出维护后显式恢复。缺失 multipart 尾部的未完成上传需重传，已发布对象不依赖该目录。
6. 如需清理恢复点之后遗留的未知区块，保持维护并排空活跃操作，先[预览存储清查](cli-reference.md#存储清查)，核对后再确认执行；并非每次恢复都需清查。
7. 移除 Compose 的临时启动参数，再用 `cli maintenance disable` 恢复写入；验证新上传和旧对象读取。存储切换 current key 后仍应保留历史 key。

## 升级与数据库迁移

1. 阅读目标 Release 的升级说明，保留当前镜像，并按上面的方式排空或一致备份待上传数据，同时保留数据库、配置、密钥及必要的 multipart 数据。
2. 停止网关，构建目标源码的镜像，保留现有数据库、data 和存储配置。
3. 启动新镜像。服务先取得独占锁、校验迁移文件并执行待应用 SQL，全部成功后才启动 HTTP、GC 和上传恢复。
4. 检查日志、`cli status`、旧对象及新上传。普通升级不回滚序列，无需换写密钥；恢复数据库时必须遵循上面的换密钥步骤。

迁移在事务中执行，失败或中断后可修正原因并重启；已成功的迁移不重复执行。不要修改已发布迁移或迁移历史。涉及索引构建时，应为大库预留启动时间和磁盘空间。

程序不自动降级数据库。回退需按恢复流程还原升级前的一致备份，并确认相关存储区块仍存在。实际 SQL 见[migrations](../migrations)。

打包功能默认开启，维护改写期间新旧载荷并存，旧载荷满足 GC 宽限和最低存储期后删除；为临时空间和 S3 请求预留预算。停用时设置 `pack.enabled=false`，历史数据仍可读。需要消除历史包时，先运行 `maintenance pack-creation stop` 并等待准备中的包收尾，再执行 `pack unpack --all` 的预览与确认操作；Web 整理工作台提供相同流程。
