# 部署、升级与恢复

默认部署运行一个单容器模式实例，同时提供管理、S3 和公共读，独占一个数据库、data 目录和存储范围。需要拆分职责或多节点入口时，使用[群集部署](cluster-deployment.md)；群集的 control、worker、data 容器仍共享同一数据库和部署密钥，但各自使用独立 data 目录。以下单容器部署命令在源码根目录执行。

单容器使用进程内任务调度、活动读写保护和存储并发限制，不创建群集节点或子任务，也不维护分布式租约或共享 staging。任务、配额、上传确认和恢复状态仍持久化在数据库中。

## 构建镜像

```sh
./build.sh
```

默认构建 Debian slim 镜像，标签为 `wxwmoe/mokyu:latest` 及项目版本号。可用 `./build.sh --alpine` 构建 Alpine 镜像，标签为 `:alpine` 及带 `-alpine` 后缀的版本号；`--no-cache` 禁用构建缓存。

构建会安装并编译 Vue 静态资源，再嵌入 Rust 二进制；运行不需要 Node.js。`--api-only` 省略内嵌前端，标签为 `:api` / 版本号加 `-api`；与 `--alpine` 组合使用 `:alpine-api` / 版本号加 `-alpine-api`。仓库不包含构建后的镜像。

可压缩的内嵌资源同时保存 gzip、Brotli 和 Zstandard 构建产物，按 `Accept-Encoding` 的 q 值选择，同权重时依次优先 zstd、br、gzip、identity。未提供或为空时返回原始表示；原始 ETag 为强标签，压缩变体使用同值弱标签，并发送 `Vary: Accept-Encoding`。公开 OpenAPI JSON 使用有界流式压缩，其余管理 API 保持原有响应与缓存策略。

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

### MySQL / MariaDB 单容器模式

设置 `[database]` 的 `type = "mysql"`，复用 `host`、`port`（默认 `3306`）、`name`、`user`、密码和 SSL 字段。自行准备 MySQL 8.0.19+ 或 MariaDB 10.6.3+ 空库与具备建表、索引和迁移权限的用户；程序自动识别服务器，数据表由 Mokyu 初始化。示例 Compose 使用 PostgreSQL 18，选择 MySQL/MariaDB 时替换数据库服务及其持久卷。`innodb_flush_log_at_trx_commit` 必须为 `1`。新部署建议使用仍在维护的稳定版或 LTS 的最新补丁。

一个 MySQL/MariaDB 库只允许一个 single 实例，启动时取得连接级独占锁；锁连接丢失会触发退出。不能启动 control、worker 或 data，也不能切换为群集。库内不建立群集协调表，载荷存储仍可选 local 或 S3。

### SQLite 单容器模式

将配置的数据库段替换为：

```toml
[database]
type = "sqlite"
# path = "/data/mokyu.db"
```

保留 `[instance]` 的部署密钥、监听和资源设置，将 `/data` 挂载为可写的本地持久目录。无需数据库服务；使用示例 Compose 时删除 `postgres` 服务、网关对它的依赖及数据库专用密码挂载。运行命令仍为 `mokyu`，不可使用 control、worker 或 data。

数据库及 WAL、本地暂存应纳入一致备份。不要在运行时移动、覆盖或删除数据库及锁文件，也不要让多个容器共用同一文件。载荷存储仍可使用 local 或 S3；选择 SQLite 不代表对象内容全部存入数据库。连接池、文件系统和权限要求见[配置参考](configuration.md#database)。

### 更换数据库类型

数据库类型之间没有自动迁移。新建目标部署，通过两端 S3 接口复制逻辑对象，并重建账户、权限、密钥和运行配置。对象和管理资源 ID 只在各自部署内有效，不承诺保留旧 ID；修改 `database.type` 或复制底层载荷不能转换数据库。不要将数据库导出文件直接导入另一种数据库。

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

示例禁用 nginx gzip，并将 `Accept-Encoding` 传给 Mokyu；`Content-Encoding`、`Vary`、`ETag` 和 `Cache-Control` 随响应透传。额外安装的 Brotli、Zstandard 或 gunzip 过滤器也应在这些入口关闭，避免二次转换。TLS、HTTP/2 和 HTTP/3 仍由反向代理终止。

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

PostgreSQL 媒体目录使用 `pg_trgm` 扩展；数据库用户须能创建该扩展，或由数据库管理员提前安装。目录索引在迁移事务中建立，服务启动后即可搜索、筛选和排序。为索引预留数据库磁盘空间，长路径和大量对象会增加占用。

需要验证数据时，手动运行[完整性巡检](cli-reference.md#完整性巡检)。存储中未索引区块仅在管理需要时使用[清查命令](cli-reference.md#存储清查)。

### 补齐无引用区块的回收标记

若数据库中存在 `ready`、无引用且 `unreferenced_at` 为空的遗留区块，先启用维护模式并等待活跃操作排空，再运行 CLI：

```sh
docker exec mokyu mokyu cli maintenance enable
docker exec mokyu mokyu cli gc mark-unreferenced
```

命令取得数据库锁，只补时间标记，不访问存储；退出维护后，区块按正常 GC 宽限回收。有引用的区块和已有时间标记保持不变。

## 备份材料

项目不代替部署环境备份存储区块或区块包。PostgreSQL 可使用 pgBackRest 备份数据库/WAL；MySQL 使用支持 InnoDB 一致备份的工具，并按恢复需求保留 binlog。配置和载荷可使用 restic 等备份工具。

| 材料 | 恢复用途 |
| --- | --- |
| 数据库一致备份及相应恢复日志 | 对象索引、权限、引用和上传状态 |
| 配置、`instance.deployment_key` | storage 身份、解密数据库中的历史区块密钥及客户端凭据 |
| 仍存在的存储区块和区块包（包括本地存储目录或 S3 命名空间） | 实际对象内容 |
| `instance.data_path/multipart` 的一致快照 | 恢复已确认但尚未 Complete 的分片上传 |
| `instance.data_path/chunks` | 包含待上传唯一副本；有积压时必须与数据库一致保存 |

上传缓存默认开启，成功响应可能表示数据库和本地文件已持久，远端存储尚未收到。本地磁盘丢失可能丢失这些已确认数据。只读缓存可重建，待上传文件不能丢弃。

建立恢复点可选择：

1. 进入维护模式、等待活跃写入排空，显式运行 `cli cache flush` 并等待任务完成，确认 `cli cache status` 无积压；停止网关，再备份数据库、配置和必要的 multipart 数据。
2. 停止写入并取得数据库、配置、multipart 和 chunks 待上传数据的一致快照。

排空只保证该恢复点，不保证此前任意 WAL 时刻：旧数据库可能引用后来已淘汰的本地来源。任意 PITR 还需匹配的数据历史，或关闭提前本地确认并遵守存储 GC 保留窗口。仅备份配置不能恢复本地唯一数据。

SQLite 优先在停止 Mokyu 后备份整个数据库目录；若存在 `mokyu.db-wal`，必须与数据库文件一起保留，不要只复制正在使用的 `.db`。在线备份须使用 SQLite 的一致备份接口或 `VACUUM INTO`，并另行保证 multipart、chunks 和载荷属于匹配的恢复点。自动 WAL checkpoint 不等于备份；不要手动删除 WAL。

**可恢复时间同时受数据库备份及恢复日志、存储 GC 和密钥保留限制。** 旧数据库引用的区块若已被回收，无法仅凭数据库或密钥重建。

## 恢复步骤

1. 停止所有会写同一数据库或存储范围的网关，保留现场，核对备份所属部署。
2. 恢复数据库、配置和全部历史密钥；一并恢复该恢复点依赖的 multipart、chunks 待上传快照。只有确认恢复点不存在待上传数据时，chunks 才可以为空。
3. 保持恢复数据库对应的 `instance.deployment_key` 不变。单容器使用 `mokyu --maintenance` 启动（Compose 临时设置 `command: ["--maintenance"]`）；群集先只启动 `mokyu control --maintenance`。启动过程先持久化维护状态，再恢复上传和任务，确认 `cli status` 的 `maintenance=true`。不要先正常启动再开启维护。
4. **生成新的实际区块写密钥**：用 `cli key create NAME block` 创建，再对每个存储执行 `cli storage key set STORAGE NAME`，保留历史读密钥。需要把存量编码全部切换时，再显式运行 `storage rewrite`。数据库 ID 可能回滚；只改名称或日期不能防止 nonce 重用。完成这一步前不要执行缓存 flush、运行会重新编码区块的维护任务或恢复写入。
5. 确认 `cli status` 的 `maintenance=true`，核对 storage 身份、历史密钥、旧对象和 Range 读取；可运行巡检。清桶任务会保持暂停，须在核验并退出维护后显式恢复。缺失 multipart 尾部的未完成上传需重传，已发布对象不依赖该目录。
6. 如需清理恢复点之后遗留的未知区块，保持维护并排空活跃操作，先[预览存储清查](cli-reference.md#存储清查)，核对后再确认执行；并非每次恢复都需清查。
7. 移除 Compose 的临时启动参数，再用 `cli maintenance disable` 恢复写入；验证新上传和旧对象读取。存储切换 current key 后仍应保留历史 key。

## 升级与数据库迁移

新数据库执行完整迁移链；已有数据库先校验迁移历史，再执行后续迁移。

PostgreSQL 执行 `0034` 前，必须已完整执行 `0001–0033` 且没有业务数据；初始化元数据、默认项目及配置行可以保留。检测到业务数据时，程序会在写入新迁移前拒绝，保留原数据和迁移历史。更早或含业务数据的旧 schema 需要新建部署，通过两端 S3 接口迁移逻辑对象，并重建账户、权限、密钥和运行配置；不要复制底层区块或区块包。

1. 保留当前镜像，并按上面的方式排空或一致备份待上传数据，同时保留数据库、配置、密钥及必要的 multipart 数据。
2. 停止网关，构建目标源码的镜像，保留现有数据库、data 和存储配置。
3. 启动新镜像。服务先取得独占锁、校验迁移文件并执行待应用 SQL，全部成功后才启动 HTTP、GC 和上传恢复。
4. 检查日志、`cli status`、旧对象及新上传。普通升级不回滚序列，无需换写密钥；恢复数据库时必须遵循上面的换密钥步骤。

PostgreSQL 和 SQLite 的迁移在事务中执行；失败或中断后修正原因再重启。MySQL 的 DDL 隐式提交，只有 migrator 明确允许重入的迁移会自动续跑，其他失败需先恢复或修复数据库。已成功的迁移不重复执行。不要修改已发布迁移或迁移历史。涉及索引构建时，应为大库预留启动时间和磁盘空间。

程序不自动降级数据库。回退需按恢复流程还原升级前的一致备份，并确认相关存储区块仍存在。实际 SQL 见[migrations](../migrations)。

打包功能默认开启，维护改写期间新旧载荷并存，旧载荷满足 GC 宽限和最低存储期后删除；为临时空间和 S3 请求预留预算。停用时设置 `pack.enabled=false`，历史数据仍可读。需要消除历史包时，先运行 `maintenance pack-creation stop` 并等待准备中的包收尾，再执行 `pack unpack --all` 的预览与确认操作；Web 整理工作台提供相同流程。
