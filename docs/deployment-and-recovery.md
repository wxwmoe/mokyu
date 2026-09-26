# 部署、升级与恢复（0.0.2）

需要 Docker 环境，以 Debian 12 为例：

## 从源码构建

```sh
./build.sh
docker image inspect wxwmoe/media-gateway:latest wxwmoe/media-gateway:0.0.2 --format '{{.Id}}'
```

Dockerfile 在构建阶段安装 Rust 所需 CMake，生成 release 二进制，运行阶段为 Debian slim、CA证书及二进制，UID/GID10001

可选 `./build.sh --alpine` 使用 Dockerfile.alpine 构建 musl 版本，得到 `:alpine` 与 `:0.0.2-alpine`

## 准备配置

在源码根目录执行；以下 `config/` 和 `data/` 是此部署新目录

```sh
mkdir -p config/secrets config/keys data
chmod 700 config config/secrets config/keys data
cp config.example.toml config/config.toml
docker run --rm wxwmoe/media-gateway:0.0.2 keygen > config/secrets/postgres-password
docker run --rm wxwmoe/media-gateway:0.0.2 keygen > config/secrets/credential-key
docker run --rm wxwmoe/media-gateway:0.0.2 keygen > config/keys/new-chunk-key
```

在 `config.toml` 填写 `database` 的 `host`、`port`、`name` 和 `user`

删除示例 `password` 并启用 `password_file="secrets/postgres-password"`，与 Compose 的 PostgreSQL 密码文件共用

也可把相同密码直接写进 `database.password`，用户名和密码由 SQLx 连接参数传递，无需URL转义

后端 `access_key` 和 `secret_key` 可以直接填写 `config.toml`，或分别改用 `access_key_file` / `secret_key_file`

复制[keyring.example.toml](../keyring.example.toml)到 `config/keys/keyring.toml`，将 key 替换为 `new-chunk-key` 的内容

确认 `active` 和 `algorithm` 匹配后移除临时 `new-chunk-key` 文件，`credential-key` 必须与区块密钥不同

编辑config.toml：

填入 `backend` 的 `endpoint`、`region`、`bucket` 和 `prefix`（可选），backend 授权须包含此 prefix 下的 Get / Put / Delete 及 List 权限

文件准备完成后：

```sh
chmod 600 config/config.toml config/secrets/* config/keys/*
chown -R 10001:10001 config data
docker compose up -d
docker compose ps
docker exec media-gateway cli status
```

示例 Compose 将 config 只读挂载、data 可写挂载，UDS 使用 tmpfs；PostgreSQL18 独立容器、volume

## 初始化逻辑桶和管理用户

```sh
docker exec media-gateway cli bucket create media
docker exec media-gateway cli credential create media
docker exec media-gateway cli domain set media.example.com media
docker exec -it media-gateway cli user create admin
```

S3 兼容访问网关，path-style 默认可用，region 需要匹配 listen.region

一个部署可为不同应用建不同逻辑桶、不同凭据、不同公共域名，区块去重跨桶进行，但授权和对象 ACL 独立

## Nginx

参考[nginx.example.conf](../nginx.example.conf)，替换域名、证书和上游

## 日常维护

```sh
docker logs --tail 100 media-gateway
docker exec media-gateway cli status
docker exec media-gateway cli gc status
docker exec media-gateway cli cleanup status
docker exec media-gateway cli task list
```

GC 和上传过期由服务内部调度，无需 `crontab` 定时执行

数据库历史同样自动清理：默认保留已删除区块日志 7 天、已结束上传 24 小时、已完成任务 30 天。失败/暂停/运行中的任务，以及尚未确认远端删除的区块，不按年龄删除。历史保留不会延长后端区块保留或数据库恢复窗口；identity 序列和密钥指纹不会被回收。

管理页显示清理结果、后台快照中的到期积压、表/索引大小、估计死行和自动维护时间。可用 `cli cleanup run` 提前执行一轮，命令不会绕过保留期。正常 DELETE 后交由 PostgreSQL autovacuum/ANALYZE 回收可复用空间和更新统计；项目不自动运行 `VACUUM FULL`，普通 VACUUM 通常也不会让操作系统看到文件立即缩小。

## 备份材料

项目不备份后端 S3 区块，推荐使用 pgBackRest 备份 PostgreSQL / WAL，restic 备份配置目录

恢复对象需要：数据库一致备份、完整历史区块密钥、`credential-key`、后端身份配置及仍存在的后端区块；`data/chunks` 可以丢弃重建

若要求恢复已确认但尚未 `Complete` 的分片上传，还需备份其 `data/multipart` 数据，仅备份配置并不保护这些数据

要取得 DB 与 multipart 一致快照，应进入维护模式、等待活跃操作排空并停止网关，再备份数据库和 multipart 目录

PITR 可恢复到的时间同时受数据库 / WAL 保留、后端全局 GC 宽限及密钥保留约束

旧数据库可能引用已被后端 GC 删除的块；备份、WAL 和 GC 策略需要共同覆盖目标恢复时间

## 恢复步骤

1. 停止所有会写同一数据库或后端范围的网关实例。保留当前现场和备份材料，核对恢复目标是本部署。
2. 恢复同一版本的数据库至独立DB或停机目标，恢复配置和完整旧密钥；有一致multipart快照则恢复它，chunks可以为空。
3. **先生成新的实际区块写密钥**，加入keyring，设置新的active ID；旧key仍保留。数据库ID可能回滚，不能沿用原实际密钥继续加密。credential-key保持与该DB匹配，不能随意替换。
4. 使用`media-gateway serve --maintenance`启动。Compose可临时给服务设置`command: ["serve", "--maintenance"]`并重建；维护状态在监听前落库，防止刚启动时接收新写入或GC。
5. 通过CLI确认maintenance=true；核对后端identity、历史密钥、旧对象和Range读取。缺失multipart尾部会使受影响的未完成上传失效，客户端需重传；已发布对象不依赖该目录。
6. 如需要清理恢复点之后留下的未知物理块，先保持维护并排空活跃操作，运行backend sweep预览，核对范围和样本后按CLI文档二次确认执行。sweep不是每次恢复必须执行的步骤；空索引会被拒绝。
7. 确认恢复完成，去掉Compose临时maintenance启动参数，CLI `maintenance disable`恢复写入，上传新对象并再读旧对象。旧密钥不能因已换active就删除。

恢复窗口内未被后端保留的块无法由数据库或密钥重建

## 版本升级与数据库迁移

1. 阅读目标版本的升级说明，保留当前镜像和配置。按上文取得数据库、配置/密钥及必要 multipart 目录的一致备份。
2. 停止旧网关，构建目标版本镜像。保留数据库、data、后端配置和历史密钥。
3. 使用新镜像启动。程序先取得数据库独占锁，再校验已应用迁移的SHA-384校验和、执行新的编号SQL文件；迁移全部成功后才启动HTTP、GC和上传恢复流程。
4. 检查启动日志、`cli status`、旧对象和新上传。普通升级不回滚数据库序列，因此不要求更换实际写密钥；从备份恢复数据库时仍须按恢复步骤换写密钥。

每个迁移在事务中执行。失败或中断不会提交该步，服务保持未启动状态；处理原因后可重新启动。已经成功的迁移不会重复执行，也不能修改已发布的迁移文件或手改迁移历史。

0.0.2 的 `0002_cleanup.sql` 添加历史清理/引用索引，并将频繁变动表的 autovacuum/analyze scale factor 分别设为 0.05/0.02。升级会在监听启动前建立索引；大库需要为索引构建预留时间及磁盘空间。`0003_task_listing.sql` 添加任务分页索引并替换旧任务状态索引，`0004_integrity.sql` 增加完整性巡检任务类型、异常表和任务轮换索引，`schema_version` 更新为 4。保持 PostgreSQL autovacuum 启用，表级参数可由数据库管理员按负载调整。

可在升级或恢复后手动运行 `cli integrity check --mode metadata` 检查已发布对象映射，再按需要用 head/full 检查远端存在性或内容。巡检可在维护模式下执行，发现异常只生成报告；它不修复数据、不重建数据库，也不替代备份。全量内容检查会读取范围内每个共享物理块一次，可设置请求/带宽限额，支持暂停与重启续跑。失败记录随任务保存，完成报告按 `cleanup.task_retention` 分批到期；详见[CLI](cli-reference.md#完整性巡检)和[报告字段](manage-api-reference.md#完整性巡检)。

新版数据库不能直接使用旧程序启动；需要回退时按恢复流程还原升级前的一致备份，且需确认对应后端区块仍被保留。项目不自动删除数据库、降级结构或重置用户数据。
