# 部署、升级与恢复

每个部署运行一个网关实例，独占数据库、`data` 和后端存储范围。以下命令在源码根目录执行。

## 构建镜像

```sh
./build.sh
```

默认构建 Debian slim 镜像，标签为 `wxwmoe/media-gateway:latest` 及项目版本号。可用 `./build.sh --alpine` 构建 Alpine 镜像，标签为 `:alpine` 及带 `-alpine` 后缀的版本号；`--no-cache` 禁用构建缓存。

下文使用默认镜像。[Compose 示例](../compose.yaml) 使用随源码维护的版本标签；选择 Alpine 时需相应调整服务的 `image`。

## 准备配置并启动

以下步骤用于首次部署；已有部署使用[升级流程](#升级与数据库迁移)。

1. 创建配置、密钥和数据目录：

   ```sh
   mkdir -p config/secrets config/keys data
   chmod 700 config config/secrets config/keys data
   cp config.example.toml config/config.toml
   cp keyring.example.toml config/keys/keyring.toml
   docker run --rm wxwmoe/media-gateway:latest keygen > config/secrets/postgres-password
   docker run --rm wxwmoe/media-gateway:latest keygen > config/secrets/credential-key
   docker run --rm wxwmoe/media-gateway:latest keygen > config/keys/new-chunk-key
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
   docker exec media-gateway cli status
   ```

Compose 将配置只读挂载、数据目录可写挂载，CLI socket 使用 tmpfs；PostgreSQL 在独立容器中使用命名卷。网关以 UID/GID 10001 运行。

## 初始化与接入

```sh
docker exec media-gateway cli bucket create media
docker exec media-gateway cli credential create media
docker exec media-gateway cli domain set media.example.com media
docker exec -it media-gateway cli user create admin
```

凭据的 secret 仅在创建时返回，保存到应用私有配置。不同应用可使用独立逻辑桶、凭据和域名；区块跨桶去重，授权各自独立。

S3 客户端使用网关 endpoint、逻辑桶和生成的凭据，签名 region 与 `listen.region` 一致。寻址、ACL 和上传限制见[S3 兼容性](s3-compatibility.md)。反代配置见[Nginx 示例](../nginx.example.conf)，需替换域名、证书和上游。

## 日常维护

```sh
docker logs --tail 100 media-gateway
docker exec media-gateway cli status
docker exec media-gateway cli gc status
docker exec media-gateway cli cleanup status
docker exec media-gateway cli task list
```

远端 GC、上传过期和数据库历史清理由服务调度，无需 crontab。管理页可查看容量、清理结果及到期积压；保留期见[清理配置](configuration.md#回收与历史清理)。历史记录保留期不延长远端区块保留或数据库恢复窗口。

保持 PostgreSQL autovacuum/ANALYZE 启用。普通 DELETE/VACUUM 释放的空间通常供数据库复用，不会立即缩小磁盘文件；项目不自动执行 `VACUUM FULL`。

需要验证数据时，手动运行[完整性巡检](cli-reference.md#完整性巡检)。后端未索引区块仅在管理需要时使用[清查命令](cli-reference.md#后端清查)。

## 备份材料

项目不备份后端 S3 区块。可用 pgBackRest 备份 PostgreSQL/WAL、restic 备份配置，亦可选用其他满足恢复要求的工具。

| 材料 | 恢复用途 |
| --- | --- |
| 数据库一致备份及所需 WAL | 对象索引、权限、引用和上传状态 |
| 配置、全部历史区块密钥、`credential-key` | 后端身份、解密区块及客户端凭据 |
| 仍存在的后端区块 | 实际对象内容 |
| `data/multipart` 的一致快照 | 恢复已确认但尚未 Complete 的分片上传 |
| `data/chunks` | 可丢弃并按需重建 |

备份数据库和 multipart 的一致快照前，进入维护模式、等待活跃操作排空并停止网关。仅备份配置不能恢复 multipart 数据。

**可恢复时间同时受数据库/WAL、后端 GC 和密钥保留限制。** 旧数据库引用的区块若已被回收，无法仅凭数据库或密钥重建。

## 恢复步骤

1. 停止所有会写同一数据库或后端范围的网关，保留现场，核对备份所属部署。
2. 恢复数据库、配置和全部历史密钥；需要恢复未完成上传时，一并恢复一致的 multipart 快照。chunks 缓存可以为空。
3. **生成新的实际区块写密钥**，加入 keyring 并设为 active，保留旧密钥供读取。数据库 ID 可能回滚，只改 key ID 或日期不能防止 nonce 重用。`credential-key` 仍须与恢复的数据库匹配。
4. 在 Compose 中临时设置 `command: ["serve", "--maintenance"]` 后启动，使维护状态在监听前生效。
5. 确认 `cli status` 的 `maintenance=true`，核对后端身份、历史密钥、旧对象和 Range 读取；可运行巡检。缺失 multipart 尾部的未完成上传需重传，已发布对象不依赖该目录。
6. 如需清理恢复点之后遗留的未知区块，保持维护并排空活跃操作，先[预览后端清查](cli-reference.md#后端清查)，核对后再确认执行；并非每次恢复都需清查。
7. 移除 Compose 的临时启动参数，再用 `cli maintenance disable` 恢复写入；验证新上传和旧对象读取。切换 active 后仍应保留历史读密钥。

## 升级与数据库迁移

1. 阅读目标 Release 的升级说明，保留当前镜像，并备份数据库、配置、密钥及必要的 multipart 数据。
2. 停止网关，构建目标源码的镜像，保留现有数据库、data 和后端配置。
3. 启动新镜像。服务先取得独占锁、校验迁移文件并执行待应用 SQL，全部成功后才启动 HTTP、GC 和上传恢复。
4. 检查日志、`cli status`、旧对象及新上传。普通升级不回滚序列，无需换写密钥；恢复数据库时必须遵循上面的换密钥步骤。

迁移在事务中执行，失败或中断后可修正原因并重启；已成功的迁移不重复执行。不要修改已发布迁移或迁移历史。涉及索引构建时，应为大库预留启动时间和磁盘空间。

程序不自动降级数据库。回退需按恢复流程还原升级前的一致备份，并确认相关后端区块仍存在。实际 SQL 见[migrations](../migrations)。
