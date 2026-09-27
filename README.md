# Media Gateway

一个 Rust 实现的 S3 媒体存储网关，支持分块、去重、压缩和加密，并将数据存储至 S3 后端

> 项目仍在开发阶段，不建议用在生产环境 ...

- FastCDC：256 KiB 最小、1 MiB 目标、4 MiB 最大；分块 BLAKE3 哈希去重
- 仅有收益时使用 zstd；区块支持 `none`、`aes-256-gcm`、`chacha20-poly1305` 加密
- 有界流式读写，支持分片上传 CDC，使用 S3-FIFO 缓存后端区块，元数据库 PostgreSQL

升级步骤和兼容范围见[部署与恢复](docs/deployment-and-recovery.md)

## 构建和启动

需要 Docker 环境，以 Debian 12 为例：

```sh
./build.sh
```

构建成功后得到同一镜像的 `wxwmoe/media-gateway:latest` 和 `wxwmoe/media-gateway:0.0.3`

> 默认运行镜像为 Debian slim

`./build.sh --alpine` 可构建 Alpine 版本，标签为 `wxwmoe/media-gateway:alpine` 和 `:0.0.3-alpine`

按 [部署与恢复](docs/deployment-and-recovery.md) 准备配置、密钥和目录，随后：

```sh
docker compose up -d
docker exec media-gateway cli status
docker exec media-gateway cli bucket create media
docker exec media-gateway cli credential create media
docker exec -it media-gateway cli user create admin
```

## 实际接口和运维文档

| 文档 | 内容 |
| --- | --- |
| [部署与恢复](docs/deployment-and-recovery.md) | 从源码构建、Compose、Nginx、初始化、升级和备份恢复 |
| [配置](docs/configuration.md) | 全部 TOML 字段、默认值、资源预算和秘密文件 |
| [CLI](docs/cli-reference.md) | 完整命令、参数、输出、危险确认和任务语义 |
| [管理 API](docs/manage-api-reference.md) | 页面、方法、路径、会话、分页与 i18n |
| [S3 兼容性](docs/s3-compatibility.md) | 已实现操作、校验、Range、ACL 和明确不支持项 |
| [数据库](docs/database-schema.md) | 所有表、字段、约束、索引和状态 |
| [存储格式](docs/storage-format.md) | CDC、AEAD/AAD、nonce、物理路径和恢复规则 |

每个部署仅运行一个网关实例，独占其数据库和 data
