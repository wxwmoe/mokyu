<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/mokyu-banner-dark.svg">
  <img src="docs/assets/mokyu-banner-light.svg" width="1280" alt="Mokyu — Little chunks, lots of love. S3-compatible media storage.">
</picture>

# Mokyu

一个 Rust 实现的 S3 媒体存储网关，支持分块去重、压缩和加密，并将数据存储至 S3 后端

> 项目仍在开发阶段，不建议用在生产环境 ...

- **存储**：FastCDC + BLAKE3 去重，zstd 压缩，可选 AES-256-GCM、ChaCha20-Poly1305 加密
- **读写**：有界流式处理，支持分片上传、Range 读取，S3-FIFO 读缓存，上传缓存和区块包优化
- **管理**：Vue 3 + Reka UI 多语言管理界面，媒体图库、上传续传、空间洞察、维护工作台和巡检
- **协作**：默认单管理员；按需启用项目管理、成员、逐桶权限、S3 应用密钥、API Token 与逻辑配额

![Mokyu management console](docs/assets/mokyu-console.png)

## 构建和启动

需要 Docker 环境，在源码根目录执行：

```sh
./build.sh
```

构建成功后得到 `wxwmoe/mokyu:latest` 和对应项目版本标签

> 默认运行镜像为 Debian slim

`./build.sh --alpine` 构建 Alpine 镜像；`--api-only` 省略内嵌前端，两项可组合

按 [部署与恢复](docs/deployment-and-recovery.md) 准备配置、密钥和目录，随后：

```sh
docker compose up -d
docker exec mokyu cli status
docker exec mokyu cli bucket create media
docker exec mokyu cli credential create media
docker exec -it mokyu cli user create admin
```

## 实际接口和运维文档

| 文档 | 内容 |
| --- | --- |
| [部署与恢复](docs/deployment-and-recovery.md) | 从源码构建、Compose、Nginx、初始化、升级和备份恢复 |
| [配置](docs/configuration.md) | 全部 TOML 字段、默认值、资源预算和秘密文件 |
| [CLI](docs/cli-reference.md) | 完整命令、参数、输出、危险确认和任务语义 |
| [管理指南](docs/management.md) | 媒体、项目授权、密钥、配额、空间洞察与维护流程 |
| [管理 API](docs/manage-api-reference.md) | 方法、路径、认证、分页、并发条件和生成契约 |
| [S3 兼容性](docs/s3-compatibility.md) | 已实现操作、校验、Range、ACL 和明确不支持项 |
| [数据库](docs/database-schema.md) | 表与字段清单、关键约束、状态和迁移规则 |
| [存储格式](docs/storage-format.md) | CDC、AEAD/AAD、nonce、物理路径和恢复规则 |

上传缓存默认开启，已确认数据可能暂存本地等待后端落盘。请按[备份与恢复要求](docs/deployment-and-recovery.md#备份材料)保护数据库、密钥和本地唯一副本

## 版权声明

> (> ʌ <) 都看到这了，点个 Star 吧 ~

**互操作参考**

- [Mastodon / AGPL-3.0](https://github.com/mastodon/mastodon)
- [Misskey / AGPL-3.0](https://github.com/misskey-dev/misskey)

**主要后端依赖**

- [tokio / MIT](https://github.com/tokio-rs/tokio)
- [axum / MIT](https://github.com/tokio-rs/axum)
- [hyper / MIT](https://github.com/hyperium/hyper)
- [s3s / Apache-2.0](https://github.com/s3s-project/s3s)
- [object_store / Apache-2.0](https://github.com/apache/arrow-rs-object-store)
- [sqlx / MIT OR Apache-2.0](https://github.com/launchbadge/sqlx)
- [fastcdc / MIT](https://github.com/nlfiedler/fastcdc-rs)
- [blake3 / CC0-1.0 OR Apache-2.0](https://github.com/BLAKE3-team/BLAKE3)
- [zstd / BSD-3-Clause](https://github.com/gyscos/zstd-rs)
- [rustls / Apache-2.0 OR ISC OR MIT](https://github.com/rustls/rustls)
- [aws-lc-rs / ISC AND (Apache-2.0 OR ISC)](https://github.com/aws/aws-lc-rs)
- [argon2 / MIT OR Apache-2.0](https://github.com/RustCrypto/password-hashes/tree/master/argon2)
- [serde / MIT OR Apache-2.0](https://github.com/serde-rs/serde)
- [clap / MIT OR Apache-2.0](https://github.com/clap-rs/clap)

**管理前端**

- [Vue / MIT](https://github.com/vuejs/core)
- [Reka UI / MIT](https://github.com/unovue/reka-ui)
- [TanStack Query / MIT](https://github.com/TanStack/query)

**图标与字体**

- [Lucide / ISC](https://github.com/lucide-icons/lucide)
- [Nunito / SIL Open Font License 1.1](https://github.com/googlefonts/nunito)
- [Resource Han Rounded / SIL Open Font License 1.1](https://github.com/CyanoHao/Resource-Han-Rounded)
- [M PLUS Rounded 1c / SIL Open Font License 1.1](https://github.com/google/fonts/tree/main/ofl/mplusrounded1c)

###### 引用的项目与相关依赖保留各自的版权及许可证

MIT © wxw.moe
