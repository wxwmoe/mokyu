<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/mokyu-banner-dark.svg">
  <img src="docs/assets/mokyu-banner-light.svg" width="1280" alt="Mokyu — Little chunks, lots of love. S3-compatible media storage.">
</picture>

# Mokyu

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

构建成功后得到同一镜像的 `wxwmoe/mokyu:latest` 和 `wxwmoe/mokyu:0.0.3`

> 默认运行镜像为 Debian slim

`./build.sh --alpine` 可构建 Alpine 版本，标签为 `wxwmoe/mokyu:alpine` 和 `:0.0.3-alpine`

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
| [管理 API](docs/manage-api-reference.md) | 页面、方法、路径、会话、分页与 i18n |
| [S3 兼容性](docs/s3-compatibility.md) | 已实现操作、校验、Range、ACL 和明确不支持项 |
| [数据库](docs/database-schema.md) | 所有表、字段、约束、索引和状态 |
| [存储格式](docs/storage-format.md) | CDC、AEAD/AAD、nonce、物理路径和恢复规则 |

每个部署仅运行一个网关实例，独占其数据库和 data

## 版权声明

> (> ʌ <) 都看到这了，点个 Star 吧 ~

互操作参考

- [mastodon / AGPL-3.0][1]
- [misskey / AGPL-3.0][2]

相关依赖

- [tokio / MIT][3]
- [axum / MIT][4]
- [hyper / MIT][5]
- [s3s / Apache-2.0][6]
- [object_store / Apache-2.0][7]
- [sqlx / MIT OR Apache-2.0][8]
- [fastcdc / MIT][9]
- [blake3 / CC0-1.0 OR Apache-2.0][10]
- [zstd / BSD-3-Clause][11]
- [rustls / Apache-2.0 OR ISC OR MIT][12]
- [aws-lc-rs / ISC AND (Apache-2.0 OR ISC)][13]
- [argon2 / MIT OR Apache-2.0][14]
- [serde / MIT OR Apache-2.0][15]
- [clap / MIT OR Apache-2.0][16]
- [Vue / MIT](https://github.com/vuejs/core)、[Reka UI / MIT](https://github.com/unovue/reka-ui)、[TanStack Query / MIT](https://github.com/TanStack/query)
- [Lucide / ISC](https://github.com/lucide-icons/lucide)、[Nunito / SIL Open Font License 1.1](https://github.com/googlefonts/nunito)

前端分发包含 `THIRD_PARTY_NOTICES.txt`，保留所用包与字体的许可证文本。

###### 引用的项目与相关依赖保留各自的版权及许可证

MIT © wxw.moe

  [1]: https://github.com/mastodon/mastodon
  [2]: https://github.com/misskey-dev/misskey
  [3]: https://github.com/tokio-rs/tokio
  [4]: https://github.com/tokio-rs/axum
  [5]: https://github.com/hyperium/hyper
  [6]: https://github.com/s3s-project/s3s
  [7]: https://github.com/apache/arrow-rs-object-store
  [8]: https://github.com/launchbadge/sqlx
  [9]: https://github.com/nlfiedler/fastcdc-rs
  [10]: https://github.com/BLAKE3-team/BLAKE3
  [11]: https://github.com/gyscos/zstd-rs
  [12]: https://github.com/rustls/rustls
  [13]: https://github.com/aws/aws-lc-rs
  [14]: https://github.com/RustCrypto/password-hashes/tree/master/argon2
  [15]: https://github.com/serde-rs/serde
  [16]: https://github.com/clap-rs/clap
