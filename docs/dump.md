# 用 Docker 导出 Mokyu 对象

`mokyu dump` 可以在原服务和业务数据库离线时，从本地或 S3 后端导出对象。要导出某个桶，后端须保留该桶已发布的元数据恢复点。工具用最近的快照及其后的连续日志重建对象索引。没有快照时，需要从头开始的完整日志。

## 启动工具容器

在 Linux 宿主机上准备两个目录。镜像默认以 `10001:10001` 运行。

| 宿主机目录 | 容器内路径 | 用途 |
| --- | --- | --- |
| `./backend` | `/data/storage` | 本地后端根目录，包含 `mokyu.json`，镜像用户需有读取权限 |
| `./dump-data` | `/data` | 保存对象索引、续传记录和导出的本地文件，镜像用户需有写入权限 |

将能解锁后端的部署密钥导出为宿主机环境变量 `MOKYU_DUMP_DEPLOYMENT_KEY`。下面用本地后端启动容器。使用 S3 后端时，先选好来源和凭据。[S3 连接设置](#s3-连接设置)

```sh
docker run -d --name mokyu-dump -w /data --no-healthcheck \
  -v "$PWD/dump-data:/data" -v "$PWD/backend:/data/storage:ro" \
  -e MOKYU_DUMP_SOURCE=/data/storage -e MOKYU_DUMP_DEPLOYMENT_KEY \
  --entrypoint sleep wxwmoe/mokyu infinity
```

部署密钥也可从文件读取。将文件只读挂载到镜像用户可读的路径，例如 `/run/secrets/deployment-key`，并设置 `MOKYU_DUMP_DEPLOYMENT_KEY_FILE=/run/secrets/deployment-key`。两种密钥变量只设其一。

在宿主机执行一次以下命令，进入容器的 `/data` 目录。接下来的示例都在这个 shell 中运行。退出后容器继续在后台运行，下次可用同一条命令进入。[Docker exec](https://docs.docker.com/reference/cli/docker/container/exec/)

```sh
docker exec -it mokyu-dump sh
```

## 列举与预览

```sh
mokyu dump ls
mokyu dump ls mokyu://media/photos/
mokyu dump mokyu://media/photos/ s3://backup/mokyu/ --preview
```

首次列举、预览或导出时，工具会自动建立 SQLite 对象索引。默认文件为当前目录的 `./metadata.db`，对应宿主机的 `./dump-data/metadata.db`。有了索引，列举和预览可以离线执行，无需部署密钥或 S3 凭据。

`mokyu dump ls` 列出恢复点中的桶。指定 `photos/` 时只列出该前缀下一级的对象和子前缀。预览最多列出 10 个对象的源路径、目标路径和大小，并汇总全部匹配对象的数量和逻辑字节数。`--preview` 也可放在源路径之前。

## 导出对象

导出时需要部署密钥及源后端的读取权限。下面将单个对象写到当前目录，对应宿主机的 `./dump-data/a.jpg`。

```sh
mokyu dump mokyu://media/photos/a.jpg ./a.jpg
```

配置好目标 S3 凭据后，可以递归导出 `photos/` 前缀，也可以导出整个 `media` 桶。

```sh
mokyu dump mokyu://media/photos/ s3://backup/mokyu/
mokyu dump mokyu://media s3://backup
```

按前缀导出时，`photos/2026/a.jpg` 写入 `s3://backup/mokyu/2026/a.jpg`。按整桶导出时，它保留原键，写入 `s3://backup/photos/2026/a.jpg`。如果桶中同时有 `photos` 和 `photos/a.jpg`，而目标用文件和目录表示对象，可将 `photos` 单独另存为 `photos.bin`，再导出 `photos/` 前缀。

## S3 连接设置

从 S3 后端读取时，可省去本地 `./backend` 挂载。将 `MOKYU_DUMP_SOURCE` 设为包含 `mokyu.json` 的物理桶或前缀，例如 `s3://physical-bucket/mokyu/`。导出命令中的 `mokyu://media/photos/` 选择逻辑对象，目标 URI `s3://backup/mokyu/` 指定写入的桶和前缀。

只为实际使用的 S3 端设置环境变量。可以在启动容器时传入，也可以进入容器后用 `export` 设置。源端需要列举和读取权限。目标端需要读取已有对象信息和写入权限，使用 Multipart 时还需能列举分片、完成或中止本次上传。

| 用途 | 读取源 S3 | 写入目标 S3 |
| --- | --- | --- |
| 访问密钥 ID | `MOKYU_DUMP_SOURCE_ACCESS_KEY_ID` | `MOKYU_DUMP_DEST_ACCESS_KEY_ID` |
| 访问密钥 | `MOKYU_DUMP_SOURCE_SECRET_ACCESS_KEY` | `MOKYU_DUMP_DEST_SECRET_ACCESS_KEY` |
| 会话令牌（临时凭据） | `MOKYU_DUMP_SOURCE_SESSION_TOKEN` | `MOKYU_DUMP_DEST_SESSION_TOKEN` |
| 服务根地址（如 `https://s3.example.com`） | `MOKYU_DUMP_SOURCE_ENDPOINT` | `MOKYU_DUMP_DEST_ENDPOINT` |
| 签名区域（如 `us-east-1`） | `MOKYU_DUMP_SOURCE_REGION` | `MOKYU_DUMP_DEST_REGION` |
| AWS profile（示例） | `MOKYU_DUMP_SOURCE_PROFILE=backup-ro` | `MOKYU_DUMP_DEST_PROFILE=export-rw` |

直接提供凭据时，访问密钥 ID 和密钥须成对设置。使用临时凭据时，一并提供同一侧的会话令牌。若使用 AWS profile，可将宿主机的 `~/.aws` 只读挂载到 `/aws`，确保镜像用户可读，并设置 `AWS_CONFIG_FILE=/aws/config` 和 `AWS_SHARED_CREDENTIALS_FILE=/aws/credentials`。两端的服务地址和签名区域可分别写在各自的 profile 中。[AWS 共享配置文件位置](https://docs.aws.amazon.com/sdkref/latest/guide/file-location.html)

## 已有目标、续传与刷新

`MOKYU_DUMP_IF_EXISTS` 决定如何处理已存在的目标。

| 值 | 处理方式 |
| --- | --- |
| `verify`（默认） | 本地文件核对长度和 SHA-256，S3 对象核对内容和需要保留的元数据。相符时记为已验证，无法确认一致时保留目标并报告冲突。 |
| `skip` | 直接跳过已有目标。 |
| `overwrite` | 允许用导出的对象替换已有目标。 |

导出进度写入标准错误。结束时会列出已导出、续传、已验证、跳过、冲突和失败的数量。遇到冲突或失败时，命令返回非零退出码，已成功导出的对象会保留。

中断后保留原索引文件和导出目标，重新执行同一条导出命令。

- 本地导出使用目标文件旁的 `.mokyu-dump-<32 位 UUID>.part` 保存进度。重跑时校验已写入的前缀，从确认的位置续写。此文件名格式由工具保留。
- S3 Multipart 导出使用索引中记录的 Upload ID 核对远端分片，复用已确认的分片。

索引固定在建库时的恢复点。要读取更新的日志或换一个源后端，将 `MOKYU_DUMP_DB` 指向新文件，例如 `/data/refreshed.db`，并设置对应的 `MOKYU_DUMP_SOURCE`。下次运行时会自动建库。保留旧库可以继续原来的导出任务，删除旧库会丢失其中的断点和 Upload ID。

## 传输参数

| 环境变量 | 默认值 | 用途 |
| --- | --- | --- |
| `MOKYU_DUMP_MULTIPART_THRESHOLD` | `100MiB` | 逻辑对象超过阈值才对目标 S3 使用 Multipart，恰好等于阈值时用单次 PUT。 |
| `MOKYU_DUMP_PART_SIZE` | `16MiB` | 新上传的目标分片大小，范围为 5 MiB 至 5 GiB。必要时自动调大，使分片数不超过 10,000。 |
| `MOKYU_DUMP_CONCURRENCY` | `4` | 所有对象和在途分片共享的并发上限。可用内存较少时，实际并发也会降低。 |
| `MOKYU_DUMP_MEMORY_LIMIT` | `256MiB` | 工作内存预算。首次建库至少需要 64 MiB，已有库导出至少需要 32 MiB。 |

导出到 S3 时，单个对象最大为 5 TiB。
