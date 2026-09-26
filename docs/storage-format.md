# 存储格式

PostgreSQL 保存对象路径、权限和引用，后端文件只保存区块载荷，不能单独重建对象索引。区块编码 `format=1`，后端标识 `format_version=1`；它们独立于数据库结构编号和程序版本。

## 分块与编码

| 项目 | 规则 |
| --- | --- |
| CDC | FastCDC v2020，Normalization Level 1、seed 0；最小 256 KiB、目标 1 MiB、最大 4 MiB |
| 边界 | 对连续最多 4 MiB 窗口执行实际切点，窗口末尾不视为 EOF；确认文件结束后才提交尾块 |
| 小文件 | 不足 256 KiB 的非空完整对象使用一个块；空对象零块 |
| 去重 | 部署内跨桶，以原始明文的完整 32 字节 BLAKE3、raw_size、algorithm、key_id 匹配 |
| 压缩 | 每块独立 Zstd frame，compressed 记录实际选择；读取不依赖写入时的等级或策略 |
| 加密 | none / AES-256-GCM / ChaCha20-Poly1305；加密密钥 32 字节，16 字节认证标签附在密文末尾 |

压缩收益门槛见[压缩策略](configuration.md#压缩策略)。去重命中直接复用，调整配置不重编码已有块。`none` 仍校验长度和 BLAKE3，但不提供保密或密钥认证；对象 ETag 与 S3 checksum 独立计算。

## nonce 与 AAD

加密前先提交 chunks 行，取得正 bigint ID，每个物理身份只加密一次。nonce 为 12 字节：

```text
u32(UTC YYYYMMDD).to_be_bytes() || u64(chunk_id).to_be_bytes()
```

日期取 created_at，是整数日期标识；唯一性来自同一密钥下不回滚复用的 ID。例：日期 2026-09-21、ID 42 得到 `01352839000000000000002a`。读取直接使用数据库保存的 nonce。

AAD 按下表逐字段拼接，不使用 JSON、分隔符或本机端序：

| 次序 | 长度（字节） | 内容 |
| --- | --- | --- |
| 1 | 9 | ASCII `MGWCHUNK` + `0x01` |
| 2 | 8 | 正 chunk ID，i64 big-endian |
| 3 | 16 | storage UUID 原始字节 |
| 4 | 32 | 原始 BLAKE3 |
| 5 | 4 | raw_size，i32 big-endian |
| 6 | 4 | stored_size（含 tag），i32 big-endian |
| 7 | 1 | compressed：0/1 |
| 8 | 1 | 算法：none=0、AES-GCM=1、ChaCha=2 |
| 9 | 2 | key_id UTF-8 字节长度，u16 big-endian |
| 10 | 可变 | key_id UTF-8 字节 |

完整块先认证，再有界解压，最后校验原始长度和 BLAKE3。ID、长度、压缩和密钥信息均受 AAD 绑定，不能互换元数据。

后端 PUT 结果不明时，可对同一 key 重传完全相同的编码字节；重新加密需新 ID 和 storage_id。数据库恢复、序列回滚或独立克隆后，恢复写入前必须换用**新的实际写密钥**，保留历史读密钥；具体步骤见[恢复说明](deployment-and-recovery.md#恢复步骤)。

## 后端布局

```text
<backend.prefix>/meta.json
<backend.prefix>/chunks/<UUID 前两位>/<32 位小写 storage UUID>
```

例如 `chunks/08/084f2ff912ff4c6daef1b416fee7b800`。物理 key 不可变且永不复用；两位分组共有 256 个前缀。sweep 只识别分组与 UUID 一致的规范路径，其他对象计入 unrecognized，不删除。

省略 prefix 或设为 `""` 时使用桶根，无前导斜杠；首次初始化要求整个桶为空。非空 prefix 只检查该命名空间。prefix 属于部署身份，修改它不会自动搬迁数据。

### 后端标识

| meta.json 字段 | 含义 |
| --- | --- |
| format_version | 固定为 1 |
| deployment_id | 与数据库一致的部署 UUID |
| chunk_layout | `uuid-prefix2` |
| created_at | 创建时间 |
| created_by | Cargo 包名与版本组成的 `wxw-media-gateway/<版本号>`，仅用于诊断 |

启动时先校验标识，再恢复上传、监听和运行 GC。标识上限 16 KiB；身份冲突、未知格式、损坏或已初始化后丢失均拒绝启动。已有正确标识不因 created_by 变化而重写。

仅数据库未初始化、无对象／区块且后端范围为空时，使用 `If-None-Match` 条件创建标识，失败不降级为覆盖写。S3 已写入但数据库确认前中断时，重启使用已提交的 deployment_id 完成确认。

标识不保存密钥、对象索引或引用计数，不属于 chunks 清查范围，也不能替代备份或跨数据库写锁。数据库迁移不会自动转换后端格式。

## 本地数据

| 路径 | 内容与用途 |
| --- | --- |
| `data/chunks/<前两位>/<UUID>.zst` | 已解密、仍压缩的 Zstd 载荷 |
| `data/chunks/<前两位>/<UUID>.raw` | 已解密、解压的原始字节 |
| `data/multipart/<fragment UUID>` | 仍需保留的原始片段；UUID 为 32 位小写十六进制 |

chunks 使用 S3-FIFO 淘汰，按实际文件大小和条目预算限制。根据 compressed、raw_size、stored_size 和 algorithm，扣除认证标签后计算压缩收益，达到[缓存阈值](configuration.md#区块缓存)才保留 .zst，其余用 .raw；不重新压缩估算。缓存含明文，新文件权限为 0600。

填充先写 .zst.tmp／.raw.tmp 再原子改名，预留和占用均计入配额。启动清理无后缀、无效、重复及未完成的缓存；修改收益阈值不影响既有后缀的读取。缺失或坏缓存按需回源重建，填充失败不阻止返回已经校验的数据。

GET/Range 每批读取最多 64 条映射，最多并行预读范围内两个块。回源先认证、解压、校验；.zst 缓存有界解压，.raw 直接校验，所有块通过长度与 BLAKE3 校验后才输出。

新块 PUT 成功后也尝试填充缓存，复用上传已有载荷；回源填充复用解密／解压结果。去重命中不主动回源预热，新上传块按普通新条目进入缓存。

multipart 片段先写文件并 fsync 文件及父目录，再提交 sealed 和映射。**仍被引用的片段不能因陈旧或缓存压力删除**；Abort／过期释放引用后才清理。已发布对象不依赖该目录。

## 分片上传与发布

1. UploadPart 生成不可变 stream，write_epoch 防止迟到请求覆盖新 part；替换成功前旧 part 仍可用。
2. 已接收的连续数据可 CDC 并上传，缺口后的数据也可形成候选块；尚未确定边界的范围通过 extents 引用远端候选或本地 fragment。
3. UploadPart 成功保证每个字节都有持久来源，不保证全部成为最终整文件 CDC 块。相邻 part 可接续处理并释放尾部。
4. Complete 按冻结的有序清单流式重建连续字节，执行相同 CDC；无需整文件落盘或一次载入全部映射，候选块持续受引用保护。
5. 发布事务核对 ready 来源、连续偏移和总长，原子切换 objects.stream_id、退役旧 stream 并保存 Complete 结果。

GET 固定当前 stream 并持有活跃保护。覆盖和删除仅解除可见引用，共享块按[数据库生命周期](database-schema.md#状态与清理)回收。
