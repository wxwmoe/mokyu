# 存储格式

数据库保存对象路径、权限和物理引用；storage 保存区块和区块包。storage 文件本身不能单独重建对象索引。格式编号独立于数据库 schema 和程序版本，新增格式只追加编号，不复用旧编号。

## 分块和去重

- 使用 FastCDC v2020，最小 256 KiB、目标 1 MiB、最大 4 MiB；不足最小值的非空完整对象使用一个区块，空对象没有区块。
- 逻辑区块按 `storage_id + hash + raw_size` 去重，`hash` 是原始明文的 32 字节 BLAKE3。同一 storage 的不同桶共享去重，不同 storage 不共享。
- 每个物理来源独立决定压缩和加密。compression profile 支持 `never`、`always`、`sample`；区块包压缩独立记录。新策略不会隐式重写已有来源。
- 加密支持 `none`、AES-256-GCM 和 ChaCha20-Poly1305；认证标签附在密文末尾。block key 由数据库管理，key 算法创建后不可更改。

逻辑区块的 `chunks.id` 在重写、迁移和重新打包时保持不变。物理区块编码使用新的 `encoding_id`，区块包使用自己的 `pack_id`；旧来源在新来源提交后按 GC 宽限回收。

## 物理编码和完整性

每个 ready 独立来源保存 `location_id`、`standalone_size`、`standalone_compressed`、`encryption_algorithm`、`block_key_id`、nonce 和 `encoding_fingerprint`；区块包保存对应的 `pack_fingerprint`。fingerprint 绑定 key、算法、压缩和 storage profile，`storage rewrite` 用它判断是否需要处理。

加密 AAD 绑定格式编号、物理编码 ID、storage UUID、明文哈希、原始长度、编码长度、压缩标志、算法和 block key UUID。读取顺序固定为认证、解密、有界解压、长度检查和 BLAKE3 检查；任何元数据或载荷不匹配都报告完整性错误。

nonce 为 12 字节：前 4 字节编码来源类型和 UTC 日期，后 8 字节为物理 ID。数据库恢复、序列回滚或独立克隆后，恢复写入前必须更换新的实际 block key，并保留历史 key 读取存量来源。

## 存储布局

```text
<storage.prefix>/meta.json
<storage.prefix>/<UUID 前两位>/<32 位小写 UUID>
<storage.prefix>/packs/<UUID 前两位>/<32 位小写 UUID>
```

`meta.json` 包含 `format_version`、`deployment_id`、`storage_id`、chunk/pack layout、创建时间和版本诊断字段。创建 storage 时只允许在目标命名空间为空的情况下用条件写入初始化；编辑已有 storage 时必须读取并匹配完整 meta，缺失或不匹配会拒绝保存。修改 prefix 不会自动搬迁载荷。

独立区块直接使用 UUID 分片路径。sweep 同时识别此前文档中的 `chunks/<分片>/<UUID>` 路径；任何仍被物理索引记录的 UUID 都受到保护。

群集上传暂存使用所选 staging storage 的保留前缀：

```text
<storage.prefix>/__mokyu/multipart/<upload-or-fragment-key>
```

暂存对象不参与区块去重、普通目录、usage、rewrite 或 GC；由 `staging_objects` 和 runtime 的 staging 字节上限管理。单容器模式的 multipart 和 pending upload 可以使用 `instance.data_path`。

## 区块包

区块包只收集同一 storage、完整且连续的 CDC 区块，成员至少两个，原始成员总长受 profile 的 `pack_max_size` 限制。包整体可压缩并按包的 key 加密，成员索引和未压缩载荷摘要写入包头及数据库。

local storage 不创建新区块包；已有包保持可读，显式 `storage unpack` 才会拆包。S3 storage 可由普通 pack 调度器处理符合条件的存量独立区块，也可由显式 `storage rewrite` 按当前 profile 全量重写。保存 storage 表单、切换 local/S3 或载入预设都不会自动启动这些任务。

## 本地目录和缓存

单容器模式的本地目录由 `instance.data_path` 指定：

```text
<data_path>/chunks/<前两位>/<UUID>.zst
<data_path>/chunks/<前两位>/<UUID>.raw
<data_path>/multipart/<fragment UUID>
<data_path>/node.json
```

本地 storage 的载荷直接从本地后端读取，不进入区块或区块包缓存；为性能可关闭本地 storage 的压缩和加密。S3 storage 才使用 profile cache，缓存文件含明文时权限为 0600，损坏缓存可回源重建。待上传的唯一副本受 pin 保护，不能按普通缓存淘汰。

普通读取在校验后返回正文，磁盘缓存填充和坏缓存清理在有界后台任务中执行；缓存额度不足或填充失败不影响已验证正文，响应完成不代表缓存已经写入。上传暂存仍按持久化要求落盘后确认。

所有写入先持久化文件或远端对象，再提交数据库映射；local 后端在确认成功前同步文件和父目录，同步失败不会发布新映射。对象发布要求 stream 连续、来源 ready、长度和哈希全部匹配。跨 storage 的复制/移动由异步 `storage_migration` 任务完成复制、校验、权限和发布一致性；部署管理员负责两端容量、凭据、网络和供应商策略。
