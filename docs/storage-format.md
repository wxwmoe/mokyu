# 存储格式

数据库保存对象路径、权限和物理引用；storage 保存原始文件、区块和区块包。storage 文件本身不能单独重建对象索引。格式编号独立于数据库 schema 和程序版本，新增格式只追加编号，不复用旧编号。

## 原始文件

local 和 S3 storage 均可关闭 profile 的 `cdc_enabled`，新写入以原始字节保存，不生成区块、存储压缩、加密或区块包。普通 PUT 和完成后的 Multipart 均对应一个不可变 UUID 文件；空对象也有完整文件和来源记录。Multipart 复用既有暂存目录、分片校验和清理，UploadPart 跳过 CDC，raw 命名空间不保存独立的客户端分片。

本地 raw 的 Multipart 向候选文件追加数据，在数据库记录分片范围；文件随实际到达数据增长，不按声明的总大小预分配。Complete 先封存候选文件，再按客户端选中的分片清单生成最终文件：连续完整布局直接复用同一文件系统上的 inode，其他布局优先尝试 Linux reflink，文件系统不支持、跨文件系统或范围未对齐时回退有界复制。布局元数据超过内存上限时沿用流式组装，可能同时保留原暂存与完整目标文件，需预留相应磁盘空间。各条路径仍核对范围、分片和完整正文的校验和，并等待文件同步与元数据发布；被替换、未选中或失败请求留下的字节不会混入最终对象。封存后的上传候选不再追加，失败后的新分片使用新的候选文件。

复制可领先持久确认两个有界批次；确认目标范围和目录后，事务更新分片映射，再释放旧候选中无引用的范围。最后一批回收交给后台 GC，待回收标记和范围映射随数据库保存，重启后继续处理；Complete 仍等待最终文件、校验和元数据发布。其他硬链接存在或文件系统不支持打洞时保留旧范围，空间不足会拒绝继续复制，已有分片仍可重试。

```text
<storage.prefix>/raw/<UUID 前两位>/<32 位小写 UUID>
<storage.prefix>/raw/<UUID 前两位>/<32 位小写 UUID>.partial
```

raw 与区块、区块包共用根目录 `meta.json` 的命名空间身份，不另建 raw 标记。本地与 S3 使用相同的最终相对路径和原始文件字节。物理路径只使用服务端 UUID，不使用桶名或对象键。`.partial` 用于本地接收和可恢复的 Multipart 组装；已确认范围可能仍被上传引用，不能手动删除，引用释放后由 GC 清理。远端使用有界 S3 multipart 传输，并记录 upload ID 供失败清理。Multipart 完成、Copy 和迁移按总长规划远端分片，从现有来源流式读取并校验，重试重新读取相同范围，不将整个分片放入内存；分片最多 10,000 个、每片不超过 5 GiB，仍受后端自身对象大小和请求超时限制。建议为远端配置 `AbortIncompleteMultipartUpload` 生命周期规则，清理创建请求已成功但响应丢失、尚无法登记 ID 的上传。

`deduplication_enabled` 默认 false。开启后按 `storage_id + 完整正文 BLAKE3 + 长度` 共享健康的最终来源，不使用 Multipart ETag 作为内容身份；不同分片边界不影响去重。`write_processing=inline` 在发布前登记或切换引用；`background` 先可靠发布独立文件和待处理记录，再计算摘要并事务切换，重启继续。后台切换保留对象版本、ETag、权限和时间，等已有读者释放保护后才执行；持续长读可能推迟去重。多余副本由 GC 回收。关闭去重仅影响新写入，已有共享文件和待处理工作保持原有合同。

`mokyu cli cache status` 的 `raw_processing` 显示待处理对象、字节和失败数量。后台失败保留来源与重试记录，不将未通过长度和摘要复核的来源登记为共享文件。后台去重减少前台计算，不减少写入确认要求，也不保证整体处理吞吐提升。

接收前先登记来源。本地在目标目录内独占创建 `.partial`，完整协议与摘要验证后同步文件、发布不可变名称并同步父目录，再提交逻辑映射；远端在完整上传确认后提交映射。覆盖创建新来源；失败不替换旧对象，未发布文件由来源记录和正常 GC 回收。原始来源保存 16 字节 MD5 摘要，普通读取先检查完整来源的长度和摘要；Range 按对象映射读取。

关闭或重新开启 CDC 只影响新写入，已有原始文件与区块格式并存。原始文件进入既有巡检、隔离、完整复检和用量统计；它们没有区块重传去重恢复，可恢复原位置的正确字节后复检，或重新上传对象。`storage rewrite` 处理区块与区块包，不隐式转换原始对象。

同一 storage 可在单容器维护状态下搬迁整个命名空间并修改连接类型；保留数据库、部署密钥、storage ID 和根 `meta.json`，同时复制历史 raw、区块与区块包。先完成或中止 Multipart、排空待写来源及后台任务，停止对原命名空间的写入；复制期间不能让两边独立写入。修改连接会核对根标记并更新 raw 来源介质，成功后须重启，旧连接不再提供数据服务。标记匹配不证明所有文件已迁齐，恢复入口前应核对完整性。

也可创建新的目标 storage 和桶，使用管理面板的跨存储复制或移动；任务按目标 CDC、去重、压缩与加密策略接收逻辑内容。该方式不修改原 storage 的连接，允许分批迁移。桶绑定的 storage ID 不变；这两种方式都不是跨数据库迁移。原始对象 Copy 先接收完整内容，再按目标去重设置选择来源；旧读取持有的文件在引用释放和保留期结束前不会回收。

## 分块和去重

- 使用 FastCDC v2020，最小 256 KiB、目标 1 MiB、最大 4 MiB；不足最小值的非空完整对象使用一个区块，空对象没有区块。
- 逻辑区块按 `storage_id + hash + raw_size` 去重，`hash` 是原始明文的 32 字节 BLAKE3。同一 storage 的不同桶共享去重，不同 storage 不共享。
- 每个物理来源独立决定压缩和加密。compression profile 支持 `never`、`always`、`sample`；区块包压缩独立记录。新策略不会隐式重写已有来源。
- 加密支持 `none`、AES-256-GCM 和 ChaCha20-Poly1305；认证标签附在密文末尾。block key 由数据库管理，key 算法创建后不可更改。

逻辑区块的 `chunks.id` 在重写、迁移和重新打包时保持不变。物理区块编码使用新的 `encoding_id`，区块包使用自己的 `pack_id`；旧来源在新来源提交后按 GC 宽限回收。

## 物理编码和完整性

每个 ready 独立来源保存 `location_id`、`standalone_size`、`standalone_compressed`、`encryption_algorithm`、`block_key_id`、`nonce_id`、nonce 和 `encoding_fingerprint`；区块包保存对应的 `pack_fingerprint`。fingerprint 绑定 key、算法、压缩和 storage profile，`storage rewrite` 用它判断是否需要处理。

加密 AAD 绑定格式编号、物理编码 ID、storage UUID、明文哈希、原始长度、编码长度、压缩标志、算法和 block key UUID。读取顺序固定为认证、解密、有界解压、长度检查和 BLAKE3 检查；任何元数据或载荷不匹配都报告完整性错误。

nonce 为 12 字节：前 4 字节编码来源类型和 UTC 日期，后 8 字节为独立的正整数 `nonce_id`。加密的物理区块编码和区块包分别使用类型标记 0 和 1；未加密来源的 `nonce_id`、nonce 和 `block_key_id` 均为空。逻辑区块、pack 成员和 raw 不单独分配 nonce。

`nonce_counters` 按 block key 和类型标记保存已预留高水位。进程在实际加密前按近期使用速率领取 64–65536 个编号，大小为 64 的倍数，数据库提交成功后才开始使用。重启、失败或取消留下的余量不回收；启动、读取、无加密写入和直接去重命中不领取范围。可用余量不足 64 时拒绝新预留，需要创建新的实际 block key 并切换关联存储；已有有效范围仍可用完。同一密钥材料不能以其他名称重新开始计数。

数据库恢复、计数回滚或独立克隆后，恢复写入前必须更换新的实际 block key，并保留历史 key 读取存量来源。已有密文使用记录中的 nonce 解密；升级不会重新编码文件。

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

local storage 不创建新区块包；已有包保持可读，显式 `storage unpack` 才会拆包。S3 storage 可由普通 pack 调度器处理符合条件的存量独立区块，也可由显式 `storage rewrite` 按当前 profile 全量重写。保存 storage 表单或载入预设都不会自动启动这些任务。

## 本地目录和缓存

单容器模式的本地目录由 `instance.data_path` 指定：

```text
<data_path>/chunks/<前两位>/<UUID>.zst
<data_path>/chunks/<前两位>/<UUID>.raw
<data_path>/raw/<前两位>/<UUID>
<data_path>/multipart/<fragment or candidate UUID>
<data_path>/node.json
```

Multipart 暂存配额按文件实际分配的磁盘块计量，稀疏空洞和已回收区间不继续占用配额；不支持物理块查询的平台保守按文件长度计量。同一文件系统上的暂存、缓存和目标后端写入共享进行中的空间预留，已落盘的部分由文件系统可用空间反映。

本地 storage 的载荷直接从本地后端读取，不进入磁盘读写缓存；为性能可关闭本地 storage 的压缩和加密。S3 storage 才使用 profile cache，损坏缓存可回源重建。待上传的唯一副本受 pin 保护，不能按普通缓存淘汰。

Linux 下，Mokyu 启动时设置 umask 为 `0022`；新建目录使用 `0755`，载荷、缓存和 Multipart 暂存等普通文件使用 `0644`。SQLite 数据库及伴随文件、节点凭据和含密钥的配置属于敏感文件，使用 `0600`；敏感临时副本也须从创建时限制权限。用户提供的配置文件需由部署者设置权限。移动、硬链接和发布保留已有权限，不重复检查或修改，也不自动调整已有文件。

普通文件可由能访问其路径的其他本地用户读取，包括通过 S3 设置为私有的内容；S3/Web 授权不限制直接访问文件系统。代理只读挂载需要的载荷目录，数据库、凭据和配置不共享。

区块的可选磁盘读缓存填充在有界后台任务中执行，成功 PUT 不等待读缓存填充；缓存额度不足或填充失败不影响已验证正文，响应完成不代表缓存已经写入。上传暂存仍按持久化要求落盘后确认。

远端 raw 的首次严格读取边校验边流式填充文件，完成后复用同一文件输出，避免为正文再完整下载一次；并发相同来源复用填充结果。缓存命中仍校验完整文件，Range 从同一文件句柄读取，不把大对象作为区块或整文件 Bytes 放入内存。raw 与 chunks 共用节点缓存额度、索引和淘汰保护；raw 缓存直接使用后端文件的 `raw_sources.id`，UUID 均为 32 位小写无连字符格式。覆盖使用新来源 UUID，去重引用共享同一缓存文件；命名不代替权限、来源状态和长度、摘要校验。后台完整性检查和来源复检直接检查后端，损坏缓存只能回源重建，不能证明后端健康。

重启恢复完整缓存文件，清理未完成填充；正在发送的文件持有 pin，实际删除后才释放缓存占用。本地 storage 的根目录不得与 data 目录中的 raw、chunks、multipart、thumbnails 命名空间重叠，符号链接按实际路径检查。

所有写入先持久化文件或远端对象，再提交数据库映射；local 后端在确认成功前同步文件和父目录，同步失败不会发布新映射。对象发布要求 stream 连续、来源 ready、长度和哈希全部匹配。跨 storage 的复制/移动由异步 `storage_migration` 任务完成复制、校验、权限和发布一致性；部署管理员负责两端容量、凭据、网络和供应商策略。
