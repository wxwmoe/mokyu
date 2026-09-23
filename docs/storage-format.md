# 存储格式（版本1）

元数据保存在 PostgreSQL，后端文件只有编码载荷，单凭后端桶不能恢复对象路径或权限

## 分块、去重与编码

原始文件按 FastCDC v2020、默认 Normalization Level1/seed0 分块：min262144、avg1048576、max4194304字节。对连续最多4MiB窗口执行实际切点，消费切点前的数据，窗口末尾不是人为EOF。尾部只有在确认文件EOF时作为最终块；不足256KiB的非空完整对象一块，空对象零块。

原始区块的完整32字节BLAKE3是去重哈希。去重域为此部署全部逻辑桶，索引同时包含raw_size、algorithm、key_id，不能跨密钥/编码模式错误复用。对象ETag/S3校验另算，不用BLAKE3代替协议ETag。

每块先尝试zstd level3；只有编码结果比原始字节短才使用，数据库compressed标识实际选择。随后按none/AES-256-GCM/ChaCha20-Poly1305编码；加密方案密钥均32字节、tag16字节，tag附加在密文末尾。none仍校验原始长度与BLAKE3，但不提供密钥认证/保密。

## nonce 与 AAD

加密前提交chunks行，取得正bigint ID，再加密一次。nonce为12字节：`u32(UTC YYYYMMDD).to_be_bytes() || u64(chunk_id).to_be_bytes()`，日期取区块created_at。日期是标识，唯一性来自密钥域内不回滚复用的ID；时钟回退不会抵消ID唯一性。

例如 `2026-09-21T00:00:00Z`、ID42：nonce十六进制 `01352839000000000000002a`。日期以整数编码，不是8字节ASCII。读取直接使用数据库保存的nonce。

AAD逐字段拼接，无JSON、分隔符或平台本机端序：

| 次序 | 长度 | 字节 |
| --- | --- | --- |
| 1 | 9 | ASCII `MGWCHUNK` + `01` |
| 2 | 8 | chunk ID，i64 big-endian（正值） |
| 3 | 16 | storage UUID原始字节 |
| 4 | 32 | 原始BLAKE3 |
| 5 | 4 | raw_size，i32 big-endian |
| 6 | 4 | stored_size（含tag），i32 big-endian |
| 7 | 1 | compressed：0/1 |
| 8 | 1 | 算法：none0、AES-GCM1、ChaCha2 |
| 9 | 2 | key_id UTF-8字节长度，u16 big-endian |
| 10 | 可变 | key_id UTF-8字节 |

完整块先认证再解压，解压输出不得超过raw_size，最后核对长度与BLAKE3。物理ID、大小、压缩、算法及密钥ID被AAD绑定，不能互换区块元数据。

数据库恢复、序列回滚、独立克隆会使旧ID再次出现。恢复写入前必须生成**新的实际区块写密钥**；改日期、只改key ID、只提高一个估计序列值都不能代替。旧密钥保留作读取。后端PUT结果不明时可以对同一物理key重传完全相同的编码字节；再次加密必须分配新ID和新storage_id。

## 物理文件与缓存

后端key：`<backend.prefix>/chunks/<UUID前两位>/<storage_uuid_simple>`，UUID为32位小写十六进制；例如`chunks/08/084f2ff912ff4c6daef1b416fee7b800`。每个物理key不可变，删除后也不复用。分成256个前缀用于组织与分批维护，不承诺特定S3服务商的性能收益。sweep只识别分组与UUID一致的规范路径，其他对象计入unrecognized且不删除。

同级`<backend.prefix>/meta.json`保存`format_version=1`、与数据库一致的`deployment_id`、`chunk_layout="uuid-prefix2"`、`created_at`和仅供诊断的`created_by="wxw-media-gateway/0.0.2"`（来自Cargo包名和版本）。已有正确身份的marker不会仅为更新诊断字段而重写。存储格式版本独立于软件版本及数据库schema版本；区块AEAD格式仍为1。

省略`backend.prefix`或设为`""`时，后端key直接为`meta.json`和`chunks/<UUID前两位>/<storage_uuid_simple>`，没有前导斜杠。首次初始化要求整个后端桶为空；非空前缀只检查该命名空间。前缀属于数据库绑定的后端身份，已有部署修改前缀会拒绝启动，不会自动搬运数据。

本地`data/chunks`缓存保存与远端对象逐字节相同的最终区块（按配置压缩和加密后的字节，含AEAD认证标签）。缓存命中后在有界内存里校验、解密和解压；不会将解密但未解压的中间数据写入该缓存。`none`模式缓存的也是最终存储字节，只是不加密。

每次启动在恢复、监听及GC之前校验标识，限制16KiB；身份冲突、未知格式、损坏或已初始化后丢失均拒绝启动。仅未初始化且后端专用前缀为空、数据库无对象/区块时，用create-only条件写创建标识。S3写成功而DB确认前中断，重启可使用数据库已提交的deployment_id完成确认。后端必须支持If-None-Match条件创建，失败不降级为覆盖写。

标识不保存密钥、对象索引或引用计数；不属于chunks扫描范围。它不是数据库备份或跨数据库写锁，克隆/恢复仍须遵守独占部署与更换实际写密钥规则。数据库迁移不会自动转换后端格式；涉及格式升级时需遵循对应版本的升级说明。

本地 `data/chunks/<前两位>/<同一32位UUID>` 保存同样的编码字节。填充先写临时文件再改名；预留和实际占用统一计量。S3-FIFO使用small/main队列（初始10%/90%目标）、0～3饱和频率和有界ghost ID；不缓存明文。缓存缺失/坏块可从后端重建；无空间填充时可直接解码读出，不破坏multipart唯一来源。

`data/multipart/<32位小写十六进制fragment UUID>` 为仍需恢复的原始片段文件；先写文件、fsync文件及父目录，再提交sealed/extent映射。此目录不等同于缓存：仍有引用的尾部不能因“旧”或缓存压力擅自删除。Abort/过期后通过引用清理释放。

## multipart 与发布

每次UploadPart生成不可变stream版本，parts.write_epoch防止旧请求迟到替换；新版本确认前旧part仍可用。已接收的连续数据可以CDC/上传后端；缺口之后也可产生候选区块，前后尚不确定的局部范围由extents引用远端候选或本地fragment。

UploadPart成功表示每个字节都有已持久化的来源，不表示全片已经形成最终整文件CDC区块。相邻part可接续处理、释放原始尾部。Complete按客户端冻结的有序清单流式重建连续字节并执行同一CDC，候选块在此之前仍受引用保护。无需整文件拼接落盘或一次加载全部映射。

发布事务核对：全部来源是ready区块、offset连续、总长一致；随后原子切换objects.stream_id、退役旧generation并保存Complete结果。GET固定当前版本，并持有活跃保护直到响应结束。删除对象仅解除可见指针，不会立刻删除共享区块。

密钥指纹、引用和删除日志是恢复与回收的必要状态，不可手工删除它们来绕过启动检查。
