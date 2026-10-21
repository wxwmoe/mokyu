# S3 兼容性

S3 入口默认 9000，公共读入口默认 9001。网关实现下列 S3 子集；不支持的操作和扩展会返回错误。

## 操作矩阵

| 操作 | 支持及边界 |
| --- | --- |
| HeadBucket / GetBucketLocation | 核对桶授权，区域为 runtime `requests.s3_region` |
| ListBuckets | 仅列已授权桶，支持 prefix/region 过滤和分页；max-buckets 默认 1000，范围 1～10000 |
| PutObject | 流式 CDC、校验、元数据、ACL 和条件发布；失败保留旧对象 |
| GetObject / HeadObject | 原始字节、ETag、长度、时间、元数据及请求的已有 checksum；匿名公共读可协商压缩；单段 Range 和条件请求 |
| DeleteObject / DeleteObjects | 逻辑删除，批量最多 1000 项，逐项错误／Quiet；共享块由 GC 回收 |
| ListObjectsV2 | prefix/delimiter/start-after/continuation-token/encoding-type=url；max-keys 为 0～1000，C 排序，游标绑定桶和前缀 |
| CreateMultipartUpload | 保存初始元数据、ACL 和校验模式，无需预知总大小 |
| UploadPart | 编号 1～10000，可并行、乱序、替换；校验并持久保存每个字节的来源后返回 |
| CompleteMultipartUpload | 按冻结清单组装，保存幂等结果；除末片外每片至少 5 MiB，ETag 和顺序须正确 |
| AbortMultipartUpload | 释放临时引用，与 Complete 互斥 |
| ListParts | 支持分页，每页的分片元数据一致；跨页不固定快照，不刷新过期时间 |
| ListMultipartUploads | prefix/delimiter/key+upload-id marker/URL 编码分页，最多 1000 项 |
| CopyObject | 同部署桶间复制，需源读和目标写权限；复用区块，支持 metadata COPY/REPLACE 及条件复制 |
| GetObjectAcl / PutObjectAcl | private/public-read 及相应 owner/full-control、AllUsers READ XML；不支持任意 IAM ACL |
| OPTIONS | 应用桶 CORS，不替代认证；规则通过 CLI/Web 配置 |

以下操作不支持：

| 操作或扩展 | 替代方式或限制 |
| --- | --- |
| CreateBucket / DeleteBucket | 使用 CLI |
| GetBucketCors / PutBucketCors / DeleteBucketCors | 使用 CLI/Web |
| GetBucketWebsite / PutBucketWebsite / DeleteBucketWebsite | 使用管理 Web/API |
| ListObjects V1 / UploadPartCopy | 使用 ListObjectsV2、普通上传或 CopyObject |
| Versioning、lifecycle、tagging、object lock、replication、IAM、STS | 不提供对应能力 |
| 浏览器 POST 表单上传、SSE、read partNumber、write offset、条件 Delete | 拒绝相关请求字段 |
| expected-owner、request-payer、SSE-C/SSE-KMS／复制源 SSE、标签、重定向、任意 grant、MFA、保留策略 | 拒绝扩展字段；CopyObject 也不接受更换 checksum 算法，版本 ID 请求不提供历史版本读写 |

## 寻址与权限

| 入口 | 寻址及权限 |
| --- | --- |
| S3 path-style | `https://s3.example.com/<bucket>/<key>`，默认可用 |
| S3 virtual-hosted | 在 Web/CLI 的 `requests.s3_domain` 设置中启用，受该域名限制 |
| S3 匿名 Host 映射 | Host 等于已有桶名时，GET/HEAD 读取该桶的公开对象，无需 domains 记录 |
| 公共读 | 按 CLI 配置的 Host→桶映射，只接受 GET/HEAD/OPTIONS；路径解码一次后作为完整 key |

客户端签名 region 必须等于 runtime `requests.s3_region`，与 storage profile 的后端 region 独立。反代需保留 Host、URI、签名头和 query，不改写编码路径。支持 SigV4 Authorization 和预签名读取，不启用 SigV2；签名请求核对有效期、参数和签名。

凭据按桶授权，写权限包含读权限；只读凭据不能上传、删除或改 ACL。匿名读取要求 public-read，不授予列桶或管理权限。公共读入口拒绝 Authorization 和 x-amz 签名 query，私有预签名下载使用 S3 入口。

匿名 Host 示例：桶 `media.example.com` 的 `https://media.example.com/photo.jpg` 读取 key `photo.jpg`。比较忽略 Host 大小写和端口，已配置的 S3 基域名／virtual-hosted 规则优先；私有或缺失对象均返回 403 AccessDenied，不列桶、不读 ACL、不查首页。带签名的请求仍按 S3 认证处理，不转为匿名请求。

ACL 变更对新请求生效，已接纳的读取可以完成；CDN 已缓存内容需部署者另行失效。

## CORS

每桶默认关闭，CLI/Web 保存的同一组规则应用于 S3 和公共读；字段见[CORS 设置](control-api-reference.md#cors-设置)。

- Web 预设：任意来源和请求头、暴露全部响应头，方法为 GET/HEAD/POST/PUT/DELETE/OPTIONS，预检缓存 86400 秒。
- 公共读和匿名 Host 映射仅允许 GET/HEAD 的预检；规则不启用跨域 Cookie 凭据，不改变 ACL 或签名校验。
- 成功、304 和对象错误响应均应用匹配规则；未匹配预检返回 403。开启 CORS 时带 `Vary: Origin`，预检另区分请求方法和请求头。

## 校验与上传

普通 PUT／part 的 ETag 为原始数据 MD5；multipart 为选定 part 的 MD5 组合加 `-N`，与 CDC 分块无关。内部 BLAKE3 仅用于去重。

| 项目 | 行为 |
| --- | --- |
| 单次大小 | PutObject / UploadPart 最多 5 GiB；更大对象使用 multipart，不设库存总量上限 |
| 校验 | Content-MD5、实际 payload SHA256、声明的 checksum/trailer；缺失、错误或矛盾时不发布 |
| 算法 | CRC32/CRC32C/CRC64NVME/SHA1/SHA256，以及 s3s 提供的 SHA512/MD5/XXHASH64/XXHASH3/XXHASH128；SDK 能否发送取决于其支持范围 |
| COMPOSITE | part 从 1 连续编号，最终 checksum 保留类型／组合后缀 |
| AWS chunked | 支持数据上传的签名 chunk 和声明 trailer；单个传输 chunk 受 runtime `requests.s3_chunk_limit` 限制 |
| 控制请求 | XML 最多 2 MiB，不接受 aws-chunked 编码 |

Complete 在发送响应头前检查授权、清单和分片，工作额度不足返回 `503 SlowDown`、`Retry-After: 60` 和 `Cache-Control: no-store`。开始处理后返回 HTTP 200，每 5 秒发送 XML 空白保活，最多等待 5 分钟；超时在 XML 正文返回 `InternalError` 和 RequestId，客户端必须检查完整正文，不能仅凭 HTTP 200 判断成功。响应设置 `X-Accel-Buffering: no`，反向代理仍需允许长响应。

HTTP 超时或客户端断开不取消已接受的完成工作。相同 UploadId 和清单重试会加入执行或返回已保存结果；另一清单不能替换正在执行的清单。完成工作使用独立有界额度，等待群集所选来源远端就绪期间不占前台上传或正文内存槽位。进程退出后，相同请求可在执行租约到期后恢复，过期执行者不能发布。群集成功响应要求所选分片及最终对象来源已经确认写入后端；仅存于其他节点本地缓存的文件不可由当前节点恢复。

传输错误、长度不符或超时使请求失败并释放未发布来源，不会把短文件当作成功上传。multipart 恢复和发布规则见[存储格式](storage-format.md#分片上传与发布)。

## Range 与条件请求

GetObject、公共读和管理下载支持单段 `bytes=a-b`、`bytes=a-`、`bytes=-n`，不支持多段 multipart/byteranges。只读取覆盖范围的区块，每块完整认证，大范围仍流式返回；无效范围返回 416 和 `Content-Range: bytes */<size>`。

支持 If-Match、If-None-Match、If-Modified-Since、If-Unmodified-Since；修改日期按秒比较，条件请求可返回 304/412。GET／公共读的 If-Range 只接受匹配的强 ETag；弱标签、日期或不匹配的标签返回完整表示。对象可能在同一秒内多次覆盖，因此 Last-Modified 不能用作强范围验证器。S3 HeadObject 的 Range 仍返回 200，Content-Length 为区间长度；公共 HEAD 可返回 206。对象保存的 Content-Type、Cache-Control 等元数据随响应返回。

HEAD 和 304 只读取当前对象元数据，不读取区块、不占正文读取名额，也不创建群集读取租约。条件请求需要正文时，服务重新固定当前版本并检查权限和条件，再发送响应；完整读取从起点直接分页，非零 Range 才查询所在区块的起点。

后续区块损坏可能使响应中止，已发出的前部区块仍经过校验；客户端须核对 Content-Length 和响应完整性。

## 公共响应压缩

公共 Web 与通过匿名认证的 S3 GetObject/HeadObject 按 Accept-Encoding 的 q 值协商，同权重时优先 zstd、br、gzip、identity；未发送或空值返回原始表示。签名与预签名请求、checksum mode、已有 Content-Encoding 和请求或对象的 no-transform 均不追加编码。无可接受表示返回 406；禁止 identity 且编码工作内存不足时返回 503。

原始表示使用强 ETag，压缩表示使用同值弱 ETag。所有可协商响应发送 Vary: Accept-Encoding；S3 另包含 Authorization 和 X-Amz-Checksum-Mode。强 If-Match 和有效 Range 优先原始表示；206 不压缩，identity 被明确禁止时忽略 Range 并返回完整压缩表示。条件判断针对最终选择的表示，If-None-Match 使用弱比较。

压缩 HEAD 不启动编码器，没有实际压缩正文时省略 Content-Length；304 保留 ETag、Vary 和缓存策略，省略 Content-Encoding 和长度，避免在重验证时错误标记浏览器已有正文。S3 response-* 覆盖仍生效，并参与压缩排除判断。

启用完整响应缓存时，匿名公共 GET 可复用已验证的原文或对应编码正文，压缩 GET/HEAD 只从实际缓存正文取得长度，不从历史统计推测。每次请求仍检查当前对象和权限，正文命中另核对最多 64 条连续 ready 映射；大文件、更多映射、取消或超出内存额度的请求继续使用流式路径。签名、校验和、private/no-store 响应不进入该缓存，Range 只复用原文切片。

对象发布时根据完整独立区块的存储大小预估收益；部分引用、pack 成员或未知大小按原始长度计入，不在 GET/HEAD 扫描全部映射。小于 1 KiB 的对象不追加压缩；初次尝试要求预计节省至少 128 字节且达到 2%。无摘要的历史对象仅对不超过 8 MiB 的文本类内容保守尝试。完整编码后异步保存各算法的收益，不更新文件 ETag、修改时间或目录版本；失败、取消与仅元数据请求不学习。

## 公共网站

仅 `instance.listen_web` 按桶网站设置执行以下路由，S3 入口不执行：

1. 精确对象优先；私有对象返回 403，不回退。
2. 缺失的 `/` 或 `/dir/` 查找该目录的 index_document，支持 HEAD、Range 和条件请求。
3. `/dir` 无同名对象但有公开首页时，308 跳转至 `/dir/`，保留 query。
4. 对象／首页不存在时，使用桶根相对 error_document 的公开内容；仍返回 404、Cache-Control: no-store，忽略 Range 和 If-*。
5. 错误页缺失、私有或配置为空时使用内置 404，不递归回退。

数据库、权限和存储故障不伪装成 404；未知 Host 不返回其他桶内容。配置入口见[网站设置](control-api-reference.md#网站设置)。

## 请求标识

进入应用的 HTTP 请求生成 UUIDv4，不信任客户端传入的同名 ID。S3 返回 `x-amz-request-id` 和 `X-Request-ID`，XML 错误体的 RequestId 与头部一致；公共读和管理端返回 X-Request-ID。HEAD 无错误体，Complete 在 HTTP 200 后产生的延迟 XML 错误也携带 ID 并计入失败。

请求 ID 属于网关，与存储提供商 ID 独立，不生成 HostId/CMReferenceId。重试产生新 ID；代理提前拒绝的请求没有网关 ID，CDN 命中可能返回原回源 ID。跨域脚本读取时需在 CORS expose 中加入相应头名。

日志以 request_id/listener/method 关联处理及响应流。失败／中断为 WARN，成功为 DEBUG，可设置：

```sh
RUST_LOG=mokyu=info,mokyu::stats=debug,s3s=warn
```

访问日志和统计标签不记录对象键、签名参数、凭据或 Cookie；管理页在 API 失败时显示 RequestId。

协议参考：[HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)、[CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)、[对象完整性](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html)。
