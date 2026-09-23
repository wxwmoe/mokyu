# S3 网关兼容性（0.0.2）

## 请求标识

网关为每个进入应用层的 HTTP 请求生成新的 UUIDv4。S3 响应始终带 `x-amz-request-id` 和 `X-Request-ID`，XML 错误体的 `RequestId` 与响应头一致；HEAD 没有错误响应体。CompleteMultipartUpload 在 HTTP 200 之后返回的延迟 XML 错误也包含相同的 RequestId，并计入失败请求。成功请求、鉴权失败、预检和匿名公共读取均带标识。公共读和管理端口返回 `X-Request-ID`。客户端传来的同名头不会成为网关的请求标识；重试会生成新的 ID。

日志的 `request_id`、`listener`、`method` 贯穿处理过程及响应流；失败/中断在 WARN 级别记录状态、已产生字节数和耗时。成功完成记录在 DEBUG，可用 `RUST_LOG=media_gateway=info,media_gateway::stats=debug,s3s=warn` 开启。管理页面在 API 请求失败时显示响应头中的 ID。对象键、签名参数、凭据和 Cookie 不进入这些访问日志或统计标签。代理在请求进入网关前产生的错误没有网关 ID；CDN 缓存命中可能返回原始回源请求的 ID。

不生成 Wasabi 的 HostId/CMReferenceId。此 ID 属于网关请求，不代表后端供应商请求；后端错误通过同一个网关请求日志上下文排查。跨域 JavaScript 需要读取 ID 时，在桶 CORS 的 expose 中加入 `x-amz-request-id`、`x-request-id`；已有规则保持原样。

S3 网关默认端口 9000，web 网关默认端口 9001

非完整 AWS S3 / IAM 实现，未支持的操作返回错误

## 操作矩阵

| 操作 | 支持及边界 |
| --- | --- |
| HeadBucket / GetBucketLocation | 按凭据授权核对桶；区域为listen.region |
| ListBuckets | 仅返回已授权桶；prefix/region过滤与分页；max-buckets默认1000，1～10000 |
| PutObject | 有界流式CDC、校验、元数据、ACL、条件发布；失败保留旧对象 |
| GetObject / HeadObject | 原始字节、ETag、长度、时间、元数据、请求的已有checksum；单段Range和条件请求 |
| DeleteObject / DeleteObjects | 逻辑删除，最多1000项批删，逐条返回错误/Quiet；不立即删除共享区块 |
| ListObjectsV2 | prefix/delimiter/start-after/continuation-token/encoding-type=url；max-keys0～1000，C排序，桶+前缀绑定游标 |
| CreateMultipartUpload | 冻结初始元数据/ACL/校验模式；不要求预知文件总大小 |
| UploadPart | 编号1～10000，并行乱序/替换；每片完成校验及来源持久化后返回 |
| CompleteMultipartUpload | 冻结清单、整文件CDC、结果幂等保存；除末片外每片至少5MiB，ETag和顺序必须正确 |
| AbortMultipartUpload | 释放临时引用；与Complete同上传锁协调 |
| ListParts | 分页，读取不刷新上传过期时间 |
| ListMultipartUploads | prefix/delimiter/key+upload-id marker/URL编码分页，最多1000项 |
| CopyObject | 同部署逻辑桶之间、需要源读和目标写权限；引用复用，metadata COPY/REPLACE及条件复制 |
| GetObjectAcl / PutObjectAcl | private / public-read；兼容相应owner/full-control与AllUsers READ XML表达，不支持任意IAM ACL |
| CORS / OPTIONS | CLI或管理Web配置桶规则，S3和公共端口应用相同规则，不替代认证；不提供S3 GetBucketCors/PutBucketCors/DeleteBucketCors操作 |
| CreateBucket / DeleteBucket | 不支持；使用CLI bucket命令 |
| GetBucketWebsite / PutBucketWebsite / DeleteBucketWebsite | 不支持；网站设置使用管理Web及其API |
| ListObjects（V1）、UploadPartCopy | 不支持；使用ListObjectsV2、普通上传/CopyObject |
| Versioning / lifecycle / tagging / object lock / replication / IAM / STS | 不支持 |
| 浏览器POST表单上传、SSE请求、read partNumber、write offset、条件Delete | 不支持；拒绝相关字段 |

不支持 S3 的 expected-owner、request-payer、SSE-C/SSE-KMS/复制源SSE、对象标签/重定向、任意grant头、MFA、保留策略等扩展。CopyObject要求改checksum算法也拒绝；不能把未处理字段当已生效。版本ID请求不提供版本读写。

## 寻址、认证和权限

默认path-style：`https://s3.example.com/<bucket>/<key>`。设置listen.s3_domain后，可使用受配置域名约束的virtual-hosted寻址；反代必须保留原Host、请求URI、签名头与query，不能改写编码路径。客户端签名region须等于listen.region，与后端region无关。

S3端口另支持匿名GET/HEAD的Host等于现有逻辑桶名：例如桶`media.example.com`的`https://media.example.com/photo.jpg`读取该桶的`photo.jpg`对象，不需要domains记录或HTTP重定向。Host比较忽略大小写和端口；已配置的S3基域名/virtual-hosted规则优先。仍执行S3的Range、条件请求和public-read检查；私有/缺失对象均为403 AccessDenied，不能匿名列桶或读ACL。根路径不会自动查找index.html。带Authorization或签名query的请求仍按原S3规则核验，绝不改写成匿名请求；此简便映射仅用于公开读取。

CORS默认规则为`[]`（关闭）。管理页可添加多条规则或填入Wasabi风格预设：任意来源和请求头、暴露全部响应头、GET/HEAD/POST/PUT/DELETE/OPTIONS、预检缓存86400秒；不声明未实现的MOVE。保存后两端口共用，公共读端口和Host等于桶名的入口只接受GET/HEAD预检。规则不启用cookie跨域凭据，不改变ACL或签名校验。成功、304及对象错误响应均应用匹配规则；未匹配的预检返回403。开启规则时响应携带Vary: Origin，预检额外区分请求方法及请求头。

SigV4 Authorization与预签名读取受支持，SigV2不启用。S3凭据由CLI生成并按桶授权，写权限包含读权限；只读凭据不能上传、删除、改ACL。公共ACL不授予列桶/管理权限。预签名与普通签名都核对有效期、请求参数及签名。

S3匿名读取只允许public-read对象；独立公共读只接受GET/HEAD/OPTIONS。公共端口严格按Host表找桶，URL路径解码一次作为完整key，不暴露桶列表或API管理。该入口拒绝Authorization及x-amz签名query；私有预签名下载使用S3入口。ACL变更在新请求生效，已接纳的固定版本响应可以完成。外部CDN已缓存内容须由部署者单独失效，网关无法撤回已缓存响应。

## 校验、ETag 和流式请求

普通PUT/part ETag为原始数据MD5；multipart ETag为各选定part MD5的组合值加`-N`，与最终CDC分块无关。内部去重仍用BLAKE3。支持Content-MD5、实际payload SHA256及声明的checksum/trailer校验；缺失、错误、矛盾的校验不发布对象。

单次PutObject和UploadPart接收上限为5GiB；更大对象使用multipart。该限制是请求层协议边界，不是库存总量上限。

checksum实现包括CRC32/CRC32C/CRC64NVME/SHA1/SHA256及s3s提供的SHA512/MD5/XXHASH64/XXHASH3/XXHASH128；客户端SDK是否能发送取决于其版本。COMPOSITE模式要求从1连续编号；返回最终对象checksum时保留相应类型/组合后缀。

支持数据上传的AWS chunked签名/声明trailer；单个传输chunk受listen.aws_chunk_limit约束，并非单文件上限。控制XML最多2MiB，控制请求上的aws-chunked编码明确拒绝。传输错误、长度不符或超时会失败并释放未发布来源，不能当作短文件成功。

## HTTP 行为

公共`listen.web`可按桶开启网站路由。精确对象始终优先（私有对象返回403，不回退）；缺失的`/`、`/dir/`查找该目录的index_document。`/dir`无同名对象且存在公开首页时308跳转到`/dir/`并保留query。首页也支持HEAD、Range和条件请求。S3端口不执行网站路由。

未找到对象/首页时，可返回桶根相对error_document的公开内容，HTTP仍为404、Cache-Control为no-store，并忽略原请求的Range及If-*条件。错误页缺失、私有或设置为空则返回内置404；不会递归回退。数据库错误、权限错误和后端故障不伪装成404。未知公共Host不会返回其他桶的网站内容。

GetObject和公共/管理下载支持单段`bytes=a-b`、`bytes=a-`及`bytes=-n`；不提供多段multipart/byteranges。Range只取覆盖的区块，每块完整认证。大范围仍流式。无效范围416并返回`Content-Range: bytes */<size>`。

支持If-Match/If-None-Match/If-Modified-Since/If-Unmodified-Since；GET/Public提供If-Range。HTTP日期按秒比较。304/412/416不伪造下载成功。S3 HeadObject Range按S3语义仍为200，Content-Length为请求区间长度，公共HEAD按HTTP Range可206。元数据中的content-type/cache-control等随对象保存并返回。

读取后续区块坏掉可导致已开始的响应中止；已经发送的前部块仍是完整认证的数据。客户端须核对Content-Length/完整响应，不能把断流当作完整文件。

S3请求语义参考：[HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)、[CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)、[对象完整性](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html)。
