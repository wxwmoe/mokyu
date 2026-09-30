# 管理页面与 API

管理端口默认 9002。部署管理员可访问全部桶和私有对象；普通成员仅能访问获授权的项目与桶。管理员由 [CLI](cli-reference.md#凭据与用户) 或首次安装引导创建，无默认账户。

导航：[会话](#会话) · [存储桶](#存储桶) · [对象](#对象) · [运行状态](#运行状态) · [后台任务](#后台任务) · [完整性巡检](#完整性巡检) · [页面](#管理页面)

## 通用约定

- 除登录、安装引导、`GET /api/info` 和 `GET /api/openapi.json` 外，API 需有效会话 Cookie 或 `Authorization: Bearer TOKEN`。Cookie 写请求要求 `Origin` 精确匹配 `manage.origin` 和 `X-CSRF-Token`；登录及安装引导要求 Origin。Bearer 不使用 CSRF，不能与 Cookie 混用或放入 URL。桶 CORS 不作用于管理端口。
- Cookie 为 `mokyu_session`，HttpOnly、SameSite=Strict；Secure 和固定有效期由[配置](configuration.md#监听与管理)决定。自行改密保留当前会话、撤销其余会话；CLI 重置密码、禁用或删除用户撤销全部会话。
- JSON 请求体上限 16 KiB，批量对象操作另有说明。GET 路由也接受 HEAD，HEAD 不返回响应体。
- 查询参数按 UTF-8 编码；key 原样保留，不规范化斜杠、空格或路径。分页 token 不应解析或跨范围复用；并发变更期间不提供跨请求快照。
- 响应使用 `Cache-Control: private, no-store`，下载另带 `Vary: Cookie`；页面 CSP 限制外部脚本、插件和被嵌入。失败可用响应头 `X-Request-ID` 排查，见[请求标识](s3-compatibility.md#请求标识)。

错误统一返回 `{error,code,request_id}`：error 为标准 HTTP 原因，code 为稳定错误标识，request_id 与响应头一致；不会回显密码、数据库错误或请求正文。调用方同时检查 HTTP 状态和 code。

`GET /api/info` 返回产品、程序版本与管理契约标识。`GET /api/openapi.json` 提供从 Rust 类型和路由生成的契约，目前覆盖账户、会话、安装引导、状态、桶列表与网站设置；其余接口以本文为准。可使用 `mokyu api-schema` 离线导出同一文档，无需配置、数据库或运行服务。

| 状态 | 常见原因 |
| --- | --- |
| 400 / 422 | 参数、查询或 JSON 无效 |
| 403 | 会话、Origin 或 CSRF 校验失败 |
| 404 | 桶、对象或任务不存在 |
| 409 | 状态冲突、桶正在清除或不允许的任务操作 |
| 412 | 对象版本已变更或条件不满足 |
| 413 | 请求体超限 |
| 503 / 500 | 繁忙、维护限制 / 内部错误 |

## 会话

| 方法与路径 | 输入 | 成功响应 |
| --- | --- | --- |
| `POST /api/login` | `{username,password}`，需 Origin | 200 `{csrf_token}`，设置 Cookie |
| `POST /api/logout` | 无 | 204，撤销会话并清除 Cookie |
| `GET /api/session` | 无 | 200 个人资料及 `csrf_token` |
| `GET /api/me` | 无 | 200 个人资料 |
| `PUT /api/me` | `{display_name,locale,theme,avatar_email,avatar_enabled}` | 200 保存后的个人资料 |
| `POST /api/me/password` | `{current_password,new_password}` | 204，保留当前会话 |
| `POST /api/me/reauth` | `{password}` | 204，更新近期验证时间 |
| `GET /api/me/sessions` | 无 | 200 `{sessions,more}`，最多 100 个有效会话 |
| `DELETE /api/me/sessions/{id}` | 会话 UUID | 204，仅能撤销自己的会话 |
| `DELETE /api/me/sessions` | 无 | 204，撤销其余会话 |

新标签页和刷新后可通过 session 取得 CSRF token。密码哈希与验证共享最多两个并行任务，失败不泄露用户名是否存在。改密、重置、撤销和登录在提交前重新校验用户与会话状态。

个人资料包含 `id,username,role,project_management,must_change_password,display_name,locale,theme,avatar_email,avatar_enabled,avatar_url`。display_name 最多 240 UTF-8 字节；avatar_email 最多 320 字节，开启头像时必填。locale 为 `en/zh-CN/ja/null`，theme 为 `auto/light/dark/null`；null 表示跟随浏览器。头像默认关闭，启用后返回规范化邮箱的 SHA-256 Gravatar URL，不上传图片。

会话条目为 `{id,created_at,last_seen_at,expires_at,user_agent,current}`，按创建时间倒序；不返回认证凭据。last_seen_at 最多每五分钟刷新一次，不延长会话寿命。新密码为 12～1024 UTF-8 字节。

### 首次安装

`GET /api/bootstrap` 返回 `{setup_required}`。只有数据库没有任何用户时，服务才在管理 socket 旁写入权限 0600 的 `setup-token`，默认 `/run/mokyu/setup-token`。`POST /api/setup` 接收 `{token,username,password}` 并返回 201；成功后令牌失效并删除文件。并发初始化只允许一次成功；CLI 创建首个用户也会关闭此入口。令牌不得放入 URL。

## 应用密钥与 API Token

S3 应用密钥属于项目，与创建者账号生命周期无关。个人管理 Token 存哈希，其逐桶权限与用户当前权限取交集；改密、重置密码、禁用或角色变更使已签发 Token 失效。两者均有过期时间和最近使用时间（最多每五分钟写入一次），secret 仅创建或轮换时返回。

应用密钥仅管理员管理。Token 创建、编辑、查询自己的条目；管理员还可查询和撤销其他用户的 Token。这些 Token 管理接口，以及 `/api/session`、`/api/logout`、`/api/me` 与其子路径，只接受 Cookie。写操作需近期密码验证。

| 方法与路径 | 输入与行为 |
| --- | --- |
| `GET /api/credentials` | `project?,after?,limit?`，limit 1～100；返回 `{credentials,next}` |
| `POST /api/credentials` | `{project_id,label,expires_in?,grants}`；返回 201 `{access_key,secret_key,credential}` |
| `PUT /api/credentials/{key}` | `{label,enabled,expires_in?,keep_expiry?,grants?}`；原子更新元数据及可选授权 |
| `PUT /api/credentials/{key}/grants` | 完整替换 `[{bucket_id,actions}]`，所有桶必须属于密钥项目 |
| `POST /api/credentials/{key}/rotate` | `{overlap,expires_in?}`；新 ID/secret 与原权限，旧密钥在重叠期或原到期时间的较早者失效 |
| `DELETE /api/credentials/{key}` | 删除应用密钥；204 |
| `GET /api/tokens` | `user?,after?,limit?`，默认自己；limit 1～100；返回 `{tokens,next}` |
| `POST /api/tokens` | `{label,system?,expires_in?,grants?}`；返回 201 `{token,secret}` |
| `PUT /api/tokens/{id}` | 同创建输入，另支持 `keep_expiry`；仅可修改自己未撤销的 Token |
| `DELETE /api/tokens/{id}` | 永久撤销；204 |

label 为 1～128 字节；grants 最多 1000 个不同桶，actions 使用项目授权动作。空授权不授予桶权限。`system=true` 仅管理员可选择，grants 必须为空，允许完整实例管理，包括全部桶和用户；仍不能调用 Cookie 专属入口。普通 Token 不能访问实例任务、全局统计或用户管理。系统 Token 无需密码重验证，所有可写路径仍重新验证其有效性。

`expires_in` 接受 `30d` 等时长，范围大于零且不超过 3650 天；应用密钥省略/null 表示不过期，Token 省略默认 90 天、null 表示不过期。更新时 `keep_expiry=true,expires_in=null` 保留原时间；轮换 overlap 为 0～30 天。密钥与 Token 正文上限 512 KiB。撤销或缩减权限会阻止尚未提交的写操作，已获授权的读取正文可继续完成。

## 存储额度

`GET /api/quotas/{kind}/{id}` 读取 project 或 bucket 的账本；`PUT` 完整替换 `{byte_limit,inflight_limit,bucket_limit}`，需要管理员及近期验证。值为非负十进制字符串或 null，null 不限、`"0"` 为零；桶仅支持 byte_limit。读取返回同名上限及 `used_bytes,reserved_bytes,inflight_bytes,object_count,bucket_count` 字符串。

项目按当前可见对象的原始字节计费，去重和压缩不抵扣额度。接收前预留、覆盖扣抵旧版本、完成时转为已用；在途预算另外包含未完成分片与并发覆盖。低于当前用量的新上限不会删文件，仍允许读取、删除和不增加用量的替换。超过上限返回 HTTP 403、`QuotaExceeded`。

桶账本需要 storage.inspect；项目成员可以读取共享上限，只有拥有全项目范围的成员可以读取项目汇总，其他成员的汇总字段为 null。受限 Token 只可读取获授权的桶账本。

## 媒体目录与检索

`GET /api/buckets/{bucket}/objects` 需要 bucket.list，返回 `{objects,prefixes,next,layout,search_mode,index_ready}`。对象摘要包含 `id`（版本 UUID）、`object_key,size,content_type,public_read,modified_at`；size 为十进制字符串。搜索限定在当前桶，不能跨授权范围。

| 参数 | 含义 |
| --- | --- |
| prefix / recursive | 当前路径前缀；recursive=true 展开子目录 |
| q / mode / search_in | 搜索词；mode=contains（默认）/prefix/exact；包含搜索的 search_in=name（默认）/path |
| kind / public | image/video/audio/document/archive/other；公开读取 true/false |
| min_size / max_size | 原始字节范围，非负十进制字符串，包含边界 |
| since / until | 更新时刻范围，RFC3339，包含边界 |
| sort / order | name（默认）/size/modified；asc（默认）/desc |
| after / limit | 上页 next；limit 1～200，默认 100 |

前缀与完整路径区分大小写，q 接在 prefix 后；包含搜索不区分大小写，通配符按普通字符处理。无法提取连续三个文字/数字的包含搜索改为路径前缀匹配，响应 search_mode 明确为 prefix。类型来自 MIME 与扩展名，只用于整理显示，不是内容安全判断。

无筛选的名称升序使用虚拟目录；其他组合返回平面文件列表，不推算文件夹大小。游标绑定桶和筛选排序参数，变更参数后从第一页开始；并发写入期间不是跨请求快照。检索查询限制为 2 秒，超时要求缩小范围或重试。

`GET /api/buckets/{bucket}/catalog` 返回索引阶段 indexes/backfill/ready。只有管理员能看到全局 scanned/current_index/last_error，其余用户这些字段为 null。索引未就绪仍可浏览目录、按完整路径或前缀检索；其他条件返回 503 CatalogBuilding。

目录摘要与发布、覆盖、ACL 变更及删除同事务更新。升级后逐批回填旧对象并并发建索引；中断后自动继续，不在启动迁移里重建整个库存。管理状态和 `cli status` 包含 catalog 进度。

## 上传与传输中心

浏览器上传复用 S3 分片的接收、额度和发布流程，不向浏览器签发 S3 secret。默认每片 16 MiB，大文件自动增大片段以满足最多 10000 片和每片最多 5 GiB 的协议限制。所有字节数用十进制字符串。

| 方法与路径 | 输入与行为 |
| --- | --- |
| `POST /api/buckets/{bucket}/uploads` | `{client_id,key,file_name,size,modified_at?,content_type?,public_read?,overwrite?}`；201 上传记录。client_id 为客户端 UUID，同用户同参数重复调用返回同一上传 |
| `GET /api/uploads` | `bucket?,state?,source?,own?,after?,limit?`；返回 `{uploads,next}`。默认 active/completing，state 支持 all/active/completing/completed/aborted；source=web/s3；limit 1～100，默认 50 |
| `GET /api/uploads/{id}` | 上传详情，含 received_bytes、expected_size、part_size、parts、expires_at、can_resume、remote_state |
| `GET /api/uploads/{id}/parts` | after 为分片号；每页最多 1000 项，返回 `{parts:[{number,size,etag,sha256}],next}` |
| `PUT /api/uploads/{id}/parts/{number}` | 原始字节正文、`X-Content-SHA256` 为 Base64 SHA-256；必须符合该分片的预期长度。返回 `{number,etag,sha256}` |
| `POST /api/uploads/{id}/complete` | `{parts:[{number,etag,sha256}]}`，连续完整清单；正文最多 2 MiB。等待发布完成后返回上传记录；相同清单可重试 |
| `DELETE /api/uploads/{id}` | 终止未完成上传，204；不存在/已结束可能返回状态错误 |

接收与续传仅限发起用户且须保留当前 object.write 权限；受限 Token 另受其动作范围约束。管理员或同时具有 object.write/bucket.settings 的成员可查看和终止其他上传，不能替他人续传。公开上传另需 object.acl。覆盖默认关闭；即使启用，完成时仍比较开始时的对象版本，发生变化返回 412。

`state=completed` 表示对象已发布。remote_state=stored 表示观察到其后端副本就绪，pending 表示仍在等待，unavailable 表示原输出流已不再跟踪；不能用“已接收”代替远端持久化确认。上传空闲期限沿用 multipart.idle_timeout；暂停浏览器不延长期限。

传输中心每个标签页同时发送最多两个分片，文件依次处理。暂停让当前分片收尾；刷新或关闭后需重新选择原文件，续传前在 worker 中逐一校验所有已接收分片。尚未接收的部分没有预存指纹，须继续选择同一份原文件。网络繁忙时分片有限重试；没有后台浏览器上传或永久保存文件权限。

## 活动审计

`GET /api/audit` 返回 `{events,next}`；`GET /api/audit/{id}` 返回完整详情。支持 `after,limit,actor,action,source,outcome,project,bucket,since,until`：ID 为十进制字符串，after 使用上页 next；limit 默认 50、范围 1～200；actor 匹配账号名称，action 匹配动作前缀；时间为带时区的 RFC3339。source 为 web/token/cli，outcome 为 succeeded/failed/partial/unknown。

管理员查看全局记录。普通用户只可查看自己的账号/Token 活动、无敏感详情的失败记录及当前可列举桶内的个人操作；降级后不再显示原管理员权限下的全局管理详情。受限管理 Token 不可访问审计，完整系统 Token 可访问。删除账号后保留当时的账号标签与 UUID，不级联删除历史。

管理写请求先持久记录意图；用户、项目授权、凭据、Token 和账户安全变更在业务事务中保存结果。其他入口记录请求结果；中断而未记录结果显示 unknown，不应推断成功或失败。普通 S3 数据读写不逐条写审计。已知账号的错误密码每分钟最多保留一条，标记身份尚未确认，不保存未知账号的尝试。

详情仅收录显式选择的业务字段，不含密码、secret、认证哈希、完整正文或查询串。批量对象操作保留计数和前 20 项结果。完整详情最多 256 KiB；列表超过 8 KiB 的详情通过单条入口按需读取。

`GET /api/audit/export` 使用同一筛选条件，返回 JSON Lines；每页默认 50、最多 100 条完整记录，响应 `X-Next-Cursor` 表示仍有后续页。页面导出当前页。审计保留由 `cleanup.audit_retention` 控制，默认 90 天，使用现有分批清理任务。

## 用户与成员

以下接口仅管理员可用，写操作需要近期密码验证。用户记录不包含密码、哈希或头像邮箱。修改角色/状态、重置密码和删除会撤销该用户的会话；不删除项目媒体与 S3 应用密钥。

| 方法与路径 | 输入与行为 |
| --- | --- |
| `GET /api/users` | `q?,role?,after?,limit?`；按用户名 C 排序，limit 1～100，返回 `{users,next}`，after 使用上页 next |
| `POST /api/users` | `{username,password,role,must_change_password?}`；role 为 admin/member，改密要求默认 true；返回 201 用户 |
| `GET /api/users/{id}` | 返回单个用户记录 |
| `PATCH /api/users/{id}` | `{role?,enabled?}`，至少一项；返回 200 用户 |
| `DELETE /api/users/{id}` | 删除账户、会话及成员授权；返回 204 |
| `POST /api/users/{id}/reset-password` | `{password,must_change_password?}`；改密要求默认 true；返回 204 |
| `GET /api/projects/{id}/members` | 返回 `{user_id,username,display_name,enabled,role,scope,grants}` 数组，最多 1000 项 |
| `PUT /api/projects/{id}/members/{user}` | `{role,scope,grants:[{bucket_id,actions}]}`，完整替换此项目授权；返回 204 |
| `DELETE /api/projects/{id}/members/{user}` | 仅移除该项目授权；返回 204 |

创建普通成员会开启项目管理。成员授权请求体上限 512 KiB，最多 1000 个桶。all 范围的 grants 必须为空；selected 范围只接受项目内的不同桶，动作不得超出角色上限。管理员无需成员授权；提升为管理员会移除旧成员关系，再降级时需重新分配。

最后一个已启用管理员不可被禁用、降级或删除；Web 与 CLI 的并发操作共用保护，返回 409 `LastAdministrator`。must_change_password 用户可登录、查看个人设置和修改密码，其余业务 API 返回 403 `PasswordChangeRequired`；自行改密后解除。

## 项目与权限

默认使用内置 Default 项目，项目管理关闭；创建额外项目会开启项目管理。关闭前必须移除额外项目、普通成员与成员关系，不会自动提升权限。桶和 S3 服务凭据各属于一个项目，凭据不能跨项目授权。

| 方法与路径 | 输入与行为 |
| --- | --- |
| `GET /api/projects` | 管理员列出全部项目；成员列出所属项目，最多 1000 个 |
| `POST /api/projects` | `{name,description?,allow_bucket_create?}`，返回 201 项目 |
| `PUT /api/projects/{id}` | 同上，更新项目；返回 200 |
| `DELETE /api/projects/{id}` | 删除空项目；内置项目不可删除；返回 204 |
| `GET /api/settings/projects` | 返回 `{enabled}` |
| `PUT /api/settings/projects` | `{enabled}`，设置项目管理开关 |

项目写操作仅管理员可用，要求最近五分钟验证密码；否则返回 403 `ReauthenticationRequired`，先调用 `/api/me/reauth`。name 为 1～128 UTF-8 字节，description 最多 2000 字节。

动作：`bucket.list`、`object.read`、`object.write`、`object.delete`、`object.acl`、`bucket.settings`、`storage.inspect`。reader 允许列举、读取和存储详情；writer 增加写入、删除和 ACL；maintainer 再增加桶设置。成员可访问项目全部桶或仅指定桶；指定动作与角色权限取交集。桶列表返回当前身份的有效 actions。

S3、管理页面和 CLI 共用存储变更逻辑。PUT、分片及异步完成在发布前复核授权版本；撤权后的旧写请求不能发布。已接收数据的后台落盘可继续。匿名 public-read 不受项目成员关系限制。全局状态、任务和维护接口仅管理员可用；成员的区块详情不暴露全局读取计数或密钥标识。

## 存储桶

| 方法与路径 | 输入 | 成功响应 |
| --- | --- | --- |
| `GET /api/buckets` | 无 | 200 桶数组，按 name 排序，最多 1000 个 |
| `GET /api/buckets/{bucket}/website` | bucket 为 UUID | 200 网站设置 |
| `PUT /api/buckets/{bucket}/website` | `{website_enabled,index_document,error_document}`，均必填 | 200 保存后的设置 |
| `GET /api/buckets/{bucket}/cors` | bucket 为 UUID | 200 CORS 规则数组 |
| `PUT /api/buckets/{bucket}/cors` | 规则数组，`[]` 关闭 | 200 保存后的规则 |

桶信息：`{id,project_id,actions,name,state,cors,website_enabled,index_document,error_document,created_at}`。设置仅在桶 active 且非维护模式时可写；创建、删除和清空桶使用 CLI。

### 网站设置

默认关闭，首页为 `index.html`、错误页为 `404.html`。

| 字段 | 约束 |
| --- | --- |
| website_enabled | bool |
| index_document | 单个文件名，1～255 UTF-8 字节 |
| error_document | 桶根相对对象键，最多 1024 UTF-8 字节；空字符串使用内置 404 |

路径拒绝前导斜杠、反斜杠、控制字符和 `.`／`..` 路径段；无效值返回 400，未知 JSON 字段返回 422。保存立即生效，路由规则见[公共网站](s3-compatibility.md#公共网站)。

### CORS 设置

每桶最多 100 条规则，同时作用于 S3 和公共读入口，不改变 ACL。

| 字段 | 类型与默认值 | 约束 |
| --- | --- | --- |
| origins | 必填字符串数组 | 非空，HTTP(S) origin 或 `*` |
| methods | 必填字符串数组 | 非空，GET/HEAD/POST/PUT/DELETE/OPTIONS |
| headers | 字符串数组，默认 `[]` | HTTP 头名或 `*` |
| expose | 字符串数组，默认 `[]` | HTTP 头名或 `*` |
| max_age | u32，默认 0 | 预检缓存秒数 |

非法规则返回 400，请求仍受 16 KiB 限制。Web 支持添加、删除、清空和填入 Wasabi 风格预设，保存后生效；匹配行为和预设内容见[S3 CORS](s3-compatibility.md#cors)。

## 对象

| 方法与路径 | 输入 | 成功响应 |
| --- | --- | --- |
| `GET /api/objects` | bucket=UUID；prefix 默认空，recursive 默认 false，token 可选，limit 默认 100、范围 0～1000 | 200 `{objects,prefixes,next_token}` |
| `GET /api/object` | bucket=UUID、key | 200 `{object,bucket_grants}` |
| `GET /api/object/chunks` | bucket、key、version=对象 UUID；after 可选，limit 默认 100、范围 1～200 | 200 `{chunks,next_offset}`；版本变更 412 |
| `POST /api/objects/actions` | `{bucket,action,objects:[{key,version}]}`；action 为 delete/private/public-read | 200 `{results:[{key,version,status}]}` |
| `GET/HEAD /api/download` | bucket、key；preview 默认 false | 原始字节或响应头，支持 Range 和条件请求 |

列表按 C 排序，游标绑定 bucket/prefix/recursive。`prefixes` 按 `/` 分组，文件和目录共用 limit；recursive=true 平铺列出匹配对象。下一页保留筛选参数并传入 next_token。

对象字段：`{id,bucket_id,object_key,kind,state,size,etag,metadata,public_read,checksums,created_at,touched_at}`。size 为原始字节，etag 不含引号；metadata 结构见[数据库](database-schema.md#jsonb-结构)。`bucket_grants` 为 `{access_key,writable}` 数组，不含 secret；凭据授权与对象 public_read 分别生效。

### 批量写操作

- objects 包含 1～1000 个不同 key，version 取对象列表／详情的 id；覆盖或删除后返回该项 412，避免操作新版本。
- 每项独立提交，允许部分成功。成功项为 200，失败项可能为 404/409/412/500/503；HTTP 200 不表示所有项成功，中断后应刷新核对结果。
- 该路由允许 2 MiB 请求体、最多两个并发请求；认证后取得名额，60 秒内未收完请求体返回 408。超限 413、繁忙 503。
- 删除走正常引用退役和 GC 流程，不同步删除共享块；ACL 不改变 S3 凭据授权。维护模式或清除中的桶拒绝写操作。
- 日志包含管理员 UUID、目标、动作、结果和 RequestId，不记录会话令牌或密钥。批次内失败也计入运行统计的失败请求。

### 区块详情

每行：`{id,offset_bytes,length,source_offset,raw_size,stored_size,independent_size_hint,payload_size,compression,algorithm,key_id,source,pack_id,reads,range_reads}`。id、offset_bytes、next_offset、pack_id 用十进制字符串表示，末页 next_offset 为 null；只读数据库，不访问后端。

source 为 chunk/pack/pending；stored_size 是当前独立副本长度，没有独立副本时为 null。independent_size_hint、compression 和 payload_size 分别为独立编码长度提示、压缩标记和扣标签后的长度，不代表区块包中某成员的实际占用。reads/range_reads 为延迟落库的累计块读取次数。length/source_offset 描述引用区间，不能据此直接推算删除释放空间。

algorithm/key_id 是逻辑块的初始编码与去重域；重编码后的实际算法和密钥由 chunk_locations 或 packs 记录，可能与初始值不同。

### 预览与下载

以下接口都用查询参数 `key`、`version`（stream UUID）绑定当前对象；对象被覆盖或删除后拒绝旧版本，不能用历史 UUID 绕过当前授权。

| 方法与路径 | 响应与权限 |
| --- | --- |
| `GET /api/buckets/{bucket}/object` | 目录项、ETag、HTTP/自定义元数据、可用预览类型；需要 `bucket.list` |
| `GET /api/buckets/{bucket}/object/content` | 原文件，另接受 `preview` bool；需要 `object.read` |
| `GET /api/buckets/{bucket}/object/thumbnail` | 按需 PNG 缩略图；每次检查 `object.read`，包含命中本地缓存的请求 |
| `GET /api/buckets/{bucket}/object/text` | `{text,truncated}`，最多读取前 64 KiB，纯文本/Markdown/CSV/JSON/XML；需要 `object.read` |

preview=false 使用 attachment 和 application/octet-stream；preview=true 仅对 JPEG/PNG/GIF/WebP/AVIF、MP4/WebM/OGG 视频及 MPEG/OGG/MP4/WebM/WAV/FLAC 音频 MIME 内联；带非 identity Content-Encoding 的对象仍下载。HTML/SVG 不嵌入管理页，XML 仅作为文本显示。响应带 nosniff、sandbox CSP 和 private/no-store；需要会话或有效 scoped Token。

缩略图采用独立、可丢弃的本地缓存，范围和资源限制见[管理配置](configuration.md#监听与管理)。不支持的格式、解码失败或超限返回 415；繁忙 503、等待超时 504。图库失败时保留类型图标，不自动下载原图。前端对象 URL 离开时释放，不保存到浏览器持久缓存。

下载可返回 200、单段 Range 206、条件命中 304、条件不符 412 或范围无效 416（含总长）。后续区块损坏可能中止已经开始的响应，不发送损坏块明文；客户端须确认响应完整。

## 运行状态

`GET /api/status` 返回 200，与 `cli status` 相同。运行计数在进程内累计、重启归零；库存容量异步采集，不随页面刷新扫描对象表。待上传诊断读取有界缓存对应的元数据。

| 字段 | 含义 |
| --- | --- |
| version / resources | 程序版本 / 生效的[资源预算](configuration.md#自动预算) |
| local_bytes | `[multipart,chunks,thumbnails]` 本地占用 |
| upload_cache | enabled、configured_size、effective_cache_bytes、effective_upload_bytes、reserved_bytes、fallbacks；pending 为 entries/bytes/oldest_at/failed_entries；pins 最多 100 项，含字符串 chunk_id、pin_type、owner_id、created_at、attempts、next_retry_at、last_error |
| gc_paused / maintenance / gc_running | GC 暂停、维护模式、GC 是否运行 |
| active_streams / data_slots_available | 活跃 stream 数 / 可用在途数据槽 |
| upload_slots_available / read_slots_available | 可用读写并发名额 |
| db_pool_size / db_pool_idle | 连接池总数 / 闲置连接 |
| backend_gets/puts/deletes | 后端区块操作累计次数 |
| backend_read_bytes / backend_write_bytes | 后端成功读取／写入的编码字节 |
| cache_hits / cache_hit_bytes | 缓存命中数 / 实际读取的 .raw 或 .zst 文件字节 |
| runtime.started_at / uptime_seconds | 进程启动时间 / 运行秒数 |
| runtime.http.s3/web/manage | 各 HTTP 入口的请求计数和耗时 |
| runtime.gc_deleted / gc_failures | 完成逻辑退役的区块数 / 本地清理、后台回收或物理删除失败次数 |
| io.backend.get/put/delete/head | 后端区块操作计数、成功传输字节和耗时，不含 marker、LIST 和 SDK 内部重试次数 |
| io.cache_lookups / cache_hit_rate | 区块读取次数 / 缓存命中比例；合并等待同次回源的请求各计一次 lookup |
| io.cpu_slots_available | 可用 CPU 名额 |
| io.backend_queues.read/upload/control | 各方向的 limit/running、queued（foreground/upload/maintenance）及 oldest_wait_seconds |
| io.cache_limit_bytes / multipart_limit_bytes | 配置的本地字节配额，未配置为 null |
| process_memory.rss_bytes / peak_rss_bytes | Linux RSS / 峰值，读取失败为 null；不是包含文件页缓存的容器内存 |
| cleanup | 历史清理状态，见下文 |
| storage | 后台容量快照，见下文 |

### 计数与耗时

每组 HTTP／后端统计包含 `started,completed,active,failed,canceled,client_errors,server_errors,bytes,failure_rate,duration_ms`。

- completed 包括成功、失败和取消；failed 包括 HTTP 4xx/5xx、流错误及 HTTP 200 中的延迟 XML 错误，与 canceled 互斥。`failure_rate=(failed+canceled)/completed`。
- client_errors/server_errors 按已产生的 HTTP 状态计数，后端组两项为 0。HTTP bytes 是交给 HTTP 层的响应体字节，不含请求、头部及网络开销，不保证客户端已收到。
- HTTP 耗时从收到完整请求头到响应结束／错误／取消；HEAD 和空响应在生成时结束。后端耗时从取得名额开始，GET 包含整个区块读取及 SDK 重试，不含解码或缓存写入。
- duration_ms 含 mean/p50/p95/p99。分位数为从 1 ms 起倍增的直方图近似上界；无样本或超过 2^23 ms 的溢出桶为 null。并发快照可能有瞬时差异，不保存历史时序。

### 清理结果

`cleanup`：`{running,interval,batch_size,max_duration,deleted_chunk_retention,upload_retention,task_retention,last_run}`，与 `cli cleanup status` 一致。

last_run 首次执行前为 null，此后为 `{started_at,finished_at,duration_ms,deleted,batches,budget_exhausted,last_error}`。deleted 统计 chunks/chunk_locations/packs/uploads/tasks/sessions/integrity_issues 已提交的删除行数；batches 包含删除零行的批次。时限耗尽后保留已提交结果、回滚未完成事务，后续轮次继续；失败时 last_error 为 cleanup_failed，详情见日志。该结果不包含本地 fragment 文件和 extent 清理。

### 容量快照

后台在启动后采集一次，之后按[统计配置](configuration.md#运行统计)调度。每轮使用只读、可重复读事务，最多一个汇总任务及连接，禁用查询并行，work_mem 为每节点 16 MiB，锁等待最多 1 秒；总时限及语句时限由 query_timeout 控制。大表汇总可能产生数据库 I/O 和临时文件。

`storage`：`{snapshot,collecting,last_attempt_at,last_error,age_seconds,stale,refresh_interval_seconds}`。首次成功前 snapshot 为 null；失败保留上次快照，last_error 为 refresh_failed。缺少快照、刷新失败或年龄超过两倍间隔时 stale=true，统计失败不阻止对象服务。

| snapshot 字段 | 口径 |
| --- | --- |
| as_of / collected_at | 数据库一致性快照时间 / 汇总完成时间 |
| objects / logical_bytes | 当前可见对象数量 / 原始字节总和，不含旧版本和未完成分片 |
| buckets / buckets_truncated | `{id,name,objects,logical_bytes}` 数组，含空桶，按 UUID 排序，最多 1000 桶；截断时全局总数仍包含全部桶 |
| chunks.states | 各状态区块行数，包括保留的 deleted 元数据 |
| chunks.stored_bytes | ready/retired/deleting 物理来源的编码大小，包含过渡副本与待回收数据 |
| chunks.unconfirmed_bytes | preparing/uploading 物理来源已记录的编码大小，远端是否存在尚不确定 |
| physical | 按 kind（chunk/pack）、state 分组的 objects/stored_bytes |
| live | 可见对象引用的唯一块数 chunks、引用区间总长 reference_bytes、唯一块原始大小 raw_bytes、尚无远端来源的 pending_raw_bytes、编码大小 stored_bytes、扣标签后的 payload_bytes |
| unreferenced | chunks/eligible_chunks 为无 extent 引用的逻辑块数及过宽限、无 owner_stream 的数量；stored_bytes 为无引用独立来源及退役物理来源字节，eligible_bytes 按物理来源宽限筛选。部分闲置区块包的剩余占用在 physical 中，实际回收还受引用和活跃保护约束 |
| tasks / uploads | 按状态计数的任务 / active、completing 上传 |
| cleanup | chunks/uploads/tasks/sessions/integrity_issues 到期历史的 `{eligible,oldest_at}`；时间分别为 deleted_at/touched_at/updated_at/expires_at，巡检异常使用所属任务 updated_at；排除仍有 extent 的块和仍有 part 的上传，可能含被锁或活跃保护暂缓的行 |
| database | `{table,total_bytes,index_bytes,live_rows_estimate,dead_rows_estimate,last_autovacuum,last_autoanalyze}`；大小含索引和 TOAST，行数为估计，维护时间可为 null |

去重节省量为 `live.reference_bytes-live.raw_bytes`；编码节省量为 `live.raw_bytes-live.pending_raw_bytes-live.payload_bytes`。物理大小按唯一可见来源计数：一个区块包即使只剩部分成员仍在使用，也计入整个包；过渡副本另外计入 physical。每个加密物理载荷扣 16 字节标签，none 为 0；分母为 0 时比例为 null。部分引用、索引开销可使节省为负，不按桶分摊共享来源。

物理统计来自数据库，不遍历后端，不包含未索引对象、meta.json、提供商对象版本或账单规则；上传和删除期间可能短暂不一致。

## 区块包

`POST /api/cache/flush` 返回 `{task_id}`，使用相同的会话、Origin 和 CSRF 验证。它将任务创建前的积压写成独立来源，在维护模式也可显式执行；通过现有任务 API 暂停/恢复。完成不阻止后续请求产生新积压，备份需先阻止写入。

对象区块的 source 可为 pending，表示目前依赖本地待上传来源；此时 stored_size 为空，independent_size_hint 仅是编码提示。巡检对本地唯一来源进行读取校验，不能宣称已确认后端存在。

| 方法与路径 | 输入 | 成功响应 |
| --- | --- | --- |
| `GET /api/packs` | after 默认 0；limit 默认 100，1～200 | `{status,packs,next_after}`，按 ID 升序 |
| `GET /api/packs/{id}` | 正 bigint 十进制 ID | `{pack,members}`，成员含逻辑区块及当前映射标志 |
| `POST /api/packs/run` | `{kind}`：pack/reuse/reclaim/range/repack | `{task_id}`，已有同类任务可返回 existing=true |
| `POST /api/packs/unpack` | `{pack_id?:字符串,all?:bool,execute?:bool}` | 默认返回 preview/packs/raw_bytes/effect；execute=true 返回 task_id |

pack_id 与 all=true 必须二选一；全部拆包要求 pack.enabled=false。包及成员的大整数 ID 使用字符串，避免浏览器精度损失。区块包页面可查看列表、成员、维护入口与拆包预览；写接口遵守会话、Origin、CSRF 和维护模式。操作、冷却和回收语义见[CLI](cli-reference.md#区块包维护)。

任务 detail.last_rewrite 保存最近一次成功切换的 before_bytes/output_bytes/temporary_added_bytes/transition_bytes，分别为旧布局、新布局、本批新增载荷及新旧并存大小；不包含更早批次仍在 GC 宽限内的副本，部署总占用以 physical 汇总为准。

区块包状态含 range_optimization；Range 任务的 detail.evaluated 为已评估包数，last_range 含 pack_id、applied、reason、partial_downloads，以及 benefit 的 observed_bytes/projected_bytes/rewrite_bytes/extra_gets/request_penalty_bytes/net_savings_bytes。收益是按历史窗口外推的预计值，重写大小采用实际编码结果，不承诺未来命中率。区块包的 range_checked_at 是最近评估时间。

## 后台任务

| 方法与路径 | 输入 | 成功响应 |
| --- | --- | --- |
| `GET /api/tasks` | state、token 可选；limit 默认 100，范围 1～200 | 200 `{tasks,next_token}`，按 created_at、id 降序 |
| `GET /api/tasks/{id}` | id 为任务 UUID | 200 任务详情 |
| `POST /api/tasks/{id}/actions` | `{action:"pause"或"resume"}` | 200 `{task_id,state}`，非法转换 409 |

任务字段：`{id,kind,bucket_id,state,cursor,processed,detail,error,created_at,updated_at}`。state 为 queued/running/paused/completed/failed，游标绑定筛选状态。API 状态值使用英语，界面负责翻译。

暂停／继续的适用状态及维护要求见[CLI 任务语义](cli-reference.md#后台任务)。整桶清除与后端清查通过 CLI 创建，Web 可控制已有任务；巡检可直接通过下列 API 创建。

## 完整性巡检

| 方法与路径 | 输入 | 成功响应 |
| --- | --- | --- |
| `POST /api/integrity` | `{mode?,bucket?,key?}`，mode 默认 metadata，可选 head/full；bucket 为名称，key 需 bucket | 200 `{task_id}` |
| `GET /api/tasks/{id}/issues` | after 默认 0，limit 默认 100，范围 1～200 | 200 `{issues,next_after}`；游标为十进制字符串或 null |
| `GET /api/tasks/{id}/issues/{issue}/objects` | after 可选，为 URL 编码的 JSON `[bucket UUID,key]` | 200 `{objects,next}`，每页 100 项 |
| `GET /api/tasks/{id}/report` | 仅已完成巡检 | JSONL 附件；其他状态 409，非巡检任务 404 |

创建请求拒绝未知字段／模式及长度不在 1～1024 UTF-8 字节内的 key；桶或精确对象不存在返回 404。最多一个 queued/running 巡检，冲突返回 409；暂停后可启动另一个，继续时仍检查此限制。模式区别见[巡检命令](cli-reference.md#完整性巡检)。

### 进度与覆盖范围

kind 为 integrity，detail 包含：

| 字段 | 含义 |
| --- | --- |
| mode / phase | 模式 / metadata、chunks、done |
| bucket_id / bucket / key | 保存的巡检范围 |
| upper_chunk_id / upper_object | 创建时的区块 ID 上界（十进制字符串）/ 对象上界 `[bucket UUID,key]` 或 null |
| objects_checked / chunks_checked / bytes_checked | 已提交批次的对象数 / 区块数 / 下载编码字节 |
| issues / skipped / finished_at | 异常数 / 因变更跳过数 / 完成时间 |

processed 为对象和区块检查数之和，不是百分比；cursor 是内部 JSON 字符串，可保存对象中途的进度。未提交批次及中断下载的流量不计入 bytes_checked，应结合运行统计判断。

巡检在线检查保存上界内、仍由范围内已发布对象引用的区块。它反映一段时间窗口，不是同一时刻快照；不重算完整对象 ETag，不检查活动上传、未引用块或可丢弃缓存。尚无远端来源的 pending 块在所有模式下校验本地唯一副本。新数据可能在扫描期间进入上界内的范围；晚于上界的新对象／区块不在覆盖范围。

当前批次持有读取保护，解除引用的数据可跳过。异常与进度原子保存，重启不重复登记；权限、连接及超时等执行错误停止任务并保留进度，不记作坏块。`completed` 且 issues>0 表示完成但有异常，`failed` 表示未完成。

### 异常与报告

异常字段：`{id,task_id,subject,code,chunk_id,storage_id,stream_id,bucket_id,object_key,detail,created_at}`；bigint ID 使用十进制字符串。

| code | 含义 |
| --- | --- |
| remote_missing / length_mismatch | 远端缺失 / 编码长度异常 |
| pending_unavailable | 本地待上传唯一副本缺失或损坏，保留现场等待恢复 |
| chunk_metadata / missing_key | 区块元数据异常 / 缺少历史密钥 |
| authentication_failed / decompression_failed / hash_mismatch | 认证 / 解压 / 原始长度或哈希校验失败 |
| object_metadata / mapping_gap / mapping_source / object_length | 对象身份或状态 / 映射缺口或重叠 / 来源 / 总长度异常 |

异常保留检查时的身份和诊断信息，不因对象删除而消失。关联对象返回当前引用的 `{bucket_id,bucket,key,version}`，可包含其他桶的共享引用；已替换或删除的历史版本不在结果中。

报告为 `application/x-ndjson`，首行 `{type:"task",task:...}`，随后逐条 `{type:"issue",issue:...}`，末行 `{type:"end",issues:N}`。每次读取 100 条、不持有长事务；过期清理或中断导致内容不全时不会输出结束行。异常随已完成任务按[保留期](configuration.md#回收与历史清理)分批清理，failed/paused 不自动到期。

## 管理页面

`GET/HEAD /` 提供 Vue 管理应用；媒体库支持桶、目录与对象浏览和授权下载。现有桶设置、任务、区块包和批量操作保留在 `/classic/`。静态资源随二进制提供，无 Node.js 运行服务。

个人设置支持 en/zh-CN/ja、自动/亮色/暗色、显示名、可选头像、改密和会话管理。语言按需加载，未登录时使用浏览器保存的选择或优先语言，无法匹配回退英语；登录后以账户设置为准，同步浏览器本地偏好。表单离开前提示未保存修改。

编译后的哈希资源支持长期缓存与 ETag；HTML 和固定名称素材重验证，API 及私有下载保持 `private, no-store`。SPA 深链接不覆盖未知 API 或静态资源的 404。`--api-only` 镜像不包含页面，详见 [前端构建](../web/README.md)。

经典控制台：

- 支持 zh-CN/en：首次按浏览器语言选择，中文以外回退英语；选择保存在 localStorage，切换保留未提交表单。名称、元数据和错误内容始终按文本显示。
- URL 保存 page、bucket、prefix、recursive、token、key、section、state、task、taskToken、`pack`、packAfter，支持刷新、前进后退和复制链接。section 为 cors/website，page 为 objects/settings/status/tasks/packs；访问仍需登录。
- 对象每页 100 项，可按前缀／完整 key 定位；勾选仅限当前页，上一页使用本标签页历史。支持图片／视频预览和原文件下载；批量操作先列出目标，再显示逐项结果。
- 任务可每 5 秒刷新，页面隐藏或离开任务页时停止；用户可关闭。暂停或终止的巡检详情停止自动轮询，可手动刷新、查看关联对象和导出报告。
- 状态页刷新只读取运行计数和已缓存的容量快照。库存通过分页浏览，不设累计对象数量上限。
