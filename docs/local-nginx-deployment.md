# 通过 Nginx 交付 raw 文件

Mokyu 先检查对象权限、当前版本和读取策略。完整的匿名 GET 可以由 Nginx 发送文件，响应压缩也交给 Nginx。按实际存储位置选择挂载目录。

| 文件来源 | 代理只读挂载 | 交付方式 |
| --- | --- | --- |
| 本地存储 | 该后端的 `raw/` | Nginx X-Accel-Redirect 或 Apache X-Sendfile |
| S3 后端 | 提供请求的 data、node 或单容器的 `cache/objects/` | Nginx X-Accel-Redirect，仅使用校验通过的完整文件 |

同一入口同时服务两种来源时，给两者配置独立的内部 URI。只向受信代理开放 Mokyu 监听端口。示例先搭建 SQLite 与本地 raw，再说明如何接入 S3 缓存。

## 创建单容器部署

本例使用官方 Nginx 1.31.8 主线镜像，客户端可通过 HTTP/1.1 或 HTTP/2 访问，Nginx 以 h2c 连接 Mokyu。宿主需要 Docker Compose 和 jq。写入使用可靠预设，示例将普通读取的额外摘要复核设为 off。[Nginx 官方版本](https://nginx.org/en/download.html)

```sh
mkdir -p config
docker run --rm wxwmoe/mokyu tool keygen
```

在仅管理员可进入的宿主机目录内创建 `config/config.toml`，将 `deployment_key` 替换为上一步输出。容器需要能进入 `config` 并读取配置文件，例如目录为 `0755`、文件为 `0644`。

```toml
[database]
type = "sqlite"

[instance]
deployment_key = "REPLACE_WITH_MOKYU_TOOL_KEYGEN_OUTPUT"
listen_control = "0.0.0.0:9002"
listen_web = "0.0.0.0:9001"
listen_s3 = "0.0.0.0:9000"
data_path = "/data"
min_free_space = "1GiB"
stream_memory_budget = "256MiB"
response_cache_size = "0B"
write_memory_size = "0B"
cache_size = "128MiB"
upload_cache_size = "64MiB"
image_cache_size = "64MiB"
```

创建 `compose.yaml`：

```yaml
services:
  mokyu:
    image: wxwmoe/mokyu
    restart: unless-stopped
    mem_limit: 1g
    stop_grace_period: 45s
    ports:
      - "127.0.0.1:9002:9002"
    volumes:
      - ./config:/config:ro
      - ./data:/data
  nginx:
    image: nginx:1.31.8-alpine
    restart: unless-stopped
    mem_limit: 256m
    ports:
      - "8080:8080"
      - "8081:8081"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - ./mokyu-file-delivery.conf:/etc/nginx/mokyu-file-delivery.conf:ro
      - ./data/storage/objects:/srv/mokyu/raw:ro
```

先启动 Mokyu 并通过 CLI 初始化，创建存储后再启动 Nginx，避免 Docker 预先创建挂载目录而使空存储初始化失败：

```sh
docker compose up -d mokyu
docker compose exec mokyu mokyu cli status --ready
docker compose exec mokyu mokyu cli init admin
docker compose exec mokyu mokyu cli storage create primary local --init
docker compose exec -T mokyu mokyu cli storage profile show primary |
  jq '.cdc_enabled = false | .deduplication_enabled = false |
      .compression_enabled = false | .pack_enabled = false | .pack_range_enabled = false |
      .read_integrity = "off" | .http_compression_enabled = false | .file_delivery = "x_accel_redirect" |
      .file_delivery_prefix = "/_mokyu_raw/"' > config/raw-profile.json
docker compose exec mokyu mokyu cli storage profile preview primary /config/raw-profile.json
docker compose exec mokyu mokyu cli storage profile set primary /config/raw-profile.json
docker compose exec mokyu mkdir -p /data/storage/objects
docker compose exec mokyu mokyu cli bucket create media
docker compose exec mokyu mokyu cli cred create media
docker compose exec mokyu mokyu cli domain set media.example.com:8081 media
```

保存凭据创建命令返回的 secret。客户端使用 S3 endpoint `http://HOST:8080`，region 与运行设置一致，上传时通过 `public-read` ACL 允许公开读取；Web 地址为 `http://media.example.com:8081/KEY`，域名需解析到宿主。生产公网入口应配置 TLS，通用结构见 [Nginx 示例](../nginx.example.conf)。管理界面通过宿主本机 `http://localhost:9002` 或 SSH 隧道访问。

## Nginx 配置

创建 `nginx.conf`：

```nginx
worker_processes auto;
events { worker_connections 1024; }
http {
    sendfile on;
    # Only the internal raw location enables compression after object authorization.
    gzip off;
    gzip_comp_level 5;
    gzip_min_length 1024;
    gzip_proxied any;
    gzip_types text/plain text/css application/javascript application/json application/xml image/svg+xml application/wasm;
    # Mokyu already varies object responses on Accept-Encoding.
    gzip_vary off;

    map $http_accept_encoding $mokyu_strict_encoding {
        default 0;
        "~*(^|,)\s*(identity|\*)\s*;\s*q=0(\.0{1,3})?\s*(,|$)" 1;
    }
    map "$http_authorization:$args" $mokyu_signed {
        default 1;
        "~*^:(?:[^&]*&)*X-Amz-(Signature|Credential)=" 1;
        "~^:" 0;
    }
    upstream mokyu_s3 { server mokyu:9000; keepalive 32; }
    upstream mokyu_web { server mokyu:9001; keepalive 32; }
    server {
        listen 8080;
        http2 on;
        client_max_body_size 0;
        set $mokyu_upstream http://mokyu_s3;
        include /etc/nginx/mokyu-file-delivery.conf;
    }
    server {
        listen 8081;
        http2 on;
        server_name media.example.com;
        set $mokyu_upstream http://mokyu_web;
        include /etc/nginx/mokyu-file-delivery.conf;
    }
}
```

创建 `mokyu-file-delivery.conf`：

```nginx
location / {
    error_page 418 = @mokyu_original;
    if ($mokyu_signed) { return 418; }
    if ($mokyu_strict_encoding) { return 418; }
    proxy_pass $mokyu_upstream;
    proxy_http_version 2;
    proxy_set_header Host $http_host;
    proxy_set_header Accept-Encoding "";
    proxy_set_header Connection "";
    proxy_set_header Upgrade "";
    proxy_set_header X-Mokyu-File-Delivery x_accel_redirect;
    proxy_request_buffering off;
    proxy_buffering off;
    proxy_read_timeout 600s;
    proxy_send_timeout 600s;
}
location ^~ /_mokyu_raw/ {
    internal;
    alias /srv/mokyu/raw/;
    disable_symlinks on;
    gzip on;
    etag off;
    if_modified_since off;
    error_page 403 404 = @mokyu_fallback;
    add_header ETag $upstream_http_etag always;
    add_header Last-Modified $upstream_http_last_modified always;
    add_header Cache-Control $upstream_http_x_mokyu_cache_control always;
    add_header Content-Disposition $upstream_http_x_mokyu_content_disposition always;
    add_header Expires $upstream_http_x_mokyu_expires always;
    add_header Content-Language $upstream_http_content_language always;
    add_header Vary $upstream_http_vary always;
    add_header X-Content-Type-Options $upstream_http_x_content_type_options always;
    add_header X-Request-Id $upstream_http_x_request_id always;
    add_header X-Amz-Request-Id $upstream_http_x_amz_request_id always;
    add_header X-Amz-Id-2 $upstream_http_x_amz_id_2 always;
    add_header Access-Control-Allow-Origin $upstream_http_access_control_allow_origin always;
    add_header Access-Control-Allow-Methods $upstream_http_access_control_allow_methods always;
    add_header Access-Control-Allow-Headers $upstream_http_access_control_allow_headers always;
    add_header Access-Control-Expose-Headers $upstream_http_access_control_expose_headers always;
    add_header Access-Control-Max-Age $upstream_http_access_control_max_age always;
}
location @mokyu_fallback {
    if ($upstream_status = "") { return 404; }
    proxy_pass $mokyu_upstream$request_uri;
    proxy_http_version 2;
    proxy_set_header Host $http_host;
    proxy_set_header Accept-Encoding "";
    proxy_set_header Connection "";
    proxy_set_header Upgrade "";
    proxy_set_header X-Mokyu-File-Delivery application;
    proxy_buffering off;
}
location @mokyu_original {
    # Keep the original request headers for signatures and strict negotiation.
    proxy_pass $mokyu_upstream;
    proxy_http_version 2;
    proxy_set_header Host $http_host;
    proxy_set_header Connection "";
    proxy_set_header Upgrade "";
    proxy_set_header X-Mokyu-File-Delivery application;
    proxy_request_buffering off;
    proxy_buffering off;
    proxy_read_timeout 600s;
    proxy_send_timeout 600s;
}
```

```sh
docker compose up -d nginx
docker compose exec nginx nginx -t
```

Nginx 对可压缩类型启用原版动态 gzip，Mokyu 提供 Vary 和对象校验标记。已存储的 Content-Encoding、带 no-transform 的对象或请求、签名、Range、条件请求、校验和请求以及 HEAD 均由应用处理。签名请求和明确禁止 identity 的客户端保留原请求头，由应用完成签名校验或返回 406。磁盘文件打开失败时，Nginx 用原 URI 和查询串重新请求 Mokyu，重新核对当前权限与版本。内部位置及回退都应保留，直接从互联网请求内部 URI 会被拒绝。

官方镜像的动态响应压缩算法是 gzip。gzip_static 用于服务预先生成的 .gz 文件，gunzip 用于解压。Mokyu 的 raw 存储和缓存只发布原始文件，代理只读挂载。响应中的 ETag、缓存策略、下载文件名、语言和 CORS 均来自当次授权的对象。[gzip](https://nginx.org/en/docs/http/ngx_http_gzip_module.html)、[gzip_static](https://nginx.org/en/docs/http/ngx_http_gzip_static_module.html)、[gunzip](https://nginx.org/en/docs/http/ngx_http_gunzip_module.html)

## 使用 S3 的本地读缓存

1. 在 S3 后端的存储设置中关闭 CDC，打开磁盘读缓存，将文件交付设为 X-Accel-Redirect，前缀设为 `/_mokyu_cache_raw/`。HTTP 响应压缩随代理交付关闭。
2. 将实际服务该请求的 Mokyu 节点的 `./data/cache/objects` 只读挂载到同一侧 Nginx 的 `/srv/mokyu/cache-raw`。复制上面完整的内部 location，把前缀改为 `/_mokyu_cache_raw/`、alias 改为 `/srv/mokyu/cache-raw/`。多个节点各自配置其本机代理与回退上游。
3. 首次完整读取按原流程从 S3 流式返回并按缓存策略回填。代理首次使用该缓存文件时核对完整摘要，同一文件的后续命中复用校验结果。文件替换或状态改变会重新校验。校验通过后，后续完整 GET 可交给 Nginx。分段缓存、待上传文件及多段拼接对象继续由应用读取。缓存逐出时可以直接回源。

对象名与磁盘名独立。本地后端按当前物理地址查找，S3 缓存按来源 ID 查找，每个 UUID 均使用末两位作为分片目录。默认的 inline 读取完整性检查会选择应用交付；选用 background 或 off 后代理可参与。恢复应用交付时设 `file_delivery="application"`、`file_delivery_prefix=null`，按需重新开启 `http_compression_enabled`。代理日志可用于核对实际传输字节和状态码。

## Apache X-Sendfile

Apache 2.4 可使用 [mod_xsendfile](https://tn123.org/mod_xsendfile/)。启用 `proxy`、`proxy_http`、`headers`、`rewrite`、`xsendfile` 模块，将同一目录只读挂载到 `/srv/raw`，并将 profile 改为 `file_delivery="x_sendfile"`、`file_delivery_prefix="/srv/raw/"`。仅将这条目录加入模块白名单，不创建公共 Alias。

下例为 S3 入口；公共 Web 使用另一 VirtualHost，将上游端口改为 `9001`。此例由 Apache 交付原始字节，保留请求头供签名及严格协商使用。

```apache
<VirtualHost *:8080>
    ProxyPreserveHost On
    ProxyErrorOverride Off
    EnableSendfile On
    XSendFile On
    XSendFilePath /srv/raw
    XSendFileIgnoreEtag Off
    XSendFileIgnoreLastModified Off
    RequestHeader set X-Mokyu-File-Delivery "x_sendfile"
    ErrorDocument 403 /__mokyu_fallback
    ErrorDocument 404 /__mokyu_fallback
    RewriteEngine On
    RewriteCond %{ENV:REDIRECT_STATUS} ^(403|404)$
    RewriteCond %{THE_REQUEST} "\s(/[^\s]*)\s"
    RewriteRule ^/?__mokyu_fallback$ http://mokyu:9000%1 [P,L,NE,E=mokyu_fallback:1]
    RequestHeader set X-Mokyu-File-Delivery "application" env=mokyu_fallback
    <Location /__mokyu_fallback>
        Require expr "%{ENV:REDIRECT_STATUS} =~ /^(403|404)$/"
    </Location>
    ProxyPass /__mokyu_fallback !
    ProxyPass / http://mokyu:9000/ nocanon
    ProxyPassReverse / http://mokyu:9000/
</VirtualHost>
```

保留错误回退及禁止客户端直接访问内部处理路径的规则。模块必须保留应用的 ETag 和 Last-Modified；不要开启 `XSendFileIgnoreEtag` 或 `XSendFileIgnoreLastModified`。切换代理协议前先部署对应配置与目录挂载，避免成功响应只有元数据却没有文件正文。
