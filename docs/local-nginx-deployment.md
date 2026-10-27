# SQLite、本地 raw 与代理文件交付

单容器可使用 SQLite 和本地 raw 文件，通过 Nginx 的 `X-Accel-Redirect` 发送匿名 S3 与公共 Web 的完整 GET 正文。Mokyu 仍检查对象映射、权限、来源与文件长度，Nginx 只读挂载该存储的 `raw` 目录。每个后端独立配置，默认由 Mokyu 发送。

代理交付关闭该存储的正文内存缓存，本地存储本身不使用磁盘读写缓存。元数据缓存仍可用，文件页缓存由操作系统管理。`sendfile on` 不代表所有 TLS、HTTP/2 或 HTTP/3 连接都使用零拷贝，实际发送方式取决于代理、内核与 TLS 配置。

## 创建单容器部署

以下文件保存在同一目录，宿主需要 Docker Compose 和 `jq`。Nginx 使用 1.30.6+ 或 1.31.7+，启用 HTTP/2 模块；普通请求和失败回退均以 h2c 回源。示例保留可靠写入与 SQLite FULL，仅关闭普通读取的额外摘要复核；对象接收完整性、元数据事务和文件长度检查仍保留。额外摘要关闭后，尚未发现的同长度文件损坏可能直接返回客户端。

```sh
mkdir -p config data
chmod 755 data
chmod 700 config
docker run --rm wxwmoe/mokyu tool keygen > config/deployment-key
chmod 600 config/deployment-key
```

创建 `config/config.toml`，将 `deployment_key` 替换为上一步输出：

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
thumbnail_cache_size = "64MiB"
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
    image: nginx:1.30.6
    restart: unless-stopped
    mem_limit: 256m
    ports:
      - "8080:8080"
      - "8081:8081"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - ./mokyu-file-delivery.conf:/etc/nginx/mokyu-file-delivery.conf:ro
      - ./data/storage/raw:/srv/mokyu/raw:ro
```

先启动 Mokyu 并通过 CLI 初始化，创建存储后再启动 Nginx，避免 Docker 预先创建挂载目录而使空存储初始化失败：

```sh
chmod 600 config/config.toml
chown -R 10001:10001 config data
docker compose up -d mokyu
docker compose exec mokyu mokyu cli status --ready
docker compose exec mokyu mokyu cli init admin
docker compose exec mokyu mokyu cli storage create primary local --init
docker compose exec -T mokyu mokyu cli storage profile show primary |
  jq '.cdc_enabled = false | .deduplication_enabled = false |
      .compression_enabled = false | .pack_enabled = false | .pack_range_enabled = false |
      .read_integrity = "off" | .file_delivery = "x_accel_redirect" |
      .file_delivery_prefix = "/_mokyu_raw/"' > config/raw-profile.json
docker compose exec mokyu mokyu cli storage profile preview primary /config/raw-profile.json
docker compose exec mokyu mokyu cli storage profile set primary /config/raw-profile.json
docker compose exec mokyu mkdir -p /data/storage/raw
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
    gzip off;
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
    proxy_pass $mokyu_upstream;
    proxy_http_version 2;
    proxy_set_header Host $http_host;
    proxy_set_header Accept-Encoding $http_accept_encoding;
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
    proxy_pass $mokyu_upstream$request_uri;
    proxy_http_version 2;
    proxy_set_header Host $http_host;
    proxy_set_header Accept-Encoding $http_accept_encoding;
    proxy_set_header Connection "";
    proxy_set_header Upgrade "";
    proxy_set_header X-Mokyu-File-Delivery application;
    proxy_buffering off;
}
```

```sh
docker compose up -d nginx
docker compose exec nginx nginx -t
```

保留整组响应头配置。Nginx 内部转向不会自动保留所有上游头；Mokyu 用内部 `X-Mokyu-*` 元数据避免文件打开失败后将旧缓存策略或下载文件名带入重试。Nginx 使用原对象的 ETag 和 Last-Modified，CORS 由 Mokyu 按当前桶策略计算，不能在代理中固定为通配符。[Nginx 上游处理](https://nginx.org/en/docs/http/ngx_http_proxy_module.html)、[响应头配置](https://nginx.org/en/docs/http/ngx_http_headers_module.html#add_header)

`X-Mokyu-File-Delivery` 只是代理能力标记，不是鉴权凭据。Mokyu 的 S3/Web 监听必须只供受信代理访问，代理覆盖客户端同名头。缺少或不匹配该标记时，Mokyu 正常发送正文。不要将 `raw` 目录配置为公共静态目录，也不要挂载整个 `/data`；数据库、配置、节点凭据和密钥不应被代理读取。

## 适用范围与回退

- 仅匿名 S3 与公共 Web 的完整 GET、单个本地 raw 文件可交付。HEAD、Range、条件请求、checksum 请求、签名 S3、带自定义对象元数据、压缩表示及旧 chunks 对象由 Mokyu 处理，仍不为该存储填充正文读缓存。
- `read_integrity=inline` 保留请求内完整摘要复核并使用普通发送。`background` 允许交付后排队复核整个来源，仍可能产生磁盘读取；`off` 跳过这项额外工作。两者的边界见[读取策略](configuration.md#storage-profile)。
- 默认 `public_cache_validation=strict`，每次读取确认当前权限。若明确接受缓存有效期，可同时启用公开与来源后台快照；两项均有效时热门交付无需前台 SQL，文件类型和长度仍逐次检查。不要把开启代理交付当成已开启缓存快照。
- 本地 raw 使用不可变物理文件名，覆盖产生新文件。若 GC 在代理打开文件前移除了旧文件，配置回到原请求、禁用再次交付，由 Mokyu 重新检查当前映射和权限；已打开的文件描述符可继续读取。
- 多个本地存储分别配置不同内部 URI、只读目录及 `file_delivery_prefix`；前缀映射直接指向该存储的 `raw` 根目录。路径后只追加服务端生成的 UUID 路径，不使用桶名或对象 key。
- `cli status` 的 `file_delivery` 统计两种交付选择次数、计划交付字节和应用回退次数。计划字节不代表客户端收到的字节，实际传输与错误还需查看代理日志。

恢复普通交付时，将完整 profile 中的 `file_delivery` 改为 `application`、`file_delivery_prefix` 改为 `null` 后保存。新请求可再次使用实例正文缓存，已在途请求继续完成。

## Apache X-Sendfile

Apache 2.4 可使用 [mod_xsendfile](https://tn123.org/mod_xsendfile/)。启用 `proxy`、`proxy_http`、`headers`、`rewrite`、`xsendfile` 模块，将同一目录只读挂载到 `/srv/raw`，并将 profile 改为 `file_delivery="x_sendfile"`、`file_delivery_prefix="/srv/raw/"`。仅将这条目录加入模块白名单，不创建公共 Alias。

下例为 S3 入口；公共 Web 使用另一 VirtualHost，将上游端口改为 `9001`。反代继续透传 Accept-Encoding，不开启额外的压缩过滤器。

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
