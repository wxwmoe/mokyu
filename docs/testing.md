# 用 Mokyu 给测试项目提供 S3

在测试机的工作目录中执行下面一条命令即可启动。Docker 会在当前目录创建缺失的 `./data`，Mokyu 会初始化空目录，创建本地 SQLite 数据库、raw 存储、测试桶和密钥。无需配置文件。

```sh
docker run -d --name mokyu-test --stop-timeout 45 \
  -p 9000:9000 -v ./data:/data wxwmoe/mokyu testing
```

使用 Compose 时，将以下内容保存为 `compose.yaml` 并执行 `docker compose up -d`。数据保存在该文件旁的 `data` 目录。

```yaml
services:
  mokyu-test:
    image: wxwmoe/mokyu
    container_name: mokyu-test
    command: testing
    ports:
      - "9000:9000"
    volumes:
      - ./data:/data
    stop_grace_period: 45s
```

## 连接测试桶

S3 默认监听 `0.0.0.0:9000`。Docker 默认将 `9000:9000` 发布到宿主机的所有网络接口。本机使用 `http://localhost:9000`，局域网客户端使用测试机的 IP 和 9000 端口。SDK 选择 path-style、SigV4 和 region `us-east-1`。

| 参数 | 值 |
| --- | --- |
| Bucket | `test` |
| Access key | `MOKYUTESTING00000001` |
| Secret key | `mokyu-testing-secret` |

这对公开密钥可读写、复制和删除测试桶里的对象。请只向允许这些操作的网络发布端口。

```sh
export AWS_ACCESS_KEY_ID=MOKYUTESTING00000001 AWS_SECRET_ACCESS_KEY=mokyu-testing-secret
export AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:9000 s3 cp example.txt s3://test/example.txt
docker exec mokyu-test mokyu cli cache flush --wait
docker restart mokyu-test
aws --endpoint-url http://localhost:9000 s3 cp s3://test/example.txt received.txt
```

## 重启与管理

保留同一个 `./data`，重启或重建容器后即可继续读取已持久化的对象、续传未完成的分片上传。首次启动请使用专属空目录。

已知长度且不超过 8 MiB 的普通 PUT 可以在正文进入内存后返回。默认写内存额度最多 64 MiB，额度不足时改写文件。进程被强杀可能丢失尚未落盘的正文。需要确认本批写入已持久化时，执行 `mokyu cli cache flush --wait` 并检查退出状态。

在容器内执行 `mokyu cli status --ready` 检查就绪状态，管理入口仅监听容器内的 `127.0.0.1:9002`。需要自定义数据路径、S3 监听或资源预算时，可以提供只含 `[instance]` 的配置，并给服务和 CLI 传相同的 `--config FILE`。例如使用 `/config/lab.toml` 时，健康检查命令为 `mokyu --config /config/lab.toml cli status --ready`。
