# 镜像回源

镜像回源在对象缺失时，从 HTTP/HTTPS 源站或外部 S3 连接读取内容，按目标桶的存储策略保存。每个桶独立配置，默认关闭。已保存的对象继续由 Mokyu 提供，源站后续修改和删除不会自动同步。

## 设置规则

在「存储桶设置 → 桶详情 → 镜像回源」打开开关，添加规则并保存。需要桶的 `bucket.settings` 权限；公开导入还需要 `object.acl`。规则按列表顺序匹配，使用第一条匹配对象键前缀的规则。

- **来源**：HTTP 基础 URL，或同一项目内已启用、当前操作者拥有读取权限的外部 S3 连接。S3 连接在「外部存储桶」管理，主、备用来源各自引用连接 UUID，并可设置独立的 `prefix`。
- **路径**：先匹配前缀，按选项移除前缀，再按原样添加规则的 `add_prefix`。S3 来源再按原样添加自身的 `prefix`，最后叠加外部连接的目录前缀。规则和来源前缀都不会自动补 `/`，目录前缀需自行以 `/` 结尾，`photo-` 则表示对象名前缀。外部桶独立设置的目录前缀仍按目录处理，非空时补 `/`。两层可选前缀均可留空，对象键中的大小写、空格、连续斜杠和点号不作路径归一化。
- **公开读**：默认关闭。开启后，匿名请求可以触发本规则，新对象以 public-read 保存；来源的 ACL 不会传到目标桶。
- **备用源**：可选。在主源缺失、超时或暂时不可用时尝试；认证失败不切换。已开始读取的正文发生错误时不会拼接另一个来源的内容。

例如匹配 `media/`，移除匹配前缀，添加 `archive/`，HTTP 基础 URL 为 `https://origin.example/assets/`：

```text
media/photos/a.jpg → https://origin.example/assets/archive/photos/a.jpg
```

同样的规则，S3 连接指向物理桶 `backup`，连接目录前缀为 `imports`，主来源 `prefix` 为 `primary/`，备用为 `fallback-`：

```text
主来源：s3://backup/imports/primary/archive/photos/a.jpg
备用源：s3://backup/imports/fallback-archive/photos/a.jpg
```

来源写法为 `{"type":"s3","external_id":"连接 UUID","prefix":"primary/"}`，`prefix` 省略或为空均不添加。连接目录和来源前缀同时为空时，从物理桶根按规则映射后的对象键读取。表单显示拼接后的来源位置；路径预览和测试结果使用同一映射。

「预览路径」使用当前草稿计算映射，不访问源站。「测试源站」从单容器实例或 control 发送 HEAD，只核对连接、认证和对象元信息，不下载正文、不保存对象。群集的 data/worker 节点也必须能够访问源站；主控测试成功不能代替节点连通性检查。不支持 HEAD 的源站可能测试失败，实际 GET 行为仍由源站决定。

HTTP 默认要求 HTTPS。使用 HTTP、私有网络地址或重定向均需显式启用；私有网络地址仅管理员可配置。即使启用私网，环回、链路本地和云元数据地址仍被阻止。DNS 解析和每次重定向都会检查地址；最多跟随 3 次同源重定向，跨来源跳转被拒绝。

HTTP URL 不能包含用户名、密码、查询串或片段。认证可写入固定请求头，值加密保存在数据库，读取配置时只返回头名称。编辑时省略 `headers` 保留同一 URL 的原值，`headers: {}` 清空；改变 URL 时必须明确重填或清空。客户端的 Cookie、Authorization、查询串及条件头不会作为源站凭据透传。请求头名称使用小写，最多 16 个；Host、Range、Cookie、跳数标记及代理、连接、S3 签名相关头由程序管理，不能覆盖。

## 读取和保存

| 请求 | 对象未保存时的行为 |
| --- | --- |
| 完整 GET | 创建或加入同一键的回填任务；首个完整读取可边读边存，跟随者等待发布 |
| HEAD | 返回缺失，不回源 |
| LIST | 只列已保存对象，不扫描或合并源站 |
| 单段 Range | 源站有可靠版本标识及长度时，可在完整回填之外请求该版本的范围 |
| 条件请求、校验请求 | 等待完整保存，再使用 Mokyu 的 ETag、时间和校验信息判断 |
| 图片处理 | 先保存原图，再按已有样式与原图保护规则处理 |

Range 优先固定强 ETag 或 S3 version ID。缺少可靠版本标识、长度未知时，只进行一次完整读取，范围请求等待发布。源站忽略 Range 而返回完整正文时，Mokyu 丢弃前缀后返回所需部分；高偏移可能消耗大量流量。额外 Range 请求与完整回填共享预算，槽位不足时会等待。任务详情分别显示完整读取字节和额外范围流量。

并发请求不会为同一键创建多个有效完整回填。等待达到上限时返回可重试错误。客户端取消后，已接纳的回填可继续；规则关闭、权限变化、删除和新写入仍会使旧任务失去发布权。

**客户端读完不等于对象已保存。** 源站正文可能已全部返回，但目标存储写入、校验或配额检查仍会失败。任务区分读取、保存和已保存；只有完成发布后，对象才进入 HEAD、LIST 和普通热读取。失败不会发布半对象，已有正常对象也不会被旧回填覆盖。

原始对象、区块、压缩、加密、区块包、配额与后端可靠性均采用目标桶现有策略。已保存的对象正常生成元数据日志；下载中的临时内容不进入恢复日志。启用镜像回源不会绕过目标存储容量和写入限制。

## 删除、暂停和重试

启用镜像回源的桶删除对象键时，会持久记录删除屏蔽，包括尚未保存的键。关闭后重新开启回源不会清除屏蔽，普通同名上传仍可成功。在「运行记录与删除屏蔽」分页查找并点击「解除回源屏蔽」，之后的新 GET 才可再次导入；旧任务不会因此重新获得发布权。

同一区域可查看回填状态、已读取字节、可计算时的速度和预计时间，并暂停或继续任务。暂停不会撤销已发送给客户端的字节；未完成的下载恢复时重新开始。排查源站权限、目标配额和后端容量后，可继续失败任务。规则被修改、对象被正常写入或删除的旧任务不能用继续操作绕过新状态，应由后续读取按当前规则重新接纳。

高级设置的默认值：连接超时 10 秒、响应头及跟随读取等待 30 秒、正文闲置超时 60 秒、缺失缓存 30 秒。连接超时范围 1–300 秒，响应头和闲置超时范围 1–3600 秒，缺失缓存范围 0–3600 秒。最大对象大小留空时不额外设置回源上限，仍受现有写入和存储限制约束；设置时接受大于零、至多 5 TiB 的字节数。

## CLI

在单容器实例或 control 容器执行。进入已有容器后，后续命令直接使用 `mokyu cli`；配置不在默认位置时，在 `cli` 前添加 `-c PATH`。

```sh
docker exec -it mokyu sh
mokyu cli bucket mirroring media show
mokyu cli bucket mirroring media show > /tmp/mirroring.json
```

编辑 JSON 的 `config`，保留所见 `revision`。以下草稿把 `media/` 下的对象从 HTTPS 源站导入；将修订号替换为 `show` 返回的值。

```json
{
  "revision": 0,
  "config": {
    "enabled": true,
    "rules": [{
      "id": "019a7000-0000-7000-8000-000000000001",
      "name": "Media origin",
      "prefix": "media/",
      "strip_prefix": true,
      "add_prefix": "archive/",
      "public_read": false,
      "source": {"type": "http", "url": "https://origin.example/assets/"},
      "backup": null
    }]
  }
}
```

```sh
mokyu cli bucket mirroring media preview media/photos/a.jpg --file /tmp/mirroring.json
mokyu cli bucket mirroring media set /tmp/mirroring.json
mokyu cli bucket mirroring media test media/photos/a.jpg
mokyu cli bucket mirroring media status --limit 20
mokyu cli bucket mirroring media blocks --limit 20
mokyu cli bucket mirroring media unblock media/photos/a.jpg
```

`preview` 不带 `--file` 时使用已保存规则。`set` 接受文件或 `-` 标准输入，最多 64 KiB，成功返回新修订；重复使用旧草稿会拒绝保存。`test --backup` 显式测试匹配规则的备用源，不自动切换。

`status` 的 `next_cursor` 传给 `--token` 获取下一页；`blocks` 的 `next` 传给 `--after`。用 `pause TASK_UUID` 和 `resume TASK_UUID` 管理该桶的指定回填任务，不能操作其他桶的任务。

## 恢复边界

镜像回源配置、来源凭据、删除屏蔽和任务保存在运行数据库。完整数据库备份可保留这些记录；对象元数据日志仅恢复已保存对象。仅从元数据日志恢复部署后，镜像回源默认关闭，需要另行恢复或确认规则及删除屏蔽。

`mokyu dump` 只读取后端中已保存、具有可恢复元数据的对象，不访问镜像源站。普通读取中的缺失回源也不代替损坏对象的修复流程。
