# 桶图片处理

在「存储桶设置 → 桶详情 → 图片处理」启用。样式适用于公开对象。磁盘产物不授予匿名权限；每次服务都须有有效的读取授权，缓存预设仍受授权快照、通知和有效期约束。管理媒体库使用独立的授权入口，可为有读取权限的用户预览私有图片。

## 创建与交换样式

在页面选择添加样式，按处理顺序添加步骤，选择输出格式和质量，再预览并保存。限制尺寸时选等比适应并关闭放大，小图会保持原尺寸。可以按前缀查找、导入和导出；桶内没有固定的样式总数上限。名字区分大小写，为 1–128 个 ASCII 字符，以字母或数字开头，可包含点、下划线和连字符，不能以点结尾或包含连续的点。`management-preview` 供媒体库保留。

Web 和 [CLI](cli-reference.md#图片样式) 使用同一种逐行文本格式。导入可以为名字加前缀；遇到重名可报错、跳过或替换。识别支持的 `image/…`、`imageMogr2/…`、`imageView2/2/…` 命令，无法表示的操作会报错。导出始终使用 Mokyu 参数，可以再次导入。示例：

```text
styleName: crop1, styleBody: imageMogr2/crop/400x400
styleName: q90.300x.auto, styleBody: mokyu/v1/orient/resize,mode=contain,width=300,height=300,upscale=false/encode,format=auto,quality=90
styleName: q90.300x.jpeg, styleBody: mokyu/v1/orient/resize,mode=contain,width=300,height=300,upscale=false/encode,format=jpeg,quality=90
```

## 公开访问与原图保护

以下路径相对于桶的公共域名。短链接默认使用 `!`；也可启用 `-`、`_`、`/`，或者使用查询参数。长参数只接受原生语法：

```text
/photo.jpg!q90.300x.jpeg
/photo.jpg?style=q90.300x.jpeg
/photo.jpg?mokyu/v1/orient/resize,mode=contain,width=300,height=300,upscale=false/encode,format=jpeg,quality=90
```

启用的分隔符和已登记的样式共同决定短路径，多个名称匹配时取最长的有效名称。配置与已有对象路径冲突时拒绝保存；写入也不能占用已保留的样式路径。找不到样式的普通路径按对象名处理，仍受原图保护和对象权限约束。

开启**原图保护**后，匿名客户端只能使用允许的命名样式，不能取原文件或自行改参数。这也适用于 HEAD、Range 和条件读取。签名 S3 请求仍按对象权限读取原文件。处理失败不会退回原图；开启保护前，应考虑 CDN 上仍有效的旧原图缓存。

## 处理顺序与格式

同一阶段用 `/` 连接操作，以 `encode` 指定该阶段的输出；只有需要拿上一阶段的编码结果作为下一次输入时才使用 `|`。例如 `mokyu/v1/orient/encode,format=png|mokyu/v1/resize,mode=contain,width=300,upscale=false/encode,format=webp`。单阶段自动选择 WebP 时，直接从输入像素编码 WebP。

| 操作 | 参数或用途 |
| --- | --- |
| `orient`、`frame,index=0` | 按方向标签旋转；显式选择帧，从 0 开始。多帧输入须选帧。 |
| `resize`、`crop` | 缩放模式为 `contain` 或 `cover`；指定宽、高及是否放大。裁剪指定宽、高和锚点，如 `anchor=center`。 |
| `background,color=ffffff` | 以六位 RGB 色合成透明区域。输出不支持透明度且未指定背景色时使用白色；能保留透明度的输出默认保留。 |
| `quality`、`encode` | 质量支持指定值、上限、相对值或沿用来源。输出可选 `source`、`jpeg`、`png`、`webp`、`jxl`、`avif` 或 `auto`；JPEG 可选渐进编码。 |

`auto` 只用于最后一阶段。显式 Accept 权重优先；权重相同则按源图选择：

| 输入 | 顺序 |
| --- | --- |
| 普通不透明图片 | JXL → WebP → JPEG |
| 普通透明图片 | JXL → WebP → PNG → JPEG |
| 支持的 HDR 图片 | JXL → AVIF → HDR JPEG |

缺少 Accept、只有通配符或没有识别到格式偏好时，以 JPEG 兜底；显式禁止 JPEG（如 `image/jpeg;q=0`）时不会强行返回，没有可选格式则返回 406。HDR JPEG 含 SDR 基础图和 HDR 增益图；不要把普通 JPEG 解码器显示的基础图当作完整 HDR。HEIC/HEIF 可用作支持的输入，不参与自动输出；不能可靠解释的动态范围和增益图会被拒绝。

## 自动跳转与 CDN

自动地址返回 `302`、`Cache-Control: no-store` 和 `Vary: Accept`。长参数跳到固定格式的长参数；`?style=name` 和未配置隐式映射的短样式在原地址附加 `format=webp` 等已选格式。固定地址可按响应的 Cache-Control 缓存。CDN 须保留完整路径和查询参数，遵守 no-store；权限或规则变更不能主动清除已在外部 CDN 保鲜期内的响应。

隐式映射**只用于分隔符短链接**。例如 auto 标签为 `Auto.Redirect`，JPEG 标签为 `Default.format`，选中 JPEG 时 `/photo.jpg!Image.Auto.Redirect` 跳到 `/photo.jpg!Image.Default.format`。按点分隔的完整后缀替换，已映射的格式才参与；必须提供 JPEG。每个受影响的样式系列都须有启用的目标样式，除固定输出格式外处理步骤一致。缺少或不匹配时整次保存失败，保留原配置。

## 大图、缓存与媒体库

桶级输入上限默认 100,000,000 字节，可设为 1–1,000,000,000 字节。原图总像素可设为 1–150,000,000，默认 150 MP；1 MP 为一百万像素，动图按宽 × 高 × 总帧数计。超过限制的输入不能生成、跳转或读取已有处理结果。单边不得超过 32,768 像素。普通非渐进 8 位 JPEG 和非隔行 8 位 PNG 的大原图可按行读取并先缩小；命名样式里的方向校正可以写在缩放之前。其他输入、变换和输出仍受节点工作内存的像素预算约束。CPU 时间、内存和临时文件另有预算，因此提高桶上限不保证任意图片都能处理。原生参数最多 8,192 字节、8 个阶段、24 个计入预算的操作。不支持的输入或超时不会生成可用缓存。

公开图片与媒体库缩略图共用 `<data_path>/cache/images/`。相同来源与等价的固定规则可以复用同一份文件；图片处理只在源数据完整性验证通过后生成产物。媒体库默认取第 0 帧、校正方向、限制在 384 × 384 内且不放大，输出 PNG；原文件最多 32 MiB，结果最多 640 KiB。缩略图响应是 `private, no-store`，命中缓存仍要验证读取权限和对象版本。
