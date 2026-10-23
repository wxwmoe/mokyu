# Mokyu 管理前端

默认 Docker 构建会自动编译并嵌入前端，运行容器不需要 Node.js

完整部署见[部署指南](../docs/deployment-and-recovery.md)，面板哈希资源的可选 Nginx 缓存见[反代示例](../nginx.example.conf)；对象文件交付见[本地部署](../docs/local-nginx-deployment.md)

单独构建需要 Node.js 24.12 或更高版本，在本目录执行：

```sh
npm ci
npm run build
```

产物位于 `web/dist/`，独立部署时：

- 静态托管 `dist/`，将 `/api/v1/` 同源反代到 Mokyu，`control.origin` 设为浏览器访问地址
- 应用页面使用 SPA fallback；未知 API 和资源返回 404，私有 API 不缓存
- 后端镜像可在仓库根目录用 `./build.sh --api-only` 构建，也可加 `--alpine`

构建同时生成有收益的 `.gz`、`.br`、`.zst` 变体。内嵌服务自动协商；独立静态托管需由服务器选择变体并正确设置 `Content-Encoding` 和 `Vary: Accept-Encoding`，未配置时原始资源仍可用。
