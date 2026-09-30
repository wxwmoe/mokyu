# Mokyu 管理前端

默认 Docker 构建会自动编译并嵌入前端，运行容器不需要 Node.js

完整部署见[部署指南](../docs/deployment-and-recovery.md)

单独构建需要 Node.js 24.12 或更高版本，在本目录执行：

```sh
npm ci
npm run build
```

产物位于 `web/dist/`，独立部署时：

- 静态托管 `dist/`，将 `/api/` 同源反代到 Mokyu，`manage.origin` 设为浏览器访问地址
- 应用页面使用 SPA fallback；未知 API 和资源返回 404，私有 API 不缓存
- 后端镜像可在仓库根目录用 `./build.sh --api-only` 构建，也可加 `--alpine`
