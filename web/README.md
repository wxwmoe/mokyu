# Mokyu 管理前端

Vue 3、TypeScript、Vite。`npm ci && npm run build` 生成 `dist/`，默认 Docker 构建会自动执行并把产物嵌入 Rust。运行容器不需要 Node.js。

- `npm run dev`：开发服务器；`MOKYU_API_ORIGIN` 指定 API 代理目标。管理端的 `manage.origin` 应与浏览器访问的地址一致。
- `npm run check`：组件与 TypeScript 检查。
- `npm run test:ui`：隔离部署的浏览器回归；需要 `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only`、`MOKYU_TEST_WEB` 和 `MOKYU_TEST_PASSWORD_FILE`，使用测试管理员与专用测试对象。
- `npm run api:types`：从 `mokyu api-schema` 生成 API 类型。可用 `MOKYU_BINARY` 指定二进制，或用 `MOKYU_API_SCHEMA` 指定导出的 JSON 文件。
- 独立部署：托管 `dist/`，将 `/api/` 同源反代给管理端；仅应用页面使用 SPA fallback，未知 API 和资源应返回 404。私有 API 不缓存。
- 仅 API 镜像：根目录执行 `./build.sh --api-only`，可与 `--alpine` 组合。管理 API、CLI、S3 与公共读取仍保留。

`public/assets/` 保存原有品牌素材。编译产物和 `node_modules/` 不提交。

界面使用 Reka UI、Lucide 图标和自托管 Nunito 字体。构建会把依赖许可证汇总到 `THIRD_PARTY_NOTICES.txt`，独立分发静态文件时保留该文件。
