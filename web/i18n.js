export const messages = {
  en: {
    language: 'Language', logout: 'Sign out', loginTitle: 'Administration sign in',
    username: 'Username', password: 'Password', login: 'Sign in', buckets: 'Bucket',
    refresh: 'Refresh', status: 'Service status', tasks: 'Background tasks', path: 'Folder path',
    website: 'Website settings', websiteEnabled: 'Enable website routing', indexDocument: 'Index filename',
    errorDocument: 'Error document key', save: 'Save', websiteSaved: 'Website settings saved.',
    websiteHelp: 'Documents must allow public read. Leave the error key empty for the built-in 404 page.',
    cors: 'CORS settings', corsHelp: 'Rules apply to S3 and public reads. No rules disables CORS. Object permissions still apply; changes take effect after saving.',
    corsPreset: 'Fill Wasabi-style preset', corsClear: 'Clear rules', corsAdd: 'Add rule', corsRemove: 'Remove rule', corsRule: 'CORS rule',
    corsOrigins: 'Allowed origins (one per line; * allows any)', corsMethods: 'Allowed methods',
    corsHeaders: 'Allowed request headers (one per line; * allows any)', corsExpose: 'Exposed response headers (one per line; * exposes all)',
    corsMaxAge: 'Preflight cache lifetime (seconds)', corsSaved: 'CORS settings saved.', corsInvalid: 'Each rule needs at least one origin and method. Maximum 100 rules.',
    name: 'Name', size: 'Size', access: 'Access', updated: 'Last modified', more: 'Load next page',
    details: 'Object details', root: 'Root', folder: 'Folder: {name}', public: 'Public read',
    private: 'Private', noBuckets: 'No buckets yet. Create and authorize one with the CLI.',
    bucketGrants: 'Bucket credentials', readWrite: 'Read and write', readOnly: 'Read only', noGrants: 'No S3 credentials have access to this bucket.',
    download: 'Download original', preview: 'Preview', rawDetails: 'API details', backToFiles: 'Back to files',
    requestFailed: 'Request failed (HTTP {status}).', networkError: 'Unable to reach the gateway.',
    objectKey: 'Object key', contentType: 'Content type', etag: 'ETag', version: 'Version',
    cpu: 'Available CPU cores', memory: 'Detected memory budget',
    multipartBytes: 'Local multipart data', cacheBytes: 'Local chunk cache',
    gc: 'Garbage collection', running: 'Running', paused: 'Paused', maintenance: 'Maintenance mode',
    enabled: 'Enabled', disabled: 'Disabled', cacheHits: 'Chunk cache hits', backendGets: 'Backend GETs',
    taskId: 'Task ID', taskType: 'Type', taskState: 'State', processed: 'Processed', noTasks: 'No background tasks.',
    purge: 'Bucket purge', sweep: 'Backend sweep', queued: 'Queued', completed: 'Completed', failed: 'Failed',
  },
  'zh-CN': {
    language: '语言', logout: '退出登录', loginTitle: '管理登录', username: '用户名', password: '密码',
    login: '登录', buckets: '存储桶', refresh: '刷新', status: '服务状态', tasks: '后台任务', path: '目录路径',
    website: '网站设置', websiteEnabled: '启用网站路由', indexDocument: '首页文件名',
    errorDocument: '错误页对象键', save: '保存', websiteSaved: '网站设置已保存。',
    websiteHelp: '首页和错误页必须允许公开读取。错误页留空时使用内置 404 页面。',
    cors: 'CORS 设置', corsHelp: '规则同时作用于 S3 和公共读接口。没有规则表示关闭 CORS；对象权限仍然生效，修改后需要保存。',
    corsPreset: '填入 Wasabi 风格预设', corsClear: '清空规则', corsAdd: '添加规则', corsRemove: '删除规则', corsRule: 'CORS 规则',
    corsOrigins: '允许的来源（每行一项；* 表示任意来源）', corsMethods: '允许的方法',
    corsHeaders: '允许的请求头（每行一项；* 表示任意请求头）', corsExpose: '向浏览器暴露的响应头（每行一项；* 表示全部）',
    corsMaxAge: '预检缓存时间（秒）', corsSaved: 'CORS 设置已保存。', corsInvalid: '每条规则至少需要一个来源和方法，最多 100 条规则。',
    name: '名称', size: '大小', access: '权限', updated: '更新时间', more: '加载下一页', details: '对象详情',
    root: '根目录', folder: '目录：{name}', public: '公开读取', private: '私有',
    bucketGrants: '存储桶凭据授权', readWrite: '读写', readOnly: '只读', noGrants: '暂无可访问此存储桶的 S3 凭据。',
    noBuckets: '还没有存储桶，请通过 CLI 创建并授权。', download: '下载原文件', preview: '预览',
    rawDetails: 'API 详情', backToFiles: '返回文件列表', requestFailed: '请求失败（HTTP {status}）。', networkError: '无法连接网关。',
    objectKey: '对象键', contentType: '内容类型', etag: 'ETag', version: '版本', cpu: '可用 CPU 核数',
    memory: '检测到的内存预算', multipartBytes: '本地分片数据', cacheBytes: '本地区块缓存',
    gc: '区块回收', running: '运行中', paused: '已暂停', maintenance: '维护模式',
    enabled: '已启用', disabled: '已关闭', cacheHits: '区块缓存命中', backendGets: '后端 GET 次数',
    taskId: '任务 ID', taskType: '类型', taskState: '状态', processed: '已处理', noTasks: '暂无后台任务。',
    purge: '清空并删除存储桶', sweep: '后端区块清查', queued: '等待中', completed: '已完成', failed: '失败',
  },
};

let saved;
try { saved = localStorage.getItem('media-gateway-language'); } catch { /* Storage can be disabled by the browser. */ }
const preferred = (navigator.languages || [navigator.language]).find(value => /^(zh|en)(-|$)/i.test(value)) || 'en';
export let locale = Object.hasOwn(messages, saved) ? saved : (preferred.toLowerCase().startsWith('zh') ? 'zh-CN' : 'en');
export function t(key, values = {}) {
  return (messages[locale][key] ?? messages.en[key] ?? key).replace(/\{(\w+)\}/g, (_, name) => String(values[name] ?? `{${name}}`));
}
export function setLocale(value) {
  if (!Object.hasOwn(messages, value)) return;
  locale = value;
  document.documentElement.lang = value;
  try { localStorage.setItem('media-gateway-language', value); } catch { /* Translation still works without persistence. */ }
}
export function translate(root = document) {
  root.querySelectorAll('[data-i18n]').forEach(element => { element.textContent = t(element.dataset.i18n); });
  root.querySelectorAll('[data-i18n-label]').forEach(element => { element.setAttribute('aria-label', t(element.dataset.i18nLabel)); });
}
