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
    requestFailedId: 'Request failed (HTTP {status}). Request ID: {requestId}',
    refreshStatistics: 'Refresh statistics', automatic: 'Automatic', startedAt: 'Started at', uptime: 'Uptime',
    dataSlots: 'Available / total data slots', dbPool: 'Idle / open database connections', cpuSlots: 'Available / total CPU jobs',
    gcDeleted: 'Chunks reclaimed since startup', gcFailures: 'GC failures since startup',
    objectCount: 'Objects', logicalBytes: 'Logical size', physicalBytes: 'Indexed remote size', unreferencedBytes: 'Unreferenced chunks',
    residentMemory: 'Process resident memory', cacheRate: 'Chunk cache hit rate',
    runtimeHelp: 'Runtime counters and latency cover this process lifetime and reset on restart. HTTP duration includes streaming; errors and interruptions are counted. P95 is an approximate histogram upper bound.',
    snapshotPending: 'Waiting for the first storage snapshot.', snapshotCollecting: 'Collecting a new snapshot.',
    snapshotTime: 'Storage snapshot: {time}. Background refresh interval: {interval} seconds.',
    snapshotFailed: 'Refresh failed; the previous snapshot is retained.', snapshotStale: 'This snapshot is stale.',
    httpStatistics: 'HTTP requests', listener: 'Listener', listener_s3: 'S3 API', listener_web: 'Public read', listener_manage: 'Management',
    completedRequests: 'Finished', activeRequests: 'Active', failureRate: 'Failure rate', clientErrors: '4xx', serverErrors: '5xx',
    canceled: 'Interrupted', meanLatency: 'Mean duration', p95Latency: 'P95 duration', responseBytes: 'Response data',
    backendStatistics: 'Backend chunk operations', method: 'Method', transferredBytes: 'Transferred data',
    storageHelp: 'Shared chunks are counted globally; buckets show visible objects only. Savings compare chunks referenced by visible objects. Compression excludes authentication tags. Recorded remote size includes pending deletion; it is not the provider bill or a backend scan.',
    spaceSavings: 'Storage and savings', metric: 'Metric', savingRate: 'Savings', dedupSavings: 'Deduplication savings',
    compressionSavings: 'Compression savings', liveStoredBytes: 'Chunks used by visible objects', gcEligibleBytes: 'Past GC grace, with no owner or references',
    unconfirmedBytes: 'Unconfirmed uploads / failed chunks', bucketStatistics: 'Bucket usage', bucketsTruncated: 'Only the first 1,000 buckets are shown. Global totals include all buckets.',
    taskStatistics: 'Task counts', count: 'Count', activeMultipart: 'Active or completing multipart uploads: {count}',
    objectKey: 'Object key', contentType: 'Content type', etag: 'ETag', version: 'Version',
    cpu: 'Available CPU cores', memory: 'Detected memory budget',
    multipartBytes: 'Local multipart data', cacheBytes: 'Local chunk cache',
    gc: 'Garbage collection', running: 'Running', paused: 'Paused', maintenance: 'Maintenance mode',
    enabled: 'Enabled', disabled: 'Disabled', cacheHits: 'Chunk cache hits', backendGets: 'Backend GETs',
    taskId: 'Task ID', taskType: 'Type', taskState: 'State', processed: 'Processed', noTasks: 'No background tasks.',
    purge: 'Bucket purge', sweep: 'Backend sweep', queued: 'Queued', completed: 'Completed', failed: 'Failed',
  },
  'zh-CN': {
    requestFailedId: '请求失败（HTTP {status}）。请求 ID：{requestId}',
    refreshStatistics: '刷新统计', automatic: '自动', startedAt: '启动时间', uptime: '运行时间',
    dataSlots: '可用 / 总数据槽位', dbPool: '空闲 / 已开数据库连接', cpuSlots: '可用 / 总 CPU 任务槽位',
    gcDeleted: '启动以来已回收区块', gcFailures: '启动以来 GC 失败次数',
    objectCount: '对象数量', logicalBytes: '逻辑大小', physicalBytes: '已索引远端占用', unreferencedBytes: '无引用区块占用',
    residentMemory: '进程常驻内存', cacheRate: '区块缓存命中率',
    runtimeHelp: '运行计数和耗时自本次进程启动累计，重启后归零。HTTP 耗时包含流式传输，错误和中断均纳入统计；P95 是直方图区间的近似上界。',
    snapshotPending: '等待首次容量快照。', snapshotCollecting: '正在采集新快照。',
    snapshotTime: '容量快照时间：{time}；后台刷新间隔：{interval} 秒。',
    snapshotFailed: '刷新失败，保留上次快照。', snapshotStale: '此快照已过期。',
    httpStatistics: 'HTTP 请求', listener: '接口', listener_s3: 'S3 API', listener_web: '公共读取', listener_manage: '管理接口',
    completedRequests: '已结束', activeRequests: '进行中', failureRate: '失败率', clientErrors: '4xx', serverErrors: '5xx',
    canceled: '已中断', meanLatency: '平均耗时', p95Latency: 'P95 耗时', responseBytes: '响应数据量',
    backendStatistics: '后端区块操作', method: '方法', transferredBytes: '传输数据量',
    storageHelp: '共享区块按全局统计，存储桶只统计当前可见对象。节省量比较当前可见对象引用的区块；压缩节省量不含认证标签。远端占用包含待删除区块，来自数据库记录，不代表提供商账单，也不扫描后端。',
    spaceSavings: '空间与节省量', metric: '指标', savingRate: '节省比例', dedupSavings: '去重节省',
    compressionSavings: '压缩节省', liveStoredBytes: '当前可见对象使用的区块', gcEligibleBytes: '已过 GC 宽限且无所有者、无引用',
    unconfirmedBytes: '尚未确认上传 / 失败区块', bucketStatistics: '存储桶用量', bucketsTruncated: '仅展示前 1,000 个存储桶，全局总数包含全部存储桶。',
    taskStatistics: '任务数量', count: '数量', activeMultipart: '活动或正在完成的分片上传：{count}',
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
