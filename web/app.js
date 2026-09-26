import { locale, setLocale, t, translate } from './i18n.js';

const $ = id => document.getElementById(id);
let prefix = '', next = null, csrf = '';
let folders = [], objects = [], panel = null, currentNotice = '', listing = 0, view = 0;
const number = value => new Intl.NumberFormat(locale).format(value);
const date = value => new Date(value).toLocaleString(locale);
function translatedError(key, values = {}) { return Object.assign(new Error(t(key, values)), { translationKey: key, values }); }
function notice(error = '') {
  currentNotice = error;
  $('notice').textContent = error.translationKey ? t(error.translationKey, error.values) : error instanceof Error ? error.message : error;
}
async function api(path, options = {}) {
  let response;
  try { response = await fetch(path, { credentials: 'same-origin', ...options }); }
  catch { throw translatedError('networkError'); }
  if (!response.ok) {
    if (response.status === 403) showLogin();
    const requestId = response.headers.get('x-request-id');
    throw translatedError(requestId ? 'requestFailedId' : 'requestFailed', { status: response.status, requestId });
  }
  return response.status === 204 ? null : response.json();
}
function showLogin() { $('login').hidden = false; $('browser').hidden = true; $('logout').hidden = true; }
function button(text, click) {
  const element = document.createElement('button');
  element.textContent = text;
  element.addEventListener('click', () => Promise.resolve(click()).catch(notice));
  return element;
}
function query(values) { return new URLSearchParams(values).toString(); }
function size(value) {
  if (value < 0) return '−' + size(-value);
  if (value < 1024) return number(value) + ' B';
  const unit = Math.min(4, Math.floor(Math.log(value) / Math.log(1024)));
  return new Intl.NumberFormat(locale, { maximumFractionDigits: 1 }).format(value / 1024 ** unit) + ' ' + ['B', 'KiB', 'MiB', 'GiB', 'TiB'][unit];
}
async function enter() {
  const buckets = await api('/api/buckets');
  $('buckets').replaceChildren();
  for (const bucket of buckets) {
    const option = document.createElement('option');
    option.value = bucket.id; option.textContent = bucket.name; $('buckets').append(option);
  }
  $('login').hidden = true; $('browser').hidden = false; $('logout').hidden = false;
  prefix = ''; await list();
}
function breadcrumbs() {
  $('breadcrumbs').replaceChildren(button(t('root'), async () => { prefix = ''; await list(); }));
  let path = '';
  for (const part of prefix.split('/').filter(Boolean)) {
    path += part + '/';
    const target = path;
    $('breadcrumbs').append(button(part, async () => { prefix = target; await list(); }));
  }
}
async function list(more = false) {
  const request = ++listing; ++view;
  notice();
  if (!more) { folders = []; objects = []; next = null; panel = null; $('detail').hidden = true; }
  renderFiles();
  if (!$('buckets').value) { notice(translatedError('noBuckets')); $('more').hidden = true; return; }
  const params = { bucket: $('buckets').value, prefix };
  if (more && next) params.token = next;
  const page = await api('/api/objects?' + query(params));
  if (request !== listing) return;
  folders.push(...page.prefixes); objects.push(...page.objects);
  next = page.next_token; renderFiles();
}
function renderFiles() {
  breadcrumbs(); $('files').replaceChildren();
  for (const folder of folders) row(button(t('folder', { name: folder.slice(prefix.length) }), async () => { prefix = folder; await list(); }), '', '', '');
  for (const object of objects) row(button(object.object_key.slice(prefix.length) || object.object_key, () => detail(object.object_key)), size(object.size), t(object.public_read ? 'public' : 'private'), date(object.touched_at));
  $('more').hidden = !next;
}
function row(name, ...cells) {
  const tr = document.createElement('tr'), first = document.createElement('td');
  first.append(name); tr.append(first);
  for (const text of cells) { const td = document.createElement('td'); td.textContent = text; tr.append(td); }
  $('files').append(tr);
}
function fields(values) {
  $('facts').replaceChildren();
  for (const [label, value] of values) {
    const term = document.createElement('dt'), description = document.createElement('dd');
    term.textContent = t(label); description.textContent = value ?? '—'; $('facts').append(term, description);
  }
}
function renderPanel(clearPreview = false) {
  if (!panel) return;
  const { type, value, params } = panel;
  $('detail').hidden = false; $('detail-title').dataset.i18n = type;
  if (clearPreview) $('detail').scrollIntoView({ block: 'start' });
  $('detail-title').textContent = t(type);
  $('info').textContent = JSON.stringify(value, null, 2);
  $('actions').replaceChildren(); $('task-list').replaceChildren();
  $('statistics').hidden = type !== 'status';
  $('statistics').replaceChildren();
  $('website-form').hidden = type !== 'website';
  $('cors-form').hidden = type !== 'cors';
  $('actions').append(button(t('backToFiles'), () => { panel = null; $('detail').hidden = true; $('refresh').focus(); $('breadcrumbs').scrollIntoView({ block: 'start' }); }));
  if (clearPreview) $('preview').replaceChildren();
  if (type === 'details') {
    const object = value.object, mime = (object.metadata.content_type || '').split(';')[0].trim();
    fields([['objectKey', object.object_key], ['size', size(object.size)], ['access', t(object.public_read ? 'public' : 'private')], ['updated', date(object.touched_at)], ['contentType', object.metadata.content_type], ['etag', object.etag]]);
    const title = document.createElement('h3'); title.textContent = t('bucketGrants'); $('task-list').append(title);
    for (const grant of value.bucket_grants) { const line = document.createElement('p'); line.textContent = grant.access_key + ' — ' + t(grant.writable ? 'readWrite' : 'readOnly'); $('task-list').append(line); }
    if (!value.bucket_grants.length) { const line = document.createElement('p'); line.textContent = t('noGrants'); $('task-list').append(line); }
    const download = document.createElement('a');
    download.className = 'button'; download.textContent = t('download'); download.href = '/api/download?' + query(params); $('actions').append(download);
    const image = ['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'image/avif'].includes(mime);
    const video = ['video/mp4', 'video/webm'].includes(mime);
    if (image || video) $('actions').append(button(t('preview'), () => {
      const media = document.createElement(image ? 'img' : 'video');
      media.src = '/api/download?' + query({ ...params, preview: 'true' });
      if (image) media.alt = object.object_key; else media.controls = true;
      $('preview').replaceChildren(media);
    }));
  } else if (type === 'cors') {
    fields([['buckets', params.name]]);
    if (clearPreview) renderCors(value);
  } else if (type === 'website') {
    fields([['buckets', params.name]]);
    if (clearPreview) {
      $('website-enabled').checked = value.website_enabled;
      $('index-document').value = value.index_document;
      $('error-document').value = value.error_document;
    }
  } else if (type === 'status') {
    fields([['version', value.version], ['startedAt', date(value.runtime.started_at)], ['uptime', number(value.runtime.uptime_seconds) + ' s'], ['cpu', number(value.resources.available_cpus)], ['memory', size(value.resources.memory_bytes)], ['multipartBytes', size(value.local_bytes[0]) + ' / ' + limit(value.io.multipart_limit_bytes)], ['cacheBytes', size(value.local_bytes[1]) + ' / ' + limit(value.io.cache_limit_bytes)], ['gc', t(value.gc_paused ? 'paused' : value.gc_running ? 'running' : 'enabled')], ['maintenance', t(value.maintenance ? 'enabled' : 'disabled')], ['dataSlots', number(value.data_slots_available) + ' / ' + number(value.resources.data_slots)], ['dbPool', number(value.db_pool_idle) + ' / ' + number(value.db_pool_size)], ['cpuSlots', number(value.io.cpu_slots_available) + ' / ' + number(value.resources.cpu_jobs)], ['gcDeleted', number(value.runtime.gc_deleted)], ['gcFailures', number(value.runtime.gc_failures)]]);
    renderStatistics(value);
    $('statistics').prepend(button(t('refreshStatistics'), async () => {
      const request = ++view;
      const updated = await api('/api/status');
      if (request === view) { panel = { type: 'status', value: updated }; renderPanel(); }
    }));
  } else {
    fields([]);
    if (!value.length) { $('task-list').textContent = t('noTasks'); return; }
    const table = document.createElement('table'), head = document.createElement('thead'), tr = document.createElement('tr');
    for (const key of ['taskId', 'taskType', 'taskState', 'processed', 'updated']) { const th = document.createElement('th'); th.textContent = t(key); tr.append(th); }
    head.append(tr); table.append(head);
    const body = document.createElement('tbody');
    for (const task of value) {
      const row = document.createElement('tr');
      for (const text of [task.id, t(task.kind), t(task.state), number(task.processed), date(task.updated_at)]) { const td = document.createElement('td'); td.textContent = text; row.append(td); }
      body.append(row);
    }
    table.append(body); $('task-list').append(table);
  }
}
const limit = value => value == null ? t('automatic') : size(value);
const percent = value => value == null ? '—' : new Intl.NumberFormat(locale, { style: 'percent', maximumFractionDigits: 1 }).format(value);
const milliseconds = value => value == null ? '—' : new Intl.NumberFormat(locale, { maximumFractionDigits: 2 }).format(value) + ' ms';
function renderStatistics(value) {
  const root = $('statistics');
  function text(tag, value, parent = root) { const node = document.createElement(tag); node.textContent = value; parent.append(node); return node; }
  function table(title, headings, rows) {
    text('h3', t(title));
    const scroll = document.createElement('div'); scroll.className = 'stats-table';
    const table = document.createElement('table');
    const caption = document.createElement('caption'); caption.textContent = t(title); caption.className = 'sr-only'; table.append(caption);
    const head = document.createElement('thead'), row = document.createElement('tr');
    headings.forEach(key => { const cell = text('th', t(key), row); cell.scope = 'col'; }); head.append(row); table.append(head);
    const body = document.createElement('tbody');
    rows.forEach(values => { const row = document.createElement('tr'); values.forEach(value => text('td', value, row)); body.append(row); });
    table.append(body); scroll.append(table); root.append(scroll);
  }
  text('p', t('runtimeHelp'));
  const cards = document.createElement('div'); cards.className = 'stats-cards'; root.append(cards);
  const snapshot = value.storage.snapshot;
  for (const [key, label] of [['objects', 'objectCount'], ['logical_bytes', 'logicalBytes']]) {
    const card = document.createElement('div'); text('span', t(label), card);
    text('strong', !snapshot ? '—' : key === 'objects' ? number(snapshot[key]) : size(snapshot[key]), card); cards.append(card);
  }
  for (const [label, content] of [['physicalBytes', snapshot ? size(snapshot.chunks.stored_bytes) : '—'], ['unreferencedBytes', snapshot ? size(snapshot.unreferenced.stored_bytes) : '—'], ['cacheRate', percent(value.io.cache_hit_rate)], ['residentMemory', value.process_memory?.rss_bytes == null ? '—' : size(value.process_memory.rss_bytes)]]) {
    const card = document.createElement('div'); text('span', t(label), card); text('strong', content, card); cards.append(card);
  }
  const inventoryNote = text('p', !snapshot ? t('snapshotPending') : t('snapshotTime', { time: date(snapshot.as_of), interval: number(value.storage.refresh_interval_seconds) }));
  inventoryNote.id = 'snapshot-state';
  if (value.storage.collecting) inventoryNote.append(document.createTextNode(' ' + t('snapshotCollecting')));
  if (value.storage.last_error) inventoryNote.append(document.createTextNode(' ' + t('snapshotFailed')));
  else if (snapshot && value.storage.stale) inventoryNote.append(document.createTextNode(' ' + t('snapshotStale')));
  const runtimeRows = Object.entries(value.runtime.http).map(([name, metric]) => [t('listener_' + name), number(metric.completed), number(metric.active), percent(metric.failure_rate), number(metric.client_errors), number(metric.server_errors), number(metric.canceled), milliseconds(metric.duration_ms.mean), milliseconds(metric.duration_ms.p95), size(metric.bytes)]);
  table('httpStatistics', ['listener', 'completedRequests', 'activeRequests', 'failureRate', 'clientErrors', 'serverErrors', 'canceled', 'meanLatency', 'p95Latency', 'responseBytes'], runtimeRows);
  const backendRows = Object.entries(value.io.backend).map(([name, metric]) => [name.toUpperCase(), number(metric.completed), number(metric.active), number(metric.failed), number(metric.canceled), milliseconds(metric.duration_ms.mean), milliseconds(metric.duration_ms.p95), size(metric.bytes)]);
  table('backendStatistics', ['method', 'completedRequests', 'activeRequests', 'failed', 'canceled', 'meanLatency', 'p95Latency', 'transferredBytes'], backendRows);
  const cleanup = value.cleanup, lastRun = cleanup.last_run;
  text('p', lastRun ? t('cleanupLastRun', { time: date(lastRun.finished_at), duration: milliseconds(lastRun.duration_ms) }) : t('cleanupPendingRun'));
  if (cleanup.running) text('p', t('cleanupRunning'));
  if (lastRun?.budget_exhausted) text('p', t('cleanupBudget'));
  if (lastRun?.last_error) text('p', t('cleanupFailed'));
  table('databaseCleanup', ['cleanupCategory', 'cleanupRemoved', 'cleanupPending', 'cleanupOldest'], ['chunks', 'uploads', 'tasks', 'sessions'].map(kind => [
    t('cleanup_' + kind), lastRun ? number(lastRun.deleted[kind]) : '—',
    snapshot ? number(snapshot.cleanup[kind].eligible) : '—', snapshot?.cleanup[kind].oldest_at ? date(snapshot.cleanup[kind].oldest_at) : '—',
  ]));
  text('p', t('cleanupHelp'));
  if (!snapshot) return;
  text('p', t('storageHelp'));
  const live = snapshot.live;
  table('spaceSavings', ['metric', 'size', 'savingRate'], [
    [t('dedupSavings'), size(live.reference_bytes - live.raw_bytes), live.reference_bytes ? percent(1 - live.raw_bytes / live.reference_bytes) : '—'],
    [t('compressionSavings'), size(live.raw_bytes - live.payload_bytes), live.raw_bytes ? percent(1 - live.payload_bytes / live.raw_bytes) : '—'],
    [t('liveStoredBytes'), size(live.stored_bytes), '—'],
    [t('gcEligibleBytes'), size(snapshot.unreferenced.eligible_bytes), '—'],
    [t('unconfirmedBytes'), size(snapshot.chunks.unconfirmed_bytes), '—'],
  ]);
  table('bucketStatistics', ['buckets', 'objectCount', 'logicalBytes'], snapshot.buckets.map(bucket => [bucket.name, number(bucket.objects), size(bucket.logical_bytes)]));
  if (snapshot.buckets_truncated) text('p', t('bucketsTruncated'));
  table('taskStatistics', ['taskState', 'count'], ['queued', 'running', 'paused', 'completed', 'failed'].map(state => [t(state), number(snapshot.tasks[state] || 0)]));
  text('p', t('activeMultipart', { count: number((snapshot.uploads.active || 0) + (snapshot.uploads.completing || 0)) }));
  table('databaseTables', ['databaseTable', 'totalSize', 'indexSize', 'liveRows', 'deadRows', 'lastAutovacuum', 'lastAutoanalyze'], snapshot.database.map(row => [
    row.table, size(row.total_bytes), size(row.index_bytes), number(row.live_rows_estimate), number(row.dead_rows_estimate),
    row.last_autovacuum ? date(row.last_autovacuum) : '—', row.last_autoanalyze ? date(row.last_autoanalyze) : '—',
  ]));
  text('p', t('databaseHelp'));
}
async function detail(key) {
  const request = ++view, params = { bucket: $('buckets').value, key };
  const value = await api('/api/object?' + query(params));
  if (request !== view) return;
  panel = { type: 'details', value, params }; renderPanel(true);
}
const corsMethods = ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'OPTIONS'];
function addCorsRule(rule = { origins: [], methods: ['GET', 'HEAD'] }) {
  if ($('cors-rules').children.length >= 100) { notice(translatedError('corsInvalid')); return; }
  const fieldset = document.createElement('fieldset'), legend = document.createElement('legend');
  legend.dataset.i18n = 'corsRule'; fieldset.append(legend);
  for (const [name, title] of [['origins', 'corsOrigins'], ['headers', 'corsHeaders'], ['expose', 'corsExpose'], ['max_age', 'corsMaxAge']]) {
    const label = document.createElement('label'), span = document.createElement('span');
    span.dataset.i18n = title;
    const input = document.createElement(name === 'max_age' ? 'input' : 'textarea');
    input.name = name;
    if (name === 'max_age') { input.type = 'number'; input.min = '0'; input.max = '4294967295'; input.required = true; input.value = rule.max_age ?? 0; }
    else { input.rows = 2; input.value = (rule[name] || []).join('\n'); input.required = name === 'origins'; }
    label.append(span, input); fieldset.append(label);
  }
  const methods = document.createElement('fieldset'), title = document.createElement('legend');
  title.dataset.i18n = 'corsMethods'; methods.className = 'cors-methods'; methods.append(title);
  for (const method of corsMethods) {
    const label = document.createElement('label'), input = document.createElement('input');
    input.type = 'checkbox'; input.name = 'methods'; input.value = method; input.checked = rule.methods.includes(method);
    label.append(input, document.createTextNode(method)); methods.append(label);
  }
  fieldset.append(methods);
  const remove = button(t('corsRemove'), () => fieldset.remove()); remove.type = 'button'; remove.dataset.i18n = 'corsRemove';
  fieldset.append(remove); $('cors-rules').append(fieldset); translate(fieldset);
}
function renderCors(rules) {
  $('cors-rules').replaceChildren(); rules.forEach(addCorsRule);
}
function corsValues() {
  return [...$('cors-rules').children].map(fieldset => ({
    ...Object.fromEntries(['origins', 'headers', 'expose'].map(name => [name, fieldset.querySelector(`[name="${name}"]`).value.split('\n').map(v => v.trim()).filter(Boolean)])),
    methods: [...fieldset.querySelectorAll('[name="methods"]:checked')].map(input => input.value),
    max_age: Number(fieldset.querySelector('[name="max_age"]').value),
  }));
}
$('language').value = locale;
setLocale(locale); translate();
$('language').addEventListener('change', event => {
  setLocale(event.target.value); translate(); renderFiles(); renderPanel(); notice(currentNotice);
});
$('login').addEventListener('submit', async event => {
  event.preventDefault();
  try {
    const form = new FormData(event.target);
    const reply = await api('/api/login', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: form.get('username'), password: form.get('password') }) });
    csrf = reply.csrf_token;
    event.target.reset(); notice(); await enter();
  } catch (error) { notice(error); }
});
$('logout').addEventListener('click', async () => {
  try { await api('/api/logout', { method: 'POST', headers: { 'X-CSRF-Token': csrf } }); csrf = ''; showLogin(); }
  catch (error) { notice(error); }
});
$('buckets').addEventListener('change', () => { prefix = ''; list().catch(notice); });
$('refresh').addEventListener('click', () => list().catch(notice));
$('more').addEventListener('click', () => list(true).catch(notice));
$('cors').addEventListener('click', async () => {
  if (!$('buckets').value) return;
  const request = ++view, params = { bucket: $('buckets').value, name: $('buckets').selectedOptions[0].textContent };
  try {
    const value = await api('/api/buckets/' + params.bucket + '/cors');
    if (request === view) { panel = { type: 'cors', value, params }; renderPanel(true); }
  } catch (error) { notice(error); }
});
$('cors-add').addEventListener('click', () => addCorsRule());
$('cors-clear').addEventListener('click', () => renderCors([]));
$('cors-preset').addEventListener('click', () => renderCors([{ origins: ['*'], methods: corsMethods, headers: ['*'], expose: ['*'], max_age: 86400 }]));
$('cors-form').addEventListener('submit', async event => {
  event.preventDefault();
  if (panel?.type !== 'cors') return;
  const current = panel, rules = corsValues();
  if (rules.some(rule => !rule.origins.length || !rule.methods.length)) { notice(translatedError('corsInvalid')); return; }
  $('cors-save').disabled = true;
  try {
    const value = await api('/api/buckets/' + current.params.bucket + '/cors', {
      method: 'PUT', headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrf }, body: JSON.stringify(rules),
    });
    if (panel === current) { panel.value = value; renderPanel(); notice({ translationKey: 'corsSaved' }); }
  } catch (error) { notice(error); }
  finally { $('cors-save').disabled = false; }
});
$('website').addEventListener('click', async () => {
  if (!$('buckets').value) return;
  const request = ++view, params = { bucket: $('buckets').value, name: $('buckets').selectedOptions[0].textContent };
  try {
    const value = await api('/api/buckets/' + params.bucket + '/website');
    if (request === view) { panel = { type: 'website', value, params }; renderPanel(true); }
  } catch (error) { notice(error); }
});
$('website-form').addEventListener('submit', async event => {
  event.preventDefault();
  if (panel?.type !== 'website') return;
  const current = panel;
  $('website-save').disabled = true;
  try {
    const value = await api('/api/buckets/' + current.params.bucket + '/website', {
      method: 'PUT', headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrf },
      body: JSON.stringify({ website_enabled: $('website-enabled').checked, index_document: $('index-document').value, error_document: $('error-document').value }),
    });
    if (panel === current) { panel.value = value; renderPanel(); notice({ translationKey: 'websiteSaved' }); }
  } catch (error) { notice(error); }
  finally { $('website-save').disabled = false; }
});
for (const [type, path] of [['status', '/api/status'], ['tasks', '/api/tasks']]) $(type).addEventListener('click', async () => {
  const request = ++view;
  try { const value = await api(path); if (request === view) { panel = { type, value }; renderPanel(true); } }
  catch (error) { notice(error); }
});
api('/api/session').then(reply => { csrf = reply.csrf_token; return enter(); }).catch(() => showLogin());
