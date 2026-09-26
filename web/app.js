import { locale, setLocale, t, translate } from './i18n.js';

const $ = id => document.getElementById(id);
let prefix = '', next = null, csrf = '', view = 0;
let folders = [], objects = [], panel = null, currentNotice = '', route = {}, tasksPage = null;
let selected = new Set(), timer = null, pending = null, executing = false, chunks = null, taskRefresh = 0, chunkRequest = 0;
let report = null, issueObjects = null, issueRequest = 0, objectRequest = 0;
const number = value => new Intl.NumberFormat(locale).format(value);
const date = value => new Date(value).toLocaleString(locale);
const query = values => new URLSearchParams(values).toString();
const node = (tag, text) => { const element = document.createElement(tag); if (text != null) element.textContent = text; return element; };
function translatedError(key, values = {}) { return Object.assign(new Error(t(key, values)), { translationKey: key, values }); }
function errorText(error) { return error.translationKey ? t(error.translationKey, error.values) : error instanceof Error ? error.message : error; }
function notice(error = '') { currentNotice = error; $('notice').textContent = errorText(error); }
async function api(path, options = {}) {
  let response;
  try { response = await fetch(path, { credentials: 'same-origin', ...options }); }
  catch { throw translatedError('networkError'); }
  const requestId = response.headers.get('x-request-id');
  if (!response.ok) {
    if (response.status === 403 && path !== '/api/login') {
      const session = await fetch('/api/session').catch(() => null);
      if (session?.status === 403) showLogin();
    }
    throw translatedError(requestId ? 'requestFailedId' : 'requestFailed', { status: response.status, requestId });
  }
  const value = response.status === 204 ? null : await response.json();
  if (options.method === 'POST' && value && !Array.isArray(value)) value.request_id = requestId;
  return value;
}
function write(path, body) { return api(path, { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrf }, body: JSON.stringify(body) }); }
function showLogin() { ++view; clearTimeout(timer); $('login').hidden = false; $('browser').hidden = true; $('logout').hidden = true; }
function button(text, click) {
  const element = node('button', text); element.type = 'button';
  element.addEventListener('click', () => Promise.resolve().then(click).catch(notice)); return element;
}
function size(value) {
  if (value < 0) return '−' + size(-value);
  if (value < 1024) return number(value) + ' B';
  const unit = Math.min(5, Math.floor(Math.log(value) / Math.log(1024)));
  return new Intl.NumberFormat(locale, { maximumFractionDigits: 1 }).format(value / 1024 ** unit) + ' ' + ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'][unit];
}
function table(headings, rows) {
  const table = node('table'), head = node('thead'), tr = node('tr'), body = node('tbody');
  headings.forEach(key => { const cell = node('th', t(key)); cell.scope = 'col'; tr.append(cell); }); head.append(tr); table.append(head);
  rows.forEach(values => { const row = node('tr'); values.forEach(value => { const cell = node('td'); cell.append(value instanceof Node ? value : document.createTextNode(value ?? '—')); row.append(cell); }); body.append(row); });
  table.append(body); return table;
}
function readRoute() {
  const p = new URLSearchParams(location.search);
  return { page: ['objects', 'settings', 'status', 'tasks'].includes(p.get('page')) ? p.get('page') : 'objects', bucket: p.get('bucket') || '', prefix: p.get('prefix') || '', token: p.get('token') || '', recursive: p.get('recursive') === 'true', key: p.get('key'), section: p.get('section') || '', state: p.get('state') || '', task: p.get('task'), taskToken: p.get('taskToken') || '' };
}
function go(values, previousPage = false) {
  const url = new URL(location.href);
  for (const [key, value] of Object.entries(values)) {
    if (value == null || value === '' || value === false) url.searchParams.delete(key);
    else url.searchParams.set(key, value);
  }
  const before = new URL(location.href), state = history.state || { index: 0 };
  const sameListing = ['page', 'bucket', 'prefix', 'recursive', 'token', 'state', 'taskToken'].every(key => before.searchParams.get(key) === url.searchParams.get(key));
  if (url.href !== location.href) history.pushState({ index: state.index + 1, previousPage: previousPage || (sameListing && state.previousPage), previousIndex: previousPage ? state.index : sameListing ? state.previousIndex : null }, '', url);
  return loadView();
}
async function enter() {
  const buckets = await api('/api/buckets'); $('buckets').replaceChildren();
  for (const bucket of buckets) { const option = node('option', bucket.name); option.value = bucket.id; $('buckets').append(option); }
  const all = node('option', t('allBuckets')); all.value = ''; all.dataset.i18n = 'allBuckets'; $('integrity-bucket').replaceChildren(all);
  for (const bucket of buckets) { const option = node('option', bucket.name); option.value = bucket.name; $('integrity-bucket').append(option); }
  const wanted = readRoute().bucket;
  if (buckets.some(b => b.id === wanted)) $('buckets').value = wanted;
  const url = new URL(location.href);
  if ($('buckets').value) url.searchParams.set('bucket', $('buckets').value);
  history.replaceState(history.state || { index: 0 }, '', url);
  $('login').hidden = true; $('browser').hidden = false; $('logout').hidden = false;
  await loadView();
}
function breadcrumbs() {
  const root = $('breadcrumbs'); root.replaceChildren(button(t('root'), () => go({ prefix: '', token: null, key: null, recursive: false })));
  let path = '';
  // Preserve empty segments: object keys are not filesystem paths.
  const parts = prefix.split('/');
  parts.forEach((part, index) => {
    if (index === parts.length - 1) return;
    path += part + '/'; const target = path;
    root.append(button(part || '/', () => go({ prefix: target, token: null, key: null, recursive: false })));
  });
}
function renderFiles() {
  breadcrumbs(); $('files').replaceChildren();
  const trim = prefix.endsWith('/') ? prefix.length : 0;
  for (const folder of folders) {
    const tr = node('tr'); tr.append(node('td'));
    const name = node('td'); name.append(button(t('folder', { name: folder.slice(trim) }), () => go({ prefix: folder, token: null, recursive: false }))); tr.append(name);
    for (let i = 0; i < 3; i++) tr.append(node('td')); $('files').append(tr);
  }
  for (const object of objects) {
    const tr = node('tr'), select = node('input'), cell = node('td'), name = node('td');
    select.type = 'checkbox'; select.checked = selected.has(object.id); select.setAttribute('aria-label', t('selectObject', { key: object.object_key }));
    select.addEventListener('change', () => { if (select.checked) selected.add(object.id); else selected.delete(object.id); renderSelection(); });
    cell.append(select); name.append(button(object.object_key.slice(trim) || object.object_key, () => go({ key: object.object_key })));
    tr.append(cell, name, node('td', size(object.size)), node('td', t(object.public_read ? 'public' : 'private')), node('td', date(object.touched_at))); $('files').append(tr);
  }
  $('empty-files').hidden = folders.length + objects.length !== 0;
  $('more').hidden = !next; $('previous').hidden = !(route.token && history.state?.previousPage);
  renderSelection();
}
function renderSelection() {
  $('selected-count').textContent = t('selectedCount', { count: number(selected.size) });
  $('select-page').checked = objects.length > 0 && selected.size === objects.length;
  $('select-page').indeterminate = selected.size > 0 && selected.size < objects.length;
  $('select-page').disabled = objects.length === 0;
  for (const id of ['bulk-public', 'bulk-private', 'bulk-delete']) $(id).disabled = selected.size === 0;
}
function fields(values) {
  $('facts').replaceChildren();
  for (const [label, value] of values) $('facts').append(node('dt', t(label)), node('dd', value ?? '—'));
}
async function loadView() {
  const request = ++view; clearTimeout(timer); route = readRoute(); prefix = route.prefix;
  report = null; issueObjects = null; $('integrity-report').hidden = true;
  $('integrity-create').hidden = route.page !== 'tasks' || route.task !== null;
  notice(); panel = null; chunks = null; $('preview').replaceChildren(); $('chunks').replaceChildren(); $('chunks').hidden = true;
  $('detail').hidden = true; $('objects-view').hidden = route.page !== 'objects' || route.key !== null;
  $('settings-view').hidden = route.page !== 'settings'; $('tasks-view').hidden = route.page !== 'tasks';
  $('bucket-toolbar').hidden = !['objects', 'settings'].includes(route.page);
  for (const [id, page] of [['objects-tab', 'objects'], ['settings', 'settings'], ['status', 'status'], ['tasks', 'tasks']]) {
    if (route.page === page) $(id).setAttribute('aria-current', 'page'); else $(id).removeAttribute('aria-current');
  }
  $('buckets').value = route.bucket; $('search-prefix').value = prefix; $('recursive').checked = route.recursive; $('task-state').value = route.state;
  if (['objects', 'settings'].includes(route.page) && !$('buckets').value) { notice(translatedError('noBuckets')); return; }
  try {
    if (route.page === 'objects') {
      if (route.key !== null) {
        const params = { bucket: route.bucket, key: route.key }, value = await api('/api/object?' + query(params));
        if (request !== view) return;
        panel = { type: 'details', value, params }; renderPanel();
      } else {
        objects = []; folders = []; next = null; selected.clear(); renderFiles(); $('empty-files').hidden = true;
        const params = { bucket: route.bucket, prefix, recursive: route.recursive };
        if (route.token) params.token = route.token;
        const value = await api('/api/objects?' + query(params)); if (request !== view) return;
        objects = value.objects; folders = value.prefixes; next = value.next_token; renderFiles();
      }
    } else if (route.page === 'settings') {
      if (!['cors', 'website'].includes(route.section)) return;
      const params = { bucket: route.bucket, name: $('buckets').selectedOptions[0].textContent };
      const value = await api('/api/buckets/' + route.bucket + '/' + route.section); if (request !== view) return;
      panel = { type: route.section, value, params }; renderPanel(true);
    } else if (route.page === 'status') {
      const value = await api('/api/status'); if (request !== view) return;
      panel = { type: 'status', value }; renderPanel();
    } else await refreshTasks(request);
  } catch (error) { if (request === view) notice(error); }
}
function renderPanel(resetForm = false) {
  if (!panel) return;
  const { type, value, params } = panel;
  $('detail').hidden = false; $('detail-title').textContent = t(type); $('detail-title').dataset.i18n = type;
  $('info').textContent = JSON.stringify(value, null, 2); $('actions').replaceChildren(); $('task-list').replaceChildren();
  $('statistics').hidden = type !== 'status'; $('statistics').replaceChildren();
  $('website-form').hidden = type !== 'website'; $('cors-form').hidden = type !== 'cors';
  $('integrity-report').hidden = type !== 'taskDetails' || value.kind !== 'integrity';
  if (type === 'details') {
    const object = value.object, metadata = object.metadata, mime = (metadata.content_type || '').split(';')[0].trim();
    fields([['objectKey', object.object_key], ['objectVersion', object.id], ['size', size(object.size)], ['access', t(object.public_read ? 'public' : 'private')], ['updated', date(object.touched_at)], ['contentType', metadata.content_type], ['etag', object.etag], ['cacheControl', metadata.cache_control], ['contentDisposition', metadata.content_disposition], ['contentEncoding', metadata.content_encoding], ['contentLanguage', metadata.content_language], ['expires', metadata.expires]]);
    if (metadata.user && Object.keys(metadata.user).length) $('task-list').append(node('h3', t('userMetadata')), table(['name', 'value'], Object.entries(metadata.user)));
    $('task-list').append(node('h3', t('bucketGrants')));
    for (const grant of value.bucket_grants) $('task-list').append(node('p', grant.access_key + ' — ' + t(grant.writable ? 'readWrite' : 'readOnly')));
    if (!value.bucket_grants.length) $('task-list').append(node('p', t('noGrants')));
    const download = node('a', t('download')); download.className = 'button'; download.href = '/api/download?' + query(params);
    $('actions').append(button(t('backToFiles'), () => go({ key: null })), download, button(t('copyKey'), () => copyKey(object.object_key)));
    const image = ['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'image/avif'].includes(mime), video = ['video/mp4', 'video/webm'].includes(mime);
    if (image || video) $('actions').append(button(t('preview'), () => {
      const media = node(image ? 'img' : 'video'); media.src = '/api/download?' + query({ ...params, preview: 'true' });
      if (image) media.alt = object.object_key; else media.controls = true; $('preview').replaceChildren(media);
    }));
    $('actions').append(button(t(object.public_read ? 'makePrivate' : 'makePublic'), () => confirmObjects(object.public_read ? 'private' : 'public-read', [object])), button(t('deleteObjects'), () => confirmObjects('delete', [object])), button(t('showChunks'), () => loadChunks(null, [])));
  } else if (type === 'cors' || type === 'website') {
    fields([['buckets', params.name]]);
    if (resetForm && type === 'cors') renderCors(value);
    if (resetForm && type === 'website') { $('website-enabled').checked = value.website_enabled; $('index-document').value = value.index_document; $('error-document').value = value.error_document; }
  } else if (type === 'status') {
    fields([['version', value.version], ['startedAt', date(value.runtime.started_at)], ['uptime', number(value.runtime.uptime_seconds) + ' s'], ['cpu', number(value.resources.available_cpus)], ['memory', size(value.resources.memory_bytes)], ['multipartBytes', size(value.local_bytes[0]) + ' / ' + limit(value.io.multipart_limit_bytes)], ['cacheBytes', size(value.local_bytes[1]) + ' / ' + limit(value.io.cache_limit_bytes)], ['gc', t(value.gc_paused ? 'paused' : value.gc_running ? 'running' : 'enabled')], ['maintenance', t(value.maintenance ? 'enabled' : 'disabled')], ['dataSlots', number(value.data_slots_available) + ' / ' + number(value.resources.data_slots)], ['dbPool', number(value.db_pool_idle) + ' / ' + number(value.db_pool_size)], ['cpuSlots', number(value.io.cpu_slots_available) + ' / ' + number(value.resources.cpu_jobs)], ['gcDeleted', number(value.runtime.gc_deleted)], ['gcFailures', number(value.runtime.gc_failures)]]);
    renderStatistics(value); $('statistics').prepend(button(t('refreshStatistics'), loadView));
  } else if (type === 'taskDetails') {
    fields([['taskId', value.id], ['taskType', t(value.kind)], ['taskState', t(value.state)], ['processed', number(value.processed)], ['created', date(value.created_at)], ['updated', date(value.updated_at)], ['taskCursor', value.cursor], ['taskError', value.error]]);
    $('task-list').append(node('p', t('taskHelp')), node('pre', JSON.stringify(value.detail, null, 2)));
    $('actions').append(button(t('closeDetails'), () => go({ task: null })));
    if (['queued', 'running'].includes(value.state)) $('actions').append(button(t('pauseTask'), () => confirmTask('pause', value)));
    if (['paused', 'failed'].includes(value.state)) $('actions').append(button(t(value.state === 'failed' ? 'retryTask' : 'resumeTask'), () => confirmTask('resume', value)));
    if (value.kind === 'integrity') {
      const d = value.detail;
      $('task-list').replaceChildren(node('p', t('integrityCoverage')), table(['metric', 'value'], [
        [t('integrityMode'), t('mode_' + d.mode)], [t('integrityPhase'), t('phase_' + d.phase)],
        [t('integrityBucket'), d.bucket ?? t('allBuckets')], [t('objectKey'), d.key ?? '—'],
        [t('objectsChecked'), number(d.objects_checked)], [t('chunksChecked'), number(d.chunks_checked)],
        [t('bytesChecked'), size(d.bytes_checked)], [t('findings'), number(d.issues)], [t('skipped'), number(d.skipped)],
      ]));
      if (value.state === 'completed') {
        $('task-list').prepend(node('p', t(d.issues ? 'integrityFound' : 'integrityClear')));
        const link = node('a', t('exportReport')); link.className = 'button'; link.href = '/api/tasks/' + value.id + '/report'; $('actions').append(link);
      }
      renderIssues();
    }
  }
}
function renderTasks() {
  $('tasks-table').hidden = route.task !== null;
  if (!tasksPage) return;
  const rows = tasksPage.tasks;
  $('tasks-table').replaceChildren(rows.length ? table(['taskId', 'taskType', 'taskState', 'processed', 'updated'], rows.map(task => [button(task.id, () => go({ task: task.id })), t(task.kind), t(task.state), number(task.processed), date(task.updated_at)])) : node('p', t('noTasks')));
  $('tasks-more').hidden = route.task !== null || !tasksPage.next_token; $('tasks-previous').hidden = route.task !== null || !(route.taskToken && history.state?.previousPage);
}
async function refreshTasks(request = view) {
  clearTimeout(timer);
  const refresh = ++taskRefresh, current = () => request === view && refresh === taskRefresh;
  const focused = document.activeElement;
  const focusText = focused?.matches('#tasks-table button, #actions button') ? focused.textContent : null;
  const params = {}; if (route.state) params.state = route.state; if (route.taskToken) params.token = route.taskToken;
  const taskId = route.task;
  try {
    const value = await api('/api/tasks?' + query(params)); if (!current()) return;
    tasksPage = value; renderTasks();
    if (taskId) { const task = await api('/api/tasks/' + encodeURIComponent(taskId)); if (!current()) return; panel = { type: 'taskDetails', value: task }; renderPanel(); if (task.kind === 'integrity') await loadIssues(report?.after ?? null, report?.previous ?? []); }
    if (focusText && (document.activeElement === focused || document.activeElement === document.body)) [...document.querySelectorAll('#tasks-table button, #actions button')].find(b => b.textContent === focusText)?.focus({ preventScroll: true });
  } catch (error) { if (current()) notice(error); }
  finally { if (current()) scheduleTasks(); }
}
function scheduleTasks() {
  clearTimeout(timer);
  if (panel?.type === 'taskDetails' && panel.value.kind === 'integrity' && ['completed', 'failed', 'paused'].includes(panel.value.state)) return;
  if (route.page === 'tasks' && !document.hidden && !$('browser').hidden && $('auto-tasks').checked) timer = setTimeout(() => refreshTasks(), 5000);
}
async function copyKey(key) {
  try { if (navigator.clipboard) { await navigator.clipboard.writeText(key); notice({ translationKey: 'copied' }); return; } } catch {}
  $('copy-value').value = key; $('copy-dialog').showModal(); $('copy-value').select();
}
async function loadChunks(after, previous) {
  const current = panel; if (current?.type !== 'details') return;
  const request = ++chunkRequest;
  const params = { ...current.params, version: current.value.object.id }; if (after !== null) params.after = after;
  const value = await api('/api/object/chunks?' + query(params)); if (panel !== current || request !== chunkRequest) return;
  chunks = { value, after, previous }; renderChunks();
}
function renderChunks() {
  if (!chunks) return;
  const root = $('chunks'), { value, after, previous } = chunks; root.hidden = false;
  root.replaceChildren(node('h3', t('showChunks')), node('p', t('chunksHelp')));
  const scroll = node('div'); scroll.className = 'table-scroll';
  scroll.append(table(['chunkId', 'offset', 'referenceLength', 'rawSize', 'encodedSize', 'compression', 'savingRate', 'encryption'], value.chunks.map(c => [c.id, number(BigInt(c.offset_bytes)), size(c.length), size(c.raw_size), c.stored_size == null ? '—' : size(c.stored_size), c.compression, c.payload_size == null ? '—' : percent(1 - c.payload_size / c.raw_size), c.algorithm]))); root.append(scroll);
  if (previous.length) root.append(button(t('previous'), () => loadChunks(previous.at(-1), previous.slice(0, -1))));
  if (value.next_offset !== null) root.append(button(t('more'), () => loadChunks(value.next_offset, [...previous, after])));
}
async function loadIssues(after, previous) {
  const task = route.task, request = ++issueRequest, currentView = view;
  const value = await api('/api/tasks/' + task + '/issues?' + query(after === null ? {} : { after }));
  if (task !== route.task || request !== issueRequest || currentView !== view) return;
  report = { value, after, previous }; renderIssues();
}
async function loadIssueObjects(issue, after = null, previous = []) {
  const task = route.task, request = ++objectRequest, currentView = view;
  const value = await api('/api/tasks/' + task + '/issues/' + issue.id + '/objects?' + query(after === null ? {} : { after: JSON.stringify(after) }));
  if (task !== route.task || request !== objectRequest || currentView !== view) return;
  issueObjects = { issue, value, after, previous }; renderIssues();
}
function renderIssues() {
  const root = $('integrity-report');
  if (panel?.type !== 'taskDetails' || panel.value.kind !== 'integrity') { root.hidden = true; return; }
  root.hidden = false; root.replaceChildren(node('h3', t('findings')));
  if (!report) return;
  const scroll = node('div'); scroll.className = 'table-scroll';
  scroll.tabIndex = 0; scroll.setAttribute('role', 'region'); scroll.setAttribute('aria-label', t('findings'));
  scroll.append(report.value.issues.length ? table(['result', 'chunkId', 'objectKey', 'created', 'affectedObjects'], report.value.issues.map(i => [t('issue_' + i.code), i.chunk_id ?? '—', i.object_key ?? '—', date(i.created_at), button(t('affectedObjects'), () => loadIssueObjects(i))])) : node('p', t('noFindings'))); root.append(scroll);
  if (report.previous.length) root.append(button(t('previous'), () => { issueObjects = null; return loadIssues(report.previous.at(-1), report.previous.slice(0, -1)); }));
  if (report.value.next_after !== null) root.append(button(t('more'), () => { issueObjects = null; return loadIssues(report.value.next_after, [...report.previous, report.after]); }));
  if (issueObjects) {
    const { issue, value, after, previous } = issueObjects, section = node('section'); section.id = 'issue-objects';
    section.append(node('h4', t('affectedObjects')), node('p', t('affectedHelp')), node('pre', JSON.stringify({ code: issue.code, chunk_id: issue.chunk_id, storage_id: issue.storage_id, version: issue.stream_id, detail: issue.detail }, null, 2)));
    for (const object of value.objects) section.append(button(object.bucket + '/' + object.key, () => go({ page: 'objects', bucket: object.bucket_id, key: object.key, task: null, token: null })));
    if (!value.objects.length) section.append(node('p', t('noAffectedObjects')));
    if (previous.length) section.append(button(t('previous'), () => loadIssueObjects(issue, previous.at(-1), previous.slice(0, -1))));
    if (value.next !== null) section.append(button(t('more'), () => loadIssueObjects(issue, value.next, [...previous, after])));
    root.append(section);
  }
}
function showConfirmation(title, help, items) {
  $('confirm-title').textContent = title; $('confirm-help').textContent = help; $('confirm-items').replaceChildren(items); $('confirm-notice').textContent = '';
  $('confirm-execute').hidden = false; $('confirm-execute').disabled = false; $('confirm-cancel').textContent = t('cancel');
  if (!$('confirm-dialog').open) $('confirm-dialog').showModal(); $('confirm-cancel').focus();
}
function confirmObjects(action, targets) {
  if (!targets.length || executing) return;
  pending = { kind: 'objects', bucket: route.bucket, action, objects: targets.map(o => ({ key: o.object_key, version: o.id })) };
  renderConfirmation();
}
function confirmTask(action, task) {
  if (executing) return;
  pending = { kind: 'task', action, task }; renderConfirmation();
}
function renderConfirmation() {
  if (!pending || executing) return;
  if (pending.kind === 'objects') showConfirmation(t('action_' + pending.action), t('confirmObjects', { count: number(pending.objects.length) }), table(['objectKey'], pending.objects.map(o => [o.key])));
  else showConfirmation(t(pending.action === 'pause' ? 'pauseTask' : 'resumeTask'), t('confirmTask', { id: pending.task.id, type: t(pending.task.kind) }), node('pre', JSON.stringify(pending.task.detail, null, 2)));
}
async function executeConfirmation() {
  if (!pending || executing) return;
  const current = pending, url = location.href; executing = true; $('confirm-execute').disabled = true;
  $('confirm-cancel').disabled = true;
  $('confirm-notice').textContent = t('working');
  try {
    if (current.kind === 'objects') {
      const value = await write('/api/objects/actions', { bucket: current.bucket, action: current.action, objects: current.objects });
      const resultTable = table(['objectKey', 'result'], value.results.map(r => [r.key, t(r.status === 200 ? 'operationSuccess' : r.status === 412 ? 'objectChanged' : 'operationFailed', { status: r.status })]));
      if (location.href === url) {
        if (route.key !== null && current.action === 'delete' && value.results[0]?.status === 200) await go({ key: null }); else await loadView();
      }
      $('confirm-items').replaceChildren(resultTable); $('confirm-help').textContent = t('operationResults');
      $('confirm-notice').textContent = t('requestId', { id: value.request_id });
    } else {
      const value = await write('/api/tasks/' + current.task.id + '/actions', { action: current.action });
      if (location.href === url) await loadView(); $('confirm-help').textContent = t('operationSuccess');
      $('confirm-notice').textContent = t('requestId', { id: value.request_id });
    }
  } catch (error) { $('confirm-notice').textContent = errorText(error) + ' ' + t('verifyResults'); }
  finally { executing = false; pending = null; $('confirm-execute').hidden = true; $('confirm-cancel').disabled = false; $('confirm-cancel').textContent = t('close'); }
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
  table('databaseCleanup', ['cleanupCategory', 'cleanupRemoved', 'cleanupPending', 'cleanupOldest'], ['chunks', 'uploads', 'tasks', 'sessions', 'integrity_issues'].map(kind => [
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
$('language').value = locale; setLocale(locale); translate();
$('language').addEventListener('change', event => {
  setLocale(event.target.value); translate(); renderFiles(); renderPanel(); renderTasks(); renderChunks(); notice(currentNotice);
  if (pending && $('confirm-dialog').open && !executing) renderConfirmation();
});
$('login').addEventListener('submit', async event => {
  event.preventDefault();
  try { const form = new FormData(event.target); const reply = await api('/api/login', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: form.get('username'), password: form.get('password') }) }); csrf = reply.csrf_token; event.target.reset(); await enter(); }
  catch (error) { notice(error); }
});
$('logout').addEventListener('click', async () => { try { await api('/api/logout', { method: 'POST', headers: { 'X-CSRF-Token': csrf } }); csrf = ''; showLogin(); } catch (error) { notice(error); } });
window.addEventListener('popstate', () => { if (!$('browser').hidden) loadView(); });
for (const [id, page] of [['objects-tab', 'objects'], ['settings', 'settings'], ['status', 'status'], ['tasks', 'tasks']]) $(id).addEventListener('click', () => go({ page, key: null, task: null }));
$('buckets').addEventListener('change', () => go({ bucket: $('buckets').value, prefix: null, key: null, token: null }));
$('refresh').addEventListener('click', loadView);
$('search-form').addEventListener('submit', event => { event.preventDefault(); go({ prefix: $('search-prefix').value, recursive: $('recursive').checked, token: null, key: null }); });
$('locate-form').addEventListener('submit', event => { event.preventDefault(); go({ key: $('exact-key').value }); });
$('more').addEventListener('click', () => { if (next) go({ token: next }, true); });
$('previous').addEventListener('click', () => history.go(history.state.previousIndex - history.state.index));
$('select-page').addEventListener('change', event => { selected = new Set(event.target.checked ? objects.map(o => o.id) : []); renderFiles(); });
for (const [id, action] of [['bulk-public', 'public-read'], ['bulk-private', 'private'], ['bulk-delete', 'delete']]) $(id).addEventListener('click', () => confirmObjects(action, objects.filter(o => selected.has(o.id))));
$('confirm-cancel').addEventListener('click', () => { if (!executing) { pending = null; $('confirm-dialog').close(); } });
$('confirm-execute').addEventListener('click', executeConfirmation);
$('confirm-dialog').addEventListener('cancel', event => { if (executing) event.preventDefault(); else pending = null; });
$('copy-close').addEventListener('click', () => $('copy-dialog').close());
$('task-state').addEventListener('change', () => go({ state: $('task-state').value, taskToken: null, task: null }));
$('tasks-more').addEventListener('click', () => { if (tasksPage?.next_token) go({ taskToken: tasksPage.next_token, task: null }, true); });
$('tasks-previous').addEventListener('click', () => history.go(history.state.previousIndex - history.state.index));
$('refresh-tasks').addEventListener('click', loadView);
$('auto-tasks').addEventListener('change', scheduleTasks);
$('integrity-bucket').addEventListener('change', () => { $('integrity-key').disabled = !$('integrity-bucket').value; if ($('integrity-key').disabled) $('integrity-key').value = ''; });
$('integrity-form').addEventListener('submit', async event => {
  event.preventDefault(); $('integrity-start').disabled = true;
  try {
    const value = await write('/api/integrity', { mode: $('integrity-mode').value, bucket: $('integrity-bucket').value || null, key: $('integrity-key').value || null });
    await go({ page: 'tasks', task: value.task_id, state: null, taskToken: null });
  } catch (error) { notice(error); } finally { $('integrity-start').disabled = false; }
});
document.addEventListener('visibilitychange', scheduleTasks);
for (const type of ['cors', 'website']) $(type).addEventListener('click', () => go({ page: 'settings', section: type }));
$('cors-add').addEventListener('click', () => addCorsRule());
$('cors-clear').addEventListener('click', () => renderCors([]));
$('cors-preset').addEventListener('click', () => renderCors([{ origins: ['*'], methods: corsMethods, headers: ['*'], expose: ['*'], max_age: 86400 }]));
for (const type of ['cors', 'website']) $(type + '-form').addEventListener('submit', async event => {
  event.preventDefault(); if (panel?.type !== type) return;
  const current = panel, value = type === 'cors' ? corsValues() : { website_enabled: $('website-enabled').checked, index_document: $('index-document').value, error_document: $('error-document').value };
  if (type === 'cors' && value.some(rule => !rule.origins.length || !rule.methods.length)) { notice(translatedError('corsInvalid')); return; }
  $(type + '-save').disabled = true;
  try {
    const saved = await api('/api/buckets/' + current.params.bucket + '/' + type, { method: 'PUT', headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': csrf }, body: JSON.stringify(value) });
    if (panel === current) { panel.value = saved; renderPanel(); notice({ translationKey: type + 'Saved' }); }
  } catch (error) { notice(error); } finally { $(type + '-save').disabled = false; }
});
api('/api/session').then(reply => { csrf = reply.csrf_token; return enter(); }).catch(() => showLogin());
