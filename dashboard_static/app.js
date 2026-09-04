const $ = (id) => document.getElementById(id);
const MATCH_DIRECTORY_INTERVAL_MS = 10000;
const state = {
  timer: null,
  matchesTimer: null,
  matches: null,
  matchesLoading: false,
  activeMatches: [],
  capacity: {active: 0, max: 8, atCapacity: false},
  heavyTasks: null,
  sessionMatchId: '',
  sessionLocked: false,
  discoveryCollapsed: false,
  viewSequence: 0,
  refreshRequestSerial: 0,
  lastRenderedRefreshSerial: 0,
  actionPending: false,
  ocrAutomaticPublishing: true,
  publishAccounts: [],
  publishAccountsLoading: false,
  publishAccountsSaving: false,
  publishAccountsDirty: false,
  openTechnicalDetails: new Set(),
};

function matchId() { return $('match-id').value.trim(); }
function showNotice(message, kind = 'warning') { const el = $('notice'); el.textContent = message; el.className = `notice ${kind === 'error' ? 'error' : kind === 'success' ? 'success' : ''}`; }
function clearNotice() { $('notice').className = 'notice hidden'; }
function statusClass(value) { return String(value || 'uncertain').toLowerCase(); }
function fmtTime(unix) { return unix ? new Date(unix * 1000).toLocaleTimeString('zh-CN', {hour12:false}) : '--'; }
function fmtDuration(seconds) { const value = Math.max(0, Math.floor(Number(seconds) || 0)); const minutes = Math.floor(value / 60); const rest = value % 60; return minutes ? `${minutes}分 ${rest}秒` : `${rest}秒`; }
function escapeHtml(value) { return String(value ?? '').replace(/[&<>'"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c])); }
function finiteNumber(value) { if (value == null || value === '') return null; const number = Number(value); return Number.isFinite(number) ? number : null; }
const FRIENDLY_ERROR_CODES = {
  GIF_SOURCE_SECRET: '还没有配置直播地址，暂时无法获取比赛画面。',
  match_not_found: '没有找到这场比赛，请检查比赛 ID。',
  event_api_error: '比赛事件暂时无法获取，系统会继续重试。',
  api_error: '接口暂时没有正常返回，系统会继续重试。',
  timeout: '等待时间过长，系统会继续重试。',
  network_error: '网络连接暂时不稳定，请稍后再试。',
};
const FRIENDLY_TERM_REPLACEMENTS = [
  [/\bOCR\b/gi, '画面时间识别'], [/T-DEED/gi, '动作精剪'], [/shotmap/gi, '事件接口'],
  [/overview/gi, '赛况接口'], [/FFmpeg/gi, '视频接收'], [/Worker/gi, '处理进程'],
  [/PID/gi, '进程编号'], [/返回码/g, '结束状态'], [/锚点/g, '对应画面'],
  [/可信时钟/g, '最近读到的比赛时间'], [/目标时钟/g, '接口给出的比赛时间'],
  [/时钟识别/g, '比赛时间读取'], [/时钟检测/g, '比赛时间读取'], [/时钟连续性/g, '比赛时间前后是否连贯'],
  [/游标/g, '已检查到的位置'], [/扫描窗口/g, '检查范围'], [/缓存被淘汰/g, '历史画面当前已不在'],
  [/缓存尾部/g, '最新保存的视频位置'], [/缓存/g, '视频保存范围'], [/推理/g, '读取画面'],
  [/视频分片/g, '视频片段'], [/分片/g, '片段'], [/resource/g, '直播地址'], [/updated_at/g, '最近更新时间'],
];
function friendlyText(value, fallback = '') {
  if (value == null) return fallback;
  let text = typeof value === 'string' ? value.trim() : String(value);
  if (!text) return fallback;
  for (const [pattern, replacement] of FRIENDLY_TERM_REPLACEMENTS) text = text.replace(pattern, replacement);
  return text;
}
function friendlyErrorMessage(value, fallback = '操作暂时没有完成，请稍后再试。') {
  if (value == null || value === '') return fallback;
  const raw = typeof value === 'object' ? (value.error || value.message || value.detail || value.kind || value.code || '') : String(value);
  const normalized = String(raw || '').trim();
  if (!normalized) return fallback;
  const objectCode = typeof value === 'object' ? String(value.kind || value.code || value.error_kind || value.last_error_kind || '').trim() : '';
  const guideKey = window.DashboardErrorMessages.keyFor(objectCode) || window.DashboardErrorMessages.keyFor(normalized);
  const lower = normalized.toLowerCase();
  const inferredKey = guideKey || (lower.includes('timeout') || lower.includes('timed out') ? 'timeout' : lower.includes('demux') || lower.includes('input/output error') || lower.includes('broken pipe') || lower.includes('invalid data found') || lower.includes('end of file') ? 'ingest_error' : lower.includes('network') || lower.includes('connection') || lower.includes('fetch') || lower.includes('urlopen') ? 'network_error' : '');
  if (inferredKey) {
    const guide = window.DashboardErrorMessages.guideFor(inferredKey);
    return `${guide.title}。原因：${guide.cause} 影响：${guide.impact} 系统处理：${guide.system} 建议：${guide.action}`;
  }
  for (const [key, message] of Object.entries(FRIENDLY_ERROR_CODES)) {
    if (normalized === key || normalized.includes(key)) return message;
  }
  if (lower.includes('timeout') || lower.includes('timed out')) return FRIENDLY_ERROR_CODES.timeout;
  if (lower.includes('network') || lower.includes('connection') || lower.includes('fetch')) return FRIENDLY_ERROR_CODES.network_error;
  if (lower.includes('ffmpeg') || lower.includes('ingest')) return '视频接收暂时失败，系统会尝试重连。';
  if (lower.includes('ocr') || lower.includes('scoreboard') || lower.includes('clock')) return '没有读到清晰的比赛时间，系统会继续查找。';
  if (lower.includes('tdeed') || lower.includes('vision')) return '画面处理暂时失败，默认 GIF 仍可继续使用。';
  if (/^[a-z0-9_.:\-/\s]+$/i.test(normalized)) return fallback;
  return friendlyText(normalized, fallback);
}

function detailedErrorMessage(value, fallback = '操作暂时没有完成，请稍后再试。') {
  const message = friendlyErrorMessage(value, fallback);
  if (!message || message.includes('原因：') || message.includes('影响：')) return message;
  return `${message} 影响：本次更新或操作可能没有完成，已经保存的数据不会因此删除。系统处理：页面会保留现有结果，可重试的请求会继续尝试。建议：核对当前页面状态和最近一次重试时间；持续失败时再检查对应服务。`;
}

function errorGuide(value, fallbackTitle = '这一步没有完成') {
  const raw = typeof value === 'object' && value !== null ? (value.kind || value.code || value.error_kind || value.last_error_kind || value.error || value.message || '') : value;
  const guide = window.DashboardErrorMessages.guideFor(raw, fallbackTitle);
  if (guide.key) return guide;
  return {...guide, cause:friendlyErrorMessage(value, guide.cause)};
}
function articleAdminUrl(articleId) {
  const value = String(articleId || '').trim();
  return /^\d{1,20}$/.test(value)
    ? `https://dadmin.dongqiudi.com/admin/archives/articlePublish?articleId=${encodeURIComponent(value)}`
    : '';
}
function publicationAccountText(record) {
  if (!record || typeof record !== 'object') return '';
  const nested = record.publish_account && typeof record.publish_account === 'object'
    ? record.publish_account
    : {};
  const userId = String(record.account_user_id ?? nested.user_id ?? '').trim();
  const userName = String(record.account_user_name ?? nested.user_name ?? '').trim();
  if (!userId && !userName) return '';
  return `发布账号：${userName || '未提供名称'}${userId ? `（${userId}）` : ''}`;
}
function automaticArticleStatus(event, artifact) {
  const uploaded = artifact && artifact.uploaded_gif;
  const ready = artifact && (artifact.status === 'encoded' || (uploaded && uploaded.gif_id));
  if (!ready || !event.event_key) return '';
  const publication = event.ocr_draft && typeof event.ocr_draft === 'object' ? event.ocr_draft : null;
  if (!publication) {
    const label = state.ocrAutomaticPublishing ? '等待自动创建文章' : '自动发布未启用';
    const reason = state.ocrAutomaticPublishing
      ? 'OCR GIF 已生成，但文章任务尚未登记'
      : '服务配置关闭了 OCR 自动文章流程';
    return `<small class="publish-result pending">${label} · ${reason}</small>`;
  }
  const status = String(publication.status || 'queued');
  const eligibility = publication.publication_eligibility && typeof publication.publication_eligibility === 'object'
    ? publication.publication_eligibility
    : publication.ocr_article_eligibility && typeof publication.ocr_article_eligibility === 'object'
      ? publication.ocr_article_eligibility
      : publication.eligibility && typeof publication.eligibility === 'object'
        ? publication.eligibility
        : null;
  const publicationHeld = status === 'held'
    || publication.stage === 'publication_gate'
    || (eligibility && eligibility.eligible === false && !['published', 'success'].includes(status));
  const articleId = publication.article_id ? String(publication.article_id) : '';
  // New no-player tasks wait only in local SQLite and do not have an
  // article_id.  Keep the old wording for legacy platform drafts, but do not
  // tell the operator that a remote draft exists when it does not.
  const draftCreated = Boolean(articleId || Number(publication.draft_created_at_unix) > 0);
  const labels = {
    queued:'等待自动创建文章', creating:'正在创建文章草稿', creating_draft:'正在创建文章草稿',
    waiting_person: draftCreated ? '草稿已创建 · 等待球员信息' : '等待球员信息 · 尚未创建草稿', publishing:'正在自动发布',
    retry_wait:'自动发布暂时失败 · 系统将重试',
    published:'已自动发布', success:'草稿已创建', failed:'自动发布失败',
    held:'未自动发布'
  };
  const articleUrl = articleAdminUrl(articleId);
  const error = publicationHeld || !publication.error ? '' : `：${escapeHtml(publication.error)}`;
  const holdReason = eligibility && eligibility.reason
    ? String(eligibility.reason)
    : (publicationHeld && publication.error ? String(publication.error) : 'OCR GIF 未满足自动发布条件');
  const holdCode = eligibility && eligibility.reason_code
    ? String(eligibility.reason_code)
    : (publicationHeld && publication.error_code ? String(publication.error_code) : 'auto_publish_not_eligible');
  const reason = publicationHeld
    ? ` · 原因：${escapeHtml(holdReason)} · 错误码 ${escapeHtml(holdCode)}`
    : status === 'waiting_person'
    ? (publication.person_deadline_at_unix
      ? ` · ${draftCreated ? '等待球员信息至' : '本地等待球员信息至'} ${new Date(Number(publication.person_deadline_at_unix) * 1000).toLocaleTimeString('zh-CN', {hour12:false})}`
      : (draftCreated ? ' · 等待接口补齐球员信息' : ' · 本地等待球员信息'))
    : status === 'retry_wait'
      ? ' · 平台或网络暂时不可用，系统会自动重试'
      : status === 'failed'
        ? ` · ${publication.error_code ? `错误码 ${escapeHtml(publication.error_code)}` : '任务未完成'}`
        : status === 'success'
          ? ' · 旧任务仅创建了草稿，等待后续处理'
          : status === 'published' && publication.publish_reason === 'team_fallback'
            ? ' · 未获取球员，已使用球队名发布'
          : '';
  const link = articleId
    ? ` · ${articleUrl ? `<a href="${articleUrl}" target="_blank" rel="noopener noreferrer">查看文章 ${escapeHtml(articleId)}</a>` : `文章 ${escapeHtml(articleId)}`}`
    : '';
  const style = status === 'published' ? 'success' : publicationHeld ? 'retry' : status === 'failed' ? 'failed' : 'pending';
  const label = publicationHeld ? '未自动发布' : (labels[status] || '自动文章处理中');
  const account = publicationAccountText(publication);
  return `<small class="publish-result ${style}">${escapeHtml(label)}${reason}${error}${account ? ` · ${escapeHtml(account)}` : ''}${link}</small>`;
}

function setPublishAccountMessage(message, kind = '') {
  const element = $('publish-account-message');
  element.textContent = message;
  element.className = `publish-account-message${kind ? ` ${kind}` : ''}`;
}

function updatePublishAccountSummary() {
  const enabled = state.publishAccounts.filter(account => account.enabled !== false).length;
  const total = state.publishAccounts.length;
  $('publish-account-count').textContent = `${enabled} 个可用 · 共 ${total} 个${state.publishAccountsDirty ? ' · 未保存' : ''}`;
  $('publish-account-save').disabled = state.publishAccountsLoading || state.publishAccountsSaving || !state.publishAccountsDirty;
  $('publish-account-add').disabled = state.publishAccountsLoading || state.publishAccountsSaving;
}

function renderPublishAccounts() {
  const list = $('publish-account-list');
  if (!state.publishAccounts.length) {
    list.innerHTML = '<div class="publish-account-empty">账号池为空，新增账号后保存。</div>';
  } else {
    list.innerHTML = state.publishAccounts.map((account, index) => `
      <div class="publish-account-row" data-account-index="${index}">
        <input class="publish-account-id" inputmode="numeric" autocomplete="off" maxlength="20" value="${escapeHtml(account.user_id)}" aria-label="账号 ${index + 1} 的用户 ID" placeholder="user_id">
        <input class="publish-account-name" autocomplete="off" maxlength="64" value="${escapeHtml(account.user_name)}" aria-label="账号 ${index + 1} 的用户名称" placeholder="user_name">
        <label class="publish-account-enabled" title="启用或停用此账号"><input type="checkbox" ${account.enabled !== false ? 'checked' : ''} aria-label="启用账号 ${index + 1}"><span></span></label>
        <button class="account-icon-button publish-account-remove" type="button" aria-label="删除账号 ${index + 1}" title="删除账号">×</button>
      </div>
    `).join('');
  }
  updatePublishAccountSummary();
}

function accountRowsFromForm() {
  const accounts = [];
  const seen = new Set();
  for (const [index, row] of [...document.querySelectorAll('.publish-account-row')].entries()) {
    const userId = row.querySelector('.publish-account-id').value.trim();
    const userName = row.querySelector('.publish-account-name').value.trim();
    if (!/^[1-9]\d{0,18}$/.test(userId) || BigInt(userId) > 9223372036854775807n) {
      throw new Error(`第 ${index + 1} 个账号的 user_id 必须是有效的正整数。`);
    }
    if (!userName) throw new Error(`第 ${index + 1} 个账号缺少 user_name。`);
    if (userName.length > 64) throw new Error(`第 ${index + 1} 个账号名称不能超过 64 个字符。`);
    if (seen.has(userId)) throw new Error(`user_id ${userId} 重复，请只保留一条。`);
    seen.add(userId);
    accounts.push({user_id:userId, user_name:userName, enabled:row.querySelector('.publish-account-enabled input').checked});
  }
  return accounts;
}

async function loadPublishAccounts() {
  if (state.publishAccountsLoading || state.publishAccountsSaving) return;
  state.publishAccountsLoading = true;
  updatePublishAccountSummary();
  setPublishAccountMessage('正在读取发布账号。');
  try {
    const data = await requestJson('/api/article-publish/accounts');
    state.publishAccounts = (Array.isArray(data.accounts) ? data.accounts : []).map(account => ({
      user_id:String(account && account.user_id != null ? account.user_id : ''),
      user_name:String(account && account.user_name != null ? account.user_name : ''),
      enabled:!account || account.enabled !== false,
    }));
    state.publishAccountsDirty = false;
    renderPublishAccounts();
    const available = Number.isInteger(Number(data.available_count)) ? Number(data.available_count) : state.publishAccounts.filter(account => account.enabled).length;
    setPublishAccountMessage(available > 0 ? `发布时会从 ${available} 个启用账号中随机选择。` : '当前没有启用账号，发布会被阻止。', available > 0 ? 'success' : 'warning');
  } catch (error) {
    setPublishAccountMessage(`账号池读取失败：${friendlyErrorMessage(error.message)}`, 'error');
  } finally {
    state.publishAccountsLoading = false;
    updatePublishAccountSummary();
  }
}

async function savePublishAccounts() {
  if (state.publishAccountsSaving) return;
  let accounts;
  try {
    accounts = accountRowsFromForm();
  } catch (error) {
    setPublishAccountMessage(error.message, 'error');
    return;
  }
  state.publishAccountsSaving = true;
  updatePublishAccountSummary();
  setPublishAccountMessage('正在保存账号池。');
  try {
    const data = await requestJson('/api/article-publish/accounts', {method:'PUT', body:JSON.stringify({accounts})});
    state.publishAccounts = (Array.isArray(data.accounts) ? data.accounts : accounts).map(account => ({
      user_id:String(account.user_id), user_name:String(account.user_name), enabled:account.enabled !== false,
    }));
    state.publishAccountsDirty = false;
    renderPublishAccounts();
    const available = Number.isInteger(Number(data.available_count)) ? Number(data.available_count) : state.publishAccounts.filter(account => account.enabled).length;
    setPublishAccountMessage(available > 0 ? `已保存，发布时会从 ${available} 个启用账号中随机选择。` : '已保存；当前没有启用账号，发布会被阻止。', available > 0 ? 'success' : 'warning');
  } catch (error) {
    setPublishAccountMessage(`保存失败：${friendlyErrorMessage(error.message)}`, 'error');
  } finally {
    state.publishAccountsSaving = false;
    updatePublishAccountSummary();
  }
}
function setDiscoveryCollapsed(collapsed) {
  state.discoveryCollapsed = Boolean(collapsed);
  $('match-discovery').classList.toggle('collapsed', state.discoveryCollapsed);
  $('discovery-toggle').setAttribute('aria-expanded', String(!state.discoveryCollapsed));
}
function activeMatchIds() {
  return new Set(state.activeMatches.map(item => String(item.match_id || '')).filter(Boolean));
}
function isActiveMatch(id) {
  const normalized = String(id || '');
  return activeMatchIds().has(normalized) || (normalized === state.sessionMatchId && state.sessionLocked);
}
function applyControlAvailability() {
  const targetId = matchId();
  const viewedId = state.sessionMatchId;
  const targetIsActive = isActiveMatch(targetId);
  const viewedIsActive = isActiveMatch(viewedId);
  const capacityReached = state.capacity.atCapacity;
  $('match-id').disabled = false;
  $('load-btn').disabled = state.actionPending || !targetId;
  $('start-btn').disabled = state.actionPending || !targetId || targetIsActive || (capacityReached && !targetIsActive);
  $('demo-btn').disabled = state.actionPending || capacityReached;
  $('stop-btn').disabled = state.actionPending || !viewedId || !viewedIsActive;
  $('new-match-btn').disabled = state.actionPending;
  $('stop-btn').title = viewedId ? `停止当前查看的比赛 ${viewedId}` : '当前没有可停止的比赛';
  document.querySelectorAll('.discovery-match').forEach(button => { button.disabled = !button.dataset.matchId; });
  $('discovery-lock').classList.toggle('hidden', !capacityReached);
}
function syncSessionSelection(data, worker, lifecycle) {
  state.sessionMatchId = String(data.match_id || matchId());
  state.sessionLocked = Boolean(worker.running || worker.desired_running || worker.cleanup_process_group || ['starting', 'playing', 'finishing', 'stopping'].includes(lifecycle.state));
  if (state.matches) {
    renderActiveMatches(
      Array.isArray(state.matches.playing) ? state.matches.playing : [],
      Array.isArray(state.matches.upcoming) ? state.matches.upcoming : [],
    );
  }
  applyControlAvailability();
}
function renderSelectionHint() {
  const selectedId = matchId();
  const pending = Boolean(selectedId) && selectedId !== state.sessionMatchId;
  const hint = $('selection-hint');
  hint.textContent = pending ? `已选择比赛 ${selectedId}，点击“查询比赛”查看详情，或点击“启动此比赛”开始处理。` : '';
  hint.classList.toggle('hidden', !pending);
}
function startPlayUtcDate(value) {
  if (value == null || !String(value).trim()) return null;
  const raw = String(value).trim();
  const numeric = finiteNumber(raw);
  if (numeric != null && numeric >= 0) {
    const milliseconds = numeric > 1e12 ? numeric : numeric * 1000;
    const parsed = new Date(milliseconds);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }
  const naive = raw.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?$/);
  const parsed = naive
    ? new Date(Date.UTC(+naive[1], +naive[2] - 1, +naive[3], +naive[4], +naive[5], +(naive[6] || 0)))
    : new Date(raw);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}
function formatStartPlayBeijing(value, includeDate = false) {
  const parsed = startPlayUtcDate(value);
  if (!parsed) return null;
  return parsed.toLocaleString('zh-CN', {
    timeZone:'Asia/Shanghai', hour12:false,
    ...(includeDate ? {year:'numeric', month:'2-digit', day:'2-digit'} : {}),
    hour:'2-digit', minute:'2-digit',
  });
}
function matchStartLabel(match) {
  const startPlay = formatStartPlayBeijing(match.start_play);
  if (startPlay) return startPlay;
  const timestamp = finiteNumber(match.sort_timestamp);
  if (timestamp == null || timestamp <= 0) return '时间待定';
  const milliseconds = timestamp > 1e12 ? timestamp : timestamp * 1000;
  return new Date(milliseconds).toLocaleTimeString('zh-CN', {hour:'2-digit', minute:'2-digit', hour12:false});
}
function matchClock(match) {
  const minute = match.minute != null && String(match.minute) !== '' ? String(match.minute) : '';
  if (!minute) return String(match.status || '').toLowerCase() === 'playing' ? '直播中' : matchStartLabel(match);
  const extra = match.minute_extra != null && String(match.minute_extra) !== '' && String(match.minute_extra) !== '0' ? `+${match.minute_extra}` : '';
  return `${minute}${extra}'${match.minute_period ? ` ${match.minute_period}` : ''}`;
}
function matchScore(match) {
  const hasHome = match.fs_A != null && String(match.fs_A) !== '';
  const hasAway = match.fs_B != null && String(match.fs_B) !== '';
  return hasHome || hasAway ? `${hasHome ? match.fs_A : 0} - ${hasAway ? match.fs_B : 0}` : 'VS';
}
function discoveryLogo(name, source) {
  const fallback = String(name || '?').trim().slice(0, 1) || '?';
  return `<span class="discovery-logo">${source ? `<img src="${escapeHtml(source)}" alt="">` : escapeHtml(fallback)}</span>`;
}
function normalizeActiveMatches(payload) {
  let items = Array.isArray(payload.active_matches) ? payload.active_matches : [];
  if (!items.length && Array.isArray(payload.active_match_ids)) {
    items = payload.active_match_ids.map(id => ({match_id:id}));
  }
  if (!items.length && payload.active_match_id) items = [{match_id:payload.active_match_id}];
  const seen = new Set();
  return items.filter(item => {
    const id = String((item && item.match_id) || '').trim();
    if (!id || seen.has(id)) return false;
    seen.add(id);
    return true;
  }).map(item => ({...item, match_id:String(item.match_id)}));
}
function normalizeCapacity(payload, activeMatches) {
  const raw = payload.capacity && typeof payload.capacity === 'object' ? payload.capacity : {};
  const active = finiteNumber(raw.active) ?? finiteNumber(raw.used) ?? finiteNumber(payload.active_match_count) ?? activeMatches.length;
  const legacySingleMatch = payload.selection_locked != null && payload.max_concurrent_matches == null && raw.max == null && raw.total == null;
  const max = finiteNumber(raw.max) ?? finiteNumber(raw.total) ?? finiteNumber(payload.max_concurrent_matches) ?? (legacySingleMatch ? 1 : 8);
  const normalizedActive = Math.max(0, Math.round(active));
  const normalizedMax = Math.max(1, Math.round(max));
  const explicitFull = raw.at_capacity ?? raw.atCapacity ?? payload.at_capacity ?? payload.selection_locked;
  return {active:normalizedActive, max:normalizedMax, atCapacity:explicitFull == null ? normalizedActive >= normalizedMax : Boolean(explicitFull)};
}
function firstMetric(source, names) {
  for (const name of names) {
    const value = finiteNumber(source[name]);
    if (value != null) return Math.max(0, Math.round(value));
  }
  return null;
}
function renderHeavyTasks(payload) {
  const raw = payload.heavy_tasks && typeof payload.heavy_tasks === 'object' ? payload.heavy_tasks : null;
  state.heavyTasks = raw;
  const element = $('heavy-task-summary');
  if (!raw) {
    element.className = 'heavy-task-summary unavailable';
    element.innerHTML = '<span>处理能力</span><b>可处理 --</b><b>处理中 --</b><b>排队 --</b><b>画面 --</b>';
    return;
  }
  const total = firstMetric(raw, ['total_slots', 'total', 'max', 'capacity', 'slots']);
  const occupied = firstMetric(raw, ['occupied', 'used', 'active', 'running']);
  const queued = firstMetric(raw, ['queued', 'queue', 'pending', 'waiting']);
  const vision = firstMetric(raw, ['vision', 'vision_active', 'vision_running', 'vision_tasks']);
  const metric = value => value == null ? '--' : value;
  element.className = `heavy-task-summary${queued ? ' queued' : ''}`;
  element.innerHTML = `<span>处理能力</span><b>可处理 ${metric(total)}</b><b>处理中 ${metric(occupied)}</b><b>排队 ${metric(queued)}</b><b>画面 ${metric(vision)}</b>`;
}
function heavyTaskWaitFor(targetMatchId, eventKey, artifactKind) {
  const raw = state.heavyTasks;
  const waiting = raw && Array.isArray(raw.waiting_items) ? raw.waiting_items : [];
  const expectedKinds = artifactKind === 'ocr_window'
    ? new Set(['vision', 'vision_ocr'])
    : artifactKind === 'tdeed_refined'
      ? new Set(['vision', 'vision_tdeed'])
      : new Set(['gif']);
  const index = waiting.findIndex(item => String(item && item.match_id || '') === String(targetMatchId || '')
    && String(item && item.event_key || '') === String(eventKey || '')
    && expectedKinds.has(String(item && item.task_kind || '')));
  if (index < 0) return null;
  const item = waiting[index] || {};
  return {
    position: index + 1,
    waitSeconds: finiteNumber(item.wait_seconds),
    queued: firstMetric(raw, ['queued', 'queue', 'pending', 'waiting']),
    occupied: firstMetric(raw, ['occupied', 'used', 'active', 'running']),
    total: firstMetric(raw, ['total_slots', 'total', 'max', 'capacity', 'slots']),
  };
}
function activeStateLabel(match) {
  const stateValue = String(match.lifecycle_state || '').toLowerCase();
  if (stateValue === 'finishing') return '收尾中';
  if (stateValue === 'stopping') return '停止中';
  if (match.worker_running) return match.worker_mode === 'demo' ? '演示运行中' : '实时运行中';
  if (match.restart_due_at_unix) return '等待自动恢复';
  if (match.cleanup_process_group) return '清理旧进程';
  return match.desired_running ? '准备恢复' : stateValue ? friendlyText(stateValue, '活动中') : '活动中';
}
function renderActiveMatches(playing, upcoming) {
  const directory = new Map([...playing, ...upcoming].map(item => [String(item.match_id || ''), item]));
  const tabs = $('active-match-tabs');
  $('active-capacity').textContent = `活动场次 ${state.capacity.active}/${state.capacity.max}`;
  $('active-overview').classList.toggle('at-capacity', state.capacity.atCapacity);
  if (!state.activeMatches.length) {
    tabs.innerHTML = '<span class="active-tabs-empty">暂无活动比赛</span>';
    return;
  }
  tabs.innerHTML = state.activeMatches.map(summary => {
    const id = String(summary.match_id || '');
    const match = {...(directory.get(id) || {}), ...summary};
    const teams = [match.team_A_name, match.team_B_name].filter(Boolean).join(' vs ');
    const title = teams || (summary.worker_mode === 'demo' ? '演示比赛' : `比赛 ${id}`);
    const score = directory.has(id) && String(match.status || '').toLowerCase() === 'playing' ? ` · ${matchScore(match)}` : '';
    const selected = id === state.sessionMatchId;
    const stateClass = summary.worker_running ? 'running' : summary.restart_due_at_unix ? 'recovering' : String(summary.lifecycle_state || 'active').toLowerCase();
    return `<button class="active-match-tab ${selected ? 'selected' : ''} ${escapeHtml(stateClass)}" type="button" role="tab" aria-selected="${selected}" data-active-match-id="${escapeHtml(id)}"><i></i><span><b>${escapeHtml(title)}</b><small>ID ${escapeHtml(id)} · ${escapeHtml(activeStateLabel(summary))}${escapeHtml(score)}</small></span></button>`;
  }).join('');
}
function discoveryMatch(match, group) {
  const id = String(match.match_id || '').trim();
  const selected = Boolean(id) && id === matchId();
  const competition = [match.competition_name, match.round_name].filter(Boolean).join(' · ') || '赛事信息待定';
  const status = String(match.status || (group === 'playing' ? 'Playing' : 'Fixture'));
  const disabled = !id;
  const meta = [matchStartLabel(match), id ? `ID ${id}` : '比赛 ID 待定'].join(' · ');
  return `<button class="discovery-match ${selected ? 'selected' : ''}" type="button" data-match-id="${escapeHtml(id)}" aria-pressed="${selected}" ${disabled ? 'disabled' : ''}><span class="discovery-match-state ${escapeHtml(statusClass(status))}"><b>${escapeHtml(group === 'playing' ? matchScore(match) : matchStartLabel(match))}</b><small>${escapeHtml(group === 'playing' ? matchClock(match) : '即将开赛')}</small></span><span class="discovery-teams"><span>${discoveryLogo(match.team_A_name, match.team_A_logo)}<b>${escapeHtml(match.team_A_name || '主队待定')}</b></span><span>${discoveryLogo(match.team_B_name, match.team_B_logo)}<b>${escapeHtml(match.team_B_name || '客队待定')}</b></span></span><span class="discovery-match-meta"><b>${escapeHtml(competition)}</b><small>${escapeHtml(meta)}</small></span></button>`;
}
function healthPresentation(health, hasData) {
  const value = String(health.state || '').toLowerCase();
  if (health.error && !hasData) return {label:friendlyText(health.label, '接口暂时异常'), cls:'error'};
  if (health.from_cache || value === 'degraded' || value === 'stale') return {label:friendlyText(health.label, '使用最近一次结果'), cls:'warning'};
  if (value === 'error' || value === 'failed' || value === 'unhealthy') return {label:friendlyText(health.label, '接口暂时异常'), cls:'error'};
  return {label:friendlyText(health.label, value === 'healthy' ? '接口正常' : hasData ? '接口正常' : '已连接'), cls:'healthy'};
}
function renderMatches(data) {
  const payload = data && typeof data === 'object' ? data : {};
  const playing = Array.isArray(payload.playing) ? payload.playing : [];
  const upcoming = Array.isArray(payload.upcoming) ? payload.upcoming : [];
  const health = payload.health && typeof payload.health === 'object' ? payload.health : {};
  state.matches = payload;
  state.activeMatches = normalizeActiveMatches(payload);
  state.capacity = normalizeCapacity(payload, state.activeMatches);
  renderHeavyTasks(payload);
  renderActiveMatches(playing, upcoming);
  const presentation = healthPresentation(health, playing.length + upcoming.length > 0);
  const healthEl = $('discovery-health');
  healthEl.className = `discovery-health ${presentation.cls}`;
  healthEl.innerHTML = `<i></i>${escapeHtml(presentation.label)}`;
  $('playing-count').textContent = `${playing.length} 场`;
  $('upcoming-count').textContent = `${upcoming.length} 场`;
  $('playing-matches').innerHTML = playing.length ? playing.map(match => discoveryMatch(match || {}, 'playing')).join('') : '<div class="discovery-empty">暂无进行中的比赛</div>';
  $('upcoming-matches').innerHTML = upcoming.length ? upcoming.map(match => discoveryMatch(match || {}, 'upcoming')).join('') : '<div class="discovery-empty">未来 15 分钟暂无比赛</div>';
  const total = finiteNumber(health.source_count) ?? finiteNumber(health.total_count) ?? playing.length + upcoming.length;
  $('discovery-summary').textContent = `进行中 ${playing.length} · 即将开赛 ${upcoming.length}`;
  const details = [];
  if (health.last_success_at_unix) details.push(`最近成功 ${fmtTime(health.last_success_at_unix)}`);
  if (finiteNumber(health.latency_ms) != null) details.push(`${Math.round(Number(health.latency_ms))}ms`);
  details.push(`目录 ${total} 场`);
  if (health.from_cache && finiteNumber(health.cache_age_seconds) != null) details.push(`缓存 ${Math.round(Number(health.cache_age_seconds))} 秒`);
  if (finiteNumber(health.consecutive_failures) > 0) details.push(`连续失败 ${Math.round(Number(health.consecutive_failures))} 次`);
  if (finiteNumber(health.consecutive_failures) > 0 && health.next_retry_at_unix) details.push(`下次重试 ${fmtTime(health.next_retry_at_unix)}`);
  if (health.error) details.push(detailedErrorMessage(health.error, '赛事目录暂时无法刷新'));
  if (state.heavyTasks && state.heavyTasks.error) details.push(`任务处理能力：${detailedErrorMessage(state.heavyTasks.error, '暂时无法读取任务处理能力')}`);
  $('discovery-detail').textContent = details.join(' · ') || '赛事目录已更新';
  applyControlAvailability();
  renderSelectionHint();
}
function exitReasonLabel(reason) {
  return ({
    match_played:'比赛正常结束', match_played_stream_incomplete:'直播提前中断，最后画面不完整',
    match_played_finish_timeout:'收尾超时，已强制停止', ingest_error:'直播接收异常',
    ingest_completed:'直播输入结束', manual_stop:'手动停止'
  })[reason] || (reason ? friendlyErrorMessage(reason, '处理结束') : '');
}
function eventPresentation(event) {
  const code = String(event.code || '').toUpperCase();
  const definitions = {
    G: { label: '进球', kind: 'goal' },
    OG: { label: '乌龙球', kind: 'own-goal' },
    PG: { label: '点球进球', kind: 'goal' },
    YC: { label: '黄牌', kind: 'yellow-card' },
    RC: { label: '红牌', kind: 'red-card' },
  };
  const definition = definitions[code] || { label: event.label || '比赛事件', kind: 'other' };
  return { code: code || '--', label: event.label || definition.label, kind: definition.kind };
}

function taskPresentation(status) {
  return ({
    history: {label:'历史事件 · 未生成', cls:'history'},
    discovered: {label:'已发现', cls:'pending'},
    pending: {label:'已记录 · 等待后续画面', cls:'pending'},
    encoding: {label:'GIF 生成中', cls:'encoding'},
    encoded: {label:'GIF 已生成', cls:'encoded'},
    failed: {label:'生成失败', cls:'failed'},
  })[status] || {label:status ? '处理中' : '等待处理', cls:'pending'};
}

function visionLocatorMethodLabel(vision) {
  if (!vision) return '';
  const raw = String(vision.locator_method || '').trim();
  const normalized = raw.toLowerCase().replace(/[\s_-]+/g, '');
  if (normalized.includes('minuterangefallback')) return '分钟范围定位';
  if (normalized.includes('ocr') && normalized.includes('tdeed')) return '画面时间 + 动作识别';
  if (normalized.includes('ocr')) return '画面时间识别';
  if (normalized.includes('tdeed')) return '动作识别';
  if (!raw && String(vision.model_name || '').toLowerCase().includes('t-deed')) return '动作识别';
  return raw ? friendlyText(raw, '画面定位') : '画面定位';
}

function visionPresentation(vision, enabled = true, resourceWait = null) {
  if (!vision) return enabled ? {label:'等待画面处理', cls:'off'} : {label:'已关闭', cls:'off'};
  if (vision.disabled === true || vision.last_error_kind === 'tdeed_disabled' || vision.error_kind === 'tdeed_disabled') return {label:'已关闭', cls:'off'};
  if (resourceWait && ['pending', 'locating'].includes(String(vision.status || ''))) return {label:'正在等待处理资源',cls:'pending'};
  if (vision.artifact_kind === 'ocr_window') {
    const pipelineStatus = String(vision.ocr_pipeline_status || '').trim();
    const availabilityGuideKey = targetAvailabilityGuideKey(vision);
    if (availabilityGuideKey === 'ocr_target_before_recording') return {label:'目标画面早于本次录像开始',cls:'failed'};
    if (availabilityGuideKey === 'ocr_target_history_cleaned') return {label:'目标历史画面当前不可用',cls:'failed'};
    const rawRescanAttempt = vision.target_rescan_attempt_count;
    const parsedRescanAttempt = Number(rawRescanAttempt);
    const rescanAttempt = rawRescanAttempt != null && rawRescanAttempt !== ''
      && Number.isInteger(parsedRescanAttempt)
      ? Math.max(0, parsedRescanAttempt)
      : 0;
    const rescanLabel = vision.target_rescan_pending
      ? `已越过目标，正在重新扫描（第${rescanAttempt + 1}次）`
      : '已越过目标，正在重新扫描';
    const pipelinePresentation = ({
      waiting_for_clock_readiness:{label:'尚未检测到比赛计时器 · 等待新增视频后再检查',cls:'pending'},
      waiting_for_clock_target:{label:'正在查找比赛时间',cls:'encoding'},
      waiting_for_target_media:{label:'计时器已确认 · 等待目标画面',cls:'pending'},
      waiting_for_latest_tail_rescan:{label:'正在扫描新增视频尾部',cls:'encoding'},
      ocr_target_rescan:{label:rescanLabel,cls:'encoding'},
      waiting_for_postroll:{label:'已找到画面 · 等待后续画面',cls:'pending'},
      ocr_second_exact:{label:'已精确到秒',cls:vision.status === 'encoded' ? 'encoded' : 'encoding'},
      ocr_second_interpolated:{label:'已根据前后画面推算到秒',cls:vision.status === 'encoded' ? 'encoded' : 'encoding'},
      ocr_second_estimated:{label:'已估算到附近几秒',cls:vision.status === 'encoded' ? 'encoded' : 'encoding'},
      ocr_second_projected:{label:'时钟被遮挡 · 已按前后时间推算',cls:vision.status === 'encoded' ? 'encoded' : 'encoding'},
      ocr_minute_fallback:{label:'已找到该分钟附近画面',cls:vision.status === 'encoded' ? 'encoded' : 'encoding'},
      ocr_range_fallback:{label:vision.fallback_complete === true ? '未精确定位 · 120 秒范围 GIF 已生成' : '未精确定位 · 残缺范围 GIF 已生成',cls:'warning'},
      ocr_no_clock_detected:{label:'没有读到画面上的比赛时间',cls:'failed'},
      ocr_target_timeout:{label:'查找比赛时间超时',cls:'failed'},
      ocr_clock_target_not_located:{label:'目标已越过但未完成可靠定位',cls:'failed'},
      ocr_target_media_not_arrived:{label:'视频还没播放到这个时间',cls:'failed'},
      ocr_target_media_stalled:{label:'视频暂时没有新画面',cls:'failed'},
      ocr_clock_paused_timeout:{label:'比赛时间暂时没有继续',cls:'failed'},
      ocr_target_before_recording:{label:'目标画面早于本次录像开始',cls:'failed'},
      ocr_target_history_cleaned:{label:'目标历史画面已经被清理',cls:'failed'},
      ocr_window_evicted:{label:'需要的历史画面当前已不在',cls:'failed'},
      ocr_discontinuous_clock:{label:'画面时间前后对不上',cls:'failed'},
      ocr_preparation_timeout:{label:'准备识别视频耗时过长',cls:'failed'},
      ocr_encode_failed:{label:'画面 GIF 生成失败',cls:'failed'},
      ocr_dependency_unavailable:{label:'画面识别服务不可用',cls:'failed'},
      ocr_incomplete:{label:'比赛已结束 · 画面时间任务未完成',cls:'failed'}
    })[pipelineStatus];
    if (pipelinePresentation) return pipelinePresentation;
    const coverageOnlyDegraded = vision.coverage_degraded === true && vision.localization_degraded !== true;
    const degraded = vision.localization_degraded === true
      || vision.localization_quality === 'degraded'
      || (!coverageOnlyDegraded && vision.degraded === true);
    return ({
      pending:{label:'等待查找比赛时间',cls:'pending'}, locating:{label:'正在查找比赛时间',cls:'encoding'},
      located:{label:degraded ? '已找到分钟附近画面' : '已找到对应比赛画面',cls:'encoding'},
      encoding:{label:degraded ? '分钟附近 GIF 生成中' : '60 秒 GIF 生成中',cls:'encoding'},
      encoded:{label:degraded ? '分钟附近 GIF 已生成' : '60 秒 GIF 已生成',cls:'encoded'},
      failed:{label:'画面处理失败',cls:'failed'}
    })[vision.status] || {label:vision.status ? '处理中' : '等待画面处理', cls:'off'};
  }
  const method = visionLocatorMethodLabel(vision);
  const minuteFallback = vision.minute_fallback === true;
  const fragmentedFallback = minuteFallback && vision.fallback_complete === false;
  const fallbackLabel = fragmentedFallback ? '分钟附近的残缺片段' : '分钟附近画面';
  return ({
    pending:{label:'等待查找视频片段',cls:'pending'}, locating:{label:'正在查找比赛画面',cls:'encoding'},
    located:{label:minuteFallback ? (fragmentedFallback ? '已找到分钟附近，但片段不完整' : '已找到分钟附近画面') : `${method ? `${method} · ` : ''}已找到对应画面`,cls:'encoding'}, encoding:{label:minuteFallback ? `${fallbackLabel} GIF 生成中` : '精剪 GIF 生成中',cls:'encoding'},
    encoded:{label:minuteFallback ? (fragmentedFallback ? '分钟附近的残缺 GIF 已生成' : '分钟附近 GIF 已生成') : `${method ? `${method} · ` : ''}精剪 GIF 已生成`,cls:'encoded'}, failed:{label:'精剪失败',cls:'failed'}
  })[vision.status] || {label:vision.status ? '处理中' : '等待画面处理', cls:'off'};
}

function visionFailureDetail(vision) {
  if (!vision) return '';
  const structured = vision.failure_reason && typeof vision.failure_reason === 'object' ? vision.failure_reason : {};
  // A pending/locating artifact may carry the previous attempt's diagnostic
  // payload. It is still recoverable, so do not render that payload as a
  // terminal failure. The pipeline badge and technical diagnostics show the
  // current retry state separately.
  if (vision.status !== 'failed') return '';
  const stage = String(structured.stage || vision.stage || '').trim().toLowerCase();
  const kind = String(structured.kind || vision.error_kind || vision.last_error_kind || '').trim();
  const stageLabel = ({
    materializing:'准备视频片段', materialize:'准备视频片段', ocr:'查找比赛时间',
    locating:'查找对应画面', tdeed:'查找精彩动作', fallback:'使用备用定位',
    tdeed_fallback:'使用备用定位', failed:'查找对应画面', buffer:'检查视频片段',
    encoding:'生成 GIF', encode:'生成 GIF',
    waiting_for_default_gif:'等待默认 GIF', scoreboard_profile:'核对画面中的比赛时间区域',
    event_second_localization:'查找事件秒数', event_localization:'查找事件画面',
    fragmented_search:'检查连续视频片段', buffer_coverage:'检查视频覆盖范围',
    ocr_clock_discovery:'读取画面比赛时间', ocr_target_localization:'查找接口对应时间', ocr_target_media_availability:'核对目标画面是否曾进入本次录像',
    ocr_window_encoding:'生成 60 秒 GIF', waiting_for_clock_readiness:'确认比赛计时器已经出现', waiting_for_clock_target:'等待比赛时间出现', waiting_for_target_media:'等待目标画面进入视频', waiting_for_latest_tail_rescan:'扫描新增视频尾部',
    waiting_for_postroll:'等待目标后的画面', ocr_second_exact:'精确到秒', ocr_second_interpolated:'根据前后画面推算秒数', ocr_second_estimated:'估算附近几秒',
    ocr_second_projected:'时钟被遮挡后按前后时间推算',
    ocr_minute_fallback:'查找分钟附近画面', ocr_range_fallback:'生成接口时间范围兜底', ocr_no_clock_detected:'读取比赛时间',
    ocr_target_timeout:'查找接口对应时间', ocr_clock_target_not_located:'验证目标画面', ocr_window_evicted:'查找历史画面',
    ocr_target_media_not_arrived:'等待目标画面', ocr_target_media_stalled:'等待新画面',
    ocr_clock_paused_timeout:'等待比赛时间继续',
    ocr_discontinuous_clock:'核对画面时间', ocr_encode_failed:'生成画面 GIF',
    ocr_dependency_unavailable:'检查画面识别服务', ocr_incomplete:'比赛结束收尾', ocr_progressive_scan:'读取比赛时间并等待',
    tdeed_model_unavailable:'加载动作识别服务', tdeed_inference:'识别精彩动作',
    tdeed_candidate_selection:'选择精彩动作片段', tdeed_output_encoding:'生成动作 GIF'
  })[stage] || '处理过程';
  let reasonLabel = ({
    waiting_for_video:'视频片段还没有准备好', video_unavailable:'暂时没有可用视频',
    buffer_history_missing:'需要的历史画面当前已经不在', buffer_gap:'视频片段中间有缺口',
    vision_deadline_exceeded:'处理等待时间已到', ocr_no_clock:'没有读到画面上的比赛时间',
    ocr_no_match:'没有找到接口对应的比赛时间', ocr_processing_failed:'读取比赛时间失败',
    scoreboard_missing:'指定画面区域没有读到比赛时间', ocr_clock_unreadable:'画面上的比赛时间不清楚',
    ocr_score_unreadable:'画面上的比分不清楚', ocr_no_score_transition:'没有确认比分变化',
    ocr_ambiguous:'读到的时间前后不一致', ocr_no_target:'没有找到对应的比赛画面', ocr_model_unavailable:'画面识别服务不可用',
    clock_profile_mismatch:'配置的比赛时间区域与实际画面不一致',
    tdeed_model_unavailable:'动作识别服务不可用', tdeed_no_candidate:'没有找到合适的精彩动作',
    tdeed_inference_failed:'识别精彩动作失败', inference_timeout:'画面处理等待时间过长',
    encode_failed:'GIF 生成失败', model_inference_failed:'识别精彩动作失败', vision_processing_failed:'画面处理失败',
    video_gap:'视频片段中间有缺口', anchor_gap:'对应画面附近有视频缺口',
    anchor_gap_too_large:'对应画面的缺口过大', anchor_shift_too_large:'最近可用画面距离过远',
    anchor_unavailable:'对应画面没有可用视频',
    degraded_clip_too_short:'找到的片段太短', default_gif_failed:'默认 GIF 生成失败',
    fragmented_minute_fallback:'只生成了分钟附近的残缺片段',
    ocr_target_localization_failed:'没有找到接口对应的比赛时间', ocr_window_encoding_failed:'60 秒 GIF 生成失败',
    tdeed_candidate_selection_failed:'没有找到合适的动作片段', tdeed_output_encoding_failed:'动作 GIF 生成失败',
    upstream_ocr_window_failed:'比赛时间识别失败，因此没有继续动作精剪', tdeed_disabled:'已按设置关闭',
    ocr_no_clock_detected:'整段视频都没有读到清晰的比赛时间', ocr_target_timeout:'在等待结束前没有找到接口对应时间',
    ocr_window_evicted:'需要的历史画面当前已经不在', ocr_discontinuous_clock:'画面时间前后对不上',
    ocr_preparation_timeout:'准备识别视频耗时过长', ocr_encode_failed:'画面 GIF 生成失败', ocr_dependency_unavailable:'画面识别服务不可用',
    ocr_video_preparation_timeout:'准备识别视频耗时过长', ocr_window_encoding_timeout:'生成画面 GIF 耗时过长',
    ocr_processing_budget_exhausted:'旧版本因累计处理时限提前停止',
    ocr_clock_target_timeout:'视频还没播放到接口给出的时间就超时了',
    ocr_target_media_not_arrived:'视频还没播放到接口给出的时间', ocr_target_media_stalled:'视频暂时没有新画面',
    ocr_clock_paused_timeout:'比赛时间暂时没有继续，可能在暂停或回放',
    ocr_target_before_recording:'目标画面早于本次录像开始',
    target_before_recording:'目标画面早于本次录像开始',
    target_not_recorded:'目标画面早于本次录像开始',
    ocr_target_history_cleaned:'目标历史画面已经被清理',
    ocr_clock_target_not_located:'视频已经超过这个时间，但没有找到清晰的比赛时间',
    ocr_no_trustworthy_clock_before_deadline:'一直没有读到清晰的比赛时间',
    ocr_postroll_timeout:'目标时间后面的画面还没有准备好', ocr_output_window_timeout:'GIF 所需画面还没有准备好',
    ocr_search_history_evicted:'目标附近历史画面当前已经不在，现有记录无法确认是未录到还是后来被清理', ocr_output_history_evicted:'生成 GIF 所需的历史画面当前已经不在',
    ocr_buffer_never_available:'一直没有可用的视频画面', ocr_output_video_gap:'生成 GIF 的视频片段不完整',
    ocr_window_encoding_failed:'画面 GIF 生成阶段失败', vision_shutdown_timeout:'比赛已结束，画面时间任务在收尾时间内未完成',
  })[kind] || friendlyErrorMessage(kind, '暂时无法判断具体原因');
  if (kind === 'ocr_clock_target_not_located') {
    reasonLabel = ({
      target_passed: '已经越过目标时间，但没有找到可用的对应画面',
      isolated: '只在一张画面读到附近时间，其他画面无法核对',
      continuity: '目标附近的比赛时间不连续',
      window_evicted: '目标历史画面已经被删除',
      media_stalled: '视频停止增长，暂时没有新画面',
      unreadable: '目标附近的比赛时间无法可靠读取'
    })[String(vision.target_failure_cause || '').trim()] || reasonLabel;
  }
  const rawMessage = String(structured.message || '');
  const message = friendlyText(rawMessage);
  const attempts = Array.isArray(vision.fragment_attempts) ? vision.fragment_attempts : [];
  const fragmentDetail = attempts.length ? `，已检查 ${attempts.length} 段视频` : '';
  const messageDetail = message && message !== reasonLabel && /[\u4e00-\u9fff]{4}/.test(rawMessage) ? `（${message}）` : '';
  const nextAction = friendlyText(vision.failure_next_action || structured.next_action || '');
  const actionDetail = nextAction && /[\u4e00-\u9fff]/.test(nextAction) ? `；建议：${nextAction}` : '';
  return `处理到：${stageLabel}；情况：${reasonLabel}${messageDetail}${fragmentDetail}${actionDetail}`;
}

function scoreboardRegionStatusText(vision) {
  if (!vision || typeof vision !== 'object') return '';
  const status = String(vision.scoreboard_region_status || '').trim();
  if (status === 'rediscovered') {
    return '之前记住的比赛时间位置不再适合当前画面，系统已重新找到位置并继续处理';
  }
  if (status === 'rediscovery_failed') {
    return '之前记住的比赛时间位置已失效，系统重新查找后仍未找到稳定位置；本次如有兜底 GIF 会继续保留';
  }
  if (status === 'explicit_profile_mismatch') {
    return '手动配置的比赛时间位置与当前画面不一致，系统为保护手动配置没有自动覆盖；需要重新配置或取消手动区域';
  }
  return '';
}

function streamTimeText(value) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds < 0) return '';
  const minute = Math.floor(seconds / 60);
  return `${minute}:${String(Math.floor(seconds % 60)).padStart(2, '0')}`;
}

function ocrClockValue(value, secondsValue) {
  if (value != null && String(value).trim()) {
    if (typeof value === 'object') {
      const text = value.text || value.clock || value.clock_text;
      if (text != null && String(text).trim()) return String(text).trim();
      const nestedSeconds = value.seconds ?? value.clock_seconds;
      if (nestedSeconds != null) return matchClockText(nestedSeconds);
      return matchClockText(secondsValue);
    }
    return String(value).trim();
  }
  return matchClockText(secondsValue);
}

function streamWindowText(window) {
  if (!window || typeof window !== 'object') return '';
  const start = window.start_stream_time ?? window.window_start_stream_time ?? window.requested_window_start_stream_time ?? window.start;
  const end = window.end_stream_time ?? window.window_end_stream_time ?? window.requested_window_end_stream_time ?? window.end;
  const startText = streamTimeText(start); const endText = streamTimeText(end);
  return startText && endText ? `${startText}-${endText}` : startText || endText;
}

function ocrPipelineDiagnosticsText(vision) {
  if (!vision) return '';
  const parts = [];
  if (vision.clock_readiness_status === 'waiting') {
    const accepted = finiteNumber(vision.clock_readiness_accepted_sample_count) ?? 0;
    parts.push(`比赛计时器确认进度 ${Math.floor(accepted)}/2 个有效时间读数`);
    const probe = streamTimeText(vision.clock_readiness_last_probe_media_end_stream_time);
    if (probe) parts.push(`上次检查到视频 ${probe}`);
    const required = finiteNumber(vision.clock_readiness_required_media_growth_seconds);
    if (required != null) parts.push(`新增约 ${Math.ceil(required)} 秒画面后再检查`);
  } else if (vision.clock_readiness_status === 'ready') {
    parts.push('比赛计时器已确认可用');
  }
  const scanWindow = streamWindowText(vision.scan_window);
  if (scanWindow) parts.push(`检查的视频位置 ${scanWindow}`);
  const cursor = streamTimeText(vision.scan_cursor);
  if (cursor) parts.push(`已经检查到视频 ${cursor}`);
  const trustedClock = ocrClockValue(vision.last_trusted_clock, vision.last_trusted_clock_seconds);
  if (trustedClock) parts.push(`最近读到的比赛时间 ${trustedClock}`);
  if (vision.target_clock_gap_seconds != null) {
    const gap = Number(vision.target_clock_gap_seconds);
    if (Number.isFinite(gap) && gap > 0) parts.push(`距离目标还差 ${Math.floor(gap)} 秒`);
  }
  if (vision.latest_media_end_stream_time != null && vision.previous_media_end_stream_time != null) {
    const tailDelta = Number(vision.latest_media_end_stream_time) - Number(vision.previous_media_end_stream_time);
    if (Number.isFinite(tailDelta)) parts.push(tailDelta > 0.25 ? `新收到约 ${Math.floor(tailDelta)} 秒视频` : '没有收到新视频');
  }
  if (vision.scan_attempt_count != null) parts.push(`已检查 ${vision.scan_attempt_count} 轮`);
  if (Number(vision.target_rescan_attempt_count) > 0) parts.push(`目标附近重新检查 ${vision.target_rescan_attempt_count} 次`);
  if (vision.history_missing_seconds != null && Number(vision.history_missing_seconds) > 0) {
    const missing = Number(vision.history_missing_seconds);
    parts.push(`前面缺少${missing < 1 ? '不到 1 秒' : `约 ${Math.round(missing)} 秒`}旧视频`);
  }
  if (vision.target_history_fully_missing) parts.push('目标附近旧视频当前已经全部不在保留范围');
  if (Array.isArray(vision.video_gaps) && vision.video_gaps.length) parts.push(`目标范围内发现 ${vision.video_gaps.length} 处视频缺口`);
  const finalWindow = streamWindowText(vision.final_clip_window);
  if (finalWindow) parts.push(`GIF 使用的视频位置 ${finalWindow}`);
  const structuredStage = vision.failure_reason && typeof vision.failure_reason === 'object' ? vision.failure_reason.stage : '';
  const stage = String(structuredStage || vision.stage || '').trim();
  if (vision.status === 'failed' && stage) {
    const stageText = ({
      ocr_clock_discovery:'读取画面比赛时间', ocr_target_localization:'查找目标画面', ocr_target_media_availability:'核对目标画面是否在本次录像内',
      ocr_window_encoding:'生成 GIF', buffer_coverage:'检查视频范围',
      waiting_for_clock_readiness:'确认比赛计时器', waiting_for_target_media:'等待目标画面',
      waiting_for_clock_target:'等待接口时间出现在画面中', waiting_for_postroll:'等待目标后面的画面',
      ocr_no_clock_detected:'读取画面比赛时间', ocr_target_timeout:'查找目标画面', ocr_clock_target_not_located:'验证目标画面',
      ocr_target_before_recording:'核对录像开始时间', ocr_target_history_cleaned:'核对历史画面',
      ocr_window_evicted:'检查历史画面', ocr_discontinuous_clock:'核对比赛时间是否连贯',
      ocr_encode_failed:'生成 GIF', ocr_dependency_unavailable:'检查画面读取服务', ocr_incomplete:'比赛结束收尾', waiting_for_latest_tail_rescan:'扫描新增视频尾部'
    })[stage] || stage;
    parts.push(`失败阶段 ${stageText}`);
  }
  if (vision.next_attempt_at_unix && ['pending', 'locating', 'located'].includes(vision.status)) parts.push(`下次尝试 ${fmtTime(vision.next_attempt_at_unix)}`);
  if (vision.deadline_at_unix && ['pending', 'locating', 'located'].includes(vision.status)) parts.push(`截止 ${fmtTime(vision.deadline_at_unix)}`);
  if (vision.failure_explanation) parts.push(`说明：${vision.failure_explanation}`);
  if (vision.failure_next_action) parts.push(`下一步：${vision.failure_next_action}`);
  return parts.join(' · ');
}

function visionOcrDiagnosticsText(vision) {
  const diagnostics = vision && vision.ocr_diagnostics;
  if (!diagnostics) return '';
  if (typeof diagnostics === 'string') return diagnostics.trim();
  if (typeof diagnostics !== 'object') return String(diagnostics);
  if (diagnostics.summary) return friendlyText(String(diagnostics.summary));
  const fields = [
    ['sampled_frames', '共检查画面'], ['frames_sampled', '共检查画面'],
    ['clock_readable_frames', '能读清比赛时间的画面'], ['clock_frames', '能读清比赛时间的画面'], ['valid_clock_frames', '能读清比赛时间的画面'],
    ['clock_repaired_frames', '修正后可用的时间画面'],
    ['scoreboard_missing_frames', '指定区域没有时间的画面'],
    ['candidate_count', '可能对应目标的画面'], ['candidates', '可能对应目标的画面']
  ];
  const seen = new Set();
  const parts = [];
  for (const [key, label] of fields) {
    if (diagnostics[key] == null || seen.has(label)) continue;
    seen.add(label); parts.push(`${label} ${diagnostics[key]} 张`);
  }
  if (diagnostics.target_clock) parts.unshift(`接口给出的比赛时间 ${diagnostics.target_clock}`);
  if (diagnostics.inference_seconds != null) parts.push(`读取画面用时 ${Number(diagnostics.inference_seconds).toFixed(1)} 秒`);
  if (diagnostics.worker_wall_seconds != null && diagnostics.inference_seconds == null) parts.push(`本次处理用时 ${Number(diagnostics.worker_wall_seconds).toFixed(1)} 秒`);
  if (diagnostics.exact_second_failure_reason) {
    const reason = ({
      target_clock_not_found:'未找到目标秒',
      no_trustworthy_clock_readings:'没有一张画面能稳定读清比赛时间',
      multiple_disjoint_occurrences:'目标秒多处出现'
    })[diagnostics.exact_second_failure_reason] || diagnostics.exact_second_failure_reason;
    parts.push(`没有精确到目标秒：${reason}`);
  }
  return parts.join(' · ');
}

function visionFailureKind(vision) {
  if (!vision || typeof vision !== 'object') return '';
  const structured = vision.failure_reason && typeof vision.failure_reason === 'object' ? vision.failure_reason : {};
  return String(structured.kind || vision.error_kind || vision.last_error_kind || '').trim();
}

function visionFailureEvidenceText(vision) {
  if (!vision || typeof vision !== 'object') return '';
  const parts = [];
  const availability = vision.target_media_availability && typeof vision.target_media_availability === 'object'
    ? vision.target_media_availability
    : null;
  if (availability && availability.status === 'before_recording') {
    const estimate = finiteNumber(availability.estimated_stream_time);
    if (estimate != null) parts.push(`推算目标画面位于本次录像开始前约 ${Math.ceil(Math.abs(estimate))} 秒`);
  } else if (availability && availability.status === 'history_unavailable') {
    const targetEnd = finiteNumber(availability.target_window_end_stream_time);
    const earliest = finiteNumber(availability.earliest_retained_stream_time);
    if (targetEnd != null && earliest != null) parts.push(`目标范围最晚到视频 ${streamTimeText(targetEnd)}，当前最早只保留到 ${streamTimeText(earliest)}`);
  }
  const target = ocrClockValue(vision.target_clock, vision.target_clock_seconds);
  const latest = ocrClockValue(vision.last_trusted_clock, vision.last_trusted_clock_seconds);
  if (target && target !== '--') parts.push(`接口给出的比赛时间是 ${target}`);
  if (latest && latest !== '--') parts.push(`最近读到的比赛时间是 ${latest}`);
  const gap = finiteNumber(vision.target_clock_gap_seconds);
  if (gap != null && gap > 0) parts.push(`当时距离目标还差约 ${Math.round(gap)} 秒`);
  const scanWindow = streamWindowText(vision.scan_window);
  if (scanWindow) parts.push(`检查了视频位置 ${scanWindow}`);
  if (vision.scan_attempt_count != null) parts.push(`累计检查 ${vision.scan_attempt_count} 轮`);
  if (Number(vision.target_rescan_attempt_count) > 0) parts.push(`目标附近重新检查 ${vision.target_rescan_attempt_count} 次`);
  const diagnostics = visionOcrDiagnosticsText(vision);
  if (diagnostics) parts.push(diagnostics);
  const missing = finiteNumber(vision.history_missing_seconds);
  if (missing != null && missing > 0) parts.push(`所需范围前面缺少${missing < 1 ? '不到 1 秒' : `约 ${Math.round(missing)} 秒`}旧视频`);
  if (vision.target_history_fully_missing) parts.push('目标附近的视频当前已经全部不在保留范围，现有数据无法判断是未录到还是后来被清理');
  if (Array.isArray(vision.video_gaps) && vision.video_gaps.length) parts.push(`检查范围内有 ${vision.video_gaps.length} 处视频缺口`);
  if (vision.inference_seconds != null) parts.push(`本次画面处理共用时 ${Number(vision.inference_seconds).toFixed(1)} 秒`);
  const attempts = Array.isArray(vision.fragment_attempts) ? vision.fragment_attempts.length : 0;
  if (attempts) parts.push(`尝试读取 ${attempts} 段视频`);
  return [...new Set(parts.filter(Boolean))].join('；');
}

function failureReportMarkup(value, options = {}) {
  const guide = errorGuide(value, options.title || '这一步没有完成');
  const rows = [
    ['发生原因', options.cause || guide.cause],
    ['造成的结果', options.impact || guide.impact],
    ['系统已经做了什么', options.system || guide.system],
    ['可核对的信息', options.evidence || '当前没有更多可核对数据。'],
    ['建议怎么处理', options.action || guide.action],
  ].filter(([, content]) => content);
  return `<div class="failure-report" role="note"><b>${escapeHtml(options.title || guide.title)}</b>${rows.map(([label, content]) => `<span><strong>${escapeHtml(label)}</strong>${escapeHtml(friendlyText(content))}</span>`).join('')}</div>`;
}

function visionFailureReportMarkup(vision) {
  if (!vision || vision.status !== 'failed') return '';
  const kind = visionFailureKind(vision);
  const availabilityGuideKey = targetAvailabilityGuideKey(vision);
  const guide = errorGuide(availabilityGuideKey || kind || vision.error || vision.failure_reason, '画面处理没有完成');
  const structured = vision.failure_reason && typeof vision.failure_reason === 'object' ? vision.failure_reason : {};
  const rawExplanation = String(vision.failure_explanation || vision.target_failure_explanation || structured.message || '');
  const explanation = friendlyText(rawExplanation);
  const cause = explanation && /[\u4e00-\u9fff]{4}/.test(rawExplanation) ? `${guide.cause} 补充说明：${explanation}` : guide.cause;
  const nextAction = friendlyText(vision.failure_next_action || structured.next_action || '');
  const action = nextAction && /[\u4e00-\u9fff]/.test(nextAction) ? `${guide.action} 本次任务建议：${nextAction}` : guide.action;
  return failureReportMarkup(availabilityGuideKey || kind || vision.error || vision.failure_reason, {title:guide.title, cause, impact:guide.impact, system:guide.system, evidence:visionFailureEvidenceText(vision), action});
}

function taskFailureReportMarkup(task) {
  if (!task || task.status !== 'failed') return '';
  const value = task.last_error_kind || task.error || task.failure_reason || 'default_gif_failed';
  const evidence = [
    task.attempt_count != null ? `已经尝试 ${task.attempt_count} 次` : '',
    task.readiness_check_count != null ? `检查视频是否准备好 ${task.readiness_check_count} 次` : '',
    task.deadline_at_unix ? `等待截止时间 ${fmtTime(task.deadline_at_unix)}` : '',
  ].filter(Boolean).join('；');
  return failureReportMarkup(value, {evidence:evidence || '这条任务没有留下更多次数或时间信息。'});
}

function coverageStatusText(value) {
  if (!value || typeof value !== 'object') return '';
  const quality = String(value.coverage_quality || '').trim().toLowerCase();
  const approximate = value.approximate === true || value.anchor_adjusted === true || quality.includes('approximate');
  const stitched = value.stitched_across_gap === true || quality.includes('stitched');
  const degraded = value.coverage_status === 'ready_degraded' || value.degraded === true;
  const complete = value.coverage_status === 'ready_full' || quality === 'complete';
  let label = '';
  if (approximate) label = stitched ? '近似 · 跨缺口拼接' : '近似';
  else if (stitched) label = '跨缺口拼接';
  else if (degraded) label = '降级';
  else if (complete) label = '完整';
  if (!label) return '';

  const parts = [`覆盖 ${label}`];
  const skipped = finiteNumber(value.skipped_gap_seconds);
  if (skipped != null && skipped > 0) parts.push(`跳过 ${Number(skipped.toFixed(1))} 秒`);
  const actualDuration = finiteNumber(value.duration_sec);
  if (stitched && actualDuration != null && actualDuration > 0) {
    parts.push(`实际 ${Number(actualDuration.toFixed(1))} 秒`);
  }
  const shift = finiteNumber(value.anchor_shift_seconds);
  if (value.anchor_adjusted === true && shift != null) {
    const signed = shift > 0 ? `+${Number(shift.toFixed(1))}` : `${Number(shift.toFixed(1))}`;
    parts.push(`对应画面移动 ${signed} 秒`);
  }
  const encodedAnchorOffset = finiteNumber(value.estimated_encoded_anchor_offset_seconds);
  if (stitched && encodedAnchorOffset != null && encodedAnchorOffset >= 0) {
    parts.push(`${value.event_frame_may_be_missing === true ? '拼接点' : '事件'}约在第 ${Number(encodedAnchorOffset.toFixed(1))} 秒`);
  }
  if (value.event_frame_may_be_missing === true) parts.push('事件画面可能缺失');
  return parts.join(' · ');
}

function ocrUserDetailText(vision) {
  if (!vision || typeof vision !== 'object') return '';
  const parts = [];
  const target = ocrClockValue(vision.target_clock, vision.target_clock_seconds);
  if (target && target !== '--') parts.push(`接口时间 ${target}`);
  if (vision.degradation_mode === 'mapped_clock_projection') {
    const error = finiteNumber(vision.estimated_error_bound_seconds);
    const mappingKind = String(vision.clock_video_mapping && vision.clock_video_mapping.mapping_kind || '');
    const basis = mappingKind === 'forward_extrapolation' ? '遮挡前' : mappingKind === 'backward_extrapolation' ? '遮挡后' : '遮挡前后';
    parts.push(`目标时间画面被遮挡，已根据${basis}连续可读的比赛时间推算${error != null ? `，预计误差不超过 ${Number(error.toFixed(1))} 秒` : ''}`);
  }
  if (vision.output_kind === 'api_time_range_fallback') {
    const available = finiteNumber(vision.available_fallback_seconds);
    parts.push(vision.fallback_explanation || (vision.fallback_complete === true
      ? 'OCR 没有完成二次定位，已生成接口时间附近约 120 秒的低清范围片段'
      : `OCR 没有完成二次定位，已生成${available != null ? `约 ${Number(available.toFixed(1))} 秒` : ''}残缺片段，可能不包含事件`));
  }
  if (vision.failure_explanation) parts.push(friendlyText(vision.failure_explanation));
  if (!parts.length && vision.status === 'failed') return '系统会保留默认 GIF，并继续记录失败原因。';
  return parts.join(' · ');
}

function visionResourceWaitText(wait) {
  if (!wait) return '';
  const parts = ['正在等待处理资源'];
  if (wait.position != null) parts.push(`当前排在第 ${wait.position} 位`);
  if (wait.waitSeconds != null) parts.push(`已等待 ${fmtDuration(wait.waitSeconds)}`);
  if (wait.occupied != null && wait.total != null) parts.push(`处理资源正在使用 ${wait.occupied}/${wait.total}`);
  parts.push('资源空闲后会自动开始，不需要重启服务');
  return parts.join(' · ');
}

function ocrPendingStatusText(vision, resourceWait) {
  const resourceText = visionResourceWaitText(resourceWait);
  if (resourceText) return resourceText;
  if (!vision || !['pending', 'locating'].includes(String(vision.status || ''))) return '';
  const status = String(vision.ocr_pipeline_status || vision.progressive_status || '');
  if (status === 'waiting_for_clock_readiness') {
    const accepted = finiteNumber(vision.clock_readiness_accepted_sample_count) ?? 0;
    const lastProbe = finiteNumber(vision.clock_readiness_last_probe_media_end_stream_time);
    const latest = finiteNumber(vision.latest_media_end_stream_time);
    const required = finiteNumber(vision.clock_readiness_required_media_growth_seconds) ?? 15;
    const growth = lastProbe != null && latest != null ? Math.max(0, latest - lastProbe) : null;
    const remaining = growth == null ? required : Math.max(0, required - growth);
    return `尚未检测到比赛计时器 · 已获得 ${Math.floor(accepted)} 个有效时间读数，需要至少 2 个连续向前的读数 · 等待新增视频后再检查${remaining > 0 ? `，还需约 ${Math.ceil(remaining)} 秒新画面` : ''}`;
  }
  if (status === 'waiting_for_target_media') return '比赛计时器已经确认，但目标时间对应的画面还没有进入视频保存范围；系统收到新画面后会自动继续。';
  return '';
}

function targetAvailabilityGuideKey(vision) {
  if (!vision || typeof vision !== 'object') return '';
  const availability = vision.target_media_availability;
  const status = String(availability && typeof availability === 'object' ? availability.status || '' : availability || '').trim().toLowerCase();
  if (['before_recording', 'target_before_recording', 'not_recorded', 'recording_started_after_target'].includes(status)) return 'ocr_target_before_recording';
  if (['history_unavailable', 'history_cleaned', 'target_history_cleaned', 'confirmed_evicted'].includes(status)) return 'ocr_target_history_cleaned';
  return '';
}

function matchClockText(value) {
  if (value == null || (typeof value === 'string' && !value.trim())) return '--';
  const seconds = Number(value);
  if (!Number.isInteger(seconds) || seconds < 0) return '--';
  const minute = Math.floor(seconds / 60);
  return `${String(minute).padStart(2, '0')}:${String(seconds % 60).padStart(2, '0')}`;
}

// The worker persists this value in event.metadata so the dashboard can
// explain which source supplied the goal without inferring from timing fields.
function goalRouteStatusLabel(value) {
  return ({
    shotmap_direct: '事件接口直接提供',
    overview_fallback_empty: '备用赛况接口提供',
    overview_fallback_no_goal: '备用赛况接口提供',
    overview_fallback_no_match: '暂未匹配到事件',
    cross_source_merged: '两路接口信息已合并',
    shotmap_late_match: '事件接口稍后补充'
  })[String(value || '').trim()] || '';
}

function setStep(id, status) { $(id).className = `pipeline-step ${status || ''}`; }
function syncInputValue(id, value) {
  if (value != null && document.activeElement !== $(id)) $(id).value = value;
}
function ingestErrorText(value) {
  if (value == null) return '';
  if (typeof value === 'string') return value.trim();
  if (typeof value === 'object') {
    for (const key of ['error', 'message', 'stderr', 'output']) {
      if (value[key] != null && String(value[key]).trim()) return String(value[key]).trim();
    }
    try { return JSON.stringify(value); } catch (_) { return String(value); }
  }
  return String(value).trim();
}

function renderRuntimeIssues(data, worker, telemetry, source, eventApi, lifecycleState, runtimeState) {
  const issues = [];
  const add = (area, value, fallback) => {
    const raw = ingestErrorText(value);
    if (!raw) return;
    const message = detailedErrorMessage(value, fallback);
    if (!message || issues.some(item => item.message === message)) return;
    issues.push({area, message});
  };
  add('直播地址', source.error, '暂时没有获取到直播地址。系统会继续重试；持续失败时请检查直播地址权限和网络。');
  add('比赛事件', telemetry.last_event_error || eventApi.error, '比赛事件暂时无法获取。已有事件会保留，系统会继续重试。');
  add('事件时间', telemetry.last_shotmap_error, '事件精确时间暂时无法获取。新进球可能稍后补充，系统会继续重试。');
  add('视频接收', telemetry.latest_ingest_error || telemetry.last_ingest_error, '视频接收中断。已有片段会保留，系统会尝试重新连接。');
  add('处理进程', worker.cleanup_failure, '前一个处理任务没有正常清理，系统已阻止重复启动。');
  if (lifecycleState === 'completed_with_warnings' && !issues.length) {
    issues.push({area:'比赛收尾', message:`处理已经结束，但有任务没有完成。影响：部分 GIF 可能缺失，已经生成的文件不受影响。建议：逐条查看下方红色任务说明。${exitReasonLabel((data.lifecycle || {}).exit_reason) ? ` 本场状态：${exitReasonLabel((data.lifecycle || {}).exit_reason)}。` : ''}`});
  } else if (runtimeState === 'failed' && !issues.length) {
    add('运行状态', telemetry.exit_message || 'vision_processing_failed', '处理异常结束。请查看下方任务和运行日志中的详细原因。');
  }
  const container = $('runtime-issues');
  container.innerHTML = issues.map(item => `<div class="runtime-issue"><b>${escapeHtml(item.area)}</b><span>${escapeHtml(item.message)}</span></div>`).join('');
  container.classList.toggle('hidden', !issues.length);
}

function logPresentation(record) {
  const names = {
    worker_started:'处理已启动', worker_exited:'处理已结束', runtime_heartbeat:'运行状态更新',
    worker_restart_scheduled:'已安排自动恢复', worker_restart_failed:'自动恢复失败', live_source_restart_failed:'切换直播地址失败',
    process_group_stop_requested:'正在清理旧任务', process_group_term_timeout:'旧任务未按时退出', process_group_killed:'已强制清理旧任务', process_group_cleanup_complete:'旧任务清理完成', process_group_cleanup_failed:'旧任务清理失败',
    event_discovered:'捕获新增事件', event_duplicate:'重复事件已忽略', event_accepted:'事件已入队',
    goal_route_status:'事件来源确认', shotmap_direct:'事件接口直接提供',
    overview_fallback_empty:'备用赛况接口提供',
    overview_fallback_no_goal:'备用赛况接口提供',
    overview_fallback_no_match:'暂未匹配到事件',
    cross_source_merged:'两路接口信息已合并', shotmap_late_match:'事件接口稍后补充',
    event_cross_source_merged:'进球来源已合并', overview_goal_ignored_shotmap_primary:'等待事件接口确认进球',
    task_transition:'任务状态变化', gif_ready:'默认 GIF 已生成', api_error:'事件接口异常',
    vision_task_enqueued:'画面处理已排队', vision_task_transition:'画面处理状态变化', refined_gif_ready:'精剪 GIF 已生成',
    ingest_restart:'直播中断，准备重连', live_source_changed:'直播地址已切换', pipeline_stopped:'处理已结束',
    match_finishing_started:'比赛结束，开始收尾', worker_finish_requested:'开始确认最后事件',
    graceful_stop_requested:'进入收尾阶段', graceful_stop_ingest:'正在停止视频接收',
    match_finishing_completed:'本场处理已完成', worker_finish_timeout:'收尾超时，开始强制清理',
    worker_finish_request_failed:'开始收尾失败', worker_finish_timeout_signal_failed:'强制停止失败',
    worker_stopped:'已手动停止', monitor_error:'运行监控异常', task_recovered:'已恢复未完成任务'
  };
  let detail = friendlyErrorMessage(record.error || record.output || record.message || record.person || '', '');
  if (record.event === 'runtime_heartbeat') detail = `${record.buffer_segment_count || 0} 个视频片段 · 已查询 ${record.event_poll_count || 0} 次`;
  if (record.event === 'task_transition') {
    const code = ['G', 'OG', 'PG', 'YC', 'RC'].includes(String(record.code || '').toUpperCase()) ? String(record.code).toUpperCase() : '';
    detail = `${taskPresentation(record.to_status).label}${code ? ` · ${code}` : ''}`;
  }
  if (record.event === 'vision_task_transition') {
    const visionRecord = {...record, status:record.to_status};
    const failure = visionFailureDetail(visionRecord);
    detail = `${visionPresentation(visionRecord).label}${failure ? ` · ${failure}` : record.error ? ` · ${friendlyErrorMessage(record.error)}` : ''}`;
  }
  if (record.event === 'event_discovered') {
    const metadata = record.metadata && typeof record.metadata === 'object' ? record.metadata : {};
    const route = goalRouteStatusLabel(record.goal_route_status || record.route_status || metadata.goal_route_status);
    detail = `${record.code || ''}${record.minute ? ` · ${record.minute}'` : ''}${record.person ? ` · ${record.person}` : ''}${route ? ` · ${route}` : ''}`;
  }
  if (record.event === 'event_cross_source_merged') {
    const metadata = record.metadata && typeof record.metadata === 'object' ? record.metadata : {};
    const route = goalRouteStatusLabel(record.goal_route_status || record.route_status || metadata.goal_route_status);
    detail = `${route || '事件接口'}${record.second != null ? ` · ${matchClockText(record.second)}` : ''}`;
  }
  if (['goal_route_status', 'shotmap_direct', 'overview_fallback_empty', 'overview_fallback_no_goal', 'overview_fallback_no_match', 'cross_source_merged', 'shotmap_late_match'].includes(record.event)) {
    const route = goalRouteStatusLabel(record.goal_route_status || record.route_status || record.status || record.event);
    detail = route || detail;
    if (record.second != null) detail += ` · ${matchClockText(record.second)}`;
  }
  if (record.event === 'overview_goal_ignored_shotmap_primary') detail = `${record.minute ? `${record.minute}' · ` : ''}等待事件接口补充数据`;
  if (record.event === 'worker_started') detail = `${record.mode === 'demo' ? '演示处理' : '实时处理'} · 已启动`;
  if (record.event === 'worker_exited') detail = '处理进程已结束';
  if (record.event === 'ingest_restart') {
    const restartCount = finiteNumber(record.restart_count);
    const delaySeconds = finiteNumber(record.delay_seconds);
    const returnCode = finiteNumber(record.return_code);
    const parts = [];
    if (restartCount != null) parts.push(`第 ${Math.max(0, Math.floor(restartCount))} 次`);
    if (delaySeconds != null) parts.push(`等待 ${Number.isInteger(delaySeconds) ? delaySeconds : delaySeconds.toFixed(1)} 秒`);
    if (returnCode != null) parts.push('视频接收已重新启动');
    detail = parts.join(' · ') || detail || '已安排重连';
  }
  if (record.event === 'pipeline_stopped') detail = `视频接收已停止 · 已检查 ${record.event_poll_count || 0} 次`;
  if (record.event === 'match_finishing_started') detail = '已连续确认比赛结束';
  if (record.event === 'worker_finish_requested') detail = '已发送正常收尾请求';
  if (record.event === 'graceful_stop_requested') detail = '继续确认最后事件并等待后置画面';
  if (record.event === 'graceful_stop_ingest') detail = `${record.pending_count || 0} 个等待任务${record.timed_out ? ' · 已超时' : ' · 已排空'}`;
  if (record.event === 'match_finishing_completed') detail = `${record.lifecycle_state === 'completed' ? '正常完成' : '完成但有警告'} · ${exitReasonLabel(record.exit_reason)}`;
  return {name:names[record.event] || '系统记录', detail};
}

function render(data) {
  state.ocrAutomaticPublishing = !data.publishing || data.publishing.ocr_automatic !== false;
  if (data.heavy_tasks && typeof data.heavy_tasks === 'object') renderHeavyTasks(data);
  const detail = data.detail || {};
  const status = data.status || 'Uncertain';
  const statusEl = $('match-status'); statusEl.textContent = `${friendlyText(data.status_label, '') || '赛况未知'} · ${friendlyText(status, '未知')}`; statusEl.className = `status-pill ${statusClass(status)}`;
  const pollingConfig = data.polling || {}; const gifConfig = data.gif || {};
  syncInputValue('event-poll', pollingConfig.events_seconds); syncInputValue('source-poll', pollingConfig.source_seconds); syncInputValue('detail-poll', pollingConfig.detail_seconds); syncInputValue('before', gifConfig.before_seconds); syncInputValue('after', gifConfig.after_seconds); syncInputValue('event-offset', gifConfig.event_to_video_offset_seconds); syncInputValue('width', gifConfig.width);
  const visionConfig = data.vision || {}; const configuredVisionEnabled = visionConfig.enabled === true; const configuredTdeedEnabled = visionConfig.tdeed_enabled === true; const configuredClockOnly = visionConfig.clock_only === true; const workerVisionEnabled = visionConfig.worker_enabled === true; const workerTdeedEnabled = visionConfig.worker_tdeed_enabled === true; const workerClockOnly = visionConfig.worker_clock_only === true; if (document.activeElement !== $('vision-enabled')) $('vision-enabled').checked = configuredVisionEnabled; if (document.activeElement !== $('vision-clock-only')) $('vision-clock-only').checked = configuredClockOnly; if (document.activeElement !== $('vision-before') && visionConfig.before_seconds != null) $('vision-before').value = visionConfig.before_seconds; if (document.activeElement !== $('vision-after') && visionConfig.after_seconds != null) $('vision-after').value = visionConfig.after_seconds;
  const workerConfigKnown = Boolean(data.worker && Array.isArray(data.worker.command) && data.worker.command.length); $('vision-state').textContent = workerConfigKnown ? `当前处理：${workerVisionEnabled ? '开启' : '关闭'}` : configuredVisionEnabled ? '下次启动：开启' : '默认关闭'; $('vision-clock-only-state').textContent = workerConfigKnown ? `当前处理：${workerClockOnly ? '只读比赛时间' : '识别全部信息'}` : configuredClockOnly ? '下次启动：只读比赛时间' : '下次启动：识别全部信息';
  $('team-a').textContent = detail.team_A_name || '主队待加载'; $('team-b').textContent = detail.team_B_name || '客队待加载';
  $('score').textContent = detail.fs_A != null && detail.fs_A !== '' ? `${detail.fs_A} - ${detail.fs_B || 0}` : '-';
  $('match-minute').textContent = detail.minute ? `${detail.minute}' ${detail.minute_period || ''}` : '--';
  $('competition').textContent = detail.competition_name || detail.match_title || '比赛详情待加载';
  const startPlay = formatStartPlayBeijing(detail.start_play, true);
  $('start-play').textContent = startPlay ? `北京时间 ${startPlay}` : '开赛时间 --';
  for (const [id, src, letter] of [['team-a-logo', detail.team_A_logo, 'A'], ['team-b-logo', detail.team_B_logo, 'B']]) { const el = $(id); el.innerHTML = src ? `<img src="${escapeHtml(src)}" alt="">` : letter; }
  const source = data.source_health || {}; $('resource').textContent = source.resource || (source.error ? detailedErrorMessage(source.error, '暂时没有获取到直播地址') : '尚未获取直播地址'); $('updated-at').textContent = source.updated_at || '--'; $('source-change').classList.toggle('hidden', !source.changed);
  const setHealth = (id, text, cls) => { const el = $(id); el.textContent = text; el.className = cls || ''; };
  const publishing = data.publishing || {}; const uploadBackend = String(publishing.ocr_image_upload_backend || ''); const uploadBackendLabel = uploadBackend === 'official' ? '懂球帝官方接口' : uploadBackend === 'self_hosted' ? '自有服务器' : '未配置'; const uploadBackendReady = publishing.ocr_image_upload_ready === true; setHealth('ocr-upload-backend', uploadBackend ? `${uploadBackendLabel}${uploadBackendReady ? '' : '（未配置）'}` : uploadBackendLabel, uploadBackendReady ? 'ok' : 'warn');
  const worker = data.worker || {}; const telemetry = data.telemetry || {}; const counts = telemetry.task_counts || {};
  const lifecycle = data.lifecycle || {}; const lifecycleState = lifecycle.state || '';
  syncSessionSelection(data, worker, lifecycle);
  const runtimeState = telemetry.state || (worker.running ? 'starting' : 'idle');
  const workerStatus = $('worker-status'); workerStatus.className = `runtime-badge ${runtimeState}`; workerStatus.innerHTML = `<i></i>${escapeHtml(friendlyText(telemetry.label || '未启动'))}`;
  setHealth('source-health', source.resource ? (worker.mode === 'demo' ? '本地素材就绪' : '地址已获取') : (source.error ? '查询失败' : '未配置'), source.resource ? 'ok' : 'warn');
  const segmentCount = telemetry.buffer_segment_count || 0; const segmentAge = telemetry.latest_segment_age_seconds;
  const bufferSuffix = lifecycleState === 'finishing' ? ' · 比赛已结束，正在收尾' : lifecycleState === 'completed' ? ' · 处理完成' : lifecycleState === 'completed_with_warnings' ? ' · 完成但有警告' : runtimeState === 'completed' ? ' · 验收完成' : runtimeState === 'disconnected' ? ' · 流已结束' : segmentAge != null ? ` · ${Math.round(segmentAge)}秒前` : '';
  setHealth('buffer-health', segmentCount ? `${segmentCount} 个视频片段${bufferSuffix}` : '等待首个视频片段', segmentCount ? 'ok' : 'off');
  const lifecycleTerminal = lifecycleState === 'completed' || lifecycleState === 'completed_with_warnings' || lifecycleState === 'stopped';
  const eventApi = data.event_api || {}; const eventHasCurrentError = Boolean(telemetry.last_event_error || eventApi.error); const eventSummary = lifecycleTerminal ? `已停止 · 共 ${telemetry.event_poll_count || 0} 次` : eventHasCurrentError ? `${telemetry.event_poll_count || 0} 次 · 当前异常` : lifecycleState === 'finishing' ? `终场确认中 · ${telemetry.event_poll_count || 0} 次` : telemetry.event_poll_count ? `${telemetry.event_poll_count} 次 · 正常${telemetry.event_error_count ? `（重试 ${telemetry.event_error_count}）` : ''}` : worker.running ? '启动轮询中' : '等待启动'; setHealth('event-health', eventSummary, eventHasCurrentError && !lifecycleTerminal ? 'warn' : telemetry.event_poll_count || worker.running || lifecycleTerminal ? 'ok' : 'off');
  const readyCount = counts.encoded || 0; const activeCount = (counts.pending || 0) + (counts.encoding || 0); setHealth('gif-health', activeCount ? `${activeCount} 个处理中` : `${readyCount} 个 GIF`, activeCount ? 'active' : readyCount ? 'ok' : 'off');
  const cleanupActive = Boolean(worker.cleanup_process_group); const cleanupLabel = worker.cleanup_failure ? '清理失败' : worker.cleanup_stage === 'kill' ? '强制清理中' : '清理中';
  const workerRestartDue = finiteNumber(worker.restart_due_at_unix); const workerRestartRemaining = workerRestartDue == null ? null : Math.max(0, Math.ceil(workerRestartDue - Date.now() / 1000));
  const workerText = cleanupActive ? `已停止 · ${cleanupLabel}` : worker.running ? '运行中' : worker.desired_running && workerRestartDue != null ? `已停止 · ${workerRestartRemaining}秒后恢复` : worker.return_code != null ? Number(worker.return_code) === 0 ? '已正常停止' : '已停止 · 需要检查' : '未启动';
  setHealth('worker-process-health', workerText, worker.cleanup_failure || runtimeState === 'failed' ? 'error' : cleanupActive || worker.desired_running && !worker.running ? 'warn' : worker.running ? 'ok' : 'off');
  const ingestRunning = typeof telemetry.ingest_running === 'boolean' ? telemetry.ingest_running : null;
  const reconnectDue = finiteNumber(telemetry.ingest_reconnect_due_unix); const reconnectRemaining = reconnectDue == null ? null : Math.max(0, Math.ceil(reconnectDue - Date.now() / 1000));
  const ingestText = ingestRunning === true ? '采集中' : ingestRunning === false && reconnectDue != null ? (reconnectRemaining > 0 ? '等待重连' : '正在重连') : ingestRunning === false ? '已断开' : worker.running ? '未上报（旧后端）' : '未启动';
  const ingestClass = ingestRunning === true ? 'ok' : ingestRunning === false && reconnectDue != null ? 'warn' : ingestRunning === false && worker.running && !lifecycleTerminal ? 'error' : 'off';
  setHealth('ingest-health', ingestText, ingestClass);
  const segmentAgeValue = finiteNumber(telemetry.latest_segment_age_seconds); const segmentAgeText = segmentAgeValue == null ? '未上报' : `${Math.max(0, Math.round(segmentAgeValue))} 秒前`; const segmentIsFresh = telemetry.segment_writing === true || segmentAgeValue != null && segmentAgeValue <= 9;
  setHealth('segment-age-health', segmentAgeText, segmentAgeValue == null ? 'off' : segmentIsFresh ? 'ok' : ingestRunning === true || worker.running ? 'warn' : 'off');
  const ingestRestartCount = finiteNumber(telemetry.ingest_restart_count); const reconnectParts = !worker.running ? ['未启动'] : ingestRestartCount == null ? ['未上报'] : [`累计 ${Math.max(0, Math.floor(ingestRestartCount))} 次`];
  if (ingestRunning === false && reconnectDue != null) reconnectParts.push(reconnectRemaining > 0 ? `${reconnectRemaining}秒后重连` : '正在尝试重连');
  setHealth('reconnect-health', reconnectParts.join(' · '), ingestRunning === false && reconnectDue != null ? 'warn' : ingestRestartCount > 0 ? 'warn' : ingestRestartCount == null ? 'off' : 'ok');
  const lifecycleLabels = {idle:'未启动', starting:'正在启动', playing:'处理中', finishing:'收尾中', completed:'处理完成', completed_with_warnings:'完成但有警告', stopping:'正在停止', stopped:'已停止', failed:'处理失败'};
  const lifecycleText = `${friendlyText(data.status_label, status || '赛况未知')} · ${lifecycleLabels[lifecycleState] || (lifecycleState ? '状态处理中' : '状态未上报')}`;
  setHealth('lifecycle-health', lifecycleText, lifecycleState === 'failed' || lifecycleState === 'completed_with_warnings' && counts.failed ? 'error' : lifecycleState === 'completed_with_warnings' || lifecycleState === 'finishing' || lifecycleState === 'stopping' ? 'warn' : lifecycleState === 'playing' || lifecycleState === 'starting' || lifecycleState === 'completed' ? 'ok' : 'off');
  setHealth('runtime-elapsed', telemetry.elapsed_seconds != null ? fmtDuration(telemetry.elapsed_seconds) : '--', telemetry.elapsed_seconds != null ? 'ok' : 'off');
  const heartbeatAge = finiteNumber(telemetry.heartbeat_age_seconds); const heartbeatFresh = telemetry.heartbeat_fresh === true || heartbeatAge != null && heartbeatAge <= 9;
  const heartbeatText = telemetry.heartbeat_unix ? worker.running ? `${fmtTime(telemetry.heartbeat_unix)} · ${Math.round(heartbeatAge || 0)}秒前` : `最后 ${fmtTime(telemetry.heartbeat_unix)}` : '尚无心跳';
  setHealth('heartbeat-at', heartbeatText, telemetry.heartbeat_unix ? worker.running ? heartbeatFresh ? 'ok' : 'warn' : 'off' : 'off');
  setHealth('polling-health', lifecycleTerminal ? '已停止' : lifecycleState === 'finishing' ? '仅终场事件确认' : worker.running ? '运行中' : '未启动', lifecycleTerminal ? 'ok' : lifecycleState === 'finishing' || worker.running ? 'active' : 'off');
  setStep('source-step', source.resource ? 'ok' : source.error ? 'warn' : 'off'); setStep('buffer-step', segmentCount ? worker.running ? 'active' : 'ok' : 'off'); setStep('event-step', eventHasCurrentError ? 'warn' : telemetry.event_poll_count ? worker.running ? 'active' : 'ok' : 'off'); setStep('gif-step', counts.failed ? 'warn' : activeCount ? 'active' : readyCount ? 'ok' : 'off');
  const latestIngestError = ingestErrorText(telemetry.latest_ingest_error) || ingestErrorText(telemetry.last_ingest_error); const ingestErrorMessage = $('ingest-error-message'); $('ingest-error').textContent = latestIngestError ? detailedErrorMessage(latestIngestError, '视频接收失败，系统会尝试重连。') : '--'; ingestErrorMessage.classList.add('hidden');
  const runtimeMessage = $('runtime-message'); const finishDeadline = lifecycle.finishing_deadline_unix ? `，最迟 ${fmtTime(lifecycle.finishing_deadline_unix)}` : ''; const reasonLabel = exitReasonLabel(lifecycle.exit_reason); const rawMessage = worker.cleanup_failure || (lifecycleState === 'finishing' ? `比赛已结束，正在确认最后事件并等待后续画面${finishDeadline}` : lifecycleState === 'completed' ? `比赛已结束，处理进程、视频接收和事件查询均已停止${reasonLabel ? `（${reasonLabel}）` : ''}` : lifecycleState === 'completed_with_warnings' ? `处理已停止，但需要检查：${reasonLabel || '有部分任务未完成'}` : ''); const message = rawMessage ? friendlyErrorMessage(rawMessage, '') : ''; runtimeMessage.textContent = message ? `最近状态：${message}` : ''; runtimeMessage.classList.toggle('hidden', !message || runtimeState === 'healthy');
  renderRuntimeIssues(data, worker, telemetry, source, eventApi, lifecycleState, runtimeState);
  const eventCounts = data.event_counts || {}; $('event-count').textContent = `事件 ${eventCounts.unique || 0} · 已生成 ${eventCounts.encoded || 0} · 处理中 ${eventCounts.processing || 0} · 历史未生成 ${eventCounts.history || 0}`;
  const list = $('events'); const events = data.events || [];
  list.innerHTML = events.length ? events.map(e => {
    const type = eventPresentation(e); const task = taskPresentation(e.status);
    const artifacts = e.vision_artifacts && typeof e.vision_artifacts === 'object' ? e.vision_artifacts : {};
    const ocrWindow = e.ocr_window || artifacts.ocr_window || null;
    const tdeed = e.vision || artifacts.tdeed_refined || null;
    const ocrResourceWait = heavyTaskWaitFor(data.match_id, e.event_key, 'ocr_window');
    const tdeedResourceWait = heavyTaskWaitFor(data.match_id, e.event_key, 'tdeed_refined');
    const visionEnabled = worker.running ? workerVisionEnabled : configuredVisionEnabled;
    const tdeedEnabled = worker.running ? workerTdeedEnabled : configuredTdeedEnabled;
    const ocrBase = e.status === 'history' && !ocrWindow ? {label:'历史事件 · 未运行',cls:'off'} : visionPresentation(ocrWindow, visionEnabled, ocrResourceWait);
    const ocr = e.status === 'encoded' && ocrWindow && ocrWindow.status === 'failed'
      ? {label:`默认 GIF 可用 · ${ocrBase.label}`, cls:'warning'}
      : ocrBase;
    const vision = !tdeed && !tdeedEnabled ? {label:'已停用',cls:'off'} : e.status === 'history' && !tdeed ? {label:'历史事件 · 未运行',cls:'off'} : visionPresentation(tdeed, tdeedEnabled, tdeedResourceWait);
    const confidence = tdeed && tdeed.confidence != null ? ` · 识别把握 ${(Number(tdeed.confidence) * 100).toFixed(1)}%` : '';
    const delta = tdeed && tdeed.anchor_delta_seconds != null ? ` · 与事件时间相差 ${Number(tdeed.anchor_delta_seconds).toFixed(1)}秒` : '';
    const defaultCoverage = coverageStatusText(e);
    const ocrCoverage = coverageStatusText(ocrWindow);
    const visionCoverage = coverageStatusText(tdeed);
    const ocrFailureDetail = visionFailureDetail(ocrWindow);
    const failureDetail = visionFailureDetail(tdeed);
    const defaultFailureMarkup = taskFailureReportMarkup(e);
    const ocrFailureMarkup = visionFailureReportMarkup(ocrWindow);
    const visionFailureMarkup = visionFailureReportMarkup(tdeed);
    const ocrDiagnostics = visionOcrDiagnosticsText(ocrWindow);
    const ocrPipelineDiagnostics = ocrPipelineDiagnosticsText(ocrWindow);
    const metadata = e.metadata && typeof e.metadata === 'object' ? e.metadata : {};
    const targetClock = (ocrWindow && ocrWindow.target_clock) || matchClockText(e.second);
    const routeStatus = goalRouteStatusLabel(metadata.goal_route_status || metadata.route_status);
    const shotmapStatus = ({
      direct:'事件接口直接提供', matched:'事件接口已匹配', missing:'事件接口暂未提供秒数',
      ambiguous:'事件时间存在歧义', invalid:'事件秒数无效', stale:'等待事件接口重新匹配'
    })[metadata.shotmap_match_status] || '';
    const secondDetail = (e.code === 'G' || e.code === 'OG' || e.code === 'PG')
      ? [targetClock && targetClock !== '--' ? `接口时间 ${targetClock}` : '', routeStatus, shotmapStatus].filter(Boolean).join(' · ')
      : '';
    const ocrSource = (() => {
      if (!ocrWindow) return '';
      const source = String(ocrWindow.localization_source || '');
      const precision = String(ocrWindow.localization_precision || ocrWindow.precision || '');
      if (ocrWindow.output_kind === 'api_time_range_fallback') return '未通过 OCR 校准';
      if (source === 'projected' || precision === 'projected_second' || ocrWindow.degradation_mode === 'mapped_clock_projection') return '根据前后比赛时间推算';
      if (source === 'estimated' || precision === 'estimated_second') return '采用目标附近的真实画面';
      if (source === 'interpolated' || precision === 'interpolated_second') return '根据前后画面推算';
      if (source === 'exact_second' || source === 'exact' || precision === 'observed_second') return '已精确到秒';
      if (source === 'minute_boundary') return '分钟附近';
      return '';
    })();
    const ocrUserDetail = [secondDetail, ocrSource, scoreboardRegionStatusText(ocrWindow), ocrPendingStatusText(ocrWindow, ocrResourceWait), ocrFailureDetail, ocrUserDetailText(ocrWindow)].filter(Boolean).join(' · ');
    const technicalDetail = [ocrPipelineDiagnostics, ocrDiagnostics, tdeed && tdeed.source_ocr_artifact ? '动作精剪使用了上方 60 秒画面时间结果' : ''].filter(Boolean).join(' · ');
    const visionDetail = [visionResourceWaitText(tdeedResourceWait), failureDetail].filter(Boolean).join(' · ');
    const technicalDetailsKey = `${data.match_id || ''}\n${e.event_key || ''}\nocr_window`;
    const technicalMarkup = technicalDetail ? `<details class="technical-details" data-details-key="${escapeHtml(technicalDetailsKey)}"${state.openTechnicalDetails.has(technicalDetailsKey) ? ' open' : ''}><summary>查看完整处理记录</summary><small>${escapeHtml(friendlyText(technicalDetail))}</small></details>` : '';
    const gifLink = artifact => artifact && artifact.output ? `<a class="gif-link" href="/api/gif/${encodeURIComponent(data.match_id)}/${encodeURIComponent(artifact.output.split('/').pop())}" target="_blank">预览</a>` : '';
    const ocrArtifactLabel = ocrWindow && ocrWindow.output_kind === 'api_time_range_fallback' ? '接口时间范围兜底' : '画面时间 60秒';
    const defaultUploaded = e.uploaded_gif && e.uploaded_gif.url ? e.uploaded_gif : null;
    const defaultPreview = e.output ? `<a class="gif-link" href="/api/gif/${encodeURIComponent(data.match_id)}/${encodeURIComponent(e.output.split('/').pop())}" target="_blank">预览</a>` : defaultUploaded ? `<a class="gif-link" href="${escapeHtml(defaultUploaded.url)}" target="_blank" rel="noopener">预览</a>` : '';
    const defaultAccount = publicationAccountText(e.publish);
    const defaultActions = e.output || defaultUploaded || defaultAccount ? `<div class="default-artifact-actions">${defaultPreview}${defaultAccount ? `<small class="publish-account-used">${escapeHtml(defaultAccount)}</small>` : ''}</div>` : '';
    const ocrPreview = gifLink(ocrWindow);
    const ocrArtifact = ocrWindow
      ? {...ocrWindow, ...(e.ocr_uploaded_gif ? {uploaded_gif:e.ocr_uploaded_gif} : {})}
      : (e.ocr_uploaded_gif ? {status:'encoded', uploaded_gif:e.ocr_uploaded_gif} : null);
    const ocrUploadedPreview = !ocrPreview && e.ocr_uploaded_gif && e.ocr_uploaded_gif.url ? `<a class="gif-link" href="${escapeHtml(e.ocr_uploaded_gif.url)}" target="_blank" rel="noopener">预览</a>` : '';
    const ocrArticleStatus = automaticArticleStatus(e, ocrArtifact);
    const ocrActions = ocrPreview || ocrUploadedPreview || ocrArticleStatus ? `<div class="artifact-actions ocr-artifact-actions">${ocrPreview || ocrUploadedPreview}${ocrArticleStatus}</div>` : '';
    return `<div class="event-row ${escapeHtml(task.cls)}"><div class="event-type event-type-${escapeHtml(type.kind)}"><span class="event-symbol" aria-hidden="true"></span><span class="event-type-text"><b>${escapeHtml(type.label)}</b><small>${escapeHtml(type.code)}</small></span></div><div class="event-minute">${escapeHtml(e.minute || '--')}'${e.minute_extra && e.minute_extra !== '0' ? `+${escapeHtml(e.minute_extra)}` : ''}</div><div class="event-person">${escapeHtml(e.person || '未提供球员')}<small>${escapeHtml(e.team || '')}${e.score ? ` · ${escapeHtml(e.score)}` : ''}${e.reason ? ` · ${friendlyText(e.reason)}` : ''}</small></div><div class="artifact-list"><div class="artifact ${e.status === 'failed' ? 'failed' : ''}"><div class="artifact-copy"><span>默认 · ${escapeHtml(task.label)}${defaultCoverage ? ` · ${escapeHtml(defaultCoverage)}` : ''}</span>${defaultFailureMarkup}</div>${defaultActions}</div><div class="artifact ${escapeHtml(ocr.cls)}"><div class="artifact-copy"><span>${escapeHtml(ocrArtifactLabel)} · ${escapeHtml(ocr.label)}${ocrCoverage ? ` · ${escapeHtml(ocrCoverage)}` : ''}${ocrUserDetail ? `<small>${escapeHtml(ocrUserDetail)}</small>` : ''}</span>${ocrFailureMarkup}${technicalMarkup}</div>${ocrActions}</div><div class="artifact ${escapeHtml(vision.cls)}"><div class="artifact-copy"><span>动作精剪 20秒 · ${escapeHtml(vision.label)}${escapeHtml(confidence)}${escapeHtml(delta)}${visionCoverage ? ` · ${escapeHtml(visionCoverage)}` : ''}${tdeed && tdeed.experimental ? ' · 实验' : ''}${visionDetail ? `<small>${escapeHtml(visionDetail)}</small>` : ''}</span>${visionFailureMarkup}</div>${gifLink(tdeed)}</div></div></div>`;
  }).join('') : '<div class="empty">暂无已发现事件。启动处理后，进球、黄牌、红牌和乌龙球会在这里显示。</div>';
  const logs = $('logs'); const records = data.logs || []; let heartbeatSeen = false; const visibleRecords = records.filter(record => record.event !== 'runtime_heartbeat' || (!heartbeatSeen && (heartbeatSeen = true))); logs.innerHTML = visibleRecords.length ? visibleRecords.slice(0, 40).map(l => { const presentation = logPresentation(l); return `<div class="log-line log-${escapeHtml(l.event || '')}"><time>${escapeHtml((l.timestamp || '').replace('T',' ').replace('Z','').slice(0,19))}</time><b>${escapeHtml(presentation.name)}</b><span>${escapeHtml(presentation.detail)}</span></div>`; }).join('') : '<div class="empty">暂无日志</div>';
  $('last-refresh').textContent = `更新于 ${new Date().toLocaleTimeString('zh-CN', {hour12:false})}`;
  if (data.event_api && data.event_api.error) showNotice(detailedErrorMessage(data.event_api.error, '比赛事件暂时无法获取，系统会继续重试。')); else if (source.error && source.error.includes('GIF_SOURCE_SECRET')) showNotice(detailedErrorMessage(source.error, '还没有配置直播地址。')); else clearNotice();
}

async function requestJson(url, options = {}) { const response = await fetch(url, {headers:{'Content-Type':'application/json'}, ...options}); const data = await response.json(); if (!response.ok) throw new Error(data.error || `请求失败 ${response.status}`); return data; }
async function refresh(requestedId = state.sessionMatchId || matchId()) {
  const id = String(requestedId || '').trim();
  if (!id || state.actionPending) return;
  const viewSequence = state.viewSequence;
  const requestSerial = ++state.refreshRequestSerial;
  try {
    const data = await requestJson(`/api/session?match_id=${encodeURIComponent(id)}`);
    if (viewSequence !== state.viewSequence || id !== state.sessionMatchId || requestSerial <= state.lastRenderedRefreshSerial) return;
    state.lastRenderedRefreshSerial = requestSerial;
    render(data);
  } catch (error) {
    if (viewSequence === state.viewSequence && id === state.sessionMatchId && requestSerial > state.lastRenderedRefreshSerial) showNotice(detailedErrorMessage(error.message), 'error');
  }
}
async function refreshMatches() {
  if (state.matchesLoading) return;
  state.matchesLoading = true;
  const healthEl = $('discovery-health');
  healthEl.className = 'discovery-health loading';
  healthEl.innerHTML = '<i></i>正在刷新';
  try {
    renderMatches(await requestJson('/api/matches'));
  } catch (error) {
    healthEl.className = `discovery-health ${state.matches ? 'warning' : 'error'}`;
    healthEl.innerHTML = `<i></i>${state.matches ? '刷新失败' : '接口不可用'}`;
    $('discovery-detail').textContent = `赛事目录暂时无法刷新，仍可手工输入比赛 ID。${detailedErrorMessage(error.message, '')}`;
  } finally {
    state.matchesLoading = false;
  }
}
async function configure(id = matchId()) { const body = {match_id:id, event_poll_seconds:+$('event-poll').value, source_poll_seconds:+$('source-poll').value, detail_poll_seconds:+$('detail-poll').value, before_seconds:+$('before').value, after_seconds:+$('after').value, event_to_video_offset_seconds:+$('event-offset').value, gif_width:+$('width').value, vision_enabled:$('vision-enabled').checked, vision_clock_only:$('vision-clock-only').checked, vision_before_seconds:+$('vision-before').value, vision_after_seconds:+$('vision-after').value}; return requestJson('/api/session', {method:'POST', body:JSON.stringify(body)}); }
function selectViewedMatch(id, {syncInput = true} = {}) {
  const normalized = String(id || '').trim();
  if (!normalized) return;
  state.sessionMatchId = normalized;
  state.sessionLocked = activeMatchIds().has(normalized);
  state.viewSequence += 1;
  state.lastRenderedRefreshSerial = state.refreshRequestSerial;
  if (syncInput) $('match-id').value = normalized;
  if (state.matches) renderMatches(state.matches);
  else applyControlAvailability();
  renderSelectionHint();
}
async function startSelectedMatch(extra = {}) {
  const id = matchId();
  if (!id || state.actionPending) return;
  selectViewedMatch(id);
  state.actionPending = true;
  applyControlAvailability();
  try {
    const configured = await configure(id);
    if (state.sessionMatchId === id) render(configured);
    const data = await requestJson('/api/session/start', {method:'POST', body:JSON.stringify({match_id:id, ...extra})});
    if (state.sessionMatchId === id) render(data);
    clearNotice();
    await refreshMatches();
  } catch (error) {
    showNotice(detailedErrorMessage(error.message), 'error');
  } finally {
    state.actionPending = false;
    applyControlAvailability();
  }
}
async function stopCurrentMatch() {
  const id = state.sessionMatchId;
  if (!id || state.actionPending) return;
  state.viewSequence += 1;
  state.lastRenderedRefreshSerial = state.refreshRequestSerial;
  state.actionPending = true;
  applyControlAvailability();
  try {
    const data = await requestJson('/api/session/stop', {method:'POST', body:JSON.stringify({match_id:id})});
    if (state.sessionMatchId === id) render(data);
    clearNotice();
    await refreshMatches();
  } catch (error) {
    showNotice(detailedErrorMessage(error.message), 'error');
  } finally {
    state.actionPending = false;
    applyControlAvailability();
  }
}
const urlMatchId = new URLSearchParams(window.location.search).get('match_id');
if (urlMatchId) $('match-id').value = urlMatchId;
state.sessionMatchId = matchId();
$('discovery-toggle').addEventListener('click', () => setDiscoveryCollapsed(!state.discoveryCollapsed));
$('active-match-tabs').addEventListener('click', event => {
  const tab = event.target.closest('.active-match-tab');
  if (!tab || !tab.dataset.activeMatchId) return;
  selectViewedMatch(tab.dataset.activeMatchId);
  refresh(tab.dataset.activeMatchId);
});
$('new-match-btn').addEventListener('click', () => {
  $('match-id').value = '';
  $('match-id').focus();
  if (state.matches) renderMatches(state.matches);
  else applyControlAvailability();
});
for (const id of ['playing-matches', 'upcoming-matches']) {
  $(id).addEventListener('click', event => {
    const button = event.target.closest('.discovery-match');
    if (!button || !button.dataset.matchId) return;
    $('match-id').value = button.dataset.matchId;
    if (state.matches) renderMatches(state.matches);
  });
}
$('match-id').addEventListener('input', () => { if (state.matches) renderMatches(state.matches); else { applyControlAvailability(); renderSelectionHint(); } });
$('load-btn').addEventListener('click', () => { const id = matchId(); if (!id) return; selectViewedMatch(id); refresh(id); });
$('demo-btn').addEventListener('click', () => {
  $('match-id').value = `demo-proof-${Date.now()}`;
  if (state.matches) renderMatches(state.matches);
  startSelectedMatch({demo:true});
});
$('start-btn').addEventListener('click', () => startSelectedMatch());
$('stop-btn').addEventListener('click', stopCurrentMatch);
$('publish-account-add').addEventListener('click', () => {
  state.publishAccounts.push({user_id:'', user_name:'', enabled:true});
  state.publishAccountsDirty = true;
  renderPublishAccounts();
  const rows = document.querySelectorAll('.publish-account-row');
  const input = rows.length ? rows[rows.length - 1].querySelector('.publish-account-id') : null;
  if (input) input.focus();
  setPublishAccountMessage('新增账号尚未保存。', 'warning');
});
$('publish-account-save').addEventListener('click', savePublishAccounts);
$('publish-account-list').addEventListener('input', event => {
  const row = event.target.closest('.publish-account-row');
  if (!row) return;
  const index = Number(row.dataset.accountIndex);
  const account = state.publishAccounts[index];
  if (!account) return;
  if (event.target.classList.contains('publish-account-id')) account.user_id = event.target.value;
  if (event.target.classList.contains('publish-account-name')) account.user_name = event.target.value;
  state.publishAccountsDirty = true;
  updatePublishAccountSummary();
  setPublishAccountMessage('账号池有未保存的修改。', 'warning');
});
$('publish-account-list').addEventListener('change', event => {
  if (!event.target.matches('.publish-account-enabled input')) return;
  const row = event.target.closest('.publish-account-row');
  const account = row ? state.publishAccounts[Number(row.dataset.accountIndex)] : null;
  if (!account) return;
  account.enabled = event.target.checked;
  state.publishAccountsDirty = true;
  updatePublishAccountSummary();
  setPublishAccountMessage('账号启用状态尚未保存。', 'warning');
});
$('publish-account-list').addEventListener('click', event => {
  const button = event.target.closest('.publish-account-remove');
  if (!button) return;
  const row = button.closest('.publish-account-row');
  if (!row) return;
  state.publishAccounts.splice(Number(row.dataset.accountIndex), 1);
  state.publishAccountsDirty = true;
  renderPublishAccounts();
  setPublishAccountMessage('账号已从列表移除，保存后生效。', 'warning');
});
$('events').addEventListener('toggle', event => {
  const details = event.target.closest('details[data-details-key]');
  if (!details) return;
  const key = String(details.dataset.detailsKey || '');
  if (!key) return;
  if (details.open) state.openTechnicalDetails.add(key);
  else state.openTechnicalDetails.delete(key);
}, true);
refresh();
refreshMatches();
loadPublishAccounts();
state.timer = setInterval(refresh, 1000);
state.matchesTimer = setInterval(refreshMatches, MATCH_DIRECTORY_INTERVAL_MS);
