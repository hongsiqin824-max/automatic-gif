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
};

function matchId() { return $('match-id').value.trim(); }
function showNotice(message, kind = 'warning') { const el = $('notice'); el.textContent = message; el.className = `notice ${kind === 'error' ? 'error' : ''}`; }
function clearNotice() { $('notice').className = 'notice hidden'; }
function statusClass(value) { return String(value || 'uncertain').toLowerCase(); }
function fmtTime(unix) { return unix ? new Date(unix * 1000).toLocaleTimeString('zh-CN', {hour12:false}) : '--'; }
function fmtDuration(seconds) { const value = Math.max(0, Math.floor(Number(seconds) || 0)); const minutes = Math.floor(value / 60); const rest = value % 60; return minutes ? `${minutes}分 ${rest}秒` : `${rest}秒`; }
function escapeHtml(value) { return String(value ?? '').replace(/[&<>'"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c])); }
function finiteNumber(value) { if (value == null || value === '') return null; const number = Number(value); return Number.isFinite(number) ? number : null; }
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
    element.innerHTML = '<span>重任务</span><b>槽位 --</b><b>占用 --</b><b>排队 --</b><b>视觉 --</b>';
    return;
  }
  const total = firstMetric(raw, ['total_slots', 'total', 'max', 'capacity', 'slots']);
  const occupied = firstMetric(raw, ['occupied', 'used', 'active', 'running']);
  const queued = firstMetric(raw, ['queued', 'queue', 'pending', 'waiting']);
  const vision = firstMetric(raw, ['vision', 'vision_active', 'vision_running', 'vision_tasks']);
  const metric = value => value == null ? '--' : value;
  element.className = `heavy-task-summary${queued ? ' queued' : ''}`;
  element.innerHTML = `<span>重任务</span><b>槽位 ${metric(total)}</b><b>占用 ${metric(occupied)}</b><b>排队 ${metric(queued)}</b><b>视觉 ${metric(vision)}</b>`;
}
function activeStateLabel(match) {
  const stateValue = String(match.lifecycle_state || '').toLowerCase();
  if (stateValue === 'finishing') return '收尾中';
  if (stateValue === 'stopping') return '停止中';
  if (match.worker_running) return match.worker_mode === 'demo' ? '演示运行中' : '实时运行中';
  if (match.restart_due_at_unix) return '等待自动恢复';
  if (match.cleanup_process_group) return '清理旧进程';
  return match.desired_running ? '准备恢复' : stateValue || '活动中';
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
  if (health.error && !hasData) return {label:health.label || '接口异常', cls:'error'};
  if (health.from_cache || value === 'degraded' || value === 'stale') return {label:health.label || '缓存可用', cls:'warning'};
  if (value === 'error' || value === 'failed' || value === 'unhealthy') return {label:health.label || '接口异常', cls:'error'};
  return {label:health.label || (value === 'healthy' ? '接口正常' : hasData ? '接口正常' : '已连接'), cls:'healthy'};
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
  if (health.error) details.push(String(health.error));
  $('discovery-detail').textContent = details.join(' · ') || '赛事目录已更新';
  applyControlAvailability();
  renderSelectionHint();
}
function exitReasonLabel(reason) {
  return ({
    match_played:'比赛正常结束', match_played_stream_incomplete:'直播提前中断，最后画面不完整',
    match_played_finish_timeout:'收尾超时，已强制停止', ingest_error:'直播接收异常',
    ingest_completed:'直播输入结束', manual_stop:'手动停止'
  })[reason] || reason || '';
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
    pending: {label:'已捕获 · 等待后置', cls:'pending'},
    encoding: {label:'GIF 编码中', cls:'encoding'},
    encoded: {label:'GIF 已生成', cls:'encoded'},
    failed: {label:'生成失败', cls:'failed'},
  })[status] || {label:status || '等待处理', cls:'pending'};
}

function visionLocatorMethodLabel(vision) {
  if (!vision) return '';
  const raw = String(vision.locator_method || '').trim();
  const normalized = raw.toLowerCase().replace(/[\s_-]+/g, '');
  if (normalized.includes('minuterangefallback')) return '分钟范围兜底';
  if (normalized.includes('ocr') && normalized.includes('tdeed')) return 'OCR + T-DEED';
  if (normalized.includes('ocr')) return 'OCR';
  if (normalized.includes('tdeed')) return 'T-DEED';
  if (!raw && String(vision.model_name || '').toLowerCase().includes('t-deed')) return 'T-DEED';
  return raw;
}

function visionPresentation(vision, enabled = true) {
  if (!vision) return enabled ? {label:'等待 AI 任务', cls:'off'} : {label:'AI 已关闭', cls:'off'};
  if (vision.artifact_kind === 'ocr_window') {
    const degraded = vision.degraded === true || vision.localization_quality === 'degraded';
    return ({
      pending:{label:'等待时钟定位',cls:'pending'}, locating:{label:'OCR 时钟定位中',cls:'encoding'},
      located:{label:degraded ? '分钟降级定位已完成' : '目标时刻已定位',cls:'encoding'},
      encoding:{label:degraded ? '分钟降级 GIF 编码中' : '60秒 GIF 编码中',cls:'encoding'},
      encoded:{label:degraded ? '分钟降级 GIF 已生成' : '60秒 GIF 已生成',cls:'encoded'},
      failed:{label:'OCR 链路失败',cls:'failed'}
    })[vision.status] || {label:vision.status || '等待 OCR 任务', cls:'off'};
  }
  const method = visionLocatorMethodLabel(vision);
  const minuteFallback = vision.minute_fallback === true;
  const fragmentedFallback = minuteFallback && vision.fallback_complete === false;
  const fallbackLabel = fragmentedFallback ? 'OCR 分钟范围残缺片段' : 'OCR 分钟兜底';
  return ({
    pending:{label:'等待搜索窗口',cls:'pending'}, locating:{label:'画面定位中',cls:'encoding'},
    located:{label:minuteFallback ? (fragmentedFallback ? 'OCR 分钟范围残缺片段定位' : 'OCR 分钟锚点已定位') : `${method ? `${method} · ` : ''}锚点已定位`,cls:'encoding'}, encoding:{label:minuteFallback ? `${fallbackLabel}编码中` : '精剪编码中',cls:'encoding'},
    encoded:{label:minuteFallback ? (fragmentedFallback ? 'OCR 分钟范围残缺片段已生成' : 'OCR 分钟兜底已生成') : `${method ? `${method} 定位 · ` : ''}精剪已生成`,cls:'encoded'}, failed:{label:'精剪失败',cls:'failed'}
  })[vision.status] || {label:vision.status || '等待 AI 任务', cls:'off'};
}

function visionFailureDetail(vision) {
  if (!vision) return '';
  const structured = vision.failure_reason && typeof vision.failure_reason === 'object' ? vision.failure_reason : {};
  if (vision.status !== 'failed' && !structured.kind) return '';
  const stage = String(structured.stage || vision.stage || '').trim().toLowerCase();
  const kind = String(structured.kind || vision.error_kind || vision.last_error_kind || '').trim();
  const stageLabel = ({
    materializing:'候选视频准备', materialize:'候选视频准备', ocr:'OCR 定位',
    locating:'画面定位', tdeed:'T-DEED 定位', fallback:'T-DEED 回退',
    tdeed_fallback:'T-DEED 回退', failed:'画面定位', buffer:'视频缓存',
    encoding:'精剪编码', encode:'精剪编码',
    waiting_for_default_gif:'等待默认 GIF', scoreboard_profile:'比分牌布局配置',
    event_second_localization:'事件秒级定位', event_localization:'事件定位',
    fragmented_search:'视频连续片段扫描', buffer_coverage:'视频窗口覆盖检查',
    ocr_clock_discovery:'OCR 时钟识别', ocr_target_localization:'OCR 目标时刻定位',
    ocr_window_encoding:'OCR 60秒 GIF 编码',
    tdeed_model_unavailable:'T-DEED 模型加载', tdeed_inference:'T-DEED 推理',
    tdeed_candidate_selection:'T-DEED 候选选择', tdeed_output_encoding:'T-DEED 20秒 GIF 编码'
  })[stage] || stage || '未知阶段';
  const reasonLabel = ({
    waiting_for_video:'视频窗口未就绪', video_unavailable:'搜索窗口没有可用视频',
    buffer_history_missing:'所需历史切片已缺失', buffer_gap:'视频切片存在缺口',
    vision_deadline_exceeded:'视觉任务超过截止时间', ocr_no_clock:'OCR 未识别到比赛时钟',
    ocr_no_match:'OCR 没有找到匹配时刻', ocr_processing_failed:'OCR 处理失败',
    scoreboard_missing:'画面中未检测到比分牌', ocr_clock_unreadable:'OCR 无法读取比赛时钟',
    ocr_score_unreadable:'OCR 无法读取比分', ocr_no_score_transition:'未检测到稳定比分变化',
    ocr_ambiguous:'OCR 结果存在歧义', ocr_no_target:'OCR 未返回目标锚点', ocr_model_unavailable:'PaddleOCR 不可用',
    clock_profile_mismatch:'比分牌布局未配置或不匹配',
    tdeed_model_unavailable:'T-DEED 模型不可用', tdeed_no_candidate:'T-DEED 未找到候选动作',
    tdeed_inference_failed:'T-DEED 推理失败', inference_timeout:'视觉推理超时',
    encode_failed:'精剪 GIF 编码失败',
    model_inference_failed:'T-DEED 推理失败', vision_processing_failed:'视觉处理失败',
    video_gap:'视频分片存在缺口', anchor_gap:'定位锚点附近存在视频缺口',
    degraded_clip_too_short:'可用精剪片段过短', default_gif_failed:'默认 GIF 生成失败',
    fragmented_minute_fallback:'OCR 分钟范围只生成了残缺片段'
    ,ocr_target_localization_failed:'OCR 未定位到目标比赛时钟'
    ,ocr_window_encoding_failed:'OCR 60秒 GIF 编码失败'
    ,tdeed_candidate_selection_failed:'T-DEED 候选位置无效'
    ,tdeed_output_encoding_failed:'T-DEED 20秒 GIF 编码失败'
    ,upstream_ocr_window_failed:'OCR 60秒链路失败，T-DEED 未运行'
  })[kind] || kind || String(vision.error || '').trim() || '未提供原因';
  const message = String(structured.message || '').trim();
  const attempts = Array.isArray(vision.fragment_attempts) ? vision.fragment_attempts : [];
  const fragmentDetail = attempts.length ? ` · 已扫描 ${attempts.length} 个连续片段` : '';
  return `阶段：${stageLabel} · 原因：${reasonLabel}${message ? ` · ${message}` : ''}${fragmentDetail}`;
}

function visionOcrDiagnosticsText(vision) {
  const diagnostics = vision && vision.ocr_diagnostics;
  if (!diagnostics) return '';
  if (typeof diagnostics === 'string') return diagnostics.trim();
  if (typeof diagnostics !== 'object') return String(diagnostics);
  if (diagnostics.summary) return String(diagnostics.summary);
  const fields = [
    ['sampled_frames', '采样'], ['frames_sampled', '采样'],
    ['clock_readable_frames', '时钟帧'], ['clock_frames', '时钟帧'], ['valid_clock_frames', '时钟帧'],
    ['score_readable_frames', '比分帧'], ['clock_repaired_frames', '修复帧'],
    ['scoreboard_missing_frames', '消失帧'],
    ['candidate_count', '候选'], ['candidates', '候选']
  ];
  const seen = new Set();
  const parts = [];
  for (const [key, label] of fields) {
    if (diagnostics[key] == null || seen.has(label)) continue;
    seen.add(label); parts.push(`${label} ${diagnostics[key]}`);
  }
  if (diagnostics.target_clock) parts.unshift(`目标时钟 ${diagnostics.target_clock}`);
  if (diagnostics.exact_second_failure_reason) {
    const reason = ({
      target_clock_not_found:'未找到目标秒',
      no_trustworthy_clock_readings:'没有可信时钟读数',
      multiple_disjoint_occurrences:'目标秒多处出现'
    })[diagnostics.exact_second_failure_reason] || diagnostics.exact_second_failure_reason;
    parts.push(`秒级降级 ${reason}`);
  }
  return parts.join(' · ');
}

function matchClockText(value) {
  const seconds = Number(value);
  if (!Number.isInteger(seconds) || seconds < 0) return '';
  const minute = Math.floor(seconds / 60);
  return `${String(minute).padStart(2, '0')}:${String(seconds % 60).padStart(2, '0')}`;
}

// The worker persists this value in event.metadata so the dashboard can
// explain which source supplied the goal without inferring from timing fields.
function goalRouteStatusLabel(value) {
  return ({
    shotmap_direct: 'shotmap 直接触发',
    overview_fallback_empty: 'overview 兜底 · shotmap 空数据',
    overview_fallback_no_goal: 'overview 兜底 · shotmap 无进球',
    overview_fallback_no_match: 'overview 兜底 · 未匹配 shotmap 进球',
    cross_source_merged: '两路来源已合并',
    shotmap_late_match: 'shotmap 延迟匹配'
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
function logPresentation(record) {
  const names = {
    worker_started:'处理进程已启动', worker_exited:'处理进程已退出', runtime_heartbeat:'运行心跳',
    worker_restart_scheduled:'已安排自动恢复', worker_restart_failed:'自动恢复失败', live_source_restart_failed:'切源恢复失败',
    process_group_stop_requested:'正在清理旧进程', process_group_term_timeout:'旧进程未按时退出', process_group_killed:'已强制清理旧进程', process_group_cleanup_complete:'旧进程清理完成', process_group_cleanup_failed:'旧进程清理失败',
    event_discovered:'捕获新增事件', event_duplicate:'重复事件已忽略', event_accepted:'事件已入队',
    goal_route_status:'进球路由判定', shotmap_direct:'shotmap 直接触发',
    overview_fallback_empty:'overview 兜底 · shotmap 空数据',
    overview_fallback_no_goal:'overview 兜底 · shotmap 无进球',
    overview_fallback_no_match:'overview 兜底 · 未匹配 shotmap 进球',
    cross_source_merged:'两路来源已合并', shotmap_late_match:'shotmap 延迟匹配',
    event_cross_source_merged:'进球来源已合并', overview_goal_ignored_shotmap_primary:'overview 进球由 shotmap 接管',
    task_transition:'任务状态变化', gif_ready:'默认 GIF 已生成', api_error:'事件接口异常',
    vision_task_enqueued:'AI 精剪已入队', vision_task_transition:'AI 精剪状态变化', refined_gif_ready:'AI 精剪已生成',
    ingest_restart:'直播断开重连', live_source_changed:'直播源已切换', pipeline_stopped:'处理链路已结束',
    match_finishing_started:'比赛结束，开始收尾', worker_finish_requested:'已通知 Worker 收尾',
    graceful_stop_requested:'Worker 已进入收尾', graceful_stop_ingest:'FFmpeg 正在停止',
    match_finishing_completed:'本场处理已完成', worker_finish_timeout:'收尾超时，开始强制清理',
    worker_finish_request_failed:'通知 Worker 收尾失败', worker_finish_timeout_signal_failed:'强制停止失败',
    worker_stopped:'已手动停止', monitor_error:'监控异常', task_recovered:'恢复未完成任务'
  };
  let detail = record.error || record.output || record.message || record.person || '';
  if (record.event === 'runtime_heartbeat') detail = `${record.buffer_segment_count || 0} 个分片 · 轮询 ${record.event_poll_count || 0} 次`;
  if (record.event === 'task_transition') detail = `${taskPresentation(record.to_status).label}${record.code ? ` · ${record.code}` : ''}`;
  if (record.event === 'vision_task_transition') {
    const visionRecord = {...record, status:record.to_status};
    const failure = visionFailureDetail(visionRecord);
    detail = `${visionPresentation(visionRecord).label}${failure ? ` · ${failure}` : record.error ? ` · ${record.error}` : ''}`;
  }
  if (record.event === 'event_discovered') {
    const metadata = record.metadata && typeof record.metadata === 'object' ? record.metadata : {};
    const route = goalRouteStatusLabel(record.goal_route_status || record.route_status || metadata.goal_route_status);
    detail = `${record.code || ''}${record.minute ? ` · ${record.minute}'` : ''}${record.person ? ` · ${record.person}` : ''}${route ? ` · ${route}` : ''}`;
  }
  if (record.event === 'event_cross_source_merged') {
    const metadata = record.metadata && typeof record.metadata === 'object' ? record.metadata : {};
    const route = goalRouteStatusLabel(record.goal_route_status || record.route_status || metadata.goal_route_status);
    detail = `${route || '主来源 shotmap'}${record.second != null ? ` · ${matchClockText(record.second)}` : ''}`;
  }
  if (['goal_route_status', 'shotmap_direct', 'overview_fallback_empty', 'overview_fallback_no_goal', 'overview_fallback_no_match', 'cross_source_merged', 'shotmap_late_match'].includes(record.event)) {
    const route = goalRouteStatusLabel(record.goal_route_status || record.route_status || record.status || record.event);
    detail = route || detail;
    if (record.second != null) detail += ` · ${matchClockText(record.second)}`;
  }
  if (record.event === 'overview_goal_ignored_shotmap_primary') detail = `${record.minute ? `${record.minute}' · ` : ''}等待 shotmap 新增数据`;
  if (record.event === 'worker_started') detail = `${record.mode === 'demo' ? '演示验收' : '实时处理'} · PID ${record.pid || '--'}`;
  if (record.event === 'worker_exited') detail = `返回码 ${record.return_code ?? '--'}`;
  if (record.event === 'ingest_restart') {
    const restartCount = finiteNumber(record.restart_count);
    const delaySeconds = finiteNumber(record.delay_seconds);
    const returnCode = finiteNumber(record.return_code);
    const parts = [];
    if (restartCount != null) parts.push(`第 ${Math.max(0, Math.floor(restartCount))} 次`);
    if (delaySeconds != null) parts.push(`等待 ${Number.isInteger(delaySeconds) ? delaySeconds : delaySeconds.toFixed(1)} 秒`);
    if (returnCode != null) parts.push(`返回码 ${returnCode}`);
    detail = parts.join(' · ') || detail || '已安排重连';
  }
  if (record.event === 'pipeline_stopped') detail = `FFmpeg 返回码 ${record.ffmpeg_return_code ?? '--'} · 轮询 ${record.event_poll_count || 0} 次`;
  if (record.event === 'match_finishing_started') detail = `已连续确认 ${record.played_confirmations || 0} 次比赛结束`;
  if (record.event === 'worker_finish_requested') detail = `PID ${record.pid || '--'} · 正常收尾信号`;
  if (record.event === 'graceful_stop_requested') detail = '继续确认最后事件并等待后置画面';
  if (record.event === 'graceful_stop_ingest') detail = `${record.pending_count || 0} 个等待任务${record.timed_out ? ' · 已超时' : ' · 已排空'}`;
  if (record.event === 'match_finishing_completed') detail = `${record.lifecycle_state === 'completed' ? '正常完成' : '完成但有警告'} · ${exitReasonLabel(record.exit_reason)}`;
  return {name:names[record.event] || record.event || '运行记录', detail};
}

function render(data) {
  const detail = data.detail || {};
  const status = data.status || 'Uncertain';
  const statusEl = $('match-status'); statusEl.textContent = `${data.status_label || ''} · ${status}`; statusEl.className = `status-pill ${statusClass(status)}`;
  const pollingConfig = data.polling || {}; const gifConfig = data.gif || {};
  syncInputValue('event-poll', pollingConfig.events_seconds); syncInputValue('source-poll', pollingConfig.source_seconds); syncInputValue('detail-poll', pollingConfig.detail_seconds); syncInputValue('before', gifConfig.before_seconds); syncInputValue('after', gifConfig.after_seconds); syncInputValue('event-offset', gifConfig.event_to_video_offset_seconds); syncInputValue('width', gifConfig.width);
  const visionConfig = data.vision || {}; const configuredVisionEnabled = visionConfig.enabled === true; const workerVisionEnabled = visionConfig.worker_enabled === true; if (document.activeElement !== $('vision-enabled')) $('vision-enabled').checked = configuredVisionEnabled; if (document.activeElement !== $('vision-before') && visionConfig.before_seconds != null) $('vision-before').value = visionConfig.before_seconds; if (document.activeElement !== $('vision-after') && visionConfig.after_seconds != null) $('vision-after').value = visionConfig.after_seconds;
  const workerConfigKnown = Boolean(data.worker && Array.isArray(data.worker.command) && data.worker.command.length); $('vision-state').textContent = workerConfigKnown ? `当前 Worker：${workerVisionEnabled ? '开启' : '关闭'}` : configuredVisionEnabled ? '下次启动：开启' : '默认关闭';
  $('team-a').textContent = detail.team_A_name || '主队待加载'; $('team-b').textContent = detail.team_B_name || '客队待加载';
  $('score').textContent = detail.fs_A != null && detail.fs_A !== '' ? `${detail.fs_A} - ${detail.fs_B || 0}` : '-';
  $('match-minute').textContent = detail.minute ? `${detail.minute}' ${detail.minute_period || ''}` : '--';
  $('competition').textContent = detail.competition_name || detail.match_title || '比赛详情待加载';
  const startPlay = formatStartPlayBeijing(detail.start_play, true);
  $('start-play').textContent = startPlay ? `北京时间 ${startPlay}` : '开赛时间 --';
  for (const [id, src, letter] of [['team-a-logo', detail.team_A_logo, 'A'], ['team-b-logo', detail.team_B_logo, 'B']]) { const el = $(id); el.innerHTML = src ? `<img src="${escapeHtml(src)}" alt="">` : letter; }
  const source = data.source_health || {}; $('resource').textContent = source.resource || (source.error || '尚未获取 resource'); $('updated-at').textContent = source.updated_at || '--'; $('source-change').classList.toggle('hidden', !source.changed);
  const setHealth = (id, text, cls) => { const el = $(id); el.textContent = text; el.className = cls || ''; };
  const worker = data.worker || {}; const telemetry = data.telemetry || {}; const counts = telemetry.task_counts || {};
  const lifecycle = data.lifecycle || {}; const lifecycleState = lifecycle.state || '';
  syncSessionSelection(data, worker, lifecycle);
  const runtimeState = telemetry.state || (worker.running ? 'starting' : 'idle');
  const workerStatus = $('worker-status'); workerStatus.className = `runtime-badge ${runtimeState}`; workerStatus.innerHTML = `<i></i>${escapeHtml(telemetry.label || '未启动')}`;
  setHealth('source-health', source.resource ? (worker.mode === 'demo' ? '本地素材就绪' : '地址已获取') : (source.error ? '查询失败' : '未配置'), source.resource ? 'ok' : 'warn');
  const segmentCount = telemetry.buffer_segment_count || 0; const segmentAge = telemetry.latest_segment_age_seconds;
  const bufferSuffix = lifecycleState === 'finishing' ? ' · 比赛已结束，正在收尾' : lifecycleState === 'completed' ? ' · 处理完成' : lifecycleState === 'completed_with_warnings' ? ' · 完成但有警告' : runtimeState === 'completed' ? ' · 验收完成' : runtimeState === 'disconnected' ? ' · 流已结束' : segmentAge != null ? ` · ${Math.round(segmentAge)}秒前` : '';
  setHealth('buffer-health', segmentCount ? `${segmentCount} 个分片${bufferSuffix}` : '等待首个分片', segmentCount ? 'ok' : 'off');
  const lifecycleTerminal = lifecycleState === 'completed' || lifecycleState === 'completed_with_warnings' || lifecycleState === 'stopped';
  const eventApi = data.event_api || {}; const eventHasCurrentError = Boolean(telemetry.last_event_error || eventApi.error); const eventSummary = lifecycleTerminal ? `已停止 · 共 ${telemetry.event_poll_count || 0} 次` : eventHasCurrentError ? `${telemetry.event_poll_count || 0} 次 · 当前异常` : lifecycleState === 'finishing' ? `终场确认中 · ${telemetry.event_poll_count || 0} 次` : telemetry.event_poll_count ? `${telemetry.event_poll_count} 次 · 正常${telemetry.event_error_count ? `（重试 ${telemetry.event_error_count}）` : ''}` : worker.running ? '启动轮询中' : '等待启动'; setHealth('event-health', eventSummary, eventHasCurrentError && !lifecycleTerminal ? 'warn' : telemetry.event_poll_count || worker.running || lifecycleTerminal ? 'ok' : 'off');
  const readyCount = counts.encoded || 0; const activeCount = (counts.pending || 0) + (counts.encoding || 0); setHealth('gif-health', activeCount ? `${activeCount} 个处理中` : `${readyCount} 个产物`, activeCount ? 'active' : readyCount ? 'ok' : 'off');
  const cleanupActive = Boolean(worker.cleanup_process_group); const cleanupLabel = worker.cleanup_failure ? '清理失败' : worker.cleanup_stage === 'kill' ? '强制清理中' : '清理中';
  const workerRestartDue = finiteNumber(worker.restart_due_at_unix); const workerRestartRemaining = workerRestartDue == null ? null : Math.max(0, Math.ceil(workerRestartDue - Date.now() / 1000));
  const workerText = cleanupActive ? `已停止 · ${cleanupLabel}` : worker.running ? `存活 · PID ${worker.pid || '--'}` : worker.desired_running && workerRestartDue != null ? `已停止 · ${workerRestartRemaining}秒后恢复` : worker.return_code != null ? `已停止 · 返回码 ${worker.return_code}` : '未启动';
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
  const lifecycleText = `${data.status_label || status || '赛况未知'} · ${lifecycleLabels[lifecycleState] || lifecycleState || '状态未上报'}`;
  setHealth('lifecycle-health', lifecycleText, lifecycleState === 'completed_with_warnings' || lifecycleState === 'failed' ? 'error' : lifecycleState === 'finishing' || lifecycleState === 'stopping' ? 'warn' : lifecycleState === 'playing' || lifecycleState === 'starting' || lifecycleState === 'completed' ? 'ok' : 'off');
  setHealth('runtime-elapsed', telemetry.elapsed_seconds != null ? fmtDuration(telemetry.elapsed_seconds) : '--', telemetry.elapsed_seconds != null ? 'ok' : 'off');
  const heartbeatAge = finiteNumber(telemetry.heartbeat_age_seconds); const heartbeatFresh = telemetry.heartbeat_fresh === true || heartbeatAge != null && heartbeatAge <= 9;
  const heartbeatText = telemetry.heartbeat_unix ? worker.running ? `${fmtTime(telemetry.heartbeat_unix)} · ${Math.round(heartbeatAge || 0)}秒前` : `最后 ${fmtTime(telemetry.heartbeat_unix)}` : '尚无心跳';
  setHealth('heartbeat-at', heartbeatText, telemetry.heartbeat_unix ? worker.running ? heartbeatFresh ? 'ok' : 'warn' : 'off' : 'off');
  setHealth('polling-health', lifecycleTerminal ? '已停止' : lifecycleState === 'finishing' ? '仅终场事件确认' : worker.running ? '运行中' : '未启动', lifecycleTerminal ? 'ok' : lifecycleState === 'finishing' || worker.running ? 'active' : 'off');
  setStep('source-step', source.resource ? 'ok' : source.error ? 'warn' : 'off'); setStep('buffer-step', segmentCount ? worker.running ? 'active' : 'ok' : 'off'); setStep('event-step', eventHasCurrentError ? 'warn' : telemetry.event_poll_count ? worker.running ? 'active' : 'ok' : 'off'); setStep('gif-step', counts.failed ? 'warn' : activeCount ? 'active' : readyCount ? 'ok' : 'off');
  const latestIngestError = ingestErrorText(telemetry.latest_ingest_error) || ingestErrorText(telemetry.last_ingest_error); const ingestErrorMessage = $('ingest-error-message'); $('ingest-error').textContent = latestIngestError || '--'; ingestErrorMessage.classList.toggle('hidden', !latestIngestError);
  const runtimeMessage = $('runtime-message'); const finishDeadline = lifecycle.finishing_deadline_unix ? `，最迟 ${fmtTime(lifecycle.finishing_deadline_unix)}` : ''; const reasonLabel = exitReasonLabel(lifecycle.exit_reason); const message = worker.cleanup_failure || (lifecycleState === 'finishing' ? `比赛已结束，Worker 正在确认最后事件并等待后置画面${finishDeadline}` : lifecycleState === 'completed' ? `比赛已结束，Worker、FFmpeg 和外部轮询均已停止${reasonLabel ? `（${reasonLabel}）` : ''}` : lifecycleState === 'completed_with_warnings' ? `处理已停止，但需要检查：${reasonLabel || '存在未完成任务'}` : telemetry.exit_message || telemetry.last_event_error || ''); runtimeMessage.textContent = message ? `最近状态：${message}` : ''; runtimeMessage.classList.toggle('hidden', !message || runtimeState === 'healthy');
  const eventCounts = data.event_counts || {}; $('event-count').textContent = `唯一事件 ${eventCounts.unique || 0} · 已生成 ${eventCounts.encoded || 0} · 处理中 ${eventCounts.processing || 0} · 历史未生成 ${eventCounts.history || 0}`;
  const list = $('events'); const events = data.events || [];
  list.innerHTML = events.length ? events.map(e => {
    const type = eventPresentation(e); const task = taskPresentation(e.status);
    const artifacts = e.vision_artifacts && typeof e.vision_artifacts === 'object' ? e.vision_artifacts : {};
    const ocrWindow = e.ocr_window || artifacts.ocr_window || null;
    const tdeed = e.vision || artifacts.tdeed_refined || null;
    const visionEnabled = worker.running ? workerVisionEnabled : configuredVisionEnabled;
    const ocr = e.status === 'history' && !ocrWindow ? {label:'历史事件 · 未运行',cls:'off'} : visionPresentation(ocrWindow, visionEnabled);
    const vision = e.status === 'history' && !tdeed ? {label:'历史事件 · 未运行',cls:'off'} : visionPresentation(tdeed, visionEnabled);
    const confidence = tdeed && tdeed.confidence != null ? ` · 置信度 ${(Number(tdeed.confidence) * 100).toFixed(1)}%` : '';
    const delta = tdeed && tdeed.anchor_delta_seconds != null ? ` · 偏移 ${Number(tdeed.anchor_delta_seconds).toFixed(1)}秒` : '';
    const defaultDegraded = e.coverage_status === 'ready_degraded' ? ' · 短片降级' : '';
    const ocrDegraded = ocrWindow && ocrWindow.coverage_status === 'ready_degraded' ? ' · 短片降级' : '';
    const visionDegraded = tdeed && tdeed.coverage_status === 'ready_degraded' ? ' · 短片降级' : '';
    const ocrFailureDetail = visionFailureDetail(ocrWindow);
    const failureDetail = visionFailureDetail(tdeed);
    const ocrDiagnostics = visionOcrDiagnosticsText(ocrWindow);
    const metadata = e.metadata && typeof e.metadata === 'object' ? e.metadata : {};
    const targetClock = (ocrWindow && ocrWindow.target_clock) || matchClockText(e.second);
    const routeStatus = goalRouteStatusLabel(metadata.goal_route_status || metadata.route_status);
    const shotmapStatus = ({
      direct:'shotmap 直接触发', matched:'shotmap 已匹配', missing:'shotmap 暂无秒',
      ambiguous:'shotmap 匹配歧义', invalid:'shotmap 秒无效', stale:'shotmap 等待重新匹配'
    })[metadata.shotmap_match_status] || '';
    const secondDetail = (e.code === 'G' || e.code === 'OG' || e.code === 'PG')
      ? [targetClock ? `目标时钟 ${targetClock}` : '', routeStatus, shotmapStatus].filter(Boolean).join(' · ')
      : '';
    const ocrSource = ocrWindow && ocrWindow.localization_source === 'exact_second' ? '秒级定位' : ocrWindow && ocrWindow.localization_source === 'minute_boundary' ? '分钟定位' : '';
    const ocrDetail = [secondDetail, ocrSource, ocrFailureDetail, ocrDiagnostics ? `OCR：${ocrDiagnostics}` : ''].filter(Boolean).join(' · ');
    const visionDetail = [failureDetail, tdeed && tdeed.source_ocr_artifact ? '基于 OCR 60秒窗口' : ''].filter(Boolean).join(' · ');
    const gifLink = artifact => artifact && artifact.output ? `<a class="gif-link" href="/api/gif/${encodeURIComponent(data.match_id)}/${encodeURIComponent(artifact.output.split('/').pop())}" target="_blank">预览</a>` : '';
    return `<div class="event-row ${escapeHtml(task.cls)}"><div class="event-type event-type-${escapeHtml(type.kind)}"><span class="event-symbol" aria-hidden="true"></span><span class="event-type-text"><b>${escapeHtml(type.label)}</b><small>${escapeHtml(type.code)}</small></span></div><div class="event-minute">${escapeHtml(e.minute || '--')}'${e.minute_extra && e.minute_extra !== '0' ? `+${escapeHtml(e.minute_extra)}` : ''}</div><div class="event-person">${escapeHtml(e.person || '未提供球员')}<small>${escapeHtml(e.team || '')}${e.score ? ` · ${escapeHtml(e.score)}` : ''}${e.reason ? ` · ${escapeHtml(e.reason)}` : ''}</small></div><div class="artifact-list"><div class="artifact"><span>默认 · ${escapeHtml(task.label)}${defaultDegraded}</span>${e.output ? `<a class="gif-link" href="/api/gif/${encodeURIComponent(data.match_id)}/${encodeURIComponent(e.output.split('/').pop())}" target="_blank">预览</a>` : ''}</div><div class="artifact ${escapeHtml(ocr.cls)}"><span>OCR 60秒 · ${escapeHtml(ocr.label)}${ocrDegraded}${ocrDetail ? `<small>${escapeHtml(ocrDetail)}</small>` : ''}</span>${gifLink(ocrWindow)}</div><div class="artifact ${escapeHtml(vision.cls)}"><span>T-DEED 20秒 · ${escapeHtml(vision.label)}${escapeHtml(confidence)}${escapeHtml(delta)}${visionDegraded}${tdeed && tdeed.experimental ? ' · 实验' : ''}${visionDetail ? `<small>${escapeHtml(visionDetail)}</small>` : ''}</span>${gifLink(tdeed)}</div></div></div>`;
  }).join('') : '<div class="empty">暂无已发现事件。启动处理后，进球、黄牌、红牌和乌龙球会在这里显示。</div>';
  const logs = $('logs'); const records = data.logs || []; let heartbeatSeen = false; const visibleRecords = records.filter(record => record.event !== 'runtime_heartbeat' || (!heartbeatSeen && (heartbeatSeen = true))); logs.innerHTML = visibleRecords.length ? visibleRecords.slice(0, 40).map(l => { const presentation = logPresentation(l); return `<div class="log-line log-${escapeHtml(l.event || '')}"><time>${escapeHtml((l.timestamp || '').replace('T',' ').replace('Z','').slice(0,19))}</time><b>${escapeHtml(presentation.name)}</b><span>${escapeHtml(presentation.detail)}</span></div>`; }).join('') : '<div class="empty">暂无日志</div>';
  $('last-refresh').textContent = `更新于 ${new Date().toLocaleTimeString('zh-CN', {hour12:false})}`;
  if (data.event_api && data.event_api.error) showNotice(`事件接口：${data.event_api.error}`); else if (source.error && source.error.includes('GIF_SOURCE_SECRET')) showNotice('当前尚未设置直播源接口 secret。你提供真实 match_id 前，可先运行演示链路。'); else clearNotice();
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
    if (viewSequence === state.viewSequence && id === state.sessionMatchId && requestSerial > state.lastRenderedRefreshSerial) showNotice(error.message, 'error');
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
    $('discovery-detail').textContent = `赛事目录：${error.message} · 仍可手工输入比赛 ID`;
  } finally {
    state.matchesLoading = false;
  }
}
async function configure(id = matchId()) { const body = {match_id:id, event_poll_seconds:+$('event-poll').value, source_poll_seconds:+$('source-poll').value, detail_poll_seconds:+$('detail-poll').value, before_seconds:+$('before').value, after_seconds:+$('after').value, event_to_video_offset_seconds:+$('event-offset').value, gif_width:+$('width').value, vision_enabled:$('vision-enabled').checked, vision_before_seconds:+$('vision-before').value, vision_after_seconds:+$('vision-after').value}; return requestJson('/api/session', {method:'POST', body:JSON.stringify(body)}); }
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
    showNotice(error.message, 'error');
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
    showNotice(error.message, 'error');
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
refresh();
refreshMatches();
state.timer = setInterval(refresh, 1000);
state.matchesTimer = setInterval(refreshMatches, MATCH_DIRECTORY_INTERVAL_MS);
