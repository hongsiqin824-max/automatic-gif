const $ = (id) => document.getElementById(id);
const state = { timer: null };

function matchId() { return $('match-id').value.trim(); }
function showNotice(message, kind = 'warning') { const el = $('notice'); el.textContent = message; el.className = `notice ${kind === 'error' ? 'error' : ''}`; }
function clearNotice() { $('notice').className = 'notice hidden'; }
function statusClass(value) { return String(value || 'uncertain').toLowerCase(); }
function fmtTime(unix) { return unix ? new Date(unix * 1000).toLocaleTimeString('zh-CN', {hour12:false}) : '--'; }
function fmtDuration(seconds) { const value = Math.max(0, Math.floor(Number(seconds) || 0)); const minutes = Math.floor(value / 60); const rest = value % 60; return minutes ? `${minutes}分 ${rest}秒` : `${rest}秒`; }
function escapeHtml(value) { return String(value ?? '').replace(/[&<>'"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c])); }
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

function setStep(id, status) { $(id).className = `pipeline-step ${status || ''}`; }
function logPresentation(record) {
  const names = {
    worker_started:'处理进程已启动', worker_exited:'处理进程已退出', runtime_heartbeat:'运行心跳',
    worker_restart_scheduled:'已安排自动恢复', worker_restart_failed:'自动恢复失败', live_source_restart_failed:'切源恢复失败',
    process_group_stop_requested:'正在清理旧进程', process_group_term_timeout:'旧进程未按时退出', process_group_killed:'已强制清理旧进程', process_group_cleanup_complete:'旧进程清理完成', process_group_cleanup_failed:'旧进程清理失败',
    event_discovered:'捕获新增事件', event_duplicate:'重复事件已忽略', event_accepted:'事件已入队',
    task_transition:'任务状态变化', gif_ready:'GIF 已生成', api_error:'事件接口异常',
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
  if (record.event === 'event_discovered') detail = `${record.code || ''}${record.minute ? ` · ${record.minute}'` : ''}${record.person ? ` · ${record.person}` : ''}`;
  if (record.event === 'worker_started') detail = `${record.mode === 'demo' ? '演示验收' : '实时处理'} · PID ${record.pid || '--'}`;
  if (record.event === 'worker_exited') detail = `返回码 ${record.return_code ?? '--'}`;
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
  $('team-a').textContent = detail.team_A_name || '主队待加载'; $('team-b').textContent = detail.team_B_name || '客队待加载';
  $('score').textContent = detail.fs_A != null && detail.fs_A !== '' ? `${detail.fs_A} - ${detail.fs_B || 0}` : '-';
  $('match-minute').textContent = detail.minute ? `${detail.minute}' ${detail.minute_period || ''}` : '--';
  $('competition').textContent = detail.competition_name || detail.match_title || '比赛详情待加载';
  $('start-play').textContent = detail.start_play ? `北京时间 ${detail.start_play}` : '开赛时间 --';
  for (const [id, src, letter] of [['team-a-logo', detail.team_A_logo, 'A'], ['team-b-logo', detail.team_B_logo, 'B']]) { const el = $(id); el.innerHTML = src ? `<img src="${escapeHtml(src)}" alt="">` : letter; }
  const source = data.source_health || {}; $('resource').textContent = source.resource || (source.error || '尚未获取 resource'); $('updated-at').textContent = source.updated_at || '--'; $('source-change').classList.toggle('hidden', !source.changed);
  const setHealth = (id, text, cls) => { const el = $(id); el.textContent = text; el.className = cls || ''; };
  const worker = data.worker || {}; const telemetry = data.telemetry || {}; const counts = telemetry.task_counts || {};
  const lifecycle = data.lifecycle || {}; const lifecycleState = lifecycle.state || '';
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
  const finishingLabel = lifecycleState === 'finishing' ? (worker.running ? '收尾中' : '等待收尾结果') : lifecycleState === 'completed' ? '已完成' : lifecycleState === 'completed_with_warnings' ? '完成但有警告' : null;
  setHealth('ffmpeg-health', cleanupActive ? `${cleanupLabel} · PGID ${worker.cleanup_process_group}` : finishingLabel || (worker.running ? `PID ${worker.pid}` : worker.desired_running && worker.restart_due_at_unix ? `等待重启 · ${fmtTime(worker.restart_due_at_unix)}` : worker.return_code != null ? `已退出 · ${worker.return_code}` : '未启动'), cleanupActive || runtimeState === 'failed' || lifecycleState === 'completed_with_warnings' ? 'warn' : lifecycleState === 'completed' ? 'ok' : worker.running ? 'ok' : 'off');
  $('runtime-elapsed').textContent = telemetry.elapsed_seconds != null ? fmtDuration(telemetry.elapsed_seconds) : '--';
  $('heartbeat-at').textContent = telemetry.heartbeat_unix ? worker.running ? `${fmtTime(telemetry.heartbeat_unix)} · ${Math.round(telemetry.heartbeat_age_seconds || 0)}秒前` : `最后 ${fmtTime(telemetry.heartbeat_unix)}` : '尚无心跳';
  setHealth('polling-health', lifecycleTerminal ? '已停止' : lifecycleState === 'finishing' ? '仅终场事件确认' : worker.running ? '运行中' : '未启动', lifecycleTerminal ? 'ok' : lifecycleState === 'finishing' || worker.running ? 'active' : 'off');
  setStep('source-step', source.resource ? 'ok' : source.error ? 'warn' : 'off'); setStep('buffer-step', segmentCount ? worker.running ? 'active' : 'ok' : 'off'); setStep('event-step', eventHasCurrentError ? 'warn' : telemetry.event_poll_count ? worker.running ? 'active' : 'ok' : 'off'); setStep('gif-step', counts.failed ? 'warn' : activeCount ? 'active' : readyCount ? 'ok' : 'off');
  const runtimeMessage = $('runtime-message'); const finishDeadline = lifecycle.finishing_deadline_unix ? `，最迟 ${fmtTime(lifecycle.finishing_deadline_unix)}` : ''; const reasonLabel = exitReasonLabel(lifecycle.exit_reason); const message = worker.cleanup_failure || (lifecycleState === 'finishing' ? `比赛已结束，Worker 正在确认最后事件并等待后置画面${finishDeadline}` : lifecycleState === 'completed' ? `比赛已结束，Worker、FFmpeg 和外部轮询均已停止${reasonLabel ? `（${reasonLabel}）` : ''}` : lifecycleState === 'completed_with_warnings' ? `处理已停止，但需要检查：${reasonLabel || '存在未完成任务'}` : telemetry.exit_message || telemetry.last_event_error || ''); runtimeMessage.textContent = message ? `最近状态：${message}` : ''; runtimeMessage.classList.toggle('hidden', !message || runtimeState === 'healthy');
  const eventCounts = data.event_counts || {}; $('event-count').textContent = `唯一事件 ${eventCounts.unique || 0} · 已生成 ${eventCounts.encoded || 0} · 处理中 ${eventCounts.processing || 0} · 历史未生成 ${eventCounts.history || 0}`;
  const list = $('events'); const events = data.events || [];
  list.innerHTML = events.length ? events.map(e => {
    const type = eventPresentation(e); const task = taskPresentation(e.status);
    return `<div class="event-row ${escapeHtml(task.cls)}"><div class="event-type event-type-${escapeHtml(type.kind)}"><span class="event-symbol" aria-hidden="true"></span><span class="event-type-text"><b>${escapeHtml(type.label)}</b><small>${escapeHtml(type.code)}</small></span></div><div class="event-minute">${escapeHtml(e.minute || '--')}'${e.minute_extra && e.minute_extra !== '0' ? `+${escapeHtml(e.minute_extra)}` : ''}</div><div class="event-person">${escapeHtml(e.person || '未提供球员')}<small>${escapeHtml(e.team || '')}${e.score ? ` · ${escapeHtml(e.score)}` : ''}${e.reason ? ` · ${escapeHtml(e.reason)}` : ''}</small></div><div class="event-status ${escapeHtml(task.cls)}"><span>${escapeHtml(task.label)}</span>${e.output ? `<a class="gif-link" href="/api/gif/${encodeURIComponent(data.match_id)}/${encodeURIComponent(e.output.split('/').pop())}" target="_blank">预览 GIF</a>` : ''}</div></div>`;
  }).join('') : '<div class="empty">暂无已发现事件。启动处理后，进球、黄牌、红牌和乌龙球会在这里显示。</div>';
  const logs = $('logs'); const records = data.logs || []; let heartbeatSeen = false; const visibleRecords = records.filter(record => record.event !== 'runtime_heartbeat' || (!heartbeatSeen && (heartbeatSeen = true))); logs.innerHTML = visibleRecords.length ? visibleRecords.slice(0, 40).map(l => { const presentation = logPresentation(l); return `<div class="log-line log-${escapeHtml(l.event || '')}"><time>${escapeHtml((l.timestamp || '').replace('T',' ').replace('Z','').slice(0,19))}</time><b>${escapeHtml(presentation.name)}</b><span>${escapeHtml(presentation.detail)}</span></div>`; }).join('') : '<div class="empty">暂无日志</div>';
  $('last-refresh').textContent = `更新于 ${new Date().toLocaleTimeString('zh-CN', {hour12:false})}`;
  if (data.event_api && data.event_api.error) showNotice(`事件接口：${data.event_api.error}`); else if (source.error && source.error.includes('GIF_SOURCE_SECRET')) showNotice('当前尚未设置直播源接口 secret。你提供真实 match_id 前，可先运行演示链路。'); else clearNotice();
}

async function requestJson(url, options = {}) { const response = await fetch(url, {headers:{'Content-Type':'application/json'}, ...options}); const data = await response.json(); if (!response.ok) throw new Error(data.error || `请求失败 ${response.status}`); return data; }
async function refresh() { try { render(await requestJson(`/api/session?match_id=${encodeURIComponent(matchId())}`)); } catch (error) { showNotice(error.message, 'error'); } }
async function configure() { const body = {match_id:matchId(), event_poll_seconds:+$('event-poll').value, source_poll_seconds:+$('source-poll').value, detail_poll_seconds:+$('detail-poll').value, before_seconds:+$('before').value, after_seconds:+$('after').value, gif_width:+$('width').value}; return requestJson('/api/session', {method:'POST', body:JSON.stringify(body)}); }
async function action(path, extra = {}) { try { const configured = await configure(); render(configured); const data = await requestJson(path, {method:'POST', body:JSON.stringify({match_id:matchId(), ...extra})}); render(data); clearNotice(); } catch (error) { showNotice(error.message, 'error'); } }
const urlMatchId = new URLSearchParams(window.location.search).get('match_id');
if (urlMatchId) $('match-id').value = urlMatchId;
$('load-btn').addEventListener('click', refresh); $('demo-btn').addEventListener('click', () => { $('match-id').value = `demo-proof-${Date.now()}`; action('/api/session/start', {demo:true}); }); $('start-btn').addEventListener('click', () => action('/api/session/start')); $('stop-btn').addEventListener('click', () => action('/api/session/stop'));
refresh(); state.timer = setInterval(refresh, 1000);
