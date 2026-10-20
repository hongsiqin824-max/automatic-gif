'use strict';
const $ = (id) => document.getElementById(id);
const REFRESH_MS = 60000;
const WORKSHOP_IDLE_MS = 90000;
// After a capture succeeds, hold the tile's status line this long before
// routine player chatter (FRAG_BUFFERED etc.) may overwrite it.
const TILE_STATUS_STICKY_MS = 5000;

const state = {
  tiles: new Map(),      // match_id -> {data, hls, playlistReady, statusTimer}
  stageMatchId: null,
  stageHls: null,
  stageVideoNode: null,      // the tile <video> DOM node currently relocated into the stage
  stageVideoHomeWrap: null,  // the .tile-video-wrap to move that node back into on close
  maxPreviews: 16,
  clipBeforeSeconds: 10,
  clipRangeMaxSeconds: 30,
  // Operator-chosen [start, end] range on the stage. ``pinId`` is the server
  // pin that hard-links the start so it survives the rolling buffer.
  range: {matchId: null, pinId: null, startMs: null, ttlTimer: null, busy: false},
  draftPoll: null,       // {matchId, timer, polls} while awaiting draft links
  alertSeq: 0,           // cursor: last event seq the page has rendered
  alertBootId: null,     // server boot id; a change means 8901 restarted
  alertInit: false,      // skip replaying history on the first poll
  alertCount: 0,         // unseen-alert badge count
  alertPanelOpen: false, // panel is collapsed to a launcher pill by default
  // ---- Two-view workflow ----
  view: 'list',                     // 'list' | 'workshop'
  allMatches: [],                   // full playing list + optional source_state
  selectedMatchIds: new Set(),      // checked match_id set
  sourceCache: {},                  // match_id → source_state (survives refreshes)
  workshopIdleTimer: null,          // setTimeout id for 90s idle cleanup
};

function escapeHtml(value) {
  return String(value == null ? '' : value).replace(/[&<>"']/g, (ch) => (
    {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]
  ));
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, {headers: {'Content-Type': 'application/json'}, ...options});
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || `请求失败 ${response.status}`);
  return data;
}

function showNotice(message, kind = 'warning') {
  const el = $('notice');
  if (!message) { el.classList.add('hidden'); return; }
  el.textContent = message;
  el.className = `notice ${kind === 'error' ? 'error' : ''}`;
}

// Read the PROGRAM-DATE-TIME of the frame currently on screen so the capture
// anchors on what the human saw, independent of stream delay.
function frameAnchorMs(video) {
  if (!video) return null;
  const hls = video._hls;
  if (hls && hls.playingDate instanceof Date && !isNaN(hls.playingDate.getTime())) {
    return hls.playingDate.getTime();
  }
  // Safari native HLS has no hls.js: derive PDT from the stream start date.
  if (typeof video.getStartDate === 'function') {
    const start = video.getStartDate();
    if (start instanceof Date && !isNaN(start.getTime()) && isFinite(video.currentTime)) {
      return start.getTime() + video.currentTime * 1000;
    }
  }
  return null;
}

function videoForMatch(matchId) {
  if (state.stageMatchId === matchId && state.stageVideoNode) {
    return state.stageVideoNode;
  }
  return document.querySelector(`.tile[data-match-id="${matchId}"] .tile-video`);
}

// Jump the playhead to the live edge so a capture reflects the newest frame
// instead of a position the operator has drifted behind.
//
// hls.liveSyncPosition is derived from the *last playlist hls.js fetched* and
// is clamped to "last known segment − target duration": it can never point
// past a segment hls.js has not seen yet. After a background pause Chrome
// throttles the playlist refresh to ~1/min, so seeking immediately lands up to
// a minute behind real live. We therefore wait for one fresh LEVEL_UPDATED
// (bounded by LIVE_SEEK_WAIT_MS) before seeking, unless the playlist is
// already fresh. The landing spot is liveSyncPosition (edge − ~6s), not the
// raw edge, so the player keeps a few seconds of buffer instead of starving.
const LIVE_SEEK_FRESH_SECONDS = 4;
const LIVE_SEEK_WAIT_MS = 2000;

function currentLevelDetails(hls) {
  const level = hls && Array.isArray(hls.levels) ? hls.levels[hls.currentLevel] : null;
  return level && level.details ? level.details : null;
}

function jumpToLiveSync(video) {
  const hls = video._hls;
  if (hls && typeof hls.liveSyncPosition === 'number' && isFinite(hls.liveSyncPosition)) {
    video.currentTime = hls.liveSyncPosition;
  } else if (video.seekable && video.seekable.length) {
    video.currentTime = video.seekable.end(video.seekable.length - 1);
  }
  if (video.paused) video.play().catch(() => {});
}

function seekToLive(video, onDone) {
  if (!video) return;
  const hls = video._hls;
  const details = currentLevelDetails(hls);
  const fresh = details && typeof details.age === 'number' && details.age < LIVE_SEEK_FRESH_SECONDS;
  if (!hls || fresh) {
    jumpToLiveSync(video);
    if (onDone) onDone();
    return;
  }
  // Stale playlist: coalesce rapid clicks, then seek once a refresh lands.
  if (video._liveSeekPending) return;
  video._liveSeekPending = true;
  let timer = null;
  const finish = () => {
    if (!video._liveSeekPending) return;
    video._liveSeekPending = false;
    clearTimeout(timer);
    try { hls.off(window.Hls.Events.LEVEL_UPDATED, finish); } catch (e) {}
    jumpToLiveSync(video);
    if (onDone) onDone();
  };
  timer = setTimeout(finish, LIVE_SEEK_WAIT_MS);
  hls.on(window.Hls.Events.LEVEL_UPDATED, finish);
  // A background-paused element has idle loaders; resuming nudges hls.js to
  // refresh the playlist promptly instead of waiting for its throttled timer.
  if (video.paused) video.play().catch(() => {});
}

function attachHls(video, playlistUrl, onStatus) {
  if (video._hls) { try { video._hls.destroy(); } catch (e) {} video._hls = null; }
  if (window.Hls && window.Hls.isSupported()) {
    const hls = new window.Hls({
      liveSyncDurationCount: 3,
      lowLatencyMode: false,
      manifestLoadingMaxRetry: 8,
      manifestLoadingRetryDelay: 1000,
      levelLoadingMaxRetry: 8,
      fragLoadingMaxRetry: 8,
    });
    video._hls = hls;
    let netRetries = 0;
    hls.on(window.Hls.Events.MANIFEST_PARSED, () => {
      video.play().catch(() => {});
      if (onStatus) onStatus('正在播放直播源', 'ok');
    });
    hls.on(window.Hls.Events.FRAG_BUFFERED, () => {
      netRetries = 0;
      if (onStatus) onStatus('正在播放直播源', 'ok');
    });
    hls.on(window.Hls.Events.ERROR, (_e, data) => {
      if (!data || !data.fatal) return;
      if (data.type === window.Hls.ErrorTypes.NETWORK_ERROR) {
        netRetries += 1;
        if (netRetries > 4) {
          try { hls.destroy(); } catch (e) {}
          video._hls = null;
          if (onStatus) onStatus('直播源不可用（源未推流）', 'fail');
          return;
        }
        if (onStatus) onStatus('网络波动，正在重连…', 'warn');
        try { hls.startLoad(); } catch (e) {}
      } else if (data.type === window.Hls.ErrorTypes.MEDIA_ERROR) {
        if (onStatus) onStatus('画面波动，正在恢复…', 'warn');
        try { hls.recoverMediaError(); } catch (e) {}
      } else {
        if (onStatus) onStatus('播放中断，正在重建…', 'warn');
        try { hls.destroy(); } catch (e) {}
        video._hls = null;
        setTimeout(() => attachHls(video, playlistUrl, onStatus), 2000);
      }
    });
    hls.loadSource(playlistUrl);
    hls.attachMedia(video);
    return hls;
  }
  if (video.canPlayType('application/vnd.apple.mpegurl')) {
    video.src = playlistUrl;
    video.addEventListener('loadedmetadata', () => { video.play().catch(() => {}); }, {once: true});
    if (onStatus) onStatus('正在播放直播源', 'ok');
  } else if (onStatus) {
    onStatus('浏览器不支持 HLS 播放', 'warn');
  }
  return null;
}

function teamsText(data) {
  const a = data.team_A_name || '主队';
  const b = data.team_B_name || '客队';
  const score = (data.fs_A != null && data.fs_A !== '') ? `${data.fs_A} - ${data.fs_B || 0}` : 'vs';
  return `<span>${escapeHtml(a)}</span><span class="score">${escapeHtml(score)}</span><span>${escapeHtml(b)}</span>`;
}

function metaText(data) {
  const minute = data.minute ? `${escapeHtml(data.minute)}' ${escapeHtml(data.minute_period || '')}` : '--';
  return `<span>${escapeHtml(data.competition_name || '未知赛事')}</span><span>${minute}</span>`;
}

// ``sticky`` pins the line for TILE_STATUS_STICKY_MS; while pinned, routine
// status updates are dropped so a fresh "已生成 GIF" stays readable. Error
// states (warn/fail) always win so a dying source is never hidden.
function setTileStatus(matchId, text, cls = '', sticky = false) {
  const node = document.querySelector(`.tile[data-match-id="${matchId}"] .tile-status`);
  if (!node) return;
  const until = Number(node.dataset.stickyUntil || 0);
  if (!sticky && until > Date.now() && cls !== 'warn' && cls !== 'fail') return;
  node.dataset.stickyUntil = sticky ? String(Date.now() + TILE_STATUS_STICKY_MS) : '0';
  node.textContent = text;
  node.className = `tile-status ${cls}`;
}

function setTileClipCount(matchId, count, flash = false) {
  const tile = state.tiles.get(matchId);
  if (tile) tile.clipCount = count;
  const badge = document.querySelector(`.tile[data-match-id="${matchId}"] [data-role="clips"]`);
  if (!badge) return;
  badge.textContent = `GIF ×${count}`;
  badge.classList.toggle('hidden', count <= 0);
  if (flash) {
    badge.classList.add('flash');
    setTimeout(() => badge.classList.remove('flash'), 1500);
  }
}

async function refreshTileClipCount(matchId) {
  try {
    const data = await requestJson(`api/monitor/clips?match_id=${encodeURIComponent(matchId)}`);
    setTileClipCount(matchId, Array.isArray(data.clips) ? data.clips.length : 0);
  } catch (e) { /* badge stays as-is */ }
}

function createTile(data) {
  const el = document.createElement('article');
  el.className = 'tile';
  el.dataset.matchId = data.match_id;
  el.innerHTML = `
    <div class="tile-video-wrap" data-role="enlarge">
      <video class="tile-video" playsinline autoplay muted></video>
      <div class="tile-overlay">正在连接直播源…</div>
    </div>
    <div class="tile-body">
      <div class="tile-teams">${teamsText(data)}</div>
      <div class="tile-meta">${metaText(data)}</div>
      <div class="tile-actions">
        <button class="button primary" data-role="capture" type="button">截取 ${state.clipBeforeSeconds} 秒</button>
        <span class="lag-badge na" data-role="lag" title="当前画面比服务器最新画面晚多少秒">落后 —</span>
        <button class="button ghost tile-live" data-role="live" type="button" title="跳到直播最新位置">回到直播</button>
        <button class="tile-clips-badge hidden" data-role="clips" type="button" title="查看本场已截取的 GIF">GIF ×0</button>
        <span class="tile-status">准备中…</span>
      </div>
    </div>`;
  el.querySelector('[data-role="enlarge"]').addEventListener('click', () => openStage(data.match_id));
  el.querySelector('[data-role="clips"]').addEventListener('click', (ev) => {
    ev.stopPropagation();
    openStage(data.match_id);
  });
  el.querySelector('[data-role="capture"]').addEventListener('click', (ev) => {
    ev.stopPropagation();
    captureClip(data.match_id, ev.currentTarget);
  });
  el.querySelector('[data-role="live"]').addEventListener('click', (ev) => {
    ev.stopPropagation();
    setTileStatus(data.match_id, '正在回到直播…');
    seekToLive(videoForMatch(data.match_id), () => setTileStatus(data.match_id, '已回到直播', 'ok'));
  });
  return el;
}

function updateTileInfo(el, data) {
  el.querySelector('.tile-teams').innerHTML = teamsText(data);
  el.querySelector('.tile-meta').innerHTML = metaText(data);
}

async function startTilePreview(matchId) {
  const tile = state.tiles.get(matchId);
  if (!tile) return;
  const el = document.querySelector(`.tile[data-match-id="${matchId}"]`);
  if (!el) return;
  const video = el.querySelector('.tile-video');
  const overlay = el.querySelector('.tile-overlay');
  setTileStatus(matchId, '正在拉起直播…');
  try {
    const data = await requestJson('api/monitor/preview/start', {
      method: 'POST', body: JSON.stringify({match_id: matchId}),
    });
    tile.playlistUrl = data.playlist_url;
    overlay.classList.add('hidden');
    el.classList.remove('no-source');
    attachHls(video, data.playlist_url, (text, cls) => {
      setTileStatus(matchId, text, cls);
      if (cls === 'fail') {
        overlay.classList.remove('hidden');
        overlay.textContent = '直播源不可用（源未推流）';
        tile.down = true;
      }
    });
    setTileStatus(matchId, data.playlist_ready ? '直播加载中…' : '直播源连接中…', 'ok');
  } catch (error) {
    overlay.classList.remove('hidden');
    const noSource = /直播源/.test(error.message);
    overlay.textContent = noSource ? '该比赛暂无直播源' : '直播源加载失败';
    if (noSource) el.classList.add('no-source'); else el.classList.remove('no-source');
    setTileStatus(matchId, error.message, 'warn');
  }
}

// ---- clip / stage / draft ----
// ``range`` = {pinId, startMs, endMs} switches the request into range mode.
// Returns true on success so range callers know whether to reset their state.
async function captureClip(matchId, button, range) {
  const prevLabel = button ? button.textContent : '';
  if (button) { button.disabled = true; button.textContent = '截取中…'; }
  setTileStatus(matchId, range ? '正在截取所选区间…' : '正在回捞最近画面…');
  try {
    const payload = {match_id: matchId};
    if (range) {
      payload.pin_id = range.pinId;
      payload.end_epoch_ms = Math.round(range.endMs);
    } else {
      const anchorMs = frameAnchorMs(videoForMatch(matchId));
      if (anchorMs != null) payload.anchor_epoch_ms = Math.round(anchorMs);
    }
    if (state.stageMatchId === matchId) {
      const personEl = $('stage-person');
      const titleEl = $('stage-title-input');
      const secondsEl = $('stage-seconds');
      if (personEl && personEl.value.trim()) payload.person = personEl.value.trim();
      if (titleEl && titleEl.value.trim()) payload.title = titleEl.value.trim();
      if (!range && secondsEl && secondsEl.value.trim()) {
        const secs = Number(secondsEl.value);
        if (Number.isFinite(secs) && secs > 0 && secs <= 120) payload.before_seconds = secs;
      }
    }
    const data = await requestJson('api/monitor/clip', {
      method: 'POST', body: JSON.stringify(payload),
    });
    const sizeMb = (data.bytes / (1024 * 1024)).toFixed(2);
    const warn = data.anchored === false;
    const publishNote = data.publish_error
      ? ' · 发文失败：' + data.publish_error
      : data.draft
      ? ' · 已建草稿'
      : '';
    const alertNote = alertMatchNote(data.alert_match);
    const prefix = warn ? '⚠ 未锚定，画面可能不准' : '已生成 GIF';
    setTileStatus(matchId, `${prefix}（${sizeMb} MB）${publishNote}${alertNote}`, warn ? 'warn' : 'ok', true);
    if (state.stageMatchId === matchId && alertNote) {
      const am = data.alert_match || {};
      addStageNote(alertNote.replace(/^ · /, ''), am.mode === 'alert' && !am.ambiguous ? 'ok' : 'warn');
    }
    const tile = state.tiles.get(matchId);
    setTileClipCount(matchId, ((tile && tile.clipCount) || 0) + 1, true);
    if (state.stageMatchId === matchId) loadStageClips(matchId);
    return true;
  } catch (error) {
    setTileStatus(matchId, error.message, 'warn');
    if (state.stageMatchId === matchId) addStageNote('截取失败：' + error.message, 'fail');
    return false;
  } finally {
    if (button) { button.disabled = false; button.textContent = prevLabel; }
  }
}

// Human-readable summary of which goal/red card the server tied this clip to,
// so the operator can spot a wrong auto-pick without extra UI.
function alertMatchNote(am) {
  if (!am || !am.mode) return '';
  const clock = [am.match_time ? `${am.match_time}'` : '', am.score || ''].filter(Boolean).join(' ');
  if (am.mode === 'alert') {
    const flag = am.weak
      ? '（⚠ 可信度低，这个球的推送可能还没到，请核对）'
      : am.ambiguous
      ? '（候选多条，请核对）'
      : '';
    return ` · 已关联提醒 ${clock}${flag}`;
  }
  if (am.mode === 'catalog') return ` · 未匹配提醒，用当前比分 ${clock}`;
  return ' · 未匹配提醒，时间/比分留空';
}

function addStageNote(text, cls) {
  const wrap = $('stage-notes');
  if (!wrap) return;
  const note = document.createElement('div');
  note.className = `stage-note ${cls || ''}`;
  note.textContent = `${text} · ${new Date().toLocaleTimeString()}`;
  wrap.prepend(note);
  while (wrap.children.length > 5) wrap.removeChild(wrap.lastChild);
}

// ---- stage clip list (server-backed; every GIF of this match, any window) ----

function fmtClipTime(unix) {
  return new Date(unix * 1000).toLocaleTimeString('zh-CN', {hour12: false});
}

function applyDraftState(draftEl, status, draftUrl, error, retriable) {
  if ((status === 'published' || status === 'drafted') && draftUrl) {
    const label = status === 'published' ? '打开文章 ↗' : '打开草稿 ↗';
    draftEl.innerHTML = `<a href="${escapeHtml(draftUrl)}" target="_blank" rel="noreferrer">${label}</a>`;
    draftEl.className = 'stage-clip-draft ok';
    return false;
  }
  if (status === 'failed' && !retriable) {
    draftEl.textContent = '草稿失败：' + (error || '未知错误');
    draftEl.className = 'stage-clip-draft fail';
    return false;
  }
  if (status === 'failed') {
    draftEl.textContent = '草稿重试中：' + (error || '未知错误');
    draftEl.className = 'stage-clip-draft';
    return true;
  }
  draftEl.textContent = '草稿生成中…';
  draftEl.className = 'stage-clip-draft';
  return true;
}

function renderClipRow(item) {
  const row = document.createElement('div');
  row.className = 'stage-clip-row';
  if (item.event_key) row.dataset.eventKey = item.event_key;
  const sizeMb = (item.bytes / (1024 * 1024)).toFixed(2);
  const link = document.createElement('a');
  link.className = 'stage-clip-open';
  link.href = item.clip_url; link.target = '_blank'; link.rel = 'noreferrer';
  link.textContent = '打开 GIF';
  const label = document.createElement('span');
  label.className = 'stage-clip-label';
  label.textContent = `${fmtClipTime(item.created_unix)} · ${sizeMb} MB`;
  const draftEl = document.createElement('span');
  draftEl.className = 'stage-clip-draft';
  if (item.event_key) {
    applyDraftState(draftEl, item.draft_status, item.draft_url, item.draft_error, item.draft_retriable);
  } else {
    draftEl.textContent = '仅本地保存';
    draftEl.className = 'stage-clip-draft warn';
  }
  row.appendChild(label);
  row.appendChild(link);
  row.appendChild(draftEl);
  return row;
}

async function loadStageClips(matchId) {
  const list = $('stage-clips');
  if (!list) return;
  let data;
  try {
    data = await requestJson(`api/monitor/clips?match_id=${encodeURIComponent(matchId)}`);
  } catch (error) {
    if (state.stageMatchId === matchId) addStageNote('读取 GIF 列表失败：' + error.message, 'fail');
    return;
  }
  if (state.stageMatchId !== matchId) return;
  const clips = Array.isArray(data.clips) ? data.clips : [];
  list.innerHTML = '';
  if (!clips.length) {
    const empty = document.createElement('div');
    empty.className = 'stage-clip-empty';
    empty.textContent = '本场还没有截取过 GIF';
    list.appendChild(empty);
  } else {
    clips.forEach((item) => list.appendChild(renderClipRow(item)));
  }
  setTileClipCount(matchId, clips.length);
  const pending = clips.some((c) => c.event_key && !(
    ((c.draft_status === 'published' || c.draft_status === 'drafted') && c.draft_url)
    || (c.draft_status === 'failed' && !c.draft_retriable)
  ));
  if (pending) startDraftPoll(matchId); else stopDraftPoll();
}

function startDraftPoll(matchId) {
  if (state.draftPoll && state.draftPoll.matchId === matchId) return;
  stopDraftPoll();
  state.draftPoll = {matchId, timer: setInterval(() => refreshDraftLinks(matchId), 2000), polls: 0};
}

function stopDraftPoll() {
  if (state.draftPoll) { clearInterval(state.draftPoll.timer); state.draftPoll = null; }
}

function refreshDraftLinks(matchId) {
  if (state.stageMatchId !== matchId) { stopDraftPoll(); return; }
  if (state.draftPoll) {
    state.draftPoll.polls += 1;
    if (state.draftPoll.polls > 90) {
      document.querySelectorAll('#stage-clips .stage-clip-draft').forEach((draftEl) => {
        if (draftEl.classList.contains('ok') || draftEl.classList.contains('fail') || draftEl.classList.contains('warn')) return;
        draftEl.textContent = '草稿状态未知，请到发文后台查看';
        draftEl.className = 'stage-clip-draft warn';
      });
      stopDraftPoll();
      return;
    }
  }
  fetch(`api/monitor/publish/status?match_id=${encodeURIComponent(matchId)}`)
    .then((response) => (response.ok ? response.json() : null))
    .then((data) => {
      if (!data || !data.enabled || !data.records) return;
      const rows = document.querySelectorAll('#stage-clips .stage-clip-row[data-event-key]');
      let pending = 0;
      rows.forEach((row) => {
        const rec = data.records[row.dataset.eventKey];
        const draftEl = row.querySelector('.stage-clip-draft');
        if (!rec || !draftEl) { pending += 1; return; }
        if (applyDraftState(draftEl, rec.status, rec.draft_url, rec.error, rec.retriable)) pending += 1;
      });
      if (pending === 0) stopDraftPoll();
    })
    .catch(() => {});
}

function stageCaptureSeconds() {
  const secondsEl = $('stage-seconds');
  if (secondsEl && secondsEl.value.trim()) {
    const secs = Number(secondsEl.value);
    if (Number.isFinite(secs) && secs > 0 && secs <= 120) return secs;
  }
  return state.clipBeforeSeconds;
}

function syncStageCaptureLabel() {
  const button = $('stage-capture');
  if (!button || button.disabled) return;
  button.textContent = `截取最近 ${stageCaptureSeconds()} 秒`;
}

// ---- stage range capture (operator-chosen start/end) ----
//
// Two clicks: 「开始」 pins the frame on screen as the start (server hard-links
// the segments so they outlive the rolling buffer); 「结束」 takes the frame on
// screen as the end and captures immediately. Clicking 「开始」 again simply
// replaces the start.

function renderRange() {
  const r = state.range;
  const startBtn = $('stage-range-start');
  const endBtn = $('stage-range-end');
  if (!startBtn || !endBtn) return;
  const hasStart = r.pinId != null && r.startMs != null;
  startBtn.textContent = hasStart ? '重新开始' : '开始';
  startBtn.disabled = r.busy;
  endBtn.disabled = !hasStart || r.busy;
}

function clearRange(options = {}) {
  const r = state.range;
  if (r.ttlTimer) { clearTimeout(r.ttlTimer); r.ttlTimer = null; }
  if (r.pinId && !options.keepServerPin) {
    fetch(`api/monitor/clip/pin/${encodeURIComponent(r.pinId)}`, {method: 'DELETE'}).catch(() => {});
  }
  state.range = {matchId: null, pinId: null, startMs: null, ttlTimer: null, busy: false};
  renderRange();
}

async function setRangeStart() {
  const matchId = state.stageMatchId;
  const video = state.stageVideoNode;
  if (!matchId || !video) return;
  const startMs = frameAnchorMs(video);
  if (startMs == null) { addStageNote('当前画面没有时间戳，无法设起点', 'fail'); return; }
  const btn = $('stage-range-start');
  btn.disabled = true;
  try {
    clearRange();
    const data = await requestJson('api/monitor/clip/pin', {
      method: 'POST', body: JSON.stringify({match_id: matchId, start_epoch_ms: Math.round(startMs)}),
    });
    if (data.max_range_seconds) state.clipRangeMaxSeconds = data.max_range_seconds;
    // The stage may have been closed or switched while the request was in
    // flight; do not attach a pin of another match (or no match) to the state.
    if (state.stageMatchId !== matchId) {
      fetch(`api/monitor/clip/pin/${encodeURIComponent(data.pin_id)}`, {method: 'DELETE'}).catch(() => {});
      return;
    }
    state.range = {
      matchId,
      pinId: data.pin_id,
      startMs: data.start_epoch_ms,
      busy: false,
      ttlTimer: setTimeout(() => {
        addStageNote('起点已失效（超时未截取），请重新点「开始」', 'fail');
        clearRange({keepServerPin: true});
      }, Math.max(1000, (data.ttl_seconds || 180) * 1000)),
    };
  } catch (error) {
    addStageNote('设起点失败：' + error.message, 'fail');
  } finally {
    btn.disabled = false;
    renderRange();
  }
}

async function setRangeEndAndCapture(button) {
  const r = state.range;
  const video = state.stageVideoNode;
  if (!video || !r.pinId || r.busy || r.matchId !== state.stageMatchId) return;
  const endMs = frameAnchorMs(video);
  if (endMs == null) { addStageNote('当前画面没有时间戳，无法设终点', 'fail'); return; }
  const seconds = (endMs - r.startMs) / 1000;
  if (seconds < 0.5) { addStageNote('终点必须在起点之后，请往后播放再点「结束」', 'fail'); return; }
  if (seconds > state.clipRangeMaxSeconds) {
    addStageNote(`区间 ${seconds.toFixed(1)}s 超过 ${state.clipRangeMaxSeconds}s 上限，请拖回一些再点「结束」`, 'fail');
    return;
  }
  const snapshot = {pinId: r.pinId, startMs: r.startMs, endMs};
  r.busy = true;
  renderRange();
  const ok = await captureClip(r.matchId, button, snapshot);
  // The server releases the pin on success. On failure keep the start so the
  // operator can pick another end and retry (the pin is still alive).
  if (state.range.pinId !== snapshot.pinId) return;
  if (ok) clearRange({keepServerPin: true});
  else { state.range.busy = false; renderRange(); }
}

function openStage(matchId) {
  const tile = state.tiles.get(matchId);
  if (!tile || !tile.playlistUrl) { showNotice('该比赛直播还没准备好', 'warning'); return; }
  const tileEl = document.querySelector(`.tile[data-match-id="${matchId}"]`);
  const wrap = tileEl ? tileEl.querySelector('.tile-video-wrap') : null;
  const video = wrap ? wrap.querySelector('.tile-video') : null;
  const host = $('stage-video-host');
  if (!video || !host) { showNotice('该比赛直播还没准备好', 'warning'); return; }
  if (state.stageMatchId && state.stageMatchId !== matchId) closeStage();
  state.stageMatchId = matchId;
  stopDraftPoll();
  const stage = $('stage');
  stage.classList.remove('hidden');
  $('stage-title').textContent = (tile.data.team_A_name || '主队') + ' vs ' + (tile.data.team_B_name || '客队');
  $('stage-status').textContent = '直播播放中';
  $('stage-status').className = 'stage-status ok';
  $('stage-notes').innerHTML = '';
  $('stage-clips').innerHTML = '<div class="stage-clip-empty">正在读取 GIF 列表…</div>';
  loadStageClips(matchId);
  const personInput = $('stage-person');
  const titleInput = $('stage-title-input');
  const secondsInput = $('stage-seconds');
  if (personInput) personInput.value = '';
  if (titleInput) titleInput.value = '';
  if (secondsInput) secondsInput.value = '';
  syncStageCaptureLabel();
  clearRange();
  state.stageVideoNode = video;
  state.stageVideoHomeWrap = wrap;
  video.classList.remove('tile-video');
  video.classList.add('stage-video');
  video.setAttribute('controls', '');
  host.appendChild(video);
  state.stageHls = video._hls || null;
  renderLagBadge($('stage-lag'), liveLagSeconds(video));
  const tileOverlay = wrap.querySelector('.tile-overlay');
  if (tileOverlay) { tileOverlay.classList.remove('hidden'); tileOverlay.textContent = '已在大窗播放'; }
  if (!video._hls) {
    state.stageHls = attachHls(video, tile.playlistUrl, (text, cls) => {
      $('stage-status').textContent = text;
      $('stage-status').className = `stage-status ${cls || ''}`;
    });
  }
}

function closeStage() {
  state.stageMatchId = null;
  stopDraftPoll();
  clearRange();
  const video = state.stageVideoNode;
  if (video) {
    video.removeAttribute('controls');
    video.classList.remove('stage-video');
    video.classList.add('tile-video');
    const wrap = state.stageVideoHomeWrap;
    if (wrap && document.body.contains(wrap)) {
      wrap.insertBefore(video, wrap.firstChild);
      const overlay = wrap.querySelector('.tile-overlay');
      if (overlay) { overlay.classList.add('hidden'); overlay.textContent = '正在连接直播源…'; }
    } else {
      if (video._hls) { try { video._hls.destroy(); } catch (e) {} video._hls = null; }
      video.remove();
    }
  }
  state.stageVideoNode = null;
  state.stageVideoHomeWrap = null;
  state.stageHls = null;
  $('stage').classList.add('hidden');
}

// ---- Two-view workflow ----

function switchView(view) {
  state.view = view;
  const isList = view === 'list';
  $('list-view').classList.toggle('hidden', !isList);
  $('workshop-view').classList.toggle('hidden', isList);
  $('workshop-toolbar').classList.toggle('hidden', isList);
  const ewBtn = $('enter-workshop-btn');
  if (ewBtn) ewBtn.classList.toggle('hidden', !isList);
  const listSummary = $('list-summary');
  if (listSummary) listSummary.classList.toggle('hidden', !isList);
  if (isList) {
    renderMatchGrid();
    updateSelectionUI();
  } else {
    const count = state.selectedMatchIds.size;
    $('workshop-summary').textContent = `正在监看 ${count} 场`;
  }
}

function sourceDot(stateVal) {
  if (stateVal === 'available') return '<span class="source-dot available" title="有直播源"></span>';
  if (stateVal === 'no_source') return '<span class="source-dot no-source" title="暂无直播源"></span>';
  if (stateVal === 'error') return '<span class="source-dot error" title="源查询失败"></span>';
  return '<span class="source-dot unknown" title="未知"></span>';
}

function sourceLabel(stateVal) {
  if (stateVal === 'available') return '有源';
  if (stateVal === 'no_source') return '无源';
  if (stateVal === 'error') return '查询失败';
  return '未知';
}

function renderMatchGrid() {
  const grid = $('match-grid');
  if (!grid) return;
  grid.innerHTML = '';
  const matches = state.allMatches;
  // 有源比赛在前，无源在后
  matches.sort((a, b) => {
    const aOk = (a.source_state || state.sourceCache[a.match_id] || 'unknown') === 'available';
    const bOk = (b.source_state || state.sourceCache[b.match_id] || 'unknown') === 'available';
    return aOk === bOk ? 0 : aOk ? -1 : 1;
  });
  const empty = $('list-empty');
  if (!matches.length) {
    if (empty) empty.classList.remove('hidden');
    return;
  }
  if (empty) empty.classList.add('hidden');
  matches.forEach((data) => {
    const mid = data.match_id;
    const checked = state.selectedMatchIds.has(mid) ? 'checked' : '';
    const srcState = data.source_state || state.sourceCache[mid] || 'unknown';
    const noSource = srcState !== 'available';
    const card = document.createElement('div');
    card.className = 'match-card' + (noSource ? ' no-source' : '');
    card.dataset.matchId = mid;
    card.innerHTML =
      `<div class="match-card-check"><input type="checkbox" class="match-checkbox" data-match-id="${escapeHtml(mid)}" ${checked}></div>` +
      `<div class="match-card-teams">${teamsText(data)}</div>` +
      `<div class="match-card-meta">${metaText(data)}</div>` +
      `<div class="match-card-source">${sourceDot(srcState)} <span class="source-label">${escapeHtml(sourceLabel(srcState))}</span></div>`;
    card.querySelector('.match-checkbox').addEventListener('change', (ev) => {
      ev.stopPropagation();
      if (ev.target.checked) {
        state.selectedMatchIds.add(mid);
      } else {
        state.selectedMatchIds.delete(mid);
      }
      updateSelectionUI();
    });
    card.addEventListener('click', (ev) => {
      if (ev.target.tagName === 'INPUT') return; // 让 checkbox 自己的 change 事件处理
      const cb = card.querySelector('.match-checkbox');
      cb.checked = !cb.checked;
      if (cb.checked) {
        state.selectedMatchIds.add(mid);
      } else {
        state.selectedMatchIds.delete(mid);
      }
      updateSelectionUI();
    });
    grid.appendChild(card);
  });
  updateSelectionUI();
}

function updateSelectionUI() {
  const btn = $('enter-workshop-btn');
  if (!btn) return;
  const n = state.selectedMatchIds.size;
  btn.textContent = n ? `进入工作台 (${n})` : '进入工作台';
  btn.disabled = n === 0;
}

function enterWorkshop() {
  if (state.selectedMatchIds.size === 0) return;
  // Cancel idle timer if we came back before the 90s deadline.
  if (state.workshopIdleTimer) { clearTimeout(state.workshopIdleTimer); state.workshopIdleTimer = null; }
  switchView('workshop');
  // Feed reconcileTiles only the selected matches (filtered from the full list).
  const selected = state.allMatches.filter((m) => state.selectedMatchIds.has(m.match_id));
  reconcileTiles(selected);
  // Start lag monitoring (once per session).
  startLagMonitoring();
}

function leaveWorkshop() {
  switchView('list');
  refreshMatches();
  // Start a 90s idle timer — if the operator doesn't re-enter the workshop
  // before it fires, tear down all live streams to free resources.
  if (state.workshopIdleTimer) clearTimeout(state.workshopIdleTimer);
  state.workshopIdleTimer = setTimeout(teardownAllTiles, WORKSHOP_IDLE_MS);
}

function teardownAllTiles() {
  if (state.view !== 'list') return; // operator came back, don't touch
  state.workshopIdleTimer = null;
  for (const matchId of Array.from(state.tiles.keys())) {
    const el = document.querySelector(`.tile[data-match-id="${matchId}"]`);
    if (el) {
      const video = el.querySelector('.tile-video');
      if (video && video._hls) { try { video._hls.destroy(); } catch (e) {} }
      el.remove();
    }
    fetch('api/monitor/preview/stop', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({match_id: matchId}),
    }).catch(() => {});
    if (state.stageMatchId === matchId) closeStage();
    state.tiles.delete(matchId);
  }
  $('grid-empty').classList.add('hidden');
  $('workshop-summary').textContent = '监看已停止';
}

// ---- reconcileTiles (modified to accept a filtered list) ----

function reconcileTiles(playing) {
  const grid = $('grid');
  const seen = new Set();
  // In workshop mode, respect the per-process MAX_PREVIEWS cap.
  const limited = playing.slice(0, state.maxPreviews);
  limited.forEach((data) => {
    if (!data.match_id) return;
    seen.add(data.match_id);
    let tile = state.tiles.get(data.match_id);
    if (!tile) {
      const el = createTile(data);
      grid.appendChild(el);
      tile = {data, playlistUrl: null, clipCount: 0};
      state.tiles.set(data.match_id, tile);
      startTilePreview(data.match_id);
      refreshTileClipCount(data.match_id);
    } else {
      tile.data = data;
      const el = document.querySelector(`.tile[data-match-id="${data.match_id}"]`);
      if (el) updateTileInfo(el, data);
    }
  });
  // Remove tiles that are no longer in the selected set.
  for (const matchId of Array.from(state.tiles.keys())) {
    if (seen.has(matchId)) continue;
    const el = document.querySelector(`.tile[data-match-id="${matchId}"]`);
    if (el) {
      const video = el.querySelector('.tile-video');
      if (video && video._hls) { try { video._hls.destroy(); } catch (e) {} }
      el.remove();
    }
    fetch('api/monitor/preview/stop', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({match_id: matchId}),
    }).catch(() => {});
    if (state.stageMatchId === matchId) closeStage();
    state.tiles.delete(matchId);
  }
  $('grid-empty').classList.toggle('hidden', state.tiles.size > 0);
  if (state.tiles.size === 0) $('grid-empty').textContent = '当前没有进行中的比赛';
  $('workshop-summary').textContent = `正在监看 ${state.tiles.size} 场`;
}

// ---- refreshMatches (split for list vs workshop) ----

async function refreshMatches() {
  const health = $('health');
  const listSummary = $('list-summary');
  try {
    const data = await requestJson('api/monitor/matches?with_sources=1');
    state.maxPreviews = data.max_previews || state.maxPreviews;
    state.clipBeforeSeconds = data.clip_before_seconds || state.clipBeforeSeconds;
    state.clipRangeMaxSeconds = data.clip_range_max_seconds || state.clipRangeMaxSeconds;
    syncStageCaptureLabel();
    const playing = Array.isArray(data.playing) ? data.playing : [];

    if (state.view === 'list') {
      // Merge source states into cache, then render grid.
      playing.forEach((m) => {
        if (m.source_state) state.sourceCache[m.match_id] = m.source_state;
      });
      state.allMatches = playing.map((m) => {
        m.source_state = m.source_state || state.sourceCache[m.match_id] || 'unknown';
        return m;
      });
      renderMatchGrid();
      if (listSummary) listSummary.textContent = `进行中 ${playing.length} 场`;
    } else {
      // Workshop: only keep tiles for the selected matches.
      const selected = playing.filter((m) => state.selectedMatchIds.has(m.match_id));
      reconcileTiles(selected);
    }

    const h = data.health || {};
    health.className = `health ${h.state === 'healthy' ? 'ok' : h.state === 'error' ? 'error' : 'loading'}`;
    health.innerHTML = `<i></i>${escapeHtml(h.label || '赛事目录')}`;
    showNotice('');
  } catch (error) {
    health.className = 'health error';
    health.innerHTML = '<i></i>接口不可用';
    if (state.view === 'list' && listSummary) listSummary.textContent = '接口不可用';
    showNotice(`无法读取比赛列表：${error.message}`, 'error');
  }
}

// ---- event bindings ----

$('enter-workshop-btn').addEventListener('click', enterWorkshop);
$('refresh-btn-2').addEventListener('click', refreshMatches);
$('back-to-list-btn').addEventListener('click', leaveWorkshop);
$('stage-close').addEventListener('click', closeStage);
$('stage-capture').addEventListener('click', (ev) => {
  if (state.stageMatchId) captureClip(state.stageMatchId, ev.currentTarget);
});
$('stage-seconds').addEventListener('input', syncStageCaptureLabel);
$('stage-range-start').addEventListener('click', setRangeStart);
$('stage-range-end').addEventListener('click', (ev) => setRangeEndAndCapture(ev.currentTarget));
$('stage-live').addEventListener('click', () => {
  if (!state.stageVideoNode) return;
  $('stage-status').textContent = '正在回到直播…';
  $('stage-status').className = 'stage-status';
  seekToLive(state.stageVideoNode, () => {
    $('stage-status').textContent = '已回到直播';
    $('stage-status').className = 'stage-status ok';
  });
});
document.addEventListener('keydown', (ev) => {
  if (ev.key !== 'Escape') return;
  if (state.alertPanelOpen) { closeAlertPanel(); return; }
  closeStage();
});
window.addEventListener('beforeunload', () => {
  for (const matchId of state.tiles.keys()) {
    navigator.sendBeacon?.('api/monitor/preview/stop', new Blob([JSON.stringify({match_id: matchId})], {type: 'application/json'}));
  }
});

// ---- keepAlive ----

// Turn ffmpeg's last stderr line into a short operator-facing hint so the tile
// says *why* the source dropped (upstream timeout vs. our side) instead of a
// bare "中断".
function describeExitReason(status) {
  const raw = String((status && (status.last_exit_reason || status.error)) || '').trim();
  if (!raw) return '';
  const lower = raw.toLowerCase();
  if (/timed? ?out|rw_timeout/.test(lower)) return '源站读超时';
  if (/connection reset|broken pipe|end of file|eof/.test(lower)) return '源站断开连接';
  if (/connection refused|no route|name or service|could not resolve/.test(lower)) return '源站不可达';
  if (/server returned 4\d\d|403|404/.test(lower)) return '源站拒绝/地址失效';
  if (/invalid data|non-monotonous|decode|corrupt/.test(lower)) return '源站流数据异常';
  return raw.length > 60 ? raw.slice(0, 60) + '…' : raw;
}

function markPreviewDown(matchId, status) {
  const tile = state.tiles.get(matchId);
  if (tile) tile.down = true;
  const cooling = !!(status && status.auto_restart_cooling);
  let message = cooling ? '直播源中断，后台持续重连中…' : '直播源中断，正在自动重连…';
  const reason = describeExitReason(status);
  if (reason) message += `（${reason}）`;
  const el = document.querySelector(`.tile[data-match-id="${matchId}"]`);
  if (el) {
    const overlay = el.querySelector('.tile-overlay');
    if (overlay) { overlay.classList.remove('hidden'); overlay.textContent = message; }
  }
  setTileStatus(matchId, message, 'warn');
  if (state.stageMatchId === matchId) {
    const node = $('stage-status');
    if (node) { node.textContent = message; node.className = 'stage-status warn'; }
  }
}

function recoverPreview(matchId, status) {
  const tile = state.tiles.get(matchId);
  if (!tile) return;
  tile.down = false;
  const staged = state.stageMatchId === matchId;
  const el = document.querySelector(`.tile[data-match-id="${matchId}"]`);
  const overlay = el ? el.querySelector('.tile-overlay') : null;
  if (overlay && !staged) overlay.classList.add('hidden');
  const video = videoForMatch(matchId);
  if (video) {
    const hls = attachHls(video, tile.playlistUrl, (text, cls) => {
      setTileStatus(matchId, text, cls);
      if (cls === 'fail' && overlay) {
        overlay.classList.remove('hidden');
        overlay.textContent = '直播源不可用（源未推流）';
        tile.down = true;
      }
    });
    if (staged) state.stageHls = hls;
  }
  const total = status && status.restart_count_total;
  const message = total ? `直播源已恢复（本场累计重连 ${total} 次）` : '直播源已恢复';
  setTileStatus(matchId, message, 'ok');
  if (staged) {
    $('stage-status').textContent = message;
    $('stage-status').className = 'stage-status ok';
    addStageNote(message, 'ok');
  }
}

function keepAlivePreviews() {
  for (const [matchId, tile] of state.tiles.entries()) {
    if (!tile.playlistUrl) continue;
    fetch(`api/monitor/preview/status?match_id=${encodeURIComponent(matchId)}`)
      .then((response) => (response.ok ? response.json() : null))
      .then((status) => {
        const current = state.tiles.get(matchId);
        if (!current || !status) return;
        if (status.running === false) {
          markPreviewDown(matchId, status);
        } else if (status.running && status.playlist_ready && current.down) {
          recoverPreview(matchId, status);
        }
      })
      .catch(() => {});
  }
}

// ---- init ----

refreshMatches();
setInterval(refreshMatches, REFRESH_MS);
setInterval(keepAlivePreviews, 8000);

// ---- Real-time goal/red-card alerts (unchanged) ----

const ALERT_POLL_MS = 3000;

function alertWho(ev) {
  let who = ev.team_name || (ev.team === 'A' || ev.team === 'B' ? `${ev.team}队` : '?');
  if (ev.team_id) who += `(${ev.team_id})`;
  return who;
}

function alertSub(ev) {
  const parts = [alertWho(ev)];
  if (ev.from != null && ev.to != null) parts[0] += ` ${ev.from}→${ev.to}`;
  if (ev.score) parts.push(`比分${ev.score}`);
  if (ev.minute) parts.push(String(ev.minute));
  return parts.join(' · ');
}

function alertTitle(ev) {
  return ev.match_title || `match ${ev.match_id}`;
}

function bumpAlertBadge() {
  state.alertCount += 1;
  const badge = $('alert-badge');
  if (badge) { badge.textContent = String(state.alertCount); badge.classList.remove('hidden'); }
}

function clearAlertBadge() {
  state.alertCount = 0;
  const badge = $('alert-badge');
  if (badge) { badge.textContent = '0'; badge.classList.add('hidden'); }
}

function openAlertPanel() {
  const panel = $('alert-panel');
  const launcher = $('alert-launcher');
  if (panel) panel.classList.remove('hidden');
  if (launcher) launcher.classList.add('hidden');
  state.alertPanelOpen = true;
  clearAlertBadge();
}

function closeAlertPanel() {
  const panel = $('alert-panel');
  const launcher = $('alert-launcher');
  if (panel) panel.classList.add('hidden');
  if (launcher) launcher.classList.remove('hidden');
  state.alertPanelOpen = false;
}

function locateMatch(matchId) {
  closeAlertPanel();
  const tile = state.tiles.get(matchId);
  const el = document.querySelector(`.tile[data-match-id="${matchId}"]`);
  if (tile && tile.playlistUrl) { openStage(matchId); return; }
  if (el) {
    el.scrollIntoView({behavior: 'smooth', block: 'center'});
    flashTile(matchId);
    showNotice('该场直播还在准备中，就绪后点格子放大再截取', 'warning');
    return;
  }
  showNotice('该比赛已不在监看列表', 'warning');
}

function flashTile(matchId) {
  const el = document.querySelector(`.tile[data-match-id="${matchId}"]`);
  if (!el) return;
  el.classList.remove('alert-flash');
  void el.offsetWidth;
  el.classList.add('alert-flash');
  setTimeout(() => el.classList.remove('alert-flash'), 2100);
}

function addAlertItem(ev) {
  const list = $('alert-list');
  if (!list) return;
  const empty = $('alert-empty');
  if (empty) empty.classList.add('hidden');
  const item = document.createElement('div');
  item.className = `alert-item ${ev.event_type === 'red_card' ? 'red' : ''}`;
  const time = new Date(ev.ts_unix ? ev.ts_unix * 1000 : Date.now()).toLocaleTimeString();
  item.innerHTML = `
    <div class="alert-item-top">
      <span class="alert-item-tag">${escapeHtml(ev.event_cn || '')}</span>
      <span>${escapeHtml(alertTitle(ev))}</span>
      <span class="alert-item-time">${escapeHtml(time)}</span>
    </div>
    <div class="alert-item-sub">${escapeHtml(alertSub(ev))}</div>`;
  item.addEventListener('click', () => locateMatch(ev.match_id));
  list.insertBefore(item, list.firstChild);
  const items = list.querySelectorAll('.alert-item');
  for (let i = items.length - 1; i >= 50; i -= 1) items[i].remove();
}

function dismissToast(toast) {
  if (!toast || toast._leaving) return;
  toast._leaving = true;
  toast.classList.add('leaving');
  setTimeout(() => toast.remove(), 300);
}

function showToast(ev) {
  const container = $('toast-container');
  if (!container) return;
  const toast = document.createElement('div');
  toast.className = `toast ${ev.event_type === 'red_card' ? 'red' : ''}`;
  toast.innerHTML = `
    <div class="toast-title">${escapeHtml(`${ev.event_cn || ''} · ${alertTitle(ev)}`)}</div>
    <div class="toast-sub">${escapeHtml(alertSub(ev))}</div>`;
  toast.addEventListener('click', () => { locateMatch(ev.match_id); dismissToast(toast); });
  container.appendChild(toast);
  setTimeout(() => dismissToast(toast), 6000);
}

function pushAlert(ev) {
  // 只推送已勾选比赛的提醒
  if (!state.selectedMatchIds.has(ev.match_id)) return;
  addAlertItem(ev);
  showToast(ev);
  if (!state.alertPanelOpen) bumpAlertBadge();
  flashTile(ev.match_id);
}

async function pollAlerts() {
  let data;
  try {
    data = await requestJson(`api/monitor/events?since=${state.alertSeq}`);
  } catch (error) {
    return;
  }
  if (!state.alertInit) {
    state.alertSeq = data.seq || 0;
    state.alertBootId = data.boot_id || null;
    state.alertInit = true;
    return;
  }
  if (data.boot_id && data.boot_id !== state.alertBootId) {
    state.alertSeq = data.seq || 0;
    state.alertBootId = data.boot_id;
    return;
  }
  const events = Array.isArray(data.events) ? data.events : [];
  for (const ev of events) {
    pushAlert(ev);
    state.alertSeq = Math.max(state.alertSeq, ev.seq || 0);
  }
  if (typeof data.seq === 'number') state.alertSeq = Math.max(state.alertSeq, data.seq);
}

$('alert-launcher').addEventListener('click', openAlertPanel);
$('alert-close').addEventListener('click', closeAlertPanel);

// ---- Lag monitoring for diagnostics ----
let lagMonitoringStarted = false;

// Seconds of continuous buffer ahead of the playhead *in the range that
// contains it*. Using the last range's end would silently span a gap (after a
// background pause the newest segments land far ahead of a stale playhead) and
// report a huge, meaningless number.
function bufferedAheadSeconds(video) {
  const ranges = video.buffered;
  const t = video.currentTime;
  for (let i = 0; i < ranges.length; i += 1) {
    if (t >= ranges.start(i) - 0.1 && t <= ranges.end(i) + 0.1) {
      return Math.max(0, ranges.end(i) - t);
    }
  }
  return 0;
}

// Seconds behind the server's newest segment, or null when hls.js has not
// loaded a playlist yet. Uses hls.latency (stream-timeline based) so ffmpeg's
// start-up burst offset and server/browser clock skew cancel out.
function liveLagSeconds(video) {
  const hls = video && video._hls;
  if (!hls || typeof hls.latency !== 'number' || !(hls.latency > 0)) return null;
  return hls.latency;
}

const LAG_OK_MAX_SECONDS = 15;
const LAG_WARN_MAX_SECONDS = 60;

function renderLagBadge(node, lag) {
  if (!node) return;
  if (lag == null) {
    node.textContent = '落后 —';
    node.className = 'lag-badge na';
    return;
  }
  const cls = lag <= LAG_OK_MAX_SECONDS ? 'ok' : lag <= LAG_WARN_MAX_SECONDS ? 'warn' : 'bad';
  node.textContent = `落后 ${Math.round(lag)}s`;
  node.className = `lag-badge ${cls}`;
}

function startLagMonitoring() {
  if (lagMonitoringStarted) return;
  lagMonitoringStarted = true;

  console.log('[LAG-MONITOR] 滞后监控已启动，每 1 秒记录一次');

  setInterval(() => {
    for (const matchId of state.tiles.keys()) {
      // videoForMatch() follows the element into the stage when it is enlarged.
      const video = videoForMatch(matchId);
      const tileBadge = document.querySelector(`.tile[data-match-id="${matchId}"] [data-role="lag"]`);
      const lag = liveLagSeconds(video);
      renderLagBadge(tileBadge, lag);
      if (state.stageMatchId === matchId) renderLagBadge($('stage-lag'), lag);
      if (!video || !video._hls) continue;
      const hls = video._hls;

      const latency = lag != null ? `${lag.toFixed(1)}s` : 'N/A';
      const target = typeof hls.targetLatency === 'number' && hls.targetLatency > 0
        ? `${hls.targetLatency.toFixed(0)}s`
        : '?';

      // PDT差：旧算法（浏览器墙钟 − 帧 PDT），含时钟差和 burst 偏移。
      // 暂时保留做对照，确认与"落后直播"相差常数后删除。
      const anchorMs = frameAnchorMs(video);
      const pdtDiff = anchorMs
        ? `${Math.round((Date.now() - anchorMs) / 1000)}s`
        : 'N/A';

      const videoState = video.paused ? 'PAUSED' :
                         (video.readyState < 3 ? 'BUFFERING' : 'PLAYING');
      const pageVisible = document.hidden ? '后台' : '前台';

      console.log(
        `[LAG] ${matchId} | 落后直播 ${latency} (目标 ${target}) | ` +
        `PDT差 ${pdtDiff} | ${videoState} | ${pageVisible} | ` +
        `本段缓冲 ${bufferedAheadSeconds(video).toFixed(0)}s`
      );
    }
  }, 1000);
}

pollAlerts();
setInterval(pollAlerts, ALERT_POLL_MS);