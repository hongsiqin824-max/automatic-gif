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
  clipRangeMaxSeconds: 30,
  // Mirrors GIF_MONITOR_CATCHUP_ENABLED; false disables tile speed-up and auto seek.
  catchupEnabled: true,
  // Operator-chosen [start, end] range on the stage. ``pinId`` is the server
  // pin that hard-links the start so it survives the rolling buffer.
  range: {matchId: null, pinId: null, startMs: null, ttlTimer: null, busy: false},
  // Date.now() of the operator's last manual scrub/pause on the stage. Stage
  // auto-seek stays off for stageUserGraceSeconds after it (see lagMonitor).
  stageUserActionAt: 0,
  stageOpenedAt: 0,      // Date.now() of the last openStage (ignore its own pause/seek noise)
  draftPoll: null,       // {matchId, timer, polls} while awaiting draft links
  alertSeq: 0,           // cursor: last event seq the page has rendered
  alertBootId: null,     // server boot id; a change means 8901 restarted
  alertInit: false,      // skip replaying history on the first poll
  alertPolling: false,   // a poll is in flight; do not start another one
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

// ---- WebRTC (WHEP) tile playback — scheme H, BACKLOG 11.13 ----
//
// When the server reports ``webrtc: true`` for a match the grid tile plays the
// mediamtx WebRTC stream (sub-second) instead of HLS. The stage and GIF capture
// always stay on HLS: WebRTC cannot scrub backwards and carries no
// PROGRAM-DATE-TIME. Any WebRTC failure falls back to HLS on the same element.
const WHEP_MAX_RETRIES = 3;
const WHEP_RETRY_MS = 2000;
const WHEP_STATS_MS = 1000;

// Latency knobs mirrored from the server .env (see applyLatencyConfig, which
// overwrites these from /api/monitor/matches). Declared up here, before any
// function that reads them can run, so attachHls never sees an uninitialised
// binding. Defaults equal the server defaults.
const LATENCY = {
  hlsLiveSyncCount: 3,       // GIF_MONITOR_HLS_LIVE_SYNC_COUNT
  catchupRateEnabled: true,  // GIF_MONITOR_CATCHUP_RATE_ENABLED
  catchupRate: 1.5,          // GIF_MONITOR_CATCHUP_RATE
  catchupMinLag: 12,         // GIF_MONITOR_CATCHUP_MIN_LAG
  tileSeekLag: 0,            // GIF_MONITOR_TILE_SEEK_LAG (0 = off)
  stageAutoSeekLag: 0,       // GIF_MONITOR_STAGE_AUTO_SEEK_LAG (0 = off)
  stageUserGraceSeconds: 60, // GIF_MONITOR_STAGE_USER_GRACE_SECONDS
  stageSeekLag: 30,          // GIF_MONITOR_STAGE_SEEK_LAG
  hardSeekLag: 150,          // GIF_MONITOR_HARD_SEEK_LAG
};

// Server-vs-browser clock offset (serverNow − Date.now()), measured once from
// /api/health so a WebRTC tile can turn "now − playout delay" into a server
// epoch that lines up with the PDT-stamped HLS segments used for capture.
let serverClockOffsetMs = 0;
let serverClockMeasured = false;
async function measureServerClock() {
  try {
    const t0 = Date.now();
    const r = await fetch('api/health', {cache: 'no-store'});
    const t1 = Date.now();
    const data = await r.json();
    if (typeof data.time_unix === 'number') {
      serverClockOffsetMs = data.time_unix * 1000 - (t0 + t1) / 2;
      serverClockMeasured = true;
    }
  } catch (e) { /* keep previous estimate */ }
}

function detachPlayer(video) {
  if (!video) return;
  if (video._hls) { try { video._hls.destroy(); } catch (e) {} video._hls = null; }
  const whep = video._whep;
  if (whep) {
    video._whep = null;
    clearInterval(whep.statsTimer);
    clearTimeout(whep.retryTimer);
    try { whep.pc.close(); } catch (e) {}
    if (whep.resourceUrl) fetch(whep.resourceUrl, {method: 'DELETE', keepalive: true}).catch(() => {});
    if (video.srcObject) video.srcObject = null;
  }
}

// Estimate how far this WebRTC tile is behind the server's live edge: the
// receiver jitter buffer (what the browser is deliberately holding) plus half
// the RTT. mediamtx itself adds only a few ms. Updated every second by
// attachWhep's stats poll and read by liveLagSeconds / frameAnchorMs.
async function sampleWhepLag(video) {
  const whep = video && video._whep;
  if (!whep || whep.pc.connectionState !== 'connected') return;
  let stats;
  try { stats = await whep.pc.getStats(); } catch (e) { return; }
  let jitterSeconds = null;
  let rtt = 0;
  for (const report of stats.values()) {
    if (report.type === 'inbound-rtp' && report.kind === 'video') {
      const dDelay = report.jitterBufferDelay - whep.prevJbDelay;
      const dCount = report.jitterBufferEmittedCount - whep.prevJbCount;
      whep.prevJbDelay = report.jitterBufferDelay;
      whep.prevJbCount = report.jitterBufferEmittedCount;
      if (dCount > 0) jitterSeconds = dDelay / dCount;
      whep.framesDecoded = report.framesDecoded;
    } else if (report.type === 'candidate-pair' && report.state === 'succeeded'
               && typeof report.currentRoundTripTime === 'number') {
      rtt = report.currentRoundTripTime;
    }
  }
  if (jitterSeconds != null) whep.lagSeconds = Math.max(0.05, jitterSeconds + rtt / 2);
}

async function attachWhep(video, whepUrl, onStatus, onFallback, retries = 0) {
  detachPlayer(video);
  if (!window.RTCPeerConnection) { if (onFallback) onFallback('浏览器不支持 WebRTC'); return null; }
  const whep = {
    pc: new RTCPeerConnection(),
    url: whepUrl,
    resourceUrl: null,
    // Carried across re-attach attempts so a dead WHEP endpoint gives up
    // after WHEP_MAX_RETRIES instead of restarting the count at every retry.
    retries,
    retryTimer: null,
    statsTimer: null,
    lagSeconds: null,
    prevJbDelay: 0,
    prevJbCount: 0,
    framesDecoded: 0,
  };
  video._whep = whep;
  const pc = whep.pc;
  pc.addTransceiver('video', {direction: 'recvonly'});
  pc.ontrack = (event) => {
    if (video._whep !== whep) return;
    video.srcObject = event.streams[0];
    video.play().catch(() => {});
  };
  const fail = (reason) => {
    if (video._whep !== whep) return;
    if (whep.retries < WHEP_MAX_RETRIES) {
      whep.retries += 1;
      if (onStatus) onStatus(`WebRTC 重连中 (${whep.retries}/${WHEP_MAX_RETRIES})…`, 'warn');
      whep.retryTimer = setTimeout(() => {
        if (video._whep !== whep) return;
        attachWhep(video, whepUrl, onStatus, onFallback, whep.retries);
      }, WHEP_RETRY_MS);
      return;
    }
    console.warn(`[WHEP] ${whepUrl} 放弃，回退 HLS：${reason}`);
    detachPlayer(video);
    if (onFallback) onFallback(reason);
  };
  pc.onconnectionstatechange = () => {
    if (video._whep !== whep) return;
    const s = pc.connectionState;
    if (s === 'connected') {
      whep.retries = 0;
      if (onStatus) onStatus('正在播放直播源（WebRTC）', 'ok');
    } else if (s === 'failed' || s === 'disconnected' || s === 'closed') {
      fail(`connection ${s}`);
    }
  };
  try {
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    // Wait for ICE gathering (host candidates only, no STUN configured) so the
    // offer we POST is complete; WHEP is a single round trip.
    await new Promise((resolve) => {
      if (pc.iceGatheringState === 'complete') { resolve(); return; }
      const t = setTimeout(resolve, 1500);
      pc.onicegatheringstatechange = () => {
        if (pc.iceGatheringState === 'complete') { clearTimeout(t); resolve(); }
      };
    });
    if (video._whep !== whep) return null;
    const res = await fetch(whepUrl, {
      method: 'POST',
      headers: {'Content-Type': 'application/sdp'},
      body: pc.localDescription.sdp,
    });
    if (!res.ok) throw new Error(`WHEP ${res.status}`);
    const location = res.headers.get('Location');
    if (location) {
      // mediamtx answers with an absolute path (/<path>/whep/<session>) that
      // knows nothing about the reverse-proxy prefix we sit behind. Keep only
      // the session id and hang it off the WHEP URL we actually posted to.
      const sessionId = location.split('/').filter(Boolean).pop();
      whep.resourceUrl = new URL(`${whepUrl.replace(/\/+$/, '')}/${sessionId}`, window.location.href).href;
    }
    const answer = await res.text();
    if (video._whep !== whep) return null;
    await pc.setRemoteDescription({type: 'answer', sdp: answer});
  } catch (error) {
    fail(error && error.message ? error.message : String(error));
    return whep;
  }
  whep.statsTimer = setInterval(() => sampleWhepLag(video), WHEP_STATS_MS);
  if (!serverClockMeasured) measureServerClock();
  return whep;
}

function isWhepVideo(video) {
  return !!(video && video._whep);
}

// Attach the right player to a grid tile from a preview/start|status payload.
// WebRTC when offered; HLS otherwise or as the fallback target.
function startTilePlayback(matchId, video, data, onStatus) {
  const tile = state.tiles.get(matchId);
  const playlistUrl = data && data.playlist_url ? data.playlist_url : (tile && tile.playlistUrl);
  const whepUrl = data && data.webrtc && data.whep_url ? data.whep_url : null;
  if (tile) tile.whepUrl = whepUrl;
  if (whepUrl) {
    attachWhep(video, whepUrl, onStatus, (reason) => {
      if (tile) tile.whepUrl = null;
      if (onStatus) onStatus('WebRTC 不可用，已切回 HLS', 'warn');
      attachHls(video, playlistUrl, onStatus);
    });
    return null;
  }
  return attachHls(video, playlistUrl, onStatus);
}

// Read the PROGRAM-DATE-TIME of the frame currently on screen so the capture
// anchors on what the human saw, independent of stream delay.
function frameAnchorMs(video) {
  if (!video) return null;
  // WebRTC carries no PDT: the frame on screen is "server now − playout
  // delay". Accuracy is ±~0.5s, acceptable for a 10s GIF; the stage (HLS)
  // keeps the exact PDT path.
  if (video._whep) {
    const lag = video._whep.lagSeconds;
    if (lag == null) return null;
    return Date.now() + serverClockOffsetMs - lag * 1000;
  }
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

function tileVideoEl(matchId) {
  return document.querySelector(`.tile[data-match-id="${matchId}"] .tile-video`);
}

// The video an operator is looking at for this match: the stage player when
// enlarged (HLS - exact PDT, scrubbable), otherwise the grid tile.
function videoForMatch(matchId) {
  if (state.stageMatchId === matchId && state.stageVideoNode) {
    return state.stageVideoNode;
  }
  return tileVideoEl(matchId);
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
  // Stamp so the stage's user-action listener can tell this seek apart from
  // a manual scrub (the `seeking` event fires for both).
  video._programmaticSeekAt = Date.now();
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
  detachPlayer(video);
  if (window.Hls && window.Hls.isSupported()) {
    const hls = new window.Hls({
      liveSyncDurationCount: LATENCY.hlsLiveSyncCount,
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
  el.querySelector('[data-role="live"]').addEventListener('click', (ev) => {
    ev.stopPropagation();
    const video = tileVideoEl(data.match_id) || videoForMatch(data.match_id);
    if (isWhepVideo(video)) { setTileStatus(data.match_id, 'WebRTC 始终在直播边缘', 'ok'); return; }
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
    startTilePlayback(matchId, video, data, (text, cls) => {
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
// ``range`` = {pinId, startMs, endMs} from 「开始/结束」; this is the only way
// to capture now (the fixed "last N seconds" buttons were removed as unused).
// Returns true on success so the range caller knows whether to reset its state.
async function captureClip(matchId, button, range) {
  const prevLabel = button ? button.textContent : '';
  if (button) { button.disabled = true; button.textContent = '截取中…'; }
  setTileStatus(matchId, '正在截取所选区间…');
  try {
    const payload = {
      match_id: matchId,
      pin_id: range.pinId,
      end_epoch_ms: Math.round(range.endMs),
    };
    const data = await requestJson('api/monitor/clip', {
      method: 'POST', body: JSON.stringify(payload),
    });
    const sizeMb = (data.bytes / (1024 * 1024)).toFixed(2);
    const warn = data.anchored === false;
    // Capture never publishes: the operator reviews the GIF in the stage
    // list, checks the pre-filled CMS fields there, and clicks 「发布」.
    const publishNote = data.publish_enabled ? ' · 核对后在下方列表点「发布」' : ' · 仅本地保存';
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
    return ` · 已关联推送 ${clock}${flag}`;
  }
  return ' · 未匹配到推送，已按「跳过」预填，请在列表里选推送或手填';
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

// ---- publish form (mirrors the CMS gif-article fields) ----
//
// An unpublished row is a small form: the push the clip belongs to (radio,
// server pre-selects the most likely one), then 事件类型 / 比赛时间 / 比分 /
// 标题. Picking a push re-fills type/time/score; 跳过 clears them. A field the
// operator has typed into is never overwritten by the radio again. Title is
// never pre-filled (it is always rewritten by hand) and gates the button.

const MATCH_EVENT_OPTIONS = [
  [0, '无'], [1, '进球'], [2, '点球'], [3, '点球不进'], [4, '红牌'], [5, '乌龙球'], [6, '进球无效'],
];

function candidateLabel(c) {
  const clock = c.match_time ? `${c.match_time}'` : '?';
  const team = c.team_name || (c.team === 'A' ? '主队' : c.team === 'B' ? '客队' : '');
  return [clock, c.event_cn || c.event_type, team, c.score].filter(Boolean).join(' ');
}

function buildClipForm(item, row) {
  const form = document.createElement('div');
  form.className = 'stage-clip-form';
  const initial = item.form || {};
  const am = item.alert_match || {};
  const candidates = Array.isArray(item.candidates) ? item.candidates : [];
  const radioName = `clip-src-${item.name.replace(/\W/g, '')}`;
  const dirty = new Set();

  const sources = document.createElement('div');
  sources.className = 'stage-clip-sources';
  const addOption = (c, flagText, isNew) => {
    const label = document.createElement('label');
    label.className = 'stage-clip-source';
    const radio = document.createElement('input');
    radio.type = 'radio'; radio.name = radioName;
    radio.value = c ? String(c.seq) : '';
    radio.dataset.matchEvent = c ? String(c.match_event) : '0';
    radio.dataset.matchTime = c ? (c.match_time || '') : '';
    radio.dataset.score = c ? (c.score || '') : '';
    radio.addEventListener('change', () => applySource(radio));
    label.appendChild(radio);
    label.appendChild(document.createTextNode(' ' + (c ? candidateLabel(c) : '跳过（没有对应推送）')));
    if (flagText) {
      const flag = document.createElement('span');
      flag.className = 'stage-clip-flag';
      flag.textContent = ' ' + flagText;
      flag.title = flagText.startsWith('⚠') ? '程序自动关联的，可信度不高，请核对' : '截取后新到的推送';
      label.appendChild(flag);
    }
    if (isNew) label.classList.add('new');
    return label;
  };
  candidates.forEach((c) => {
    const picked = initial.alert_seq != null && c.seq === initial.alert_seq;
    const flag = picked && (am.weak || am.ambiguous) ? '⚠ 请核对' : '';
    sources.appendChild(addOption(c, flag, false));
  });
  const skip = addOption(null, '', false);
  sources.appendChild(skip);
  form.appendChild(sources);

  const fields = document.createElement('div');
  fields.className = 'stage-clip-fields';
  const mk = (container, key, el, labelText) => {
    const wrap = document.createElement('label');
    wrap.className = 'stage-clip-field';
    wrap.appendChild(document.createTextNode(labelText));
    el.addEventListener('input', () => { dirty.add(key); syncPublishButton(); });
    el.addEventListener('change', () => { dirty.add(key); syncPublishButton(); });
    wrap.appendChild(el);
    container.appendChild(wrap);
  };
  const typeSel = document.createElement('select');
  typeSel.className = 'stage-input stage-clip-select';
  MATCH_EVENT_OPTIONS.forEach(([value, text]) => {
    const opt = document.createElement('option');
    opt.value = String(value); opt.textContent = text;
    typeSel.appendChild(opt);
  });
  mk(fields, 'match_event', typeSel, '类型');
  const timeEl = document.createElement('input');
  timeEl.className = 'stage-input stage-clip-input stage-clip-time';
  timeEl.type = 'text'; timeEl.placeholder = '19 / 90+3'; timeEl.maxLength = 7;
  mk(fields, 'match_time', timeEl, '时间');
  const scoreEl = document.createElement('input');
  scoreEl.className = 'stage-input stage-clip-input stage-clip-score';
  scoreEl.type = 'text'; scoreEl.placeholder = '3-1 主队在前'; scoreEl.maxLength = 7;
  mk(fields, 'match_score', scoreEl, '比分');
  form.appendChild(fields);

  const titleRow = document.createElement('div');
  titleRow.className = 'stage-clip-fields';
  const titleEl = document.createElement('input');
  titleEl.className = 'stage-input stage-clip-input stage-clip-title';
  titleEl.type = 'text'; titleEl.maxLength = 100; titleEl.placeholder = '标题（必填）';
  mk(titleRow, 'title', titleEl, '标题');
  const publish = document.createElement('button');
  publish.className = 'button primary stage-clip-publish';
  publish.type = 'button';
  publish.textContent = '发布';
  publish.addEventListener('click', () => publishClip(item.name, readForm(), publish, row));
  titleRow.appendChild(publish);
  form.appendChild(titleRow);

  function applySource(radio) {
    // The pick itself counts as an edit (so a list rebuild keeps it); re-fill
    // only the fields the operator has not touched.
    dirty.add('alert_seq');
    if (!dirty.has('match_event')) typeSel.value = radio.dataset.matchEvent;
    if (!dirty.has('match_time')) timeEl.value = radio.dataset.matchTime;
    if (!dirty.has('match_score')) scoreEl.value = radio.dataset.score;
    syncPublishButton();
  }
  function syncPublishButton() {
    const ok = titleEl.value.trim().length > 0;
    publish.disabled = !ok;
    publish.title = ok ? '' : '请填写标题后再发布';
  }
  function readForm() {
    const checked = form.querySelector(`input[name="${radioName}"]:checked`);
    return {
      alert_seq: checked && checked.value ? Number(checked.value) : null,
      match_event: Number(typeSel.value),
      match_time: timeEl.value.trim(),
      match_score: scoreEl.value.trim(),
      title: titleEl.value.trim(),
    };
  }
  function writeForm(values) {
    const seq = values.alert_seq;
    const radio = seq == null
      ? skip.querySelector('input')
      : form.querySelector(`input[name="${radioName}"][value="${seq}"]`);
    (radio || skip.querySelector('input')).checked = true;
    typeSel.value = String(values.match_event ?? 0);
    timeEl.value = values.match_time || '';
    scoreEl.value = values.match_score || '';
    titleEl.value = values.title || '';
    syncPublishButton();
  }
  // Append a push that arrived after capture; selection and fields untouched.
  function addCandidate(c) {
    if (form.querySelector(`input[name="${radioName}"][value="${c.seq}"]`)) return;
    sources.insertBefore(addOption(c, '新', true), sources.firstChild);
  }
  writeForm(initial);
  form._clip = {readForm, writeForm, addCandidate, dirty};
  return form;
}

function buildPublishedSummary(item) {
  const f = item.form || {};
  const type = (MATCH_EVENT_OPTIONS.find(([v]) => v === Number(f.match_event)) || [0, '无'])[1];
  const parts = [type, f.match_time ? `${f.match_time}'` : '', f.match_score || '', f.title || ''].filter(Boolean);
  const el = document.createElement('span');
  el.className = 'stage-clip-summary';
  el.textContent = parts.join(' · ');
  el.title = '已发布内容（只读）';
  return el;
}

function renderClipRow(item, publishEnabled) {
  const row = document.createElement('div');
  row.className = 'stage-clip-row';
  if (item.event_key) row.dataset.eventKey = item.event_key;
  const head = document.createElement('div');
  head.className = 'stage-clip-head';
  const sizeMb = (item.bytes / (1024 * 1024)).toFixed(2);
  const link = document.createElement('a');
  link.className = 'stage-clip-open';
  link.href = item.clip_url; link.target = '_blank'; link.rel = 'noreferrer';
  link.textContent = '打开 GIF';
  const label = document.createElement('span');
  label.className = 'stage-clip-label';
  label.textContent = `${fmtClipTime(item.created_unix)} · ${sizeMb} MB`;
  if (item.anchored === false) {
    label.textContent += ' · ⚠未锚定';
    label.title = '截取时没有时间戳，画面可能不准，发布前请打开核对';
  }
  head.appendChild(label);
  head.appendChild(link);
  row.appendChild(head);
  if (!publishEnabled) {
    const note = document.createElement('span');
    note.className = 'stage-clip-draft warn';
    note.textContent = '仅本地保存';
    head.appendChild(note);
    return row;
  }
  if (item.event_key) {
    // Already sent: what went out (read-only) plus the draft/article link.
    head.appendChild(buildPublishedSummary(item));
    const draftEl = document.createElement('span');
    draftEl.className = 'stage-clip-draft';
    applyDraftState(draftEl, item.draft_status, item.draft_url, item.draft_error, item.draft_retriable);
    head.appendChild(draftEl);
    return row;
  }
  row.classList.add('editing');
  row.appendChild(buildClipForm(item, row));
  return row;
}

async function publishClip(name, formValues, button, row) {
  const matchId = state.stageMatchId;
  if (!matchId) return;
  if (!formValues.title) { addStageNote('请填写标题后再发布', 'warn'); return; }
  button.disabled = true;
  button.textContent = '发布中…';
  row.querySelectorAll('input,select').forEach((el) => { el.disabled = true; });
  try {
    const data = await requestJson('api/monitor/clip/publish', {
      method: 'POST',
      body: JSON.stringify({match_id: matchId, name, ...formValues}),
    });
    addStageNote(data.delivery_mode === 'publish' ? '已提交发布' : '已提交建草稿', 'ok');
    if (state.stageMatchId === matchId) loadStageClips(matchId);
  } catch (error) {
    addStageNote('发布失败：' + error.message, 'fail');
    button.disabled = false;
    button.textContent = '发布';
    row.querySelectorAll('input,select').forEach((el) => { el.disabled = false; });
  }
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
  // The list is rebuilt after every capture; keep whatever the operator had
  // already changed in an unpublished row's form.
  const typed = new Map();
  list.querySelectorAll('.stage-clip-row[data-name] .stage-clip-form').forEach((formEl) => {
    const api = formEl._clip;
    if (api && api.dirty.size) typed.set(formEl.closest('.stage-clip-row').dataset.name, {values: api.readForm(), dirty: api.dirty});
  });
  list.innerHTML = '';
  if (!clips.length) {
    const empty = document.createElement('div');
    empty.className = 'stage-clip-empty';
    empty.textContent = '本场还没有截取过 GIF';
    list.appendChild(empty);
  } else {
    clips.forEach((item) => {
      const row = renderClipRow(item, !!data.publish_enabled);
      row.dataset.name = item.name;
      const kept = typed.get(item.name);
      const formEl = row.querySelector('.stage-clip-form');
      if (kept && formEl && formEl._clip) {
        formEl._clip.writeForm(kept.values);
        kept.dirty.forEach((k) => formEl._clip.dirty.add(k));
      }
      list.appendChild(row);
    });
  }
  setTileClipCount(matchId, clips.length);
  const pending = clips.some((c) => c.event_key && !(
    ((c.draft_status === 'published' || c.draft_status === 'drafted') && c.draft_url)
    || (c.draft_status === 'failed' && !c.draft_retriable)
  ));
  if (pending) startDraftPoll(matchId); else stopDraftPoll();
}

// A push for the stage match arrived after capture: offer it on every
// unpublished row as a new radio option, without changing the selection.
function offerAlertToClipForms(ev) {
  if (!state.stageMatchId || String(ev.match_id) !== String(state.stageMatchId)) return;
  const m = /(\d{1,3})(?:\s*\+\s*(\d{1,2}))?\s*'?\s*$/.exec(String(ev.minute || ''));
  const candidate = {
    seq: ev.seq,
    event_type: ev.event_type,
    event_cn: ev.event_cn,
    team: ev.team,
    team_name: ev.team_name || '',
    match_event: ev.event_type === 'red_card' ? 4 : 1,
    match_time: m ? (m[2] ? `${m[1]}+${m[2]}` : m[1]) : '',
    score: ev.score || '',
  };
  document.querySelectorAll('#stage-clips .stage-clip-form').forEach((formEl) => {
    if (formEl._clip) formEl._clip.addCandidate(candidate);
  });
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

// ---- Stage user-action tracking (for GIF_MONITOR_STAGE_AUTO_SEEK_LAG) ----
// A manual scrub-back or pause on the stage's native controls means the
// operator is working there; auto-seek must leave the stage alone for the
// grace period. Not counted: our own jumpToLiveSync (stamped), hls.js stall
// nudges (+0.1s) and live re-syncs (forward) - only a seek that moves the
// playhead backwards by at least MANUAL_SEEK_BACK_MIN_SECONDS qualifies.
const PROGRAMMATIC_SEEK_WINDOW_MS = 1500;
const MANUAL_SEEK_BACK_MIN_SECONDS = 1;

function noteStageUserAction(video, kind) {
  // Only the open stage counts: a tile has no controls, and the DOM moves in
  // openStage/closeStage can fire `pause` on their own.
  if (!state.stageMatchId || !isStageVideo(video)) return;
  if (Date.now() - state.stageOpenedAt < PROGRAMMATIC_SEEK_WINDOW_MS) return;
  if (kind === 'seeking') {
    if (video._programmaticSeekAt && Date.now() - video._programmaticSeekAt < PROGRAMMATIC_SEEK_WINDOW_MS) return;
    const from = video._lastPlayheadTime;
    if (typeof from !== 'number' || video.currentTime > from - MANUAL_SEEK_BACK_MIN_SECONDS) return;
  }
  state.stageUserActionAt = Date.now();
}

function attachStageUserActionListeners(video) {
  if (!video || video._stageActionBound) return;
  video._stageActionBound = true;
  video.addEventListener('timeupdate', () => { if (!video.seeking) video._lastPlayheadTime = video.currentTime; });
  video.addEventListener('seeking', () => noteStageUserAction(video, 'seeking'));
  video.addEventListener('pause', () => noteStageUserAction(video, 'pause'));
}

// Why the stage must not be auto-seeked right now, or null when it may be.
function stageAutoSeekBlocker(video) {
  if (state.range.pinId != null) return 'range';
  if (video.paused) return 'paused';
  const grace = LATENCY.stageUserGraceSeconds * 1000;
  if (grace > 0 && Date.now() - state.stageUserActionAt < grace) return 'grace';
  return null;
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
  state.stageOpenedAt = Date.now();
  state.stageUserActionAt = 0;
  stopDraftPoll();
  const stage = $('stage');
  stage.classList.remove('hidden');
  $('stage-title').textContent = (tile.data.team_A_name || '主队') + ' vs ' + (tile.data.team_B_name || '客队');
  $('stage-status').textContent = '直播播放中';
  $('stage-status').className = 'stage-status ok';
  $('stage-notes').innerHTML = '';
  $('stage-clips').innerHTML = '<div class="stage-clip-empty">正在读取 GIF 列表…</div>';
  loadStageClips(matchId);
  clearRange();
  const stageStatus = (text, cls) => {
    $('stage-status').textContent = text;
    $('stage-status').className = `stage-status ${cls || ''}`;
  };
  if (isWhepVideo(video)) {
    // WebRTC tile: leave it playing in the grid and give the stage its own HLS
    // player - the stage must scrub and carry PDT for exact capture.
    const own = document.createElement('video');
    own.className = 'stage-video';
    own.setAttribute('playsinline', '');
    own.setAttribute('autoplay', '');
    own.muted = true;
    own.setAttribute('controls', '');
    host.appendChild(own);
    state.stageVideoNode = own;
    state.stageVideoHomeWrap = null;
    state.stageOwnVideo = true;
    state.stageHls = attachHls(own, tile.playlistUrl, stageStatus);
    attachStageUserActionListeners(own);
    renderLagBadge($('stage-lag'), null);
    return;
  }
  state.stageVideoNode = video;
  state.stageVideoHomeWrap = wrap;
  state.stageOwnVideo = false;
  // The stage is the capture/scrub surface: never speed it up (see applyTileCatchupRate).
  video.playbackRate = 1;
  video.classList.remove('tile-video');
  video.classList.add('stage-video');
  video.setAttribute('controls', '');
  host.appendChild(video);
  attachStageUserActionListeners(video);
  state.stageHls = video._hls || null;
  renderLagBadge($('stage-lag'), liveLagSeconds(video));
  const tileOverlay = wrap.querySelector('.tile-overlay');
  if (tileOverlay) { tileOverlay.classList.remove('hidden'); tileOverlay.textContent = '已在大窗播放'; }
  if (!video._hls) {
    state.stageHls = attachHls(video, tile.playlistUrl, stageStatus);
  }
}

function closeStage() {
  state.stageMatchId = null;
  stopDraftPoll();
  clearRange();
  const video = state.stageVideoNode;
  if (video && state.stageOwnVideo) {
    detachPlayer(video);
    video.remove();
  } else if (video) {
    video.removeAttribute('controls');
    video.classList.remove('stage-video');
    video.classList.add('tile-video');
    const wrap = state.stageVideoHomeWrap;
    if (wrap && document.body.contains(wrap)) {
      wrap.insertBefore(video, wrap.firstChild);
      const overlay = wrap.querySelector('.tile-overlay');
      if (overlay) { overlay.classList.add('hidden'); overlay.textContent = '正在连接直播源…'; }
    } else {
      detachPlayer(video);
      video.remove();
    }
  }
  state.stageVideoNode = null;
  state.stageVideoHomeWrap = null;
  state.stageOwnVideo = false;
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
      detachPlayer(el.querySelector('.tile-video'));
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
      detachPlayer(el.querySelector('.tile-video'));
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
    state.clipRangeMaxSeconds = data.clip_range_max_seconds || state.clipRangeMaxSeconds;
    if (typeof data.catchup_enabled === 'boolean') state.catchupEnabled = data.catchup_enabled;
    applyLatencyConfig(data.latency);
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
    // The stage keeps its own HLS player when the tile is on WebRTC; only
    // re-attach the element that actually belongs to this tile.
    const target = staged && state.stageOwnVideo ? tileVideoEl(matchId) : video;
    const hls = startTilePlayback(matchId, target, status, (text, cls) => {
      setTileStatus(matchId, text, cls);
      if (cls === 'fail' && overlay) {
        overlay.classList.remove('hidden');
        overlay.textContent = '直播源不可用（源未推流）';
        tile.down = true;
      }
    });
    if (staged && !state.stageOwnVideo) state.stageHls = hls;
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
          // resource === null means the server has no entry for this match at
          // all (process restart, 90s idle reap, or a stop sent by another
          // tab). The backend auto-restart only covers a *dead* ffmpeg whose
          // entry still exists, so in this case nobody would ever relaunch it
          // and "正在自动重连" would be a lie: re-issue preview/start ourselves.
          if (status.resource == null) restartTilePreview(matchId);
        } else if (status.running && status.playlist_ready && current.down) {
          recoverPreview(matchId, status);
        }
      })
      .catch(() => {});
  }
}

async function restartTilePreview(matchId) {
  const tile = state.tiles.get(matchId);
  if (!tile || tile.restarting) return;
  tile.restarting = true;
  try {
    const data = await requestJson('api/monitor/preview/start', {
      method: 'POST', body: JSON.stringify({match_id: matchId}),
    });
    if (!state.tiles.get(matchId)) return;
    tile.playlistUrl = data.playlist_url;
    recoverPreview(matchId, data);
  } catch (error) {
    // Leave the tile in its "down" state; the next keepAlive tick retries.
    setTileStatus(matchId, '重新拉起直播失败：' + error.message, 'warn');
  } finally {
    tile.restarting = false;
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
  // 上一轮还没返回（8901 卡顿时常见）就不再发第二个同游标请求，
  // 否则两个响应带回同一批事件，面板里就会出现重复的提醒。
  if (state.alertPolling) return;
  state.alertPolling = true;
  let data;
  try {
    data = await requestJson(`api/monitor/events?since=${state.alertSeq}`);
  } catch (error) {
    return;
  } finally {
    state.alertPolling = false;
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
    // 游标之内的事件已经渲染过，再收到一次（并发轮询/重复响应）直接丢弃。
    if (typeof ev.seq === 'number' && ev.seq <= state.alertSeq) continue;
    pushAlert(ev);
    offerAlertToClipForms(ev);
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
  if (video && video._whep) {
    const lag = video._whep.lagSeconds;
    return typeof lag === 'number' && lag > 0 ? lag : null;
  }
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
  node.textContent = `落后 ${lag < 1 ? lag.toFixed(1) : Math.round(lag)}s`;
  node.className = `lag-badge ${cls}`;
}

// ---- Auto catch-up (BACKLOG 11.8 rules 1/4 + tile speed-up) ----
//
// Lag accumulates: every stall leaves the playhead permanently further behind
// because nothing pulls it forward again. Three mechanisms, by severity:
//
// * Tile speed-up: a grid tile has no scrub bar, so any lag there is
//   involuntary. Above CATCHUP_RATE_MIN_LAG we run it at CATCHUP_RATE until it
//   is back near the hls.js target, then restore 1x. Never applied to the
//   stage video: the operator may have scrubbed back deliberately to capture.
//   (hls.js has maxLiveSyncPlaybackRate, but in 1.5.x it only engages with
//   lowLatencyMode on, which changes other loading behaviour we do not want.)
//   Own switch: GIF_MONITOR_CATCHUP_RATE_ENABLED.
// * Tile instant jump: at TILE_SEEK_LAG (0 = off) a tile seeks straight to
//   live instead of waiting for the speed-up to close the gap. Timeliness
//   over continuity; the stage is excluded for the same reason as above.
// * Stage jump: at STAGE_AUTO_SEEK_LAG (0 = off) the stage seeks too, but
//   only when nobody is working there - no range pin, not paused, and no
//   manual scrub-back/pause within STAGE_USER_GRACE_SECONDS (see
//   stageAutoSeekBlocker). Rules 1/4 below still apply to the stage.
// * Rule 1: when the tab returns to the foreground, seek tiles to live
//   (Chrome throttles hidden tabs so they fall minutes behind). The stage only
//   seeks when it is clearly adrift (> STAGE_FOREGROUND_SEEK_LAG) so a
//   deliberate scrub survives a quick glance at another window.
// * Rule 4: at HARD_SEEK_LAG, seek unconditionally - 30 s later the server has
//   deleted that part of the 180 s ring and hls.js would hard-jump on its own.
//
// All thresholds (and hls.js liveSyncDurationCount) are tunable from the
// server .env; the LATENCY object (declared near the top of this file) only
// holds defaults until /api/monitor/matches delivers the effective set.
let latencyConfigLogged = false;

function applyLatencyConfig(cfg) {
  if (!cfg || typeof cfg !== 'object') return;
  const num = (v, prev) => (typeof v === 'number' && v > 0 ? v : prev);
  LATENCY.hlsLiveSyncCount = num(cfg.hls_live_sync_count, LATENCY.hlsLiveSyncCount);
  if (typeof cfg.catchup_rate_enabled === 'boolean') LATENCY.catchupRateEnabled = cfg.catchup_rate_enabled;
  LATENCY.catchupRate = num(cfg.catchup_rate, LATENCY.catchupRate);
  LATENCY.catchupMinLag = num(cfg.catchup_min_lag, LATENCY.catchupMinLag);
  // 0 is a legitimate value here (= off), so do not route it through num().
  if (typeof cfg.tile_seek_lag === 'number' && cfg.tile_seek_lag >= 0) LATENCY.tileSeekLag = cfg.tile_seek_lag;
  if (typeof cfg.stage_auto_seek_lag === 'number' && cfg.stage_auto_seek_lag >= 0) LATENCY.stageAutoSeekLag = cfg.stage_auto_seek_lag;
  if (typeof cfg.stage_user_grace_seconds === 'number' && cfg.stage_user_grace_seconds >= 0) LATENCY.stageUserGraceSeconds = cfg.stage_user_grace_seconds;
  LATENCY.stageSeekLag = num(cfg.stage_seek_lag, LATENCY.stageSeekLag);
  LATENCY.hardSeekLag = num(cfg.hard_seek_lag, LATENCY.hardSeekLag);
  if (!latencyConfigLogged) {
    latencyConfigLogged = true;
    console.log(
      `[LATENCY-CONFIG] 片长 ${cfg.segment_seconds}s × 同步 ${LATENCY.hlsLiveSyncCount} 片 = 目标 ${cfg.hls_target_seconds}s | ` +
      `回看 ${cfg.history_seconds}s | x264 ${cfg.x264_preset} | 追帧总开关 ${state.catchupEnabled ? '开' : '关'} | ` +
      `倍速 ${LATENCY.catchupRateEnabled ? `${LATENCY.catchupRate}x@≥${LATENCY.catchupMinLag}s` : '关'} | ` +
      `小窗跳回 ${LATENCY.tileSeekLag > 0 ? `≥${LATENCY.tileSeekLag}s` : '关'} | ` +
      `大窗跳回 ${LATENCY.stageAutoSeekLag > 0 ? `≥${LATENCY.stageAutoSeekLag}s（操作后豁免 ${LATENCY.stageUserGraceSeconds}s）` : '关'} | ` +
      `大窗前台≥${LATENCY.stageSeekLag}s 硬跳≥${LATENCY.hardSeekLag}s` +
      (Array.isArray(cfg.adjusted) && cfg.adjusted.length ? ` | 已被服务器修正: ${cfg.adjusted.join(', ')}` : '')
    );
  }
}
const AUTO_SEEK_NOTE_MS = 3000;

function isStageVideo(video) {
  return !!video && state.stageVideoNode === video;
}

function noteAutoSeek(matchId, reason, lag) {
  const text = `落后 ${Math.round(lag)}s，已自动回到直播`;
  console.log(`[AUTO-SEEK] ${matchId} | ${reason} | 落后 ${lag.toFixed(1)}s`);
  setTileStatus(matchId, text, 'warn', true);
  if (state.stageMatchId === matchId) {
    addStageNote(text, 'warn');
    const status = $('stage-status');
    if (status) {
      status.textContent = text;
      status.className = 'stage-status warn';
      setTimeout(() => {
        if (status.textContent === text) {
          status.textContent = '直播播放中';
          status.className = 'stage-status ok';
        }
      }, AUTO_SEEK_NOTE_MS);
    }
  }
}

function autoSeekToLive(matchId, video, reason, lag) {
  if (!state.catchupEnabled) return;
  if (!video || !video._hls || video._liveSeekPending) return;
  noteAutoSeek(matchId, reason, lag);
  seekToLive(video);
}

// Hold CATCHUP_RATE while a tile is adrift; drop back to 1x once it is within
// a segment of the target (or when it is moved into the stage / lag unknown).
function applyTileCatchupRate(video, lag) {
  if (!video) return;
  if (!state.catchupEnabled || !LATENCY.catchupRateEnabled) {
    // Switch flipped off at runtime: make sure nothing is left running fast.
    if (video.playbackRate !== 1) video.playbackRate = 1;
    return;
  }
  const hls = video._hls;
  const target = hls && typeof hls.targetLatency === 'number' && hls.targetLatency > 0
    ? hls.targetLatency
    : LATENCY.catchupMinLag;
  const adrift = lag != null && hls && !isStageVideo(video) && !video.paused
    && lag >= LATENCY.catchupMinLag && bufferedAheadSeconds(video) > 1;
  const settled = lag == null || lag <= target + 1;
  if (adrift && video.playbackRate !== LATENCY.catchupRate) {
    video.playbackRate = LATENCY.catchupRate;
  } else if (!adrift && video.playbackRate !== 1 && (settled || isStageVideo(video) || !hls)) {
    video.playbackRate = 1;
  }
}

function onVisibilityRestored() {
  if (document.hidden || !state.catchupEnabled) return;
  for (const matchId of state.tiles.keys()) {
    const video = videoForMatch(matchId);
    if (!video || !video._hls) continue;
    const lag = liveLagSeconds(video);
    if (isStageVideo(video) && !(lag != null && lag > LATENCY.stageSeekLag)) continue;
    autoSeekToLive(matchId, video, 'foreground', lag == null ? 0 : lag);
  }
}

document.addEventListener('visibilitychange', onVisibilityRestored);

function startLagMonitoring() {
  if (lagMonitoringStarted) return;
  lagMonitoringStarted = true;

  console.log('[LAG-MONITOR] 滞后监控已启动，每 1 秒记录一次');

  setInterval(() => {
    for (const matchId of state.tiles.keys()) {
      // Badge each surface from its own player: with a WebRTC tile the stage
      // runs a separate HLS element, so the two lags differ. On HLS the tile
      // element itself moves into the stage, so fall back to that element to
      // keep the tile badge populated exactly as before.
      // videoForMatch() follows the element into the stage when it is enlarged.
      const video = videoForMatch(matchId);
      const tileVideo = tileVideoEl(matchId) || video;
      const tileBadge = document.querySelector(`.tile[data-match-id="${matchId}"] [data-role="lag"]`);
      renderLagBadge(tileBadge, liveLagSeconds(tileVideo));
      if (tileVideo && tileVideo._whep) {
        const w = tileVideo._whep;
        console.log(
          `[LAG] ${matchId} | WebRTC 落后 ${w.lagSeconds == null ? 'N/A' : w.lagSeconds.toFixed(2) + 's'} | ` +
          `${w.pc.connectionState} | ${document.hidden ? '后台' : '前台'} | frames ${w.framesDecoded}`
        );
      }
      const lag = liveLagSeconds(video);
      if (state.stageMatchId === matchId) renderLagBadge($('stage-lag'), lag);
      if (!video || !video._hls) continue;
      const hls = video._hls;

      if (lag != null && lag >= LATENCY.hardSeekLag) {
        autoSeekToLive(matchId, video, 'hard-limit', lag);
      } else if (lag != null && LATENCY.tileSeekLag > 0 && lag >= LATENCY.tileSeekLag && !isStageVideo(video)) {
        // Tile-only instant jump (GIF_MONITOR_TILE_SEEK_LAG).
        autoSeekToLive(matchId, video, 'tile-limit', lag);
      } else if (lag != null && LATENCY.stageAutoSeekLag > 0 && lag >= LATENCY.stageAutoSeekLag && isStageVideo(video)) {
        // Stage jump (GIF_MONITOR_STAGE_AUTO_SEEK_LAG), unless the operator is
        // working there: range pin set, paused, or a manual action within grace.
        const blocker = stageAutoSeekBlocker(video);
        if (blocker) {
          if (!video._stageHoldLogged) {
            video._stageHoldLogged = true;
            console.log(`[AUTO-SEEK] ${matchId} | stage-hold (${blocker}) | 落后 ${lag.toFixed(1)}s，大窗有人在用，不跳`);
          }
        } else {
          video._stageHoldLogged = false;
          autoSeekToLive(matchId, video, 'stage-limit', lag);
        }
      } else {
        if (video._stageHoldLogged) video._stageHoldLogged = false;
        applyTileCatchupRate(video, lag);
      }

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
      const rate = video.playbackRate !== 1 ? ` | ${video.playbackRate}x追帧` : '';

      console.log(
        `[LAG] ${matchId} | 落后直播 ${latency} (目标 ${target}) | ` +
        `PDT差 ${pdtDiff} | ${videoState} | ${pageVisible} | ` +
        `本段缓冲 ${bufferedAheadSeconds(video).toFixed(0)}s${rate}`
      );
    }
  }, 1000);
}

pollAlerts();
setInterval(pollAlerts, ALERT_POLL_MS);