'use strict';
const $ = (id) => document.getElementById(id);
const REFRESH_MS = 30000;

const state = {
  tiles: new Map(),      // match_id -> {data, hls, playlistReady, statusTimer}
  stageMatchId: null,
  stageHls: null,
  maxPreviews: 16,
  clipBeforeSeconds: 15,
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
  if (state.stageMatchId === matchId) {
    const stageVideo = $('stage-video');
    if (stageVideo) return stageVideo;
  }
  return document.querySelector(`.tile[data-match-id="${matchId}"] .tile-video`);
}

// Jump the playhead to the live edge so a capture reflects the newest frame
// instead of a position the operator has drifted behind.
function seekToLive(video) {
  if (!video) return;
  const hls = video._hls;
  if (hls && typeof hls.liveSyncPosition === 'number' && isFinite(hls.liveSyncPosition)) {
    video.currentTime = hls.liveSyncPosition;
  } else if (video.seekable && video.seekable.length) {
    video.currentTime = video.seekable.end(video.seekable.length - 1);
  }
  video.play().catch(() => {});
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
        // A live source that never delivers (dead RTMP) would otherwise loop
        // "reconnecting" forever; after a few tries, surface a real failure.
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

function setTileStatus(matchId, text, cls = '') {
  const node = document.querySelector(`.tile[data-match-id="${matchId}"] .tile-status`);
  if (node) { node.textContent = text; node.className = `tile-status ${cls}`; }
}

function createTile(data) {
  const el = document.createElement('article');
  el.className = 'tile';
  el.dataset.matchId = data.match_id;
  el.innerHTML = `
    <div class="tile-video-wrap" data-role="enlarge">
      <span class="tile-badge">${escapeHtml(data.status_label || '进行中')}</span>
      <span class="tile-expand">点击放大</span>
      <video class="tile-video" playsinline autoplay muted></video>
      <div class="tile-overlay">正在连接直播源…</div>
    </div>
    <div class="tile-body">
      <div class="tile-teams">${teamsText(data)}</div>
      <div class="tile-meta">${metaText(data)}</div>
      <div class="tile-actions">
        <button class="button primary" data-role="capture" type="button">截取 ${state.clipBeforeSeconds} 秒</button>
        <span class="tile-status">准备中…</span>
      </div>
    </div>`;
  el.querySelector('[data-role="enlarge"]').addEventListener('click', () => openStage(data.match_id));
  el.querySelector('[data-role="capture"]').addEventListener('click', (ev) => {
    ev.stopPropagation();
    captureClip(data.match_id, ev.currentTarget);
  });
  return el;
}

function updateTileInfo(el, data) {
  el.querySelector('.tile-teams').innerHTML = teamsText(data);
  el.querySelector('.tile-meta').innerHTML = metaText(data);
  const badge = el.querySelector('.tile-badge');
  if (badge) badge.textContent = data.status_label || '进行中';
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
    const data = await requestJson('/api/monitor/preview/start', {
      method: 'POST', body: JSON.stringify({match_id: matchId}),
    });
    tile.playlistUrl = data.playlist_url;
    overlay.classList.add('hidden');
    attachHls(video, data.playlist_url, (text, cls) => {
      setTileStatus(matchId, text, cls);
      // Surface a terminal playback failure on the tile instead of a blank frame.
      if (cls === 'fail') {
        overlay.classList.remove('hidden');
        overlay.textContent = '直播源不可用（源未推流）';
      }
    });
    setTileStatus(matchId, data.playlist_ready ? '直播加载中…' : '直播源连接中…', 'ok');
  } catch (error) {
    overlay.classList.remove('hidden');
    const noSource = /直播源/.test(error.message);
    overlay.textContent = noSource ? '该比赛暂无直播源' : '直播源加载失败';
    setTileStatus(matchId, error.message, 'warn');
  }
}

async function captureClip(matchId, button) {
  const prevLabel = button ? button.textContent : '';
  if (button) { button.disabled = true; button.textContent = '截取中…'; }
  setTileStatus(matchId, '正在回捞最近画面…');
  try {
    const anchorMs = frameAnchorMs(videoForMatch(matchId));
    const payload = {match_id: matchId};
    if (anchorMs != null) payload.anchor_epoch_ms = Math.round(anchorMs);
    const data = await requestJson('/api/monitor/clip', {
      method: 'POST', body: JSON.stringify(payload),
    });
    const sizeMb = (data.bytes / (1024 * 1024)).toFixed(2);
    const node = document.querySelector(`.tile[data-match-id="${matchId}"] .tile-status`);
    // anchored=false means the server fell back to the buffer tail (no frame
    // timestamp), so the moment may be off — make that visible, not silent.
    const warn = data.anchored === false;
    if (node) {
      node.className = `tile-status ${warn ? 'warn' : 'ok'}`;
      const prefix = warn ? '⚠ 未锚定，画面可能不准 · ' : '已生成 GIF';
      node.innerHTML = `${prefix}（${sizeMb} MB） <a class="tile-clip-link" href="${data.clip_url}" target="_blank" rel="noreferrer">打开</a>`;
    }
    if (state.stageMatchId === matchId) addStageClip(data.clip_url, sizeMb, warn);
  } catch (error) {
    setTileStatus(matchId, error.message, 'warn');
  } finally {
    if (button) { button.disabled = false; button.textContent = prevLabel; }
  }
}

function addStageClip(url, sizeMb, warn) {
  const wrap = $('stage-clips');
  const link = document.createElement('a');
  link.href = url; link.target = '_blank'; link.rel = 'noreferrer';
  const mark = warn ? '⚠ 未锚定 ' : '';
  link.textContent = `${mark}GIF ${sizeMb} MB · ${new Date().toLocaleTimeString()}`;
  wrap.prepend(link);
}

function openStage(matchId) {
  const tile = state.tiles.get(matchId);
  if (!tile || !tile.playlistUrl) { showNotice('该比赛直播还没准备好', 'warning'); return; }
  state.stageMatchId = matchId;
  const stage = $('stage');
  stage.classList.remove('hidden');
  $('stage-title').textContent = (tile.data.team_A_name || '主队') + ' vs ' + (tile.data.team_B_name || '客队');
  $('stage-status').textContent = '直播播放中';
  $('stage-status').className = 'stage-status ok';
  $('stage-clips').innerHTML = '';
  const video = $('stage-video');
  state.stageHls = attachHls(video, tile.playlistUrl, (text, cls) => {
    $('stage-status').textContent = text;
    $('stage-status').className = `stage-status ${cls || ''}`;
  });
}

function closeStage() {
  state.stageMatchId = null;
  const video = $('stage-video');
  if (video._hls) { try { video._hls.destroy(); } catch (e) {} video._hls = null; }
  try { video.pause(); } catch (e) {}
  video.removeAttribute('src');
  try { video.load(); } catch (e) {}
  $('stage').classList.add('hidden');
}

function reconcileTiles(playing) {
  const grid = $('grid');
  const seen = new Set();
  const limited = playing.slice(0, state.maxPreviews);
  limited.forEach((data) => {
    if (!data.match_id) return;
    seen.add(data.match_id);
    let tile = state.tiles.get(data.match_id);
    if (!tile) {
      const el = createTile(data);
      grid.appendChild(el);
      tile = {data, playlistUrl: null};
      state.tiles.set(data.match_id, tile);
      startTilePreview(data.match_id);
    } else {
      tile.data = data;
      const el = document.querySelector(`.tile[data-match-id="${data.match_id}"]`);
      if (el) updateTileInfo(el, data);
    }
  });
  for (const matchId of Array.from(state.tiles.keys())) {
    if (seen.has(matchId)) continue;
    const el = document.querySelector(`.tile[data-match-id="${matchId}"]`);
    if (el) {
      const video = el.querySelector('.tile-video');
      if (video && video._hls) { try { video._hls.destroy(); } catch (e) {} }
      el.remove();
    }
    fetch('/api/monitor/preview/stop', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({match_id: matchId}),
    }).catch(() => {});
    if (state.stageMatchId === matchId) closeStage();
    state.tiles.delete(matchId);
  }
  $('grid-empty').classList.toggle('hidden', state.tiles.size > 0);
  if (state.tiles.size === 0) $('grid-empty').textContent = '当前没有进行中的比赛';
}

async function refreshMatches() {
  const health = $('health');
  try {
    const data = await requestJson('/api/monitor/matches');
    state.maxPreviews = data.max_previews || state.maxPreviews;
    state.clipBeforeSeconds = data.clip_before_seconds || state.clipBeforeSeconds;
    const playing = Array.isArray(data.playing) ? data.playing : [];
    reconcileTiles(playing);
    const shown = Math.min(playing.length, state.maxPreviews);
    $('summary').textContent = `进行中 ${playing.length} 场 · 正在监看 ${shown} 场`;
    const h = data.health || {};
    health.className = `health ${h.state === 'healthy' ? 'ok' : h.state === 'error' ? 'error' : 'loading'}`;
    health.innerHTML = `<i></i>${escapeHtml(h.label || '赛事目录')}`;
    showNotice('');
  } catch (error) {
    health.className = 'health error';
    health.innerHTML = '<i></i>接口不可用';
    showNotice(`无法读取比赛列表：${error.message}`, 'error');
  }
}

$('refresh-btn').addEventListener('click', refreshMatches);
$('stage-close').addEventListener('click', closeStage);
$('stage-capture').addEventListener('click', (ev) => {
  if (state.stageMatchId) captureClip(state.stageMatchId, ev.currentTarget);
});
$('stage-live').addEventListener('click', () => {
  seekToLive($('stage-video'));
  $('stage-status').textContent = '已回到直播';
  $('stage-status').className = 'stage-status ok';
});
document.addEventListener('keydown', (ev) => { if (ev.key === 'Escape') closeStage(); });
window.addEventListener('beforeunload', () => {
  for (const matchId of state.tiles.keys()) {
    navigator.sendBeacon?.('/api/monitor/preview/stop', new Blob([JSON.stringify({match_id: matchId})], {type: 'application/json'}));
  }
});

function keepAlivePreviews() {
  for (const [matchId, tile] of state.tiles.entries()) {
    if (!tile.playlistUrl) continue;
    fetch(`/api/monitor/preview/status?match_id=${encodeURIComponent(matchId)}`).catch(() => {});
  }
}

refreshMatches();
setInterval(refreshMatches, REFRESH_MS);
setInterval(keepAlivePreviews, 25000);
