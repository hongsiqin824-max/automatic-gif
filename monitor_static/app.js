'use strict';
const $ = (id) => document.getElementById(id);
const REFRESH_MS = 30000;

const state = {
  tiles: new Map(),      // match_id -> {data, hls, playlistReady, statusTimer}
  stageMatchId: null,
  stageHls: null,
  maxPreviews: 16,
  clipBeforeSeconds: 10,
  draftPoll: null,       // {matchId, timer, polls} while awaiting draft links
  tileDraftTimer: null,  // single poll loop for grid-capture draft links
  tileDraftPolls: 0,
  alertSeq: 0,           // cursor: last event seq the page has rendered
  alertBootId: null,     // server boot id; a change means 8901 restarted
  alertInit: false,      // skip replaying history on the first poll
  alertCount: 0,         // unseen-alert badge count
  alertPanelOpen: false, // panel is collapsed to a launcher pill by default
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
        // Mark the tile down so keepAlivePreviews re-attaches hls once the
        // backend (which keeps auto-restarting ffmpeg) is serving again.
        // Without this, a tile that failed via the hls error path never
        // self-heals and only recovers when manually enlarged.
        tile.down = true;
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
    // Optional operator fields only exist while the stage of this match is open.
    if (state.stageMatchId === matchId) {
      const personEl = $('stage-person');
      const titleEl = $('stage-title-input');
      const secondsEl = $('stage-seconds');
      if (personEl && personEl.value.trim()) payload.person = personEl.value.trim();
      if (titleEl && titleEl.value.trim()) payload.title = titleEl.value.trim();
      if (secondsEl && secondsEl.value.trim()) {
        const secs = Number(secondsEl.value);
        if (Number.isFinite(secs) && secs > 0 && secs <= 120) payload.before_seconds = secs;
      }
    }
    const data = await requestJson('/api/monitor/clip', {
      method: 'POST', body: JSON.stringify(payload),
    });
    const sizeMb = (data.bytes / (1024 * 1024)).toFixed(2);
    const node = document.querySelector(`.tile[data-match-id="${matchId}"] .tile-status`);
    // anchored=false means the server fell back to the buffer tail (no frame
    // timestamp), so the moment may be off — make that visible, not silent.
    const warn = data.anchored === false;
    const publishNote = data.publish_error
      ? ' · 发文失败：' + data.publish_error
      : data.draft
      ? ' · 已建草稿'
      : '';
    if (node) {
      node.className = `tile-status ${warn ? 'warn' : 'ok'}`;
      const prefix = warn ? '⚠ 未锚定，画面可能不准 · ' : '已生成 GIF';
      node.innerHTML = `${prefix}（${sizeMb} MB）${escapeHtml(publishNote)} <a class="tile-clip-link" href="${data.clip_url}" target="_blank" rel="noreferrer">打开</a>`;
    }
    if (state.stageMatchId === matchId) {
      addStageClip(data.clip_url, sizeMb, warn, data);
      // A draft is created asynchronously; poll until its admin link is ready.
      if (data.draft && data.draft.event_key) startDraftPoll(matchId);
    } else if (node && data.draft && data.draft.event_key) {
      // Grid capture (stage closed): the draft is still built asynchronously,
      // so remember which event this status line is waiting on and poll until
      // its admin link lands, mirroring the stage path.
      node.dataset.pendingDraft = data.draft.event_key;
      startTileDraftPoll();
    }
  } catch (error) {
    setTileStatus(matchId, error.message, 'warn');
    // In stage (enlarged) mode the tile status is hidden behind the big player,
    // so echo the failure where the operator is actually looking.
    if (state.stageMatchId === matchId) addStageNote('截取失败：' + error.message, 'fail');
  } finally {
    if (button) { button.disabled = false; button.textContent = prevLabel; }
  }
}

function addStageNote(text, cls) {
  const wrap = $('stage-clips');
  if (!wrap) return;
  const note = document.createElement('div');
  note.className = `stage-note ${cls || ''}`;
  note.textContent = `${text} · ${new Date().toLocaleTimeString()}`;
  wrap.prepend(note);
}

function addStageClip(url, sizeMb, warn, data) {
  const wrap = $('stage-clips');
  if (!wrap) return;
  // Show the GIF itself (it autoplays) plus an async draft-link slot, so the
  // operator sees the capture immediately and can jump to the draft once ready.
  const card = document.createElement('div');
  card.className = 'stage-clip-card';
  const eventKey = data && data.draft && data.draft.event_key;
  if (eventKey) card.dataset.eventKey = eventKey;

  const gifLink = document.createElement('a');
  gifLink.href = url; gifLink.target = '_blank'; gifLink.rel = 'noreferrer';
  const img = document.createElement('img');
  img.src = url; img.alt = '截取的 GIF';
  gifLink.appendChild(img);

  const label = document.createElement('span');
  label.className = 'stage-clip-label';
  const mark = warn ? '⚠ 未锚定 ' : '';
  label.textContent = `${mark}${sizeMb} MB · ${new Date().toLocaleTimeString()}`;

  const draftEl = document.createElement('div');
  draftEl.className = 'stage-clip-draft';
  if (data && data.publish_error) {
    draftEl.textContent = '发文失败：' + data.publish_error;
    draftEl.classList.add('fail');
  } else if (eventKey) {
    draftEl.textContent = '草稿生成中…';
  } else {
    draftEl.textContent = '仅本地保存';
  }

  card.appendChild(gifLink);
  card.appendChild(label);
  card.appendChild(draftEl);
  wrap.prepend(card);
  // Pull the newest capture into view inside the scrollable stage panel.
  card.scrollIntoView({block: 'nearest'});
}

// Poll the publish status until each pending draft gets an admin link (or fails),
// then stop. Only runs while the owning match is on stage.
function startDraftPoll(matchId) {
  if (state.draftPoll && state.draftPoll.matchId === matchId) return;
  stopDraftPoll();
  state.draftPoll = {matchId, timer: setInterval(() => refreshDraftLinks(matchId), 2000), polls: 0};
  refreshDraftLinks(matchId);
}

function stopDraftPoll() {
  if (state.draftPoll) { clearInterval(state.draftPoll.timer); state.draftPoll = null; }
}

function refreshDraftLinks(matchId) {
  if (state.stageMatchId !== matchId) { stopDraftPoll(); return; }
  if (state.draftPoll) {
    state.draftPoll.polls += 1;
    if (state.draftPoll.polls > 90) {  // give up after ~3min (90 × 2s)
      // Say so instead of freezing on "草稿生成中…": the backend may still be
      // working, the operator just has to check the console from here on.
      document.querySelectorAll('#stage-clips .stage-clip-draft').forEach((draftEl) => {
        if (draftEl.classList.contains('ok') || draftEl.classList.contains('fail')) return;
        draftEl.textContent = '草稿状态未知，请到发文后台查看';
        draftEl.className = 'stage-clip-draft warn';
      });
      stopDraftPoll();
      return;
    }
  }
  fetch(`/api/monitor/publish/status?match_id=${encodeURIComponent(matchId)}`)
    .then((response) => (response.ok ? response.json() : null))
    .then((data) => {
      if (!data || !data.enabled || !data.records) return;
      const cards = document.querySelectorAll('#stage-clips .stage-clip-card[data-event-key]');
      let pending = 0;
      cards.forEach((card) => {
        const rec = data.records[card.dataset.eventKey];
        const draftEl = card.querySelector('.stage-clip-draft');
        if (!rec || !draftEl) { pending += 1; return; }
        // Both 'drafted' and 'published' are terminal backend statuses (see
        // TERMINAL_DELIVERY_STATUSES in article_draft_queue.py). draft mode is
        // the default, so only matching 'published' left its link unreachable.
        if ((rec.status === 'published' || rec.status === 'drafted') && rec.draft_url) {
          const label = rec.status === 'published' ? '打开文章 ↗' : '打开草稿 ↗';
          draftEl.innerHTML = `<a href="${rec.draft_url}" target="_blank" rel="noreferrer">${label}</a>`;
          draftEl.className = 'stage-clip-draft ok';
        } else if (rec.status === 'failed' && !rec.retriable) {
          draftEl.textContent = '草稿失败：' + (rec.error || '未知错误');
          draftEl.className = 'stage-clip-draft fail';
        } else if (rec.status === 'failed') {
          // Retriable failure: still pending, but say why the wait is long.
          draftEl.textContent = '草稿重试中：' + (rec.error || '未知错误');
          draftEl.className = 'stage-clip-draft';
          pending += 1;
        } else {
          draftEl.textContent = '草稿生成中…';
          draftEl.className = 'stage-clip-draft';
          pending += 1;
        }
      });
      if (pending === 0) stopDraftPoll();
    })
    .catch(() => {});
}

// Grid-capture drafts are polled by a single shared loop keyed on the
// pendingDraft marker each tile status line carries. Only one timer runs; it
// stops itself once every waiting line is resolved or gives up.
function startTileDraftPoll() {
  state.tileDraftPolls = 0;  // reset the give-up window on each new capture
  if (state.tileDraftTimer) return;
  state.tileDraftTimer = setInterval(refreshTileDraftLinks, 2000);
  refreshTileDraftLinks();
}

function stopTileDraftPoll() {
  if (state.tileDraftTimer) { clearInterval(state.tileDraftTimer); state.tileDraftTimer = null; }
}

function refreshTileDraftLinks() {
  const nodes = document.querySelectorAll('.tile-status[data-pending-draft]');
  if (!nodes.length) { stopTileDraftPoll(); return; }
  state.tileDraftPolls += 1;
  if (state.tileDraftPolls > 90) {  // give up after ~3min, same as the stage poll (90 × 2s)
    nodes.forEach((node) => { delete node.dataset.pendingDraft; });
    stopTileDraftPoll();
    return;
  }
  // Each distinct match only needs one status request per tick.
  const matchIds = new Set();
  nodes.forEach((node) => {
    const tileEl = node.closest('.tile');
    if (tileEl && tileEl.dataset.matchId) matchIds.add(tileEl.dataset.matchId);
  });
  matchIds.forEach((matchId) => {
    fetch(`/api/monitor/publish/status?match_id=${encodeURIComponent(matchId)}`)
      .then((response) => (response.ok ? response.json() : null))
      .then((data) => {
        if (!data || !data.enabled || !data.records) return;
        const node = document.querySelector(`.tile[data-match-id="${matchId}"] .tile-status[data-pending-draft]`);
        if (!node) return;
        const rec = data.records[node.dataset.pendingDraft];
        if (!rec) return;
        // 'drafted' and 'published' are both terminal (TERMINAL_DELIVERY_STATUSES
        // in article_draft_queue.py); append the admin link to the status line.
        if ((rec.status === 'published' || rec.status === 'drafted') && rec.draft_url) {
          const label = rec.status === 'published' ? '打开文章 ↗' : '打开草稿 ↗';
          const link = document.createElement('a');
          link.className = 'tile-clip-link';
          link.href = rec.draft_url; link.target = '_blank'; link.rel = 'noreferrer';
          link.textContent = label;
          node.appendChild(document.createTextNode(' · '));
          node.appendChild(link);
          delete node.dataset.pendingDraft;
        } else if (rec.status === 'failed' && !rec.retriable) {
          node.appendChild(document.createTextNode(' · 草稿失败'));
          delete node.dataset.pendingDraft;
        }
      })
      .catch(() => {});
  });
}

function stageCaptureSeconds() {
  // Mirror exactly what captureClip() will send: an out-of-range or unparsable
  // value is dropped there, so the backend falls back to its own default.
  const secondsEl = $('stage-seconds');
  if (secondsEl && secondsEl.value.trim()) {
    const secs = Number(secondsEl.value);
    if (Number.isFinite(secs) && secs > 0 && secs <= 120) return secs;
  }
  return state.clipBeforeSeconds;
}

function syncStageCaptureLabel() {
  const button = $('stage-capture');
  // Leave the transient '截取中…' label alone; captureClip restores it itself.
  if (!button || button.disabled) return;
  button.textContent = `截取最近 ${stageCaptureSeconds()} 秒`;
}

function openStage(matchId) {
  const tile = state.tiles.get(matchId);
  if (!tile || !tile.playlistUrl) { showNotice('该比赛直播还没准备好', 'warning'); return; }
  state.stageMatchId = matchId;
  stopDraftPoll();
  const stage = $('stage');
  stage.classList.remove('hidden');
  $('stage-title').textContent = (tile.data.team_A_name || '主队') + ' vs ' + (tile.data.team_B_name || '客队');
  $('stage-status').textContent = '直播播放中';
  $('stage-status').className = 'stage-status ok';
  $('stage-clips').innerHTML = '';
  // Reset optional fields so a previous match's name/title never leak over.
  const personInput = $('stage-person');
  const titleInput = $('stage-title-input');
  const secondsInput = $('stage-seconds');
  if (personInput) personInput.value = '';
  if (titleInput) titleInput.value = '';
  if (secondsInput) secondsInput.value = '';
  syncStageCaptureLabel();
  const video = $('stage-video');
  state.stageHls = attachHls(video, tile.playlistUrl, (text, cls) => {
    $('stage-status').textContent = text;
    $('stage-status').className = `stage-status ${cls || ''}`;
  });
}

function closeStage() {
  state.stageMatchId = null;
  stopDraftPoll();
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
    // Keep the two hard-codable labels honest once the backend default is known.
    const subheadSeconds = $('subhead-seconds');
    if (subheadSeconds) subheadSeconds.textContent = String(state.clipBeforeSeconds);
    syncStageCaptureLabel();
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
$('stage-seconds').addEventListener('input', syncStageCaptureLabel);
$('stage-live').addEventListener('click', () => {
  seekToLive($('stage-video'));
  $('stage-status').textContent = '已回到直播';
  $('stage-status').className = 'stage-status ok';
});
document.addEventListener('keydown', (ev) => {
  if (ev.key !== 'Escape') return;
  if (state.alertPanelOpen) { closeAlertPanel(); return; }
  closeStage();
});
window.addEventListener('beforeunload', () => {
  for (const matchId of state.tiles.keys()) {
    navigator.sendBeacon?.('/api/monitor/preview/stop', new Blob([JSON.stringify({match_id: matchId})], {type: 'application/json'}));
  }
});

// A stopped ffmpeg (running:false) means the live source died; the browser may
// still show stale buffered frames, so cover them and say so plainly. The
// backend keeps auto-restarting (fast retries, then low-frequency probing), so
// it always phrases as reconnecting — the source self-heals when it comes back.
function markPreviewDown(matchId, cooling) {
  const tile = state.tiles.get(matchId);
  if (tile) tile.down = true;
  const message = cooling ? '直播源中断，后台持续重连中…' : '直播源中断，正在自动重连…';
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

// The backend restarted ffmpeg and a fresh playlist is ready; re-attach the
// player (segments reset to seg_00000) and clear the "down" overlay.
function recoverPreview(matchId) {
  const tile = state.tiles.get(matchId);
  if (!tile) return;
  tile.down = false;
  const el = document.querySelector(`.tile[data-match-id="${matchId}"]`);
  if (el) {
    const overlay = el.querySelector('.tile-overlay');
    if (overlay) overlay.classList.add('hidden');
    const video = el.querySelector('.tile-video');
    if (video) {
      attachHls(video, tile.playlistUrl, (text, cls) => {
        setTileStatus(matchId, text, cls);
        if (cls === 'fail' && overlay) {
          overlay.classList.remove('hidden');
          overlay.textContent = '直播源不可用（源未推流）';
          // Keep the tile "down" so the next keepAlive poll retries recovery.
          tile.down = true;
        }
      });
    }
  }
  setTileStatus(matchId, '直播源已恢复', 'ok');
  if (state.stageMatchId === matchId) {
    const stageVideo = $('stage-video');
    if (stageVideo) {
      state.stageHls = attachHls(stageVideo, tile.playlistUrl, (text, cls) => {
        $('stage-status').textContent = text;
        $('stage-status').className = `stage-status ${cls || ''}`;
      });
    }
    addStageNote('直播源已恢复', 'ok');
  }
}

function keepAlivePreviews() {
  for (const [matchId, tile] of state.tiles.entries()) {
    if (!tile.playlistUrl) continue;
    fetch(`/api/monitor/preview/status?match_id=${encodeURIComponent(matchId)}`)
      .then((response) => (response.ok ? response.json() : null))
      .then((status) => {
        const current = state.tiles.get(matchId);
        if (!current || !status) return;
        if (status.running === false) {
          markPreviewDown(matchId, status.auto_restart_cooling);
        } else if (status.running && status.playlist_ready && current.down) {
          recoverPreview(matchId);
        }
      })
      .catch(() => {});
  }
}

refreshMatches();
setInterval(refreshMatches, REFRESH_MS);
setInterval(keepAlivePreviews, 8000);

// ---- Real-time goal/red-card alerts (fed by the external push monitor) ----
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

// The panel is collapsed by default so it never covers the clickable grid.
// Opening it is a deliberate action, so a brief overlap is acceptable.
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

// Clicking an alert takes the operator to that match: enlarge it if its preview
// is ready, otherwise scroll it into view and flash it so they can start one.
// The badge is only cleared when the panel is opened (= the whole list is seen);
// locating one match from a toast must not zero out the other unread alerts.
function locateMatch(matchId) {
  closeAlertPanel();  // collapse so the panel never covers the tile/stage it points to
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
  void el.offsetWidth;  // restart the animation if it is already running
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
  addAlertItem(ev);
  showToast(ev);
  if (!state.alertPanelOpen) bumpAlertBadge();  // only count what the operator hasn't seen
  flashTile(ev.match_id);
}

async function pollAlerts() {
  let data;
  try {
    data = await requestJson(`/api/monitor/events?since=${state.alertSeq}`);
  } catch (error) {
    return;  // transient; next tick retries
  }
  // First successful poll only establishes the baseline cursor so a freshly
  // opened page does not replay every buffered alert as a flurry of toasts.
  if (!state.alertInit) {
    state.alertSeq = data.seq || 0;
    state.alertBootId = data.boot_id || null;
    state.alertInit = true;
    return;
  }
  // 8901 restarted (its in-memory seq reset to 0): re-baseline the cursor to the
  // new seq instead of staying stuck behind a cursor that can never be reached.
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

pollAlerts();
setInterval(pollAlerts, ALERT_POLL_MS);
