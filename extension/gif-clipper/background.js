// Service worker: the only place that talks to 8901 (its fetches carry the
// 8901 cookie; a content script's fetch would be sent as the live site and
// blocked by SameSite=Lax). Also: per-tab match binding, main-recording-frame
// election, keyboard commands → frame, side panel.

const DEFAULT_BASE = "https://matchgif.aisportsapp.com/gif-monitor/";
const CONFIG_TTL_MS = 60_000;
const MATCHES_TTL_MS = 60_000;

let configCache = { at: 0, value: null };
let matchesCache = { at: 0, value: null };

// ---- 8901 base URL / config --------------------------------------------

async function baseUrl() {
  const { baseUrl } = await chrome.storage.local.get("baseUrl");
  const raw = (baseUrl || DEFAULT_BASE).trim();
  return raw.endsWith("/") ? raw : raw + "/";
}

async function api(path, init = {}) {
  const url = (await baseUrl()) + path.replace(/^\//, "");
  const response = await fetch(url, { credentials: "include", ...init });
  let data = null;
  try {
    data = await response.json();
  } catch (_err) {
    data = null;
  }
  if (response.status === 401 && data && data.login_url) {
    const err = new Error("需要登录 8901");
    err.code = "login_required";
    err.loginUrl = new URL(data.login_url, url).href;
    throw err;
  }
  if (!response.ok) {
    const err = new Error((data && data.error) || `HTTP ${response.status}`);
    err.code = (data && data.code) || `http_${response.status}`;
    err.status = response.status;
    throw err;
  }
  return data;
}

async function getConfig(force = false) {
  if (!force && configCache.value && Date.now() - configCache.at < CONFIG_TTL_MS) {
    return configCache.value;
  }
  const value = await api("api/monitor/ext/config");
  configCache = { at: Date.now(), value };
  return value;
}

async function getMatches(force = false) {
  if (!force && matchesCache.value && Date.now() - matchesCache.at < MATCHES_TTL_MS) {
    return matchesCache.value;
  }
  const data = await api("api/monitor/matches");
  const playing = Array.isArray(data.playing) ? data.playing : [];
  // Same rule as the page: finished matches past the grace window are only
  // listed for their unpublished GIFs and can no longer be recorded.
  const recordable = playing.filter((m) => !(m.finished && m.grace_active === false));
  const toOption = (m, upcoming) => ({
    match_id: String(m.match_id),
    label: `${m.team_A_name || "?"} vs ${m.team_B_name || "?"}${m.competition_name ? `（${m.competition_name}）` : ""}`,
    score: upcoming ? `未开赛 ${kickoffLabel(m.start_play)}`.trim() : (m.score || ""),
    upcoming,
  });
  // Not-yet-started matches (kick-off within the catalog's upcoming window)
  // come after the live ones, so the operator can pick the match before the
  // teams walk out instead of waiting for it to flip to Playing.
  const upcoming = Array.isArray(data.upcoming) ? data.upcoming : [];
  const seen = new Set(recordable.map((m) => String(m.match_id)));
  const value = [
    ...recordable.map((m) => toOption(m, false)),
    ...upcoming.filter((m) => m && !seen.has(String(m.match_id))).map((m) => toOption(m, true)),
  ];
  matchesCache = { at: Date.now(), value };
  return value;
}

// ``start_play`` is "YYYY-MM-DD HH:MM:SS"; only the clock matters in a dropdown.
function kickoffLabel(startPlay) {
  const match = /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})/.exec(String(startPlay || ""));
  return match ? `${match[4]}:${match[5]}` : "";
}

// ---- per-tab state (storage.session survives SW restarts, not browser) ----

async function tabState(tabId) {
  const key = `tab:${tabId}`;
  const stored = (await chrome.storage.session.get(key))[key];
  return stored || { matchId: null, mainFrameId: null, shown: false };
}

// Read-modify-write per tab, one at a time: an election landing between the
// read and the write of another patch must not put back a stale field.
const tabStateWrites = new Map(); // tabId -> tail of that tab's write chain

function setTabState(tabId, patch) {
  const run = (tabStateWrites.get(tabId) || Promise.resolve()).then(async () => {
    const key = `tab:${tabId}`;
    const next = { ...(await tabState(tabId)), ...patch };
    await chrome.storage.session.set({ [key]: next });
    return next;
  });
  tabStateWrites.set(tabId, run.catch(() => null));
  return run;
}

// ---- bar + side panel shown per tab ---------------------------------------
// Both stay out of the way until the operator asks for them (toolbar icon or
// Alt+1/2/3); recording runs in the background either way, so the buffer is
// already full when they do. Mirrored in memory because sidePanel.open() has
// to be decided synchronously in the click / key handler: after an await
// Chrome no longer counts it as the user's gesture.

const shownTabs = new Set();
const shownTouched = new Set(); // changed since this worker started: storage copy is older
chrome.storage.session.get(null).then((all) => {
  for (const [key, value] of Object.entries(all || {})) {
    const tabId = Number(key.slice(4));
    if (key.startsWith("tab:") && value && value.shown && !shownTouched.has(tabId)) shownTabs.add(tabId);
  }
}).catch(() => null);

async function setShown(tabId, shown) {
  shownTouched.add(tabId);
  if (shown) shownTabs.add(tabId);
  else shownTabs.delete(tabId);
  await setTabState(tabId, { shown });
  // Every frame: the top-frame bar shows / hides, and the recording frame
  // (maybe a cross-origin iframe) only toasts on its own while shown.
  await chrome.tabs.sendMessage(tabId, { type: "ui-shown", shown }).catch(() => null);
}

// 8901's own pages are never recorded: it cuts its GIFs server-side, and its
// preview players would only cost CPU and memory here.
function isMonitorPage(pageUrl, base) {
  let page;
  let root;
  try {
    page = new URL(pageUrl);
    root = new URL(base);
  } catch (_err) {
    return false;
  }
  if (page.origin !== root.origin) return false;
  const prefix = root.pathname.replace(/\/+$/, "");
  return !prefix || page.pathname === prefix || page.pathname.startsWith(prefix + "/");
}

// ---- sticky crop per match (storage.local: survives reloads and restarts) ----
// entry = {crop, last, aspect, at}. crop {x,y,w,h} (0..1) or null = full
// frame (also what a match without an entry gets); last keeps the most recent
// box so switching back from full frame restores it; aspect = width/height of
// the video the box was drawn on, so a later stream with another shape is
// not cropped blindly.

async function getCrop(matchId) {
  if (!matchId) return null;
  const key = `crop:${matchId}`;
  return (await chrome.storage.local.get(key))[key] || null;
}

async function setCrop(matchId, crop, aspect) {
  if (!matchId) return null;
  const prev = (await getCrop(matchId)) || {};
  const next = {
    crop: crop || null,
    last: crop || prev.last || null,
    aspect: crop && aspect > 0 ? aspect : prev.aspect || null,
    at: Date.now(),
  };
  await chrome.storage.local.set({ [`crop:${matchId}`]: next });
  return next;
}

chrome.tabs.onRemoved.addListener((tabId) => {
  chrome.storage.session.remove(`tab:${tabId}`);
  frameReports.delete(tabId);
  shownTabs.delete(tabId);
  shownTouched.delete(tabId);
  tabStateWrites.delete(tabId);
});

// ---- main recording frame election ---------------------------------------
// Every frame with a playing <video> reports {area, playing}; the largest
// playing one records. Re-elected whenever a report changes.

const frameReports = new Map(); // tabId -> Map(frameId -> {area, playing, at})

async function electMainFrame(tabId) {
  const reports = frameReports.get(tabId);
  if (!reports || !reports.size) {
    await setTabState(tabId, { mainFrameId: null });
    return null;
  }
  let best = null;
  for (const [frameId, r] of reports) {
    if (!r.playing) continue;
    if (!best || r.area > best.area) best = { frameId, area: r.area };
  }
  const prev = (await tabState(tabId)).mainFrameId;
  const nextId = best ? best.frameId : null;
  if (prev !== nextId) {
    await setTabState(tabId, { mainFrameId: nextId });
    if (prev != null) sendToFrame(tabId, prev, { type: "recorder-stop" });
    if (nextId != null) {
      const config = await getConfig().catch(() => null);
      sendToFrame(tabId, nextId, {
        type: "recorder-start",
        bufferSeconds: (config && config.buffer_seconds) || 300,
        bitrate: (config && config.bitrate) || 2_000_000,
      });
    }
    broadcastBar(tabId, { type: "bar-source", frameId: nextId, area: best ? best.area : 0 });
  }
  return nextId;
}

function sendToFrame(tabId, frameId, msg) {
  return chrome.tabs.sendMessage(tabId, msg, { frameId }).catch(() => null);
}

function broadcastBar(tabId, msg) {
  // The bar lives only in the top frame (frameId 0).
  return sendToFrame(tabId, 0, msg);
}

// Where the fresh GIF landed, as one short line for the bar. The bar's status
// is a single line of plain text with only ok/warn/fail colouring, so the
// server's wording is boiled down to a verdict rather than passed through:
// the operator's eyes are on the video, and this line is the only thing that
// stays put (the toast disappears after a few seconds).
function clipLandingStatus(appended, name) {
  if (appended && appended.error) {
    return { text: "GIF 已生成，但未能追加，已单独保存", level: "warn" };
  }
  if (appended && appended.main) {
    const conflict = appended.push_state && appended.push_state.state === "conflict";
    return {
      text:
        `GIF 已生成，已追加为第 ${appended.gif_count} 张` +
        (conflict ? "；对应的推送和主图不是同一条" : ""),
      level: conflict ? "warn" : "ok",
    };
  }
  return { text: `GIF 已生成：${name}`, level: "ok" };
}

// ---- message router -------------------------------------------------------

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  const tabId = sender.tab && sender.tab.id;
  const frameId = sender.frameId;
  const handle = async () => {
    switch (msg.type) {
      case "frame-report": {
        if (tabId == null) return null;
        const playing = !!msg.playing && !isMonitorPage(sender.tab.url, await baseUrl());
        if (!frameReports.has(tabId)) frameReports.set(tabId, new Map());
        const reports = frameReports.get(tabId);
        if (playing) reports.set(frameId, { area: msg.area, playing: true, at: Date.now() });
        else reports.delete(frameId);
        await electMainFrame(tabId);
        return { ok: true };
      }
      case "get-tab-state":
        return tabId == null ? null : await tabState(tabId);
      case "set-match": {
        if (tabId == null) return null;
        await setTabState(tabId, { matchId: msg.matchId || null });
        await chrome.sidePanel.setOptions({ tabId, path: await panelPath(msg.matchId) });
        return { ok: true };
      }
      case "set-shown":
        // The bar's × (hide): recording keeps going, only the UI goes away.
        if (tabId == null) return null;
        await setShown(tabId, !!msg.shown);
        return { ok: true };
      case "bar-relay":
        // A recording frame (maybe a cross-origin iframe) wants the top-frame bar updated.
        if (tabId != null && msg.payload) broadcastBar(tabId, msg.payload);
        return { ok: true };
      case "recorder-status": {
        if (tabId == null) return null;
        const state = await tabState(tabId);
        if (state.mainFrameId == null) return { elected: false, recording: false };
        return (await sendToFrame(tabId, state.mainFrameId, { type: "recorder-status" })) || { elected: false, recording: false };
      }
      case "command": {
        // The bar's buttons go through the same path as the keyboard commands.
        if (tabId == null) return null;
        await runCommand(msg.command, tabId);
        return { ok: true };
      }
      case "get-config":
        return await getConfig(!!msg.force);
      case "get-matches":
        return await getMatches(!!msg.force);
      case "get-base-url":
        return await baseUrl();
      case "get-crop":
        return await getCrop(msg.matchId);
      case "set-crop":
        return await setCrop(msg.matchId, msg.crop, msg.aspect);
      case "open-login": {
        const url = msg.loginUrl || (await baseUrl());
        await chrome.tabs.create({ url });
        return { ok: true };
      }
      case "clip-created": {
        // Recorder finished + uploaded; notify the bar and refresh the panel.
        if (tabId == null) return null;
        const landed = clipLandingStatus(msg.appended, msg.name);
        broadcastBar(tabId, { type: "bar-status", text: landed.text, level: landed.level });
        chrome.runtime.sendMessage({ type: "panel-refresh", tabId, matchId: msg.matchId, name: msg.name }).catch(() => null);
        try {
          await chrome.sidePanel.open({ tabId });
        } catch (_err) {
          // No side panel: the bar is the only thing the operator can see, so
          // keep the landing result in the text instead of dropping back to a
          // generic line.
          broadcastBar(tabId, {
            type: "bar-status",
            text: `${landed.text}，点工具栏图标打开侧栏发布`,
            level: landed.level,
          });
        }
        return { ok: true };
      }
      case "upload-begin":
        return beginUpload(tabId, frameId, msg);
      default:
        return null;
    }
  };
  handle().then(sendResponse, (err) => sendResponse({ error: String(err && err.message || err), code: err && err.code, loginUrl: err && err.loginUrl }));
  return true;
});

// ---- upload relay: content script streams base64 chunks over a Port ------
// MV3 messages are JSON only, so the recorder sends each webm as base64
// chunks over chrome.runtime.connect; we reassemble into Blobs and POST.

const uploads = new Map(); // uploadId -> {tabId, meta, files: Blob[][], current: string[]}

function beginUpload(tabId, frameId, msg) {
  const uploadId = `${tabId}:${Date.now()}:${Math.random().toString(36).slice(2, 8)}`;
  uploads.set(uploadId, { tabId, frameId, meta: msg.meta, files: [], current: [] });
  return { uploadId };
}

chrome.runtime.onConnect.addListener((port) => {
  if (port.name !== "upload") return;
  port.onMessage.addListener(async (msg) => {
    const job = uploads.get(msg.uploadId);
    if (!job) {
      port.postMessage({ type: "error", error: "upload session lost" });
      return;
    }
    if (msg.type === "chunk") {
      // Decode per chunk: each chunk is independently base64-padded, so the
      // strings cannot be concatenated and decoded as one.
      try {
        job.current.push(base64ToBytes(msg.data));
      } catch (err) {
        port.postMessage({ type: "error", error: `chunk decode failed: ${err && err.message}` });
      }
      return;
    }
    if (msg.type === "file-end") {
      const parts = job.current;
      job.current = [];
      job.files.push(new Blob(parts, { type: "video/webm" }));
      port.postMessage({ type: "progress", files: job.files.length });
      return;
    }
    if (msg.type === "finish") {
      uploads.delete(msg.uploadId);
      try {
        const result = await postClip(job);
        port.postMessage({ type: "done", result });
      } catch (err) {
        port.postMessage({ type: "error", error: String(err && err.message || err), code: err && err.code, loginUrl: err && err.loginUrl });
      }
      return;
    }
  });
  port.onDisconnect.addListener(() => {
    for (const [id, job] of uploads) {
      if (job.port === port) uploads.delete(id);
    }
  });
});

function base64ToBytes(b64) {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

async function postClip(job) {
  const { meta, files } = job;
  console.log("[upload] postClip", { files: files.length, sizes: files.map((b) => b.size), segments: meta.segments });
  const form = new FormData();
  form.append("match_id", meta.matchId);
  form.append("start_epoch_ms", String(meta.startMs));
  form.append("end_epoch_ms", String(meta.endMs));
  form.append("segments_json", JSON.stringify(meta.segments));
  form.append("source_host", meta.sourceHost || "");
  form.append("capture_mode", meta.captureMode || "captureStream");
  if (meta.crop) form.append("crop", JSON.stringify(meta.crop));
  files.forEach((blob, i) => form.append("file", blob, `seg_${i}.webm`));
  return api("api/monitor/ext/clip", { method: "POST", body: form });
}

// ---- keyboard commands → main recording frame ---------------------------

// Alt+1/2/3 in a tab whose UI is hidden bring the bar and the side panel up
// first, so the result of the key press is visible and the clip lands in an
// open panel. The panel has to be opened right here, before any await.
const PANEL_COMMANDS = new Set(["set-start", "finish", "review"]);

chrome.commands.onCommand.addListener((command, tab) => {
  const tabId = tab && tab.id;
  if (tabId == null) return;
  if (PANEL_COMMANDS.has(command) && !shownTabs.has(tabId)) {
    chrome.sidePanel.open({ tabId }).catch(() => null);
  }
  runCommand(command, tabId);
});

async function runCommand(command, tabId) {
  const state = await tabState(tabId);
  if (command === "toggle-bar") {
    await setShown(tabId, !state.shown);
    return;
  }
  if (!state.shown) await setShown(tabId, true);
  if (state.mainFrameId == null) {
    broadcastBar(tabId, { type: "bar-status", text: "没有找到正在播放的视频", level: "fail" });
    return;
  }
  if (command === "set-start") {
    sendToFrame(tabId, state.mainFrameId, { type: "recorder-set-start" });
  } else if (command === "finish" || command === "review") {
    if (!state.matchId) {
      broadcastBar(tabId, { type: "bar-status", text: "请先在小条里选择比赛", level: "fail" });
      return;
    }
    if (command === "finish") {
      sendToFrame(tabId, state.mainFrameId, { type: "recorder-finish", matchId: state.matchId });
    } else {
      const config = await getConfig().catch(() => null);
      sendToFrame(tabId, state.mainFrameId, {
        type: "review-open",
        matchId: state.matchId,
        maxRangeSeconds: (config && config.clip_range_max_seconds) || 30,
      });
    }
  }
}

// ---- side panel -------------------------------------------------------------

async function panelPath(matchId) {
  const params = new URLSearchParams();
  if (matchId) params.set("match_id", matchId);
  return `panel.html${params.toString() ? "?" + params.toString() : ""}`;
}

// Toolbar icon = "use the clipper in this tab": side panel and bar together.
// Handled here rather than by openPanelOnActionClick, which would open the
// panel without telling us (onClicked does not fire then).
chrome.sidePanel.setPanelBehavior({ openPanelOnActionClick: false }).catch(() => null);

chrome.action.onClicked.addListener((tab) => {
  if (!tab || tab.id == null) return;
  chrome.sidePanel.open({ tabId: tab.id }).catch(() => null); // first: still the user's click
  setShown(tab.id, true);
});

chrome.tabs.onActivated.addListener(async ({ tabId }) => {
  const state = await tabState(tabId);
  await chrome.sidePanel.setOptions({ tabId, path: await panelPath(state.matchId) }).catch(() => null);
});

// A tab that navigated (same tab, new site) keeps its match; push it back to
// the freshly injected bar.
chrome.webNavigation?.onCommitted?.addListener(async ({ tabId, frameId }) => {
  if (frameId !== 0) return;
  frameReports.delete(tabId);
  await setTabState(tabId, { mainFrameId: null });
});
