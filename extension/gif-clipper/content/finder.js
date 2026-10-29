// Runs in every frame. Finds the largest playing <video>, reports it to the
// background (which elects one frame per tab to record), owns the recorder
// when elected, and handles set-start / finish from the background.
//
// The recording source is sticky: once a <video> is chosen it stays the
// source while it is in the document, no matter whether it is paused,
// stalled or seeking. It is replaced only when it leaves the DOM (hard
// switch) or another video keeps playing for CHALLENGE_MS and either is
// bigger or the current one is not playing (soft switch). While the review
// window is open only the hard switch applies.
//
// Pure helper `pickSource` is exported for node tests (no DOM needed).
(() => {
  const CHALLENGE_MS = 8000;

  // current:    {el, connected, playing, area} | null — the sticky source
  // candidates: [{el, area}] — other videos that are playing right now
  // challenger: {el, since} | null — candidate being timed for a soft switch
  // Returns {next, reason, challenger}; next is an element or null.
  function pickSource(current, candidates, challenger, now, reviewOpen) {
    let best = null;
    for (const c of candidates) {
      if (current && c.el === current.el) continue;
      if (!best || c.area > best.area) best = c;
    }
    if (!current || !current.connected) {
      return { next: best ? best.el : null, reason: current ? "hard" : "none", challenger: null };
    }
    if (reviewOpen || !best || (current.playing && best.area <= current.area)) {
      return { next: current.el, reason: "keep", challenger: null };
    }
    if (challenger && challenger.el === best.el) {
      if (now - challenger.since >= CHALLENGE_MS) {
        return { next: best.el, reason: "soft", challenger: null };
      }
      return { next: current.el, reason: "keep", challenger };
    }
    return { next: current.el, reason: "keep", challenger: { el: best.el, since: now } };
  }

  // The match's saved crop for a clip. ``entry`` = {crop, aspect} from
  // background (or null); ``aspect`` = current video width / height. A box
  // drawn on a differently shaped stream would cut the wrong area, so it is
  // withheld (full frame) and the caller warns instead.
  const ASPECT_TOLERANCE = 0.02;
  function cropForClip(entry, aspect) {
    const crop = entry && entry.crop ? entry.crop : null;
    if (!crop) return { crop: null, aspectChanged: false };
    const saved = entry.aspect;
    if (saved > 0 && aspect > 0 && Math.abs(aspect / saved - 1) > ASPECT_TOLERANCE) {
      return { crop: null, aspectChanged: true };
    }
    return { crop, aspectChanged: false };
  }

  if (typeof module !== "undefined" && module.exports) {
    module.exports = { pickSource, CHALLENGE_MS, cropForClip };
  }
  if (typeof window === "undefined") return;
  window.__gifClipperCropForClip = cropForClip; // the top-frame bar uses the same rule
  if (window.__gifClipperFinder) return;
  window.__gifClipperFinder = true;

  const toast = window.__gifClipperToast || (() => {});
  const { RollingRecorder } = window.__gifClipperRecorder;
  const MIN_W = 320;
  const MIN_H = 180;
  const CHUNK_BYTES = 2 * 1024 * 1024;

  let current = null;     // the <video> we reported / record
  let currentArea = 0;    // last known size of ``current`` (0 while emptied)
  let challenger = null;  // {el, since} — see pickSource
  let challengeTimer = null;
  let recorder = null;
  let recorderOpts = null;
  let elected = false;
  let busy = false;
  let reportTimer = null;
  let shown = false;      // the operator has the clipper up in this tab (bar / panel)
  let shownKnown = false; // a ui-shown message already arrived (newer than the initial read)

  // Toasts nobody asked for (source switch, player reconnect, CORS) appear
  // only while the clipper is up in this tab: recording also runs in tabs the
  // operator never opened it in. Answers to key presses always toast.
  function notify(text, level, ms) {
    if (shown) toast(text, level, ms);
  }

  function isPlaying(v) {
    return !v.paused && v.readyState >= 2 && v.videoWidth >= MIN_W && v.videoHeight >= MIN_H;
  }

  function send(msg) {
    return new Promise((resolve) => {
      try {
        chrome.runtime.sendMessage(msg, (resp) => {
          void chrome.runtime.lastError;
          resolve(resp || null);
        });
      } catch (_err) {
        resolve(null);
      }
    });
  }

  function report() {
    if (current && current.videoWidth && current.videoHeight) {
      currentArea = current.videoWidth * current.videoHeight;
    }
    const candidates = [];
    for (const v of document.querySelectorAll("video")) {
      if (v !== current && isPlaying(v)) candidates.push({ el: v, area: v.videoWidth * v.videoHeight });
    }
    const cur = current
      ? { el: current, connected: current.isConnected, playing: isPlaying(current), area: currentArea }
      : null;
    const pick = pickSource(cur, candidates, challenger, Date.now(), !!(review && !review.closed));
    challenger = pick.challenger;
    clearTimeout(challengeTimer);
    if (challenger) {
      // Re-check right when the challenger's playing time reaches the bar
      // instead of waiting for the next event or 10 s poll.
      challengeTimer = setTimeout(report, Math.max(0, CHALLENGE_MS - (Date.now() - challenger.since)) + 50);
    }
    if (pick.next !== current) {
      if (review && !review.closed) review.close();
      if (recorder) {
        recorder.stop();
        recorder = null;
        if (elected) notify("视频源已切换，缓冲重新开始", "warn");
      }
      current = pick.next;
      currentArea = current ? current.videoWidth * current.videoHeight : 0;
      if (current && elected && recorderOpts) startRecorder();
    }
    send({
      type: "frame-report",
      playing: !!current,
      area: currentArea,
    });
  }

  function scheduleReport() {
    clearTimeout(reportTimer);
    reportTimer = setTimeout(report, 400);
  }

  function startRecorder() {
    if (!current || recorder) return;
    recorder = new RollingRecorder(current, {
      ...recorderOpts,
      onStatus: (text, level) => notify(text, level),
    });
    if (!recorder.start()) recorder = null;
  }

  // Canvas readback check: SecurityError here == black captureStream later.
  function canvasBlocked(v) {
    try {
      const c = document.createElement("canvas");
      c.width = 8;
      c.height = 8;
      const ctx = c.getContext("2d");
      ctx.drawImage(v, 0, 0, 8, 8);
      ctx.getImageData(0, 0, 1, 1);
      return false;
    } catch (_err) {
      return true;
    }
  }

  function formatMb(bytes) {
    return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
  }

  async function uploadClip(matchId, picked) {
    // Total bytes of what actually goes out, so the toast can report the
    // upload instead of a file count the operator cannot act on.
    const uploadBytes = picked.segments.reduce((sum, s) => sum + s.blob.size, 0);
    const begin = await send({
      type: "upload-begin",
      meta: {
        matchId,
        startMs: picked.startMs,
        endMs: picked.endMs,
        segments: picked.segments.map((s) => ({ start_epoch_ms: s.startEpochMs, duration_ms: s.durationMs })),
        sourceHost: location.hostname,
        captureMode: "captureStream",
        crop: picked.crop || null,
      },
    });
    if (!begin || !begin.uploadId) throw new Error((begin && begin.error) || "上传会话建立失败");
    const port = chrome.runtime.connect({ name: "upload" });
    const result = await new Promise(async (resolve, reject) => {
      port.onMessage.addListener((m) => {
        if (m.type === "done") resolve(m.result);
        else if (m.type === "error") {
          const err = new Error(m.error);
          err.code = m.code;
          err.loginUrl = m.loginUrl;
          reject(err);
        } else if (m.type === "progress") {
          toast(`正在上传 ${formatMb(uploadBytes)}…`, "", 10_000);
        }
      });
      port.onDisconnect.addListener(() => reject(new Error("上传连接断开")));
      try {
        for (const seg of picked.segments) {
          const buf = new Uint8Array(await seg.blob.arrayBuffer());
          for (let off = 0; off < buf.length; off += CHUNK_BYTES) {
            const slice = buf.subarray(off, Math.min(off + CHUNK_BYTES, buf.length));
            let bin = "";
            for (let i = 0; i < slice.length; i += 0x8000) {
              bin += String.fromCharCode.apply(null, slice.subarray(i, i + 0x8000));
            }
            port.postMessage({ type: "chunk", uploadId: begin.uploadId, data: btoa(bin) });
          }
          port.postMessage({ type: "file-end", uploadId: begin.uploadId });
        }
        port.postMessage({ type: "finish", uploadId: begin.uploadId });
      } catch (err) {
        reject(err);
      }
    });
    port.disconnect();
    return { ...result, uploadBytes };
  }

  let review = null;      // open ReviewOverlay, if any
  const pendingClips = []; // [{matchId, range}] confirmed while an upload was running
  const MAX_PENDING = 3;

  // Build and upload a clip. ``range`` = {startMs, endMs, crop?}; the Alt+2
  // path passes the operator-set start and "now", resolved at key press so
  // a queued request does not grow by the time it waited. Uploads are
  // serialised (two concurrent finish() calls would each close and reopen
  // the live segment); a request that arrives while one is running is
  // queued instead of dropped, so a review selection is never lost.
  async function generateClip(matchId, range) {
    if (!recorder) return;
    if (busy) {
      if (pendingClips.length >= MAX_PENDING) {
        toast("排队的太多了，等上一条传完再试", "fail", 3000);
        return;
      }
      pendingClips.push({ matchId, range });
      toast(`上一条还在上传，这条已排队（${pendingClips.length}）`, "warn", 3000);
      return;
    }
    busy = true;
    toast("生成中…", "", 10_000);
    try {
      const picked = await recorder.finish(range);
      const startedAt = Date.now();
      const result = await uploadClip(matchId, picked);
      // Split the wait the operator just sat through: how long the upload
      // took vs how long the server spent encoding. Before this the single
      // 「上传中」 toast covered both and neither could be told apart.
      const encodeMs = Number(result.encode_ms) || 0;
      const uploadSeconds = Math.max(0, Date.now() - startedAt - encodeMs) / 1000;
      const timing =
        `上传 ${formatMb(result.uploadBytes)} 用 ${uploadSeconds.toFixed(1)} 秒` +
        ` · 生成 ${(encodeMs / 1000).toFixed(1)} 秒`;
      const size = `${(result.bytes / 1024 / 1024).toFixed(2)} MB`;
      // Say where it landed: without the side panel open, this toast is the
      // only place the operator learns whether 「再截一个视角」 caught this one.
      const appended = result.appended || null;
      if (appended && appended.error) {
        toast(`GIF 已生成（${size}），但未能追加，已单独保存 · ${timing}`, "warn", 4000);
      } else if (appended && appended.main) {
        const conflict = appended.push_state && appended.push_state.state === "conflict";
        toast(
          `GIF 已生成（${size}），已追加为第 ${appended.gif_count} 张` +
            (conflict ? "；⚠ 对应的推送和主图不是同一条" : "") + ` · ${timing}`,
          conflict ? "warn" : "ok",
          conflict ? 5000 : 3000,
        );
      } else {
        toast(`GIF 已生成（${size}）· ${timing}`, "ok", 3000);
      }
      await send({ type: "clip-created", matchId, name: result.name, appended });
    } catch (err) {
      if (err && err.code === "login_required") {
        toast("请先登录 8901（已打开登录页）", "fail", 4000);
        await send({ type: "open-login", loginUrl: err.loginUrl });
      } else {
        toast(`失败：${err && err.message}`, "fail", 4000);
      }
    } finally {
      busy = false;
      const next = pendingClips.shift();
      if (next && recorder) generateClip(next.matchId, next.range);
    }
  }

  // ---- sticky crop (saved per match by background) ----
  function videoAspect() {
    return current && current.videoWidth && current.videoHeight ? current.videoWidth / current.videoHeight : 0;
  }

  async function loadCrop(matchId) {
    const entry = await send({ type: "get-crop", matchId });
    return entry && !entry.error ? entry : null;
  }

  function saveCrop(matchId, crop) {
    return send({ type: "set-crop", matchId, crop: crop || null, aspect: videoAspect() });
  }

  // Crop for an Alt+2 clip: the saved box, or full frame with a warning when
  // the stream changed shape since the box was drawn.
  async function clipCrop(matchId) {
    const { crop, aspectChanged } = cropForClip(await loadCrop(matchId), videoAspect());
    if (aspectChanged) toast("画面比例变了，这张按整幅出图；按 Alt+3 回看确认裁剪框", "warn", 4000);
    return crop;
  }

  // Review overlay over the live player: close the open segment first so the
  // last seconds are playable, then stitch the closed segments.
  async function openReview(matchId, maxRangeSeconds) {
    const { ReviewOverlay } = window.__gifClipperReview || {};
    if (!ReviewOverlay) {
      toast("回看组件未加载", "fail");
      return;
    }
    await recorder.closeLiveSegment();
    const segments = recorder.snapshot();
    if (!segments.length) {
      toast("缓冲还是空的，再等几秒", "warn");
      return;
    }
    const saved = await loadCrop(matchId);
    if (cropForClip(saved, videoAspect()).aspectChanged) {
      toast("画面比例变了，请确认裁剪框位置", "warn", 4000);
    }
    review = new ReviewOverlay({
      anchor: current,
      segments,
      mimeType: recorder.mimeType || "video/webm",
      maxRangeSeconds,
      crop: (saved && saved.crop) || null,
      lastCrop: (saved && saved.last) || null,
      onCropChange: (crop) => saveCrop(matchId, crop),
      onConfirm: (range) => {
        // Generating with the box confirms it for the current stream shape.
        saveCrop(matchId, range.crop);
        generateClip(matchId, range);
      },
      onClose: () => {
        review = null;
      },
    });
  }

  chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
    switch (msg && msg.type) {
      case "recorder-start": {
        elected = true;
        recorderOpts = { bufferSeconds: msg.bufferSeconds, bitrate: msg.bitrate };
        if (current && canvasBlocked(current)) {
          notify("此站画面受限（跨域无 CORS），换个源试试", "fail", 4000);
        }
        startRecorder();
        sendResponse({ ok: !!recorder });
        return false;
      }
      case "ui-shown": {
        const wasShown = shown;
        shown = !!msg.shown;
        shownKnown = true;
        // Say now what was kept quiet while hidden and still matters.
        if (shown && !wasShown && elected && current && canvasBlocked(current)) {
          toast("此站画面受限（跨域无 CORS），换个源试试", "fail", 4000);
        }
        return false;
      }
      case "recorder-stop": {
        elected = false;
        if (recorder) recorder.stop();
        recorder = null;
        sendResponse({ ok: true });
        return false;
      }
      case "recorder-set-start": {
        if (!recorder) {
          toast("还没开始录制", "fail");
          sendResponse({ ok: false });
          return false;
        }
        const at = recorder.setStart();
        const t = new Date(at);
        toast(`起点 ${t.toTimeString().slice(0, 8)}（缓冲 ${recorder.bufferedSeconds()}s）`, "ok");
        send({ type: "bar-relay", payload: { type: "bar-range", startMs: at } });
        sendResponse({ ok: true, startMs: at });
        return false;
      }
      case "recorder-finish": {
        if (!recorder) {
          toast("还没开始录制", "fail");
          sendResponse({ ok: false });
          return false;
        }
        if (recorder.startMs == null) {
          toast("还没有设起点", "fail");
          sendResponse({ ok: false });
          return false;
        }
        const range = { startMs: recorder.startMs, endMs: Date.now() };
        recorder.clearStart();
        send({ type: "bar-relay", payload: { type: "bar-range", startMs: null } });
        // Same box as the review window: the match's saved crop applies here too.
        clipCrop(msg.matchId).then((crop) => generateClip(msg.matchId, { ...range, crop }));
        sendResponse({ ok: true });
        return false;
      }
      case "review-open": {
        if (!recorder || !current) {
          toast("还没开始录制", "fail");
          sendResponse({ ok: false });
          return false;
        }
        if (review && !review.closed) {
          review.close();
          sendResponse({ ok: true, closed: true });
          return false;
        }
        openReview(msg.matchId, msg.maxRangeSeconds);
        sendResponse({ ok: true });
        return false;
      }
      case "recorder-status": {
        sendResponse({
          elected,
          recording: !!recorder,
          bufferedSeconds: recorder ? recorder.bufferedSeconds() : 0,
          startMs: recorder ? recorder.startMs : null,
          size: current ? `${current.videoWidth}x${current.videoHeight}` : "",
        });
        return false;
      }
      default:
        return false;
    }
  });

  // Discover videos: playing/pause events bubble-capture to document, plus a
  // MutationObserver for elements that get swapped on source change.
  ["playing", "pause", "emptied", "ended", "loadedmetadata"].forEach((ev) =>
    document.addEventListener(ev, (e) => {
      if (!e.target || e.target.tagName !== "VIDEO") return;
      // Same element came back with a new source: pick up its new track.
      if (e.target === current && recorder && (ev === "loadedmetadata" || ev === "playing")) {
        recorder.ensureLive();
      }
      scheduleReport();
    }, true)
  );
  const observer = new MutationObserver((muts) => {
    for (const m of muts) {
      const touched = [...m.addedNodes, ...m.removedNodes].some(
        (n) => n.nodeType === 1 && (n.tagName === "VIDEO" || (n.querySelector && n.querySelector("video")))
      );
      if (touched) {
        scheduleReport();
        return;
      }
    }
  });
  const install = () => observer.observe(document.documentElement, { childList: true, subtree: true });
  if (document.documentElement) install();
  else document.addEventListener("DOMContentLoaded", install, { once: true });
  send({ type: "get-tab-state" }).then((state) => {
    if (!shownKnown && state) shown = !!state.shown;
  });
  scheduleReport();
  setInterval(report, 10_000);
})();
