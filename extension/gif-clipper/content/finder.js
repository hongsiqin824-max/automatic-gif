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

  if (typeof module !== "undefined" && module.exports) {
    module.exports = { pickSource, CHALLENGE_MS };
  }
  if (typeof window === "undefined") return;
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
        if (elected) toast("视频源已切换，缓冲重新开始", "warn");
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
      onStatus: (text, level) => toast(text, level),
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

  async function uploadClip(matchId, picked) {
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
          toast(`上传中 ${m.files}/${picked.segments.length}…`, "", 10_000);
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
    return result;
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
      const result = await uploadClip(matchId, picked);
      toast(`GIF 已生成（${(result.bytes / 1024 / 1024).toFixed(2)} MB）`, "ok", 3000);
      await send({ type: "clip-created", matchId, name: result.name });
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
    review = new ReviewOverlay({
      anchor: current,
      segments,
      mimeType: recorder.mimeType || "video/webm",
      maxRangeSeconds,
      onConfirm: (range) => generateClip(matchId, range),
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
          toast("此站画面受限（跨域无 CORS），换个源试试", "fail", 4000);
        }
        startRecorder();
        sendResponse({ ok: !!recorder });
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
        generateClip(msg.matchId, range);
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
  scheduleReport();
  setInterval(report, 10_000);
})();
