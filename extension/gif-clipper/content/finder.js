// Runs in every frame. Finds the largest playing <video>, reports it to the
// background (which elects one frame per tab to record), owns the recorder
// when elected, and handles set-start / finish from the background.
(() => {
  if (window.__gifClipperFinder) return;
  window.__gifClipperFinder = true;

  const toast = window.__gifClipperToast || (() => {});
  const { RollingRecorder } = window.__gifClipperRecorder;
  const MIN_W = 320;
  const MIN_H = 180;
  const CHUNK_BYTES = 2 * 1024 * 1024;

  let current = null;     // the <video> we reported / record
  let recorder = null;
  let recorderOpts = null;
  let elected = false;
  let busy = false;
  let reportTimer = null;

  function isPlaying(v) {
    return !v.paused && v.readyState >= 2 && v.videoWidth >= MIN_W && v.videoHeight >= MIN_H;
  }

  function bestVideo() {
    let best = null;
    for (const v of document.querySelectorAll("video")) {
      if (!isPlaying(v)) continue;
      if (!best || v.videoWidth * v.videoHeight > best.videoWidth * best.videoHeight) best = v;
    }
    return best;
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
    const v = bestVideo();
    if (v !== current) {
      if (recorder) {
        recorder.stop();
        recorder = null;
        if (elected) toast("视频源已切换，缓冲重新开始", "warn");
      }
      current = v;
      if (v && elected && recorderOpts) startRecorder();
    }
    send({
      type: "frame-report",
      playing: !!v,
      area: v ? v.videoWidth * v.videoHeight : 0,
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
        if (!recorder || busy) {
          toast(busy ? "正在生成上一条…" : "还没开始录制", "fail");
          sendResponse({ ok: false });
          return false;
        }
        busy = true;
        toast("生成中…", "", 10_000);
        (async () => {
          try {
            const picked = await recorder.finish();
            const result = await uploadClip(msg.matchId, picked);
            toast(`GIF 已生成（${(result.bytes / 1024 / 1024).toFixed(2)} MB）`, "ok", 3000);
            await send({ type: "clip-created", matchId: msg.matchId, name: result.name });
          } catch (err) {
            if (err && err.code === "login_required") {
              toast("请先登录 8901（已打开登录页）", "fail", 4000);
              await send({ type: "open-login", loginUrl: err.loginUrl });
            } else {
              toast(`失败：${err && err.message}`, "fail", 4000);
            }
          } finally {
            busy = false;
            send({ type: "bar-relay", payload: { type: "bar-range", startMs: null } });
          }
        })();
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
      if (e.target && e.target.tagName === "VIDEO") scheduleReport();
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
