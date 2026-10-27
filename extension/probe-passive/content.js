// 被动采集探测：注入到所有 frame。只做三件事：
//   1. 某个 <video> 开始播放（面积 ≥ 320×180）→ 等 3 s 避开广告预播 → 跑一次 5 s 探测 → 发给 background。
//   2. 监听 <video> 元素的新增 / 移除 / 换源，统计「换主播、换线路时元素是否被替换」。
//   3. 不上传、不下载、不碰页面 DOM 样式。探测用的 canvas / 录制对象都是离屏临时对象。
// 同一个 video 元素只探测一次（WeakSet），同一「站 + 播放方式 + 画面可读性」组合由 background 去重。
(() => {
  if (window.__gifProbePassiveInstalled) return;
  window.__gifProbePassiveInstalled = true;

  const MIN_AREA_W = 320;
  const MIN_AREA_H = 180;
  const SETTLE_MS = 3000;
  const RECORD_SECONDS = 5;

  const probed = new WeakSet();
  const pendingTimers = new WeakMap();

  function topHost() {
    try {
      if (window === window.top) return location.hostname;
      const origins = location.ancestorOrigins;
      if (origins && origins.length) {
        return new URL(origins[origins.length - 1]).hostname;
      }
    } catch (_err) {
      /* 跨域时拿不到，退到 referrer */
    }
    try {
      return document.referrer ? new URL(document.referrer).hostname : "";
    } catch (_err) {
      return "";
    }
  }

  function frameMeta() {
    const host = topHost();
    const frameHost = location.hostname;
    return {
      host: host || frameHost,
      frameHost,
      inIframe: window !== window.top,
      crossOrigin: Boolean(host) && host !== frameHost,
    };
  }

  function send(type, payload) {
    try {
      chrome.runtime.sendMessage({ type, ...frameMeta(), ...payload }, () => {
        void chrome.runtime.lastError;
      });
    } catch (_err) {
      /* 扩展被重载后旧 content script 会失联，忽略 */
    }
  }

  function brightnessOf(video) {
    const canvas = document.createElement("canvas");
    canvas.width = 32;
    canvas.height = 18;
    const ctx = canvas.getContext("2d");
    ctx.drawImage(video, 0, 0, 32, 18);
    const data = ctx.getImageData(0, 0, 32, 18).data;
    let sum = 0;
    for (let i = 0; i < data.length; i += 4) {
      sum += data[i] + data[i + 1] + data[i + 2];
    }
    return Math.round(sum / (data.length / 4) / 3);
  }

  function srcKindOf(video) {
    const src = video.currentSrc || video.src || "";
    if (src.startsWith("blob:")) return "MSE";
    if (src) return "direct";
    return "none";
  }

  // 不录制的轻量检查：播放方式、DRM、canvas 能否读像素。结果决定要不要做 5 s 录制。
  function quickCheck(video) {
    const info = {
      size: `${video.videoWidth}x${video.videoHeight}`,
      srcKind: srcKindOf(video),
      drm: Boolean(video.mediaKeys),
      canvas: "ok",
      liveBrightness: null,
    };
    try {
      info.liveBrightness = brightnessOf(video);
    } catch (err) {
      info.canvas = `blocked:${err.name}`;
    }
    return info;
  }

  async function recordSample(video) {
    const rec = {};
    let stream;
    try {
      stream = video.captureStream();
      rec.captureStream = `ok tracks=${stream.getVideoTracks().length}`;
    } catch (err) {
      rec.captureStream = `fail:${err.name}`;
      return rec;
    }
    const mimeCandidates = ["video/webm;codecs=vp8", "video/webm;codecs=vp9", "video/webm"];
    const mimeType = mimeCandidates.find((m) => MediaRecorder.isTypeSupported(m)) || "";
    rec.mimeType = mimeType || "default";
    const chunks = [];
    let recorder;
    try {
      recorder = new MediaRecorder(stream, mimeType ? { mimeType } : undefined);
    } catch (err) {
      rec.error = `MediaRecorder ${err.name}`;
      stream.getTracks().forEach((t) => t.stop());
      return rec;
    }
    const started = performance.now();
    const done = new Promise((resolve) => {
      recorder.ondataavailable = (e) => {
        if (e.data && e.data.size) chunks.push(e.data);
      };
      recorder.onerror = (e) => {
        rec.error = `recorder ${e.error ? e.error.name : "unknown"}`;
        resolve();
      };
      recorder.onstop = resolve;
    });
    try {
      recorder.start(1000);
    } catch (err) {
      rec.error = `start ${err.name}`;
      stream.getTracks().forEach((t) => t.stop());
      return rec;
    }
    await new Promise((r) => setTimeout(r, RECORD_SECONDS * 1000));
    if (recorder.state !== "inactive") recorder.stop();
    await done;
    stream.getTracks().forEach((t) => t.stop());
    const elapsed = (performance.now() - started) / 1000;
    const blob = new Blob(chunks, { type: mimeType || "video/webm" });
    rec.seconds = Number(elapsed.toFixed(1));
    rec.bytes = blob.size;
    rec.kbps = Math.round((blob.size * 8) / Math.max(elapsed, 0.1) / 1000);
    if (blob.size === 0) {
      rec.verdict = "no-data";
      return rec;
    }
    // 回放一帧看亮度：canvas 能读不代表录出来不黑，两者都看。
    const url = URL.createObjectURL(blob);
    try {
      const pv = document.createElement("video");
      pv.muted = true;
      pv.playsInline = true;
      pv.src = url;
      await new Promise((resolve) => {
        pv.onloadeddata = resolve;
        pv.onerror = resolve;
        setTimeout(resolve, 3000);
      });
      pv.currentTime = Math.min(1, RECORD_SECONDS / 2);
      await new Promise((resolve) => {
        pv.onseeked = resolve;
        setTimeout(resolve, 2000);
      });
      rec.playbackSize = `${pv.videoWidth}x${pv.videoHeight}`;
      rec.recordedBrightness = brightnessOf(pv);
      rec.verdict = rec.recordedBrightness > 8 ? "ok" : "black";
      pv.removeAttribute("src");
      pv.load();
    } catch (err) {
      rec.verdict = `playback-fail:${err.name}`;
    } finally {
      URL.revokeObjectURL(url);
    }
    return rec;
  }

  function isPlaying(video) {
    return (
      !video.paused &&
      video.readyState >= 2 &&
      video.videoWidth >= MIN_AREA_W &&
      video.videoHeight >= MIN_AREA_H
    );
  }

  async function probe(video) {
    if (probed.has(video)) return;
    probed.add(video);
    if (!isPlaying(video)) {
      probed.delete(video);
      return;
    }
    const quick = quickCheck(video);
    // background 先按 (host, frameHost, srcKind, canvas) 去重：已有详细记录就只加 seen。
    let decision;
    try {
      decision = await new Promise((resolve) => {
        chrome.runtime.sendMessage({ type: "probe-check", ...frameMeta(), quick }, (resp) => {
          void chrome.runtime.lastError;
          resolve(resp || { record: false });
        });
      });
    } catch (_err) {
      return;
    }
    if (!decision.record) return;
    if (!isPlaying(video)) {
      probed.delete(video);
      return;
    }
    const record = await recordSample(video);
    send("probe-result", { quick, record, url: location.href.slice(0, 160) });
  }

  function schedule(video) {
    if (probed.has(video) || pendingTimers.has(video)) return;
    const timer = setTimeout(() => {
      pendingTimers.delete(video);
      probe(video);
    }, SETTLE_MS);
    pendingTimers.set(video, timer);
  }

  document.addEventListener(
    "playing",
    (e) => {
      const target = e.target;
      if (target && target.tagName === "VIDEO") schedule(target);
    },
    true
  );

  // 换源统计：元素被移除 / 新增 → "替换"；同一元素 src 变 → "复用"。
  const knownVideos = new WeakSet();
  function noteVideo(video) {
    if (knownVideos.has(video)) return;
    knownVideos.add(video);
    send("video-event", { kind: "added" });
    video.addEventListener("emptied", () => send("video-event", { kind: "src-changed" }), true);
  }
  function scan(root) {
    if (!root || !root.querySelectorAll) return;
    if (root.tagName === "VIDEO") noteVideo(root);
    root.querySelectorAll("video").forEach(noteVideo);
  }
  function collectRemoved(root, out) {
    if (!root || !root.querySelectorAll) return;
    if (root.tagName === "VIDEO") out.push(root);
    root.querySelectorAll("video").forEach((v) => out.push(v));
  }

  function installObserver() {
    scan(document.documentElement);
    const observer = new MutationObserver((mutations) => {
      for (const m of mutations) {
        m.addedNodes.forEach((n) => scan(n));
        const removed = [];
        m.removedNodes.forEach((n) => collectRemoved(n, removed));
        removed.forEach((v) => {
          if (knownVideos.has(v)) {
            send("video-event", { kind: "removed" });
            const timer = pendingTimers.get(v);
            if (timer) {
              clearTimeout(timer);
              pendingTimers.delete(v);
            }
          }
        });
      }
    });
    observer.observe(document.documentElement, { childList: true, subtree: true });
  }

  if (document.documentElement) {
    installObserver();
  } else {
    document.addEventListener("DOMContentLoaded", installObserver, { once: true });
  }
})();
