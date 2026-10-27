// 在页面每个 frame 里执行的探测函数。必须自包含：executeScript 会把它序列化后注入，
// 不能引用这个文件里的其他符号。
async function probeFrame(seconds, download) {
  const videos = Array.from(document.querySelectorAll("video"));
  if (!videos.length) {
    return null;
  }

  function brightnessOf(video) {
    // 画一帧到 canvas 再读像素：跨域且无 CORS 的 video 会抛 SecurityError，
    // 这正是 captureStream 会录成黑屏的同一个原因。
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

  const report = {
    frame: location.href.slice(0, 160),
    topFrame: window === window.top,
    videos: [],
  };

  let target = null;
  for (const v of videos) {
    const src = v.currentSrc || v.src || "";
    const info = {
      size: `${v.videoWidth}x${v.videoHeight}`,
      paused: v.paused,
      readyState: v.readyState,
      srcKind: src.startsWith("blob:") ? "MSE(blob)" : src ? "direct" : "none",
      src: src.slice(0, 120),
      crossOrigin: v.crossOrigin,
      drm: Boolean(v.mediaKeys),
    };
    try {
      info.canvasRead = "ok";
      info.liveBrightness = brightnessOf(v);
    } catch (err) {
      info.canvasRead = `blocked:${err.name}`;
    }
    try {
      const stream = v.captureStream();
      info.captureStream = `ok tracks=${stream.getVideoTracks().length}`;
      stream.getTracks().forEach((t) => t.stop());
    } catch (err) {
      info.captureStream = `fail:${err.name} ${err.message}`;
    }
    report.videos.push(info);
    const playing = !v.paused && v.readyState >= 2 && v.videoWidth > 0;
    if (playing && (!target || v.videoWidth * v.videoHeight > target.videoWidth * target.videoHeight)) {
      target = v;
    }
  }

  if (!target) {
    report.record = "skip: 本 frame 没有正在播放的 video";
    return report;
  }

  // 真正录一段，看 MediaRecorder 是否拒绝、码率多少、录出来的帧是不是黑的。
  const rec = { target: `${target.videoWidth}x${target.videoHeight}` };
  let stream;
  try {
    stream = target.captureStream();
  } catch (err) {
    rec.error = `captureStream ${err.name}: ${err.message}`;
    report.record = rec;
    return report;
  }
  const mimeCandidates = ["video/webm;codecs=vp8", "video/webm;codecs=vp9", "video/webm"];
  const mimeType = mimeCandidates.find((m) => MediaRecorder.isTypeSupported(m)) || "";
  rec.mimeType = mimeType || "(默认)";
  const chunks = [];
  let recorder;
  try {
    recorder = new MediaRecorder(stream, mimeType ? { mimeType } : undefined);
  } catch (err) {
    rec.error = `MediaRecorder 构造失败 ${err.name}: ${err.message}`;
    stream.getTracks().forEach((t) => t.stop());
    report.record = rec;
    return report;
  }
  const started = performance.now();
  const done = new Promise((resolve) => {
    recorder.ondataavailable = (e) => {
      if (e.data && e.data.size) chunks.push(e.data);
    };
    recorder.onerror = (e) => {
      rec.error = `MediaRecorder error: ${e.error ? e.error.name : "unknown"}`;
      resolve();
    };
    recorder.onstop = resolve;
  });
  try {
    recorder.start(1000);
  } catch (err) {
    rec.error = `start 失败 ${err.name}: ${err.message}`;
    stream.getTracks().forEach((t) => t.stop());
    report.record = rec;
    return report;
  }
  await new Promise((r) => setTimeout(r, seconds * 1000));
  if (recorder.state !== "inactive") recorder.stop();
  await done;
  stream.getTracks().forEach((t) => t.stop());
  const elapsed = (performance.now() - started) / 1000;
  const blob = new Blob(chunks, { type: mimeType || "video/webm" });
  rec.seconds = Number(elapsed.toFixed(1));
  rec.bytes = blob.size;
  rec.kbps = Math.round((blob.size * 8) / Math.max(elapsed, 0.1) / 1000);
  rec.chunks = chunks.length;

  // 回放录出来的 webm 抽一帧看亮度，确认不是黑屏。
  if (blob.size > 0) {
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
      pv.currentTime = Math.min(1, seconds / 2);
      await new Promise((resolve) => {
        pv.onseeked = resolve;
        setTimeout(resolve, 2000);
      });
      rec.playbackSize = `${pv.videoWidth}x${pv.videoHeight}`;
      rec.recordedBrightness = brightnessOf(pv);
      rec.verdict = rec.recordedBrightness > 8 ? "画面正常" : "疑似黑屏";
    } catch (err) {
      rec.playbackCheck = `失败 ${err.name}`;
    }
    if (download) {
      const a = document.createElement("a");
      a.href = url;
      a.download = `probe_${location.hostname}_${Date.now()}.webm`;
      document.documentElement.appendChild(a);
      a.click();
      a.remove();
      rec.downloaded = a.download;
    }
    setTimeout(() => URL.revokeObjectURL(url), 30000);
  } else {
    rec.verdict = "没有录到任何数据";
  }
  report.record = rec;
  return report;
}

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg && msg.type === "probe") {
    runProbe(msg).then(sendResponse, (err) => sendResponse({ error: String(err) }));
    return true;
  }
  return false;
});

async function runProbe({ tabId, seconds, download }) {
  const startedAt = new Date().toISOString();
  await chrome.storage.local.set({ probeState: { status: "running", startedAt, seconds } });
  let frames;
  try {
    frames = await chrome.scripting.executeScript({
      target: { tabId, allFrames: true },
      func: probeFrame,
      args: [seconds, Boolean(download)],
    });
  } catch (err) {
    const failed = { status: "error", startedAt, error: String(err) };
    await chrome.storage.local.set({ probeState: failed });
    return failed;
  }
  const results = frames
    .map((f) => ({ frameId: f.frameId, ...(f.result || {}) }))
    .filter((f) => f.videos);
  const finished = {
    status: "done",
    startedAt,
    finishedAt: new Date().toISOString(),
    seconds,
    frameCount: frames.length,
    results,
  };
  await chrome.storage.local.set({ probeState: finished });
  return finished;
}
