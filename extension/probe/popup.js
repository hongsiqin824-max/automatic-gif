const $ = (id) => document.getElementById(id);
let lastState = null;

function render(state) {
  lastState = state;
  const status = $("status");
  const out = $("out");
  if (!state) {
    status.textContent = "";
    return;
  }
  if (state.status === "running") {
    status.textContent = `录制中（${state.seconds}s）… 开始于 ${state.startedAt.slice(11, 19)}`;
    out.textContent = "等待页面返回结果。";
    return;
  }
  if (state.status === "error") {
    status.innerHTML = `<span class="bad">执行失败</span>`;
    out.textContent = state.error;
    return;
  }
  const lines = [];
  const verdicts = [];
  for (const frame of state.results || []) {
    lines.push(`[frame ${frame.frameId}${frame.topFrame ? " 主页面" : " iframe"}] ${frame.frame}`);
    frame.videos.forEach((v, i) => {
      lines.push(
        `  video#${i} ${v.size} ${v.paused ? "暂停" : "播放中"} rs=${v.readyState} ${v.srcKind}` +
          `${v.drm ? " DRM!" : ""} canvas=${v.canvasRead}` +
          (v.liveBrightness != null ? ` 亮度=${v.liveBrightness}` : "") +
          ` captureStream=${v.captureStream}`
      );
      if (v.src) lines.push(`    src: ${v.src}`);
    });
    const r = frame.record;
    if (typeof r === "string") {
      lines.push(`  录制: ${r}`);
    } else if (r) {
      if (r.error) {
        lines.push(`  录制失败: ${r.error}`);
        verdicts.push({ ok: false, text: `frame ${frame.frameId}: ${r.error}` });
      } else {
        lines.push(
          `  录制 ${r.target} ${r.mimeType} → ${r.seconds}s ${(r.bytes / 1024 / 1024).toFixed(2)} MB ≈ ${r.kbps} kbps` +
            ` 回放${r.playbackSize || "?"} 亮度=${r.recordedBrightness ?? "?"} → ${r.verdict}`
        );
        if (r.downloaded) lines.push(`  已下载: ${r.downloaded}`);
        verdicts.push({ ok: r.verdict === "画面正常", text: `frame ${frame.frameId}: ${r.verdict}，${r.kbps} kbps` });
      }
    }
    lines.push("");
  }
  if (!state.results || !state.results.length) {
    lines.push(`扫描了 ${state.frameCount} 个 frame，没有找到 <video> 元素。`);
    lines.push("可能：视频在 canvas 里渲染、页面还没开始播放、或者在插件拿不到的 frame 里。");
  }
  const summary = verdicts.length
    ? verdicts.map((v) => `<span class="${v.ok ? "ok" : "bad"}">${v.text}</span>`).join("　")
    : "无可录制的 video";
  status.innerHTML = `完成 ${state.finishedAt.slice(11, 19)}　${summary}`;
  out.textContent = lines.join("\n");
}

$("run").addEventListener("click", async () => {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tab || !tab.id) return;
  const seconds = Math.max(3, Math.min(60, Number($("seconds").value) || 10));
  const download = $("download").checked;
  $("status").textContent = "正在注入…";
  chrome.runtime.sendMessage({ type: "probe", tabId: tab.id, seconds, download }, (resp) => {
    if (chrome.runtime.lastError) {
      $("status").innerHTML = `<span class="bad">${chrome.runtime.lastError.message}</span>`;
      return;
    }
    if (resp) render(resp);
  });
});

$("copy").addEventListener("click", async () => {
  if (!lastState) return;
  await navigator.clipboard.writeText(JSON.stringify(lastState, null, 2));
  $("copy").textContent = "已复制";
  setTimeout(() => ($("copy").textContent = "复制"), 1200);
});

chrome.storage.onChanged.addListener((changes, area) => {
  if (area === "local" && changes.probeState) render(changes.probeState.newValue);
});

chrome.storage.local.get("probeState").then(({ probeState }) => {
  if (probeState) render(probeState);
});
