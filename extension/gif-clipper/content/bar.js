// Draggable Shadow-DOM bar in the top frame only: pick match / set start /
// finish / status. The keyboard shortcuts are the primary path; this is the
// visible fallback and the only place to choose the match.
(() => {
  if (window !== window.top || window.__gifClipperBar) return;
  window.__gifClipperBar = true;

  const toast = window.__gifClipperToast || (() => {});
  let host = null;
  let ui = null;
  let hidden = false;
  let startMs = null;
  let tickTimer = null;

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

  function build() {
    host = document.createElement("div");
    host.id = "gif-clipper-bar-host";
    const root = host.attachShadow({ mode: "closed" });
    const style = document.createElement("style");
    style.textContent = `
      :host { all: initial; position: fixed; left: 16px; bottom: 16px; z-index: 2147483646; }
      .bar { display: flex; align-items: center; gap: 8px; padding: 8px 10px; border-radius: 10px;
        background: rgba(14, 18, 26, .94); color: #e9eef5; border: 1px solid rgba(255,255,255,.12);
        box-shadow: 0 6px 24px rgba(0,0,0,.4); font: 13px/1.3 -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif;
        user-select: none; max-width: 92vw; }
      .grip { cursor: grab; color: #7f8ba0; padding: 0 2px; font-size: 16px; }
      select { font: inherit; background: #1c2230; color: #e9eef5; border: 1px solid #2d3647; border-radius: 6px;
        padding: 4px 6px; max-width: 220px; }
      button { font: inherit; border: 0; border-radius: 6px; padding: 5px 10px; cursor: pointer; color: #fff;
        background: #2f3a4f; white-space: nowrap; }
      button.primary { background: #2f7cf6; }
      button:disabled { opacity: .45; cursor: default; }
      kbd { font: 11px/1 ui-monospace, Menlo, monospace; opacity: .7; margin-left: 4px; }
      .status { color: #9fb0c8; max-width: 280px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .status.ok { color: #5fd38a; } .status.fail { color: #ff7b7b; } .status.warn { color: #ffc46b; }
      .rec { color: #ff6b6b; font-weight: 600; }
      .hide { background: transparent; color: #7f8ba0; padding: 4px 6px; }
    `;
    const bar = document.createElement("div");
    bar.className = "bar";
    bar.innerHTML = `
      <span class="grip" title="拖动">⋮⋮</span>
      <select data-role="match" title="选择这个标签页正在看的比赛"><option value="">选择比赛…</option></select>
      <button data-role="start" title="把当前画面设为起点（Alt+1）">设起点<kbd>Alt+1</kbd></button>
      <button data-role="finish" class="primary" disabled title="结束并生成 GIF（Alt+2）">结束<kbd>Alt+2</kbd></button>
      <span data-role="status" class="status">正在找视频…</span>
      <button data-role="hide" class="hide" title="隐藏小条（Alt+0 再显示）">×</button>
    `;
    root.append(style, bar);
    ui = {
      bar,
      match: bar.querySelector('[data-role="match"]'),
      start: bar.querySelector('[data-role="start"]'),
      finish: bar.querySelector('[data-role="finish"]'),
      status: bar.querySelector('[data-role="status"]'),
      hide: bar.querySelector('[data-role="hide"]'),
    };
    ui.start.addEventListener("click", () => send({ type: "command", command: "set-start" }));
    ui.finish.addEventListener("click", () => send({ type: "command", command: "finish" }));
    ui.hide.addEventListener("click", () => setHidden(true, true));
    ui.match.addEventListener("change", async () => {
      await send({ type: "set-match", matchId: ui.match.value });
      setStatus(ui.match.value ? "已选比赛" : "未选比赛", ui.match.value ? "ok" : "warn");
      updateButtons();
    });
    makeDraggable(bar.querySelector(".grip"));
    (document.fullscreenElement || document.documentElement).appendChild(host);
  }

  function makeDraggable(grip) {
    let drag = null;
    grip.addEventListener("pointerdown", (e) => {
      const rect = host.getBoundingClientRect();
      drag = { dx: e.clientX - rect.left, dy: e.clientY - rect.top };
      grip.setPointerCapture(e.pointerId);
    });
    grip.addEventListener("pointermove", (e) => {
      if (!drag) return;
      host.style.left = `${Math.max(0, e.clientX - drag.dx)}px`;
      host.style.top = `${Math.max(0, e.clientY - drag.dy)}px`;
      host.style.bottom = "auto";
    });
    grip.addEventListener("pointerup", () => (drag = null));
  }

  function setStatus(text, level = "") {
    if (!ui) return;
    ui.status.textContent = text;
    ui.status.className = `status ${level}`;
  }

  function setHidden(next, persist) {
    hidden = next;
    if (host) host.style.display = hidden ? "none" : "";
    if (persist) send({ type: "set-bar-hidden", hidden });
  }

  function updateButtons() {
    if (!ui) return;
    const hasMatch = !!ui.match.value;
    ui.finish.disabled = !(hasMatch && startMs != null);
    ui.finish.title = hasMatch ? "结束并生成 GIF（Alt+2）" : "先选择比赛";
  }

  function renderRange() {
    if (startMs == null) {
      ui.start.innerHTML = "设起点<kbd>Alt+1</kbd>";
      ui.start.classList.remove("rec");
      return;
    }
    const sec = ((Date.now() - startMs) / 1000).toFixed(1);
    ui.start.innerHTML = `<span class="rec">● ${sec}s</span><kbd>Alt+1</kbd>`;
  }

  async function loadMatches(state) {
    const matches = await send({ type: "get-matches" });
    if (!matches || matches.error) {
      if (matches && matches.code === "login_required") {
        setStatus("请先登录 8901", "fail");
        ui.match.innerHTML = '<option value="">登录后刷新页面</option>';
      } else {
        setStatus("读取比赛列表失败", "fail");
      }
      return;
    }
    const keep = state.matchId || ui.match.value;
    ui.match.innerHTML = '<option value="">选择比赛…</option>';
    for (const m of matches) {
      const opt = document.createElement("option");
      opt.value = m.match_id;
      opt.textContent = m.score ? `${m.label} ${m.score}` : m.label;
      ui.match.appendChild(opt);
    }
    if (keep && [...ui.match.options].some((o) => o.value === keep)) {
      ui.match.value = keep;
    } else if (keep) {
      const opt = document.createElement("option");
      opt.value = keep;
      opt.textContent = `比赛 ${keep}（已结束/不在列表）`;
      ui.match.appendChild(opt);
      ui.match.value = keep;
    }
    updateButtons();
  }

  async function pollStatus() {
    const s = await send({ type: "recorder-status" });
    if (!s) return;
    if (!s.elected || !s.recording) {
      setStatus("没有找到正在播放的视频", "warn");
    } else {
      setStatus(`缓冲 ${s.bufferedSeconds}s · ${s.size}`, "ok");
    }
    if (s.startMs !== undefined && s.startMs !== startMs && s.startMs == null) startMs = null;
    renderRange();
    updateButtons();
  }

  chrome.runtime.onMessage.addListener((msg) => {
    if (!msg || !ui) return false;
    switch (msg.type) {
      case "bar-status":
        setStatus(msg.text, msg.level);
        break;
      case "bar-range":
        startMs = msg.startMs == null ? null : msg.startMs;
        renderRange();
        updateButtons();
        break;
      case "bar-visibility":
        setHidden(!!msg.hidden, false);
        break;
      case "bar-source":
        if (msg.frameId == null) setStatus("没有找到正在播放的视频", "warn");
        break;
      default:
        break;
    }
    return false;
  });

  document.addEventListener("fullscreenchange", () => {
    if (!host) return;
    const parent = document.fullscreenElement || document.documentElement;
    if (host.parentNode !== parent) parent.appendChild(host);
  });

  (async () => {
    build();
    const state = (await send({ type: "get-tab-state" })) || {};
    setHidden(!!state.barHidden, false);
    await loadMatches(state);
    pollStatus();
    tickTimer = setInterval(() => {
      renderRange();
    }, 200);
    setInterval(pollStatus, 2000);
    setInterval(() => loadMatches({ matchId: ui.match.value }), 60_000);
  })();
})();
