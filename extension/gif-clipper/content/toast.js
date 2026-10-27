// Tiny toast in every frame (so feedback is visible even when the <video> is
// in a cross-origin iframe shown fullscreen). Shadow DOM keeps page CSS out.
(() => {
  if (window.__gifClipperToast) return;
  let host = null;
  let timer = null;

  function ensureHost() {
    if (host && host.isConnected) return host;
    host = document.createElement("div");
    host.id = "gif-clipper-toast-host";
    const root = host.attachShadow({ mode: "closed" });
    const style = document.createElement("style");
    style.textContent = `
      :host { all: initial; position: fixed; top: 16px; right: 16px; z-index: 2147483647; pointer-events: none; }
      .toast { font: 500 14px/1.4 -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif; color: #fff;
        background: rgba(20, 24, 32, .92); border-left: 4px solid #4ea1ff; padding: 10px 14px; border-radius: 6px;
        box-shadow: 0 4px 16px rgba(0,0,0,.35); max-width: 360px; opacity: 0; transform: translateY(-6px);
        transition: opacity .15s, transform .15s; white-space: pre-line; }
      .toast.show { opacity: 1; transform: none; }
      .toast.ok { border-left-color: #35c46a; }
      .toast.fail { border-left-color: #ff5d5d; }
      .toast.warn { border-left-color: #ffb13b; }
    `;
    const box = document.createElement("div");
    box.className = "toast";
    root.append(style, box);
    host.__box = box;
    const parent = document.fullscreenElement || document.documentElement;
    parent.appendChild(host);
    return host;
  }

  function show(text, level = "", ms = 2200) {
    const h = ensureHost();
    const box = h.__box;
    box.textContent = text;
    box.className = `toast show ${level}`;
    clearTimeout(timer);
    timer = setTimeout(() => box.classList.remove("show"), ms);
  }

  // Follow fullscreen so the toast stays inside the fullscreen tree.
  document.addEventListener("fullscreenchange", () => {
    if (!host) return;
    const parent = document.fullscreenElement || document.documentElement;
    if (host.parentNode !== parent) parent.appendChild(host);
  });

  window.__gifClipperToast = show;
})();
