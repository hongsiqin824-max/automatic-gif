// Review overlay: play back the rolling buffer on top of the live player so
// the operator can scrub to the exact moment and mark [start, end].
//
// Lives in the same frame as the recorder (the <video> may sit in a
// cross-origin iframe; the blobs are here, so the player is here too). The
// segments are stitched into one timeline with MediaSource: each 30 s webm
// is appended at timestampOffset = segment.start - buffer.start. MediaRecorder
// webm leaves its last cluster open, so the SourceBuffer must be abort()ed
// before changing the offset (verified 2026-10-27 in a probe page).
//
// Keys while open: Space play/pause, ←/→ ±1 s, Shift+←/→ ±5 s, , . ±1 frame,
// [ set start, ] set end, Enter confirm, Esc close.
(() => {
  if (window.__gifClipperReview) return;

  const toast = window.__gifClipperToast || (() => {});
  const FRAME_S = 1 / 25;
  const DEFAULT_LOOKBACK_S = 10;

  class ReviewOverlay {
    constructor({ anchor, segments, mimeType, maxRangeSeconds, onConfirm, onClose }) {
      this.anchor = anchor;               // the live <video> we cover
      this.segments = segments;           // [{blob, startEpochMs, durationMs}] in order
      this.mimeType = mimeType;
      this.maxRange = maxRangeSeconds || 30;
      this.onConfirm = onConfirm;
      this.onClose = onClose;
      this.base = segments[0].startEpochMs;
      const last = segments[segments.length - 1];
      this.total = (last.startEpochMs + last.durationMs - this.base) / 1000;
      this.inS = null;
      this.outS = null;
      this.ready = false;
      this.closed = false;
      this._build();
      this._bind();
      this._assemble();
      this._tick = setInterval(() => this._render(), 100);
      this._resize = () => this._place();
      window.addEventListener("resize", this._resize);
      window.addEventListener("scroll", this._resize, true);
    }

    // ---- DOM ----
    _build() {
      this.host = document.createElement("div");
      this.host.id = "gif-clipper-review-host";
      const root = this.host.attachShadow({ mode: "closed" });
      const style = document.createElement("style");
      style.textContent = `
        :host { all: initial; position: fixed; z-index: 2147483645; }
        .wrap { position: absolute; inset: 0; background: #000; display: flex; flex-direction: column;
          font: 13px/1.3 -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif; color: #e9eef5;
          border: 2px solid #2f7cf6; box-sizing: border-box; }
        video { flex: 1; min-height: 0; width: 100%; background: #000; object-fit: contain; }
        .ctl { flex: 0 0 auto; background: rgba(14,18,26,.96); padding: 8px 10px 10px; display: flex; flex-direction: column; gap: 6px; }
        .row { display: flex; align-items: center; gap: 8px; }
        .tl { position: relative; height: 22px; background: #1c2230; border-radius: 4px; cursor: pointer; user-select: none; }
        .tl .buf { position: absolute; top: 0; bottom: 0; background: #2a3448; border-radius: 4px; }
        .tl .sel { position: absolute; top: 0; bottom: 0; background: rgba(47,124,246,.45); border-left: 2px solid #4ea1ff; border-right: 2px solid #4ea1ff; box-sizing: border-box; }
        .tl .head { position: absolute; top: -3px; bottom: -3px; width: 2px; background: #fff; }
        .tl .mark { position: absolute; top: 0; bottom: 0; width: 2px; background: #ffc46b; }
        .time { font-variant-numeric: tabular-nums; min-width: 86px; }
        button { font: inherit; border: 0; border-radius: 6px; padding: 5px 10px; cursor: pointer; color: #fff; background: #2f3a4f; white-space: nowrap; }
        button.primary { background: #2f7cf6; }
        button:disabled { opacity: .45; cursor: default; }
        kbd { font: 11px/1 ui-monospace, Menlo, monospace; opacity: .7; margin-left: 4px; }
        .hint { color: #9fb0c8; margin-left: auto; white-space: nowrap; }
        .hint.fail { color: #ff7b7b; }
        .range { color: #ffc46b; font-variant-numeric: tabular-nums; }
        .loading { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; color: #9fb0c8; background: rgba(0,0,0,.6); pointer-events: none; }
      `;
      const wrap = document.createElement("div");
      wrap.className = "wrap";
      wrap.innerHTML = `
        <video muted playsinline></video>
        <div class="loading" data-role="loading">正在拼接缓冲…</div>
        <div class="ctl">
          <div class="tl" data-role="tl">
            <div class="buf" data-role="buf"></div>
            <div class="sel" data-role="sel" style="display:none"></div>
            <div class="mark" data-role="in" style="display:none"></div>
            <div class="mark" data-role="out" style="display:none"></div>
            <div class="head" data-role="head"></div>
          </div>
          <div class="row">
            <button data-role="play" title="播放/暂停（空格）">▶</button>
            <span class="time" data-role="time">0:00 / 0:00</span>
            <button data-role="back5" title="后退 5 秒（Shift+←）">−5s</button>
            <button data-role="back1" title="后退 1 秒（←）">−1s</button>
            <button data-role="fwd1" title="前进 1 秒（→）">+1s</button>
            <button data-role="fwd5" title="前进 5 秒（Shift+→）">+5s</button>
            <button data-role="setin" title="当前画面设为起点（[）">起点<kbd>[</kbd></button>
            <button data-role="setout" title="当前画面设为终点（]）">终点<kbd>]</kbd></button>
            <span class="range" data-role="range"></span>
            <button data-role="ok" class="primary" disabled title="生成 GIF（Enter）">生成<kbd>Enter</kbd></button>
            <button data-role="close" title="关闭（Esc）">关闭<kbd>Esc</kbd></button>
            <span class="hint" data-role="hint">拖时间轴找画面</span>
          </div>
        </div>`;
      root.append(style, wrap);
      this.ui = {};
      wrap.querySelectorAll("[data-role]").forEach((el) => (this.ui[el.dataset.role] = el));
      this.video = wrap.querySelector("video");
      (document.fullscreenElement || document.documentElement).appendChild(this.host);
      this._place();
    }

    _place() {
      const r = this.anchor.getBoundingClientRect();
      const minH = 160;
      const h = Math.max(r.height, minH);
      Object.assign(this.host.style, {
        left: `${Math.max(0, r.left)}px`,
        top: `${Math.max(0, r.top)}px`,
        width: `${Math.max(320, r.width)}px`,
        height: `${h}px`,
      });
    }

    _bind() {
      const u = this.ui;
      u.play.addEventListener("click", () => this.togglePlay());
      u.back5.addEventListener("click", () => this.seekBy(-5));
      u.back1.addEventListener("click", () => this.seekBy(-1));
      u.fwd1.addEventListener("click", () => this.seekBy(1));
      u.fwd5.addEventListener("click", () => this.seekBy(5));
      u.setin.addEventListener("click", () => this.setIn());
      u.setout.addEventListener("click", () => this.setOut());
      u.ok.addEventListener("click", () => this.confirm());
      u.close.addEventListener("click", () => this.close());
      const scrub = (e) => {
        const rect = u.tl.getBoundingClientRect();
        const f = Math.min(1, Math.max(0, (e.clientX - rect.left) / rect.width));
        this.seekTo(f * this.total);
      };
      u.tl.addEventListener("pointerdown", (e) => {
        u.tl.setPointerCapture(e.pointerId);
        this.video.pause();
        scrub(e);
        const move = (ev) => scrub(ev);
        const up = () => {
          u.tl.removeEventListener("pointermove", move);
          u.tl.removeEventListener("pointerup", up);
        };
        u.tl.addEventListener("pointermove", move);
        u.tl.addEventListener("pointerup", up);
      });
      this._onKey = (e) => {
        if (this.closed) return;
        const k = e.key;
        let handled = true;
        if (k === "Escape") this.close();
        else if (k === "Enter") this.confirm();
        else if (k === " ") this.togglePlay();
        else if (k === "ArrowLeft") this.seekBy(e.shiftKey ? -5 : -1);
        else if (k === "ArrowRight") this.seekBy(e.shiftKey ? 5 : 1);
        else if (k === ",") this.seekBy(-FRAME_S);
        else if (k === ".") this.seekBy(FRAME_S);
        else if (k === "[") this.setIn();
        else if (k === "]") this.setOut();
        else handled = false;
        if (handled) {
          e.preventDefault();
          e.stopImmediatePropagation();
        }
      };
      window.addEventListener("keydown", this._onKey, true);
    }

    // ---- MSE ----
    async _assemble() {
      const ms = new MediaSource();
      this.ms = ms;
      this.video.src = URL.createObjectURL(ms);
      await new Promise((r) => ms.addEventListener("sourceopen", r, { once: true }));
      let sb;
      try {
        sb = ms.addSourceBuffer(this.mimeType);
      } catch (err) {
        this._fail(`浏览器不支持回放此格式（${this.mimeType}）`);
        return;
      }
      sb.mode = "segments";
      // Newest first: the default position (now − 10 s) becomes playable
      // after one append instead of ten.
      const order = [...this.segments].reverse();
      for (const seg of order) {
        if (this.closed) return;
        try {
          const buf = await seg.blob.arrayBuffer();
          if (sb.updating) await this._updateEnd(sb);
          sb.abort();
          sb.timestampOffset = (seg.startEpochMs - this.base) / 1000;
          const done = this._updateEnd(sb);
          sb.appendBuffer(buf);
          await done;
        } catch (err) {
          this._fail(`拼接失败：${err && err.message}`);
          return;
        }
        if (!this.ready) {
          this.ready = true;
          this.ui.loading.style.display = "none";
          this.seekTo(Math.max(0, this.total - DEFAULT_LOOKBACK_S));
        }
      }
      try {
        if (ms.readyState === "open") ms.endOfStream();
      } catch (_err) {
        /* fine */
      }
      this.ui.hint.textContent = `缓冲 ${this._fmt(this.total)}，拖时间轴找画面`;
    }

    _updateEnd(sb) {
      return new Promise((resolve, reject) => {
        const ok = () => {
          sb.removeEventListener("error", bad);
          resolve();
        };
        const bad = () => {
          sb.removeEventListener("updateend", ok);
          reject(new Error("SourceBuffer error"));
        };
        sb.addEventListener("updateend", ok, { once: true });
        sb.addEventListener("error", bad, { once: true });
      });
    }

    _fail(text) {
      this.ui.loading.textContent = text;
      this.ui.hint.textContent = text;
      this.ui.hint.className = "hint fail";
      toast(text, "fail", 4000);
    }

    // ---- transport ----
    togglePlay() {
      if (!this.ready) return;
      if (this.video.paused) this.video.play().catch(() => null);
      else this.video.pause();
    }

    seekBy(delta) {
      this.seekTo(this.video.currentTime + delta);
    }

    seekTo(t) {
      if (!this.ready) return;
      const clamped = Math.min(Math.max(0, t), Math.max(0, this.total - 0.05));
      this.video.pause();
      this.video.currentTime = clamped;
    }

    setIn() {
      if (!this.ready) return;
      this.inS = this.video.currentTime;
      if (this.outS != null && this.outS <= this.inS) this.outS = null;
      this._render();
    }

    setOut() {
      if (!this.ready) return;
      this.outS = this.video.currentTime;
      if (this.inS != null && this.outS <= this.inS) {
        toast("终点必须在起点之后", "fail");
        this.outS = null;
      }
      this._render();
    }

    _valid() {
      if (this.inS == null || this.outS == null) return false;
      const len = this.outS - this.inS;
      return len >= 0.5 && len <= this.maxRange;
    }

    confirm() {
      if (!this._valid()) {
        if (this.inS != null && this.outS != null) toast(`区间需在 0.5~${this.maxRange} 秒之间`, "fail");
        else toast("先用 [ 和 ] 设起点和终点", "warn");
        return;
      }
      const range = {
        startMs: Math.round(this.base + this.inS * 1000),
        endMs: Math.round(this.base + this.outS * 1000),
      };
      this.close();
      this.onConfirm(range);
    }

    close() {
      if (this.closed) return;
      this.closed = true;
      clearInterval(this._tick);
      window.removeEventListener("keydown", this._onKey, true);
      window.removeEventListener("resize", this._resize);
      window.removeEventListener("scroll", this._resize, true);
      try {
        this.video.pause();
        this.video.removeAttribute("src");
        this.video.load();
      } catch (_err) {
        /* fine */
      }
      this.host.remove();
      if (this.onClose) this.onClose();
    }

    // ---- render ----
    _fmt(s) {
      const m = Math.floor(s / 60);
      const r = s - m * 60;
      return `${m}:${r.toFixed(1).padStart(4, "0")}`;
    }

    _render() {
      if (this.closed) return;
      const u = this.ui;
      const t = this.video.currentTime || 0;
      const pct = (x) => `${(this.total ? (x / this.total) * 100 : 0).toFixed(3)}%`;
      u.head.style.left = pct(t);
      u.time.textContent = `${this._fmt(t)} / ${this._fmt(this.total)}`;
      u.play.textContent = this.video.paused ? "▶" : "❚❚";
      // Buffered span (grows while appending newest → oldest).
      const b = this.video.buffered;
      if (b.length) {
        u.buf.style.left = pct(b.start(0));
        u.buf.style.width = pct(b.end(b.length - 1) - b.start(0));
      }
      u.in.style.display = this.inS == null ? "none" : "";
      u.out.style.display = this.outS == null ? "none" : "";
      if (this.inS != null) u.in.style.left = pct(this.inS);
      if (this.outS != null) u.out.style.left = pct(this.outS);
      if (this.inS != null && this.outS != null) {
        u.sel.style.display = "";
        u.sel.style.left = pct(this.inS);
        u.sel.style.width = pct(this.outS - this.inS);
        u.range.textContent = `${(this.outS - this.inS).toFixed(1)}s`;
      } else {
        u.sel.style.display = "none";
        u.range.textContent = this.inS != null ? `起点 ${this._fmt(this.inS)}` : "";
      }
      u.ok.disabled = !this._valid();
    }
  }

  window.__gifClipperReview = { ReviewOverlay };
})();
