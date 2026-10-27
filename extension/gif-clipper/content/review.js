// Review window: play back the rolling buffer in a draggable, resizable
// floating panel next to the live player so the operator can scrub to the
// exact moment and mark [start, end] while still seeing the live feed.
//
// Lives in the same frame as the recorder (the <video> may sit in a
// cross-origin iframe; the blobs are here, so the player is here too). The
// segments are stitched into one timeline with MediaSource: each 30 s webm
// is appended at timestampOffset = segment.start - buffer.start. MediaRecorder
// webm leaves its last cluster open, so the SourceBuffer must be abort()ed
// before changing the offset (verified 2026-10-27 in a probe page).
//
// Keys while open: Space play/pause, ←/→ ±1 s, Shift+←/→ ±5 s, , . ±1 frame,
// [ set start, ] set end, C toggle crop box, Enter confirm, Esc close.
//
// Pure helper `unstickTarget` is exported for node tests (no DOM needed).
(() => {
  // Segments do not always touch: a stall or a paused source leaves a hole
  // in the wall-clock timeline, and the element sits in "waiting" forever at
  // a hole. Returns where to move the playhead, or null to leave it.
  // ``ranges`` = [[start, end], ...] from video.buffered; ``stalled`` = the
  // element fired "waiting" (playback hit the tail of a range), otherwise
  // this is a seek and only a position outside every range is moved. Never
  // returns the current position: setting currentTime fires "seeking" again,
  // so a target equal to ``t`` would loop forever.
  function unstickTarget(ranges, t, stalled) {
    const EPS = 0.15;
    const n = ranges.length;
    if (!n) return null;
    for (let i = 0; i < n; i++) {
      const [start, end] = ranges[i];
      if (t < start) return start + 0.01; // in the hole before this range
      if (t < end) {
        if (stalled && t >= end - EPS && i + 1 < n) return ranges[i + 1][0] + 0.01;
        return null;
      }
    }
    // Past the last range: park on its last frame (only if not already there).
    const last = Math.max(ranges[n - 1][0], ranges[n - 1][1] - 0.05);
    return t > last + 0.01 ? last : null;
  }

  if (typeof module !== "undefined" && module.exports) {
    module.exports = { unstickTarget };
  }
  if (typeof window === "undefined") return;
  if (window.__gifClipperReview) return;

  const toast = window.__gifClipperToast || (() => {});
  const FRAME_S = 1 / 25;
  const DEFAULT_LOOKBACK_S = 10;
  const MIN_W = 360;
  const MIN_H = 240;
  const MIN_CROP = 0.1;   // smallest crop side as a fraction of the frame
  let lastBox = null;     // remembered window position/size within this page
  let lastCrop = null;    // remembered crop box {x,y,w,h} (0..1) within this page

  class ReviewOverlay {
    constructor({ anchor, segments, mimeType, maxRangeSeconds, onConfirm, onClose }) {
      this.anchor = anchor;               // the live <video>; only used for initial placement
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
      this.crop = null;                   // {x,y,w,h} fractions of the frame, or null = full frame
      this.ready = false;
      this.assembled = false;             // all segments appended; gap-jumping is safe
      this.closed = false;
      this._build();
      this._bind();
      this._assemble();
      this._tick = setInterval(() => this._render(), 100);
      this._resize = () => this._clamp();
      window.addEventListener("resize", this._resize);
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
          border: 2px solid #2f7cf6; border-radius: 8px; overflow: hidden; box-sizing: border-box;
          box-shadow: 0 12px 40px rgba(0,0,0,.6); }
        .bar { flex: 0 0 auto; display: flex; align-items: center; gap: 8px; padding: 6px 10px;
          background: #2f7cf6; color: #fff; cursor: move; user-select: none; touch-action: none; }
        .bar .title { font-weight: 600; }
        .bar .sub { opacity: .85; font-size: 12px; }
        .bar .x { margin-left: auto; cursor: pointer; opacity: .9; padding: 0 4px; font-size: 16px; line-height: 1; }
        .bar .x:hover { opacity: 1; }
        .stage { position: relative; flex: 1; min-height: 0; }
        video { position: absolute; inset: 0; width: 100%; height: 100%; background: #000; object-fit: contain; }
        .croplayer { position: absolute; overflow: hidden; display: none; touch-action: none; }
        .croplayer.on { display: block; }
        .cropbox { position: absolute; box-sizing: border-box; border: 1px solid #ffc46b; cursor: move;
          box-shadow: 0 0 0 9999px rgba(0,0,0,.55); }
        .cropbox .h { position: absolute; width: 12px; height: 12px; background: #ffc46b; border: 1px solid #000; box-sizing: border-box; }
        .cropbox .h[data-h="nw"] { left: -6px; top: -6px; cursor: nwse-resize; }
        .cropbox .h[data-h="ne"] { right: -6px; top: -6px; cursor: nesw-resize; }
        .cropbox .h[data-h="sw"] { left: -6px; bottom: -6px; cursor: nesw-resize; }
        .cropbox .h[data-h="se"] { right: -6px; bottom: -6px; cursor: nwse-resize; }
        .cropbox .h[data-h="n"] { left: calc(50% - 6px); top: -6px; cursor: ns-resize; }
        .cropbox .h[data-h="s"] { left: calc(50% - 6px); bottom: -6px; cursor: ns-resize; }
        .cropbox .h[data-h="w"] { left: -6px; top: calc(50% - 6px); cursor: ew-resize; }
        .cropbox .h[data-h="e"] { right: -6px; top: calc(50% - 6px); cursor: ew-resize; }
        .cropbox .lbl { position: absolute; left: 0; top: -20px; font-size: 11px; color: #ffc46b; white-space: nowrap; pointer-events: none; }
        .grip { position: absolute; right: 0; bottom: 0; width: 18px; height: 18px; cursor: nwse-resize; touch-action: none;
          background: linear-gradient(135deg, transparent 50%, rgba(255,255,255,.45) 50%, rgba(255,255,255,.45) 60%, transparent 60%, transparent 75%, rgba(255,255,255,.45) 75%, rgba(255,255,255,.45) 85%, transparent 85%); }
        .ctl { flex: 0 0 auto; background: rgba(14,18,26,.96); padding: 8px 10px 10px; display: flex; flex-direction: column; gap: 6px; }
        .row { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
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
        <div class="bar" data-role="bar">
          <span class="title">回看</span>
          <span class="sub">拖这里移动 · 右下角拉大小</span>
          <span class="x" data-role="x" title="关闭（Esc）">✕</span>
        </div>
        <div class="stage" data-role="stage">
          <video muted playsinline></video>
          <div class="croplayer" data-role="croplayer">
            <div class="cropbox" data-role="cropbox">
              <span class="lbl" data-role="croplbl"></span>
              <span class="h" data-h="nw"></span><span class="h" data-h="n"></span><span class="h" data-h="ne"></span>
              <span class="h" data-h="w"></span><span class="h" data-h="e"></span>
              <span class="h" data-h="sw"></span><span class="h" data-h="s"></span><span class="h" data-h="se"></span>
            </div>
          </div>
          <div class="loading" data-role="loading">正在拼接缓冲…</div>
        </div>
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
            <button data-role="cropbtn" title="框选画面区域（C）">裁剪<kbd>C</kbd></button>
            <button data-role="cropclear" title="取消裁剪，输出整幅画面" style="display:none">整幅</button>
            <span class="range" data-role="range"></span>
            <button data-role="ok" class="primary" disabled title="生成 GIF（Enter）">生成<kbd>Enter</kbd></button>
            <button data-role="close" title="关闭（Esc）">关闭<kbd>Esc</kbd></button>
            <span class="hint" data-role="hint">拖时间轴找画面</span>
          </div>
        </div>
        <div class="grip" data-role="grip" title="拖动调整大小"></div>`;
      root.append(style, wrap);
      this.ui = {};
      wrap.querySelectorAll("[data-role]").forEach((el) => (this.ui[el.dataset.role] = el));
      this.video = wrap.querySelector("video");
      this.video.addEventListener("waiting", () => this._unstick(true));
      this.video.addEventListener("seeking", () => this._unstick(false));
      (document.fullscreenElement || document.documentElement).appendChild(this.host);
      this._place();
    }

    // Initial box: last position in this page if any, else a window about
    // 60 % of the live player, offset to its lower-right so both stay visible.
    _place() {
      if (lastBox) {
        this._setBox(lastBox);
        return;
      }
      const r = this.anchor.getBoundingClientRect();
      const w = Math.max(MIN_W, Math.round(r.width * 0.6));
      const h = Math.max(MIN_H, Math.round(r.height * 0.6) + 90);
      this._setBox({
        left: r.left + r.width - w + 16,
        top: r.top + r.height - h + 16,
        width: w,
        height: h,
      });
    }

    _setBox(box) {
      const vw = window.innerWidth;
      const vh = window.innerHeight;
      const width = Math.min(Math.max(MIN_W, box.width), vw);
      const height = Math.min(Math.max(MIN_H, box.height), vh);
      const left = Math.min(Math.max(0, box.left), vw - width);
      const top = Math.min(Math.max(0, box.top), vh - height);
      this.box = { left, top, width, height };
      lastBox = this.box;
      Object.assign(this.host.style, {
        left: `${left}px`,
        top: `${top}px`,
        width: `${width}px`,
        height: `${height}px`,
      });
      if (this.crop) this._renderCrop();
    }

    _clamp() {
      if (this.box) this._setBox(this.box);
    }

    _bindWindow() {
      const u = this.ui;
      // Pointer-driven drag/resize; the callback maps the drag delta to a box.
      const track = (el, toBox) => {
        el.addEventListener("pointerdown", (e) => {
          if (e.button !== 0) return;
          e.preventDefault();
          const start = { x: e.clientX, y: e.clientY, box: { ...this.box } };
          el.setPointerCapture(e.pointerId);
          const move = (ev) => this._setBox(toBox(start, ev.clientX - start.x, ev.clientY - start.y));
          const up = () => {
            el.removeEventListener("pointermove", move);
            el.removeEventListener("pointerup", up);
            el.removeEventListener("pointercancel", up);
          };
          el.addEventListener("pointermove", move);
          el.addEventListener("pointerup", up);
          el.addEventListener("pointercancel", up);
        });
      };
      track(u.bar, (s, dx, dy) => ({ ...s.box, left: s.box.left + dx, top: s.box.top + dy }));
      track(u.grip, (s, dx, dy) => ({ ...s.box, width: s.box.width + dx, height: s.box.height + dy }));
      u.x.addEventListener("pointerdown", (e) => e.stopPropagation());
      u.x.addEventListener("click", () => this.close());
    }

    // ---- crop ----
    // The crop box lives in a layer that exactly covers the letterboxed
    // frame inside the stage, so box fractions map 1:1 onto the video.
    _frameRect() {
      const sw = this.ui.stage.clientWidth;
      const sh = this.ui.stage.clientHeight;
      const vw = this.video.videoWidth || 16;
      const vh = this.video.videoHeight || 9;
      const s = Math.min(sw / vw, sh / vh);
      const w = vw * s;
      const h = vh * s;
      return { left: (sw - w) / 2, top: (sh - h) / 2, width: w, height: h };
    }

    toggleCrop() {
      if (!this.ready) return;
      if (this.crop) {
        this.clearCrop();
        return;
      }
      this.crop = lastCrop ? { ...lastCrop } : { x: 0.2, y: 0.2, w: 0.6, h: 0.6 };
      this._renderCrop();
    }

    clearCrop() {
      this.crop = null;
      lastCrop = null;
      this._renderCrop();
    }

    _setCrop(c) {
      const w = Math.min(1, Math.max(MIN_CROP, c.w));
      const h = Math.min(1, Math.max(MIN_CROP, c.h));
      const x = Math.min(Math.max(0, c.x), 1 - w);
      const y = Math.min(Math.max(0, c.y), 1 - h);
      this.crop = { x, y, w, h };
      lastCrop = this.crop;
      this._renderCrop();
    }

    _renderCrop() {
      const u = this.ui;
      u.croplayer.classList.toggle("on", !!this.crop);
      u.cropbtn.classList.toggle("primary", !!this.crop);
      u.cropclear.style.display = this.crop ? "" : "none";
      if (!this.crop) return;
      const f = this._frameRect();
      Object.assign(u.croplayer.style, {
        left: `${f.left}px`, top: `${f.top}px`, width: `${f.width}px`, height: `${f.height}px`,
      });
      const c = this.crop;
      Object.assign(u.cropbox.style, {
        left: `${c.x * 100}%`, top: `${c.y * 100}%`, width: `${c.w * 100}%`, height: `${c.h * 100}%`,
      });
      u.croplbl.textContent = `${Math.round(c.w * 100)}% × ${Math.round(c.h * 100)}%`;
    }

    _bindCrop() {
      const u = this.ui;
      u.cropbtn.addEventListener("click", () => this.toggleCrop());
      u.cropclear.addEventListener("click", () => this.clearCrop());
      u.cropbox.addEventListener("pointerdown", (e) => {
        if (e.button !== 0 || !this.crop) return;
        e.preventDefault();
        e.stopPropagation();
        const handle = e.target.dataset ? e.target.dataset.h || "" : "";
        const layer = u.croplayer.getBoundingClientRect();
        const start = { x: e.clientX, y: e.clientY, c: { ...this.crop } };
        u.cropbox.setPointerCapture(e.pointerId);
        const move = (ev) => {
          const dx = (ev.clientX - start.x) / layer.width;
          const dy = (ev.clientY - start.y) / layer.height;
          this._setCrop(this._dragCrop(start.c, handle, dx, dy));
        };
        const up = () => {
          u.cropbox.removeEventListener("pointermove", move);
          u.cropbox.removeEventListener("pointerup", up);
          u.cropbox.removeEventListener("pointercancel", up);
        };
        u.cropbox.addEventListener("pointermove", move);
        u.cropbox.addEventListener("pointerup", up);
        u.cropbox.addEventListener("pointercancel", up);
      });
    }

    // Apply a drag delta (fractions) to the crop from ``handle``: "" = move,
    // otherwise one of n/s/e/w/ne/nw/se/sw. Edges never cross each other.
    _dragCrop(c, handle, dx, dy) {
      if (!handle) return { ...c, x: c.x + dx, y: c.y + dy };
      let x0 = c.x;
      let y0 = c.y;
      let x1 = c.x + c.w;
      let y1 = c.y + c.h;
      if (handle.includes("w")) x0 = Math.min(Math.max(0, x0 + dx), x1 - MIN_CROP);
      if (handle.includes("e")) x1 = Math.max(Math.min(1, x1 + dx), x0 + MIN_CROP);
      if (handle.includes("n")) y0 = Math.min(Math.max(0, y0 + dy), y1 - MIN_CROP);
      if (handle.includes("s")) y1 = Math.max(Math.min(1, y1 + dy), y0 + MIN_CROP);
      return { x: x0, y: y0, w: x1 - x0, h: y1 - y0 };
    }

    _bind() {
      this._bindWindow();
      this._bindCrop();
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
        else if (k === "c" || k === "C") this.toggleCrop();
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
      // after one append instead of ten. Each segment's tail runs a few
      // frames past the next segment's wall-clock start (the new recorder
      // starts before the old one stops), and a later append overwrites
      // earlier data at the same time, so without a limit an older segment
      // would clobber the newer one's only keyframe and freeze that whole
      // segment. appendWindowEnd clips the older segment at the newer
      // segment's start instead.
      const order = [...this.segments].reverse();
      let windowEnd = Infinity;
      for (const seg of order) {
        if (this.closed) return;
        const offset = (seg.startEpochMs - this.base) / 1000;
        try {
          const buf = await seg.blob.arrayBuffer();
          if (sb.updating) await this._updateEnd(sb);
          sb.abort();
          sb.timestampOffset = offset;
          sb.appendWindowEnd = windowEnd;
          const done = this._updateEnd(sb);
          sb.appendBuffer(buf);
          await done;
        } catch (err) {
          this._fail(`拼接失败：${err && err.message}`);
          return;
        }
        windowEnd = offset;
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
      this.assembled = true;
      this._unstick(!this.video.paused);
      this.ui.hint.textContent = `缓冲 ${this._fmt(this.total)}，拖时间轴找画面`;
    }

    // Move the playhead off a hole / past-the-end position (see unstickTarget).
    _unstick(stalled) {
      if (!this.assembled || this.closed) return;
      const v = this.video;
      const ranges = [];
      for (let i = 0; i < v.buffered.length; i++) ranges.push([v.buffered.start(i), v.buffered.end(i)]);
      const target = unstickTarget(ranges, v.currentTime, stalled);
      if (target == null) return;
      if (target < v.currentTime) v.pause(); // parked on the last frame
      v.currentTime = target;
    }

    _updateEnd(sb) {
      return new Promise((resolve, reject) => {
        const ok = () => {
          sb.removeEventListener("error", bad);
          resolve();
        };
        const bad = () => {
          sb.removeEventListener("updateend", ok);
          reject(new Error(`SourceBuffer error（${this.mimeType}）`));
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
      const round4 = (v) => Math.round(v * 10000) / 10000;
      const range = {
        startMs: Math.round(this.base + this.inS * 1000),
        endMs: Math.round(this.base + this.outS * 1000),
        crop: this.crop
          ? { x: round4(this.crop.x), y: round4(this.crop.y), w: round4(this.crop.w), h: round4(this.crop.h) }
          : null,
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
