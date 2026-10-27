// Rolling buffer recorder for one <video>. Segmented MediaRecorder: a new
// recorder every SEGMENT_MS on the same captured stream, so every segment is
// an independently playable webm (a timesliced single recorder only puts the
// header in its first chunk, so old chunks could never be dropped).
//
// Pure helper `selectSegments` is exported for node tests (no DOM needed).
(() => {
  const SEGMENT_MS = 30_000;

  function selectSegments(segments, startMs, endMs) {
    // Keep every segment that overlaps [startMs - 1s, endMs]; segments are
    // already in time order. Returns [] when the start fell off the buffer.
    const picked = [];
    for (const seg of segments) {
      const segEnd = seg.startEpochMs + seg.durationMs;
      if (segEnd < startMs - 1000) continue;
      if (seg.startEpochMs > endMs) break;
      picked.push(seg);
    }
    if (!picked.length) return [];
    const first = picked[0];
    if (startMs < first.startEpochMs - 1000) return [];
    return picked;
  }

  class RollingRecorder {
    constructor(video, { bufferSeconds = 300, bitrate = 2_000_000, onStatus = () => {} } = {}) {
      this.video = video;
      this.bufferMs = bufferSeconds * 1000;
      this.bitrate = bitrate;
      this.onStatus = onStatus;
      this.segments = [];        // {blob, startEpochMs, durationMs}
      this.stream = null;
      this.recorder = null;
      this.rotateTimer = null;
      this.startMs = null;       // operator-set start (Date.now())
      this.liveStartMs = null;   // Date.now() when the current (open) segment began
      this.stopped = false;
      this.mimeType = ["video/webm;codecs=vp8", "video/webm;codecs=vp9", "video/webm"]
        .find((m) => MediaRecorder.isTypeSupported(m)) || "";
    }

    start() {
      this.stopped = false;
      try {
        this.stream = this.video.captureStream();
      } catch (err) {
        this.onStatus(`captureStream 失败：${err.name}`, "fail");
        return false;
      }
      if (!this.stream.getVideoTracks().length) {
        this.onStatus("视频轨道为空", "fail");
        return false;
      }
      this._rotate();
      return true;
    }

    _rotate() {
      if (this.stopped) return;
      const prev = this.recorder;
      const chunks = [];
      let rec;
      try {
        rec = new MediaRecorder(this.stream, {
          mimeType: this.mimeType || undefined,
          videoBitsPerSecond: this.bitrate,
        });
      } catch (err) {
        this.onStatus(`MediaRecorder 失败：${err.name}`, "fail");
        return;
      }
      const entry = { blob: null, startEpochMs: 0, durationMs: 0 };
      rec.ondataavailable = (e) => {
        if (e.data && e.data.size) chunks.push(e.data);
      };
      rec.onstart = () => {
        entry.startEpochMs = Date.now();
        this.liveStartMs = entry.startEpochMs;
      };
      rec.onstop = () => {
        entry.durationMs = Date.now() - entry.startEpochMs;
        entry.blob = new Blob(chunks, { type: this.mimeType || "video/webm" });
        if (entry.blob.size) {
          this.segments.push(entry);
          this._trim();
        }
      };
      rec.onerror = () => this.onStatus("录制出错，缓冲重置", "fail");
      this.recorder = rec;
      rec.start(1000);
      if (prev && prev.state !== "inactive") prev.stop();
      this.rotateTimer = setTimeout(() => this._rotate(), SEGMENT_MS);
    }

    _trim() {
      const cutoff = Date.now() - this.bufferMs;
      while (this.segments.length && this.segments[0].startEpochMs + this.segments[0].durationMs < cutoff) {
        this.segments.shift();
      }
    }

    bufferedSeconds() {
      // Oldest footage we hold: the first closed segment, or the segment
      // still being recorded when nothing has rotated out yet.
      let oldest = this.segments.length ? this.segments[0].startEpochMs : null;
      if (oldest == null && this.liveStartMs != null) oldest = this.liveStartMs;
      if (oldest == null) return 0;
      return Math.max(0, Math.round((Date.now() - oldest) / 1000));
    }

    setStart() {
      this.startMs = Date.now();
      return this.startMs;
    }

    clearStart() {
      this.startMs = null;
    }

    // Close the live segment so its footage is included, then pick segments.
    async finish(endMs = Date.now()) {
      if (this.startMs == null) throw new Error("还没有设起点");
      const startMs = this.startMs;
      clearTimeout(this.rotateTimer);
      const live = this.recorder;
      if (live && live.state !== "inactive") {
        await new Promise((resolve) => {
          const prevStop = live.onstop;
          live.onstop = (e) => {
            prevStop(e);
            resolve();
          };
          live.stop();
        });
      }
      // Resume rolling immediately so the next capture has footage.
      if (!this.stopped) this._rotate();
      const picked = selectSegments(this.segments, startMs, endMs);
      this.startMs = null;
      if (!picked.length) throw new Error("起点已超出缓冲，请重新设起点");
      return { startMs, endMs, segments: picked };
    }

    stop() {
      this.stopped = true;
      clearTimeout(this.rotateTimer);
      if (this.recorder && this.recorder.state !== "inactive") this.recorder.stop();
      if (this.stream) this.stream.getTracks().forEach((t) => t.stop());
      this.segments = [];
      this.startMs = null;
    }
  }

  if (typeof window !== "undefined") {
    window.__gifClipperRecorder = { RollingRecorder, selectSegments, SEGMENT_MS };
  }
  if (typeof module !== "undefined" && module.exports) {
    module.exports = { selectSegments, SEGMENT_MS };
  }
})();
