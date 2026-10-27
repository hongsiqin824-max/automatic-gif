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
      this.captured = null;      // what captureStream() returned (gets the new track on a source swap)
      this.stream = null;        // video-only stream the MediaRecorder records
      this.track = null;         // the video track being recorded
      this.recorder = null;
      this.rotateTimer = null;
      this.startMs = null;       // operator-set start (Date.now())
      this.liveStartMs = null;   // Date.now() when the current (open) segment began
      this.recapturedAt = 0;     // rate limit for recapture()
      this.recaptureTimer = null;
      this.pending = false;      // recapture failed (no track yet); retry on ensureLive()
      this.awaitingFrames = false; // source not playing at rotate time; open a segment on ensureLive()
      this.stopped = false;
      this.mimeType = ["video/webm;codecs=vp8", "video/webm;codecs=vp9", "video/webm"]
        .find((m) => MediaRecorder.isTypeSupported(m)) || "";
    }

    start() {
      this.stopped = false;
      if (!this._capture()) return false;
      this._rotate();
      return true;
    }

    // Grab a fresh stream from the same <video>. When the player swaps
    // ``src`` / its MediaSource on the same element, Chrome does NOT end the
    // old track: it silently stops delivering frames and a new track is added
    // to the captured stream (verified 2026-10-27, Chrome 156, both kinds of
    // swap). So follow ``addtrack`` and recapture in place instead of
    // treating it as a new source (buffer is kept).
    _capture(quiet = false) {
      let captured;
      try {
        captured = this.video.captureStream();
      } catch (err) {
        if (!quiet) this.onStatus(`captureStream 失败：${err.name}`, "fail");
        return false;
      }
      const videoTracks = captured.getVideoTracks();
      if (!videoTracks.length) {
        if (!quiet) this.onStatus("视频轨道为空", "fail");
        return false;
      }
      // Video only: GIFs have no sound, and a webm with an opus track would
      // not match the video-only codec string the review overlay hands to MSE.
      const track = videoTracks[0];
      this.captured = captured;
      this.track = track;
      this.stream = new MediaStream([track]);
      captured.addEventListener("addtrack", (e) => {
        // Chrome also fires addtrack for the track captureStream() returned
        // with, so only a different video track means a source swap.
        if (e.track.kind !== "video" || this.captured !== captured) return;
        if (this.track && e.track.id === this.track.id) return;
        this.recapture();
      });
      track.addEventListener("ended", () => {
        if (this.track === track) this.recapture();
      });
      return true;
    }

    // Stop every captured track (incl. audio and any track a source swap
    // added) so no capture keeps running behind our back.
    _release() {
      if (this.captured) this.captured.getTracks().forEach((t) => t.stop());
      if (this.stream) this.stream.getTracks().forEach((t) => t.stop());
      this.captured = null;
      this.stream = null;
      this.track = null;
    }

    // Close the open segment, drop the dead stream and capture the same
    // element again. Segments already recorded stay in the buffer. Right
    // after a src swap the element may have no track yet; then we mark
    // ``pending`` and the finder calls ``ensureLive()`` on loadedmetadata.
    recapture() {
      if (this.stopped || !this.video.isConnected) return false;
      // At most one recapture per 500 ms, but never drop one: a second swap
      // right after the first must still be followed, so defer instead.
      const wait = this.recapturedAt + 500 - Date.now();
      if (!this.pending && wait > 0) {
        clearTimeout(this.recaptureTimer);
        this.recaptureTimer = setTimeout(() => this.recapture(), wait);
        return true;
      }
      clearTimeout(this.recaptureTimer);
      this.recapturedAt = Date.now();
      this.pending = false;
      clearTimeout(this.rotateTimer);
      const live = this.recorder;
      this.recorder = null;
      this._halt(live);
      this._release();
      if (!this._capture(true)) {
        this.pending = true;
        return false;
      }
      this._rotate();
      this.onStatus("播放器重连，继续录制", "warn");
      return true;
    }

    ensureLive() {
      if (this.pending) return this.recapture();
      if (this.awaitingFrames) {
        this._rotate();
        return !!this.recorder;
      }
      return true;
    }

    // Frames are flowing: playing, with decoded data ahead. Chrome's webm
    // muxer stamps a segment relative to its FIRST FRAME, not start(), so a
    // segment opened while the source is paused / stalled would claim a
    // wall-clock start earlier than its footage, and the server (which
    // concatenates by media length) would land on the wrong moment.
    _sourceLive() {
      const v = this.video;
      return !v.paused && !v.ended && v.readyState >= 3;
    }

    _rotate() {
      if (this.stopped) return;
      const prev = this.recorder;
      clearTimeout(this.rotateTimer);
      if (!this.stream || !this._sourceLive()) {
        // Close what we have and wait for "playing" (finder → ensureLive()).
        this.recorder = null;
        this.liveStartMs = null;
        this.awaitingFrames = true;
        this._halt(prev);
        return;
      }
      this.awaitingFrames = false;
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
        // What the encoder actually produces (may add codec details).
        if (rec.mimeType) this.mimeType = rec.mimeType;
      };
      rec.onstop = () => {
        // ``stopAt`` is taken synchronously in _halt(); the stop event only
        // fires after the last data is flushed.
        entry.durationMs = (rec.__stopAt || Date.now()) - entry.startEpochMs;
        entry.blob = new Blob(chunks, { type: this.mimeType || "video/webm" });
        // A header-only stub (two closes within a few ms) would be rejected
        // by the server's duration > 0 check and fail the whole upload.
        if (entry.blob.size && entry.durationMs >= 200) {
          this.segments.push(entry);
          this._trim();
        }
      };
      rec.onerror = () => this.onStatus("录制出错，缓冲重置", "fail");
      this.recorder = rec;
      // Timestamp taken right here, not in onstart: the start event is a
      // queued task and lands late whenever the main thread is busy (e.g.
      // base64-encoding an upload), which would place the segment late on
      // the review timeline and widen the overlap at the seam.
      entry.startEpochMs = Date.now();
      this.liveStartMs = entry.startEpochMs;
      rec.start(1000);
      this._halt(prev);
      this.rotateTimer = setTimeout(() => this._rotate(), SEGMENT_MS);
    }

    _halt(rec) {
      if (!rec || rec.state === "inactive") return false;
      rec.__stopAt = Date.now();
      rec.stop();
      return true;
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
    // ``range`` = {startMs, endMs, crop?}; the caller resolves the Alt+1
    // start point and "now" itself (and clears the start point), so a clip
    // from the review window never consumes a pending Alt+1 start.
    async finish(range) {
      const { startMs, endMs } = range;
      await this.closeLiveSegment();
      const picked = selectSegments(this.segments, startMs, endMs);
      if (!picked.length) throw new Error("起点已超出缓冲，请重新设起点");
      return { startMs, endMs, segments: picked, crop: range.crop || null };
    }

    // Stop the open segment (so it becomes a playable file) and start a new
    // one right away. Used before finishing a clip and before a review so the
    // last few seconds are included.
    async closeLiveSegment() {
      clearTimeout(this.rotateTimer);
      const live = this.recorder;
      if (live && live.state !== "inactive") {
        await new Promise((resolve) => {
          const prevStop = live.onstop;
          live.onstop = (e) => {
            prevStop(e);
            resolve();
          };
          this._halt(live);
        });
      }
      if (!this.stopped) this._rotate();
    }

    // Shallow copy of the closed segments for the review overlay.
    snapshot() {
      return this.segments.map((s) => ({ blob: s.blob, startEpochMs: s.startEpochMs, durationMs: s.durationMs }));
    }

    stop() {
      this.stopped = true;
      clearTimeout(this.rotateTimer);
      clearTimeout(this.recaptureTimer);
      this._halt(this.recorder);
      this._release();
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
