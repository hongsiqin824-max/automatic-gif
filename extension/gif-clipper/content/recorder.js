// Rolling buffer recorder for one <video>. Segmented MediaRecorder: a new
// recorder every SEGMENT_MS on the same captured stream, so every segment is
// an independently playable webm (a timesliced single recorder only puts the
// header in its first chunk, so old chunks could never be dropped).
//
// Pure helper `selectSegments` is exported for node tests (no DOM needed).
(() => {
  const SEGMENT_MS = 30_000;
  const KEYFRAME_MS = 2000;

  // ---- webm cluster slicing ----------------------------------------------
  // Chrome's MediaRecorder puts a keyframe (and a new Cluster) every
  // videoKeyFrameIntervalDuration, so a segment can be cut down to "the
  // keyframe just before the clip, through the keyframe just after it"
  // instead of uploading the whole 30 s. The cut is a byte slice of the
  // original blob: the EBML header up to the first Cluster, then the kept
  // Clusters. Anything we cannot parse returns null and the caller uploads
  // the whole segment, so a bad parse can never produce a broken upload.

  const CLUSTER_ID = 0x1f43b675;
  const TIMECODE_ID = 0xe7;
  const TIMECODE_SCALE_ID = 0x2ad7b1;
  const DURATION_ID = 0x4489;

  function readVint(bytes, pos, keepMarker) {
    if (pos >= bytes.length) return null;
    const lead = bytes[pos];
    let mask = 0x80;
    let length = 1;
    while (length <= 8 && (lead & mask) === 0) {
      mask >>= 1;
      length += 1;
    }
    if (length > 8 || pos + length > bytes.length) return null;
    let value = keepMarker ? lead : lead & (mask - 1);
    for (let i = 1; i < length; i++) value = value * 256 + bytes[pos + i];
    if (!keepMarker && value === Math.pow(2, 7 * length) - 1) value = -1; // unknown size
    return { value, next: pos + length };
  }

  // Walk the element tree. ``stopId`` ends the walk when that element id is
  // reached at this level (used to stop inside a Cluster once its Timecode
  // is found, so frame data is never scanned).
  function walkElements(bytes, pos, end, onElement, stopId) {
    while (pos < end) {
      const elementPos = pos;
      const id = readVint(bytes, pos, true);
      if (!id) return null;
      const size = readVint(bytes, id.next, false);
      if (!size) return null;
      if (stopId != null && id.value === stopId) return pos;
      const unknown = size.value < 0;
      const childEnd = unknown ? end : size.next + size.value;
      if (childEnd > end) return null;
      const resume = onElement(id.value, size.next, childEnd, unknown, elementPos);
      if (resume === false) return pos;
      if (typeof resume === "number") return resume;
      if (unknown) return null; // cannot skip past an unknown-size element
      pos = childEnd;
    }
    return pos;
  }

  // Find every top-level Cluster from ``from`` on. Chrome's MediaRecorder
  // muxes live, so it writes Clusters (and the Segment) with *unknown* size:
  // a cluster's extent cannot be read from its header, the only way to bound
  // it is to find where the next one starts (verified Chrome 156, 2026-10-29).
  // Scanning for the 4-byte Cluster id alone would be fragile, so a hit only
  // counts when it carries a readable size vint followed by a Timecode
  // element of 1-8 bytes. A cluster that does state its size (ffmpeg-written
  // files) is skipped over directly, so frame data is never scanned there.
  function scanClusters(bytes, from) {
    const found = [];
    for (let i = from; i + 4 <= bytes.length; i++) {
      if (bytes[i] !== 0x1f || bytes[i + 1] !== 0x43 || bytes[i + 2] !== 0xb6 || bytes[i + 3] !== 0x75) {
        continue;
      }
      const size = readVint(bytes, i + 4, false);
      if (!size) continue;
      const tcId = readVint(bytes, size.next, true);
      if (!tcId || tcId.value !== TIMECODE_ID) continue;
      const tcSize = readVint(bytes, tcId.next, false);
      if (!tcSize || tcSize.value < 1 || tcSize.value > 8) continue;
      if (tcSize.next + tcSize.value > bytes.length) continue;
      let timecode = 0;
      for (let k = tcSize.next; k < tcSize.next + tcSize.value; k++) {
        timecode = timecode * 256 + bytes[k];
      }
      const knownEnd = size.value >= 0 ? size.next + size.value : null;
      if (knownEnd != null && knownEnd > bytes.length) continue;
      found.push({ offset: i, timecodeRaw: timecode, knownEnd, tcPos: tcSize.next, tcLen: tcSize.value });
      i = (knownEnd != null ? knownEnd : tcSize.next + tcSize.value) - 1;
    }
    return found;
  }

  // [{offset, end, timecodeMs}] for every Cluster, in file order. null = give up
  // and let the caller upload the whole segment.
  function indexClusters(bytes) {
    let segmentPos = null;
    const head = walkElements(bytes, 0, bytes.length, (id, content) => {
      if (id === 0x18538067) { // Segment
        segmentPos = content;
        return false;
      }
      return null;
    });
    if (head == null || segmentPos == null) return null;
    let scale = 1_000_000; // Matroska default: 1 ms per timecode unit
    let firstCluster = null;
    let durationElement = null;
    walkElements(bytes, segmentPos, bytes.length, (id, content, contentEnd, _unknown, elementPos) => {
      if (id === CLUSTER_ID) {
        firstCluster = elementPos;
        return false;
      }
      if (id === 0x1549a966) { // Info: TimecodeScale, and where Duration sits
        walkElements(bytes, content, contentEnd, (childId, childContent, childEnd, _u, childPos) => {
          if (childId === TIMECODE_SCALE_ID) {
            let value = 0;
            for (let i = childContent; i < childEnd; i++) value = value * 256 + bytes[i];
            if (value > 0) scale = value;
          }
          if (childId === DURATION_ID) durationElement = { start: childPos, end: childEnd };
          return null;
        });
      }
      return null;
    });
    if (firstCluster == null) return null;
    const found = scanClusters(bytes, firstCluster);
    if (found.length < 2) return null;
    const clusters = found.map((entry, i) => ({
      offset: entry.offset,
      // An unknown-size cluster runs until the next one (or end of file);
      // truncating there is exactly what a demuxer expects.
      end: entry.knownEnd != null
        ? entry.knownEnd
        : (i + 1 < found.length ? found[i + 1].offset : bytes.length),
      timecodeMs: entry.timecodeRaw * scale / 1_000_000,
      timecodeRaw: entry.timecodeRaw,
      tcPos: entry.tcPos,
      tcLen: entry.tcLen,
    }));
    for (let i = 1; i < clusters.length; i++) {
      if (clusters[i].offset <= clusters[i - 1].offset) return null;
      if (clusters[i].timecodeMs < clusters[i - 1].timecodeMs) return null;
    }
    return { headerEnd: clusters[0].offset, clusters, durationElement };
  }

  // Which bytes of one segment cover [wantStartMs, wantEndMs] (absolute
  // epoch ms). Returns {start, end, startEpochMs, durationMs} indexes into
  // ``bytes``, or null when the index is missing / the cut would not shrink
  // the upload (caller sends the whole segment unchanged).
  function sliceRange(index, segStartMs, segDurationMs, wantStartMs, wantEndMs) {
    if (!index) return null;
    const { clusters } = index;
    const origin = clusters[0].timecodeMs;
    const abs = (cluster) => segStartMs + (cluster.timecodeMs - origin);
    let from = 0;
    while (from + 1 < clusters.length && abs(clusters[from + 1]) <= wantStartMs) from += 1;
    // Cluster i holds the frames up to cluster i+1's timecode, so the last one
    // needed is the last whose own timecode is still before the clip end.
    let to = from;
    while (to + 1 < clusters.length && abs(clusters[to + 1]) < wantEndMs) to += 1;
    const start = clusters[from].offset;
    const end = to + 1 < clusters.length ? clusters[to + 1].offset : clusters[to].end;
    if (start <= index.headerEnd && end >= bytesEnd(clusters)) return null;
    const startEpochMs = abs(clusters[from]);
    const durationMs = (to + 1 < clusters.length ? abs(clusters[to + 1]) : segStartMs + segDurationMs) - startEpochMs;
    if (durationMs <= 0) return null;
    return { start, end, startEpochMs, durationMs, indexHeaderEnd: index.headerEnd, from, to };
  }

  // The bytes to upload for ``range``: the header, then the kept clusters.
  // Two fixes make the cut a self-consistent file of its own, which matters
  // as soon as the server concatenates it with the next segment (the concat
  // demuxer places each file after the previous one's *declared* duration):
  //   - the header's Duration (the whole segment's) is overwritten with an
  //     EBML Void of the same length, so no offsets move;
  //   - the kept clusters' timecodes are rebased to start at 0, in place
  //     (block times are cluster-relative, so frames are untouched).
  // Returns null when either rewrite is impossible; the caller then uploads
  // the whole segment.
  function buildCut(bytes, index, range) {
    const header = bytes.slice(0, index.headerEnd);
    const dur = index.durationElement;
    if (dur) {
      const length = dur.end - dur.start;
      if (length < 2 || length - 2 > 126) return null;
      header[dur.start] = 0xec;
      header[dur.start + 1] = 0x80 | (length - 2);
      header.fill(0, dur.start + 2, dur.end);
    }
    const body = bytes.slice(range.start, range.end);
    const base = index.clusters[range.from].timecodeRaw;
    for (let k = range.from; k <= range.to; k++) {
      const cluster = index.clusters[k];
      let value = cluster.timecodeRaw - base;
      for (let b = cluster.tcLen - 1; b >= 0; b--) {
        body[cluster.tcPos - range.start + b] = value % 256;
        value = Math.floor(value / 256);
      }
    }
    const out = new Uint8Array(header.length + body.length);
    out.set(header, 0);
    out.set(body, header.length);
    return out;
  }

  function bytesEnd(clusters) {
    return clusters[clusters.length - 1].end;
  }

  // Upload only the clusters covering [wantStartMs, wantEndMs]. Falls back to
  // the whole segment whenever the file cannot be indexed or the cut would
  // not shrink it, so a parse failure never changes what gets uploaded.
  async function sliceSegment(seg, wantStartMs, wantEndMs) {
    const whole = { blob: seg.blob, startEpochMs: seg.startEpochMs, durationMs: seg.durationMs };
    let bytes;
    try {
      bytes = new Uint8Array(await seg.blob.arrayBuffer());
    } catch (_err) {
      return whole;
    }
    const index = indexClusters(bytes);
    const range = sliceRange(index, seg.startEpochMs, seg.durationMs, wantStartMs, wantEndMs);
    if (!range) return whole;
    const cutBytes = buildCut(bytes, index, range);
    if (!cutBytes || cutBytes.length >= seg.blob.size) return whole;
    const cut = new Blob([cutBytes], { type: seg.blob.type || "video/webm" });
    return { blob: cut, startEpochMs: range.startEpochMs, durationMs: range.durationMs };
  }

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
          // Pin the keyframe interval instead of relying on Chrome's
          // undocumented default (~100 frames): cluster slicing uploads
          // from the keyframe before the clip, so a shorter interval means
          // less to upload. Measured at 720p: 2 s costs 0.1 dB vs the
          // default, invisible, and the bitrate is unchanged.
          videoKeyFrameIntervalDuration: KEYFRAME_MS,
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
      const trimmed = [];
      for (const seg of picked) {
        trimmed.push(await sliceSegment(seg, startMs, endMs));
      }
      return { startMs, endMs, segments: trimmed, crop: range.crop || null };
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
    module.exports = { selectSegments, SEGMENT_MS, KEYFRAME_MS, indexClusters, sliceRange, buildCut };
  }
})();
