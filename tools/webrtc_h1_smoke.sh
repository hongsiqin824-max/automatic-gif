#!/usr/bin/env bash
# H1 local smoke test for scheme H (BACKLOG 11.13): run mediamtx, push a
# synthetic test stream into it with ffmpeg, then open a minimal WHEP page
# that plays the stream over WebRTC and shows the estimated glass-to-glass
# delay. Development only; nothing here is used in production.
#
#   bash tools/webrtc_h1_smoke.sh            # test pattern with burnt-in clock
#   bash tools/webrtc_h1_smoke.sh rtmp://…   # a real RTMP source instead
#
# Then open http://127.0.0.1:8890/ (served by this script). Ctrl-C stops all.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$ROOT/tmp/webrtc_h1"
SOURCE="${1:-}"
mkdir -p "$WORK"

command -v mediamtx >/dev/null || { echo "mediamtx not installed (brew install mediamtx)"; exit 1; }
command -v ffmpeg >/dev/null || { echo "ffmpeg not installed"; exit 1; }

cat > "$WORK/mediamtx.yml" <<'YML'
logLevel: info
api: yes
apiAddress: 127.0.0.1:9997
rtsp: yes
rtspAddress: 127.0.0.1:8554
rtmp: yes
rtmpAddress: 127.0.0.1:1935
hls: no
srt: no
webrtc: yes
webrtcAddress: 127.0.0.1:8889
webrtcAllowOrigin: '*'
webrtcLocalUDPAddress: 127.0.0.1:8189
webrtcIPsFromInterfaces: no
webrtcAdditionalHosts: [127.0.0.1]
paths:
  test:
YML

mediamtx "$WORK/mediamtx.yml" > "$WORK/mediamtx.log" 2>&1 &
MTX_PID=$!
sleep 1.5

if [ -n "$SOURCE" ]; then
  # Real source: let mediamtx pull it so this mirrors the target architecture.
  curl -fsS -X POST 127.0.0.1:9997/v3/config/paths/replace/test \
    -H 'Content-Type: application/json' \
    -d "{\"source\":\"$SOURCE\",\"sourceOnDemand\":false}" >/dev/null
  FF_PID=""
else
  # Synthetic source with a wall-clock burnt in so delay can be read off the
  # screen against the page's own clock.
  ffmpeg -hide_banner -loglevel warning -re \
    -f lavfi -i "testsrc2=size=854x480:rate=25" \
    -vf "drawtext=fontfile=/System/Library/Fonts/Menlo.ttc:text='%{localtime\:%H\\\\\:%M\\\\\:%S}':fontsize=64:fontcolor=white:box=1:boxcolor=black@0.6:x=20:y=20" \
    -c:v libx264 -preset veryfast -tune zerolatency -profile:v baseline -pix_fmt yuv420p \
    -g 25 -keyint_min 25 -bf 0 -an \
    -f rtsp -rtsp_transport tcp rtsp://127.0.0.1:8554/test \
    > "$WORK/ffmpeg.log" 2>&1 &
  FF_PID=$!
fi

cat > "$WORK/index.html" <<'HTML'
<!doctype html>
<meta charset="utf-8">
<title>H1 WHEP smoke</title>
<style>
  body{font:14px/1.4 -apple-system,Menlo,monospace;background:#111;color:#ddd;margin:0;padding:16px}
  video{width:854px;max-width:100%;background:#000;display:block}
  #clock{font-size:64px;font-weight:700;margin:8px 0}
  #stats{white-space:pre;color:#9f9}
</style>
<div id="clock">--:--:--</div>
<video id="v" autoplay muted playsinline></video>
<div id="stats">connecting…</div>
<script>
const WHEP = 'http://127.0.0.1:8889/test/whep';
const v = document.getElementById('v'), st = document.getElementById('stats');
setInterval(() => { document.getElementById('clock').textContent = new Date().toTimeString().slice(0, 8); }, 100);

async function start() {
  const pc = new RTCPeerConnection();
  pc.addTransceiver('video', {direction: 'recvonly'});
  pc.ontrack = (e) => { v.srcObject = e.streams[0]; };
  pc.onconnectionstatechange = () => { st.textContent = 'state: ' + pc.connectionState; };
  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  await new Promise(r => { if (pc.iceGatheringState === 'complete') r(); else pc.onicegatheringstatechange = () => pc.iceGatheringState === 'complete' && r(); });
  const res = await fetch(WHEP, {method: 'POST', headers: {'Content-Type': 'application/sdp'}, body: pc.localDescription.sdp});
  if (!res.ok) { st.textContent = 'WHEP POST failed: ' + res.status + ' ' + await res.text(); return; }
  await pc.setRemoteDescription({type: 'answer', sdp: await res.text()});
  let prevDelay = 0, prevCount = 0;
  setInterval(async () => {
    let jb = null, rtt = null, fps = null, w = null, h = null;
    for (const r of (await pc.getStats()).values()) {
      if (r.type === 'inbound-rtp' && r.kind === 'video') {
        const dD = r.jitterBufferDelay - prevDelay, dC = r.jitterBufferEmittedCount - prevCount;
        if (dC > 0) jb = dD / dC;
        prevDelay = r.jitterBufferDelay; prevCount = r.jitterBufferEmittedCount;
        fps = r.framesPerSecond; w = r.frameWidth; h = r.frameHeight;
      }
      if (r.type === 'candidate-pair' && r.state === 'succeeded') rtt = r.currentRoundTripTime;
    }
    st.textContent =
      `state: ${pc.connectionState}\n` +
      `jitterBuffer ≈ ${jb == null ? '?' : (jb * 1000).toFixed(0)} ms   rtt ≈ ${rtt == null ? '?' : (rtt * 1000).toFixed(0)} ms\n` +
      `${w}x${h} @ ${fps} fps\n` +
      `读屏法：页面大钟 − 画面内时钟 = 端到端延迟（含编码+网络+抖动缓冲）`;
  }, 1000);
}
start().catch(e => { st.textContent = 'error: ' + e; });
</script>
HTML

( cd "$WORK" && python3 -m http.server 8890 --bind 127.0.0.1 > "$WORK/http.log" 2>&1 ) &
HTTP_PID=$!

cleanup() {
  echo; echo "stopping…"
  [ -n "${FF_PID}" ] && kill "$FF_PID" 2>/dev/null || true
  kill "$HTTP_PID" "$MTX_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

sleep 2
echo "mediamtx api : $(curl -fsS 127.0.0.1:9997/v3/paths/list | head -c 300)"
echo
echo "open  http://127.0.0.1:8890/"
echo "logs  $WORK/{mediamtx,ffmpeg,http}.log"
echo "Ctrl-C to stop"
wait "$MTX_PID"
