const STORAGE_KEY = "passiveProbe";
const $ = (id) => document.getElementById(id);
let lastState = null;

function cell(text, cls) {
  const td = document.createElement("td");
  td.textContent = text;
  if (cls) td.className = cls;
  return td;
}

function fmtTime(iso) {
  return iso ? String(iso).slice(5, 16).replace("T", " ") : "";
}

function verdictCell(row) {
  const quick = row.quick || {};
  const rec = row.record || {};
  if (row.pending) return cell("录制中…", "warn");
  if (quick.canvas !== "ok") return cell("读不到", "bad");
  if (rec.verdict === "ok") return cell("正常", "ok");
  if (rec.verdict === "black") return cell("黑屏", "bad");
  if (rec.error || (rec.captureStream && rec.captureStream.startsWith("fail"))) return cell("录制失败", "bad");
  return cell(rec.verdict || "?", "warn");
}

function render(state) {
  lastState = state;
  const sitesBody = $("sites").querySelector("tbody");
  const eventsBody = $("events").querySelector("tbody");
  sitesBody.innerHTML = "";
  eventsBody.innerHTML = "";
  if (!state) {
    $("summary").textContent = "还没有记录。";
    return;
  }
  const rows = Object.values(state.sites || {}).sort((a, b) =>
    String(b.lastAt).localeCompare(String(a.lastAt))
  );
  let blocked = 0;
  let drm = 0;
  let iframe = 0;
  const hosts = new Set();
  for (const row of rows) {
    const quick = row.quick || {};
    const rec = row.record || {};
    hosts.add(row.host);
    if (quick.canvas !== "ok" && !quick.drm) blocked += 1;
    if (quick.drm) drm += 1;
    if (row.inIframe) iframe += 1;
    const tr = document.createElement("tr");
    const hostCell = cell(row.host + (row.crossOrigin ? ` → ${row.frameHost}` : ""));
    hostCell.className = "host";
    hostCell.title = row.url || "";
    tr.appendChild(hostCell);
    tr.appendChild(cell(row.inIframe ? (row.crossOrigin ? "跨域" : "同域") : "否"));
    tr.appendChild(cell(quick.srcKind || "?"));
    tr.appendChild(verdictCell(row));
    tr.appendChild(cell(quick.drm ? "是" : "", quick.drm ? "bad" : ""));
    tr.appendChild(cell(rec.kbps != null ? String(rec.kbps) : ""));
    tr.appendChild(cell(rec.recordedBrightness != null ? String(rec.recordedBrightness) : quick.liveBrightness != null ? `${quick.liveBrightness}*` : ""));
    tr.appendChild(cell(String(row.seen || 0)));
    tr.appendChild(cell(fmtTime(row.lastAt)));
    sitesBody.appendChild(tr);
  }
  const events = Object.entries(state.videoEvents || {}).sort((a, b) =>
    String(b[1].lastAt).localeCompare(String(a[1].lastAt))
  );
  for (const [host, ev] of events) {
    const tr = document.createElement("tr");
    const hostCell = cell(host);
    hostCell.className = "host";
    tr.appendChild(hostCell);
    tr.appendChild(cell(String(ev.added || 0)));
    tr.appendChild(cell(String(ev.removed || 0)));
    tr.appendChild(cell(String(ev.srcChanged || 0)));
    tr.appendChild(cell(fmtTime(ev.lastAt)));
    eventsBody.appendChild(tr);
  }
  $("summary").textContent = rows.length
    ? `${hosts.size} 个站 / ${rows.length} 条记录 · 读不到画面 ${blocked} · DRM ${drm} · 在 iframe 里 ${iframe} · 自 ${fmtTime(state.startedAt)}`
    : "还没有记录。打开直播页播起来，等几秒再看。";
}

$("copy").addEventListener("click", async () => {
  if (!lastState) return;
  await navigator.clipboard.writeText(JSON.stringify(lastState, null, 2));
  $("copy").textContent = "已复制";
  setTimeout(() => ($("copy").textContent = "复制 JSON"), 1200);
});

$("clear").addEventListener("click", () => {
  if (!confirm("清空全部采集记录？")) return;
  chrome.runtime.sendMessage({ type: "clear" }, () => {
    void chrome.runtime.lastError;
    chrome.storage.local.get(STORAGE_KEY).then((v) => render(v[STORAGE_KEY]));
  });
});

chrome.storage.onChanged.addListener((changes, area) => {
  if (area === "local" && changes[STORAGE_KEY]) render(changes[STORAGE_KEY].newValue);
});

chrome.storage.local.get(STORAGE_KEY).then((v) => render(v[STORAGE_KEY]));
