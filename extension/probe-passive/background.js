// 汇总各 frame 发来的探测结果。只写 storage.local，不联网。
//
// 存储结构 passiveProbe:
//   sites:  { "<host>|<frameHost>|<srcKind>|<canvas>": { ...详细探测, seen, firstAt, lastAt } }
//   videoEvents: { "<host>": { added, removed, srcChanged, lastAt } }
// sites 上限 200 条，满了丢最久没见的。

const STORAGE_KEY = "passiveProbe";
const MAX_SITES = 200;

const empty = () => ({ sites: {}, videoEvents: {}, startedAt: new Date().toISOString() });

let writeChain = Promise.resolve();
function update(mutator) {
  // 串行化读-改-写，避免多个 frame 同时上报时互相覆盖。
  writeChain = writeChain.then(async () => {
    const stored = (await chrome.storage.local.get(STORAGE_KEY))[STORAGE_KEY] || empty();
    const result = mutator(stored);
    await chrome.storage.local.set({ [STORAGE_KEY]: stored });
    return result;
  });
  return writeChain;
}

function siteKey(msg) {
  const canvas = msg.quick && msg.quick.canvas === "ok" ? "ok" : "blocked";
  const srcKind = (msg.quick && msg.quick.srcKind) || "none";
  return `${msg.host}|${msg.frameHost}|${srcKind}|${canvas}`;
}

function trimSites(sites) {
  const keys = Object.keys(sites);
  if (keys.length <= MAX_SITES) return;
  keys
    .sort((a, b) => String(sites[a].lastAt).localeCompare(String(sites[b].lastAt)))
    .slice(0, keys.length - MAX_SITES)
    .forEach((k) => delete sites[k]);
}

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (!msg || !msg.type) return false;

  if (msg.type === "probe-check") {
    const key = siteKey(msg);
    const now = new Date().toISOString();
    update((state) => {
      const existing = state.sites[key];
      if (existing && existing.record) {
        existing.seen = (existing.seen || 0) + 1;
        existing.lastAt = now;
        return { record: false };
      }
      // 占位，避免同一站多个 frame 同时触发多次录制。
      state.sites[key] = {
        ...(existing || { firstAt: now, seen: 0 }),
        host: msg.host,
        frameHost: msg.frameHost,
        inIframe: Boolean(msg.inIframe),
        crossOrigin: Boolean(msg.crossOrigin),
        quick: msg.quick,
        seen: ((existing && existing.seen) || 0) + 1,
        lastAt: now,
        pending: true,
      };
      trimSites(state.sites);
      return { record: true };
    }).then(sendResponse);
    return true;
  }

  if (msg.type === "probe-result") {
    const key = siteKey(msg);
    update((state) => {
      const row = state.sites[key] || { firstAt: new Date().toISOString(), seen: 1 };
      state.sites[key] = {
        ...row,
        host: msg.host,
        frameHost: msg.frameHost,
        inIframe: Boolean(msg.inIframe),
        crossOrigin: Boolean(msg.crossOrigin),
        url: msg.url,
        quick: msg.quick,
        record: msg.record,
        lastAt: new Date().toISOString(),
        pending: false,
      };
    });
    return false;
  }

  if (msg.type === "video-event") {
    const host = msg.host || msg.frameHost || "?";
    update((state) => {
      const row = state.videoEvents[host] || { added: 0, removed: 0, srcChanged: 0 };
      if (msg.kind === "added") row.added += 1;
      else if (msg.kind === "removed") row.removed += 1;
      else if (msg.kind === "src-changed") row.srcChanged += 1;
      row.lastAt = new Date().toISOString();
      state.videoEvents[host] = row;
    });
    return false;
  }

  if (msg.type === "clear") {
    update((state) => {
      Object.assign(state, empty());
    }).then(() => sendResponse({ ok: true }));
    return true;
  }

  return false;
});
