// Side panel shell: an <iframe> onto 8901's page in panel mode, plus a bridge
// that forwards "clip created / match changed" from the background into the
// iframe via postMessage (the page only accepts chrome-extension:// origins).
const frame = document.getElementById("frame");
let baseUrl = "";
let currentMatchId = new URLSearchParams(location.search).get("match_id") || "";

function send(msg) {
  return new Promise((resolve) => {
    chrome.runtime.sendMessage(msg, (resp) => {
      void chrome.runtime.lastError;
      resolve(resp || null);
    });
  });
}

function panelUrl(matchId) {
  const url = new URL(baseUrl);
  url.searchParams.set("mode", "panel");
  if (matchId) url.searchParams.set("match_id", matchId);
  return url.href;
}

function postToPage(payload) {
  if (!frame.contentWindow || !baseUrl) return;
  frame.contentWindow.postMessage(payload, new URL(baseUrl).origin);
}

async function init() {
  baseUrl = (await send({ type: "get-base-url" })) || "";
  if (!baseUrl) {
    document.getElementById("notice").style.display = "block";
    frame.style.display = "none";
    return;
  }
  frame.src = panelUrl(currentMatchId);
}

chrome.runtime.onMessage.addListener((msg) => {
  if (!msg || msg.type !== "panel-refresh") return false;
  if (msg.matchId && msg.matchId !== currentMatchId) {
    currentMatchId = msg.matchId;
    postToPage({ type: "ext-set-match", match_id: currentMatchId });
  }
  postToPage({ type: "ext-clip-created", match_id: msg.matchId, name: msg.name });
  return false;
});

init();
