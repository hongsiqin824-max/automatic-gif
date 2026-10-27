const DEFAULT_BASE = "https://matchgif.aisportsapp.com/gif-monitor/";
const input = document.getElementById("base");
chrome.storage.local.get("baseUrl").then(({ baseUrl }) => {
  input.value = baseUrl || DEFAULT_BASE;
});
document.getElementById("save").addEventListener("click", async () => {
  let value = input.value.trim() || DEFAULT_BASE;
  if (!value.endsWith("/")) value += "/";
  await chrome.storage.local.set({ baseUrl: value });
  const saved = document.getElementById("saved");
  saved.textContent = "已保存";
  setTimeout(() => (saved.textContent = ""), 1500);
});
