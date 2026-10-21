# 8901 监控台 · 上服务器落地清单（DEPLOY_8901）

> 面向把「足球比赛直播监控台」（端口 8901，`monitor_server.py`）从本机搬到服务器。
> 按阶段执行；标注「不改代码 / 可能改代码 / 需改代码」。不改动现有应用逻辑的步骤优先。

## 优先级总览
| 优先 | 阶段 | 收益 | 是否改代码 |
|---|---|---|---|
| ⭐ 最高 | 阶段 1 HTTP/2 反代 | 解决多路预览播不动 | 不改代码（纯部署） |
| 高 | 阶段 2 负载评估与转码规格 | 决定能开几场 | 可能改代码 |
| 必备 | 阶段 3 开机自启 | 稳定常驻 | 不改代码 |
| 必备 | 阶段 4 发文开关与环境变量 | 功能开关 | 不改代码（配置） |
| 顺手 | 阶段 5 小护栏（超时+信号量） | 防偶发尖峰 | 需改代码 |
| 验收 | 阶段 6 上线肉眼验收 | 质量确认 | 不改代码 |

说明：真正决定「服务器上能否正常盯场」的是阶段 1 与阶段 2（常驻预览转码是 CPU 大头，截取编码只是瞬时小头）。阶段 5 收益小但成本极低，上线前顺手做。

---

## 阶段 0 · 前置准备（不改代码）
- [ ] 服务器装好 **Python 3.9+、FFmpeg**（如需跑测试再装 Node）；确认 `ffmpeg -version` 可用。
- [ ] 确认服务器能连通直播源与懂球帝接口（已实测公网可直连，无需飞连 / 白名单）。
- [ ] 准备项目目录与 `.env`（变量见阶段 4）。
- [ ] 手动前台起一次：`python3 monitor_server.py`，本机 `curl http://127.0.0.1:8901/api/health` 返回 ok。
- **验收**：health 接口通、日志无报错。

## 阶段 1 · ⭐ HTTP/2 反代（最高收益，纯部署，不改代码）
> 解决浏览器对单域名 ~6 个并发连接上限导致的「多路预览播不动」。
- [ ] 准备域名 + 证书（HTTP/2 要求 HTTPS）。
- [ ] 选 **Caddy（推荐：自动 HTTPS + 默认 HTTP/2）** 或 Nginx：
  - Caddy（`Caddyfile`）：
    ```
    monitor.你的域名.com {
        reverse_proxy 127.0.0.1:8901
    }
    ```
  - Nginx：
    ```
    server {
        listen 443 ssl;
        http2 on;                      # 旧版写法：listen 443 ssl http2;
        server_name monitor.你的域名.com;
        ssl_certificate     /path/fullchain.pem;
        ssl_certificate_key /path/privkey.pem;
        location / {
            proxy_pass http://127.0.0.1:8901;
            proxy_http_version 1.1;
            proxy_set_header Host $host;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_buffering off;       # HLS/分片别缓冲，避免延迟
        }
    }
    ```
- [ ] 保持 `GIF_MONITOR_HOST=127.0.0.1`，让 8901 只接受本机反代访问（外部仅经反代进入）。
- [ ] 反代**不要覆盖**分片的 `Cache-Control: no-store`。
- [ ] 四类路径用 `location /` 全量转发即可：`/`、`/static/*`、`/preview/<id>/*`、`/api/monitor/*`、`/clips/*`。无 WebSocket/SSE，无需额外 upgrade 配置。
- **验收**：浏览器 F12 → Network，请求 Protocol 列为 `h2`；同时开 8 场都能播；后端日志能看到**全部场次**的 `index.m3u8`/`seg` 请求（不再只有 3 场）。

## 阶段 2 · 负载评估与转码规格（可能改代码）
> CPU 大头 = 常驻预览转码（每场一路 libx264 480p，最多 `GIF_MONITOR_MAX_PREVIEWS` 场）。
- [ ] 起服务后跑 `uptime`（1/5/15 分钟 load）+ `nproc`（核数），据此判断。
- [ ] 决策（据实选其一）：
  - 够用 → 不动；
  - 有 GPU → 上硬件编码（Linux `h264_nvenc` / QSV / VAAPI）【改 `live_preview.py` 的 `_build_command`】；
  - 不够又无 GPU → 降规格（360p / 20fps / `ultrafast`）【改 `live_preview.py` 的 `PREVIEW_HEIGHT` 等】。
- [ ] 用 `GIF_MONITOR_MAX_PREVIEWS` 限制最大同看场数，匹配服务器实际能力。
- **验收**：目标场数下 load 稳定在可接受区间、画面不卡。

## 阶段 3 · 开机自启（systemd，不改应用代码）
- [ ] 配 systemd 服务：`ExecStart=python3 monitor_server.py`，`WorkingDirectory` 指向项目目录，日志重定向到 `monitor.log`，`Restart=always`。
- [ ] `systemctl enable --now` 开机自启。
- [ ] 参考项目已有的 `SERVICE_RESTART_GUIDE.md` / `SERVICE_SHUTDOWN_GUIDE.md`。
- **验收**：`systemctl status` 正常；重启机器后服务自动拉起。

## 阶段 4 · 发文开关与环境变量（配置，不改代码）
- [ ] 关键开关：
  - `GIF_MONITOR_PUBLISH_ENABLED`：true 才自动建草稿；缺凭证会自动降级为「仅本地保存」并打 `manual_publish_disabled`。
  - `GIF_MONITOR_DELIVERY_MODE`：`draft`（建草稿）/ `publish`（直接发）；上线初期建议 `draft`。
  - `GIF_MONITOR_AUTO_RESTART`：预览自愈，建议 true。
- [ ] 发文栈变量齐全：`OCR_IMAGE_UPLOAD_BACKEND`、`GIF_UPLOAD_ENDPOINT` / `GIF_UPLOAD_TOKEN`、`GIF_PUBLIC_ORIGIN`、`OPEN_PLATFORM_*`（OAuth 首次授权）、发布账号池文件等。
- [ ] 其它可调：`GIF_MONITOR_CLIP_SECONDS`(10)、`GIF_MONITOR_CLIP_WIDTH`(440)、`GIF_MONITOR_CLIP_FPS`(16.667)、`GIF_MONITOR_PREVIEW_SEGMENTS`(90)、`GIF_PREVIEW_IDLE_TIMEOUT_SECONDS`(120)、`GIF_MONITOR_HOST`/`GIF_MONITOR_PORT`。
- **验收**：`GIF_MONITOR_PUBLISH_ENABLED=true` 时截取能建草稿；关掉时仅本地保存、服务不报错。

## 阶段 5 · 小护栏（需改代码，收益小 / 成本极低，上线前顺手做）
- [ ] **收紧编码超时**：`_encode_clip_gif` 的 `subprocess.run(..., timeout=120)` → 30~45s。卡住的编码能更快失败、更快释放资源。
- [ ] **加编码并发信号量**：新增 `GIF_MONITOR_MAX_CONCURRENT_ENCODES`（默认 2~3）。`monitor_clip` 编码前 `acquire`、`finally` 里 `release`；满员时短等或回 HTTP 429。保持「请求同步返回 GIF」的现有体验。
  - 为什么用信号量而非 `BoundedTaskPool`：截取需要**当场返回 GIF**，而 `BoundedTaskPool` 是「后台跑 + 之后 collect_done」的 fire-and-forget 设计，不适合同步返回；信号量只是并发闸门，编码仍在请求线程同步跑。
- **验收**：模拟多人同点截取，CPU 不被瞬间打满；单次编码卡住时能更快失败释放。

## 阶段 6 · 上线肉眼验收（真实比赛时，不改代码）
- [ ] 先「回到直播」再「截取」，确认 GIF 画面对得上、时长等于设定秒数。
- [ ] 看日志 `monitor_clip` 为 `anchored:true`、`with_pdt == segments`。
- [ ] 截取后主屏看到 GIF 缩略图；草稿生成后点「打开草稿」能跳转后台。
- [ ] 源抖动时格子显示「自动重连 / 持续重连中」，恢复后画面自动复原。

## 阶段 7 · 小窗 WebRTC（方案 H，BACKLOG 11.13；纯部署 + 一个 `.env` 开关）
> 小窗延迟从 ~4.5s 降到 <1s。大窗与截 GIF 仍走 HLS，一行代码不动。
> 总开关 `GIF_MONITOR_WEBRTC_ENABLED` 默认 **false**：代码先上、mediamtx 后装、最后打开；任何一步不满意把开关关回去重启即回现状。
> 本地已用 mediamtx v1.21.2 跑通全链路（含 mediamtx 挂掉回退 HLS、WHEP 不通 3 次重试后回退）。

### 7.0 先把代码上去（开关关着，行为 = 现状 + 追帧 + 1s 片）
```bash
deploy-gif
curl -fsS http://127.0.0.1:8001/api/health
journalctl -u gif-monitor -n 50 --no-pager | grep monitor_latency_config   # 应见 "webrtc_enabled": false
```
浏览器 `Cmd+Shift+R`，盯一场确认和之前一样能播、能截。**这一步没问题再往下。**

### 7.1 装 mediamtx（/opt/mediamtx，独立 systemd）
```bash
# 1) 二进制（Rocky 9 x86_64）。GitHub 慢就换 ghproxy 之类镜像，或本机下好 scp 上去。
sudo mkdir -p /opt/mediamtx && cd /opt/mediamtx
V=v1.21.2
curl -fL -o mediamtx.tar.gz \
  https://github.com/bluenviron/mediamtx/releases/download/${V}/mediamtx_${V}_linux_amd64.tar.gz
tar xzf mediamtx.tar.gz mediamtx && rm -f mediamtx.tar.gz
./mediamtx --version                       # 应打印 v1.21.2

# 2) 配置：用仓库里的模板，只改一处公网 IP
sudo cp /opt/gif-monitor/deploy/mediamtx/mediamtx.yml /opt/mediamtx/mediamtx.yml
PUB_IP=$(curl -fsS https://ifconfig.me)      # 或在腾讯云控制台看
sudo sed -i "s/PUBLIC_IP_HERE/${PUB_IP}/" /opt/mediamtx/mediamtx.yml
grep webrtcAdditionalHosts /opt/mediamtx/mediamtx.yml   # 确认已替换

# 3) 专用用户 + systemd
sudo useradd -r -s /sbin/nologin -d /opt/mediamtx mediamtx 2>/dev/null || true
sudo chown -R mediamtx:mediamtx /opt/mediamtx
sudo cp /opt/gif-monitor/deploy/mediamtx/mediamtx.service /etc/systemd/system/mediamtx.service
sudo systemctl daemon-reload
sudo systemctl enable --now mediamtx
sudo systemctl status mediamtx --no-pager
journalctl -u mediamtx -n 20 --no-pager
```
期望日志三行 `started with listeners`：RTSP `127.0.0.1:8554`、WebRTC `127.0.0.1:8889 (TCP/HTTP), :8189 (UDP/ICE), :8189 (TCP/ICE)`、API `127.0.0.1:9997`。**不应有 WAR/ERR。**

本机自检：
```bash
curl -s http://127.0.0.1:9997/v3/paths/list       # {"itemCount":0,"pageCount":0,"items":[]}
ss -lunp | grep 8189 && ss -ltnp | grep 8189       # UDP + TCP 都在 0.0.0.0:8189
```

### 7.2 安全组（腾讯云控制台）
入站放行 **UDP 8189** 和 **TCP 8189**，来源 0.0.0.0/0（WebRTC 媒体直连，不经 nginx）。
9997 / 8554 / 8889 只监听 127.0.0.1，**不要**开。

### 7.3 nginx：WHEP 反代（/etc/nginx/conf.d/matchgif.conf）
在现有 `location /gif-monitor/ { … }` **之前**加一段（更长前缀优先，顺序其实无所谓，放前面便于阅读）：
```nginx
# gif-monitor 小窗 WebRTC：WHEP 信令转给 mediamtx（POST 建会话 / PATCH ICE / DELETE 关会话）
location /gif-monitor/whep/ {
    proxy_pass http://127.0.0.1:8889/;   # 尾斜杠必须有：/gif-monitor/whep/<id>/whep → /<id>/whep
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_buffering off;
    proxy_read_timeout 30s;
}
```
```bash
sudo cp /etc/nginx/conf.d/matchgif.conf /etc/nginx/conf.d/matchgif.conf.bak-whep-$(date +%Y%m%d)
sudo nginx -t && sudo systemctl reload nginx
# 自检：两条都说明请求已经穿过 nginx 到了 mediamtx（nginx 自己的 404 不会带这些）
curl -s -D - -o /dev/null -X OPTIONS https://matchgif.aisportsapp.com/gif-monitor/whep/x/whep \
  -H 'Origin: https://matchgif.aisportsapp.com' -H 'Access-Control-Request-Method: POST' \
  | grep -i 'Access-Control-Allow-Methods'          # 期望: OPTIONS, GET, POST, PATCH, DELETE
curl -s -X POST https://matchgif.aisportsapp.com/gif-monitor/whep/x/whep \
  -H 'Content-Type: application/sdp' --data 'v=0'   # 期望: {"status":"error","error":"no stream is available on path 'x'"}
```
说明：mediamtx 的 `Location` 头是绝对路径 `/<id>/whep/<session>`，不带 `/gif-monitor/whep/` 前缀；前端已改为只取 session id 拼回自己 POST 的地址，所以反代不需要 `proxy_redirect`。

### 7.4 打开开关（/opt/gif-monitor/.env）
```bash
cp /opt/gif-monitor/.env /opt/gif-monitor/.env.bak-webrtc-$(date +%Y%m%d)
# 追加或修改：
GIF_MONITOR_WEBRTC_ENABLED=true
# 下面 6 个保持默认即可（写出来便于以后调）：
GIF_MONITOR_MEDIAMTX_API=http://127.0.0.1:9997
GIF_MONITOR_MEDIAMTX_RTSP=rtsp://127.0.0.1:8554
GIF_MONITOR_WHEP_BASE=whep/
GIF_MONITOR_MEDIAMTX_TIMEOUT_SECONDS=2
GIF_MONITOR_MEDIAMTX_READY_SECONDS=6
sudo systemctl restart gif-monitor
journalctl -u gif-monitor -n 50 --no-pager | grep monitor_latency_config   # "webrtc_enabled": true
```

### 7.5 验收（一场比赛）
浏览器 `Cmd+Shift+R` 后开一场：
| 看什么 | 期望 |
|---|---|
| 小窗状态文字 | 「正在播放直播源（WebRTC）」 |
| 小窗徽标 | `落后 0.x s`（一位小数） |
| Console | `[LAG] <id> \| WebRTC 落后 0.xxs \| connected \| …` |
| 与 VLC 同看 | 相差 <1s |
| 点放大 | 大窗「正在播放直播源」、徽标 `落后 3s` 左右、可拖；小窗仍在播 |
| 大窗截 GIF | 10s 原速，状态栏能关联提醒 |
| `curl -s 127.0.0.1:9997/v3/paths/list` | 每场一个 path，`ready: true`，readers 含 `rtspSession`（ffmpeg）和 `webRTCSession`（浏览器） |
| 关掉这场 | path 被删除 |
| `sudo systemctl stop mediamtx` | 小窗 `WebRTC 重连中 (1/3)…` → `WebRTC 不可用，已切回 HLS`，继续播；8901 日志 `webrtc_fallback_hls`。再 `start` 后新开的场重新走 WebRTC |

小窗一直停在「WebRTC 重连中」最后回退 HLS，但 7.3 自检是通的 → 多半是 **8189 没放行**或 `webrtcAdditionalHosts` 没填对（浏览器 F12 → `chrome://webrtc-internals` 看 ICE 候选是否有公网 IP）。

### 7.6 回退
- 只关功能：`.env` 改 `GIF_MONITOR_WEBRTC_ENABLED=false` → `systemctl restart gif-monitor`。mediamtx 可留着跑（空闲时几乎不占资源）。
- 彻底卸：`systemctl disable --now mediamtx`；nginx 删 `location /gif-monitor/whep/` 段 reload；安全组关 8189。

---

## 附：相关环境变量速查
| 变量 | 默认 | 作用 |
|---|---|---|
| `GIF_MONITOR_HOST` | 127.0.0.1 | 监听地址（反代后保持本机） |
| `GIF_MONITOR_PORT` | 8901 | 端口 |
| `GIF_MONITOR_MAX_PREVIEWS` | 16 | 最大同看场数 |
| `GIF_MONITOR_CLIP_SECONDS` | 10 | 默认截取秒数 |
| `GIF_MONITOR_CLIP_WIDTH` | 440 | GIF 宽 |
| `GIF_MONITOR_CLIP_FPS` | 16.667 | GIF 帧率（50/3，与 8899 一致） |
| `GIF_MONITOR_PREVIEW_SEGMENTS` | 180 | 滚动缓冲分片数（1s/片，约 180s） |
| `GIF_PREVIEW_SEGMENT_SECONDS` | 1 | HLS 片长（秒）；改 2 回到旧片长（配合 SEGMENTS=90） |
| `GIF_MONITOR_HLS_LIVE_SYNC_COUNT` | 3 | hls.js 停在边缘后几片（2~6）；目标延迟 = 片长 × 此值 |
| `GIF_MONITOR_CATCHUP_ENABLED` | true | 小窗自动追帧/自动回到直播；false 完全关闭 |
| `GIF_MONITOR_CATCHUP_RATE` | 1.5 | 追帧倍速（1.0~2.0） |
| `GIF_MONITOR_CATCHUP_MIN_LAG` | 12 | 落后多少秒开始追（下限 目标+2） |
| `GIF_MONITOR_STAGE_SEEK_LAG` | 30 | 大窗切回前台落后多少秒才跳回直播 |
| `GIF_MONITOR_HARD_SEEK_LAG` | 150 | 无条件跳回直播阈值（上限 回看秒数−10） |
| `GIF_PREVIEW_X264_PRESET` | veryfast | x264 预设；仅 CPU 吃紧时调 |
| `GIF_MONITOR_WEBRTC_ENABLED` | false | 小窗 WebRTC（需 mediamtx）；false=纯 HLS |
| `GIF_PREVIEW_IDLE_TIMEOUT_SECONDS` | 120 | 预览空闲回收秒数 |
| `GIF_MONITOR_AUTO_RESTART` | true | 预览断线自愈 |
| `GIF_MONITOR_PUBLISH_ENABLED` | true | 是否自动建草稿发文 |
| `GIF_MONITOR_DELIVERY_MODE` | draft | draft / publish |
| `GIF_MONITOR_MAX_CONCURRENT_ENCODES` | （阶段 5 新增）2~3 | 截取编码并发上限 |

延迟参数生效值核对：启动后 `grep monitor_latency_config gif-monitor.log`（或 journalctl）看一行 JSON；`adjusted` 非空表示有值越界被修正。浏览器 Console 对应 `[LATENCY-CONFIG]`。
