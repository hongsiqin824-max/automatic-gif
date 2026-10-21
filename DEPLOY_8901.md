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
| `GIF_PREVIEW_SEGMENT_SECONDS` | 1 | HLS 分片秒数；改 2 回到旧片长（配合 SEGMENTS=90） |
| `GIF_MONITOR_CATCHUP_ENABLED` | true | 小窗自动追帧/自动回到直播；false 完全关闭 |
| `GIF_MONITOR_WEBRTC_ENABLED` | false | 小窗 WebRTC（需 mediamtx）；false=纯 HLS |
| `GIF_PREVIEW_IDLE_TIMEOUT_SECONDS` | 120 | 预览空闲回收秒数 |
| `GIF_MONITOR_AUTO_RESTART` | true | 预览断线自愈 |
| `GIF_MONITOR_PUBLISH_ENABLED` | true | 是否自动建草稿发文 |
| `GIF_MONITOR_DELIVERY_MODE` | draft | draft / publish |
| `GIF_MONITOR_MAX_CONCURRENT_ENCODES` | （阶段 5 新增）2~3 | 截取编码并发上限 |
