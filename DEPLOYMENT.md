# 部署说明 — 8901 足球直播监控台 (gif-monitor)

> 本文件记录本服务在**公司生产服务器**上的部署形态与更新方式。
> 只写配置项名称与结构，**不含任何密钥**（真实值在服务器 `/opt/gif-monitor/.env`，权限 600）。

## 1. 它是什么

8901 监控台 = 足球直播「半人工监控台」。独立于 8899 大盘：
- 自动列出进行中的比赛，提供直播预览供人工盯场；
- 进球后人工点「截取」回捞最近 N 秒生成 GIF；
- draft 模式下自动在懂球帝开放平台建**草稿**，人工审核后再发布；
- 接收 match-timeline 投递的进球/红牌事件，推飞书 + 页内提醒。

入口：`monitor_server.py`（启动时自动加载同目录 `.env`）。

## 2. 生产部署形态

| 项 | 值 |
|---|---|
| 服务器 | Rocky Linux 9.8（`dnf`），matchgif.aisportsapp.com |
| 目录 | `/opt/gif-monitor` |
| 代码来源 | gitee `https://gitee.com/tsdka/automatic-gif.git`（部署只用 gitee） |
| Python 环境 | `/opt/gif-monitor/.venv`，第三方依赖仅 `flask` |
| 监听 | `127.0.0.1:8001`（由 `.env` 的 `GIF_MONITOR_PORT=8001` 指定） |
| systemd | `gif-monitor.service`（`ExecStart=/opt/gif-monitor/.venv/bin/python -u /opt/gif-monitor/monitor_server.py`） |
| 对外路径 | `https://matchgif.aisportsapp.com/gif-monitor/` |
| 外部依赖 | `ffmpeg`（系统装，5.1.10）；截取用 |
| 投递模式 | `GIF_MONITOR_DELIVERY_MODE=draft`（人工审核；验收通过后可改 `publish`） |

### Nginx（/etc/nginx/conf.d/matchgif.conf）
```nginx
# gif-monitor
location = /gif-monitor { return 301 /gif-monitor/; }
location /gif-monitor/ {
    proxy_pass http://127.0.0.1:8001/;   # 尾斜杠必须有：去掉 /gif-monitor/ 前缀再转给 8901
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

### 密钥/数据文件（都在服务器，不进仓库）
- `/opt/gif-monitor/.env`（chmod 600）——所有配置与密钥
- `/opt/gif-monitor/data/publish_accounts.json`（chmod 600）——发布账号池（6 个）
- `/opt/gif-monitor/data/open-platform-token.json`——OAuth 授权后**运行时生成**，首次部署没有属正常

## 3. ⚠️ 子路径部署的核心约束（最容易踩的坑）

因为挂在子路径 `/gif-monitor/` 下，**所有返回给浏览器的 URL 必须用相对路径**（不能以 `/` 开头），
否则浏览器会解析到域名根、绕过前缀 → 404/502。已处理的位置：

| 文件 | 内容 |
|---|---|
| `monitor_static/index.html` | `static/styles.css`、`static/vendor/hls.min.js`、`static/app.js` |
| `monitor_static/app.js` | 全部 `api/monitor/...` fetch 地址 |
| `live_preview.py` | `playlist_url` 返回 `preview/...` |
| `monitor_server.py` | `clip_url` 返回 `clips/...` |

**以后新增任何"返回给前端的 URL"（接口、静态、m3u8、GIF 等），一律不要以 `/` 开头。**
本地根路径部署（`127.0.0.1:8901/`）下相对路径同样工作，两边通吃。

## 4. 更新上线流程

**本地（Mac）** — 一次 `git push` 同时推 gitee + GitHub（已在本仓库 `gitee` 远端配置双 push URL）：
```bash
git add -A
git commit -m "说明"
git push            # 自动推 gitee + GitHub
```

**服务器** — 一键更新（脚本 `/usr/local/bin/deploy-gif`，同时更新 8901 与 match-timeline，仅对有更新的服务重启）：
```bash
deploy-gif
```

**浏览器** — `Cmd + Shift + R` 硬刷新验证。

> 说明：改 Python（monitor_server.py 等）需重启；改前端（app.js/css/html）pull 后刷新即可。`deploy-gif` 两种都覆盖。

## 5. 常见排查

- **页面 502，但直连 200**：`curl 127.0.0.1:8001` 若 200，问题在 nginx→上游。先查 SELinux（本机为 Disabled，可排除）；再看是否 reload 瞬时竞态，稍后重试。
- **GIF 卡片黑屏 / 点开 502**：多半是又出现了根绝对路径（见第 3 节），检查对应 URL 是否以 `/` 开头。
- **服务状态**：`systemctl status gif-monitor`、`journalctl -u gif-monitor -n 50 --no-pager`。

## 6. 相关

- 配套服务 match-timeline（进球/红牌监测）见 `../monitor/DEPLOYMENT.md`
- 通用部署手册与服务器总账见 `../server-deploy-kit/`
