# GIF 日报

日报由独立脚本发送，不会进入 Dashboard、OCR worker 或文章发布 worker 的主循环。
每天 19:00（`Asia/Shanghai`）统计前一天 19:00（含）到当天 19:00（不含）检测到的所有事件。

## 统计口径

- “应发”来自每场比赛的 `pipeline_state.sqlite3.event_tasks`，按 `discovered_at_unix` 统计，并排除已经被事件身份合并的旧记录。
- 文章状态只关联 OCR GIF 队列（`artifact_kind=ocr_window`、`delivery_mode=draft`）。
- 日报展示发送时的最终状态：已发布、OCR 失败、发布失败、规则拦截、处理中/待重试、未登记。
- “未登记”用于发现事件已检测但 OCR 结果没有进入文章队列的异常。
- 不重新访问懂球帝接口，也不扫描 GIF 文件；没有文章记录的比赛仍会显示比赛 ID和失败原因。

## 配置飞书

在服务器项目目录的私有 `.env` 中加入（不要提交到 Git）：

```dotenv
GIF_DAILY_REPORT_TIMEZONE=Asia/Shanghai
GIF_DAILY_REPORT_FEISHU_WEBHOOK=<飞书机器人 webhook>
```

用户提供的飞书机器人地址应填入 `<飞书机器人 webhook>`。脚本启动时会读取 `.env`，shell 环境变量优先。

先手动预览，不发送：

```bash
cd /opt/automatic-gif-release-c24aa3c
/opt/automatic-gif/.venv/bin/python daily_gif_report.py --dry-run
```

## 每天 19:00 执行

在服务器 SSH 终端执行一次以下命令，创建当前用户的 cron 任务：

```bash
crontab -e
```

加入以下两行。`CRON_TZ` 确保服务器系统时区不是 UTC+8 时仍按北京时间触发：

```cron
CRON_TZ=Asia/Shanghai
0 19 * * * cd /opt/automatic-gif-release-c24aa3c && /opt/automatic-gif/.venv/bin/python daily_gif_report.py >> daily_gif_report.log 2>&1
```

日报读取的是比赛状态库，不依赖 GIF 文件。为避免终场清理在日报前删除前一窗口的比赛目录，服务器的 `GIF_SESSION_RETENTION_SECONDS` 至少应覆盖 26 小时；当前默认 24 小时的部署应调整为 `172800`（48 小时）后再重启 Dashboard。

同一个统计窗口只会成功发送一次；网络失败会记录为失败，下次手动重试或当天再次触发会重新发送。需要人工重发时：

```bash
cd /opt/automatic-gif-release-c24aa3c
/opt/automatic-gif/.venv/bin/python daily_gif_report.py --force
```

## 排查

```bash
tail -n 100 /opt/automatic-gif-release-c24aa3c/daily_gif_report.log
/opt/automatic-gif/.venv/bin/python daily_gif_report.py --dry-run
```

`[daily-report:error]` 表示日报本身失败，不会改变任何比赛任务或文章状态。
