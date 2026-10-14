# 服务器服务完整重启手册

本文档用于服务器上完整重启 Football GIF Dashboard。完整重启会停止当前比赛 Worker，再停止旧 Dashboard，启动新 Dashboard，最后由页面或接口重新启动需要处理的比赛。

如果不希望中断正在处理的比赛，不要执行本文档的完整流程；应只重启前端或等待比赛完成。完整重启会中断直播拉流、OCR 和 GIF 任务，未完成任务需要重新启动或按项目恢复逻辑重新接纳。

## 1. 固定位置和默认端口

服务器发布目录：

```text
/opt/automatic-gif-release-c24aa3c
```

默认 Dashboard 地址：

```text
http://127.0.0.1:8899/
```

服务器标准启动命令使用：

```text
/opt/automatic-gif/.venv/bin/python dashboard_server.py
```

不要用系统默认 `python3` 代替服务器项目虚拟环境，避免加载错误的 Flask、PaddleOCR 或其他依赖。

## 2. 重启前记录当前运行情况

先进入发布目录：

```bash
cd /opt/automatic-gif-release-c24aa3c
pwd
```

检查 Dashboard：

```bash
lsof -nP -iTCP:8899 -sTCP:LISTEN
```

如果端口正在监听，记录 Dashboard PID，并检查活动比赛：

```bash
curl -sS http://127.0.0.1:8899/api/matches | python3 -m json.tool
```

建议记录：

- `match_id`；
- 普通 GIF 前置和后置秒数；
- OCR/视觉是否启用；
- 文章发布或草稿模式；
- 当前正在等待、编码或发布的任务。

Dashboard 重启后，页面中的临时运行设置需要重新核对，不能假定旧值仍在内存中。

## 3. 重启前停止旧 Worker

只要旧 Dashboard 还能访问，逐场执行：

```bash
curl -sS -X POST http://127.0.0.1:8899/api/session/stop \
  -H 'Content-Type: application/json' \
  -d '{"match_id":"实际比赛ID"}' | python3 -m json.tool
```

如果 `active_match_count` 已经为 `0`，可以跳过逐场停止，但仍需执行：

```bash
pgrep -af 'event_driven_pipeline.py'
```

如发现遗留 Worker，必须按照 [SERVICE_SHUTDOWN_GUIDE.md](SERVICE_SHUTDOWN_GUIDE.md) 的进程组核对和清理步骤处理，不能直接进入启动步骤。

## 4. 停止旧 Dashboard

查询并确认 Dashboard PID：

```bash
lsof -nP -iTCP:8899 -sTCP:LISTEN
ps -p <dashboard_pid> -o pid,ppid,pgid,lstart,args
```

确认命令是当前发布目录中的 `dashboard_server.py` 后：

```bash
kill -TERM <dashboard_pid>
sleep 5
lsof -nP -iTCP:8899 -sTCP:LISTEN
```

如果端口仍被监听，重新查询 PID，不要重复对旧数字发送信号。若有 Worker 遗留，先按关闭手册清理。

## 5. 可选：代码更新时才执行拉取

如果只是重启服务，不需要拉代码。只有明确需要部署已经合并到 `main` 的代码时，才在固定发布目录执行：

```bash
cd /opt/automatic-gif-release-c24aa3c
GIT_TERMINAL_PROMPT=0 git -c http.version=HTTP/1.1 pull --ff-only origin main
```

成功时应出现类似：

```text
Updating <old>..<new>
Fast-forward
```

禁止用 `git reset --hard`、`git clean -fd` 或强制覆盖服务器专属配置。拉取后确认 OCR 环境链接仍可执行：

```bash
test -x tmp/ocr_venv/bin/python \
  && echo "OCR environment OK" \
  || echo "OCR environment missing"
```

如果输出 `OCR environment missing`，先停止部署，不要删除或重建环境；需要单独排查服务器环境。

## 6. 启动新的 Dashboard

确认端口已经释放后执行：

```bash
cd /opt/automatic-gif-release-c24aa3c
nohup env PYTHONUNBUFFERED=1 \
  /opt/automatic-gif/.venv/bin/python dashboard_server.py \
  >> dashboard.log 2>&1 &
echo $!
```

最后一行会打印新 Dashboard 的启动 PID。`nohup` 让服务脱离当前 SSH 终端，`PYTHONUNBUFFERED=1` 让日志及时写入 `dashboard.log`。

如果需要让服务保持在当前终端前台运行，可使用：

```bash
cd /opt/automatic-gif-release-c24aa3c
/opt/automatic-gif/.venv/bin/python dashboard_server.py
```

前台方式下按 `Control+C` 停止；生产服务器建议使用前面的 `nohup` 方式或已有 systemd 管理，不要同时启动两个实例。

## 7. 启动后健康检查

等待几秒后执行：

```bash
sleep 3
curl -fsS http://127.0.0.1:8899/api/health | python3 -m json.tool
```

正常响应应包含：

```json
{
    "ok": true,
    "port": 8899
}
```

再确认监听进程和命令：

```bash
lsof -nP -iTCP:8899 -sTCP:LISTEN
ps -p <new_dashboard_pid> -o pid,ppid,pgid,lstart,args
```

如果健康检查失败，立即查看最近日志：

```bash
tail -n 100 dashboard.log
```

常见原因是端口仍被占用、虚拟环境不可执行、`.env` 或服务器专属配置缺失。不要连续重复启动，否则可能产生多个 Dashboard。

健康检查通过只代表 Dashboard 已经监听 8899，不代表自动接纳已经启动比赛。自动接纳还要等待赛事目录轮询、直播源查询和必要的空源确认；因此启动后短时间内 `active_match_count` 为 0 可能是正常的。

## 8. 检查自动发现和容量

Dashboard 健康后，查看比赛和自动接纳状态：

```bash
curl -sS http://127.0.0.1:8899/api/matches | python3 -m json.tool
curl -sS http://127.0.0.1:8899/api/auto-admission | python3 -m json.tool
```

重点确认：

- `health.status` 正常；
- `active_match_count` 与实际 Worker 数一致；
- `available_worker_slots` 没有被旧进程占用；
- 自动接纳没有持续报错；
- `heavy_tasks` 没有异常堆积。

赛事目录中的 `playing` 只表示外部赛事接口认为比赛进行中，不等于本服务已经启动 Worker。实际是否在处理，以 `active_matches`、会话状态和进程检查为准。

如果等待一个完整轮询周期后仍没有 Worker，先查看 `/api/auto-admission` 中对应比赛的状态和原因，再检查直播源接口，不要重复启动多个 Dashboard。

## 9. 重新启动比赛 Worker

打开：

```text
http://服务器地址:8899/
```

然后：

1. 重新确认并保存 GIF 前置、后置时间；
2. 重新确认 OCR、视觉和发布/草稿配置；
3. 输入或选择正确的 `match_id`；
4. 点击“启动实时处理”；
5. 等待页面出现 Worker 运行状态和日志。

也可以使用接口启动单场比赛，但必须传入项目页面要求的完整参数。不要凭空拼接 JSON；优先使用页面操作，或先查看现有启动请求格式。

启动后用以下命令确认实际 Worker：

```bash
pgrep -af 'event_driven_pipeline.py'
```

必要时再查看该 Worker 的 PID、PGID 和完整参数：

```bash
ps -p <worker_pid> -o pid,ppid,pgid,lstart,args
```

## 10. 重启完成验收清单

```text
[ ] 旧 Worker 已通过 Dashboard 停止，或已按 PGID 清理
[ ] 旧 Dashboard 已退出，8899 已释放
[ ] 没有执行批量 kill 或删除数据目录
[ ] 代码更新时 pull 使用了 --ff-only
[ ] tmp/ocr_venv/bin/python 检查为 OCR environment OK
[ ] 新 Dashboard 使用 /opt/automatic-gif/.venv/bin/python 启动
[ ] /api/health 返回 ok=true 且 port=8899
[ ] dashboard.log 没有启动异常
[ ] 自动接纳和容量状态正常
[ ] 已重新核对页面运行参数
[ ] 需要处理的比赛已重新启动并出现 Worker
```

## 11. 常见问题

### 端口仍显示被占用

重新执行：

```bash
lsof -nP -iTCP:8899 -sTCP:LISTEN
ps -p <pid> -o pid,ppid,pgid,lstart,args
```

确认该 PID 是旧 Dashboard 后再发送 `kill -TERM`。如果不是本项目，不能终止它，应改用已经确认的其他端口或联系服务器维护人。

### Dashboard 已关闭，但 Worker 仍在运行

这是因为 Worker 是独立进程组。按关闭手册先查询 PGID，再对确认过的进程组发送 `SIGINT`，必要时再升级为 `SIGTERM`，最后才考虑 `SIGKILL`。

### 服务一启动就退出

查看：

```bash
tail -n 100 dashboard.log
```

常见原因包括 Python 路径错误、环境变量不完整、端口冲突或服务器专属配置被覆盖。不要删除数据库、TS 分片或 OCR 环境来处理启动失败。

### 服务器重启后服务没有自动启动

本手册是人工启动流程。若希望机器重启后自动恢复，应另行配置并验收 systemd 服务，至少确保 `KillMode=control-group`、工作目录、Python 路径、日志路径和健康检查都正确；不要临时创建未经验证的 systemd 单元。
