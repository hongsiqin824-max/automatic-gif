# 服务器服务安全关闭手册

本文档用于关闭服务器上的 Football GIF Dashboard 及它启动的比赛 Worker。以后关闭服务时，按本文档执行，不要直接批量杀 Python 进程。

## 1. 适用范围和目录

服务器固定发布目录目前是：

```text
/opt/automatic-gif-release-c24aa3c
```

本项目有两层进程：

| 进程 | 作用 | 关闭时的要求 |
| --- | --- | --- |
| `dashboard_server.py` | 网页、API、自动发现比赛、启动/管理 Worker | 最后关闭 |
| `event_driven_pipeline.py` | 单场比赛的直播拉流、TS 分片、OCR、GIF | 先关闭 |
| `ffmpeg` | Worker 启动的拉流或编码子进程 | 必须随对应 Worker 一起确认 |

只关闭 Dashboard 不代表 Worker 一定退出。完整关闭必须是：先停比赛 Worker，再停 Dashboard，最后检查端口和遗留进程。

## 2. 安全规则

执行任何 `kill` 前，必须先用 `ps` 核对 PID、完整命令和进程组 PGID。

不要使用以下命令：

```bash
killall python3
pkill -f python
pkill -f ffmpeg
```

这些命令可能误杀服务器上的其他服务。不要使用 `git clean -fd`、删除数据库、删除 TS/GIF 目录来“清理进程”。关闭服务不会删除业务数据。

## 3. 第一步：进入发布目录并确认端口

```bash
cd /opt/automatic-gif-release-c24aa3c
pwd
lsof -nP -iTCP:8899 -sTCP:LISTEN
```

`pwd` 应输出固定发布目录。`lsof` 如果看到类似下面的内容，第二列就是 Dashboard PID：

```text
COMMAND   PID   USER  ... NAME
python   104507  root  ... TCP 127.0.0.1:8899 (LISTEN)
```

如果 8899 没有输出，说明 Dashboard 可能已经关闭；继续执行第 6 节的遗留 Worker 检查，不要因为端口为空就认为所有进程都已清理。

确认 Dashboard PID 的完整命令：

```bash
ps -p 104507 -o pid,ppid,pgid,lstart,args
```

把示例中的 `104507` 替换成 `lsof` 实际显示的 PID。命令中应包含 `dashboard_server.py`，否则先停止操作并核对端口或 PID。

## 4. 第二步：通过 Dashboard 停止活动比赛

只要 8899 还在运行，优先通过接口逐场停止，这会清除自动恢复意图，并让 Worker 正常清理 FFmpeg：

```bash
curl -sS http://127.0.0.1:8899/api/matches | python3 -m json.tool
```

查看返回的 `active_match_ids` 或 `active_matches`。对每个实际运行的 `match_id` 执行一次：

```bash
curl -sS -X POST http://127.0.0.1:8899/api/session/stop \
  -H 'Content-Type: application/json' \
  -d '{"match_id":"实际比赛ID"}' | python3 -m json.tool
```

例如比赛 ID 是 `54515122`：

```bash
curl -sS -X POST http://127.0.0.1:8899/api/session/stop \
  -H 'Content-Type: application/json' \
  -d '{"match_id":"54515122"}' | python3 -m json.tool
```

停止后可以核对：

```bash
curl -sS 'http://127.0.0.1:8899/api/session?match_id=54515122' \
  | python3 -m json.tool
```

应看到 `desired_running` 为 `false`，并且生命周期不再是 `starting`、`playing`、`finishing` 或 `stopping`。

如果停止接口返回 JSON 中包含 `error`，尤其是 HTTP 409，不要把它当成已经停止；继续执行第 6 节的进程核对，确认 Worker 进程组确实消失。

如果 `active_match_count` 已经是 `0`，说明 Dashboard 当前没有纳入管理的活动比赛，可以跳过本节；但仍必须执行下一节，因为可能存在旧 Dashboard 未登记的 Worker。

## 5. 第三步：关闭 Dashboard

### 方式 A：Dashboard 在当前终端前台运行

回到启动它的终端按：

```text
Control + C
```

### 方式 B：Dashboard 由 nohup 或后台命令运行

再次确认 PID：

```bash
lsof -nP -iTCP:8899 -sTCP:LISTEN
ps -p 104507 -o pid,ppid,pgid,lstart,args
```

确认命令是本项目的 `dashboard_server.py` 后，发送正常终止信号：

```bash
kill -TERM 104507
```

等待 5 秒：

```bash
sleep 5
```

检查端口是否释放：

```bash
lsof -nP -iTCP:8899 -sTCP:LISTEN
```

没有输出表示 Dashboard 已停止监听。此时仍要继续检查 Worker。

## 6. 第四步：检查遗留 Worker

```bash
pgrep -af 'event_driven_pipeline.py'
```

没有输出表示没有匹配到 Worker。若有输出，不要直接执行 `kill -9`，对每个 PID 分别确认。

假设显示 Worker PID 为 `92341`：

```bash
ps -p 92341 -o pid,ppid,pgid,lstart,args
```

确认命令行包含 `event_driven_pipeline.py` 和正确的 `--match-id`，再取得它的进程组号：

```bash
WORKER_PGID=$(ps -p 92341 -o pgid= | tr -d ' ')
echo "$WORKER_PGID"
```

如果 Worker 在查询过程中已经退出，PGID 可能为空。发送信号前必须执行下面的校验；校验失败就停止操作，不要执行后面的 `kill`：

```bash
case "$WORKER_PGID" in
  ''|*[!0-9]*) echo "PGID 无效，停止操作"; exit 1;;
esac
if [ "$WORKER_PGID" -le 1 ]; then
  echo "PGID 过小，停止操作"
  exit 1
fi
```

查看该进程组的全部成员：

```bash
ps -eo pid,ppid,pgid,lstart,args | awk -v pgid="$WORKER_PGID" '$3 == pgid'
```

只有当列表中的进程都属于这场比赛的 Worker、FFmpeg 或其明确子进程时，才可以继续。若混入无关服务，停止操作并人工核对。

## 7. 第五步：按进程组清理遗留 Worker

### 7.1 第一次：正常中断

```bash
kill -INT -- -"$WORKER_PGID"
sleep 10
ps -eo pid,ppid,pgid,lstart,args | awk -v pgid="$WORKER_PGID" '$3 == pgid'
```

`-INT` 类似终端中的 `Control+C`，给 Worker 和 FFmpeg 留出写入状态、关闭文件和正常收尾的机会。

### 7.2 第二次：正常终止

如果进程组仍然存在：

```bash
kill -TERM -- -"$WORKER_PGID"
sleep 5
ps -eo pid,ppid,pgid,lstart,args | awk -v pgid="$WORKER_PGID" '$3 == pgid'
```

### 7.3 最后才强制结束

只有在已经确认 PGID 属于本项目、前两种信号均无效，并且进程持续占用资源时，才执行：

```bash
kill -KILL -- -"$WORKER_PGID"
```

然后再次检查进程组。`SIGKILL` 可能来不及写最终日志或状态，不应作为常规关闭方式。

## 8. 第六步：最终验收

依次执行：

```bash
lsof -nP -iTCP:8899 -sTCP:LISTEN
pgrep -af 'dashboard_server.py'
pgrep -af 'event_driven_pipeline.py'
```

三条命令都没有输出，表示 Dashboard 和比赛 Worker 已关闭。

再检查 FFmpeg：

```bash
pgrep -af 'ffmpeg'
```

如果有输出，逐个使用 `ps -p <PID> -o pid,ppid,pgid,lstart,args` 检查是否属于本项目，不能直接批量杀。

确认服务已关闭时，健康检查应连接失败：

```bash
curl -fsS http://127.0.0.1:8899/api/health
```

类似下面的结果是预期的：

```text
curl: (7) Failed to connect to 127.0.0.1 port 8899
```

最后等待 15 秒再检查一次 8899，排除 systemd 或其他外部守护程序自动拉起服务：

```bash
sleep 15
lsof -nP -iTCP:8899 -sTCP:LISTEN
pgrep -af 'dashboard_server.py'
pgrep -af 'event_driven_pipeline.py'
```

## 9. 关闭完成清单

```text
[ ] 已确认服务器发布目录
[ ] 已查询 Dashboard PID 和完整启动命令
[ ] 已通过接口停止活动比赛，或确认 active_match_count 为 0
[ ] 已停止 Dashboard
[ ] 已检查 event_driven_pipeline.py 遗留 Worker
[ ] 已按 PGID 核对并清理遗留 Worker/FFmpeg
[ ] 8899 没有监听进程
[ ] 没有 dashboard_server.py 进程
[ ] 没有 event_driven_pipeline.py 进程
[ ] 已等待 15 秒确认服务没有自动重启
```

正常关闭不会删除 SQLite、TS、GIF、日志、OCR 虚拟环境或发布记录。
