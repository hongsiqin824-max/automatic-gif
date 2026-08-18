# Dashboard 与 Worker 重启操作手册

本文档适用于本项目的本地开发和运行环境，说明修改代码或配置后，如何安全停止旧进程、启动最新代码并完成验证。

## 1. 先理解两个进程

项目运行时主要有两层进程：

1. **Dashboard**：由 `dashboard_server.py` 启动，提供网页和控制接口。
2. **Worker**：Dashboard 点击“启动”后创建，实际执行直播录制、事件轮询、普通 GIF 和 AI 剪辑。Worker 还会启动 FFmpeg 子进程。

Worker 使用独立进程组运行。因此：

- 只停止 Dashboard，**不保证** Worker 和 FFmpeg 同时停止。
- 修改 Worker 相关代码后，只重启 Dashboard 但没有停止旧 Worker，旧 Worker 仍可能继续执行旧代码。
- 完整重启必须遵循：**先停止 Worker，再停止 Dashboard，最后重新启动 Dashboard 和 Worker**。

## 2. 端口说明

项目 `.env` 当前配置的默认地址是：

```text
http://127.0.0.1:8899/
```

如果启动命令临时设置了其他端口，例如：

```bash
GIF_DASHBOARD_PORT=8901 python3 dashboard_server.py
```

那么该实例地址就是 `http://127.0.0.1:8901/`。Shell 环境变量优先于 `.env`。

不要仅根据浏览器地址猜测应该停止哪个进程。先检查端口：

```bash
cd "/Users/demo/Desktop/automatic gif"
lsof -nP -iTCP:8899 -sTCP:LISTEN
lsof -nP -iTCP:8901 -sTCP:LISTEN
```

输出示例：

```text
COMMAND   PID USER   FD   TYPE ... NAME
Python  12345 demo    4u  IPv4 ... TCP 127.0.0.1:8899 (LISTEN)
```

- `PID` 是 Dashboard 进程号，每次启动都可能变化。
- 没有输出，表示该端口当前没有监听进程。
- 如果两个端口都有输出，表示可能同时运行了两个 Dashboard 实例。不要直接全部终止，应分别确认。

查看某个 PID 的完整启动命令：

```bash
ps -p 12345 -o pid,ppid,lstart,command=
```

请把示例中的 `12345` 替换为刚才 `lsof` 查到的真实 PID。

## 3. 推荐的完整重启流程

### 第一步：记录当前比赛和设置

在 Dashboard 中记录：

- 当前 `match_id`
- 普通 GIF 前置时间和后置时间
- AI 剪辑是否启用
- AI 剪辑前置时间和后置时间
- 其他本次运行需要的参数

这些运行设置主要保存在 Dashboard 内存中。Dashboard 重启后应重新检查并提交设置，不能假定旧值仍然有效。

### 第二步：先停止 Worker

#### 方法 A：通过网页停止（推荐）

打开正在运行的 Dashboard，确认比赛 ID 正确，然后点击“停止”。等待页面显示 Worker 已停止。

网页停止操作会：

- 清除 Worker 的自动恢复意图，避免 Worker 被 Dashboard 再次拉起；
- 向整个 Worker 进程组发送正常中断信号；
- 等待 Worker 收尾；
- 清理该 Worker 启动的 FFmpeg 进程。

#### 方法 B：通过接口停止

如果网页无法操作，可以先查询当前活动比赛：

```bash
curl -sS http://127.0.0.1:8899/api/matches | python3 -m json.tool
```

找到返回结果中的 `active_match_id`，然后执行：

```bash
curl -sS -X POST http://127.0.0.1:8899/api/session/stop \
  -H 'Content-Type: application/json' \
  -d '{"match_id":"你的比赛ID"}' | python3 -m json.tool
```

如果实际 Dashboard 在 `8901`，把以上 URL 中的 `8899` 改为 `8901`。

这个接口需要正确的 `match_id`。不要直接照抄“你的比赛ID”文字。

停止后可检查会话状态：

```bash
curl -sS 'http://127.0.0.1:8899/api/session?match_id=你的比赛ID' \
  | python3 -m json.tool
```

重点确认返回数据中 Worker 不再运行、`desired_running` 为 `false`，生命周期不再处于 `starting`、`playing`、`finishing` 或 `stopping`。

### 第三步：停止 Dashboard

如果 Dashboard 正在当前终端前台运行，回到该终端按：

```text
Control + C
```

这是最简单的正常停止方式。

如果找不到原终端，先查询端口对应的 PID：

```bash
lsof -nP -iTCP:8899 -sTCP:LISTEN
```

确认进程后，发送正常终止信号：

```bash
ps -p 12345 -o pid,ppid,lstart,command=
kill -TERM 12345
```

把 `12345` 替换为真实 Dashboard PID。先运行 `ps` 是为了避免 PID 已变化而终止错误进程。

不要使用以下宽泛命令：

```text
killall python3
pkill -f python
```

它们可能同时终止机器上的其他 Python 服务。

也不要把 `kill -9` 作为常规停止方式。`SIGKILL` 不给进程清理资源和写入最终状态的机会，只应在确认目标正确且普通停止持续无效时使用。

### 第四步：确认旧 Dashboard 已退出

```bash
lsof -nP -iTCP:8899 -sTCP:LISTEN
```

没有输出表示 `8899` 已释放。如果之前运行在 `8901`，也检查对应端口：

```bash
lsof -nP -iTCP:8901 -sTCP:LISTEN
```

还可以检查是否存在遗留 Worker：

```bash
pgrep -fl 'event_driven_pipeline.py'
```

没有输出表示没有匹配到 Worker。若仍有输出，不要直接批量终止；先使用下面的命令逐个确认：

```bash
ps -p 23456 -o pid,ppid,pgid,lstart,command=
```

把 `23456` 替换为真实 Worker PID。如果确实是本项目遗留 Worker，优先重新启动原 Dashboard 并通过停止接口清理。只有在 Dashboard 已无法恢复时，才根据已确认的进程组号对该进程组发送 `SIGINT`。

例如上一步 `ps` 明确显示 Worker 的 `PGID` 是 `34567`，可执行：

```bash
kill -INT -- -34567
```

这里的负号表示目标是整个进程组，而不只是一个 PID，这样 Worker 和它启动的 FFmpeg 可以一起收到中断信号。`34567` 必须替换为 `ps` 实际显示的 **PGID**，不要凭 PID 猜测，也不要照抄示例数字。等待进程正常收尾后，再重新运行 `pgrep` 检查。

### 第五步：可选，先运行测试

修改后建议先执行完整测试：

```bash
cd "/Users/demo/Desktop/automatic gif"
python3 -m unittest -v
```

测试通过不代表真实直播源一定可用，但能提前发现大部分代码回归。

### 第六步：启动最新 Dashboard

使用 `.env` 中的默认端口启动：

```bash
cd "/Users/demo/Desktop/automatic gif"
python3 dashboard_server.py
```

终端应显示类似：

```text
Football GIF dashboard: http://127.0.0.1:8899
```

该命令使用新的 Python 进程重新读取 `dashboard_server.py`、相关导入代码和 `.env`，因此 Dashboard 后端会加载磁盘上的最新代码。

如果明确需要使用 `8901`：

```bash
cd "/Users/demo/Desktop/automatic gif"
GIF_DASHBOARD_PORT=8901 python3 dashboard_server.py
```

同一套任务通常只保留一个 Dashboard 实例，避免两个实例分别启动 Worker、重复消费同一场比赛。

### 第七步：验证 Dashboard

另开一个终端执行：

```bash
curl -sS http://127.0.0.1:8899/api/health | python3 -m json.tool
```

正常响应示例：

```json
{
    "ok": true,
    "port": 8899,
    "time_unix": 1780000000.0
}
```

然后打开：

```text
http://127.0.0.1:8899/
```

若使用 `8901`，健康检查和浏览器地址都相应改成 `8901`。

### 第八步：重新配置并启动 Worker

在 Dashboard 中：

1. 选择或输入正确的比赛 ID。
2. 重新检查普通 GIF 前置、后置时间。
3. 重新检查 AI 剪辑开关及其前置、后置时间。
4. 提交设置并点击“启动”。
5. 确认页面显示新 Worker PID，运行日志开始更新。

Dashboard 启动 Worker 时，会创建新的 `python3 event_driven_pipeline.py ...` 进程。此时 Worker 才会加载 `event_driven_pipeline.py`、`live_goal_pipeline.py`、`pipeline_runtime.py`、`vision_runtime.py`、`vision_locator.py` 等文件的最新代码。

## 4. 以后修改代码时，最短应该怎么做

### 修改了 Worker/GIF/AI 相关 Python 文件

例如修改了：

- `event_driven_pipeline.py`
- `live_goal_pipeline.py`
- `pipeline_runtime.py`
- `vision_runtime.py`
- `vision_locator.py`

至少需要：

1. 在 Dashboard 点击“停止”，让旧 Worker 和 FFmpeg 退出。
2. 重新确认前后置时间和 AI 设置。
3. 点击“启动”，创建新 Worker。

如果同时修改了 Dashboard 后端，或者不能确定模块由哪个进程加载，则执行第 3 节的完整重启。

### 修改了 `dashboard_server.py`

需要完整重启 Dashboard。若 Worker 正在运行，仍然必须先停止 Worker，再停止 Dashboard。

### 修改了 `.env`

需要重启 Dashboard，因为 `.env` 在 Dashboard 启动时读取。若配置会影响 Worker，重启 Dashboard 后还要重新启动 Worker。

### 只修改了前端静态文件

例如修改了：

- `dashboard_static/app.js`
- `dashboard_static/index.html`
- `dashboard_static/runtime.css`

通常不需要重启 Python 进程，先在浏览器执行强制刷新：

```text
macOS: Command + Shift + R
```

如果页面仍显示旧内容，再完整重启 Dashboard，以排除浏览器缓存或实例地址错误。

### 修改了测试或普通文档

只修改测试文件或 Markdown 文档，不影响正在运行的服务，不需要重启。

## 5. 一份可以每次照着做的检查清单

```text
[ ] 记录 match_id、前置/后置时间和 AI 设置
[ ] 在正确的 Dashboard 实例中停止 Worker
[ ] 确认 Worker 和 FFmpeg 已退出
[ ] 用 Control+C 或 kill -TERM 停止正确的 Dashboard PID
[ ] 用 lsof 确认目标端口已释放
[ ] 可选：运行 python3 -m unittest -v
[ ] 运行 python3 dashboard_server.py
[ ] 调用 /api/health，确认 ok=true 且端口正确
[ ] 打开正确端口的 Dashboard
[ ] 重新确认设置后启动 Worker
[ ] 确认新 Worker PID 和运行日志出现
```

## 6. 常见问题

### 启动时报 `Address already in use`

说明目标端口仍被某个进程占用。不要立即改用另一个端口掩盖问题，先检查：

```bash
lsof -nP -iTCP:8899 -sTCP:LISTEN
```

确认 PID 和命令，判断它是不是旧 Dashboard。若是，按前文正常停止；若不是，不要终止它，应选择一个确认不会冲突的端口。

### 重启后前置/后置时间怎么变回去了

这些设置属于 Dashboard 会话内存状态。Dashboard 重启后需要重新检查并提交。Worker 启动时，Dashboard 会把当时的前置/后置参数写入 Worker 启动命令；Worker 启动后再修改页面数值，不会自动改变已经运行的旧 Worker。

要让新时间确定生效：先停止 Worker，设置新值，再启动 Worker，并在页面或日志中核对 Worker 使用的参数。

### Dashboard 已重启，为什么还像在运行旧代码

最常见原因有三个：

1. 旧 Worker 没有停止，仍在执行旧进程中已经加载的代码。
2. 浏览器打开的是另一个端口，例如重启了 `8899`，实际查看的却是 `8901`。
3. 只刷新了普通页面，浏览器仍缓存旧静态资源。

依次检查 Worker PID、Dashboard 监听端口，并强制刷新浏览器。

### 可以直接关闭终端窗口吗

不建议。Dashboard 可能退出，但独立进程组中的 Worker/FFmpeg 不一定随之退出。应先通过网页或停止接口停止 Worker，再用 `Control+C` 停止 Dashboard。

### 输出 GIF 和任务记录会因为重启被删除吗

正常重启不会主动删除已经生成的 GIF、SQLite 任务数据或缓冲区清单。不要额外执行清理目录、删除数据库或批量终止进程的命令。

## 7. 最常用命令速查

检查端口：

```bash
lsof -nP -iTCP:8899 -sTCP:LISTEN
```

停止 Worker：

```bash
curl -sS -X POST http://127.0.0.1:8899/api/session/stop \
  -H 'Content-Type: application/json' \
  -d '{"match_id":"你的比赛ID"}' | python3 -m json.tool
```

停止 Dashboard：

```text
在 Dashboard 所在终端按 Control+C
```

启动 Dashboard：

```bash
cd "/Users/demo/Desktop/automatic gif"
python3 dashboard_server.py
```

验证 Dashboard：

```bash
curl -sS http://127.0.0.1:8899/api/health | python3 -m json.tool
```

## 8. 命令参数解释

### `cd "/Users/demo/Desktop/automatic gif"`

- `cd`：切换当前终端所在目录。
- 路径两侧的双引号不能随意省略，因为目录名 `automatic gif` 中包含空格。
- 进入项目目录后，`python3 dashboard_server.py` 才能稳定使用项目中的相对路径和 `.env`。

### `lsof -nP -iTCP:8899 -sTCP:LISTEN`

- `lsof`：列出进程打开的文件和网络端口。
- `-n`：不把 IP 地址反查成主机名，查询更直接。
- `-P`：直接显示端口数字，不把 `8899` 转成服务名称。
- `-iTCP:8899`：只查 TCP 端口 `8899`。
- `-sTCP:LISTEN`：只显示正在监听连接的服务端进程。

这条命令只查询，不会停止或修改任何进程。

### `ps -p 12345 -o pid,ppid,lstart,command=`

- `ps`：查看进程信息。
- `-p 12345`：只查看 PID 为 `12345` 的进程。
- `-o ...`：指定显示字段。
- `pid`：当前进程号。
- `ppid`：父进程号。
- `lstart`：完整启动时间。
- `command=`：完整启动命令；末尾的 `=` 用于省略字段标题。

这一步用于在停止进程前再次确认目标确实是本项目 Dashboard 或 Worker。

### `kill -TERM 12345`

- `kill`：向指定进程发送信号，并不天然等同于强制杀死。
- `-TERM`：发送正常终止信号 `SIGTERM`，允许 Dashboard 正常退出。
- `12345`：目标 PID，必须使用刚查询并确认的真实值。

### `kill -INT -- -34567`

- `-INT`：发送与终端 `Control+C` 类似的中断信号 `SIGINT`。
- `--`：表示后面的内容不再被解析为命令选项。
- `-34567`：负数表示进程组 PGID `34567`，所以 Worker 和 FFmpeg 会一起收到信号。

这是 Dashboard 无法调用时的应急 Worker 清理命令，不是常规首选命令。

### `curl -sS -X POST ...`

- `curl`：向 Dashboard HTTP 接口发送请求。
- `-sS`：隐藏普通进度信息，但仍显示错误。
- `-X POST`：使用 HTTP POST 方法执行停止操作。
- `-H 'Content-Type: application/json'`：声明请求体是 JSON。
- `-d '{...}'`：发送包含 `match_id` 的 JSON 数据。
- `|`：把左侧命令的输出交给右侧命令。
- `python3 -m json.tool`：格式化 JSON，便于人工确认返回状态。

### `python3 dashboard_server.py`

- `python3`：启动一个新的 Python 解释器进程。
- `dashboard_server.py`：运行 Dashboard 入口文件。
- 命令保持在前台运行，终端会持续显示服务日志；按 `Control+C` 可正常停止。

每次重新执行都会从磁盘重新加载代码。它不会自动替换已经运行的旧实例，所以启动前必须先确认目标端口已经释放。

### `GIF_DASHBOARD_PORT=8901 python3 dashboard_server.py`

- `GIF_DASHBOARD_PORT=8901`：只对本次命令临时设置端口。
- 该设置优先于 `.env` 中的 `GIF_DASHBOARD_PORT=8899`。
- 关闭这个进程后，临时设置不会自动修改 `.env`。

### `pgrep -fl 'event_driven_pipeline.py'`

- `pgrep`：按命令特征查找进程。
- `-f`：匹配完整命令行，而不只匹配短进程名。
- `-l`：显示匹配到的 PID 和进程信息。

它用于发现可能遗留的 Worker，只查询、不终止。匹配结果必须再用 `ps` 确认。
