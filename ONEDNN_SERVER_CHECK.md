# 服务器 oneDNN 关闭核验手册

本文只适用于生产服务器。当前约定是：

- 服务器关闭 PaddleOCR 使用的 oneDNN（Paddle 的环境变量名称是 `FLAGS_use_mkldnn`）。
- 本地 Apple Silicon 环境可以按本地需要开启或关闭 oneDNN；不要把服务器的 `.env` 或启动命令原样复制到本地。
- oneDNN 只影响 PaddleOCR 识别进程，不影响 FFmpeg、默认 GIF 生成、事件接口或文章发布接口。
- 服务器发布目录、密钥、SQLite 数据库和 GIF 文件不在本手册中修改。

## 一、先看清楚进程链路

实际调用关系如下：

```text
Dashboard
  -> event_driven_pipeline.py（比赛 Worker）
  -> scoreboard_ocr.py（OCR 客户端）
  -> tmp/ocr_venv/bin/python
  -> scoreboard_ocr_worker.py（PaddleOCR）
```

`dashboard_server.py` 启动时读取项目目录的 `.env`。代码使用
`os.environ.setdefault`，因此已经存在的 Shell 或 systemd 环境变量优先级更高，
`.env` 中的同名值不会覆盖它。比赛 Worker 和持久 OCR Worker 都继承启动它们的
父进程环境，所以只检查 `.env` 不足以证明真实 OCR 进程已经关闭 oneDNN。

另外，持久 OCR Worker 是独立的 `--serve-socket` 进程，默认可以空闲约 900 秒。
重启 Dashboard 后它不一定自动退出。必须确认旧 OCR Worker 已结束，并让下一次
OCR 请求重新启动一个继承 `FLAGS_use_mkldnn=0` 的进程。

## 二、没有比赛时可以完成的检查

以下命令都在服务器 SSH 终端执行。当前服务器发布目录为：

```bash
cd /opt/automatic-gif-release-c24aa3c
pwd
```

预期输出：

```text
/opt/automatic-gif-release-c24aa3c
```

如果路径不同，先停止操作并以实际正在运行的 release 目录为准；不要在错误目录
修改 `.env`。

### 1. 检查 `.env` 是否写了关闭值

不要用 `cat .env`，因为其中可能包含密钥。只输出 oneDNN 这一行：

```bash
grep -nE '^[[:space:]]*(export[[:space:]]+)?FLAGS_use_mkldnn[[:space:]]*=' .env
```

理想输出只有一行，值为 `0`，例如：

```text
27:FLAGS_use_mkldnn=0
```

结果含义：

- 一行且为 `0`：文件配置正确。
- 没有输出：`.env` 没有配置，必须依靠启动命令或 systemd；当前不能算已确认。
- 值为 `1`、`true` 或出现多行：存在开启或重复配置风险，先不要启动比赛。

可在不泄露其他配置的情况下检查解析后的值：

```bash
awk -F= '
  /^[[:space:]]*(export[[:space:]]+)?FLAGS_use_mkldnn[[:space:]]*=/ {
    key=$1; gsub(/^[[:space:]]+|[[:space:]]+$/, "", key)
    sub(/^export[[:space:]]+/, "", key)
    value=$2; gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
    print "line " NR ": " key "=" value
  }
' .env
```

### 2. 检查 Dashboard 是否正在运行

```bash
ps -eo pid,ppid,lstart,args | grep -E '[d]ashboard_server.py'
```

没有输出表示 Dashboard 当前没有运行。若有输出，记下真实 PID，不要照抄示例数字。
例如：

```text
  24680   12001 Mon Aug 31 10:00:00 2026 /opt/automatic-gif/.venv/bin/python dashboard_server.py
```

检查该 Dashboard 进程实际收到的环境变量：

```bash
DASHBOARD_PID=24680
tr '\0' '\n' < "/proc/$DASHBOARD_PID/environ" \
  | grep '^FLAGS_use_mkldnn=' \
  || echo 'Dashboard 进程没有看到 FLAGS_use_mkldnn'
```

预期输出：

```text
FLAGS_use_mkldnn=0
```

如果显示 `1`，说明 Shell、systemd 或其他启动器覆盖了 `.env`。如果显示“没有看到”，
说明 Dashboard 没有继承该变量；仅凭 `.env` 不能把它当作关闭状态。由于代码可能
在进程启动后才从 `.env` 加入变量，最可靠的做法是按第四节在启动命令中显式写 `0`。

### 3. 检查 OCR 虚拟环境和 Paddle 探针

```bash
OCR_PY=/opt/automatic-gif-release-c24aa3c/tmp/ocr_venv/bin/python
test -x "$OCR_PY" && echo "OCR environment OK" || echo "OCR environment missing"
```

正常输出：

```text
OCR environment OK
```

然后执行一次独立的 Paddle 运行时探针：

```bash
FLAGS_use_mkldnn=0 "$OCR_PY" -c '
import os, paddle
print({"env": os.getenv("FLAGS_use_mkldnn"),
       "paddle": paddle.get_flags(["FLAGS_use_mkldnn"])})
'
```

关键预期结果：

```text
{'env': '0', 'paddle': {'FLAGS_use_mkldnn': False}}
```

Paddle 可能同时输出模型或版本提示；只要最后的字典中 `env` 是 `'0'`、`paddle` 是
`False`，就说明这个 OCR Python 环境能够按关闭方式运行。这个探针只证明本次命令，
不能替代真实比赛 OCR Worker 的检查。

## 三、重启前清理旧进程

没有比赛时，先检查是否有遗留的持久 OCR Worker：

```bash
pgrep -af '[s]coreboard_ocr_worker.py'
```

没有输出是正常的。若有输出，逐个确认：

```bash
OCR_PID=24701
ps -p "$OCR_PID" -o pid,ppid,pgid,lstart,args=
readlink -f "/proc/$OCR_PID/cwd"
```

只有在命令明确显示该 PID 属于当前项目的
`/opt/automatic-gif-release-c24aa3c/scoreboard_ocr_worker.py --serve-socket ...`
时，才结束这个**单独的 PID**：

```bash
kill -TERM "$OCR_PID"
sleep 2
ps -p "$OCR_PID" -o pid,ppid,lstart,args= || echo "旧 OCR Worker 已退出"
```

不要使用 `killall python3`、`pkill -f python` 或宽泛的 `pkill`，否则可能终止机器上
其他服务。若 OCR Worker 不属于当前 release，先记录它的目录和启动命令，不要误杀。

Dashboard/比赛 Worker 的完整停止顺序仍按 [RESTART_GUIDE.md](RESTART_GUIDE.md)：
先通过页面或接口停止比赛 Worker，再停止 Dashboard，最后确认旧进程已经退出。

## 四、推荐的服务器启动方式

### 方式 A：服务器使用 nohup 启动

即使 `.env` 已有 `FLAGS_use_mkldnn=0`，也建议在启动命令中再次明确写 `0`，防止
当前 SSH Shell 中残留 `FLAGS_use_mkldnn=1`：

```bash
cd /opt/automatic-gif-release-c24aa3c
nohup env PYTHONUNBUFFERED=1 FLAGS_use_mkldnn=0 \
  /opt/automatic-gif/.venv/bin/python dashboard_server.py \
  >> dashboard.log 2>&1 &
echo $!
```

`echo` 输出的是新的 Dashboard PID。等待约 3 秒后检查：

```bash
sleep 3
curl -fsS http://127.0.0.1:8899/api/health
```

必须看到类似：

```json
{"ok":true,"port":8899}
```

再确认新 Dashboard 的进程环境：

```bash
ps -eo pid,ppid,lstart,args | grep -E '[d]ashboard_server.py'
DASHBOARD_PID=<上一步查到的新PID>
tr '\0' '\n' < "/proc/$DASHBOARD_PID/environ" \
  | grep '^FLAGS_use_mkldnn=' \
  || echo 'Dashboard 进程没有看到 FLAGS_use_mkldnn'
```

必须输出 `FLAGS_use_mkldnn=0`。查看启动日志时不要把带密钥的环境内容贴出来：

```bash
tail -n 80 /opt/automatic-gif-release-c24aa3c/dashboard.log
```

### 方式 B：服务器使用 systemd

先找实际服务名：

```bash
systemctl list-units --type=service --all | grep -iE 'automatic|gif|dashboard'
```

把下面的 `<服务名>` 替换成真实名称，查看其启动配置：

```bash
sudo systemctl cat <服务名>
sudo systemctl show <服务名> -p MainPID -p Environment
```

推荐使用 systemd drop-in，不直接改发行版生成的 unit 文件。执行：

```bash
sudo systemctl edit <服务名>
```

编辑器打开后，在文件中写入：

```ini
[Service]
Environment=FLAGS_use_mkldnn=0
```

保存并退出编辑器。如果使用 `systemctl edit` 后看到的 drop-in 文件已有 `[Service]`，只需在该段增加
`Environment=FLAGS_use_mkldnn=0`，不要删除原有的 `ExecStart`、用户、工作目录或其他环境变量。

然后执行：

```bash
sudo systemctl daemon-reload
sudo systemctl restart <服务名>
sudo systemctl status <服务名> --no-pager
```

再用本手册的 `ps`、`/proc/$PID/environ` 和 `/api/health` 检查。systemd 的
`Environment=FLAGS_use_mkldnn=0` 必须和实际启动 Dashboard 的 unit 对应；只修改另一个
没有被使用的 unit 不会生效。

## 五、有比赛且 OCR 正在运行时的最终验收

前四步只能确认配置和 Paddle 环境。要确认真实链路，必须在 Dashboard 中启动一场开启
OCR 的比赛，并在页面出现 OCR 处理中时执行：

```bash
pgrep -af '[s]coreboard_ocr_worker.py'
```

对输出中的每一个真实 PID 检查：

```bash
for pid in $(pgrep -f '[s]coreboard_ocr_worker.py' || true); do
  echo "----- OCR PID: $pid -----"
  ps -p "$pid" -o pid,ppid,lstart,args=
  printf 'cwd: '
  readlink -f "/proc/$pid/cwd" || true
  tr '\0' '\n' < "/proc/$pid/environ" \
    | grep '^FLAGS_use_mkldnn=' \
    || echo '没有看到 FLAGS_use_mkldnn'
done
```

每个属于当前 release 的 OCR Worker 都应显示：

```text
FLAGS_use_mkldnn=0
```

如果没有 OCR Worker：

- 没有比赛或 OCR 尚未开始时，这是正常现象；
- 不能据此说 OCR 已关闭 oneDNN，也不能据此说 OCR 失败；
- 等页面显示 OCR 处理中后再执行本节命令。

如果只看到短暂的一次性 OCR 进程，可以连续观察几秒：

```bash
while true; do
  date '+%F %T'
  pgrep -af '[s]coreboard_ocr_worker.py' || true
  sleep 1
done
```

观察到目标 PID 后按 `Control-C` 停止观察，再检查该 PID 的 `/proc` 环境。完成检查后
可按 `Control-C` 退出循环，不会影响服务。

## 六、验收记录模板

每次服务器更新或重启后，建议保存以下**不含密钥**的记录：

```text
检查时间：YYYY-MM-DD HH:MM（服务器时区）
release 目录：/opt/automatic-gif-release-c24aa3c
Dashboard PID：
Dashboard FLAGS_use_mkldnn：0 / 其他 / 未设置
OCR Python：/opt/automatic-gif-release-c24aa3c/tmp/ocr_venv/bin/python
Paddle 探针：env=0，paddle=False / 失败原因
真实 OCR Worker PID：无比赛 / PID 列表
真实 OCR Worker FLAGS_use_mkldnn：全部为 0 / 其他
真实 OCR Worker cwd：
健康检查：{"ok":true,"port":8899}
日志结论：
```

只有在“Dashboard 环境为 0、Paddle 探针为 False、真实 OCR Worker（如有）环境也为
0”全部满足时，才可以写“服务器 OCR 已确认关闭 oneDNN”。没有比赛时，最后一项应记为
“无比赛，待下一场 OCR 运行时补验”，不要写成已完成。

## 七、异常对照表

| 现象 | 含义 | 处理 |
| --- | --- | --- |
| `.env` 为 0，Dashboard 为 1 | Shell 或 systemd 覆盖了 `.env` | 用启动命令 `env FLAGS_use_mkldnn=0`，或在正确的 systemd unit 设置 `Environment=`，再重启 |
| Dashboard 为 0，但 OCR Worker 为 1 | 旧的 detached Worker 尚未退出，或 Worker 由另一套服务启动 | 确认 PID、cwd、命令后只结束该项目旧 PID，再让新 OCR 请求重新拉起 |
| 没有 OCR Worker | 当前没有正在执行 OCR | 不作成功/失败判断，等 OCR 任务运行时复查 |
| OCR Python 不存在 | 服务器 OCR 虚拟环境缺失或路径错误 | 先修复/恢复 `tmp/ocr_venv`；不要把它误判为 oneDNN 问题 |
| Paddle 探针导入失败 | Paddle/PaddleOCR 依赖或模型环境问题 | 记录完整错误，先处理 OCR 环境；不要修改默认 GIF 链路 |
| 所有环境都为 0，仍生成兜底 GIF | 原因不一定是 oneDNN | 继续看 OCR 日志中的视频分片、时钟读取、队列等待、超时和回退原因；oneDNN 检查本身已通过 |

## 八、本地与服务器的边界

本地服务可以使用自己的 `.env` 和 Python 环境开启 oneDNN；服务器服务必须单独使用
`FLAGS_use_mkldnn=0`。两边代码可以相同，运行环境变量仍然可以不同。更新代码时只
同步版本控制中的代码，不要覆盖服务器 `.env`、`tmp/ocr_venv`、账号池、SQLite 或
`data/published_gifs`。服务器关闭 oneDNN 的验收以本手册第五节真实 OCR Worker 的
`/proc` 结果为准，不以本地结果或 Paddle 独立探针代替。
