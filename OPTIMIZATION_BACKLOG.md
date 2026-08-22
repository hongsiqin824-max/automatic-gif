# 后续优化清单

本清单记录当前已确认、但暂不影响主流程的优化项。加入清单不代表立即修改，实施前需要先确认产品契约和兼容范围。

## 1. TimelineState 的 origin 更新策略

- **优先级**：中
- **位置**：`pipeline_runtime.py` 的 `upsert_timeline_state()`
- **现状**：`ON CONFLICT` 分支会直接更新 `timeline_origin_wall_unix` 和 `timeline_origin_stream_time`。
- **风险**：同一 match 重复 upsert 时，如果传入不同 origin，已有的 wall time/stream time 换算坐标可能整体漂移；两个 origin 必须作为一对处理。
- **当前影响**：现有主流程通常会复用已保存的 origin，并在 manifest 与数据库不一致时拒绝恢复，因此暂未观察到必现故障。
- **后续处理**：先明确 origin 是“创建后不可变”，还是允许显式重新校准；随后补充成对更新、manifest 同步和重复 upsert 的回归测试。

## 2. legacy scoreboard 路径的降级模式参数

- **优先级**：中（仅在 legacy 路径仍受支持时）
- **位置**：`live_goal_pipeline.py` 的 scoreboard 调用链
- **现状**：legacy scoreboard 路径未传递 `allow_degraded=True`。
- **风险**：在降级条件下，旧路径可能因为未开启降级模式而拒绝继续处理或表现与新版 event-driven 路径不一致。
- **当前影响**：当前 Dashboard 使用 `event_driven_pipeline.py`，因此主流程不受影响；如果旧脚本仍被直接调用或作为回退方案，风险仍然存在。
- **后续处理**：确认 legacy 脚本是否继续对外支持；若继续支持，统一参数传递并补充降级场景测试；若已废弃，则在文档中明确边界。

## 3. 普通时间/补时 ROI 分类缓存与当前事件自动重定位

- **状态**：待实施
- **优先级**：高（先实施当前事件重定位，再实施分类缓存）
- **范围**：仅 OCR 精确定位链路；默认 GIF 不得等待或依赖本功能
- **主要位置**：`pipeline_runtime.py`、`vision_runtime.py`、`scoreboard_ocr_worker.py`、`test_ocr_budget_roi_cache.py`、`test_vision_runtime.py`

### 3.1 现状与问题

- `scoreboard_roi_cache` 目前以 `match_id` 作为唯一主键，每场比赛只能保存一份 ROI。
- 普通时间的时钟常为 `42:17`，补时可变为 `90+2`、`90+5`，记分牌宽度和时钟位置可能同时改变。
- 已观察到比赛 `54355915` 的普通 ROI 约为 `[186, 91, 304, 125]`，`90+2` 的自动发现 ROI 约为 `[104, 96, 385, 128]`，补时布局明显更宽。
- 已观察到比赛 `54480314` 在 `90+5` 目标附近使用旧 ROI 出现 `clock_profile_mismatch`。
- 当带缓存 profile 的当前 OCR 请求失配时，代码会记录失败并使缓存失效，但只允许下一个请求无缓存重新发现 ROI；当前事件仍可能直接进入后续降级或失败。
- 普通时间也出现过 profile 失配，因此“只分成普通/补时两份缓存”不是完整解法，必须同时实施当前事件的有界重定位。

### 3.2 目标契约

1. 事件进入 OCR 时，根据 `minute_extra` 或事件分钟中的 `+` 得到 `layout_mode`：`normal` 或 `stoppage`。
2. 自动 ROI 缓存按 `match_id + layout_mode + resolution_key` 隔离，不再让普通时间和补时相互覆盖。
3. 当自动缓存 profile 触发 `clock_profile_mismatch` 时，当前事件立即去掉缓存 profile，在相同已租用视频窗口上自动发现 ROI 并重试一次。
4. 自动重定位每个事件最多一次，尝试次数持久化，进程重启不得导致无限重试。
5. 重定位成功后，仅更新当前 `layout_mode + resolution_key` 的自动缓存；不覆盖其他布局的缓存。
6. 人工明确配置的 `scoreboard_profile` 保持最高优先级，失配时不允许自动覆盖或重定位。
7. 两次 OCR 都失败后才进入现有降级语义；不改动 OCR/T-DEED/分钟级 GIF/最终失败的现有优先级。
8. 默认 GIF 仍按原有独立任务立即生成，不等待 ROI 查找、OCR 或重定位。

### 3.3 数据结构与迁移方案

- 新增版本化缓存表，建议名为 `scoreboard_roi_cache_v2`，联合主键为 `(match_id, layout_mode, resolution_key)`。
- 字段至少包含 `profile_json`、`confidence`、`success_streak`、`failure_streak`、`updated_at_unix`；`layout_mode` 只接受 `normal/stoppage`。
- `resolution_key` 使用 OCR 实际分析画面尺寸，格式为 `WIDTHxHEIGHT`，例如 `1920x1080`，不使用未验证的接口字段。
- 读取时优先选择相同布局和完全相同分辨率的记录；当本次分辨率尚不可知时，可暂用当前布局最新记录，交由 worker 的分辨率/内容质量验证把关。
- 旧表中的 profile 可能来自普通时间，也可能来自补时，不能无条件迁移为 `normal`。新代码只读 v2 表，旧表暂时保留但不再读取，等新缓存稳定后再单独清理，避免破坏性迁移。
- 失败计数只更新当前键；`clock_profile_mismatch` 立即使当前键失效，不影响同场比赛的另一种布局。

### 3.4 当前事件自救流程

```text
解析 layout_mode
  -> 查找对应布局/分辨率的自动 ROI 缓存
  -> 有缓存：使用缓存执行 OCR
       -> 成功：增加当前缓存成功计数
       -> clock_profile_mismatch：
            1. 保存原始失败诊断
            2. 持久化 roi_rediscovery_attempt_count = 1
            3. 将 scoreboard_profile 置空
            4. 在当前事件的同一视频窗口上重试一次
            5. 成功则保存新 ROI 并继续精确定位
            6. 失败则记录第二次失败，进入现有降级链路
  -> 无缓存：按现有方式自动发现 ROI，成功后写入对应缓存
```

实施时的关键约束：

- 只有 `_job_with_cached_scoreboard_profile()` 实际注入的自动缓存才可触发重定位；以此区分自动 profile 和人工 profile。
- 重试前必须先将尝试次数写入 `ocr_window` 任务的 `progressive_scan` 状态，不能只保存在内存里。
- 重试复用当前的切片租约和候选窗口，不重新触发默认 GIF，不改写事件时间。
- 初次失配诊断和重定位诊断必须分开保存，不得用最终结果覆盖第一次失败原因。
- 第一版只对 `clock_profile_mismatch` 开启当前事件重定位。`ocr_clock_unreadable` 和 `scoreboard_missing` 可能是无记分牌或源画面问题，不盲目增加第二次昂贵 OCR；待诊断样本足够后再评估扩展。

### 3.5 诊断与 Dashboard 呈现

每个 OCR 任务应保存以下字段：

- `scoreboard_layout_mode`：`normal` 或 `stoppage`。
- `scoreboard_cache_resolution_key`：本次命中或写入的分辨率键。
- `scoreboard_cache_status`：`miss`、`reused`、`discovered`、`invalidated`、`replaced`。
- `scoreboard_cached_profile_id`：实际使用的缓存 profile ID。
- `roi_rediscovery_attempt_count`：当前事件已尝试次数，最大为 1。
- `roi_rediscovery_trigger`：首次 OCR 失配的 kind 和 message。
- `roi_rediscovery_status`：`not_needed`、`running`、`succeeded`、`failed`、`skipped_explicit_profile`。
- `roi_rediscovery_diagnostics`：第二次 OCR 的独立诊断。

Dashboard 对应展示为：

- 成功：“缓存的记分牌布局已失效，本事件自动重定位成功”。
- 失败：“缓存布局失配，已在当前事件重新查找，但仍未找到可用时钟”，并继续展示后续降级结果。
- 人工 profile 失配：“人工配置的记分牌区域与当前画面不匹配，未自动覆盖配置”。

### 3.6 实施顺序

1. 新增 `layout_mode` 解析函数和单元测试，不改变现有运行分支。
2. 先实施当前事件一次性重定位及持久化次数，解决“要等下一个事件才自愈”的核心问题。
3. 新增 v2 缓存表和按键读写 API，保留旧表，用存储层测试验证隔离。
4. 将 OCR 主链路切换到 v2 缓存，将自动发现结果写入对应布局和分辨率。
5. 增加诊断字段和 Dashboard 文案，确保可区分“首次失配”、“重定位成功”和“两次均失败”。
6. 使用保存的普通时间、`45+N`、`90+N` 真实视频做回放对比，通过后再在生产 AI 精剪路径启用。
7. 稳定运行一段时间后，根据命中率、重定位成功率和额外 OCR 耗时，再决定是否清理旧缓存表或扩展重试错误类型。

### 3.7 必须覆盖的测试

- `normal`、`45+2`、`90+5` 及异常 `minute_extra` 的布局分类。
- 同一 `match_id` 的普通/补时缓存可同时存在，成功和失败计数互不影响。
- 同一布局不同分辨率的缓存不互相覆盖。
- 旧表存在时数据库可正常启动，且旧 profile 不会被错误归类为 `normal`。
- 自动缓存首次 `clock_profile_mismatch` 后，当前事件确实以 `scoreboard_profile=None` 重试一次。
- 重定位成功后正常输出 OCR GIF，并只更新当前布局/分辨率缓存。
- 重定位再次失败时不第三次重试，两次诊断均被保留，并进入现有降级路径。
- 进程在重定位过程中重启后，持久化次数阻止无限重试。
- 人工 profile 触发失配时不进行自动重定位，配置内容不被覆盖。
- 默认 GIF 在 OCR 重定位成功、失败或超时的所有场景中都可以独立生成。
- 现有 OCR/T-DEED/分钟级 GIF/失败语义的回归测试全部通过。

### 3.8 验收标准和预期效果

- 首次进入补时布局时，即使误用了普通时间 ROI，当前事件也能立即重新查找 ROI，不再必须等下一个事件。
- 同场比赛后续的普通时间和补时事件各自复用对应 ROI，降低重复自动搜索和布局串用。
- 不承诺所有记分牌都必然识别成功；无记分牌、画面遮挡、源切换、OCR 模型无法读数等问题仍可能失败，但失败阶段和两次尝试原因必须明确可见。
- 对首次失配的事件会增加一次 OCR 计算和处理时间；通过“仅自动缓存失配”和“每事件最多一次”控制资源上限。
- 默认 GIF 时延和成功率不应因此变化；若回放或生产监控发现默认 GIF 任务受 OCR 线程/进程资源竞争影响，本功能不得直接全量开启。
