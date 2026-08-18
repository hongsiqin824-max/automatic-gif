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
