# 比赛直播源查询接口 — 对接文档（for xuxinan）

> 面向调用方（写脚本对接）。本文档只讲「怎么调、传什么、返回什么、怎么判断成败」，不涉及内部实现。
> 一切以本文档为准。

---

## 1. 接口概览

| 项 | 值 |
|---|---|
| 用途 | 查询一场比赛在 `live_sources` 表中已配置的直播源记录（即 add 接口写入的那条） |
| 方法 | `POST` |
| 路径 | `/inner/tool/match/live_source/query` |
| 域名 | `openapi.dongqiudi.com`（生产）。完整 URL：`https://openapi.dongqiudi.com/internal/sport-data/inner/tool/match/live_source/query` |
| Content-Type | `application/json` |
| 鉴权 | `secret`（放 body）+ `user`（放 query，openapi 网关强制要求），无需额外 header |

> 📌 与 add 接口的区别：add 的参数全部在请求体；本查询接口中 **`user` 必须放在 query 上**（经 `openapi.dongqiudi.com` 网关访问的硬性要求，不放 query 请求进不来）、**`secret` 放 body**、`match_id` 可放 query 或 body（query 优先，见第 3 节）。

---

## 2. 鉴权

鉴权信息 `secret` + `user`，两个字段的传递位置**不同**：

| 字段 | 位置 | 值 |
|---|---|---|
| `secret` | **请求体 body** | `9b4414dc-4de5-e106-3728-f2f4a04d97b4` |
| `user` | **URL query（必须）** | `xuxinan@dongqiudi.com`（会自动去除首尾空格后比对） |

secret 错误 → `errno=1, message="secret错误"`；user 不对 → `errno=1, message="user校验失败"`。

---

## 3. 请求参数

### 3.1 参数列表

| 字段 | 类型 | 必填 | 位置 | 说明 |
|---|---|---|---|---|
| `secret` | string | ✅ | body | 鉴权密钥 |
| `user` | string | ✅ | **query（必须）** | 固定填 `xuxinan@dongqiudi.com` |
| `match_id` | int | ✅ | query 或 body | 比赛 ID（整数，`>0`） |

### 3.2 各字段位置规则 ⭐

三个字段的传递位置不同，务必按此放置：

| 字段 | 位置 | 说明 |
|---|---|---|
| `user` | **query（必须）** | 经 `openapi.dongqiudi.com` 网关访问，**user 必须放在 URL query 上**，否则请求进不来 |
| `secret` | **body** | 放在 JSON 请求体里 |
| `match_id` | query 或 body | **query 优先**：query 里有就用 query 的；query 里没有再回退到 body |

> 📌 只有 `match_id` 是「query 优先、body 兜底」；`user` 必须在 query、`secret` 在 body，两者位置不能放错。

### 3.3 两种推荐调用方式

**方式 A（推荐）：`user` + `match_id` 放 query，`secret` 放 body**

```
POST https://openapi.dongqiudi.com/internal/sport-data/inner/tool/match/live_source/query?user=xuxinan@dongqiudi.com&match_id=123456
Content-Type: application/json

{"secret": "9b4414dc-4de5-e106-3728-f2f4a04d97b4"}
```

**方式 B：`user` 放 query，`secret` + `match_id` 放 body**

```jsonc
// URL: .../query?user=xuxinan@dongqiudi.com
{
  "secret":   "9b4414dc-4de5-e106-3728-f2f4a04d97b4",
  "match_id": 123456
}
```

> 两种方式区别仅在 `match_id` 放哪：query（方式 A）或 body（方式 B）。`user` 永远在 query、`secret` 永远在 body，不能互换。

---

## 4. 返回结构

外层统一为 `{errno, message, data}`。

### 4.1 查到记录（成功）

```json
{
  "errno": 0,
  "message": "success",
  "data": {
    "id": 10234,
    "resource": "rtmp://example.com/live/stream1",
    "cname": "pushurl1",
    "show_app": 3,
    "net_live": 3,
    "created_at": "2026-08-12 14:30:00",
    "updated_at": "2026-08-12 15:00:00"
  }
}
```

`data` 各字段含义：

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | int | `live_sources` 表主键 |
| `resource` | string | 直播源标识（add 接口写入的推流地址等） |
| `cname` | string | 推流通道名，add 写入固定为 `pushurl1` |
| `show_app` | int | App 端展示开关，add 写入固定为 `3` |
| `net_live` | int | 网络直播开关，add 写入固定为 `3` |
| `created_at` | string | 记录创建时间（`YYYY-MM-DD HH:mm:ss`） |
| `updated_at` | string | 记录最后更新时间（同上格式） |

> 📌 查询命中条件：`relate_type="match"` + `relate_id=match_id` + `live_type="ls"`，与 add 的写入条件完全对应——**add 写入哪条，本接口就查哪条**。

### 4.2 无数据（比赛未配置直播源）

比赛 ID 合法，但该比赛尚未通过 add 写入过直播源记录：

```json
{
  "errno": 0,
  "message": "无数据",
  "data": {}
}
```

> 注意：`errno` 仍为 `0`（请求本身处理成功），只是没有匹配记录。脚本判别「有无数据」看 `data` 是否非空对象 / `message` 是否为 `"无数据"`（见第 5 节）。

### 4.3 整请求失败

鉴权/参数问题时，`errno=1`，`message` 是原因，`data` 为空对象：

```json
{
  "errno": 1,
  "message": "match_id不能为空",
  "data": {}
}
```

---

## 5. 判断成败（调用方脚本逻辑）

按以下顺序判断：

1. **先看 `errno`**：
   - `errno == 1` → **请求失败**，未查出任何数据。看 `message` 定位原因（第 6 节），大多需人工处理或修正参数。
   - `errno == 0` → 请求处理成功，继续看下一步。
2. **`errno==0` 时看 `data`**：
   - `data` 非空（含 `id`/`resource` 等字段）→ **查到记录**，按字段使用即可。
   - `data` 为空对象 `{}`（`message` 为 `"无数据"`）→ **该比赛未配置直播源**，不是错误，按业务处理（如提示需先 add）。

> 📌 **判断有没有查到数据，永远以 `data` 内容为准**，不要用 `message` 做唯一判据。

---

## 6. `errno=1` 的 message 一览（失败原因）

| message | 原因 | 处理 |
|---|---|---|
| `secret错误` | secret 不对（body 没给对） | 检查 body 里的 secret |
| `user校验失败` | user 不对，或没把 user 放在 query | 用 `xuxinan@dongqiudi.com`，并确保它在 URL query 上 |
| `match_id不能为空` | query 和 body 都没给 `match_id`，或 `match_id <= 0` | 检查 `match_id` |
| （查询失败错误原文） | 数据库查询时故障（如 `查询失败: ...`） | 间隔几秒重试 1~2 次，仍失败告警交人工 |

> 说明：本接口对「比赛不存在」**不单独报错**。若该 `match_id` 未写入过直播源记录，一律返回 `errno=0, message="无数据"`；若 `match_id` 本身就不合法（`<=0` 或没传），才返回 `errno=1, message="match_id不能为空"`。

---

## 7. 完整示例

### 7.1 方式 A：`user`+`match_id` 在 query，`secret` 在 body

```bash
curl -X POST 'https://openapi.dongqiudi.com/internal/sport-data/inner/tool/match/live_source/query?user=xuxinan@dongqiudi.com&match_id=123456' \
  -H 'Content-Type: application/json' \
  -d '{"secret": "9b4414dc-4de5-e106-3728-f2f4a04d97b4"}'
```

### 7.2 方式 B：`user` 在 query，`secret`+`match_id` 在 body

```bash
curl -X POST 'https://openapi.dongqiudi.com/internal/sport-data/inner/tool/match/live_source/query?user=xuxinan@dongqiudi.com' \
  -H 'Content-Type: application/json' \
  -d '{
    "secret": "9b4414dc-4de5-e106-3728-f2f4a04d97b4",
    "match_id": 123456
  }'
```

### 7.3 可能的返回

**查到记录：**
```json
{
  "errno": 0,
  "message": "success",
  "data": {
    "id": 10234,
    "resource": "rtmp://example.com/live/stream1",
    "cname": "pushurl1",
    "show_app": 3,
    "net_live": 3,
    "created_at": "2026-08-12 14:30:00",
    "updated_at": "2026-08-12 15:00:00"
  }
}
```

**无数据（该比赛没配过直播源）：**
```json
{"errno": 0, "message": "无数据", "data": {}}
```

**鉴权失败：**
```json
{"errno": 1, "message": "user校验失败", "data": {}}
```

**缺少 match_id：**
```json
{"errno": 1, "message": "match_id不能为空", "data": {}}
```

---

## 8. 给写脚本的几条约束（务必遵守）

1. **一次一场比赛**：`match_id` 是单个整数，批量多场请循环分多次调用。不同比赛之间可并发，互不影响。
2. **`user` 必须在 query、`secret` 必须在 body**：这是 openapi 网关的硬性要求，位置放错请求进不来。只有 `match_id` 可放 query 或 body（query 优先）。
3. **判断有无数据看 `data`，不看 `message`**：`errno==0 && data 非空` 才算查到。
4. **`errno=1` 不要无限重试**：除 `查询失败: ...` 这类瞬时故障可间隔重试 1~2 次外，`secret错误`/`user校验失败`/`match_id不能为空` 属参数问题，重试无意义，应修正后重发。
5. **只读查询，无副作用**：本接口不改任何数据，可放心重复调用；对同一 `match_id` 并发查询无风险（与 add 的并发限制不同）。

---

## 9. 已知行为与注意事项

1. **本接口只查 add 写入的那条记录**。命中条件固定为 `relate_type="match"` + `relate_id=match_id` + `live_type="ls"`，与 add 的 upsert 条件一一对应。`live_sources` 表里同一比赛可能存在的其它 `live_type` / `relate_type` 记录**不会被返回**。
2. **返回字段是关键子集，不是整行**。只返回 `id / resource / cname / show_app / net_live / created_at / updated_at` 七个字段，不返回表里的其余列。如需更多字段，联系服务端。
3. **`无数据` 不等于「比赛不存在」**。`match_id` 是否对应真实比赛，本接口不校验——只看 `live_sources` 表里有没有对应记录。要确认比赛本身是否存在，走其它接口。
4. **DB 瞬时故障**：极少数情况下查询时数据库抖动，会返回 `errno=1, message="查询失败: ..."`。间隔几秒重试 1~2 次即可；多次仍失败再告警交人工。
5. **与 add 接口的关系**：add = 写入/覆盖（`relate_type=match` + `live_type=ls` 那条），query = 读出这条。典型用法是「先 add 写入，再 query 核对」或「query 发现无数据，则 add 写入」。

---

## 附：与 add 接口的对比

| 项 | add（写入） | query（查询，本接口） |
|---|---|---|
| 路径 | `POST /inner/tool/match/live_source/add` | `POST /inner/tool/match/live_source/query` |
| 参数来源 | 仅 body | `user`=query（必须）、`secret`=body、`match_id`=query 或 body |
| 必填参数 | `secret` / `user` / `match_id` / `resource` | `secret` / `user` / `match_id` |
| 返回 data | 始终空对象 `{}` | 命中时为记录字段，未命中为 `{}` |
| 副作用 | 写入/覆盖 `live_sources` 一行 | 无（只读） |
| 并发 | 同一 `(match_id)` 需串行 | 可并发，无风险 |
