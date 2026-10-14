# 编辑文章（admin\-archive\-createarticle）

# 创建/编辑文章（admin\-archive\-createarticle）

> 该接口位于 web\-admin 服务，用于外部系统创建懂球帝文章。**不改动原后台发布逻辑**，内部构造请求并复用后台「创建文章」流程完成发布。
> 
> 

## 基本信息

|项|值|
|---|---|
|**接口名（api\_name）**|`admin-archive-createarticle`|
|**请求方法**|`POST`|
|**Content\-Type**|`application/x-www-form-urlencoded`|
|**need\_login**|`2`（飞书登录）|
|**接口说明**|创建并发布一篇文章，返回文章 id。发布者默认归属操作运营账号（可用 user\_id 指定）。|

---

## 文章类型总览

文章类型由 `type` \+ `style` 组合决定，不同类型有各自的专有参数。

|类型|`type`|`style`|说明 / 专有参数|
|---|---|---|---|
|普通文章|`article`|`default`|常规图文，无专有参数|
|微头条|`feed`|`mini_top`|短内容，无封面强制|
|懂球号|`feed`|`default`|懂球号发文|
|跳转文章|`article`|`default`|专有：`redirect_url`|
|gif 合集|`article`|`gif`|专有：`match_id` `match_time` `add_to_tab` `match_event` `match_score`|

---

## 一、通用参数（所有类型）

### 必填

|字段|类型|必填|说明|示例|
|---|---|---|---|---|
|`title`|string|是|文章标题|`梅西读秒绝杀`|
|`body`|string|是|正文（HTML）|`<p>正文内容</p>`|
|`archive_level`|string|是|文章分级，可选值 S / A / B / C（字母，非数字）|A|

### 可选

|字段|类型|必填|说明|示例|
|---|---|---|---|---|
|archive\_id|int|否|文章 id，传则走编辑，不传走新建|3801234|
|`litpic`|string|条件|封面图 path。**普通文章正文无  时必填**；微头条 / gif 不强制|`/fastdfs7/M00/...jpg`|
|`status`|int|否|`1` 发布（默认）/ `0` 草稿|`1`|
|`user_id`|int|否|发布账号（来自「查询发布账号」）；不传归属操作账号|`13421038`|
|`user_name`|string|否|发布账号名|`足球实战技巧`|
|`writer`|string|否|作者名|`小编`|
|`channels`|string|否|主标签 id，逗号分隔（来自「关联标签搜索」value）|`14572,2310`|
|`channelsnew`|string|否|副标签 id，逗号分隔（用法同 channels）|`12078558`|
|`tabs`|array|否|栏目 id（来自「文章创建选项」allTabs）。**数组格式**，用 `tabs[]=1&tabs[]=284` 传|`tabs[]=1&tabs[]=284`|
|`classifications`|array|否|专栏 id（来自「文章创建选项」online\_classifications）。**数组格式**，用 `classifications[]=48` 传|`classifications[]=48`|
|`archivetopic`|string|否|关联话题（来自「话题搜索」）|`#世界杯`|
|`published_at`|string|否|发布时间（定时发布，`Y-m-d H:i:s`）|`2026-07-22 10:00:00`|

> 其它可选：`display_time`（展示时间）、`offline_at`（下线时间）、`sort_time`（排序时间）、`editorWord`（编辑按语）、`slide`（幻灯）、`ignore`（忽略项）。

## 二、跳转文章专有参数

|字段|类型|必填|说明|示例|
|---|---|---|---|---|
|`redirect_url`|string|是|跳转目标，需传**合成好的完整格式**（见下表）|`game/50001234`|

`redirect_url` 按跳转目标拼接：

|跳转目标|redirect\_url 格式|
|---|---|
|比赛|`game/{比赛id}`，如 `game/50001234`|
|贴子|`article/{贴子id}`，如 `article/3801234`|
|话题|`dongqiudi://v3/topic/topicDetail?param={"topicName":"话题名"}`|
|圈子|`circle/{圈子id}`，如 `circle/789`|
|其他|直接填完整链接，如 `https://...`|

## 三、gif 合集专有参数

需配合 `style=gif`；`match_id` 必填（否则报「请选择关联比赛」），其余 `match_time`/`add_to_tab`/`match_event`/`match_score` 为可选。

|字段|类型|必填|说明|示例|
|---|---|---|---|---|
|`match_id`|string|是|关联比赛 id（来自「文章创建选项」matches）。**style=gif 时必填，不传会报「请选择关联比赛」**|`50001234`|
|`match_time`|string|否|比赛时间（进球时刻）|`19` 或 `90+3`|
|`add_to_tab`|int|否|上新闻列表：`1` 是 / `0` 否|`1`|
|`match_event`|int|否|事件类型：0 无 / 1 进球 / 2 点球 / 3 点球不进 / 4 红牌 / 5 乌龙球 / 6 进球无效|`1`|
|`match_score`|string|否|比分（主队在前）|`3-1`|

> 说明：为避免外部越权，审核 / 运营编排 / 商业化类字段（如 audit\_cmmt、is\_brand、is\_business\_ad、top、is\_selected 等）不接收，传了也会被忽略。

---

## 响应参数

统一响应结构：

```JSON
{
  "code": 0,
  "message": "ok",
  "data": { "archive_id": 3801234 }
}






```

|字段|类型|说明|
|---|---|---|
|`code`|int|业务状态码，`0` 表示成功|
|`message`|string|提示信息|
|`data.archive_id`|int|创建成功的文章 id|

### 错误码返回

|code|message|触发条件|
|---|---|---|
|`1`|xxx 不能为空|缺 title / body / archive\_level / litpic（普通文章正文无图）|
|`3`|（原发布逻辑错误信息）|发布流程校验不通过|
|`5`|服务异常|内部异常|

---

## curl 请求示例

**普通文章**

```Bash
curl -X POST 'http://admin.prod.dongqiudi.lan/admin/archive/openapi/createArticle' \
  -d 'title=梅西读秒绝杀' \
  -d 'body=<p>正文内容</p>' \
  -d 'archive_level=A' \
  -d 'litpic=/fastdfs7/M00/xx.jpg' \
  -d 'channels=14572' \
  -d 'tabs[]=1' \
  -d 'classifications[]=48'

```

```SQL
curl -X POST 'http://admin.prod.dongqiudi.lan/admin/archive/openapi/createArticle' -d 'title=接口测试-梅西读秒绝杀' -d 'body=<p>这是一篇开放平台创建文章接口的测试正文。</p><p><img src="https://img1.qunliao.info/fastdfs8/M00/57/64/rBXRDGo_sGaAHXo2AAAdlZbpWg8520.jpg" /></p>' -d 'archive_level=A' -d 'channels=14572' -d 'tabs[]=1' -d 'classifications[]=48' -d 'user_id=17404847' -d 'status=0'

```

**跳转文章（跳比赛）**

```Bash
curl -X POST 'http://admin.prod.dongqiudi.lan/admin/archive/openapi/createArticle' \
  -d 'title=点击查看比赛' \
  -d 'body=<p>详情见比赛页</p>' \
  -d 'archive_level=A' \
  -d 'redirect_url=article/35695144'






```

**gif 合集（关联比赛进球）**

```Bash
curl -X POST 'http://admin.prod.dongqiudi.lan/admin/archive/openapi/createArticle' \
  -d 'title=梅西进球集锦' \
  -d 'body=<p><img src="..."></p>' \
  -d 'archive_level=A' \
  -d 'style=gif' \
  -d 'match_id=50001234' \
  -d 'match_time=19' \
  -d 'add_to_tab=1' \
  -d 'match_event=1' \
  -d 'match_score=3-1'

```

正式调用走开放平台统一入口：

```Bash
POST https://platform.dongqiudi.com/open/v1/do?api_name=admin-archive-createarticle






```

---

## 注意事项

1. **不改动原接口**：内部复用后台「创建文章」发布逻辑，与后台页面发布行为一致。

2. **type / style 要按类型传对组合**（见「文章类型总览」）。

3. **跳转文章的 redirect\_url 需传合成好的完整格式**（`game/id`、`article/id` 等），不是原始 id。

4. **gif 合集**：填 `match_id` 才会处理 `match_time`/`add_to_tab`/`match_event`/`match_score`。

5. **参数配套获取**：`channels` 用「关联标签搜索」，`tabs`/`classifications`/`match_id` 用「文章创建选项」，`user_id` 用「查询发布账号」，`archivetopic` 用「话题搜索」。

6. **越权保护**：审核 / 运营编排 / 商业化字段一律不接收。

