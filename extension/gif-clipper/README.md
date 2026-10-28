# GIF 工作台 · 直播页截取插件（v0.1 骨架）

在任意直播页面上录最近几分钟画面，设起点 / 结束生成 GIF，发到 8901 工作台的侧边栏里填表发布。
没有站点列表：只要页面里有正在播放的 `<video>` 就能用。

## 安装（开发者模式）

1. `chrome://extensions` → 打开「开发者模式」→「加载已解压的扩展程序」→ 选 `gif-clipper` 文件夹。
2. 点扩展「详情」→「扩展程序选项」，确认 8901 地址（生产默认即可；本地开发填 `http://127.0.0.1:8902/`）。
3. 先在浏览器里登录过一次 8901（飞书登录）。

## 使用

1. 打开直播页，视频播起来后左下角出现小条，显示「缓冲 Ns」。
2. 小条里选这个标签页在看的比赛（换站、刷新都记得住）。
3. 看到要截的画面：**Alt+1** 设起点 → 等到终点 **Alt+2** 结束。右上角 toast 会提示进度。
4. 侧边栏自动弹出（弹不出就点工具栏图标），里面是 8901 的发布卡：预览、填类型/时间/比分/标题、发布。

小条被广告盖住或全屏时不用管它，快捷键照常工作。**Alt+0** 显示 / 隐藏小条。

裁剪框按比赛记住：在回看（**Alt+3**）里按 C 框一次，之后这场比赛的回看和 Alt+1/Alt+2 截图都按这个框出图（刷新、重开浏览器都在）。小条上的「裁剪 …」按钮点一下改为整幅，再点恢复；画面比例变了会提示，并暂按整幅出图，直到在回看里重新确认。

## 服务端前提

8901 的 `.env` 里 `GIF_MONITOR_EXTENSION_ENABLED=1`，否则上传返回 404。

## 结构

```
manifest.json       MV3；content_scripts all_frames；commands Alt+1/2/0；side_panel
background.js       唯一和 8901 通信的地方（带 Cookie）；按 tab 记比赛；选主录制 frame；上传中转
content/toast.js    每个 frame 的右上角提示
content/recorder.js 30 s 分段滚动缓冲；selectSegments 纯函数
content/finder.js   找最大在播 video、上报、被选中后录制、处理设起点/结束、分块上传
content/bar.js      顶层 frame 的小条（Shadow DOM，可拖）
panel.html/.js      侧栏：iframe 指向 8901 `/?mode=panel&match_id=…`，转发 postMessage
options.html/.js    8901 地址
```

## 已知未做（见实施清单）

- tabCapture 兜底（跨域直链无 CORS 的站画面读不出）——等 P0.5 数据。
- 重新裁剪按钮（服务端 recrop 接口已有）。
- 延迟校准 `source_offset_seconds`。
