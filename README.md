# XferRust

一个用 Rust 编写的高性能、低资源占用的独立下载引擎，支持 **HTTP(S)** 与
**BitTorrent（BT）** 下载，自带命令行界面，也可作为后台服务供网页端、桌面应用等
程序集成。

## 它能做什么

- **下载 HTTP / HTTPS 文件**，支持断点续传——暂停后继续，已下载的部分不会浪费。
- **下载 BT 种子与磁力链接**，多 peer 并行加速，rarest-first 选片。
- **双轨智能调度**：HTTP 预分配连接数（`split`）与 BT 连接数（`bt-max-peers`）相互独立，
  各自按吞吐边际收益自适应增减连接——有收益就扩张，停滞就换血淘汰慢节点。
- **完成后自动校验**文件完整性（sha-1 / sha-256 / sha-512 / md5）。
- **可视化命令行界面**，实时查看进度、速度、剩余时间与速度走势。
- **可常驻后台**作为守护进程，供其他程序通过 RPC 远程控制。
- **资源友好**：内存与 CPU 占用低，冷启动快。

## 安装

### 预编译产物（推荐）

每个平台发布两个独立压缩包，**按需下载一个即可**：

| 平台 | TUI 版（交互界面 + 下载） | 引擎内核版（无 TUI，后台服务 / 嵌入集成） |
|---|---|---|
| Linux x86_64 | `xfer-tui-linux-x86_64.tar.gz` | `xferrust-linux-x86_64.tar.gz` |
| Windows x86_64 | `xfer-tui-windows-x86_64.tar.gz` | `xferrust-windows-x86_64.tar.gz` |
| macOS 通用（Intel + Apple Silicon） | `xfer-tui-darwin-universal.tar.gz` | `xferrust-darwin-universal.tar.gz` |

从 GitHub [Releases](https://github.com/MochengCK/XferRust/releases) 页面下载。

### 从源码构建

需要 [Rust 工具链](https://rustup.rs/)：

```bash
cargo build --release
# 产物：
#   target/release/xfer      命令行界面（TUI 版）
#   target/release/xferrust  引擎守护进程（无 TUI，供应用集成）
```

查看版本：`xfer --version`

## 快速开始

**直接下载一个文件：**

```bash
xfer download https://example.com/bigfile.zip -d ~/Downloads -o bigfile.zip
```

**打开可视化主界面：**

```bash
xfer
```

**后台服务 + 远程控制：**

```bash
xfer daemon --rpc-secret=mytoken --dir=~/Downloads &
xfer add https://example.com/a.zip --token mytoken
```

---

## 命令行使用指南

`xfer` 覆盖三类使用方式：**可视化主界面**、**单任务下载**、**守护进程 + 远程子命令**。

### 1. 可视化主界面（推荐入口）

```bash
xfer                    # 等价于 xfer tui
xfer tui [-d 目录] [-j 并发数]
```

| 参数 | 默认值 | 说明 |
|---|---|---|
| `-d, --dir` | 当前目录 | 下载保存目录 |
| `-j, --max-concurrent` | `3` | 最大并发下载数 |

主界面内嵌引擎（无需守护进程），实时刷新。任务与设置持久化在会话文件
`~/.xfer/session.json`（每 30 秒自动保存，退出时保存），重启后自动恢复
历史任务；`-d` / `-j` 未显式指定时沿用会话中保存的设置。

**列表视图**

```
┌ XferRust v0.2.0 ──────────────────────────────┐
│ 总速度 9.6 MiB/s   活动 2  等待 1  停止 1      │
├─ 任务（4）────────────────────────────────────┤
│ active   ████████░░░░ 66.7% ubuntu.iso  ...   │ ← 选中行高亮
│ waiting  ░░░░░░░░░░░░  0.0% backup.tar.zst …  │
├───────────────────────────────────────────────┤
│ （操作反馈消息，显示 2 秒）                    │
└───────────────────────────────────────────────┘
 a 添加 · Enter 详情 · ↑↓ 选择 · r 暂停/恢复 · x 移除 · c 清除完成 · s 设置 · q 退出
```

| 按键 | 动作 |
|---|---|
| `a` | 弹出输入框，粘贴 URL 后 `Enter` 添加任务，`ESC` 取消 |
| `↑` `↓` 或 `k` `j` | 上下选择任务 |
| `Enter` | 进入任务详情（进度仪表 + 速度走势图） |
| `r` | 暂停 / 恢复选中任务（切换） |
| `x` | 移除选中任务（确认后删除，可勾选同时删除已下载文件） |
| `c` | 清除全部已完成记录 |
| `s` | 打开设置页 |
| `Tab` | 任务列表 / 侧栏分类焦点切换 |
| `1` ~ `5` | 快捷切换分类筛选 |
| `q` / `Ctrl-C` | 退出 |

**详情视图**：Gauge 进度仪表（百分比、已下载/总大小、速度、剩余时间、平均速度）
+ 最近 60 秒速度 Sparkline 走势。`ESC` / `Enter` 返回列表，`r` / `x` 同列表；
`Tab` 在 tracker / peer 表格间切换焦点，方向键与 `PgUp` / `PgDn` 滚动，
`t` 为 BT 任务追加 tracker。

**设置页**（`s` 键）：可调最大并发数、HTTP 分片连接数（`split`）、
每服务器最大连接数、`bt-max-peers`、`min-split-size`、下载/上传限速、
默认下载目录，管理全局 tracker 列表与 tracker 订阅源，以及界面语言
（简体 / 繁體 / English，可持久化到会话）。

> 界面语言也可在启动时通过环境变量指定：`XFER_LANG=zh|en|zh_tw xfer`。

### 2. 单任务下载

```bash
xfer download <url> [-d 目录] [-o 文件名] [--checksum 算法=摘要]
xfer <url> ...          # URL 直接开头等价于 download
```

| 参数 | 说明 |
|---|---|
| `-d, --dir <目录>` | 保存目录，默认当前目录 |
| `-o, --out <文件名>` | 指定输出文件名（优先级：out > Content-Disposition > URL） |
| `--checksum 算法=摘要` | 完成后校验，支持 `sha-1` / `sha-256` / `sha-512` / `md5` |

示例：

```bash
xfer download https://example.com/bigfile.zip -d ~/Downloads -o bigfile.zip
xfer download https://example.com/iso --checksum sha-256=ab34…
```

行为：全屏 TUI 显示实时进度；`q` / `ESC` / `Ctrl-C` 取消（退出码 130）；
完成退出码 0 并打印文件路径，失败退出码 1 并打印错误码与信息。

### 3. 守护进程

```bash
xfer daemon [--rpc-listen-port=端口] [--rpc-secret=密钥]
            [--dir=目录] [--max-concurrent-downloads=N]
```

> `--log` / `--log-level` 仅 `xferrust` 生效（见下表），`xfer daemon` 接受但暂不启用。

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `--rpc-listen-port` | `6800` | RPC 监听端口（127.0.0.1） |
| `--rpc-secret` | 无 | 鉴权密钥；未设置免鉴权 |
| `--dir` | `.` | 默认下载目录 |
| `--max-concurrent-downloads` | `5` | 最大并发 |
| `--log` / `--log-level` | 无 | 文件日志 / 级别（error/warn/notice/info/debug）；仅 `xferrust` 生效——日志按大小轮转（单文件 10 MB、保留 5 份） |

`xferrust` 二进制参数与 `xfer daemon` 相同，供与应用打包集成。
未知 `--k=v` 选项会被忽略并告警。

守护进程与 TUI 共用会话文件 `~/.xfer/session.json`：启动时恢复历史任务与
设置（仅显式传入 `--dir` / `--max-concurrent-downloads` 才覆盖会话设置），
运行期间自动保存，退出时写入。

### 4. 远程子命令（操作运行中的守护进程）

公共参数：`--connect <ws地址>`（默认 `ws://127.0.0.1:6800/jsonrpc`）、
`--token <密钥>`（对应 daemon 的 `--rpc-secret`）。

```bash
# 添加任务，返回 gid
xfer add <url> [-d 目录] [-o 文件名] [--checksum 算法=摘要] [--token 密钥]

# 添加 BT 任务（.torrent 文件）
xfer add <file.torrent> [-d 目录] [--token 密钥]

# 添加磁力链接任务（引擎经 ut_metadata 自动获取元数据后下载）
xfer add "magnet:?xt=urn:btih:<40-hex>&dn=名称&tr=http://tracker/announce" [--token 密钥]

# 任务详情（JSON）
xfer tell <gid>

# 任务列表（--scope all|active|waiting|stopped，默认 all）
xfer list [--scope active]

# 任务操作
xfer pause <gid>
xfer resume <gid>
xfer remove <gid>

# 全局统计（总速度 + 各状态任务计数）
xfer stat
```

完整会话示例：

```bash
$ xfer daemon --rpc-secret=tok --dir=~/Downloads &
RPC 监听于 http://127.0.0.1:6800/jsonrpc（Ctrl-C 退出）

$ xfer add https://example.com/big.zip --token tok
9f3ba2c4d81e0755

$ xfer list --token tok
GID              状态       进度        大小      速度  文件
9f3ba2c4d81e0755 active     42.3%   2.2 GiB  8.4 MiB/s  big.zip

$ xfer pause 9f3ba2c4d81e0755 --token tok
OK

$ xfer stat --token tok
下载速度 0 B/s · 活动 0 · 等待 0 · 停止 0（累计 0）
```

### 5. 退出码约定

| 退出码 | 含义 |
|---|---|
| `0` | 成功（下载完成 / 正常退出） |
| `1` | 任务失败（网络错误、校验不符等）或 RPC 调用失败 |
| `2` | 用法错误（缺少参数、未知子命令） |
| `130` | 用户取消（q / ESC / Ctrl-C） |

---

## 外部客户端集成指南

面向需要在应用中集成 XferRust 下载能力的客户端开发者，覆盖 RPC 连接、鉴权、
方法调用、事件订阅与前端兼容协议。

### 1. 部署形态

引擎以守护进程形式运行，客户端通过本地 RPC 控制：

```bash
# 启动守护进程（默认 127.0.0.1:6800，仅本机可访问）
xfer daemon --rpc-listen-port=6800 --rpc-secret=mytoken --dir=~/Downloads

# 或使用与应用打包的守护进程二进制（参数相同，另支持 --log/--log-level）
xferrust --rpc-listen-port=6800 --rpc-secret=mytoken
```

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `--rpc-listen-port` | `6800` | RPC 监听端口（绑定 127.0.0.1） |
| `--rpc-secret` | 无 | RPC 鉴权密钥；未设置则免鉴权 |
| `--dir` | `.` | 默认下载目录 |
| `--max-concurrent-downloads` | `5` | 最大并发下载数 |
| `--log` / `--log-level` | 无 | 文件日志与级别（error/warn/notice/info/debug）；仅 `xferrust` 生效（按大小轮转，单文件 10 MB、保留 5 份） |

### 2. 连接与协议

**端点**：`POST /jsonrpc`（单发请求）与 `WS /jsonrpc`（长连接复用）共用同一地址，
默认 `http://127.0.0.1:6800/jsonrpc`。

**帧格式**：JSON-RPC 2.0，支持 batch（数组请求 → 数组响应，仅回带 `id` 的条目）。

```json
// 请求
{"jsonrpc": "2.0", "id": 1, "method": "engine.getVersion", "params": {"token": "mytoken"}}
// 成功响应
{"jsonrpc": "2.0", "id": 1, "result": {"name": "XferRust", "version": "0.2.0", "features": ["http", "resume", "checksum", "bt", "events"]}}
// 错误响应
{"jsonrpc": "2.0", "id": 1, "error": {"code": 1, "message": "Unauthorized"}}
```

**双协议族自动识别**：连接首条请求命中 `task.*` / `engine.*` / `events.*` 即为
原生协议；命中 `aria2.*`、无前缀旧名或 `system.*` 即为前端兼容协议。同一连接
固定为一个协议族，事件帧按族过滤。

### 3. 鉴权

- **原生协议**：参数对象中的 `"token"` 字段，值须与 `--rpc-secret` 严格相等。
- **前端兼容协议**：位置参数首元素 `"token:<secret>"`（带 `token:` 前缀），
  服务端剥离后转发业务逻辑。
- 未配置 secret 时跳过鉴权；鉴权失败返回
  `{"error": {"code": 1, "message": "Unauthorized"}}`。

### 4. 原生协议方法

数值字段均为真实 JSON 数值类型。

#### 4.1 任务管理

| 方法 | 参数 | 返回 |
|---|---|---|
| `task.add` | `uris`(数组,必填), `dir`, `out`, `checksum`, `position`(可选,≥0 插入队列)；或 `torrent`(base64)；或 `magnet`(磁力链接) | `{"gid": "<16-hex>"}` |
| `task.tell` | `gid`, `keys`(数组,可选) | 任务状态对象 |
| `task.list` | `scope`("active"/"waiting"/"stopped"/"all",默认"all"), `offset`, `num`(-1=全部), `keys` | 任务状态对象数组 |
| `task.pause` | `gid` | `{"ok": true}` |
| `task.resume` | `gid` | `{"ok": true}` |
| `task.remove` | `gid` | `{"ok": true}` |
| `task.purgeResults` | — | `{"ok": true}` |
| `task.removeResult` | `gid`（须终态） | `{"ok": true}` |
| `task.getFiles` | `gid` | 文件列表 |
| `task.getUris` | `gid` | URI 列表 |
| `task.getPeers` | `gid` | peer 列表（BT） |
| `task.getTrackers` | `gid` | tracker 列表（BT） |
| `task.addTrackers` | `gid`, `trackers`(数组,非空) | `{"ok": true}` |
| `task.getOption` | `gid` | 选项对象（全局打底 + 任务级覆盖） |
| `task.changeOption` | `gid`, 键值对 | `{"ok": true}` |

添加任务示例：

```json
{"jsonrpc": "2.0", "id": 1, "method": "task.add",
 "params": {"token": "mytoken", "uris": ["https://example.com/big.zip"],
            "dir": "/Downloads", "out": "big.zip",
            "checksum": "sha-256=<hex>"}}
```

`checksum` 支持算法：`sha-1` / `sha-256` / `sha-512` / `md5`。

#### 4.2 引擎管理

| 方法 | 参数 | 返回 |
|---|---|---|
| `engine.getVersion` | — | `{"name", "version", "features"}` |
| `engine.globalStat` | — | `{"downloadSpeed", "uploadSpeed", "numActive", "numWaiting", "numStopped", "numStoppedTotal"}` |
| `engine.getOptions` | — | 全局选项对象 |
| `engine.changeOptions` | 键值对（生效项：`max-concurrent-downloads`、`dir`、`split`、`max-connection-per-server`、`min-split-size`、`bt-max-peers`、`bt-adaptive`、`max-overall-download-limit`、`max-overall-upload-limit`、`bt-encryption`、`bt-protocol`；其余忽略） | `{"ok": true}` |
| `engine.saveSession` | — | `{"ok": true}` |
| `engine.shutdown` / `engine.forceShutdown` | — | `{"ok": true}` |

全局 tracker 与订阅源：

| 方法 | 参数 | 返回 |
|---|---|---|
| `engine.getTrackers` | — | `{"trackers": [URL, ...]}` |
| `engine.addTracker` | `tracker`(URL) | `{"ok": true}` |
| `engine.removeTracker` | `tracker`(URL) | `{"ok": true}` |
| `engine.getSubscriptions` | — | 订阅源列表 |
| `engine.addSubscription` | `name`, `url`, `enabled`(可选,默认 true) | 订阅源对象 |
| `engine.removeSubscription` | `id` | `{"ok": true}` |
| `engine.toggleSubscription` | `id` | `{"ok": true}` |
| `engine.refreshSubscription` | `id` | `{"count": n}`（拉取条数） |
| `engine.refreshAllSubscriptions` | — | `{"count": n}` |
| `engine.getAutoUpdateTrackers` | — | `{"enabled": bool}` |
| `engine.setAutoUpdateTrackers` | `enabled`(bool) | `{"ok": true}` |

订阅源行为：

- **添加/启用即自动拉取**：`addSubscription`、`toggleSubscription`（重新启用）会
  立即在后台拉取一次并同步到全局 tracker 列表，所有客户端（TUI / RPC）行为一致，
  无需调用方手动补刷新。
- **同步语义（非只增不减）**：订阅源刷新时，远程新增的 tracker 加入全局列表；
  远程已移除且该订阅源曾贡献的 tracker 从全局列表剔除。手动添加的
  (`engine.addTracker`) 与其他订阅源仍提供的 tracker 不受影响。
- **每日自动更新**：后台每小时检查一次，距上次成功更新 ≥24h 的订阅源才会刷新
  （`autoUpdateTrackers` 开启时）。手动 `refreshSubscription` /
  `refreshAllSubscriptions` 不受 TTL 限制，立即全量刷新。
- **安全阀**：远程返回空列表视为异常，保留现有 tracker 并记录错误，避免误清空。

#### 4.3 任务状态对象

```
gid, status, totalLength, completedLength, uploadLength, downloadSpeed,
uploadSpeed, bitfield, connections, errorCode, errorMessage, elapsedMs, dir,
files[{index, path, length, completedLength, selected, uris[{uri, status}]}],
numSeeders, seeder, numPieces, pieceLength
```

- `status`：`waiting` / `active` / `paused` / `complete` / `error` / `removed`
- `errorCode`：`0` 无错，`2` 超时，`3` 资源不存在，`5` 网络问题，`9` 校验不符，`1` 其他

### 5. 事件订阅

原生协议客户端在 WebSocket 连接上发送一次 `events.subscribe`，之后服务端持续推送：

| 引擎事件 | 推送方法 | params |
|---|---|---|
| 任务开始 | `task.start` | `{"gid"}` |
| 已暂停 | `task.pause` | `{"gid"}` |
| 已停止 | `task.stop` | `{"gid"}` |
| 下载完成 | `task.complete` | `{"gid"}` |
| 出错 | `task.error` | `{"gid", "errorCode", "errorMessage"}` |
| 进度（1Hz） | `task.progress` | `{"gid", "status", "completedLength", "totalLength", "downloadSpeed"}` |

事件帧无 `id` 字段，据此与响应帧区分：

```json
{"jsonrpc": "2.0", "method": "task.progress",
 "params": {"gid": "abc123...", "status": "active",
            "completedLength": 1048576, "totalLength": 10485760, "downloadSpeed": 262144}}
```

推荐模式：`events.subscribe` + `task.progress` 事件驱动 UI 刷新，免轮询；
连接断开后重连并重新订阅。

### 6. 前端兼容协议（aria2 风格）

供既有 aria2 系客户端接入，位置参数，数值字段以字符串承载。

- 业务方法：`aria2.addUri` / `aria2.addTorrent` / `getPeers` / `remove` / `forceRemove` /
  `pause` / `forcePause` / `unpause` / `tellStatus` / `tellActive` / `tellWaiting` /
  `tellStopped` / `getGlobalStat` / `getVersion` / `getFiles` / `getURIs` / `getOption` /
  `changeOption` / `getGlobalOption` / `changeGlobalOption` / `purgeDownloadResult` /
  `removeDownloadResult` / `saveSession` / `shutdown` / `forceShutdown`
- 系统方法：`system.multicall` / `system.listMethods` / `system.listNotifications`
- 事件（无需订阅，识别协议族后自动推送）：`aria2.onDownloadStart` /
  `onDownloadPause` / `onDownloadStop` / `onDownloadComplete` / `onDownloadError` /
  `onBtDownloadComplete`

调用示例：

```json
{"jsonrpc": "2.0", "id": 1, "method": "aria2.addUri",
 "params": ["token:mytoken", ["https://example.com/a.zip"], {"dir": "/Downloads"}]}
```

### 7. Rust 进程内集成

客户端也可不经过 RPC，直接以库形式内嵌引擎（`xfer-engine` 不依赖任何传输层）：

```rust
use serde_json::json;
use xfer_engine::TaskManager;

let mgr = TaskManager::start(std::path::PathBuf::from("/Downloads"), 3);
let gid = mgr.add_uri(
    vec!["https://example.com/big.zip".into()],
    &json!({}),
    None,
)?;
let mut events = mgr.events().subscribe(); // broadcast 事件流
// mgr.tell_status_native / pause / unpause / remove / list_native / global_stat_native
```

### 8. 集成检查清单

1. 启动或连接守护进程，`engine.getVersion` 握手确认版本与能力。
2. 配置 secret 时所有请求携带 token（原生 `params.token` / 兼容 `token:<secret>`）。
3. WebSocket 长连接上先 `events.subscribe`，按 `method` 分发事件帧。
4. 以 `gid` 为主键维护任务表，`task.progress` 更新进度，终态事件（`complete` /
   `error`）收敛任务。
5. 断线重连后重新订阅，并用 `task.list` 全量对账。

---

## License

GPL-3.0-only（GNU General Public License v3.0，仅此版本）。完整许可证文本见 [LICENSE](LICENSE)。
