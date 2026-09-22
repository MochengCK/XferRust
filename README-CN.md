<p align="center">
  <a href="./README.md">English</a> | 简体中文
</p>

# XferRust

一个用 Rust 编写的高性能、低资源占用的独立下载引擎，支持 **HTTP(S)**、
**HLS（M3U8）** 与 **BitTorrent（BT）** 下载。自带全屏终端界面，也可作为
后台服务运行，供网页端、桌面应用等客户端通过 JSON-RPC 远程控制。

## 它能做什么

**协议**

- **HTTP / HTTPS**：多连接分片下载（`split`、`max-connection-per-server`、
  `min-split-size`）、断点续传，并写出控制文件——中断后从实际
  停下的位置继续，而不是从头再来。
- **HLS（M3U8）**：`.m3u8` 地址按播放列表下载——主清单自动选流
  （`hls-variant=worst` 可改取最低码率），分片并发拉取后按清单顺序拼成
  **一个**文件，不需要事后合并。支持 `#EXT-X-MAP`（fMP4 初始化段）、
  `#EXT-X-BYTERANGE` 区间分片与 `#EXT-X-KEY` AES-128
  分片加密（含密钥轮换；`SAMPLE-AES` 会明确报错而不是静默产出坏文件）。
  分片大小会先预探测一遍，总长因此可知，进度与剩余时间可用；断点续传按
  "已 fsync 的连续前缀"续接（`hls-probe-size=false` 可关闭预探测）。
- **BitTorrent**：种子与磁力链接，多 peer 并行 + rarest-first 选片；底层是
  完整网络栈：DHT（BEP 5）、IPv6 DHT（BEP 32）、peer 交换（BEP 11）、
  本地节点发现、uTP 与 MSE/PE 加密。
- **磁力链接按需选文件**：粘贴磁力链接后引擎立即解析元数据，解析完成弹出文件表格
  （勾选 + 文件大小 + 已选汇总），确认后才开始下载——不必为几个文件下载整个种子。
- **做种控制**：可保持做种或完成后停止，并支持分享率与时间双条件
  （`bt-seed-ratio` / `bt-seed-time`）；`task.stopSeed` 可随时停止一个正在
  做种的任务。

**调度与网络**

- **双轨智能调度**：HTTP 预分配连接数（`split`）与 BT 连接数（`bt-max-peers`）
  相互独立，各自按吞吐边际收益自适应增减连接——有收益就扩张，停滞就换血淘汰
  慢节点。
- **逐任务请求**：任务级请求头（`header`）、`referer`、`user-agent` 会被统一
  应用到探测、单连接与分片三条路径，需要签名或 Referer 才能访问的地址因此能
  一路走通；另有全局代理（`all-proxy` 配 `no-proxy` 排除列表）。
- **限速**：全局（`max-overall-download-limit` / `max-overall-upload-limit`）
  与任务级（`max-download-limit` / `max-upload-limit`）两级。
- **对端管理**：对端按四个分组上报——已连接 / 正在连接 / 已断开 / 已封禁；
  封禁、解封支持永久或指定时长，并有 IP 黑名单；UPnP 与 NAT-PMP 端口映射
  打通入向连接。
- **Tracker**：任务级 tracker 列表、全局列表，以及每日自动刷新的订阅源。
  刷新是"同步"而非"只增"，远端返回空列表会被当作异常处理，不会误清你的 tracker。

**存储与校验**

- **分片级存储**：经片级写回缓存（`disk-cache`）的顺序或乱序写入，写入前逐片
  做 SHA-1 校验，续传位图始终不领先于真正落盘的内容。
- **校验**：完成后的自动校验支持 sha-1 / sha-256 / sha-512 / md5；
  `task.verifyFiles` 可对已有任务随时重新校验。
- **瞬态失败重试**：HTTP 请求失败会对同一 URI 重试（最多 3 次），并沿用断点
  续传语义；4xx 响应与本地 I/O 错误不重试。
- **磁力元数据缓存**：可选把解析出的种子元数据写到
  `<下载目录>/<infohash>.torrent`，下次启动直接复用
  （`bt-save-metadata` / `bt-load-saved-metadata`）。

**交互形态**

- **可视化终端界面**：实时进度、速度、剩余时间与速度走势图；也可在 shell 里
  直接下单个任务；还有守护进程 + 远程子命令用于脚本化。
- **同一端点上两套 RPC 协议族**：原生 `task.*` / `engine.*` / `events.*`，以及
  aria2 兼容族（`aria2.*` / `system.*`），按连接自动识别——既有的 aria2
  客户端可以原样接入。
- **事件驱动**：WebSocket 订阅一次，所有状态变更主动推送（进度 1 Hz），
  不用轮询，也不会漂移。
- **可进程内嵌入**：`xfer-engine` 不依赖 RPC 层，宿主应用可以直接驱动任务管理，
  不需要再起一个守护进程。
- **资源友好**：无 GC、无运行时、无隐藏线程，内存与 CPU 占用低、冷启动快。

## 安装

### 预编译产物（推荐）

每个平台按需下载独立压缩包：

| 平台 | TUI 版（交互界面 + 下载） | 引擎内核版（无 TUI，后台服务 / 嵌入集成） |
|---|---|---|
| Linux x86_64 | `xfer-tui-linux-x86_64.tar.gz` | `xferrust-linux-x86_64.tar.gz` |
| Linux arm64 | `xfer-tui-linux-arm64.tar.gz` | `xferrust-linux-arm64.tar.gz`（musl 静态） |
| Windows x86_64 | `xfer-tui-windows-x86_64.tar.gz` | `xferrust-windows-x86_64.tar.gz` |
| macOS（TUI 为 Intel + Apple Silicon 通用） | `xfer-tui-darwin-universal.tar.gz` | `xferrust-darwin-aarch64.tar.gz` / `xferrust-darwin-x86_64.tar.gz`（按架构分开） |
| Android arm64-v8a | — | `xferrust-android-arm64-v8a.tar.gz` |

从 GitHub [Releases](https://github.com/MochengCK/XferRust/releases) 页面下载。

### 从源码构建

需要 [Rust 工具链](https://rustup.rs/)：

```bash
cargo build --release
# 产物：
#   target/release/xfer      命令行界面（TUI 版）
#   target/release/xferrust  引擎守护进程（无 TUI，供应用集成）
```

TUI 由默认的 `tui` feature 提供。只构建引擎（体积更小、不依赖终端——用于嵌入
与 Android）：

```bash
cargo build --release --no-default-features --bin xferrust
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

`xfer` 覆盖三种使用方式：**可视化主界面**、**单任务下载**、**守护进程 + 远程子命令**。

### 1. 可视化主界面（推荐入口）

```bash
xfer                    # 等价于 xfer tui
xfer tui [-d dir] [-j max-concurrent]
```

| 选项 | 默认值 | 说明 |
|---|---|---|
| `-d, --dir` | 当前目录 | 下载保存目录 |
| `-j, --max-concurrent` | `3` | 最大并发下载数 |

主界面内置引擎（无需守护进程），实时刷新。任务与设置持久化在会话文件
`~/.xfer/session.json`（每 30 秒自动保存一次，退出时也会写入），重启后恢复；
未显式传 `-d` / `-j` 时，沿用会话里保存的值。

**列表视图**

```
┌ XferRust v0.3.2 ──────────────────────────────┐
│ ↓ 9.6 MiB/s  ↑ 0 B/s    活动 2  等待 1  已停止 1 │
├─ 任务 (4)─────────────────────────────────────┤
│ 下载中   ████████░░░░ 66.7% ubuntu.iso  ...   │ ← 选中行高亮
│ 等待中   ░░░░░░░░░░░░  0.0% backup.tar.zst …  │
├───────────────────────────────────────────────┤
│ （操作反馈信息，显示 2 秒）                    │
└───────────────────────────────────────────────┘
 a 添加 · Enter 详情 · ↑↓ 选择 · r 暂停/继续 · x 移除 · c 清除已完成
 s 设置 · 1-5/Tab 筛选 · S 停止做种 · q 退出
```

| 按键 | 作用 |
|---|---|
| `a` | 弹出输入框，粘贴 URL 后 `Enter` 添加任务，`ESC` 取消（磁力链接进入解析与文件选择流程） |
| `↑` `↓` 或 `k` `j` | 上下选择任务 |
| `Enter` | 进入任务详情（进度仪表 + 速度走势图） |
| `r` | 暂停 / 恢复选中任务（切换） |
| `x` | 移除选中任务（确认后删除，可勾选同时删除已下载文件） |
| `c` | 清除全部已完成记录 |
| `s` | 打开设置页 |
| `S` | 停止选中 BT 任务的做种 |
| `Tab` | 任务列表 / 侧栏分类焦点切换 |
| `1` ~ `5` | 快捷切换分类筛选 |
| `q` | 退出（会先询问确认：`y` / `q` / `Enter` 确认，`n` / `ESC` 取消） |
| `Ctrl-C` | 立即退出 |

焦点在侧栏时，`↑` `↓` / `k` `j` 用于切换分类，`→` / `l` 返回任务列表。

**磁力链接流程**（`a` 粘贴 `magnet:` 链接）：先显示解析弹窗（种子名、已连接 peer 数、
已用时间）；元数据解析完成后任务自动暂停并弹出文件表——`↑` `↓` / `PgUp` `PgDn` /
`Home` `End` 移动，`Space` 勾选，`a` 全选 / 反选，`Enter` 确认（只下载勾选的文件），
`ESC` 取消并移除任务；底栏实时显示「已选 n/N · 已选大小 / 总大小」。未确认就退出的
任务，下次启动会重新弹出文件表。

**详情视图**：Gauge 进度仪表（百分比、已下载/总大小、速度、剩余时间、平均速度）
+ 速度走势图（保留 120 个采样，约最近 40 秒）。`ESC` / `Enter` 返回列表；`r` / `x` /
`S` 与列表中一致；`Tab` 在 tracker 表与 peer 表之间切换焦点，方向键与
`PgUp` / `PgDn` 滚动，`t` 可为 BT 任务添加 tracker。

**设置页**（`s` 键）——分三个页签，`Tab` 循环切换，`↑` `↓` 移动，`←` `→` 调整
（也可用 `+` / `-`），`a` 切换 / 展开：

- *传输*：最大并发数、HTTP 分片连接数（`split`）、单服务器最大连接数、
  `min-split-size`、`bt-max-peers`、BT 智能调度、全局下载限速、全局上传限速。
- *BitTorrent*：加密模式（`bt-encryption`）、传输协议（`bt-protocol`）、
  BT 监听端口、DHT 监听端口、本地节点发现、端口映射（UPnP / NAT-PMP）、
  完成行为（做种 / 完成即止）、做种分享率、默认保存目录。
- *Tracker 与界面*：全局 tracker 列表、tracker 订阅源、界面语言
  （简体 / 繁体 / English，写入会话持久化）。

> 界面语言也可在启动时用环境变量指定：`XFER_LANG=zh|en|zh_tw xfer`。

### 2. 单任务下载

```bash
xfer download <url> [-d dir] [-o filename] [--checksum algo=digest]
xfer https://example.com/file.zip   # 裸 http(s):// URL 等价于 download
```

| 选项 | 说明 |
|---|---|
| `-d, --dir <目录>` | 保存目录，默认当前目录 |
| `-o, --out <文件名>` | 指定输出文件名（优先级：out > Content-Disposition > URL） |
| `--checksum 算法=摘要` | 完成后校验，支持 `sha-1` / `sha-256` / `sha-512` / `md5` |

示例：

```bash
xfer download https://example.com/bigfile.zip -d ~/Downloads -o bigfile.zip
xfer download https://example.com/iso --checksum sha-256=ab34…
```

行为：全屏 TUI 实时显示进度；`q` / `ESC` / `Ctrl-C` 取消（退出码 130）；成功退出码 0
并打印文件路径；失败退出码 1 并给出错误码与信息。

> 裸参数只有在以 `http://` 或 `https://` 开头时才按下载处理。种子文件与磁力链接
> 请走下面的子命令（`add`，或主界面的输入框）。

### 3. 守护进程

```bash
xfer daemon [--rpc-listen-port=port] [--rpc-secret=secret]
            [--dir=dir] [--max-concurrent-downloads=N]
```

| 选项 | 默认值 | 说明 |
|---|---|---|
| `--rpc-listen-port` | `6800` | RPC 监听端口（127.0.0.1） |
| `--rpc-secret` | 无 | 鉴权密钥；未设置免鉴权 |
| `--dir` | `.` | 默认下载目录 |
| `--max-concurrent-downloads` | `5` | 最大并发 |
| `--log` / `--log-level` | 无 | 文件日志 / 级别（error/warn/notice/info/debug）；仅 `xferrust` 生效——日志按大小轮转（单文件 10 MB、保留 5 份） |
| `--save-session` / `--input-file` | `~/.xfer/session.json` | 会话文件路径（启动时载入、退出时写入） |

`xferrust` 二进制接受与 `xfer daemon` 相同的参数，供与应用打包使用。除上表的核心
选项外，`xferrust` 还接受任意 `--key=value` 作为**外部注入的默认值**：已实现的键
启动即生效，尚未实现的键会被存进全局选项（可用 `engine.getOptions` 读回），
既不会报错也不会有警告——宿主应用可以把自己的整套配置直接透传进来。

> `xfer daemon` 是面向用户的入口，同样能解析这些标志，但不对外暴露上表之外的
> 选项；需要下发完整配置时请用 `xferrust`。

守护进程与 TUI 共用会话文件 `~/.xfer/session.json`：启动时恢复历史任务与设置
（仅显式传入的 `--dir` / `--max-concurrent-downloads` 覆盖会话设置），运行中自动
保存，退出时写入。

### 4. 远程子命令（操作运行中的守护进程）

通用选项：`--connect <ws-url>`（默认 `ws://127.0.0.1:6800/jsonrpc`）与
`--token <secret>`（与守护进程的 `--rpc-secret` 一致）。

```bash
# 添加任务，返回 gid                （别名：dl，也可用 download）
xfer add <url> [-d dir] [-o filename] [--checksum algo=digest] [--token secret]

# 添加 BT 任务（.torrent 文件）
xfer add <file.torrent> [-d dir] [--token secret]

# 添加磁力链接任务（引擎经 ut_metadata 自动获取元数据后下载）
xfer add "magnet:?xt=urn:btih:<40-hex>&dn=name&tr=http://tracker/announce" [--token secret]

# 任务详情（JSON）
xfer tell <gid>

# 任务列表（--scope all|active|waiting|stopped，默认 all）    （别名：ls）
xfer list [--scope active]

# 任务操作                        （remove 的别名：rm）
xfer pause <gid>
xfer resume <gid>
xfer remove <gid>

# 全局统计（总速度 + 各状态任务计数）
xfer stat
```

一次完整会话：

```bash
$ xfer daemon --rpc-secret=tok --dir=~/Downloads &
RPC listening on http://127.0.0.1:6800/jsonrpc (Ctrl-C to exit)

$ xfer add https://example.com/big.zip --token tok
9f3ba2c4d81e0755

$ xfer list --token tok
GID              Status     Prog      Size      Speed      Name
9f3ba2c4d81e0755 active     42.3%   2.2 GiB  8.4 MiB/s  big.zip

$ xfer pause 9f3ba2c4d81e0755 --token tok
OK

$ xfer stat --token tok
Down speed 0 B/s · active 0 · waiting 0 · stopped 0 (total 0)
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

面向把 XferRust 下载能力集成进自家应用的客户端开发者：RPC 连接、鉴权、方法调用、
事件订阅，以及 aria2 兼容的前端协议。

### 1. 部署形态

引擎以守护进程运行，客户端通过本机 RPC 控制：

```bash
# 启动守护进程（默认 127.0.0.1:6800，仅本机可访问）
xfer daemon --rpc-listen-port=6800 --rpc-secret=mytoken --dir=~/Downloads

# 或使用与应用打包的守护进程二进制（参数相同，另支持 --log/--log-level）
xferrust --rpc-listen-port=6800 --rpc-secret=mytoken
```

| 选项 | 默认值 | 说明 |
|---|---|---|
| `--rpc-listen-port` | `6800` | RPC 监听端口（绑定 127.0.0.1） |
| `--rpc-secret` | 无 | RPC 鉴权密钥；未设置则免鉴权 |
| `--dir` | `.` | 默认下载目录 |
| `--max-concurrent-downloads` | `5` | 最大并发下载数 |
| `--log` / `--log-level` | 无 | 文件日志与级别（error/warn/notice/info/debug）；仅 `xferrust` 生效（按大小轮转，单文件 10 MB、保留 5 份） |

其余任意 `--key=value` 由 `xferrust` 作为外部注入的默认选项接收
（见「命令行使用指南 → 守护进程」）。

### 2. 连接与协议

**端点**：`POST /jsonrpc`（单发请求）与 `WS /jsonrpc`（长连接复用）共用同一地址，
默认 `http://127.0.0.1:6800/jsonrpc`。

**帧格式**：JSON-RPC 2.0，支持 batch（数组请求 → 数组响应，仅回带 `id` 的条目）。

```json
// 请求
{"jsonrpc": "2.0", "id": 1, "method": "engine.getVersion", "params": {"token": "mytoken"}}
// 成功响应
{"jsonrpc": "2.0", "id": 1, "result": {"name": "XferRust", "version": "0.3.2",
 "features": ["http", "resume", "checksum", "bt", "events", "bitfield",
              "wanted-bitfield", "ban-peer", "change-uri", "get-servers",
              "verify-files"]}}
// 错误响应
{"jsonrpc": "2.0", "id": 1, "error": {"code": 1, "message": "Unauthorized"}}
```

**双协议族自动识别**：连接首条请求命中 `task.*` / `engine.*` / `events.*` 即为
原生协议；命中 `aria2.*`、无前缀的旧方法名或 `system.*` 则为前端兼容协议。
事件帧按协议族分别过滤。

- **WebSocket** 连接在识别出协议族后就固定下来，该连接的后续请求需留在同一族内。
- **HTTP POST** 每条请求独立判定，因此单发调用可以自由混用两族。
- 原生协议族只在客户端发送 `events.subscribe` 之后才推送事件；兼容协议族则在
  识别出协议族后立即推送。

### 3. 鉴权

- **原生协议**：params 对象中的 `"token"` 字段必须与 `--rpc-secret` 严格相等。
- **前端兼容协议**：第一个位置参数为 `"token:<secret>"`（带 `token:` 前缀），
  服务端在分发前会剥离它。`system.*` 方法不做顶层鉴权——`system.multicall`
  内部的每个子调用各带 token，逐个校验。
- 未配置密钥时跳过鉴权；鉴权失败返回
  `{"error": {"code": 1, "message": "Unauthorized"}}`。

### 4. 原生协议方法

数值字段是真实的 JSON 数字。

#### 4.1 任务管理

| 方法 | 参数 | 返回 |
|---|---|---|
| `task.add` | `uris`(数组,必填), `dir`, `out`, `checksum`, `position`(可选,≥0 插入队列)；或 `torrent`(base64)；或 `magnet`(磁力链接)。其余键会作为任务级选项透传 | `{"gid": "<16-hex>"}` |
| `task.tell` | `gid`, `keys`(数组,可选) | 任务状态对象 |
| `task.list` | `scope`("active"/"waiting"/"stopped"/"all",默认"all"), `offset`, `num`(-1=全部), `keys` | 任务状态对象数组 |
| `task.pause` | `gid` | `{"ok": true}` |
| `task.resume` | `gid` | `{"ok": true}` |
| `task.remove` | `gid`, `deleteFiles`(布尔,默认 false) | `{"ok": true}` |
| `task.purgeResults` | — | `{"ok": true}` |
| `task.removeResult` | `gid`（须终态） | `{"ok": true}` |
| `task.getFiles` | `gid` | 文件列表 |
| `task.getUris` | `gid` | URI 列表 |
| `task.getServers` | `gid` | 服务器列表（HTTP 返回单条目；BT 或终态任务返回空数组） |
| `task.changeUri` | `gid`, `fileIndex`(默认 1), `delUris`(数组), `addUris`(数组) | `{"ok": true, "added": n}` |
| `task.verifyFiles` | `gid`, `algorithm`(默认 `"sha256"`，也支持 `"size"`) | 校验结果（阻塞调用） |
| `task.getPeers` | `gid` | peer 列表，按已连接 / 正在连接 / 已断开 / 已封禁分组 |
| `task.getTrackers` | `gid` | tracker 列表（BT） |
| `task.addTrackers` | `gid`, `trackers`(数组,非空) | `{"ok": true}` |
| `task.banPeer` | `ip`, `duration`(默认 `-1` = 永久；>0 = 秒) | `{"ok": true}` |
| `task.unbanPeer` | `ip` | `{"ok": true}` |
| `task.stopSeed` | `gid`（仅在 `seeding` 状态下有意义） | `{"ok": true}` |
| `task.getOption` | `gid` | 选项对象（全局打底 + 任务级覆盖） |
| `task.changeOption` | `gid`, 键值对（含 `select-file` / `selectFile` 热更新文件选择与任务级限速） | `{"ok": true}` |

`task.add` / `task.changeOption` 常用任务级选项：

| 选项 | 含义 |
|---|---|
| `header` | 任务级请求头，可以是 `"名称: 值"` 字符串数组，也可以是 CRLF/LF 分行字符串。探测、单连接与分片请求都会带上同一组头；`Range`、`Host`、`Content-Length`、`Connection`、`Accept-Encoding` 会被自动丢弃，且**永不注入 Origin**。 |
| `referer` | `Referer` 头的便捷写法 |
| `user-agent` | 对该任务覆盖全局 User-Agent |
| `max-download-limit` / `max-upload-limit` | 任务级限速（覆盖全局限速） |
| `bt-file-selection` | 启用磁力文件选择流程：解析元数据后暂停，等待 `select-file` |
| `select-file` | 要下载的文件序号（从 1 开始），例如 `"1,3"` |
| `checksum` | 完成后校验（`sha-1` / `sha-256` / `sha-512` / `md5`） |

添加任务示例：

```json
{"jsonrpc": "2.0", "id": 1, "method": "task.add",
 "params": {"token": "mytoken", "uris": ["https://example.com/big.zip"],
            "dir": "/Downloads", "out": "big.zip",
            "checksum": "sha-256=<hex>",
            "header": ["Referer: https://example.com/page"],
            "max-download-limit": "2M"}}
```

只有 `uris` / `torrent` / `magnet` 与少数结构性键是保留字段，其余一律存在任务上——
宿主应用可以把自己的选项集整体透传，不必担心引擎因未知键报错。

#### 4.2 引擎管理

| 方法 | 参数 | 返回 |
|---|---|---|
| `engine.getVersion` | — | `{"name", "version", "features"}` |
| `engine.globalStat` | — | `{"downloadSpeed", "uploadSpeed", "numActive", "numWaiting", "numStopped", "numStoppedTotal"}` |
| `engine.getOptions` | — | 全局选项对象 |
| `engine.changeOptions` | 键值对（见下方白名单） | `{"ok": true}` |
| `engine.saveSession` | — | `{"ok": true}` |
| `engine.shutdown` / `engine.forceShutdown` | — | `{"ok": true}` |

`engine.changeOptions` 会在运行期应用以下键（其余键仍会被写进全局选项，可用
`engine.getOptions` 读回，只是在引擎实现它之前不产生效果）：

| 分组 | 键 |
|---|---|
| 并发与文件 | `max-concurrent-downloads`、`dir`、`continue` |
| HTTP | `split`、`max-connection-per-server`、`min-split-size`、`max-overall-download-limit`、`max-overall-upload-limit`、`user-agent`、`all-proxy`、`no-proxy` |
| BitTorrent | `bt-max-peers`、`bt-adaptive`、`bt-encryption`、`bt-protocol`、`bt-seed-mode`、`bt-seed-ratio`、`bt-seed-time`、`bt-trackers`、`bt-ip-ban-list`、`auto-update-trackers`、`bt-listen-port`、`bt-enable-lpd`、`bt-port-mapping`、`bt-save-metadata`、`bt-load-saved-metadata` |
| 节点发现 | `enable-dht`、`enable-dht6`、`enable-peer-exchange`、`dht-listen-port` |
| 存储 | `disk-cache` |

说明：

- `enable-dht` 对私有种子恒为关闭。
- `enable-dht6` 为 BEP 32 双栈，绑定失败时回退 IPv4。
- `bt-seed-time` 单位为分钟；`0` 表示不限时间。
- `all-proxy` 作用于 HTTP(S) 与 tracker 请求；`no-proxy` 是逗号分隔的排除列表。
  Tracker 订阅的拉取由应用侧完成（引擎的订阅抓取不走代理）。

全局 tracker 与订阅：

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

订阅行为：

- **添加 / 启用即拉取**：`addSubscription` 与 `toggleSubscription`（重新启用）
  会立即在后台拉取一次并同步进全局 tracker 列表；TUI 与 RPC 客户端行为一致，
  调用方无需再补一次 refresh。
- **同步语义（而非只增）**：订阅刷新时，远端新增的 tracker 会加入全局列表；
  远端已删除、且此前由该订阅贡献的 tracker 会被清理。手动添加的
  （`engine.addTracker`）与仍由其它订阅提供的 tracker 不受影响。
- **每日自动更新**：后台任务每小时检查一次，仅刷新超过 24 小时未更新的订阅
  （需 `autoUpdateTrackers` 开启）。手动 `refreshSubscription` /
  `refreshAllSubscriptions` 会忽略这个期限，立即完整刷新。
- **安全阀**：远端返回空列表会被视为异常——保留现有 tracker 并记录错误，
  避免误清空。

#### 4.3 任务状态对象

```
gid, status, totalLength, completedLength, uploadLength, downloadSpeed,
uploadSpeed, averageSpeed, bitfield, wantedBitfield, partialBitfield,
connections, errorCode, errorMessage, elapsedMs, finishedAt, dir, filename,
infoHash, bittorrent, awaitingSelection, seedRatio, numSeeders, seeder,
numPieces, pieceLength,
files[{index, path, length, completedLength, selected, uris[{uri, status}]}]
```

- `status`：`waiting` / `active` / `paused` / `seeding` / `complete` /
  `error` / `removed`
- `errorCode`：`0` 无错误，`2` 超时，`3` 未找到，`5` 网络，`9` 校验不符，
  `1` 其它
- `bitfield` 是已落盘的片；`wantedBitfield` 是任务还需要下载的片位图
  （全选时为空串）；`partialBitfield` 暴露写了一半的片——UI 画分片状态靠它。
- 磁力任务等待用户选文件期间 `awaitingSelection` 为 `true`；BT 任务的当前分享率
  在 `seedRatio`。

### 5. 事件订阅

原生协议客户端在 WebSocket 连接上发送一次 `events.subscribe`，服务端随后持续推送：

| 引擎事件 | 方法 | params |
|---|---|---|
| 任务开始 | `task.start` | `{"gid"}` |
| 已暂停 | `task.pause` | `{"gid"}` |
| 已停止 | `task.stop` | `{"gid"}` |
| 下载完成 | `task.complete` | `{"gid"}` |
| 出错 | `task.error` | `{"gid", "errorCode", "errorMessage"}` |
| 进度（1 Hz） | `task.progress` | `{"gid", "status", "completedLength", "totalLength", "downloadSpeed"}` |

事件帧不带 `id` 字段，以此与响应区分：

```json
{"jsonrpc": "2.0", "method": "task.progress",
 "params": {"gid": "abc123...", "status": "active",
            "completedLength": 1048576, "totalLength": 10485760, "downloadSpeed": 262144}}
```

推荐做法：`events.subscribe` + `task.progress` 的事件驱动式界面刷新，不做轮询；
断线重连后重新订阅一次。

### 6. 前端兼容协议（aria2 风格）

面向既有的 aria2 客户端：位置参数、数值字段以字符串承载。

- 业务方法：`aria2.addUri` / `aria2.addTorrent` / `getPeers` / `remove` / `forceRemove` /
  `pause` / `forcePause` / `unpause` / `tellStatus` / `tellActive` / `tellWaiting` /
  `tellStopped` / `getGlobalStat` / `getVersion` / `getFiles` / `getURIs` / `getOption` /
  `changeOption` / `getGlobalOption` / `changeGlobalOption` / `purgeDownloadResult` /
  `removeDownloadResult` / `saveSession` / `shutdown` / `forceShutdown`
- 系统方法：`system.multicall` / `system.listMethods` / `system.listNotifications`
- 事件（无需订阅，识别出协议族后自动推送）：`aria2.onDownloadStart` /
  `onDownloadPause` / `onDownloadStop` / `onDownloadComplete` / `onDownloadError` /
  `onBtDownloadComplete`

调用示例：

```json
{"jsonrpc": "2.0", "id": 1, "method": "aria2.addUri",
 "params": ["token:mytoken", ["https://example.com/a.zip"], {"dir": "/Downloads"}]}
```

### 7. Rust 进程内集成

客户端也可以不经 RPC，直接把引擎作为库嵌入（`xfer-engine` 不依赖 RPC 层）：

```rust
use serde_json::json;
use xfer_engine::TaskManager;

let mgr = TaskManager::start(std::path::PathBuf::from("/Downloads"), 3);
let gid = mgr.add_uri(
    vec!["https://example.com/big.zip".into()],
    &json!({}),
    None,
)?;
let mut events = mgr.events().subscribe(); // 广播事件流
// mgr.tell_status_native / pause / unpause / remove / list_native / global_stat_native
```

传任务级选项的方式与 RPC 一致——就是 `task.add` 里那个键值对象：

```rust
let gid = mgr.add_uri(
    vec!["https://example.com/signed.zip".into()],
    &json!({
        "header": ["Referer: https://example.com/page"],
        "max-download-limit": "2M"
    }),
    None,
)?;
```

磁力任务支持按文件下载：添加时传 `bt-file-selection` 选项；元数据解析完成后任务
自动暂停（`awaitingSelection = true`）；从 `files[]` 读取文件列表与大小交给用户
选择，然后传入从 0 开始的文件序号并恢复：

```rust
let gid = mgr.add_uri(
    vec!["magnet:?xt=urn:btih:...".into()],
    &json!({"bt-file-selection": "true"}),
    None,
)?;
// 解析完成后（任务已暂停）：files[].index - 1 即文件序号
mgr.select_files(&gid, &[0, 2])?; // 只下载第 1 与第 3 个文件
mgr.unpause(&gid)?;               // 确认后立即开始下载
```

### 8. 集成检查清单

1. 启动或连接守护进程；先用 `engine.getVersion` 握手，确认版本与能力。
2. 配置了密钥时，每个请求都要带上 token（原生 `params.token` /
   兼容协议 `token:<secret>`）。
3. 在 WebSocket 连接上先发 `events.subscribe`，再按 `method` 分发事件帧。
4. 以 `gid` 为键维护任务表；用 `task.progress` 更新进度，用终态事件
   （`complete` / `error`）结算任务。
5. 重连之后重新订阅，并用一次完整的 `task.list` 对齐状态。

---

## License

GPL-3.0（GNU General Public License v3.0，仅此版本）。完整许可证文本见
[LICENSE](LICENSE)。
