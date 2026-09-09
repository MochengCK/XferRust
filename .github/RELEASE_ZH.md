**摘要**：本版本在 v0.3.0 基础上继续演进，围绕「HTTP 能力补齐、前端协议对接、可观测性、会话持久化」四条主线。HTTP 下载获得与 BT 同级的真实分片位图（`numPieces` / `pieceLength` / `bitfield`，随落盘实时推进）与全局限速执行（跨任务共享令牌桶，`1M`/`500K` 单位直通，运行时热生效）；`task.getTrackers` 升级为带 per-tracker announce 状态（协议 / 工作状态 / 做种数 / 下次 announce 时间）。协议对接侧：任务状态新增 aria2 风格 `bittorrent` 对象与 `infoHash` 字段（前端 BT 识别与任务命名依赖）、命令行运行时选项直通（`--key=value`，含 `--enable-upnp` 等应用侧开关自动映射）、`engine.changeOptions` 支持全局 tracker 全量替换（`bt-trackers`）与订阅自动更新开关。可观测性侧：下载速度改为每秒刷新的 3 秒滑动窗口、暴露 BT 分片位图与逐对端位图、新增 BT 对端 IP 封禁（限时/永久），以及 `task.changeUri` / `task.getServers` 两个 aria2 兼容 RPC。本轮还补齐任务级平均速度（`averageSpeed`，活动阶段累计、随会话持久化）、细化 HTTP 分片粒度（几 MB 的小文件也能及时点亮分片）、修复并扩展文件选择链路（`task.changeOption` 真正应用 `select-file`、`files[].selected` 按真实选择上报、HTTP/HTTPS 任务支持选择文件），单文件磁力元数据就绪后自动全量续下。本轮还新增任务级限速（`max-download-limit` / `max-upload-limit`，HTTP 与 BT 全覆盖，生效值取 min(单任务, 全局)、运行时热生效、随会话持久化），`.torrent` 添加支持 `bt-file-selection` 文件勾选流程。本轮还新增分片三态展示（`partialBitfield`：部分下载分片位图，UI 区分未开始/下载中/已完成）、分片位图会话持久化（重启后已完成任务分片不丢）。CI 新增 linux-arm64（aarch64 musl 静态）构建矩阵。

## 新功能

### 任务状态 BT 标识（bittorrent / infoHash）

- 任务状态响应（`task.tell` / `task.list`，aria2 风格与原生数值两种编码）新增 `bittorrent` 对象与 `infoHash` 字段：非 BT 任务 `bittorrent` 为 `null`（前端判 BT 依据 `task.bittorrent` 真值）；元数据就绪（.torrent / 磁力已取回元信息）为 `{"info": {"name", "hash"}}`；磁力元数据获取中为 `{}`（前端据此显示「获取元数据中」）
- `infoHash` 为 info 字典哈希的十六进制表示：.torrent 任务取解析元信息时计算的哈希，磁力任务取握手/元数据交换得到的 `bt_info_hash`

### 全局 tracker 与订阅开关直通

- `engine.changeOptions` 新增 `bt-trackers`：支持字符串数组或换行/逗号分隔字符串，全量替换语义（应用端每次推送完整列表），与手动增删相同的增量语义同步到所有活动 BT 任务（新增注入、移除剔除），来源记为 `manual`
- 新增开关 `auto-update-trackers` 控制订阅源自动更新（布尔语义，`"false"` / `"0"` 均视为关闭）

### 命令行运行时选项直通

- 引擎命令行支持 `--key=value` 形式的运行时全局选项直通注入（与 `engine.changeOptions` 同一存储，CLI 传值覆盖会话恢复的同名旧值）：`split`、`max-connection-per-server`、`min-split-size`、全局限速、`bt-max-peers`、`bt-adaptive`、`bt-seed-mode`、`bt-seed-ratio`、`bt-encryption`、`bt-protocol`、`bt-listen-port`、`dht-listen-port`、`bt-enable-lpd`、`bt-port-mapping`
- 应用侧开关自动映射：`--enable-upnp` / `--enable-nat-pmp` → `bt-port-mapping`，`--enable-utp` → `bt-protocol`（`tcp+utp` / `tcp`），宿主应用启动引擎时无需再通过 RPC 二次下发

### 分片展示、速度实时化与对端管理

- 下载速度实时化：速度采样从「每 3 秒更新一次的 3 秒窗口均值」改为**每秒刷新的 3 秒滑动窗口**——窗口平均仍能抹平片级批量落盘造成的 0↔尖峰抖动，但速度值每秒都在更新，前端不再出现速度数字长时间纹丝不动的情况
- BT 分片位图暴露：任务状态响应（`task.tell` / `task.list`）的 `bitfield` 字段不再恒为空串，输出真实「已下载片」位图（aria2 兼容 hex 编码：每片 1 bit、字节内高位在前）；驱动侧 1Hz 同步，任务暂停后保留最后已知状态，客户端可据此渲染分片进度图
- 逐对端位图：`task.getPeers` 每个对端新增 `bitfield` 字段（对端已拥有片的 hex 位图，seed 为全 1），可渲染对端分片分布
- BT 对端 IP 封禁：新增 RPC `task.banPeer`（`duration` 秒后自动解封，`<= 0` 为永久；封禁立即断开该 IP 现有连接并清出待连队列）/ `task.unbanPeer`（全局语义，作用所有 BT 任务）；封禁名单随会话持久化，重启后继续生效
- 全局封禁名单选项：`engine.changeOptions` 新增 `bt-ip-ban-list`（IP 数组或换行/逗号分隔字符串，全量替换永久封禁），应用端偏好设置可直通下发
- HTTP 任务改 URI：新增 RPC `task.changeUri`（aria2 兼容语义：waiting/paused 状态下按 `fileIndex` 删除 `delUris`、追加 `addUris`；active 状态拒绝，由应用端回退为「重建任务」）
- 服务器列表：新增 RPC `task.getServers`（HTTP 任务返回 aria2 兼容的服务器条目 `currentUri` / `downloadSpeed` / `downloadLength`，BT 任务返回空数组）

### 任务平均速度（averageSpeed）

- 任务状态响应（`task.tell` / `task.list`，aria2 风格与原生数值两种编码）新增 `averageSpeed` 字段（字节/秒）：驱动侧 1Hz ticker 在活动下载阶段逐秒累计「完成字节增量 + 活动时长」，均值 = 累计字节 / 活动秒数；做种与暂停阶段不累计、不稀释
- 累计数据随会话持久化，重启续传后平均速度不漂移；应用端进度窗口与任务详情直取该字段实时刷新，无需前端自行采样估算

### 文件选择（select-file）全链路

- `task.changeOption` 真正应用 `select-file`（aria2 语义：1 起算的逗号分隔文件序号，空 = 全选）：此前该键只被存入任务选项、从未应用，用户勾选保存后重开详情页显示「一个未选」，选择也从未生效。现即时应用：BT 运行中热生效（重算所需片位图与总量），暂停/等待中的任务在下次启动时生效
- `files[].selected` 按真实选择上报：原生编码此前硬编码 `true`，aria2 兼容编码同步输出 `"true"` / `"false"` 字符串
- 选择文件扩展到 HTTP/HTTPS 任务：单文件布局（文件数恒为 1），选择状态持久化并在下次启动时生效；磁力元数据未就绪时仍返回错误
- `task.add`（`addUri` / `addTorrent`）支持 `select-file` 预选文件；无效/越界取值降级为告警，不中断任务添加
- `.torrent` 添加支持 `bt-file-selection` 勾选流程：与磁力同一状态机——添加时置位等待标记，1Hz 巡检发现元数据（种子添加时即刻可得）就绪后自动暂停，等应用端勾选文件后以 `select-file` 恢复续下
- 单文件磁力自动续下：磁力元数据就绪后若为单文件布局（无选择意义），自动清除等待标记并以重启意图重新入队、按全量选择直接续下，无需用户手动恢复；多文件磁力维持「元数据就绪 → 自动暂停等待勾选」流程

### HTTP 分片位图与全局限速

- HTTP 任务分片位图：`task.tell` / `task.list`（aria2 风格 + 原生数值两种编码）对 HTTP 任务输出真实分片数据——`numPieces` / `pieceLength` / `bitfield`（aria2 兼容 hex 编码）。片长 = 进度显示粒度，与分段粒度（`min-split-size`）解耦：取 `min(min-split-size, max(total/2048, 64KB))`——几 MB 的小文件也能及时点亮分片（此前片长固定取 `min-split-size`，一片 = 整个分段，下载几 MB 位图仍全零），大文件按 `min-split-size` 保持既有粒度。写侧按落盘区间增量记账：多连接分片写线程与单连接顺序写两条路径均覆盖；服务器无视 Range 重发全量时位图作废重建；控制文件水位预填保证重启恢复后位图与真实落盘自洽；未知总长或不支持 Range 时不输出；任务暂停后保留最后已知状态。此前 HTTP 任务恒为 `numPieces=0` / `bitfield` 空，桌面任务列表与详情页分片图无法显示
- HTTP 全局限速执行：异步令牌桶限速器注入每条下载连接——多连接在读循环、单连接在逐块落盘前消费令牌，令牌不足时异步等待，TCP 背压自然收敛。此前限速只下发到 BT 引擎，HTTP 下载完全不受限速约束
- 单任务限速：`task.changeOption` 支持 `max-download-limit` / `max-upload-limit`（aria2 语义，`1M`/`500K` 单位直通），实际生效值 = min(单任务, 全局)（0 = 不限不参与约束），运行时热生效并随会话持久化；HTTP 每个任务持有独立限速器承载合成值（单连接与 split 多连接共享，全局或任务级变更即时重新同步），BT 由各 TorrentEngine 按合成值下发；`task.getOption` 输出当前任务限速值。全局限速自此对 HTTP/HTTPS 与 BT 全量生效
- 限速值解析升级：`max-overall-download-limit` / `max-overall-upload-limit` 接受 aria2 风格单位（`1M` / `500K` / 纯整数字节），与桌面端配置格式兼容（此前仅接受纯整数，带单位的值被拒绝或静默当作不限速）
- 单键错误不再中断整批设置：`changeOptions` 中限速值 / `bt-encryption` / `bt-protocol` / 端口类选项取值非法时降级为告警并跳过该键，其余设置项照常生效——此前一个非法键导致整批 changeOptions 返回错误、用户改一处限速会把所有系统设置一起弄失效
- tracker announce 状态：`task.getTrackers` 从仅返回 URL 升级为带 per-tracker 状态——`protocol`（http / https / udp / ws）、`status`（working / not-working / waiting）、`seeders` / `leechers`（tracker 报告的 complete / incomplete）、`peers`、`lastAnnounceTime` / `nextAnnounceTime`（成功响应 interval 推算）、`error`（最近一次失败原因）；BT 引擎在每轮 announce 聚合时逐 URL 记录，未 announce 过的 URL 保持 waiting

## 新功能（续）

### 分片三态展示（partialBitfield）

- 任务状态响应（`task.tell` / `task.list`，aria2 风格与原生数值两种编码）新增 `partialBitfield` 字段：HTTP 任务输出部分下载分片位图（已落盘 > 0 但未满的分片标记为 1），BT 任务恒为空串。配合已有的 `bitfield`（全满分片），UI 可渲染三态分片图：未开始（灰）、下载中（黄）、已完成（绿）。`PieceTrack` 新增 `partial_bitfield()` 方法，遍历逐片已落盘字节量，判断 `0 < done[i] < len_at(i)` 的分片

### 分片位图会话持久化

- 会话保存（`session_json`）新增 `btBitfield`（BT 分片位图 hex）、`httpNumPieces` / `httpPieceLen`（HTTP 分片维度），重启恢复（`restore_tasks`）时从这些字段重建 `bt_bitfield` 与 `http_pieces`（`PieceTrack`），并根据已下载字节回填分片状态（已完成任务直接全满）。此前分片位图不随会话持久化，重启后已完成任务的分片图消失（`numPieces=0` / `bitfield` 空）

## 问题修复

- 修复重启后已完成任务分片进度消失：分片位图（`bt_bitfield` / `http_pieces`）此前不随会话保存与恢复，重启后 `numPieces=0` / `bitfield` 空串，UI 分片图空白。现会话文件持久化分片数据，恢复时重建位图
- 修复「选择文件」保存后不生效且重开详情页全部显示未选：`changeOption` 的 `select-file` 此前只被存储、从未应用，且原生编码 `files[].selected` 硬编码 `true`，详见「文件选择（select-file）全链路」
- 修复几 MB 的小文件 HTTP 分片位图恒为零：片长曾固定取 `min-split-size`，小文件整个下载量尚不足以点亮一片，详见「HTTP 分片位图与全局限速」
- 修复 HTTP 任务完成后误报 `seeder=true`：`seeder` 语义修正为「本端为 BT 任务且已完整」，此前按「completed ≥ total」对所有任务类型计算，HTTP 任务下载完成即误报，应用端据此把普通任务标成“做种中”并补发 BT 完成事件
- 修复 HTTP 任务分片数据恒为空（`numPieces=0` / `bitfield` 空串），详见「HTTP 分片位图与全局限速」
- 修复限速设置不生效：带单位的限速值（如 `1M`）此前被引擎拒绝或静默按不限速处理，且 HTTP 下载路径完全没有限速执行，详见「HTTP 分片位图与全局限速」

## 构建与发布

- CI 构建矩阵新增 `linux-arm64`（aarch64-unknown-linux-musl）：产物 `xfer-tui-linux-arm64.tar.gz` / `xferrust-linux-arm64.tar.gz` 随 Release 发布
- musl 静态链接：无 glibc 版本依赖，无需随包附带 lib/ 动态库目录，解压即可运行
- linux-arm64 交叉编译改用 cargo-zigbuild，替换不稳定的 musl.cc 下载源
