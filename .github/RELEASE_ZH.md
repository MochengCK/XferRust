**摘要**

- 新增任务文件校验 RPC `task.verifyFiles`（存在性 / 大小 / 流式哈希，引擎原生执行）
- UDP 打洞 `ut_holepunch` 对齐 libtorrent 标准，可与 qBittorrent 等标准客户端互为中介穿透双 NAT
- HTTP 分片位图、全局限速与任务级限速、任务平均速度、命令行选项直通
- 文件选择 `select-file` 全链路生效，`.torrent` 支持 `bt-file-selection` 勾选流程
- 分片三态展示（`partialBitfield`）与会话持久化；BT 对端 IP 封禁；tracker announce 状态
- `task.getPeers` 新增对端拨号 / 传输统计与已封禁分组展示
- 修复磁力 / 种子任务跳过「待选择文件」直接下载、中文任务名乱码、平均速度计算不准
- 重启后所有未完成任务恢复为暂停；单任务限速优先覆盖全局

## 新特性

### 任务状态 BT 标识（bittorrent / infoHash）

- 任务状态响应（`task.tell` / `task.list`，aria2 风格与原生数值两种编码）新增 `bittorrent` 对象与 `infoHash` 字段
- 非 BT 任务 `bittorrent` 为 `null`；元数据就绪为 `{"info": {"name", "hash"}}`；磁力元数据获取中为 `{}`（前端据此显示「获取元数据中」）
- `infoHash` 为 info 字典哈希的十六进制表示：.torrent 任务取解析时计算的哈希，磁力任务取握手 / 元数据交换得到的 `bt_info_hash`

### 全局 tracker 与订阅开关直通

- `engine.changeOptions` 新增 `bt-trackers`：字符串数组或换行 / 逗号分隔字符串，全量替换语义，与手动增删相同的增量语义同步到所有活动 BT 任务
- 新增开关 `auto-update-trackers` 控制订阅源自动更新（`"false"` / `"0"` 均视为关闭）

### 命令行运行时选项直通

- 引擎命令行支持 `--key=value` 直通注入全局选项（与 `engine.changeOptions` 同一存储，CLI 传值覆盖会话恢复旧值）：`split`、`max-connection-per-server`、`min-split-size`、全局限速、`bt-max-peers`、`bt-adaptive`、`bt-seed-mode`、`bt-seed-ratio`、`bt-encryption`、`bt-protocol`、`bt-listen-port`、`dht-listen-port`、`bt-enable-lpd`、`bt-port-mapping`
- 应用侧开关自动映射：`--enable-upnp` / `--enable-nat-pmp` → `bt-port-mapping`，`--enable-utp` → `bt-protocol`
- 任意未知 `--key=value` 宽容接受为全局默认值（详见行为变更），宿主应用可透传完整配置，引擎升级后无需改动启动参数

### 分片展示、速度实时化与对端管理

- 下载速度改为**每秒刷新的 3 秒滑动窗口**：窗口平均抹平片级批量落盘的抖动，速度值每秒更新，不再长时间纹丝不动
- BT 分片位图暴露：`bitfield` 输出真实「已下载片」位图（aria2 兼容 hex 编码），驱动侧 1Hz 同步，暂停后保留最后已知状态
- 逐对端位图：`task.getPeers` 每个对端新增 `bitfield` 字段（对端已拥有片，seed 为全 1）
- BT 对端 IP 封禁：`task.banPeer`（`duration` 秒后自动解封，`<= 0` 永久；立即断开现有连接）/ `task.unbanPeer`；封禁名单随会话持久化
- 全局封禁名单选项：`engine.changeOptions` 新增 `bt-ip-ban-list`（IP 数组或换行 / 逗号分隔字符串，全量替换永久封禁）
- 新增 `task.changeUri`（aria2 兼容：waiting/paused 下按 `fileIndex` 删 `delUris` 追加 `addUris`，active 拒绝）与 `task.getServers`（HTTP 任务返回服务器条目，BT 返回空数组）

### 任务平均速度（averageSpeed）

- 任务状态响应新增 `averageSpeed` 字段（字节/秒）：活动下载阶段逐秒累计「字节增量 + 时长」，做种与暂停阶段不累计不稀释
- 累计数据随会话持久化，重启续传后均值不漂移；应用端直取该字段刷新，无需自行采样

### 文件选择（select-file）全链路

- `task.changeOption` 真正应用 `select-file`（aria2 语义：1 起算的逗号分隔文件序号，空 = 全选）：BT 运行中热生效，暂停 / 等待任务下次启动生效
- `files[].selected` 按真实选择上报：原生编码此前硬编码 `true`，aria2 编码输出 `"true"` / `"false"`
- 文件选择扩展到 HTTP/HTTPS 任务：单文件布局，选择状态持久化、下次启动生效
- `task.add` 支持 `select-file` 预选；无效 / 越界取值降级为告警，不中断添加
- `.torrent` 添加支持 `bt-file-selection` 勾选流程：与磁力同一状态机——元数据就绪后自动暂停，应用端勾选后以 `select-file` 恢复
- 单文件磁力自动续下：元数据就绪后若为单文件布局，自动按全量选择续下，无需手动恢复

### HTTP 分片位图与全局限速

- HTTP 任务输出真实分片数据：`numPieces` / `pieceLength` / `bitfield`（aria2 兼容 hex 编码）
- 片长 = 进度显示粒度，与分段粒度解耦：取 `min(min-split-size, max(total/2048, 64KB))`——几 MB 小文件也能及时点亮分片，大文件保持 `min-split-size` 粒度
- 写侧按落盘区间增量记账，多连接与单连接路径均覆盖；服务器无视 Range 重发全量时位图作废重建；控制文件水位预填保证重启后位图与落盘自洽；未知总长或不支持 Range 时不输出
- HTTP 全局限速执行：异步令牌桶注入每条下载连接，令牌不足时异步等待，TCP 背压自然收敛。此前 HTTP 下载完全不受限速约束
- 限速值解析升级：全局限速接受 aria2 风格单位（`1M` / `500K` / 纯整数字节），此前仅接受纯整数
- 单键错误不再中断整批设置：`changeOptions` 中非法取值降级为告警并跳过该键，其余设置照常生效

### 任务级限速（max-download-limit / max-upload-limit）

- `task.changeOption` 支持 `max-download-limit` / `max-upload-limit`（aria2 语义，`1M`/`500K` 单位直通），HTTP 与 BT 全覆盖，运行时热生效、随会话持久化
- 生效值语义见行为变更：单任务优先覆盖全局；`task.getOption` 输出当前任务限速值
- HTTP 每任务持有独立限速器（单连接与 split 多连接共享），BT 由各 TorrentEngine 按合成值下发

### tracker announce 状态

- `task.getTrackers` 从仅返回 URL 升级为带 per-tracker 状态：`protocol`（http/https/udp/ws）、`status`（working/not-working/waiting）、`seeders` / `leechers`、`peers`、`lastAnnounceTime` / `nextAnnounceTime`、`error`
- BT 引擎在每轮 announce 聚合时逐 URL 记录，未 announce 过的 URL 保持 waiting

### 分片三态展示与持久化

- 任务状态响应新增 `partialBitfield`：HTTP 任务输出部分下载分片位图（已落盘 > 0 但未满的分片为 1），BT 任务恒为空串；配合 `bitfield` 可渲染未开始 / 下载中 / 已完成三态分片图
- 会话保存新增 `btBitfield`、`httpNumPieces` / `httpPieceLen`，重启恢复时重建位图并按已下载字节回填状态——此前重启后已完成任务分片图消失

### 任务文件校验（task.verifyFiles）

- 新增原生 RPC `task.verifyFiles`：对指定任务执行存在性检查、文件大小比对、流式哈希计算（`algorithm` 支持 `size` / `sha256` / `sha1` / `md5` / `sha512`，大小写不敏感）
- 路径解析与读盘全部在引擎内完成：BT 多文件按「目录名/相对路径」拼接任务目录，HTTP 任务取实际落盘路径；未选择的 BT 文件不参与校验
- 返回结构化结果：`status`（`ok` / `missing` / `sizeMismatch`）、`count`、`missing` / `mismatched` 与 `hashes`（`path` + `digest` 十六进制摘要，`size` 校验时为空）
- `engine.getVersion` 的 `features` 新增 `"verify-files"`；`xfer-storage` 新增 `file_digest_hex` 与已有 `verify_file_hash` 共享底层实现

### UDP 打洞标准化（ut_holepunch，libtorrent 事实标准）

- 线格式对齐 libtorrent：`msg_type(1) + addr_type(1) + addr(4/16) + port(2)`，仅 `failed` 追加 4 字节错误码；消息类型与错误码枚举与 `bt_peer_connection` 完全一致。此前私有线格式无法被任何标准客户端解析
- 中介（rendezvous）语义对齐：先解析目标连接（精确 endpoint 匹配 + 同 IP 回退），不可达 / 不支持 / 目标为发起方分别回对应 `failed`；成功则双向转发 connect
- 补齐发起侧：直连重试耗尽后，主动向已连接且广告 `ut_holepunch` 的 peer 请求中介（每目标最多 2 轮、每轮最多 2 个中介）；此前只会被动应答，双 NAT 下无法与标准客户端互相穿透
- 未在扩展握手广告 `ut_holepunch` 的对端发来的消息一律忽略；PEX `added.f` 标志位修正为 libtorrent 语义（0x08 = 支持 ut_holepunch，0x04 = uTP）

### 对端信息扩展与封禁展示（task.getPeers）

- 每个对端新增拨号 / 传输统计字段：`downSpeed` / `upSpeed` / `tcpFails` / `utpFails` / `udpFails` / `attempting`
- 新增已封禁分组：封禁按 IP 记录（`addr` 仅地址、`port` 空），输出 `remainingSecs`（0 = 永久）、`source` 与 `banReason`（`manual` = 手动封禁 / `ban_list` = 名单下发）；一次 `getPeers` 同时给出在线 / 尝试中 / 已断开 / 已封禁四类
- 封禁条目区分来源：`task.banPeer` 记为手动，`bt-ip-ban-list` 下发记为名单；随会话持久化，旧会话文件无来源字段时视为名单下发

### 字符集探测解码（xfer-types::text）

- 新增文本解码模块：显式 charset → 严格 UTF-8 → GB18030 → lossy 逐级回退，统一供磁力 / .torrent / HTTP 解析使用
- 磁力 `dn` 百分号编码、.torrent `name` 与路径段、HTTP `Content-Disposition: filename*`（RFC 5987，尊重声明的 gb2312/gbk 字符集）与 URL 路径百分号编码均走该模块

## 问题修复

- 修复磁力 / .torrent 任务跳过「待选择文件」直接开始下载：`task.add` 此前只透传 `dir` / `out` / `checksum`，`bt-file-selection` / `select-file` 等任务级选项被静默丢弃；现除协议保留键外全量透传，元数据就绪后照常自动暂停等待勾选
- 修复任务平均速度不准：均值公式 `bytes/(ms/1000)` 在活动时长不足 2 秒时因整数截断被放大（ms=1999 误差近 2 倍，开头几秒虚高），改为 `bytes*1000/ms`；速度采样同时改为从任务启动瞬间开始，首个 1Hz tick 的字节计入均值（此前被丢弃，短任务均值系统性偏低）
- 修复磁力任务元数据就绪后「待选择文件」延迟出现：元数据获取循环内的 tracker announce（单次可阻塞到 15s 超时）与 1 秒轮询不感知取消，暂停意图要等本轮结束才落地；现取消优先处理，暂停即时生效
- 修复中文任务名 / 文件名乱码（显示为一串 `????`）：中文站点磁力 `dn` 常用 GBK 百分号编码、老中文种子 `name` 与路径段为 GBK 字节（此前非 UTF-8 会被整包拒收）、HTTP `filename*` 声明 gb2312/gbk 时按 UTF-8 解码——均改走字符集探测解码，详见「字符集探测解码」
- 修复重启后已完成任务分片进度消失：分片位图此前不随会话保存与恢复，详见「分片三态展示与持久化」
- 修复「选择文件」保存后不生效且重开详情页全部显示未选：`select-file` 此前只被存储从未应用，详见「文件选择（select-file）全链路」
- 修复几 MB 小文件 HTTP 分片位图恒为零：片长曾固定取 `min-split-size`，详见「HTTP 分片位图与全局限速」
- 修复 HTTP 任务完成后误报 `seeder=true`：`seeder` 语义修正为「本端为 BT 任务且已完整」，此前按「completed ≥ total」对所有任务类型计算，应用端据此把普通任务标成「做种中」
- 修复限速设置不生效：带单位的限速值此前被拒绝或静默按不限速处理，且 HTTP 下载路径完全没有限速执行，详见「HTTP 分片位图与全局限速」

## 行为变更

- 单任务限速生效语义调整为「单任务优先覆盖全局」：已设置的任务限速优先生效，可高于也可低于全局；未设置（0）跟随全局。此前为两者取小，单任务限速无法高于全局
- 重启恢复语义收紧：所有未完成任务（活动 / 等待 / 暂停）一律恢复为暂停，不再自动开始下载；断点数据随会话保存，手动恢复即续传
- 引擎命令行未知选项不再「告警丢弃」：任意 `--key=value` 宽容接受为全局默认值，`engine.getOptions` 可读回、随会话持久化，仅裸位置参数视为无效告警忽略

## 构建与发布

- CI 构建矩阵新增 `linux-arm64`（aarch64-unknown-linux-musl 静态链接，无 glibc 依赖、免带 lib/ 目录），交叉编译使用 cargo-zigbuild
- macOS 引擎内核产物按架构拆分：TUI 仍为双架构通用二进制，引擎内核分为 `xferrust-darwin-aarch64.tar.gz` 与 `xferrust-darwin-x86_64.tar.gz`，嵌入端按目标架构直取，无需 thin 提取
