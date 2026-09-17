**摘要**

- 修复 BT 公网下载速度远低于其他客户端（同一公开种子实测 <1MB/s → 17.1MB/s）：tracker 丢弃 IPv6 peer、DHT 双栈绑定必然失败、KRPC 查询必然超时三处根因
- IPv6 补全：tracker 非 compact 列表的裸 IPv6 字面量与 BEP 32 `peers6` 解析、IPv4 + IPv6 双路 announce 合并去重，uTP / UDP tracker / BT 监听全部双栈化（IPv6 主机上此前起不来）
- HTTPS 补齐本机系统根证书（企业代理 / 自签 CA 环境不再全线失败）；`no-proxy` 真正生效
- 修复 HTTP 分片下载在控制文件段表不可用时**一个字节都没下就报完成**（磁盘上留下残缺文件却显示成功）；续传位图不再领先于磁盘，开启 `disk-cache` 后崩溃恢复不再产出「已完成的空洞」
- 修复 DHT 三处对外行为错误：响应不校验来源地址（猜中 16 位事务 id 即可注入伪造响应）、`announce_peer` 恒宣告 `implied_port=1`（对端把 DHT 端口记成我们的 BT 端口）、回 `get_peers` 把 IPv6 peer 编成 `0.0.0.0`
- 修复「只有 1 个文件的多文件种子」落盘布局（文件被写成 `<name>` 占了目录名，旧版已下载数据自动迁移）与单文件种子的 `files[].path` / `task.verifyFiles` 路径（`<name>/<name>` → `<name>`）
- 修复 NAT-PMP 的 TCP 端口映射从未生效（opcode 6 → RFC 6886 规定的 2）、DHT 路由表保存必 panic、版本号出现两位数时 peer-id 生成必 panic
- 冷启动更快、空闲更省：DHT 查询超时 10s → 3s 且 bootstrap 与一轮迭代的 find_node 改为并发，release 构建改按性能优化（详见「构建与发布」）

## 问题修复

- 修复非 compact peer 列表中的 IPv6 地址被丢弃：此前把地址与端口拼成 `ip:port` 再解析，裸 IPv6 字面量必然失败（端口被吞进地址）导致整条 peer 丢失；现先按 IP 字面量解析再组装端口，单条非法只跳过该条
- 修复 tracker 的 `peers6` 字段被忽略：新增 BEP 32 compact 解析（18 字节/条 = 16 字节 IPv6 + 2 字节端口，端口为 0 丢弃），长度非 18 倍数时截断尾部并告警
- 修复始终拿不到 IPv6 seeder：域名解析 IPv4 优先，announce 必然从 IPv4 发出，而 tracker 只把 IPv6 peer 回给 IPv6 来源（BEP 7）；现每个 tracker 并发发起 IPv4 / IPv6 两路 announce 并合并去重，域名无 IPv6 地址或为 IP 直连时自动跳过，IPv4 失败时回退 IPv6 结果
- 修复 DHT 双栈节点在 macOS 上必然启动失败：绑定地址 `[::]` 此前作为域名交给解析器（报错后回退纯 IPv4）；现按 IP 字面量解析（自动剥离方括号）
- 修复双栈 DHT socket 收发地址族不匹配：发往 IPv4 节点前转 v4-mapped、接收到的 v4-mapped 源地址还原为 IPv4，路由表、known_peers 与交给 BT 的 peer 地址仍保持 IPv4
- 修复 DHT 所有 KRPC 查询必然超时：查询方与常驻接收循环争抢同一 socket，响应被接收循环取走后按查询解析失败静默丢弃，调用方只能等到超时；现以「事务 id → 通道」登记待响应查询，接收循环识别响应后按 tid 投递（发送失败或超时即摘除登记项）
- 修复 DHT 冷启动慢：bootstrap 各节点与一轮迭代的 find_node 改为并发执行
- 修复 uTP 无法与 IPv6 对端通信：uTP socket 此前绑纯 IPv4，双栈监听后向 IPv6 / IPv4 对端发送都需地址族转换——不转换时 sendto 报 `Invalid argument (os error 22)`，与 IPv6 对端的 uTP 拨号必然失败（每个对端白等一轮握手超时才回退 TCP，日志实测 3s/对端）；现绑定 `[::]` 双栈 socket 并在收发两端做 v4-mapped 转换
- 修复无 IPv6 或 IPv6-only 主机上 BT 无法监听：TCP 监听此前绑 `0.0.0.0`（IPv6-only 主机上任务启动即失败，双栈主机收不到 IPv6 入站连接）；现优先绑 `[::]` 双栈，并在 `IPV6_V6ONLY=1` 的系统（如 Windows 默认）上补一个纯 IPv4 监听，双栈已覆盖时该补充绑定冲突会被忽略
- 修复 UDP tracker 只支持 IPv4：socket 此前绑 `0.0.0.0`，IPv6-only 主机上 `udp://` tracker 全部不可用、IPv6 tracker 发送失败；现双栈绑定并做发送地址转换
- 修复 v4-mapped 形式的对端地址未归一：部分 tracker / PEX 用 `::ffff:a.b.c.d` 传 IPv4 地址，此前会与 IPv4 字面量形成两条对端记录，且在没有 IPv6 路由的环境里拨号必然失败；现入库前统一还原为 IPv4
- 修复 HTTPS 在自签 CA / 企业代理（TLS 拦截）环境全部失败：TLS 此前只信任编译期内置的 Mozilla 根证书，本机系统证书仓库（含企业 CA、用户导入证书）完全不参与校验；现同时加载系统根证书（与原内置根证书取并集，不改变公共站点校验结果）
- 修复 `no-proxy` 选项不生效：该选项此前只被存储、从未用于构建 HTTP 客户端，代理环境里 `no-proxy` 列出的局域网 / 回环地址仍被塞进代理，本可直连的地址反而失败；现作为代理的直连例外真正生效（走环境变量代理时由 `NO_PROXY` 环境变量控制）
- 修复 HTTP 分片下载的控制文件段表不可用时被判为已完成：段表非严格平铺（有间隙 / 重叠 / 空表）时既不回落也不重建段表，`todo` 因此算出 0 → 直接 `finished`——**一个字节都没下载就报成功**，且目标文件不被截齐到正确长度；现校验不过的控制文件一律等价于「无控制文件」（把已有文件当连续前缀、重新按剩余量切段）
- 修复续传位图领先于磁盘：开启 `disk-cache` 后片数据先落入内存回写缓冲，而片完成时立即写续传控制文件——此时位图声明的已完成片可能一个字节都没落盘，崩溃 / 断电恢复后按位图跳过这些片，产出「已完成的空洞」（静默损坏）。现持久化位图排除仍在回写缓存中的片（代价仅是崩溃后重下缓存内那几个片），干净暂停改为先 `flush_all` 落盘再写位图
- 修复 DHT KRPC 响应不校验来源地址：待响应表仅以 16 位事务 id 为键，一轮并发查询（bootstrap + 迭代 find_node 可达数百个）按生日界就有可观概率撞号把响应错投给另一个节点的等待者，任意主机只要猜中 tid 也能注入伪造响应（peer 会直接进入 BT 连接队列）；现按键改为「来源地址 + tid」，且无等待者的响应不再进入查询处理路径
- 修复 NAT-PMP 的 TCP 端口映射从未生效：opcode 写成 6（既非 1 也非 2），网关一律回不支持，TCP 映射全靠 UPnP 回退兜底；按 RFC 6886 §3.3 改为 2
- 修复 DHT `announce_peer` 恒以 `implied_port=1` 发送：BEP 5 规定该位为 1 时对端忽略 `port` 参数、改用 UDP 源端口，而本端 announce 走的是 DHT socket（端口与 BT 监听端口不同）——对端因此把 DHT 端口记成我们的 BT 端口，散播出去的是没人监听的死地址；现显式声明 `implied_port=0` 并带上真实 BT 监听端口，本端记入 known_peers 的自身地址也改用该端口
- 修复 DHT 回 `get_peers` 时把 IPv6 peer 编成 `0.0.0.0`：所有 peer 统一按 6 字节 IPv4 compact 编码，IPv6 地址的 4 个 IP 字节被填 0（端口非 0 能过接收侧过滤），拿到它的客户端会把 `0.0.0.0:port` 当 peer 去连；现按 BEP 5/BEP 32 拆分——IPv4 进 `values`、IPv6 进 `peers6`（18 字节/条）
- 修复 DHT 路由表保存会在启用持久化后必然 panic：定期保存任务与 `shutdown` 都调用 `RwLock::blocking_read`，而它跑在异步执行上下文里（"Cannot block the current thread from within a runtime"）；现定期保存改走异步读锁，同步入口改用 `try_read` 并在锁被占用时跳过本轮
- 修复「只有 1 个 `files` 条目的多文件种子」落盘成 `<name>` 普通文件：此前以 `files.len() == 1 && path.len() == 1` 判定单文件，这种种子（info 里是 `files` 列表而非 `length`）的文件被写到 `<name>`，占了目录名且文件自身的路径段被整个丢掉，与其他客户端的磁盘布局不一致（无法继续做种）；现以 info 字典写的是 `length` 还是 `files` 为准（`Info::multi_file` 结构位贯穿解析 → 布局 → 落盘 → 校验），文件按 `<name>/<path>` 落盘
- 旧版已下载的此类数据自动迁移：升级后首次打开该任务时把 `<name>`（普通文件）搬进 `<name>/<file>`，续传与做种数据不丢；位置被外来文件占用时明确报错而非误搬
- 修复单文件种子的 `files[].path` 与 `task.verifyFiles` 校验路径：两者此前统一按 `{name}/{path}` 拼接，单文件种子得到 `<name>/<name>`（不存在的路径）——详情页显示错误位置、校验必然报 missing；现统一走 `Info::file_rel_path`（单文件 = `<name>`，多文件 = `<name>/<path>`），与落盘布局同口径
- 修复版本号出现两位数时生成 peer-id 必然 panic：前缀此前用 `format!("-XR{maj}{min}{mic}0-")` 拼接，0.10.0 会拼出 9 字节前缀，`copy_from_slice` 长度不匹配越界（`debug_assert` 在 release 不生效，等于升到 0.10.x 后每个任务第一次生成 peer-id 就崩）；现按 4 个字符位定长构造（0-9 → 数字、10-35 → 字母、超出饱和），结构上不可能越界

## 行为变更

- DHT KRPC 查询超时由 10s 收紧到 3s：不可达节点更快判定失败，bootstrap 与 get_peers 迭代不再被慢节点拖住
- 同一 HTTP tracker 每轮会收到两次 announce 请求（IPv4 + IPv6）：tracker 侧请求量翻倍，换取只回给 IPv6 来源的全部 seeder；无 IPv6 可用时不产生额外请求
- `no-proxy` 由「仅记录」变为真正生效：命中该列表的主机不再走 `all-proxy` 配置的代理
- HTTPS 信任链在本机系统根证书范围内放宽（并集语义）：本机被信任的自签证书也会被引擎接受，公共站点的校验结果不变
- DHT 响应必须来自被查询的那个地址才会被采纳：来自其他地址的响应（伪造 / 过期）直接丢弃，不再进入查询处理流程
- 段表不可用的 HTTP 控制文件不再按原表恢复，改为与「无控制文件」同路径：已有文件视作连续前缀、剩余部分重新切段（可能重下少量重叠字节，换取不产出假完成的残缺文件）
- 开启 `disk-cache` 时续传位图不再声明尚未落盘的片：崩溃恢复后这些片会被重新下载（此前是静默产出损坏文件）；正常暂停 / 停止路径仍先落盘再写位图，进度不受影响
- 单文件与多文件种子的判据改为种子结构位（info 写 `length` 还是 `files`）：只有 1 个文件的多文件种子从「文件占目录名」改为「落在 `<name>/` 目录里」，与 qBittorrent / aria2 等客户端布局一致；旧版已下载的数据在升级后自动迁移，无需重下
- 单文件种子的 `files[].path` 由 `<name>/<name>` 修正为 `<name>`（与 aria2 一致）：客户端按 `bittorrent.info.name` 取名的行为不变

## 构建与发布

- 引擎版本升至 0.3.1（`engine.getVersion` 返回 0.3.1）
- 新增依赖 `rustls-native-certs`（读取各平台系统证书仓库：macOS Security.framework、Windows schannel、Linux /etc/ssl + openssl-probe）
- release 构建从按体积优化（`opt-level="s"`）改为按性能优化（`opt-level=3`）：BT 收发、SHA-1 校验、分片读写等热点更快；体积仍由 `lto` + `strip` 控制，保持单文件发布
- 空闲时 IO 与唤醒优化：会话内容无变化时 30s 定期保存不再重写整份会话文件（分片位图可达数百 KB，空闲时纯属重复写入）；uTP 管理器无连接时 tick 由 1ms 放宽到 100ms，建连 / 入站路径即时恢复，重传与 SACK 定时不受影响
