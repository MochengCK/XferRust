**摘要**

- 修复 BT 任务下载速度远低于其他客户端：tracker 返回的 IPv6 peer 此前被整条静默丢弃、DHT 双栈在 macOS 上必然回退纯 IPv4；同一公开种子实测峰值由 <1MB/s 提升到 17.1MB/s
- tracker announce 补全 IPv6：非 compact 列表中的裸 IPv6 地址按字面量解析、新增 `peers6`（BEP 32）字段解析
- 每个 HTTP tracker 并发发起 IPv4 与 IPv6 两路 announce 并合并去重，不再漏掉只回给 IPv6 来源的 seeder
- 修复 DHT 双栈绑定必然失败（`[::]` 被当成域名解析）导致回退纯 IPv4
- 修复 DHT 所有查询必然超时：响应被当成查询解析失败后静默丢弃，现按事务 id 投递给等待中的查询
- DHT 查询超时 10s → 3s，bootstrap 与一轮迭代的 find_node 改为并发，冷启动更快
- 修复 uTP / UDP tracker / BT 监听只支持 IPv4：IPv6 主机上 uTP 与 IPv6 入站连接起不来，双栈 socket 向 IPv6 对端发送直接报 `EINVAL`（IPv6 对端只能白等握手超时后回退 TCP）
- HTTPS 证书信任库补齐本机系统根证书（企业代理、自签 CA、系统证书仓库环境不再全线失败）；`no-proxy` 选项真正生效

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

## 行为变更

- DHT KRPC 查询超时由 10s 收紧到 3s：不可达节点更快判定失败，bootstrap 与 get_peers 迭代不再被慢节点拖住
- 同一 HTTP tracker 每轮会收到两次 announce 请求（IPv4 + IPv6）：tracker 侧请求量翻倍，换取只回给 IPv6 来源的全部 seeder；无 IPv6 可用时不产生额外请求
- `no-proxy` 由「仅记录」变为真正生效：命中该列表的主机不再走 `all-proxy` 配置的代理
- HTTPS 信任链在本机系统根证书范围内放宽（并集语义）：本机被信任的自签证书也会被引擎接受，公共站点的校验结果不变

## 构建与发布

- 引擎版本升至 0.3.1（`engine.getVersion` 返回 0.3.1）
- 新增依赖 `rustls-native-certs`（读取各平台系统证书仓库：macOS Security.framework、Windows schannel、Linux /etc/ssl + openssl-probe）