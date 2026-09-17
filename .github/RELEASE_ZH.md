**摘要**

- 修复 BT 任务下载速度远低于其他客户端：tracker 返回的 IPv6 peer 此前被整条静默丢弃、DHT 双栈在 macOS 上必然回退纯 IPv4；同一公开种子实测峰值由 <1MB/s 提升到 17.1MB/s
- tracker announce 补全 IPv6：非 compact 列表中的裸 IPv6 地址按字面量解析、新增 `peers6`（BEP 32）字段解析
- 每个 HTTP tracker 并发发起 IPv4 与 IPv6 两路 announce 并合并去重，不再漏掉只回给 IPv6 来源的 seeder
- 修复 DHT 双栈绑定必然失败（`[::]` 被当成域名解析）导致回退纯 IPv4
- 修复 DHT 所有查询必然超时：响应被当成查询解析失败后静默丢弃，现按事务 id 投递给等待中的查询
- DHT 查询超时 10s → 3s，bootstrap 与一轮迭代的 find_node 改为并发，冷启动更快

## 问题修复

- 修复非 compact peer 列表中的 IPv6 地址被丢弃：此前把地址与端口拼成 `ip:port` 再解析，裸 IPv6 字面量必然失败（端口被吞进地址）导致整条 peer 丢失；现先按 IP 字面量解析再组装端口，单条非法只跳过该条
- 修复 tracker 的 `peers6` 字段被忽略：新增 BEP 32 compact 解析（18 字节/条 = 16 字节 IPv6 + 2 字节端口，端口为 0 丢弃），长度非 18 倍数时截断尾部并告警
- 修复始终拿不到 IPv6 seeder：域名解析 IPv4 优先，announce 必然从 IPv4 发出，而 tracker 只把 IPv6 peer 回给 IPv6 来源（BEP 7）；现每个 tracker 并发发起 IPv4 / IPv6 两路 announce 并合并去重，域名无 IPv6 地址或为 IP 直连时自动跳过，IPv4 失败时回退 IPv6 结果
- 修复 DHT 双栈节点在 macOS 上必然启动失败：绑定地址 `[::]` 此前作为域名交给解析器（报错后回退纯 IPv4）；现按 IP 字面量解析（自动剥离方括号）
- 修复双栈 DHT socket 收发地址族不匹配：发往 IPv4 节点前转 v4-mapped、接收到的 v4-mapped 源地址还原为 IPv4，路由表、known_peers 与交给 BT 的 peer 地址仍保持 IPv4
- 修复 DHT 所有 KRPC 查询必然超时：查询方与常驻接收循环争抢同一 socket，响应被接收循环取走后按查询解析失败静默丢弃，调用方只能等到超时；现以「事务 id → 通道」登记待响应查询，接收循环识别响应后按 tid 投递（发送失败或超时即摘除登记项）
- 修复 DHT 冷启动慢：bootstrap 各节点与一轮迭代的 find_node 改为并发执行

## 行为变更

- DHT KRPC 查询超时由 10s 收紧到 3s：不可达节点更快判定失败，bootstrap 与 get_peers 迭代不再被慢节点拖住
- 同一 HTTP tracker 每轮会收到两次 announce 请求（IPv4 + IPv6）：tracker 侧请求量翻倍，换取只回给 IPv6 来源的全部 seeder；无 IPv6 可用时不产生额外请求

## 构建与发布

- 引擎版本升至 0.3.1（`engine.getVersion` 返回 0.3.1）