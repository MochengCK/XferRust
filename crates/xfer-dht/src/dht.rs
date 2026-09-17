//! DHT 节点主逻辑（BEP 5）。
//!
//! 功能：
//! - bootstrap：从种子节点开始 ping + find_node 自身 ID 填充路由表；
//! - get_peers：迭代式查找（获得 peer 列表或继续向最近节点查询）；
//! - announce_peer：向找到的最近节点宣告持有某 info_hash；
//! - 响应其他节点的 ping/find_node/get_peers/announce_peer 请求；
//! - 路由表持久化：启动时加载、定期保存到文件。
//!
//! 冷启动节奏（§7.8）：
//! - 首次 get_peers 立即执行；
//! - 未找到 peer 时按 5s/30s/2min 三档重试。

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use sha1::{Digest, Sha1};
use tokio::net::UdpSocket;
use tokio::sync::{oneshot, Mutex, RwLock, Semaphore};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use xfer_bencode::Value;
use xfer_types::InfoHash;

use crate::krpc::{
    encode_announce_peer, encode_get_peers, encode_ping, encode_response, encode_response_v6,
    gen_tid, parse_get_peers_response, parse_query, parse_response, IncomingQuery, KrpcError,
    PeerAddr,
};
#[allow(unused_imports)]
use crate::node_id::node_id_from_seed;
use crate::node_id::NodeId;
use crate::routing_table::{NodeEntry, RoutingTable};
use crate::DHT_PORT;
#[allow(unused_imports)]
use crate::K;

/// DHT 配置。
#[derive(Debug, Clone)]
pub struct DhtConfig {
    /// 本端节点 ID（None 时随机生成）。
    pub node_id: Option<NodeId>,
    /// 绑定地址（None 时：`enable_ipv6` 为 true 用 "[::]" 双栈，否则 "0.0.0.0"）。
    pub bind_addr: Option<String>,
    /// 监听端口（0 = 系统分配）。
    pub listen_port: u16,
    /// 路由表持久化文件路径（None = 不持久化）。
    pub routing_table_file: Option<PathBuf>,
    /// 是否启用 IPv6（BEP 32）：绑定双栈 socket、bootstrap v6 节点、
    /// 解析/返回 nodes6 与 peers6。关闭时行为与纯 IPv4 完全一致。
    pub enable_ipv6: bool,
}

impl Default for DhtConfig {
    fn default() -> Self {
        Self {
            node_id: None,
            bind_addr: None,
            listen_port: DHT_PORT,
            routing_table_file: None,
            enable_ipv6: false,
        }
    }
}

/// get_peers 查询结果。
#[derive(Debug, Clone)]
pub struct GetPeersResult {
    /// 找到的 peer 地址列表。
    pub peers: Vec<SocketAddr>,
    /// 查询过程中访问的节点（用于后续 announce_peer）。
    pub announce_nodes: Vec<NodeEntry>,
}

/// known_peers 的 info_hash 总量上限——恶意节点对随机 info_hash 洪水
/// announce_peer 时，无上限的 HashMap 可被撑到 GB 级。触顶按插入顺序 FIFO
/// 淘汰（活跃下载的 info_hash 会被本端/对端继续 announce 刷新回来）。
const MAX_KNOWN_INFOHASHES: usize = 4096;
/// 每 info_hash 的 peer 地址上限（原有行为显式化为常量）。
const MAX_PEERS_PER_INFOHASH: usize = 100;
/// 接收处理任务并发上限。
const RECV_CONCURRENCY: usize = 256;
/// KRPC 查询超时。响应通常几十毫秒到达，3s 无响应即视为对端不可达
/// （原值 10s 会让不可达节点大幅拖慢 bootstrap / get_peers 迭代）。
const KRPC_QUERY_TIMEOUT: Duration = Duration::from_secs(3);

/// DHT 节点。
pub struct Dht {
    socket: Arc<UdpSocket>,
    our_id: NodeId,
    table: Arc<RwLock<RoutingTable>>,
    /// token 管理：本端对每个远端节点的 token（get_peers 响应附带）。
    /// token = SHA1(远端 ID || 本端 ID || salt)[:8]
    token_salt: [u8; 8],
    /// 已知 peer（info_hash → SocketAddr 集合）——本端下载或被 announce 告知。
    known_peers: Arc<Mutex<HashMap<InfoHash, Vec<SocketAddr>>>>,
    /// info_hash 的插入顺序（总量上限触顶时按 FIFO 淘汰最旧的
    /// info_hash，防恶意 announce 洪水把 known_peers 撑爆内存）。
    known_order: Arc<Mutex<VecDeque<InfoHash>>>,
    /// 收包处理任务并发上限（每 datagram spawn 一个任务原本无限制，
    /// 洪水攻击可瞬时堆出十万级任务；满载时直接丢包——UDP 无序可靠
    /// 语义，KRPC 查询自带 tid 重试）。
    recv_permits: Arc<Semaphore>,
    /// 待响应查询表：事务 id → 响应送达通道。
    ///
    /// 单一 socket 只有一个常驻接收者（`recv_loop`），查询方不能自行
    /// `recv_from`（两边竞争会把响应包抢走）。因此查询发出前在此登记，
    /// `recv_loop` 收到响应后按 tid 投递；没有这张表时响应包会经
    /// `parse_query` 解析失败被静默丢弃，所有查询必然超时。
    pending: Mutex<HashMap<[u8; 2], oneshot::Sender<Vec<u8>>>>,
    /// 路由表持久化文件。
    routing_table_file: Option<PathBuf>,
    /// 是否启用 IPv6（BEP 32）：决定 bootstrap 是否含 v6 节点、响应是否
    /// 附 nodes6。socket 已由绑定地址决定双栈能力。
    enable_ipv6: bool,
    /// socket 是否为 IPv6（双栈绑定）。发往 IPv4 目标前必须转成
    /// v4-mapped 地址，否则 libc::sendto 因地址族不匹配直接失败。
    v6_socket: bool,
    /// 取消令牌。
    cancel: CancellationToken,
}

impl Dht {
    /// 创建并绑定 UDP socket。
    ///
    /// `enable_ipv6` 且未显式指定 bind_addr 时绑定 "[::]"（双栈：同一
    /// socket 兼顾 IPv4-mapped 与 IPv6 报文），否则沿用 IPv4 "0.0.0.0"。
    pub async fn new(config: DhtConfig) -> Result<Arc<Self>, String> {
        let node_id = config.node_id.unwrap_or_else(NodeId::random);
        let default_host = if config.enable_ipv6 { "[::]" } else { "0.0.0.0" };
        let bind_host = config.bind_addr.as_deref().unwrap_or(default_host);
        let bind_addr = resolve_bind_addr(bind_host, config.listen_port).await?;
        let socket = UdpSocket::bind(bind_addr)
            .await
            .map_err(|e| format!("DHT UDP 绑定失败（{bind_host}）: {e}"))?;
        // 确保使用回环地址用于本地测试（避免 macOS No route to host）
        let local_addr = socket
            .local_addr()
            .map_err(|e| format!("获取本地地址失败: {e}"))?;
        tracing::info!(id = %node_id, addr = %local_addr, "DHT 节点已绑定");

        let mut salt = [0u8; 8];
        getrandom::fill(&mut salt).expect("系统随机源不可用");

        // 从持久化加载路由表
        let table = if let Some(path) = &config.routing_table_file {
            load_routing_table(path).unwrap_or_else(|| RoutingTable::new(node_id))
        } else {
            RoutingTable::new(node_id)
        };

        Ok(Arc::new(Self {
            socket: socket.into(),
            our_id: node_id,
            table: Arc::new(RwLock::new(table)),
            token_salt: salt,
            known_peers: Arc::new(Mutex::new(HashMap::new())),
            known_order: Arc::new(Mutex::new(VecDeque::new())),
            recv_permits: Arc::new(Semaphore::new(RECV_CONCURRENCY)),
            pending: Mutex::new(HashMap::new()),
            routing_table_file: config.routing_table_file,
            enable_ipv6: config.enable_ipv6,
            v6_socket: local_addr.is_ipv6(),
            cancel: CancellationToken::new(),
        }))
    }

    /// 发送目标地址转换：双栈（IPv6）socket 上发往 IPv4 目标必须先转成
    /// v4-mapped 形式（`::ffff:a.b.c.d`），否则 sendto 因地址族不匹配失败。
    fn wire_dst(&self, to: SocketAddr) -> SocketAddr {
        match (self.v6_socket, to) {
            (true, SocketAddr::V4(v4)) => {
                SocketAddr::new(IpAddr::V6(v4.ip().to_ipv6_mapped()), v4.port())
            }
            _ => to,
        }
    }

    /// 接收侧地址规范化：双栈 socket 上 IPv4 报文的源地址是 v4-mapped
    /// 形式，统一还原成 IPv4——路由表、known_peers、以及最终交给 BT
    /// 引擎去连接的 peer 地址都应是 v4。
    fn canonical_addr(addr: SocketAddr) -> SocketAddr {
        match addr {
            SocketAddr::V6(v6) => match v6.ip().to_ipv4_mapped() {
                Some(v4) => SocketAddr::new(IpAddr::V4(v4), v6.port()),
                None => addr,
            },
            v4 => v4,
        }
    }

    /// 发送 KRPC 报文（内部完成地址族转换）。
    async fn send_wire(&self, data: &[u8], to: SocketAddr) -> std::io::Result<usize> {
        self.socket.send_to(data, self.wire_dst(to)).await
    }

    /// 是否启用 IPv6（BEP 32）。
    pub fn enable_ipv6(&self) -> bool {
        self.enable_ipv6
    }

    pub fn our_id(&self) -> NodeId {
        self.our_id
    }

    pub fn local_addr(&self) -> Result<SocketAddr, String> {
        self.socket
            .local_addr()
            .map_err(|e| format!("获取本地地址失败: {e}"))
    }

    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// 启动后台任务：接收处理 + 定期保存路由表。
    pub fn spawn_background(self: &Arc<Self>) {
        let dht = self.clone();
        tokio::spawn(async move { dht.recv_loop().await });
        // 定期保存路由表（每 5 分钟）
        if self.routing_table_file.is_some() {
            let dht = self.clone();
            tokio::spawn(async move {
                let mut iv = tokio::time::interval(Duration::from_secs(300));
                loop {
                    iv.tick().await;
                    dht.save_routing_table();
                }
            });
        }
    }

    /// 关闭 DHT 节点。
    pub fn shutdown(&self) {
        self.cancel.cancel();
        self.save_routing_table();
    }

    /// 主接收循环：处理来自其他节点的查询。
    async fn recv_loop(self: Arc<Self>) {
        let mut buf = vec![0u8; 4096]; // KRPC 包通常 < 1500
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                r = self.socket.recv_from(&mut buf) => {
                    let (n, addr) = match r {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(error = %e, "DHT recv_from 失败");
                            continue;
                        }
                    };
                    // 双栈 socket 上 IPv4 报文源地址是 v4-mapped，统一还原
                    let addr = Self::canonical_addr(addr);
                    let data = buf[..n].to_vec();
                    // 响应包（y=r/e）按 tid 投递给等待中的查询。
                    // recv_loop 是 socket 的唯一接收者，查询方不能自行
                    // recv_from；若在这里落到 handle_incoming，会因
                    // parse_query 只认查询而静默丢弃 → 查询方必然超时。
                    if let Some(tid) = response_tid(&data) {
                        if let Some(tx) = self.pending.lock().await.remove(&tid) {
                            let _ = tx.send(data);
                            continue;
                        }
                    }
                    let dht = self.clone();
                    // 并发上限——洪水攻击下不无限制 spawn；满载直接丢包
                    //（UDP 无可靠语义，KRPC 查询自带 tid 重试机制）
                    let permits = dht.recv_permits.clone();
                    let Ok(permit) = permits.try_acquire_owned() else {
                        tracing::debug!(from = %addr, "DHT 处理任务已满载，丢弃数据包");
                        continue;
                    };
                    tokio::spawn(async move {
                        if let Err(e) = dht.handle_incoming(&data, addr).await {
                            tracing::debug!(from = %addr, error = %e, "DHT 消息处理失败");
                        }
                        drop(permit); // 任务结束归还许可
                    });
                }
            }
        }
    }

    /// 处理一条来自其他节点的消息。
    async fn handle_incoming(&self, data: &[u8], from: SocketAddr) -> Result<(), String> {
        let query = match parse_query(data) {
            Ok(q) => q,
            Err(_) => return Ok(()), // 忽略无法解析的消息
        };

        match query {
            IncomingQuery::Ping { tid, id } => {
                // 添加到路由表
                self.add_node(NodeEntry { id, addr: from }).await;
                // 回复我们的 ID
                let resp = encode_response(&tid, &self.our_id, &[], None);
                self.send_wire(&resp, from)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            IncomingQuery::FindNode { tid, id, target } => {
                self.add_node(NodeEntry { id, addr: from }).await;
                let table = self.table.read().await;
                let closest = table.closest(&target, K);
                let token = self.make_token(&id);
                let resp = self.encode_nodes_response(&tid, &closest, from, Some(&token));
                drop(table);
                let _ = self.send_wire(&resp, from).await;
            }
            IncomingQuery::GetPeers { tid, id, info_hash } => {
                self.add_node(NodeEntry { id, addr: from }).await;
                let token = self.make_token(&id);
                let peers = self
                    .known_peers
                    .lock()
                    .await
                    .get(&info_hash)
                    .cloned()
                    .unwrap_or_default();
                if !peers.is_empty() {
                    let peer_addrs: Vec<PeerAddr> =
                        peers.iter().map(|a| PeerAddr { addr: *a }).collect();
                    let resp = crate::krpc::encode_get_peers_response_with_peers(
                        &tid,
                        &self.our_id,
                        &peer_addrs,
                        &token,
                    );
                    let _ = self.send_wire(&resp, from).await;
                } else {
                    // 没有已知 peer → 返回最近的 K 个节点
                    let table = self.table.read().await;
                    let target_node = NodeId::from_info_hash(info_hash);
                    let closest = table.closest(&target_node, K);
                    let resp = self.encode_nodes_response(&tid, &closest, from, Some(&token));
                    drop(table);
                    let _ = self.send_wire(&resp, from).await;
                }
            }
            IncomingQuery::AnnouncePeer {
                tid,
                id,
                info_hash,
                port,
                implied_port,
                token,
            } => {
                // 校验 token
                let expected = self.make_token(&id);
                if token != expected {
                    let err = crate::krpc::encode_error(&tid, 203, "Invalid token");
                    let _ = self.send_wire(&err, from).await;
                    return Ok(());
                }
                // 确定宣告的端口
                let announce_port = if implied_port {
                    from.port()
                } else {
                    port.unwrap_or(from.port())
                };
                let announce_addr = SocketAddr::new(from.ip(), announce_port);
                self.add_node(NodeEntry { id, addr: from }).await;
                self.add_known_peer(info_hash, announce_addr).await;
                // 回复确认
                let resp = encode_response(&tid, &self.our_id, &[], None);
                let _ = self.send_wire(&resp, from).await;
            }
        }
        Ok(())
    }

    /// 编码节点响应：请求方是 IPv6 时按 BEP 32 附回 `nodes6`（IPv6 节点），
    /// IPv4 节点始终走 `nodes` 字段。
    fn encode_nodes_response(
        &self,
        tid: &[u8],
        closest: &[NodeEntry],
        from: SocketAddr,
        token: Option<&[u8]>,
    ) -> Vec<u8> {
        if !self.enable_ipv6 || !from.is_ipv6() {
            let v4: Vec<NodeEntry> = closest
                .iter()
                .filter(|n| n.addr.is_ipv4())
                .cloned()
                .collect();
            // 纯 v4 响应：v6 节点无处安放（对端未声明 v6 能力）
            return encode_response_v6(tid, &self.our_id, &v4, &[], token);
        }
        let v4: Vec<NodeEntry> = closest
            .iter()
            .filter(|n| n.addr.is_ipv4())
            .cloned()
            .collect();
        let v6: Vec<NodeEntry> = closest
            .iter()
            .filter(|n| n.addr.is_ipv6())
            .cloned()
            .collect();
        encode_response_v6(tid, &self.our_id, &v4, &v6, token)
    }

    /// 添加已知 peer（用于 announce_peer 与本端下载宣告）。
    async fn add_known_peer(&self, info_hash: InfoHash, addr: SocketAddr) {
        let mut map = self.known_peers.lock().await;
        // 新 info_hash 在插入前检查总量上限，触顶按插入顺序 FIFO 淘汰
        // 最旧的（活跃下载的 hash 会被后续 announce 重新插入并刷新）。
        if !map.contains_key(&info_hash) {
            let mut order = self.known_order.lock().await;
            while map.len() >= MAX_KNOWN_INFOHASHES {
                match order.pop_front() {
                    Some(old) => {
                        map.remove(&old);
                    }
                    // 队列与 map 失步（历史遗留）：兜底删除任意一个
                    None => {
                        let Some(victim) = map.keys().next().copied() else {
                            break;
                        };
                        map.remove(&victim);
                    }
                }
            }
            order.push_back(info_hash);
        }
        let entry = map.entry(info_hash).or_default();
        // 去重：同一 addr 重复 announce 不挤占配额
        if entry.contains(&addr) {
            return;
        }
        entry.push(addr);
        // 每 info_hash 内保留最多 MAX_PEERS_PER_INFOHASH 个 peer
        if entry.len() > MAX_PEERS_PER_INFOHASH {
            entry.drain(0..(entry.len() - MAX_PEERS_PER_INFOHASH));
        }
    }

    /// 添加节点到路由表。
    async fn add_node(&self, entry: NodeEntry) {
        let mut table = self.table.write().await;
        table.add(entry);
    }

    /// 生成 token（用于校验 announce_peer）。
    /// token = SHA1(远端 ID || 本端 ID || salt) 前 8 字节。
    fn make_token(&self, remote_id: &NodeId) -> Vec<u8> {
        let mut h = Sha1::new();
        h.update(remote_id.as_bytes());
        h.update(self.our_id.as_bytes());
        h.update(self.token_salt);
        let r: [u8; 20] = h.finalize().into();
        r[..8].to_vec()
    }

    // ------------------------------------------------------------------
    // Bootstrap
    // ------------------------------------------------------------------

    /// 启动 bootstrap：解析种子节点 → ping → find_node 自身 ID 填充路由表。
    ///
    /// `enable_ipv6` 为 true 时同时解析 BOOTSTRAP_V6（BEP 32），v6 查询
    /// 由双栈 socket 直接发出。
    pub async fn bootstrap(self: &Arc<Self>) -> Result<(), String> {
        let bootstrap_addrs = resolve_bootstrap(self.enable_ipv6).await;
        if bootstrap_addrs.is_empty() {
            tracing::warn!("无法解析任何 bootstrap 节点");
            return Ok(());
        }
        tracing::debug!(
            count = bootstrap_addrs.len(),
            ipv6 = self.enable_ipv6,
            "DHT bootstrap 节点已解析"
        );

        // 向每个 bootstrap 节点并发发送 find_node（查自身 ID）——
        // 串行时不可达节点各耗一个超时，冷启动会被拖到几十秒
        let target = self.our_id;
        let mut discovered: Vec<NodeEntry> = Vec::new();
        let mut set = tokio::task::JoinSet::new();
        for addr in &bootstrap_addrs {
            let dht = self.clone();
            let addr = *addr;
            set.spawn(async move { (addr, dht.find_node_query(addr, target).await) });
        }
        while let Some(res) = set.join_next().await {
            match res {
                Ok((_, Ok(nodes))) => discovered.extend(nodes),
                Ok((addr, Err(e))) => {
                    tracing::debug!(addr = %addr, error = %e, "bootstrap find_node 失败")
                }
                Err(e) => tracing::debug!(error = %e, "bootstrap find_node 任务失败"),
            }
        }

        // 添加发现的节点到路由表
        {
            let mut table = self.table.write().await;
            for n in &discovered {
                table.add(n.clone());
            }
        }

        // 向新发现的节点继续 find_node（一轮迭代，并发）
        let to_query: Vec<NodeEntry> = discovered.into_iter().take(K).collect();
        let mut set = tokio::task::JoinSet::new();
        for n in &to_query {
            let dht = self.clone();
            let addr = n.addr;
            set.spawn(async move { (addr, dht.find_node_query(addr, target).await) });
        }
        while let Some(res) = set.join_next().await {
            match res {
                Ok((_, Ok(nodes))) => {
                    let mut table = self.table.write().await;
                    for n in &nodes {
                        table.add(n.clone());
                    }
                }
                Ok((addr, Err(e))) => {
                    tracing::debug!(addr = %addr, error = %e, "bootstrap 迭代 find_node 失败")
                }
                Err(e) => tracing::debug!(error = %e, "bootstrap 迭代任务失败"),
            }
        }

        let table_len = self.table.read().await.len();
        tracing::info!(nodes = table_len, "DHT bootstrap 完成");
        Ok(())
    }

    // ------------------------------------------------------------------
    // get_peers 迭代查找
    // ------------------------------------------------------------------

    /// 迭代式 get_peers：从路由表最近节点开始，逐层向目标 info_hash 靠近。
    /// 返回找到的 peer 列表与访问过的节点（用于 announce_peer）。
    pub async fn get_peers(
        self: &Arc<Self>,
        info_hash: InfoHash,
    ) -> Result<GetPeersResult, String> {
        let target_node = NodeId::from_info_hash(info_hash);
        let mut visited: HashSet<NodeId> = HashSet::new();
        let mut all_peers: Vec<SocketAddr> = Vec::new();
        let mut announce_nodes: Vec<NodeEntry> = Vec::new();
        let mut all_known: Vec<NodeEntry>;

        // 初始候选：路由表最近 K 个
        {
            let table = self.table.read().await;
            all_known = table.closest(&target_node, K);
        }

        // 如果路由表为空，尝试 bootstrap
        if all_known.is_empty() {
            self.bootstrap().await?;
            let table = self.table.read().await;
            all_known = table.closest(&target_node, K);
        }

        // 迭代：向最近节点发 get_peers
        let mut iterations = 0;
        let max_iterations = 20; // 防止无限循环
        while iterations < max_iterations {
            iterations += 1;

            // 选出未访问的最近 K 个节点
            let to_query: Vec<NodeEntry> = all_known
                .iter()
                .filter(|n| !visited.contains(&n.id))
                .take(K)
                .cloned()
                .collect();
            if to_query.is_empty() {
                break;
            }

            let mut new_nodes: Vec<NodeEntry> = Vec::new();
            let mut found_peers = false;
            for n in &to_query {
                visited.insert(n.id);
                match self.get_peers_query(n.addr, info_hash).await {
                    Ok(resp) => {
                        announce_nodes.push(n.clone());
                        if let Some(peers) = resp.peers {
                            for p in &peers {
                                all_peers.push(p.addr);
                            }
                            found_peers = true;
                        } else {
                            // 继续：加入新发现的节点
                            for n2 in &resp.nodes {
                                new_nodes.push(n2.clone());
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!(addr = %n.addr, error = %e, "get_peers 查询失败");
                        // 标记坏节点
                        let mut table = self.table.write().await;
                        table.mark_bad(&n.id);
                    }
                }
            }

            if found_peers {
                // 找到 peers，停止迭代
                break;
            }

            if new_nodes.is_empty() {
                // 没有更多候选，停止
                break;
            }

            // 合并新旧候选，去重后按距离排序，取最近 K 个
            all_known.extend(new_nodes);
            all_known.sort_by_key(|n| n.id.xor(&target_node));
            all_known.dedup_by(|a, b| a.id == b.id);
            all_known.truncate(K);

            // 加入路由表
            let mut table = self.table.write().await;
            for n in &all_known {
                table.add(n.clone());
            }
        }

        // 去重 peer 列表
        all_peers.sort();
        all_peers.dedup();

        Ok(GetPeersResult {
            peers: all_peers,
            announce_nodes,
        })
    }

    /// 向找到的节点列表发送 announce_peer（宣告本端持有 info_hash）。
    pub async fn announce_peer(
        self: &Arc<Self>,
        info_hash: InfoHash,
        port: u16,
        announce_nodes: &[NodeEntry],
    ) -> Result<(), String> {
        let mut success = 0;
        for n in announce_nodes.iter().take(K) {
            // 需要先获取 token（通过 get_peers 查询时已获得，这里补查）
            let token = match self.get_peers_query(n.addr, info_hash).await {
                Ok(resp) => resp.token,
                Err(_) => continue,
            };
            let tid = gen_tid();
            let wire = encode_announce_peer(&tid, &self.our_id, &info_hash, port, &token, true);
            // 用 send_raw 确保 tid 匹配（与 recv_loop 共享 socket，
            // 响应可能被 recv_loop 消费；发送成功即视为宣告送达）
            match self.send_raw(&tid, &wire, n.addr).await {
                Ok(_) => success += 1,
                Err(KrpcError::Timeout) => {
                    // 超时仍算发送成功（fire-and-forget 语义）
                    success += 1;
                }
                Err(_) => {}
            }
        }
        tracing::info!(info_hash = %info_hash, port, announced = success, "DHT announce_peer 完成");
        // 同时记入本端已知 peer
        self.add_known_peer(info_hash, self.local_addr()?).await;
        Ok(())
    }

    // ------------------------------------------------------------------
    // KRPC 查询原语
    // ------------------------------------------------------------------

    /// 发送 find_node 查询并等待响应。
    async fn find_node_query(
        &self,
        addr: SocketAddr,
        target: NodeId,
    ) -> Result<Vec<NodeEntry>, KrpcError> {
        let tid = gen_tid();
        let wire = crate::krpc::encode_find_node(&tid, &self.our_id, &target);
        self.send_and_parse(&tid, &wire, addr)
            .await
            .map(|r| r.nodes)
    }

    /// 发送 get_peers 查询并等待响应。
    async fn get_peers_query(
        &self,
        addr: SocketAddr,
        info_hash: InfoHash,
    ) -> Result<crate::krpc::GetPeersResponse, KrpcError> {
        let tid = gen_tid();
        let wire = encode_get_peers(&tid, &self.our_id, &info_hash);
        let data = self.send_raw(&tid, &wire, addr).await?;

        // 添加响应者到路由表
        let resp = parse_get_peers_response(&data, &tid)?;
        {
            let mut table = self.table.write().await;
            table.add(NodeEntry {
                id: resp.responder_id,
                addr,
            });
        }
        Ok(resp)
    }

    /// 发送 ping 查询并等待响应。
    #[allow(dead_code)]
    async fn ping_query(&self, addr: SocketAddr) -> Result<NodeId, KrpcError> {
        let tid = gen_tid();
        let wire = encode_ping(&tid, &self.our_id);
        let r = self.send_and_parse(&tid, &wire, addr).await?;
        Ok(r.responder_id)
    }

    /// 发送请求 → 接收响应 → 解析（通用响应）。
    async fn send_and_parse(
        &self,
        tid: &[u8; 2],
        wire: &[u8],
        addr: SocketAddr,
    ) -> Result<crate::krpc::NodeResponse, KrpcError> {
        let data = self.send_raw(tid, wire, addr).await?;
        let resp = parse_response(&data, tid)?;
        // 添加响应者到路由表
        {
            let mut table = self.table.write().await;
            table.add(NodeEntry {
                id: resp.responder_id,
                addr,
            });
        }
        Ok(resp)
    }

    /// 底层发送-接收原语：发送 wire 到 addr，等待匹配 tid 的响应。
    ///
    /// 响应由 `recv_loop` 接收后经 `pending` 表按 tid 分发，本函数只
    /// 等待自己的 oneshot 通道（超时 3s）。发送失败/超时都会摘除登记项，
    /// 避免 pending 表泄漏。
    async fn send_raw(
        &self,
        tid: &[u8; 2],
        wire: &[u8],
        addr: SocketAddr,
    ) -> Result<Vec<u8>, KrpcError> {
        let (tx, rx) = oneshot::channel();
        // 先登记再发送——响应可能极快到达，晚登记会丢包
        self.pending.lock().await.insert(*tid, tx);
        if let Err(e) = self.send_wire(wire, addr).await {
            self.pending.lock().await.remove(tid);
            return Err(KrpcError::Network(e.to_string()));
        }
        match timeout(KRPC_QUERY_TIMEOUT, rx).await {
            Ok(Ok(data)) => Ok(data),
            Ok(Err(_)) => Err(KrpcError::Timeout),
            Err(_) => {
                self.pending.lock().await.remove(tid);
                Err(KrpcError::Timeout)
            }
        }
    }

    /// 保存路由表到文件。
    fn save_routing_table(&self) {
        let Some(path) = &self.routing_table_file else {
            return;
        };
        // 同步读取 + 序列化
        let table = self.table.blocking_read();
        let j = table.to_json();
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let tmp = path.with_extension("json.tmp");
        if serde_json::to_string_pretty(&j)
            .map_err(|e| e.to_string())
            .and_then(|text| {
                std::fs::write(&tmp, text).map_err(|e| e.to_string())?;
                std::fs::rename(&tmp, path).map_err(|e| e.to_string())
            })
            .is_err()
        {
            tracing::warn!("DHT 路由表保存失败");
        }
    }
}

// ----------------------------------------------------------------------
// 辅助函数
// ----------------------------------------------------------------------

/// 若数据包是 KRPC 响应（`y` 为 `r`/`e`）且含 2 字节事务 id，返回其 tid。
///
/// 响应不能交给 `handle_incoming`（只识别 `y=q` 查询，其余静默丢弃），
/// 必须由 `recv_loop` 借此函数识别后投递给对应的等待者。
fn response_tid(data: &[u8]) -> Option<[u8; 2]> {
    let v = xfer_bencode::decode(data).ok()?;
    let d = v.as_dict()?;
    match d.get(b"y".as_slice()).and_then(Value::as_str) {
        Some("r") | Some("e") => {}
        _ => return None,
    }
    let t = d.get(b"t".as_slice()).and_then(Value::as_bytes)?;
    if t.len() != 2 {
        return None;
    }
    Some([t[0], t[1]])
}

/// 解析 bootstrap 节点地址（DNS → SocketAddr）。
///
/// `enable_ipv6` 时一并解析 BOOTSTRAP_V6（IPv6 节点）；单族解析失败
/// 只记 debug，不影响另一族。两族都解析不到时退回已知 IPv4 兜底 IP。
async fn resolve_bootstrap(enable_ipv6: bool) -> Vec<SocketAddr> {
    let mut addrs = Vec::new();
    // IPv4 bootstrap
    for (host, port) in crate::BOOTSTRAP_V4 {
        if let Ok(addr) = tokio::net::lookup_host(format!("{host}:{port}")).await {
            for a in addr {
                if a.is_ipv4() {
                    addrs.push(a);
                }
            }
        }
    }
    // IPv6 bootstrap（DHT over IPv6）
    if enable_ipv6 {
        for (host, port) in crate::BOOTSTRAP_V6 {
            match tokio::net::lookup_host(format!("{host}:{port}")).await {
                Ok(iter) => {
                    for a in iter {
                        if a.is_ipv6() {
                            addrs.push(a);
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!(host = %host, error = %e, "IPv6 bootstrap 节点解析失败");
                }
            }
        }
    }
    // 全部失败：回退已知 IPv4 IP（避免 DNS 故障导致完全无法 bootstrap）
    if addrs.iter().all(|a| !a.is_ipv4()) {
        addrs.push("67.215.218.13:6881".parse().unwrap()); // dht.libtorrent.org 常用 IP
    }
    addrs
}

/// 解析绑定地址：优先按 IP 字面量（允许 "[::]" 这种带方括号写法），
/// 失败再走 DNS。
///
/// `UdpSocket::bind(("[::]", port))` 会把 "[::]" 原样交给 DNS 解析器，
/// macOS 报 `nodename nor servname provided` → IPv6 双栈绑定必然失败，
/// BEP 32（DHT over IPv6）整条链路不可用，只能回退纯 IPv4。
async fn resolve_bind_addr(host: &str, port: u16) -> Result<SocketAddr, String> {
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = bare.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| format!("DHT 绑定地址解析失败（{host}）: {e}"))?
        .next()
        .ok_or_else(|| format!("DHT 绑定地址无解析结果（{host}）"))
}

/// 从文件加载路由表。
fn load_routing_table(path: &std::path::Path) -> Option<RoutingTable> {
    let text = std::fs::read_to_string(path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    RoutingTable::from_json(&v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UdpSocket;

    /// 创建一个模拟 KRPC 节点（接收 ping/find_node/get_peers 并响应）。
    async fn spawn_mock_krpc_node(id: NodeId, port: u16) -> Arc<UdpSocket> {
        let socket = Arc::new(UdpSocket::bind(("127.0.0.1", port)).await.unwrap());
        let s = socket.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            while let Ok((n, from)) = s.recv_from(&mut buf).await {
                let data = &buf[..n];
                match parse_query(data) {
                    Ok(IncomingQuery::Ping { tid, .. }) => {
                        let resp = encode_response(&tid, &id, &[], None);
                        let _ = s.send_to(&resp, from).await;
                    }
                    Ok(IncomingQuery::FindNode { tid, .. }) => {
                        // 返回 2 个假节点
                        let nodes = vec![
                            NodeEntry {
                                id: NodeId::from_bytes(&[1; 20]),
                                addr: "127.0.0.1:7001".parse().unwrap(),
                            },
                            NodeEntry {
                                id: NodeId::from_bytes(&[2; 20]),
                                addr: "127.0.0.1:7002".parse().unwrap(),
                            },
                        ];
                        let resp = encode_response(&tid, &id, &nodes, None);
                        let _ = s.send_to(&resp, from).await;
                    }
                    Ok(IncomingQuery::GetPeers { tid, info_hash, .. }) => {
                        let token = b"mock_tok".to_vec();
                        // 返回一个假 peer
                        let peer = PeerAddr {
                            addr: "127.0.0.1:51413".parse().unwrap(),
                        };
                        let resp = crate::krpc::encode_get_peers_response_with_peers(
                            &tid,
                            &id,
                            &[peer],
                            &token,
                        );
                        let _ = s.send_to(&resp, from).await;
                        // 同时也返回 nodes
                        let _ = info_hash; // 避免 unused 警告
                    }
                    _ => {}
                }
            }
        });
        socket
    }

    #[tokio::test]
    async fn dht_ping_and_find_node() {
        let mock_id = node_id_from_seed(b"mock-node-1");
        let mock_socket = spawn_mock_krpc_node(mock_id, 0).await;
        let mock_addr = mock_socket.local_addr().unwrap();

        let dht = Dht::new(DhtConfig {
            node_id: Some(node_id_from_seed(b"test-dht")),
            listen_port: 0,
            bind_addr: Some("127.0.0.1".into()),
            routing_table_file: None,
            enable_ipv6: false,
        })
        .await
        .unwrap();
        // 查询依赖 recv_loop 分发响应，测试须显式启动
        dht.spawn_background();

        // find_node 查询
        let target = NodeId::random();
        let nodes = dht.find_node_query(mock_addr, target).await.unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].id, NodeId::from_bytes(&[1; 20]));
        assert_eq!(nodes[1].id, NodeId::from_bytes(&[2; 20]));

        // 响应者应被加入路由表（至少 1 个——响应者自身；
        // 返回的假节点可能被分到同一 bucket 导致部分被拒）
        let table = dht.table.read().await;
        assert!(!table.is_empty(), "路由表不应为空");
    }

    #[tokio::test]
    async fn dht_get_peers_returns_peers() {
        let mock_id = node_id_from_seed(b"mock-node-peers");
        let mock_socket = spawn_mock_krpc_node(mock_id, 0).await;
        let mock_addr = mock_socket.local_addr().unwrap();

        let dht = Dht::new(DhtConfig {
            node_id: Some(node_id_from_seed(b"test-dht-peers")),
            listen_port: 0,
            bind_addr: Some("127.0.0.1".into()),
            routing_table_file: None,
            enable_ipv6: false,
        })
        .await
        .unwrap();
        dht.spawn_background();

        let ih = InfoHash::from_bytes(&[0x42; 20]);
        let resp = dht.get_peers_query(mock_addr, ih).await.unwrap();
        assert!(resp.peers.is_some());
        assert_eq!(resp.peers.as_ref().unwrap()[0].addr.port(), 51413);
        assert_eq!(resp.token, b"mock_tok".to_vec());
    }

    #[tokio::test]
    async fn dht_iterative_get_peers() {
        // 设置 3 个 mock 节点
        let mut mock_addrs = Vec::new();
        for i in 0..3u8 {
            let mut seed = b"mock".to_vec();
            seed.push(i);
            let id = node_id_from_seed(&seed);
            let s = spawn_mock_krpc_node(id, 0).await;
            mock_addrs.push(s.local_addr().unwrap());
        }

        let dht = Dht::new(DhtConfig {
            node_id: Some(node_id_from_seed(b"test-iter")),
            listen_port: 0,
            bind_addr: Some("127.0.0.1".into()),
            routing_table_file: None,
            enable_ipv6: false,
        })
        .await
        .unwrap();
        dht.spawn_background();

        // 预填路由表
        {
            let mut table = dht.table.write().await;
            for (i, addr) in mock_addrs.iter().enumerate() {
                let mut seed = b"mock".to_vec();
                seed.push(i as u8);
                table.add(NodeEntry {
                    id: node_id_from_seed(&seed),
                    addr: *addr,
                });
            }
        }

        let ih = InfoHash::from_bytes(&[0x42; 20]);
        let result = dht.get_peers(ih).await.unwrap();
        assert!(!result.peers.is_empty());
        assert!(result.peers.contains(&"127.0.0.1:51413".parse().unwrap()));
    }

    #[tokio::test]
    async fn dht_responds_to_ping() {
        let dht = Dht::new(DhtConfig {
            node_id: Some(node_id_from_seed(b"responder")),
            listen_port: 0,
            bind_addr: Some("127.0.0.1".into()),
            routing_table_file: None,
            enable_ipv6: false,
        })
        .await
        .unwrap();
        dht.spawn_background();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let dht_addr = dht.local_addr().unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let tid = gen_tid();
        let our_id = NodeId::random();
        let wire = encode_ping(&tid, &our_id);
        client.send_to(&wire, dht_addr).await.unwrap();

        let mut buf = vec![0u8; 1024];
        let (n, _) = timeout(Duration::from_secs(2), client.recv_from(&mut buf))
            .await
            .expect("超时")
            .unwrap();
        let resp = parse_response(&buf[..n], &tid).unwrap();
        assert_eq!(resp.responder_id, dht.our_id());

        dht.shutdown();
    }

    #[tokio::test]
    async fn dht_token_roundtrip() {
        // 验证 announce_peer 的 token 校验
        let dht = Dht::new(DhtConfig {
            node_id: Some(node_id_from_seed(b"token-test")),
            listen_port: 0,
            bind_addr: Some("127.0.0.1".into()),
            routing_table_file: None,
            enable_ipv6: false,
        })
        .await
        .unwrap();
        dht.spawn_background();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let dht_addr = dht.local_addr().unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote_id = NodeId::random();

        // 1. get_peers 获取 token
        let ih = InfoHash::from_bytes(&[0x55; 20]);
        let tid = gen_tid();
        let wire = encode_get_peers(&tid, &remote_id, &ih);
        client.send_to(&wire, dht_addr).await.unwrap();

        let mut buf = vec![0u8; 2048];
        let (n, _) = timeout(Duration::from_secs(2), client.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let resp = parse_get_peers_response(&buf[..n], &tid).unwrap();
        assert!(!resp.token.is_empty());

        // 2. 用正确 token announce_peer
        let tid2 = gen_tid();
        let client_port = client.local_addr().unwrap().port();
        let wire = encode_announce_peer(&tid2, &remote_id, &ih, client_port, &resp.token, false);
        client.send_to(&wire, dht_addr).await.unwrap();

        let (n, _) = timeout(Duration::from_secs(2), client.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let resp2 = parse_response(&buf[..n], &tid2).unwrap();
        assert_eq!(resp2.responder_id, dht.our_id());

        // 3. 用错误 token announce_peer → 应收到错误
        let tid3 = gen_tid();
        let wire = encode_announce_peer(&tid3, &remote_id, &ih, client_port, b"wrong", false);
        client.send_to(&wire, dht_addr).await.unwrap();

        let (n, _) = timeout(Duration::from_secs(2), client.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        match parse_response(&buf[..n], &tid3) {
            Err(KrpcError::Remote { .. }) => {} // 预期错误
            _ => panic!("错误 token 应返回错误"),
        }

        dht.shutdown();
    }

    // ------------------------------------------------------------------
    // DHT over IPv6（BEP 32）
    // ------------------------------------------------------------------

    /// 模拟 IPv6 KRPC 节点：响应 ping / find_node（回 nodes6）/ get_peers
    /// （回 peers6）。绑定失败（机器无 IPv6 栈）返回 None，测试跳过。
    async fn spawn_mock_krpc_node_v6(id: NodeId) -> Option<Arc<UdpSocket>> {
        let socket = Arc::new(UdpSocket::bind(("[::1]", 0)).await.ok()?);
        let s = socket.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            while let Ok((n, from)) = s.recv_from(&mut buf).await {
                let data = &buf[..n];
                match parse_query(data) {
                    Ok(IncomingQuery::Ping { tid, .. }) => {
                        let resp = encode_response(&tid, &id, &[], None);
                        let _ = s.send_to(&resp, from).await;
                    }
                    Ok(IncomingQuery::FindNode { tid, .. }) => {
                        // 回 IPv6 节点（nodes6 字段）
                        let nodes6 = vec![NodeEntry {
                            id: NodeId::from_bytes(&[0xA1; 20]),
                            addr: "[::1]:7001".parse().unwrap(),
                        }];
                        let resp = encode_response_v6(&tid, &id, &[], &nodes6, None);
                        let _ = s.send_to(&resp, from).await;
                    }
                    Ok(IncomingQuery::GetPeers { tid, .. }) => {
                        // 回 peers6（18 字节 compact）
                        let mut c = Vec::new();
                        c.extend_from_slice(&std::net::Ipv6Addr::LOCALHOST.octets());
                        c.extend_from_slice(&51414u16.to_be_bytes());
                        let mut r = std::collections::BTreeMap::new();
                        r.insert(b"id".to_vec(), xfer_bencode::Value::Bytes(id.as_bytes().to_vec()));
                        r.insert(
                            b"token".to_vec(),
                            xfer_bencode::Value::Bytes(b"tok6".to_vec()),
                        );
                        r.insert(b"peers6".to_vec(), xfer_bencode::Value::Bytes(c));
                        let mut top = std::collections::BTreeMap::new();
                        top.insert(b"t".to_vec(), xfer_bencode::Value::Bytes(tid.to_vec()));
                        top.insert(b"y".to_vec(), xfer_bencode::Value::Bytes(b"r".to_vec()));
                        top.insert(b"r".to_vec(), xfer_bencode::Value::Dict(r));
                        let resp = xfer_bencode::encode(&xfer_bencode::Value::Dict(top));
                        let _ = s.send_to(&resp, from).await;
                    }
                    _ => {}
                }
            }
        });
        Some(socket)
    }

    #[tokio::test]
    async fn dht_ipv6_binds_and_queries_nodes6() {
        let mock_id = node_id_from_seed(b"mock-v6-node");
        let Some(mock) = spawn_mock_krpc_node_v6(mock_id).await else {
            // 环境无 IPv6：跳过（不算失败）
            return;
        };
        let mock_addr = mock.local_addr().unwrap();
        assert!(mock_addr.is_ipv6());

        // enable_ipv6 + 未显式指定 bind_addr → 默认绑双栈 "[::]"；
        // 这里为了不依赖全局 v6 地址，显式绑回环
        let dht = Dht::new(DhtConfig {
            node_id: Some(node_id_from_seed(b"test-dht-v6")),
            bind_addr: Some("[::1]".into()),
            listen_port: 0,
            routing_table_file: None,
            enable_ipv6: true,
        })
        .await
        .unwrap();
        dht.spawn_background();
        assert!(dht.local_addr().unwrap().is_ipv6());
        assert!(dht.enable_ipv6());

        // find_node：响应里的 nodes6 应被解析为 IPv6 节点
        let nodes = dht.find_node_query(mock_addr, NodeId::random()).await.unwrap();
        assert_eq!(nodes.len(), 1);
        assert!(nodes[0].addr.is_ipv6());
        assert_eq!(nodes[0].addr.port(), 7001);

        // get_peers：响应里的 peers6 应被解析为 IPv6 peer
        let ih = InfoHash::from_bytes(&[0x77; 20]);
        let resp = dht.get_peers_query(mock_addr, ih).await.unwrap();
        let peers = resp.peers.expect("应返回 peers6 解析出的 peer");
        assert_eq!(peers.len(), 1);
        assert!(peers[0].addr.is_ipv6());
        assert_eq!(peers[0].addr.port(), 51414);
        assert_eq!(resp.token, b"tok6".to_vec());

        dht.shutdown();
    }

    #[tokio::test]
    async fn dht_v6_requester_receives_nodes6() {
        let mock_id = node_id_from_seed(b"mock-v6-requester");
        let Some(mock) = spawn_mock_krpc_node_v6(mock_id).await else {
            return;
        };
        let _mock_addr = mock.local_addr().unwrap();

        let dht = Dht::new(DhtConfig {
            node_id: Some(node_id_from_seed(b"test-dht-v6-resp")),
            bind_addr: Some("[::1]".into()),
            listen_port: 0,
            routing_table_file: None,
            enable_ipv6: true,
        })
        .await
        .unwrap();
        dht.spawn_background();

        // 路由表里同时放 v4 与 v6 节点
        {
            let mut table = dht.table.write().await;
            table.add(NodeEntry {
                id: NodeId::from_bytes(&[1; 20]),
                addr: "127.0.0.1:7001".parse().unwrap(),
            });
            table.add(NodeEntry {
                id: NodeId::from_bytes(&[2; 20]),
                addr: "[::1]:7002".parse().unwrap(),
            });
        }

        // 模拟 IPv6 请求方发 find_node：应收到 nodes6（v6 节点）
        let client = UdpSocket::bind(("[::1]", 0)).await.unwrap();
        let client_id = node_id_from_seed(b"v6-client");
        let tid = gen_tid();
        let wire = crate::krpc::encode_find_node(&tid, &client_id, &NodeId::random());
        client.send_to(&wire, dht.local_addr().unwrap()).await.unwrap();
        let mut buf = vec![0u8; 4096];
        let (n, from) = tokio::time::timeout(Duration::from_secs(2), client.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(from, dht.local_addr().unwrap(), "回复应来自 DHT 自身");
        let resp = parse_response(&buf[..n], &tid).unwrap();
        assert!(
            resp.nodes.iter().any(|node| node.addr.is_ipv6()),
            "v6 请求方应收到 nodes6 中的 IPv6 节点"
        );

        dht.shutdown();
    }

    #[tokio::test]
    async fn dht_v4_only_requester_gets_no_v6_nodes() {
        let dht = Dht::new(DhtConfig {
            node_id: Some(node_id_from_seed(b"test-dht-v4-resp")),
            bind_addr: Some("127.0.0.1".into()),
            listen_port: 0,
            routing_table_file: None,
            enable_ipv6: true,
        })
        .await
        .unwrap();
        dht.spawn_background();
        {
            let mut table = dht.table.write().await;
            table.add(NodeEntry {
                id: NodeId::from_bytes(&[1; 20]),
                addr: "127.0.0.1:7001".parse().unwrap(),
            });
            table.add(NodeEntry {
                id: NodeId::from_bytes(&[2; 20]),
                addr: "[::1]:7002".parse().unwrap(),
            });
        }
        // IPv4 请求方：只回 nodes（v4），不塞 nodes6
        let client = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        let client_id = node_id_from_seed(b"v4-client");
        let tid = gen_tid();
        let wire = crate::krpc::encode_find_node(&tid, &client_id, &NodeId::random());
        client.send_to(&wire, dht.local_addr().unwrap()).await.unwrap();
        let mut buf = vec![0u8; 4096];
        let (n, from) = tokio::time::timeout(Duration::from_secs(2), client.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(from, dht.local_addr().unwrap(), "回复应来自 DHT 自身");
        let resp = parse_response(&buf[..n], &tid).unwrap();
        assert!(resp.nodes.iter().all(|node| node.addr.is_ipv4()));

        dht.shutdown();
    }
}
