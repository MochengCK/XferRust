//! K-bucket 路由表（BEP 5）。
//!
//! 160 个 bucket，每个最多 K=8 个节点。
//! 节点按与本端 ID 的 XOR 距离分配到 bucket。
//! 距离的前导零个数决定 bucket 索引（0..159）。

use std::collections::VecDeque;
use std::net::SocketAddr;

use crate::node_id::NodeId;
use crate::K;

/// 路由表中的节点条目。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeEntry {
    pub id: NodeId,
    pub addr: SocketAddr,
}

/// K-bucket 路由表。
pub struct RoutingTable {
    /// 本端节点 ID。
    our_id: NodeId,
    /// 160 个 bucket，每个最多 K 个节点。
    buckets: Vec<Bucket>,
}

/// 单个 K-bucket。
struct Bucket {
    /// 已确认活跃的节点（FIFO，最近活跃在尾部）。
    nodes: VecDeque<NodeEntry>,
    /// 待确认的替换候选（bucket 满时收到新节点加入此列）。
    replacements: VecDeque<NodeEntry>,
    /// 最近更新时间（用于 stale bucket 判断）。
    last_changed: std::time::Instant,
}

impl Bucket {
    fn new() -> Self {
        Self {
            nodes: VecDeque::with_capacity(K),
            replacements: VecDeque::with_capacity(K * 2),
            last_changed: std::time::Instant::now(),
        }
    }

    fn is_full(&self) -> bool {
        self.nodes.len() >= K
    }

    fn touch(&mut self) {
        self.last_changed = std::time::Instant::now();
    }
}

impl RoutingTable {
    pub fn new(our_id: NodeId) -> Self {
        let mut buckets = Vec::with_capacity(160);
        for _ in 0..160 {
            buckets.push(Bucket::new());
        }
        Self { our_id, buckets }
    }

    pub fn our_id(&self) -> NodeId {
        self.our_id
    }

    /// 计算 bucket 索引（0..159）。
    fn bucket_index(&self, id: &NodeId) -> usize {
        let xor = self.our_id.xor(id);
        let lz = NodeId::leading_zero_bits(&xor);
        // 前导零 160 → bucket 0（自身，不会出现在路由表）
        // 前导零 0 → bucket 159（距离最远）
        lz.min(159)
    }

    /// 尝试添加/更新节点。满时加入替换队列。
    pub fn add(&mut self, entry: NodeEntry) {
        if entry.id == self.our_id {
            return;
        }
        let idx = self.bucket_index(&entry.id);
        let bucket = &mut self.buckets[idx];

        // 已存在 → 更新（移到尾部 = 最近活跃）
        if let Some(pos) = bucket.nodes.iter().position(|n| n.id == entry.id) {
            if bucket.nodes[pos].addr != entry.addr {
                bucket.nodes[pos].addr = entry.addr;
            }
            let node = bucket.nodes.remove(pos).unwrap();
            bucket.nodes.push_back(node);
            bucket.touch();
            return;
        }

        if bucket.is_full() {
            // 已满 → 加入替换队列（不主动 ping，M3 首版简化）
            // 避免重复
            if !bucket.replacements.iter().any(|n| n.id == entry.id) {
                bucket.replacements.push_back(entry);
                if bucket.replacements.len() > K * 2 {
                    bucket.replacements.pop_front();
                }
            }
            return;
        }

        bucket.nodes.push_back(entry);
        bucket.touch();
    }

    /// 标记节点为坏（从路由表中移除，用替换队列填充）。
    pub fn mark_bad(&mut self, id: &NodeId) {
        let idx = self.bucket_index(id);
        let bucket = &mut self.buckets[idx];
        if let Some(pos) = bucket.nodes.iter().position(|n| &n.id == id) {
            bucket.nodes.remove(pos);
            // 从替换队列中取一个顶替
            if let Some(rep) = bucket.replacements.pop_front() {
                bucket.nodes.push_back(rep);
                bucket.touch();
            }
        }
    }

    /// 获取距离目标 info_hash 最近的 K 个节点（用于 find_node/get_peers 响应）。
    pub fn closest(&self, target: &NodeId, count: usize) -> Vec<NodeEntry> {
        let mut all: Vec<(NodeId, NodeEntry)> = Vec::new();
        for bucket in &self.buckets {
            for n in &bucket.nodes {
                all.push((n.id, n.clone()));
            }
        }
        // 按 XOR 距离排序（升序 = 越近越前）
        all.sort_by_key(|(id, _)| {
            let xor = id.xor(target);
            // 转为 [u8;20] 的自然字节序比较即可（大端 XOR 距离）
            xor
        });
        all.into_iter().take(count).map(|(_, e)| e).collect()
    }

    /// 全表节点数。
    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.nodes.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 全部节点列表（持久化/调试用）。
    pub fn all_nodes(&self) -> Vec<NodeEntry> {
        self.buckets
            .iter()
            .flat_map(|b| b.nodes.iter().cloned())
            .collect()
    }

    /// 序列化为 JSON（持久化用）。
    pub fn to_json(&self) -> serde_json::Value {
        let nodes: Vec<serde_json::Value> = self
            .all_nodes()
            .into_iter()
            .map(|n| {
                serde_json::json!({
                    "id": n.id.to_hex(),
                    "addr": n.addr.to_string(),
                })
            })
            .collect();
        serde_json::json!({
            "our_id": self.our_id.to_hex(),
            "nodes": nodes,
        })
    }

    /// 从 JSON 恢复（持久化加载用）。
    pub fn from_json(v: &serde_json::Value) -> Option<Self> {
        let our_hex = v["our_id"].as_str()?;
        let our_id = NodeId::from_hex(our_hex)?;
        let mut table = Self::new(our_id);
        if let Some(arr) = v["nodes"].as_array() {
            for n in arr {
                let id_hex = n["id"].as_str()?;
                let addr_s = n["addr"].as_str()?;
                let id = NodeId::from_hex(id_hex)?;
                let addr: SocketAddr = addr_s.parse().ok()?;
                table.add(NodeEntry { id, addr });
            }
        }
        Some(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_id::node_id_from_seed;

    fn entry(seed: &[u8], port: u16) -> NodeEntry {
        NodeEntry {
            id: node_id_from_seed(seed),
            addr: format!("127.0.0.1:{port}").parse().unwrap(),
        }
    }

    #[test]
    fn add_and_lookup() {
        let our = node_id_from_seed(b"our-node");
        let mut rt = RoutingTable::new(our);
        assert!(rt.is_empty());

        // 添加若干节点（用不同 seed 使它们分散在不同 bucket）
        for i in 0..20u16 {
            rt.add(entry(format!("node-{i}").as_bytes(), 1000 + i));
        }
        // 由于 K-bucket 容量限制(K=8)，部分节点可能被拒（加入替换队列但不计入 len）
        let added = rt.len();
        assert!(added > 0 && added <= 20, "应添加 1-20 个节点，实际 {added}");

        // 查找最近的 8 个
        let target = node_id_from_seed(b"target");
        let closest = rt.closest(&target, 8);
        assert_eq!(closest.len(), 8);

        // 最近的一定是距离最小的
        let dists: Vec<[u8; 20]> = closest.iter().map(|n| n.id.xor(&target)).collect();
        for i in 1..dists.len() {
            assert!(dists[i - 1] <= dists[i], "应按距离升序排列");
        }
    }

    #[test]
    fn bucket_capacity_limit() {
        let our = node_id_from_seed(b"cap-test");
        let mut rt = RoutingTable::new(our);

        // 构造多个与本端距离相同的节点（落入同一 bucket）
        // 通过修改同一 byte 使它们进入同一 bucket
        let mut base = our.0;
        for i in 0..(K + 5) {
            base[19] = i as u8; // 改变末字节 → 距离差异在末位
            let id = NodeId(base);
            rt.add(NodeEntry {
                id,
                addr: format!("10.0.0.{i}:6881").parse().unwrap(),
            });
        }
        // 该 bucket 最多 K 个
        let total = rt.len();
        assert_eq!(total, K, "bucket 容量限制为 K={K}");
    }

    #[test]
    fn duplicate_update_moves_to_tail() {
        let our = node_id_from_seed(b"dup-test");
        let mut rt = RoutingTable::new(our);
        let id = node_id_from_seed(b"node-1");
        rt.add(NodeEntry {
            id,
            addr: "127.0.0.1:1000".parse().unwrap(),
        });
        rt.add(NodeEntry {
            id: node_id_from_seed(b"node-2"),
            addr: "127.0.0.1:1001".parse().unwrap(),
        });
        // 重新添加 node-1（更新地址）
        rt.add(NodeEntry {
            id,
            addr: "127.0.0.1:2000".parse().unwrap(),
        });
        // 应更新地址，不增加数量
        assert_eq!(rt.len(), 2);
        // 找到更新后的地址
        let all = rt.all_nodes();
        let n = all.iter().find(|n| n.id == id).unwrap();
        assert_eq!(n.addr.port(), 2000);
    }

    #[test]
    fn json_roundtrip() {
        let our = node_id_from_seed(b"persist");
        let mut rt = RoutingTable::new(our);
        rt.add(entry(b"n1", 1001));
        rt.add(entry(b"n2", 1002));
        rt.add(entry(b"n3", 1003));

        let j = rt.to_json();
        let restored = RoutingTable::from_json(&j).unwrap();
        assert_eq!(restored.len(), 3);
        assert_eq!(restored.our_id(), our);
    }
}
