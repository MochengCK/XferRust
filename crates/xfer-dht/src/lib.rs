//! xfer-dht：Mainline DHT（BEP 5）。
//!
//! - `node_id`：节点 ID（160-bit，与 info_hash 同空间）；
//! - `routing_table`：K-bucket 路由表；
//! - `krpc`：KRPC 协议（bencode over UDP，ping/find_node/get_peers/announce_peer）；
//! - `dht`：DHT 节点主逻辑（bootstrap、get_peers、announce_peer、路由表持久化）。
//!
//! 关键正确性（§7 继承）：
//! - KRPC 事务 id 由本端生成，响应 t 字段必须匹配请求；
//! - 路由表按 XOR 距离组织 K-bucket（K=8）；
//! - get_peers 返回 token，announce_peer 须用同一节点的 token；
//! - bootstrap 节点：IPv4×4 + IPv6×3。

mod dht;
mod krpc;
mod node_id;
mod routing_table;

pub use dht::{Dht, DhtConfig, GetPeersResult};
pub use krpc::{KrpcError, PeerAddr};
pub use node_id::NodeId;
pub use routing_table::RoutingTable;

/// DHT 标准端口号。
pub const DHT_PORT: u16 = 6881;

/// K-bucket 容量（BEP 5 约定）。
pub const K: usize = 8;

/// Bootstrap 节点（IPv4）。
pub const BOOTSTRAP_V4: &[(&str, u16)] = &[
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("dht.libtorrent.org", 25401),
    ("dht.aelitis.com", 6881),
];

/// Bootstrap 节点（IPv6）。
pub const BOOTSTRAP_V6: &[(&str, u16)] = &[
    ("router.silotis.org", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.bittorrent.com", 6881),
];
