//! Peer Exchange（BEP 11）。
//!
//! PEX 通过 peer wire 扩展消息（ut_pex 消息 id）交换已连接的 peer 列表。
//!
//! 消息格式（bencode 字典）：
//! ```text
//! {
//!   "added": <compact peers (4+2 each)>,
//!   "added.f": <flags byte per peer>,
//!   "dropped": <compact peers (4+2 each)>,
//! }
//! ```
//!
//! 关键正确性（§7.3）：added.f 的 uTP 能力位是 **0x04**（不是 0x01）。
//! 0x01 = 加密支持位（BEP 10），0x02 = uTP/SeedEx，0x04 = uTP。
//! 各家实现差异大，但 uTP 位必须在 0x04。
//!
//! 扩展消息 ID（BEP 9/10 的 ut_pex 消息）：
//! - 握手时协商 ext id（通常 ut_pex → 1）；
//! - 定期（约每分钟）发送 PEX 消息。

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use xfer_bencode::{decode, encode, Value};
use xfer_types::PeerId;

/// PEX 消息扩展名称（握手中协商用）。
pub const PEX_EXT_NAME: &str = "ut_pex";

/// PEX peer 标志位。
pub mod flags {
    /// 0x01: 加密支持（BEP 10）。
    pub const ENCRYPTION: u8 = 0x01;
    /// 0x02: SeedEx / uTP 提示（部分实现）。
    pub const SEED_EX: u8 = 0x02;
    /// 0x04: uTP 支持（BEP 29）——关键正确性位。
    pub const UTP: u8 = 0x04;
    /// 0x08: 可连接（reachable）。
    pub const REACHABLE: u8 = 0x08;
}

/// PEX 消息中的单个 peer。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PexPeer {
    pub addr: SocketAddr,
    pub flags: u8,
}

/// PEX 消息（added + dropped）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PexMessage {
    pub added: Vec<PexPeer>,
    pub dropped: Vec<SocketAddr>,
}

impl PexMessage {
    /// 编码为 bencode 字典（用于 ext 消息 payload）。
    ///
    /// 注意：IPv6 peer 无法编码为 PEX v1 的 IPv4 compact 格式，会被跳过。
    /// `added` 和 `added.f` 的长度始终保持一致。
    pub fn encode(&self) -> Vec<u8> {
        let mut map = BTreeMap::new();

        // added: compact peers（跳过 IPv6，PEX v1 只支持 IPv4）
        let mut added_bytes = Vec::new();
        let mut added_f: Vec<u8> = Vec::new();
        for p in &self.added {
            if let Some(bytes) = compact_peer_bytes(&p.addr) {
                added_bytes.extend_from_slice(&bytes);
                added_f.push(p.flags);
            }
        }
        map.insert(b"added".to_vec(), Value::Bytes(added_bytes));
        map.insert(b"added.f".to_vec(), Value::Bytes(added_f));

        // dropped: compact peers（跳过 IPv6）
        let mut dropped_bytes = Vec::new();
        for addr in &self.dropped {
            if let Some(bytes) = compact_peer_bytes(addr) {
                dropped_bytes.extend_from_slice(&bytes);
            }
        }
        map.insert(b"dropped".to_vec(), Value::Bytes(dropped_bytes));

        encode(&Value::Dict(map))
    }

    /// 从 bencode 字典解码。
    pub fn decode(data: &[u8]) -> Result<Self, String> {
        let v = decode(data).map_err(|e| format!("PEX bencode 解析失败: {e}"))?;
        let d = v
            .as_dict()
            .ok_or_else(|| "PEX 消息必须是字典".to_string())?;

        let mut msg = PexMessage::default();

        // added + added.f
        if let Some(Value::Bytes(added)) = d.get(b"added".as_slice()) {
            let flags = d
                .get(b"added.f".as_slice())
                .and_then(Value::as_bytes)
                .unwrap_or(&[]);
            let peers = parse_compact_peers(added);
            for (i, addr) in peers.into_iter().enumerate() {
                let flag = flags.get(i).copied().unwrap_or(0);
                msg.added.push(PexPeer { addr, flags: flag });
            }
        }

        // dropped
        if let Some(Value::Bytes(dropped)) = d.get(b"dropped".as_slice()) {
            msg.dropped = parse_compact_peers(dropped);
        }

        Ok(msg)
    }
}

/// PEX 交换控制器：维护已添加/已删除的 peer 集合，
/// 生成增量 PEX 消息（首次发送全量，之后只发增量）。
pub struct PexExchange {
    /// 已在上一条 PEX 消息中通告的 peer 集合。
    sent: std::collections::HashSet<SocketAddr>,
}

impl PexExchange {
    pub fn new() -> Self {
        Self {
            sent: Default::default(),
        }
    }

    /// 根据当前已连接 peer 列表，生成增量 PEX 消息。
    ///
    /// - 新增的 peer → added（含 flags）
    /// - 断开的 peer → dropped
    /// - 未变的 peer → 不发
    pub fn generate_message(&mut self, current_peers: &[(SocketAddr, PeerId, u8)]) -> PexMessage {
        let current_set: std::collections::HashSet<SocketAddr> =
            current_peers.iter().map(|(a, _, _)| *a).collect();

        // added = 当前有但上一次没发的
        let added: Vec<PexPeer> = current_peers
            .iter()
            .filter(|(addr, _, _)| !self.sent.contains(addr))
            .map(|(addr, _, flags)| PexPeer {
                addr: *addr,
                flags: *flags,
            })
            .collect();

        // dropped = 上一次有但现在没有的
        let dropped: Vec<SocketAddr> = self
            .sent
            .iter()
            .filter(|addr| !current_set.contains(addr))
            .copied()
            .collect();

        // 更新已发送集合
        self.sent = current_set;

        PexMessage { added, dropped }
    }

    /// 处理收到的 PEX 消息，返回新增 peer 和被删除 peer。
    pub fn handle_message(&self, msg: &PexMessage) -> (Vec<PexPeer>, Vec<SocketAddr>) {
        let new_peers: Vec<PexPeer> = msg.added.to_vec();
        let dropped: Vec<SocketAddr> = msg.dropped.to_vec();
        (new_peers, dropped)
    }
}

impl Default for PexExchange {
    fn default() -> Self {
        Self::new()
    }
}

// ----------------------------------------------------------------------
// 辅助函数
// ----------------------------------------------------------------------

/// 将 SocketAddr 编码为 compact 格式（仅 IPv4，6 字节）。
///
/// PEX v1 (BEP 11) 只支持 IPv4 compact 格式。IPv6 peer 应使用
/// PEX v2 (BEP 16) 的 `added6` 字段，当前实现不支持。
/// 返回 `None` 表示该地址无法编码为 IPv4 compact 格式。
fn compact_peer_bytes(addr: &SocketAddr) -> Option<[u8; 6]> {
    let ip = match addr.ip() {
        IpAddr::V4(v4) => v4.octets(),
        IpAddr::V6(_) => return None, // PEX v1 只支持 IPv4
    };
    let port = addr.port().to_be_bytes();
    Some([ip[0], ip[1], ip[2], ip[3], port[0], port[1]])
}

/// 从 compact 格式解析 peer 列表（每 6 字节一个）。
fn parse_compact_peers(data: &[u8]) -> Vec<SocketAddr> {
    let mut peers = Vec::with_capacity(data.len() / 6);
    for chunk in data.chunks_exact(6) {
        let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        peers.push(SocketAddr::new(IpAddr::V4(ip), port));
    }
    peers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pex_message_encode_decode_roundtrip() {
        let msg = PexMessage {
            added: vec![
                PexPeer {
                    addr: "127.0.0.1:6881".parse().unwrap(),
                    flags: flags::UTP,
                },
                PexPeer {
                    addr: "10.0.0.1:51413".parse().unwrap(),
                    flags: flags::ENCRYPTION | flags::UTP,
                },
            ],
            dropped: vec!["192.168.1.1:8080".parse().unwrap()],
        };

        let wire = msg.encode();
        let decoded = PexMessage::decode(&wire).unwrap();

        assert_eq!(decoded, msg);
    }

    #[test]
    fn pex_message_empty_roundtrip() {
        let msg = PexMessage::default();
        let wire = msg.encode();
        let decoded = PexMessage::decode(&wire).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn pex_flags_utp_is_0x04() {
        // §7.3：uTP 位必须是 0x04，不是 0x01
        assert_eq!(flags::UTP, 0x04);
        assert_ne!(flags::UTP, flags::ENCRYPTION);
    }

    #[test]
    fn pex_exchange_incremental() {
        let mut pex = PexExchange::new();

        let peer1: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let peer2: SocketAddr = "10.0.0.1:6881".parse().unwrap();
        let peer3: SocketAddr = "10.0.0.2:6881".parse().unwrap();

        // 第一条消息：全量
        let msg1 = pex.generate_message(&[
            (peer1, PeerId([1; 20]), flags::UTP),
            (peer2, PeerId([2; 20]), flags::UTP),
        ]);
        assert_eq!(msg1.added.len(), 2);
        assert!(msg1.dropped.is_empty());

        // 第二条消息：新增 peer3、无删除
        let msg2 = pex.generate_message(&[
            (peer1, PeerId([1; 20]), flags::UTP),
            (peer2, PeerId([2; 20]), flags::UTP),
            (peer3, PeerId([3; 20]), flags::UTP),
        ]);
        assert_eq!(msg2.added.len(), 1);
        assert_eq!(msg2.added[0].addr, peer3);
        assert!(msg2.dropped.is_empty());

        // 第三条消息：peer2 断开
        let msg3 = pex.generate_message(&[
            (peer1, PeerId([1; 20]), flags::UTP),
            (peer3, PeerId([3; 20]), flags::UTP),
        ]);
        assert!(msg3.added.is_empty());
        assert_eq!(msg3.dropped.len(), 1);
        assert_eq!(msg3.dropped[0], peer2);
    }

    #[test]
    fn pex_handle_message() {
        let pex = PexExchange::new();
        let msg = PexMessage {
            added: vec![PexPeer {
                addr: "127.0.0.1:6881".parse().unwrap(),
                flags: flags::UTP,
            }],
            dropped: vec!["10.0.0.1:6881".parse().unwrap()],
        };
        let (new_peers, dropped) = pex.handle_message(&msg);
        assert_eq!(new_peers.len(), 1);
        assert_eq!(dropped.len(), 1);
    }

    #[test]
    fn pex_decode_without_flags() {
        // added.f 缺失时 flags 默认为 0
        let mut map = BTreeMap::new();
        let peer = compact_peer_bytes(&"127.0.0.1:6881".parse().unwrap()).unwrap();
        map.insert(b"added".to_vec(), Value::Bytes(peer.to_vec()));
        let wire = encode(&Value::Dict(map));

        let msg = PexMessage::decode(&wire).unwrap();
        assert_eq!(msg.added.len(), 1);
        assert_eq!(msg.added[0].flags, 0);
    }
}
