//! KRPC 协议（BEP 5）：bencode over UDP 的事务式 RPC。
//!
//! 四个核心方法：
//! - `ping`：检查节点存活，交换 ID；
//! - `find_node`：查找指定 ID 的节点（返回最近 K 个）；
//! - `get_peers`：查询某 info_hash 的 peer 列表（或返回最近节点继续迭代）；
//! - `announce_peer`：宣告本端拥有某 info_hash 的数据（需 get_peers 获得的 token）。
//!
//! 事务 id（`t`）由发起方生成，响应必须回带相同的 t。
//! 消息字段：`y=d`（请求/响应）、`y=q`（查询）、`y=e`（错误）。

use std::collections::BTreeMap;
use std::net::SocketAddr;

use xfer_bencode::{decode, encode, Value};
use xfer_types::InfoHash;

use crate::node_id::NodeId;
use crate::routing_table::NodeEntry;

/// KRPC 错误。
#[derive(Debug, thiserror::Error)]
pub enum KrpcError {
    #[error("bencode 解析失败: {0}")]
    Bencode(String),
    #[error("消息格式非法: {0}")]
    Format(String),
    #[error("KRPC 错误响应: code={code} msg={msg}")]
    Remote { code: i64, msg: String },
    #[error("超时")]
    Timeout,
    #[error("事务 id 不匹配")]
    TidMismatch,
    #[error("网络错误: {0}")]
    Network(String),
}

/// compact peer 地址：6 字节（4 IP + 2 端口 BE）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerAddr {
    pub addr: SocketAddr,
}

impl PeerAddr {
    /// 编码为 6 字节 compact 格式。
    pub fn encode_compact(&self) -> [u8; 6] {
        let mut out = [0u8; 6];
        if let std::net::IpAddr::V4(v4) = self.addr.ip() {
            out[0..4].copy_from_slice(&v4.octets());
        } else {
            // IPv6 peer 在 compact peers6 字段处理（此处返回 IPv4 表示）
            out[0..4].copy_from_slice(&[0; 4]);
        }
        let port = self.addr.port().to_be_bytes();
        out[4..6].copy_from_slice(&port);
        out
    }

    /// 从 6 字节 compact 解码（IPv4）。
    pub fn from_compact(data: &[u8]) -> Vec<PeerAddr> {
        let mut out = Vec::new();
        for c in data.chunks_exact(6) {
            let ip = [c[0], c[1], c[2], c[3]];
            let port = u16::from_be_bytes([c[4], c[5]]);
            if port == 0 {
                continue;
            }
            out.push(PeerAddr {
                addr: SocketAddr::from((ip, port)),
            });
        }
        out
    }

    /// 从 18 字节 compact 解码（IPv6，每 18 字节 = 16 IP + 2 端口）。
    pub fn from_compact6(data: &[u8]) -> Vec<PeerAddr> {
        let mut out = Vec::new();
        for c in data.chunks_exact(18) {
            let mut ip = [0u8; 16];
            ip.copy_from_slice(&c[0..16]);
            let port = u16::from_be_bytes([c[16], c[17]]);
            if port == 0 {
                continue;
            }
            out.push(PeerAddr {
                addr: SocketAddr::new(std::net::IpAddr::V6(std::net::Ipv6Addr::from(ip)), port),
            });
        }
        out
    }
}

/// compact 节点信息：26 字节（20 ID + 4 IP + 2 端口）。
pub fn encode_nodes_compact(nodes: &[NodeEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(nodes.len() * 26);
    for n in nodes {
        out.extend_from_slice(n.id.as_bytes());
        if let std::net::IpAddr::V4(v4) = n.addr.ip() {
            out.extend_from_slice(&v4.octets());
        } else {
            out.extend_from_slice(&[127, 0, 0, 1]); // 回退
        }
        out.extend_from_slice(&n.addr.port().to_be_bytes());
    }
    out
}

/// 从 compact nodes（26 字节/条）解码。
pub fn decode_nodes_compact(data: &[u8]) -> Vec<NodeEntry> {
    let mut out = Vec::new();
    for c in data.chunks_exact(26) {
        let mut id = [0u8; 20];
        id.copy_from_slice(&c[0..20]);
        let ip = [c[20], c[21], c[22], c[23]];
        let port = u16::from_be_bytes([c[24], c[25]]);
        if port == 0 {
            continue;
        }
        out.push(NodeEntry {
            id: NodeId::from_bytes(&id),
            addr: SocketAddr::from((ip, port)),
        });
    }
    out
}

/// 从 compact nodes6（38 字节/条 = 20 ID + 16 IP + 2 端口）解码。
#[allow(dead_code)]
pub fn decode_nodes6_compact(data: &[u8]) -> Vec<NodeEntry> {
    let mut out = Vec::new();
    for c in data.chunks_exact(38) {
        let mut id = [0u8; 20];
        id.copy_from_slice(&c[0..20]);
        let mut ip = [0u8; 16];
        ip.copy_from_slice(&c[20..36]);
        let port = u16::from_be_bytes([c[36], c[37]]);
        if port == 0 {
            continue;
        }
        out.push(NodeEntry {
            id: NodeId::from_bytes(&id),
            addr: SocketAddr::new(std::net::IpAddr::V6(std::net::Ipv6Addr::from(ip)), port),
        });
    }
    out
}

// ----------------------------------------------------------------------
// KRPC 消息编码
// ----------------------------------------------------------------------

/// 生成随机事务 id（2 字节）。
pub fn gen_tid() -> [u8; 2] {
    let mut buf = [0u8; 2];
    getrandom::fill(&mut buf).expect("系统随机源不可用");
    buf
}

/// 编码 ping 请求。
pub fn encode_ping(tid: &[u8; 2], our_id: &NodeId) -> Vec<u8> {
    let mut args = BTreeMap::new();
    args.insert(b"id".to_vec(), Value::Bytes(our_id.as_bytes().to_vec()));
    encode_query(tid, b"ping", args)
}

/// 编码 find_node 请求。
pub fn encode_find_node(tid: &[u8; 2], our_id: &NodeId, target: &NodeId) -> Vec<u8> {
    let mut args = BTreeMap::new();
    args.insert(b"id".to_vec(), Value::Bytes(our_id.as_bytes().to_vec()));
    args.insert(b"target".to_vec(), Value::Bytes(target.as_bytes().to_vec()));
    encode_query(tid, b"find_node", args)
}

/// 编码 get_peers 请求。
pub fn encode_get_peers(tid: &[u8; 2], our_id: &NodeId, info_hash: &InfoHash) -> Vec<u8> {
    let mut args = BTreeMap::new();
    args.insert(b"id".to_vec(), Value::Bytes(our_id.as_bytes().to_vec()));
    args.insert(
        b"info_hash".to_vec(),
        Value::Bytes(info_hash.as_bytes().to_vec()),
    );
    encode_query(tid, b"get_peers", args)
}

/// 编码 announce_peer 请求。
pub fn encode_announce_peer(
    tid: &[u8; 2],
    our_id: &NodeId,
    info_hash: &InfoHash,
    port: u16,
    token: &[u8],
    implied_port: bool,
) -> Vec<u8> {
    let mut args = BTreeMap::new();
    args.insert(b"id".to_vec(), Value::Bytes(our_id.as_bytes().to_vec()));
    args.insert(
        b"info_hash".to_vec(),
        Value::Bytes(info_hash.as_bytes().to_vec()),
    );
    // implied_port=true 时对端应从 UDP 源地址取端口（穿透 NAT）
    args.insert(
        b"implied_port".to_vec(),
        Value::Int(if implied_port { 1 } else { 0 }),
    );
    args.insert(b"port".to_vec(), Value::Int(port as i64));
    args.insert(b"token".to_vec(), Value::Bytes(token.to_vec()));
    encode_query(tid, b"announce_peer", args)
}

fn encode_query(tid: &[u8; 2], method: &[u8], args: BTreeMap<Vec<u8>, Value>) -> Vec<u8> {
    let mut top = BTreeMap::new();
    top.insert(b"t".to_vec(), Value::Bytes(tid.to_vec()));
    top.insert(b"y".to_vec(), Value::Bytes(b"q".to_vec()));
    top.insert(b"q".to_vec(), Value::Bytes(method.to_vec()));
    top.insert(b"a".to_vec(), Value::Dict(args));
    encode(&Value::Dict(top))
}

// ----------------------------------------------------------------------
// KRPC 响应解析
// ----------------------------------------------------------------------

/// get_peers 响应：或返回 peers，或返回 nodes（继续迭代）。
#[derive(Debug, Clone)]
pub struct GetPeersResponse {
    /// 对端节点 ID（响应中的 id 字段）。
    pub responder_id: NodeId,
    /// token（用于 announce_peer）。
    pub token: Vec<u8>,
    /// Some(peers) = 直接获得 peer 列表；None = 需继续迭代 nodes。
    pub peers: Option<Vec<PeerAddr>>,
    /// 最近节点列表（peers 为 None 时用于继续查找）。
    pub nodes: Vec<NodeEntry>,
}

/// find_node / ping 响应的通用解析结果。
#[derive(Debug, Clone)]
pub struct NodeResponse {
    /// 对端节点 ID。
    pub responder_id: NodeId,
    /// 返回的最近节点列表。
    pub nodes: Vec<NodeEntry>,
    /// 可选 token（get_peers 响应才有）。
    #[allow(dead_code)]
    pub token: Option<Vec<u8>>,
}

/// 解析 KRPC 响应消息（匹配给定 tid）。
pub fn parse_response(data: &[u8], expected_tid: &[u8; 2]) -> Result<NodeResponse, KrpcError> {
    let v = decode(data).map_err(|e| KrpcError::Bencode(e.to_string()))?;
    let d = v
        .as_dict()
        .ok_or_else(|| KrpcError::Format("响应顶层必须是字典".into()))?;

    // 校验事务 id
    let t = d
        .get(b"t".as_slice())
        .and_then(Value::as_bytes)
        .ok_or_else(|| KrpcError::Format("响应缺少 t 字段".into()))?;
    if t != expected_tid {
        return Err(KrpcError::TidMismatch);
    }

    // 检查 y 字段
    match d.get(b"y".as_slice()).and_then(Value::as_str) {
        Some("r") => {}
        Some("e") => {
            // 错误响应：e = [code, message]
            if let Some(err) = d.get(b"e".as_slice()).and_then(Value::as_list) {
                if err.len() >= 2 {
                    let code = err[0].as_int().unwrap_or(203);
                    let msg = err[1]
                        .as_str()
                        .or_else(|| {
                            err[1]
                                .as_bytes()
                                .map(|b| std::str::from_utf8(b).unwrap_or(""))
                        })
                        .unwrap_or("未知错误")
                        .to_string();
                    return Err(KrpcError::Remote { code, msg });
                }
            }
            return Err(KrpcError::Remote {
                code: 203,
                msg: "未知错误".into(),
            });
        }
        _ => return Err(KrpcError::Format("响应 y 字段非法".into())),
    }

    let r = d
        .get(b"r".as_slice())
        .and_then(Value::as_dict)
        .ok_or_else(|| KrpcError::Format("响应缺少 r 字段".into()))?;

    let id_bytes = r
        .get(b"id".as_slice())
        .and_then(Value::as_bytes)
        .ok_or_else(|| KrpcError::Format("响应缺少 id 字段".into()))?;
    if id_bytes.len() != 20 {
        return Err(KrpcError::Format("响应 id 长度非法".into()));
    }
    let mut id_arr = [0u8; 20];
    id_arr.copy_from_slice(id_bytes);
    let responder_id = NodeId::from_bytes(&id_arr);

    let token = r
        .get(b"token".as_slice())
        .and_then(Value::as_bytes)
        .map(|b| b.to_vec());

    let nodes = if let Some(n) = r.get(b"nodes".as_slice()).and_then(Value::as_bytes) {
        decode_nodes_compact(n)
    } else {
        Vec::new()
    };

    Ok(NodeResponse {
        responder_id,
        nodes,
        token,
    })
}

/// 解析 get_peers 专用的响应（同时检查 peers/nodes 分支）。
pub fn parse_get_peers_response(
    data: &[u8],
    expected_tid: &[u8; 2],
) -> Result<GetPeersResponse, KrpcError> {
    let v = decode(data).map_err(|e| KrpcError::Bencode(e.to_string()))?;
    let d = v
        .as_dict()
        .ok_or_else(|| KrpcError::Format("响应顶层必须是字典".into()))?;

    let t = d
        .get(b"t".as_slice())
        .and_then(Value::as_bytes)
        .ok_or_else(|| KrpcError::Format("响应缺少 t 字段".into()))?;
    if t != expected_tid {
        return Err(KrpcError::TidMismatch);
    }

    match d.get(b"y".as_slice()).and_then(Value::as_str) {
        Some("r") => {}
        Some("e") => {
            if let Some(err) = d.get(b"e".as_slice()).and_then(Value::as_list) {
                if err.len() >= 2 {
                    let code = err[0].as_int().unwrap_or(203);
                    let msg = err[1].as_str().unwrap_or("未知错误").to_string();
                    return Err(KrpcError::Remote { code, msg });
                }
            }
            return Err(KrpcError::Remote {
                code: 203,
                msg: "未知错误".into(),
            });
        }
        _ => return Err(KrpcError::Format("响应 y 字段非法".into())),
    }

    let r = d
        .get(b"r".as_slice())
        .and_then(Value::as_dict)
        .ok_or_else(|| KrpcError::Format("响应缺少 r 字段".into()))?;

    let id_bytes = r
        .get(b"id".as_slice())
        .and_then(Value::as_bytes)
        .ok_or_else(|| KrpcError::Format("响应缺少 id 字段".into()))?;
    if id_bytes.len() != 20 {
        return Err(KrpcError::Format("响应 id 长度非法".into()));
    }
    let mut id_arr = [0u8; 20];
    id_arr.copy_from_slice(id_bytes);
    let responder_id = NodeId::from_bytes(&id_arr);

    let token = r
        .get(b"token".as_slice())
        .and_then(Value::as_bytes)
        .map(|b| b.to_vec())
        .unwrap_or_default();

    // 检查 peers（value 或 bytes）
    let peers = match r.get(b"values".as_slice()) {
        Some(Value::List(items)) => {
            let mut peers = Vec::new();
            for it in items {
                if let Some(b) = it.as_bytes() {
                    // 6 字节 IPv4 或 18 字节 IPv6
                    if b.len() == 6 {
                        peers.extend_from_slice(&PeerAddr::from_compact(b));
                    } else if b.len() == 18 {
                        peers.extend_from_slice(&PeerAddr::from_compact6(b));
                    }
                }
            }
            Some(peers)
        }
        // 兼容部分实现：peers 字段
        Some(Value::Bytes(b)) if b.len() % 6 == 0 && !b.is_empty() => {
            Some(PeerAddr::from_compact(b))
        }
        _ => None,
    };

    let nodes = if let Some(n) = r.get(b"nodes".as_slice()).and_then(Value::as_bytes) {
        decode_nodes_compact(n)
    } else {
        Vec::new()
    };

    Ok(GetPeersResponse {
        responder_id,
        token,
        peers,
        nodes,
    })
}

/// 解析收到的查询消息（用于处理来自其他节点的请求）。
#[derive(Debug, Clone)]
pub enum IncomingQuery {
    Ping {
        tid: Vec<u8>,
        id: NodeId,
    },
    FindNode {
        tid: Vec<u8>,
        id: NodeId,
        target: NodeId,
    },
    GetPeers {
        tid: Vec<u8>,
        id: NodeId,
        info_hash: InfoHash,
    },
    AnnouncePeer {
        tid: Vec<u8>,
        id: NodeId,
        info_hash: InfoHash,
        port: Option<u16>,
        implied_port: bool,
        token: Vec<u8>,
    },
}

/// 解析来自其他节点的查询消息。
pub fn parse_query(data: &[u8]) -> Result<IncomingQuery, KrpcError> {
    let v = decode(data).map_err(|e| KrpcError::Bencode(e.to_string()))?;
    let d = v
        .as_dict()
        .ok_or_else(|| KrpcError::Format("消息顶层必须是字典".into()))?;

    let tid = d
        .get(b"t".as_slice())
        .and_then(Value::as_bytes)
        .ok_or_else(|| KrpcError::Format("消息缺少 t 字段".into()))?
        .to_vec();

    let y = d
        .get(b"y".as_slice())
        .and_then(Value::as_str)
        .ok_or_else(|| KrpcError::Format("消息缺少 y 字段".into()))?;

    if y != "q" {
        return Err(KrpcError::Format(format!(
            "仅处理查询消息(y=q)，收到 y={y}"
        )));
    }

    let q = d
        .get(b"q".as_slice())
        .and_then(Value::as_str)
        .ok_or_else(|| KrpcError::Format("消息缺少 q 字段".into()))?;

    let a = d
        .get(b"a".as_slice())
        .and_then(Value::as_dict)
        .ok_or_else(|| KrpcError::Format("消息缺少 a 字段".into()))?;

    let id_bytes = a
        .get(b"id".as_slice())
        .and_then(Value::as_bytes)
        .ok_or_else(|| KrpcError::Format("查询缺少 id 参数".into()))?;
    if id_bytes.len() != 20 {
        return Err(KrpcError::Format("查询 id 长度非法".into()));
    }
    let mut id_arr = [0u8; 20];
    id_arr.copy_from_slice(id_bytes);

    match q {
        "ping" => Ok(IncomingQuery::Ping {
            tid,
            id: NodeId::from_bytes(&id_arr),
        }),
        "find_node" => {
            let target_bytes = a
                .get(b"target".as_slice())
                .and_then(Value::as_bytes)
                .ok_or_else(|| KrpcError::Format("find_node 缺少 target 参数".into()))?;
            if target_bytes.len() != 20 {
                return Err(KrpcError::Format("find_node target 长度非法".into()));
            }
            let mut target_arr = [0u8; 20];
            target_arr.copy_from_slice(target_bytes);
            Ok(IncomingQuery::FindNode {
                tid,
                id: NodeId::from_bytes(&id_arr),
                target: NodeId::from_bytes(&target_arr),
            })
        }
        "get_peers" => {
            let ih_bytes = a
                .get(b"info_hash".as_slice())
                .and_then(Value::as_bytes)
                .ok_or_else(|| KrpcError::Format("get_peers 缺少 info_hash 参数".into()))?;
            if ih_bytes.len() != 20 {
                return Err(KrpcError::Format("get_peers info_hash 长度非法".into()));
            }
            let mut ih_arr = [0u8; 20];
            ih_arr.copy_from_slice(ih_bytes);
            Ok(IncomingQuery::GetPeers {
                tid,
                id: NodeId::from_bytes(&id_arr),
                info_hash: InfoHash::from_bytes(&ih_arr),
            })
        }
        "announce_peer" => {
            let ih_bytes = a
                .get(b"info_hash".as_slice())
                .and_then(Value::as_bytes)
                .ok_or_else(|| KrpcError::Format("announce_peer 缺少 info_hash 参数".into()))?;
            if ih_bytes.len() != 20 {
                return Err(KrpcError::Format("announce_peer info_hash 长度非法".into()));
            }
            let mut ih_arr = [0u8; 20];
            ih_arr.copy_from_slice(ih_bytes);
            let port = a
                .get(b"port".as_slice())
                .and_then(Value::as_int)
                .filter(|&p| p > 0 && p <= 65535)
                .map(|p| p as u16);
            let implied_port = a
                .get(b"implied_port".as_slice())
                .and_then(Value::as_int)
                .map(|n| n != 0)
                .unwrap_or(false);
            let token = a
                .get(b"token".as_slice())
                .and_then(Value::as_bytes)
                .map(|b| b.to_vec())
                .unwrap_or_default();
            Ok(IncomingQuery::AnnouncePeer {
                tid,
                id: NodeId::from_bytes(&id_arr),
                info_hash: InfoHash::from_bytes(&ih_arr),
                port,
                implied_port,
                token,
            })
        }
        _ => Err(KrpcError::Format(format!("未知查询方法: {q}"))),
    }
}

/// 编码 ping/find_node/get_peers 响应（用于回应其他节点）。
pub fn encode_response(
    tid: &[u8],
    our_id: &NodeId,
    nodes: &[NodeEntry],
    token: Option<&[u8]>,
) -> Vec<u8> {
    let mut r = BTreeMap::new();
    r.insert(b"id".to_vec(), Value::Bytes(our_id.as_bytes().to_vec()));
    if !nodes.is_empty() {
        r.insert(b"nodes".to_vec(), Value::Bytes(encode_nodes_compact(nodes)));
    }
    if let Some(t) = token {
        r.insert(b"token".to_vec(), Value::Bytes(t.to_vec()));
    }
    let mut top = BTreeMap::new();
    top.insert(b"t".to_vec(), Value::Bytes(tid.to_vec()));
    top.insert(b"y".to_vec(), Value::Bytes(b"r".to_vec()));
    top.insert(b"r".to_vec(), Value::Dict(r));
    encode(&Value::Dict(top))
}

/// 编码 get_peers 响应（带 peers）。
pub fn encode_get_peers_response_with_peers(
    tid: &[u8],
    our_id: &NodeId,
    peers: &[PeerAddr],
    token: &[u8],
) -> Vec<u8> {
    let mut r = BTreeMap::new();
    r.insert(b"id".to_vec(), Value::Bytes(our_id.as_bytes().to_vec()));
    let values: Vec<Value> = peers
        .iter()
        .map(|p| Value::Bytes(p.encode_compact().to_vec()))
        .collect();
    r.insert(b"values".to_vec(), Value::List(values));
    r.insert(b"token".to_vec(), Value::Bytes(token.to_vec()));
    let mut top = BTreeMap::new();
    top.insert(b"t".to_vec(), Value::Bytes(tid.to_vec()));
    top.insert(b"y".to_vec(), Value::Bytes(b"r".to_vec()));
    top.insert(b"r".to_vec(), Value::Dict(r));
    encode(&Value::Dict(top))
}

/// 编码错误响应。
pub fn encode_error(tid: &[u8], code: i64, msg: &str) -> Vec<u8> {
    let mut top = BTreeMap::new();
    top.insert(b"t".to_vec(), Value::Bytes(tid.to_vec()));
    top.insert(b"y".to_vec(), Value::Bytes(b"e".to_vec()));
    top.insert(
        b"e".to_vec(),
        Value::List(vec![
            Value::Int(code),
            Value::Bytes(msg.as_bytes().to_vec()),
        ]),
    );
    encode(&Value::Dict(top))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    fn test_node(seed: &str, port: u16) -> NodeEntry {
        NodeEntry {
            id: NodeId::from_hex(&format!("{:040x}", seed.len() as u64 * port as u64)).unwrap(),
            addr: format!("127.0.0.1:{port}").parse().unwrap(),
        }
    }

    #[test]
    fn ping_roundtrip() {
        let our = NodeId::random();
        let tid = gen_tid();
        let wire = encode_ping(&tid, &our);
        assert!(!wire.is_empty());

        // 模拟对端响应
        let mut r = BTreeMap::new();
        r.insert(b"id".to_vec(), Value::Bytes(our.as_bytes().to_vec()));
        let mut top = BTreeMap::new();
        top.insert(b"t".to_vec(), Value::Bytes(tid.to_vec()));
        top.insert(b"y".to_vec(), Value::Bytes(b"r".to_vec()));
        top.insert(b"r".to_vec(), Value::Dict(r));
        let resp = encode(&Value::Dict(top));

        let parsed = parse_response(&resp, &tid).unwrap();
        assert_eq!(parsed.responder_id, our);
    }

    #[test]
    fn compact_nodes_roundtrip() {
        let nodes: Vec<NodeEntry> = (0..8)
            .map(|i| {
                let mut id = [0u8; 20];
                id[0] = i;
                NodeEntry {
                    id: NodeId::from_bytes(&id),
                    addr: format!("10.0.0.{i}:6881").parse().unwrap(),
                }
            })
            .collect();

        let wire = encode_nodes_compact(&nodes);
        assert_eq!(wire.len(), 26 * 8);
        let decoded = decode_nodes_compact(&wire);
        assert_eq!(decoded.len(), 8);
        for (orig, dec) in nodes.iter().zip(&decoded) {
            assert_eq!(orig.id, dec.id);
            assert_eq!(orig.addr, dec.addr);
        }
    }

    #[test]
    fn compact_peers_decode() {
        // 127.0.0.1:6881
        let data = [127, 0, 0, 1, 0x1A, 0xE1];
        let peers = PeerAddr::from_compact(&data);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].addr, "127.0.0.1:6881".parse().unwrap());
    }

    #[test]
    fn tid_mismatch_detected() {
        let tid = gen_tid();
        let wrong = [tid[0] ^ 0xFF, tid[1] ^ 0xFF];
        let mut r = BTreeMap::new();
        r.insert(
            b"id".to_vec(),
            Value::Bytes(NodeId::random().as_bytes().to_vec()),
        );
        let mut top = BTreeMap::new();
        top.insert(b"t".to_vec(), Value::Bytes(tid.to_vec()));
        top.insert(b"y".to_vec(), Value::Bytes(b"r".to_vec()));
        top.insert(b"r".to_vec(), Value::Dict(r));
        let resp = encode(&Value::Dict(top));

        assert!(parse_response(&resp, &wrong).is_err());
    }

    #[test]
    fn get_peers_response_with_peers() {
        let tid = gen_tid();
        let our = NodeId::random();
        let peer = PeerAddr {
            addr: "192.168.1.1:51413".parse().unwrap(),
        };
        let token = b"tok123".to_vec();
        let resp =
            encode_get_peers_response_with_peers(&tid, &our, std::slice::from_ref(&peer), &token);
        let parsed = parse_get_peers_response(&resp, &tid).unwrap();
        assert_eq!(parsed.responder_id, our);
        assert_eq!(parsed.token, token);
        assert!(parsed.peers.is_some());
        assert_eq!(parsed.peers.as_ref().unwrap()[0].addr, peer.addr);
        assert!(parsed.nodes.is_empty());
    }

    #[test]
    fn get_peers_response_with_nodes() {
        let tid = gen_tid();
        let our = NodeId::random();
        let nodes: Vec<NodeEntry> = (0..3)
            .map(|i| {
                let mut id = [0u8; 20];
                id[0] = i + 1;
                NodeEntry {
                    id: NodeId::from_bytes(&id),
                    addr: format!("10.0.0.{i}:6881").parse().unwrap(),
                }
            })
            .collect();
        let token = b"tok".to_vec();
        let resp = encode_response(&tid, &our, &nodes, Some(&token));
        let parsed = parse_get_peers_response(&resp, &tid).unwrap();
        assert!(parsed.peers.is_none());
        assert_eq!(parsed.nodes.len(), 3);
        assert_eq!(parsed.token, token);
    }

    #[test]
    fn announce_peer_encoding() {
        let tid = gen_tid();
        let our = NodeId::random();
        let ih = InfoHash::from_bytes(&[0xAB; 20]);
        let token = b"tok".to_vec();
        let wire = encode_announce_peer(&tid, &our, &ih, 6881, &token, false);
        // 验证可被 parse_query 解析
        let parsed = parse_query(&wire).unwrap();
        match parsed {
            IncomingQuery::AnnouncePeer {
                info_hash, port, ..
            } => {
                assert_eq!(info_hash, ih);
                assert_eq!(port, Some(6881));
            }
            _ => panic!("应为 AnnouncePeer"),
        }
    }

    #[test]
    fn error_response_parsed() {
        let tid = gen_tid();
        let err = encode_error(&tid, 203, "Server Error");
        let result = parse_response(&err, &tid);
        match result {
            Err(KrpcError::Remote { code, msg }) => {
                assert_eq!(code, 203);
                assert_eq!(msg, "Server Error");
            }
            _ => panic!("应为 Remote 错误"),
        }
    }

    #[test]
    fn query_parsing_all_types() {
        let tid = gen_tid();
        let our = NodeId::random();
        let target = NodeId::random();
        let ih = InfoHash::from_bytes(&[0xCD; 20]);

        // ping
        let wire = encode_ping(&tid, &our);
        assert!(matches!(
            parse_query(&wire).unwrap(),
            IncomingQuery::Ping { .. }
        ));

        // find_node
        let wire = encode_find_node(&tid, &our, &target);
        match parse_query(&wire).unwrap() {
            IncomingQuery::FindNode { target: t, .. } => assert_eq!(t, target),
            _ => panic!("应为 FindNode"),
        }

        // get_peers
        let wire = encode_get_peers(&tid, &our, &ih);
        match parse_query(&wire).unwrap() {
            IncomingQuery::GetPeers { info_hash: h, .. } => assert_eq!(h, ih),
            _ => panic!("应为 GetPeers"),
        }
    }
}
