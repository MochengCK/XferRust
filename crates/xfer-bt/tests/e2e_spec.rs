//! 严格规范参考种子端到端测试（消除「双方都错」陷阱）。
//!
//! 背景：既有全部 e2e 测试的 seed 端复用 `xfer_bt::message` 的编解码器——
//! 若引擎与测试共享同一个编码错误，所有本地测试仍会通过，而真实网络
//! 表现为「有节点无速度」。本文件因此 **完全不复用** xfer-bt 的
//! `Message`/`PeerReader`：
//!
//! - [`spec`]：照 BEP 3/5/6/10 官方规范手写的 wire 编解码（含手算黄金向量），
//!   独立验证引擎发出的每一个字节；
//! - [`RefSeeder`]：模拟 libtorrent / qBittorrent 级别的严格客户端：
//!   字节级握手校验、16KiB 请求策略（超限拒绝/违规记录）、
//!   choked 期间忽略请求、协议违规立即断开并记录、BEP 10 扩展握手、
//!   AllowedFast、rechoke 轮换、keepalive；
//! - 生产形态配置：`adaptive = true`（生产默认，此前所有 e2e 均为 false）、
//!   被动连接（入站）路径——真实群中大量数据来自对端主动连入，
//!   此前从未被任何测试覆盖。
//!
//! 断言：下载完成 + 文件字节一致 + 违规日志为空。
//! 任一失败即直接暴露真实环境零速度的根因。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use xfer_bencode::{parse_torrent, TorrentMeta};
use xfer_bt::{TorrentConfig, TorrentEngine};
use xfer_types::PeerId;

const PIECE_LEN: usize = 256 * 1024;
const MAX_BLOCK: u32 = 16 * 1024;

fn sha1_of(b: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(b);
    h.finalize().into()
}

// ===========================================================================
// 独立手写 bencode 编码（.torrent 与 tracker 响应用；不经过 xfer-bencode）
// ===========================================================================

fn be_int(i: i64) -> Vec<u8> {
    format!("i{i}e").into_bytes()
}

fn be_bytes(b: &[u8]) -> Vec<u8> {
    let mut v = format!("{}:", b.len()).into_bytes();
    v.extend_from_slice(b);
    v
}

/// 字典：调用方保证键已按字节序排序（此处内部再排序一次兜底）。
fn be_dict(mut pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<u8> {
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let mut v = vec![b'd'];
    for (k, val) in pairs {
        v.extend_from_slice(&be_bytes(&k));
        v.extend_from_slice(&val);
    }
    v.push(b'e');
    v
}

/// 极简 bencode 整数取值（独立实现，刻意不复用 xfer-bencode 解码器，
/// 避免「双方都错」陷阱）。在字典字节中查找 `key` 对应的整数值。
fn be_get_int(dict: &[u8], key: &[u8]) -> Option<i64> {
    let pos = dict.windows(key.len()).position(|w| w == key)?;
    let rest = &dict[pos + key.len()..];
    if rest.first() != Some(&b'i') {
        return None;
    }
    let end = rest.iter().position(|&c| c == b'e')?;
    std::str::from_utf8(&rest[1..end]).ok()?.parse().ok()
}

/// info 字典原始字节（磁力场景作为 ut_metadata 元数据提供）。
fn build_info_dict(data: &[u8], piece_len: usize) -> Vec<u8> {
    let pieces: Vec<u8> = data.chunks(piece_len).flat_map(sha1_of).collect();
    be_dict(vec![
        (b"length".to_vec(), be_int(data.len() as i64)),
        (b"name".to_vec(), be_bytes(b"data.bin")),
        (b"piece length".to_vec(), be_int(piece_len as i64)),
        (b"pieces".to_vec(), be_bytes(&pieces)),
    ])
}

fn build_torrent(data: &[u8], piece_len: usize, tracker_url: &str) -> Vec<u8> {
    let info = build_info_dict(data, piece_len);
    be_dict(vec![
        (b"announce".to_vec(), be_bytes(tracker_url.as_bytes())),
        (b"info".to_vec(), info),
    ])
}

// ===========================================================================
// tracker（手写 bencode 响应）
// ===========================================================================

async fn tracker_announce(
    Query(_q): Query<HashMap<String, String>>,
    State(state): State<Arc<RwLock<Vec<SocketAddr>>>>,
) -> Response {
    let addrs = state.read().unwrap().clone();
    let mut peers = Vec::new();
    for addr in &addrs {
        if let std::net::IpAddr::V4(v4) = addr.ip() {
            peers.extend_from_slice(&v4.octets());
            peers.extend_from_slice(&addr.port().to_be_bytes());
        }
    }
    let resp = be_dict(vec![
        (b"interval".to_vec(), be_int(60)),
        (b"complete".to_vec(), be_int(addrs.len() as i64)),
        (b"peers".to_vec(), be_bytes(&peers)),
    ]);
    ([(header::CONTENT_TYPE, "text/plain")], resp).into_response()
}

async fn start_tracker() -> (SocketAddr, Arc<RwLock<Vec<SocketAddr>>>) {
    let state: Arc<RwLock<Vec<SocketAddr>>> = Arc::new(RwLock::new(Vec::new()));
    let app = Router::new()
        .route("/announce", get(tracker_announce))
        .with_state(state.clone());
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(l, app).await;
    });
    (addr, state)
}

// ===========================================================================
// spec_udp：照 BEP 15 官方规范手写的严格 UDP tracker（独立于 xfer-discovery）
// ===========================================================================

mod spec_udp {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tokio::net::UdpSocket;

    use super::SeedLog;

    pub const ACTION_CONNECT: u32 = 0;
    pub const ACTION_ANNOUNCE: u32 = 1;
    /// BEP 15 magic（connect 请求前 8 字节）。
    pub const PROTOCOL_ID: u64 = 0x41727101980;
    /// 本 tracker 签发的 connection_id（固定值，便于字节级校验）。
    pub const ISSUED_CONN_ID: u64 = 0x1234_5678_9ABC_DEF0;

    pub struct UdpTrackerStats {
        pub announces: AtomicU64,
    }

    /// 启动严格 BEP 15 UDP tracker，返回（监听地址，统计）。
    /// 字节级校验：magic、包长、connection_id、info_hash、event 取值、
    /// port 非零；任何不符 → VIOLATION 记录（真实 tracker 会直接无视/拒绝）。
    pub async fn start(
        info_hash: [u8; 20],
        peers: Vec<SocketAddr>,
        log: Arc<SeedLog>,
    ) -> (SocketAddr, Arc<UdpTrackerStats>) {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = sock.local_addr().unwrap();
        let stats = Arc::new(UdpTrackerStats {
            announces: AtomicU64::new(0),
        });
        let st = stats.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            loop {
                let (n, src) = match sock.recv_from(&mut buf).await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let pkt = &buf[..n];
                if n < 16 {
                    log.violation(src, format!("BEP15: 包过短 {n}<16"));
                    continue;
                }
                let header_id = u64::from_be_bytes(pkt[0..8].try_into().unwrap());
                let action = u32::from_be_bytes(pkt[8..12].try_into().unwrap());
                let tid = u32::from_be_bytes(pkt[12..16].try_into().unwrap());
                match action {
                    ACTION_CONNECT => {
                        if header_id != PROTOCOL_ID {
                            log.violation(
                                src,
                                format!(
                                    "BEP15 connect: magic 不符，期望 0x{PROTOCOL_ID:X}，实际 0x{header_id:X}"
                                ),
                            );
                            continue;
                        }
                        let mut resp = Vec::with_capacity(16);
                        resp.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
                        resp.extend_from_slice(&tid.to_be_bytes());
                        resp.extend_from_slice(&ISSUED_CONN_ID.to_be_bytes());
                        let _ = sock.send_to(&resp, src).await;
                        log.note(src, "connect OK");
                    }
                    ACTION_ANNOUNCE => {
                        if header_id != ISSUED_CONN_ID {
                            log.violation(
                                src,
                                format!(
                                    "BEP15 announce: connection_id 无效，期望 0x{:X}，实际 0x{header_id:X}",
                                    ISSUED_CONN_ID
                                ),
                            );
                            continue;
                        }
                        if n != 98 {
                            log.violation(src, format!("BEP15 announce: 包长 {n} != 98"));
                            continue;
                        }
                        if pkt[16..36] != info_hash {
                            log.violation(src, "BEP15 announce: info_hash 不符");
                            continue;
                        }
                        let event = u32::from_be_bytes(pkt[80..84].try_into().unwrap());
                        if event > 3 {
                            log.violation(src, format!("BEP15 announce: 非法 event {event}"));
                            continue;
                        }
                        let port = u16::from_be_bytes(pkt[96..98].try_into().unwrap());
                        if port == 0 {
                            log.violation(src, "BEP15 announce: port = 0");
                            continue;
                        }
                        st.announces.fetch_add(1, Ordering::Relaxed);
                        log.note(
                            src,
                            format!(
                                "announce OK (event={event}, port={port}, downloaded={}, left={})",
                                u64::from_be_bytes(pkt[56..64].try_into().unwrap()),
                                u64::from_be_bytes(pkt[64..72].try_into().unwrap()),
                            ),
                        );
                        let mut resp = Vec::with_capacity(20 + 6 * peers.len());
                        resp.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
                        resp.extend_from_slice(&tid.to_be_bytes());
                        resp.extend_from_slice(&60u32.to_be_bytes()); // interval
                        resp.extend_from_slice(&0u32.to_be_bytes()); // leechers
                        resp.extend_from_slice(&(peers.len() as u32).to_be_bytes()); // seeders
                        for p in &peers {
                            if let std::net::IpAddr::V4(v4) = p.ip() {
                                resp.extend_from_slice(&v4.octets());
                                resp.extend_from_slice(&p.port().to_be_bytes());
                            }
                        }
                        let _ = sock.send_to(&resp, src).await;
                    }
                    other => {
                        log.violation(src, format!("BEP15: 未知 action {other}"));
                    }
                }
            }
        });
        (addr, stats)
    }
}

// ===========================================================================
// spec：照官方规范手写的 peer wire 编解码（独立于 xfer-bt）
// ===========================================================================

mod spec {
    //! BEP 3 消息 id 0-8；BEP 5 Port=9；BEP 6：Suggest=0x0D、HaveAll=0x0E、
    //! HaveNone=0x0F、Reject=0x10、AllowedFast=0x11；BEP 10 Extended=0x14。

    pub const ID_CHOKE: u8 = 0;
    pub const ID_UNCHOKE: u8 = 1;
    pub const ID_INTERESTED: u8 = 2;
    pub const ID_NOT_INTERESTED: u8 = 3;
    pub const ID_HAVE: u8 = 4;
    pub const ID_BITFIELD: u8 = 5;
    pub const ID_REQUEST: u8 = 6;
    pub const ID_PIECE: u8 = 7;
    pub const ID_CANCEL: u8 = 8;
    pub const ID_PORT: u8 = 9;
    pub const ID_SUGGEST: u8 = 0x0D;
    pub const ID_HAVE_ALL: u8 = 0x0E;
    pub const ID_HAVE_NONE: u8 = 0x0F;
    pub const ID_REJECT: u8 = 0x10;
    pub const ID_ALLOWED_FAST: u8 = 0x11;
    pub const ID_EXTENDED: u8 = 0x14;

    /// 现代客户端能力位：BEP 10（reserved[5] bit 0x10）+ BEP 6（reserved[7]
    /// bit 0x04）+ BEP 5 DHT（reserved[7] bit 0x01）。
    pub const RESERVED_MODERN: [u8; 8] = [0, 0, 0, 0, 0, 0x10, 0, 0x05];
    /// 老式客户端：全零（无扩展）。
    pub const RESERVED_LEGACY: [u8; 8] = [0; 8];

    pub fn supports_fast(reserved: &[u8; 8]) -> bool {
        reserved[7] & 0x04 != 0
    }
    pub fn supports_ext(reserved: &[u8; 8]) -> bool {
        reserved[5] & 0x10 != 0
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum Msg {
        KeepAlive,
        Choke,
        Unchoke,
        Interested,
        NotInterested,
        Have(u32),
        Bitfield(Vec<u8>),
        Request { index: u32, begin: u32, length: u32 },
        Piece { index: u32, begin: u32, block: Vec<u8> },
        Cancel { index: u32, begin: u32, length: u32 },
        Port(u16),
        Suggest(u32),
        HaveAll,
        HaveNone,
        Reject { index: u32, begin: u32, length: u32 },
        AllowedFast(u32),
        Extended { ext_id: u8, body: Vec<u8> },
        Unknown { id: u8, payload: Vec<u8> },
    }

    // ---- 编码（seed → engine），全部按规范手写 ----

    fn frame(id: u8, payload: &[u8]) -> Vec<u8> {
        let len = (1 + payload.len()) as u32;
        let mut v = Vec::with_capacity(4 + 1 + payload.len());
        v.extend_from_slice(&len.to_be_bytes());
        v.push(id);
        v.extend_from_slice(payload);
        v
    }

    pub fn keepalive() -> Vec<u8> {
        vec![0, 0, 0, 0]
    }
    pub fn choke() -> Vec<u8> {
        frame(ID_CHOKE, &[])
    }
    pub fn unchoke() -> Vec<u8> {
        frame(ID_UNCHOKE, &[])
    }
    pub fn interested() -> Vec<u8> {
        frame(ID_INTERESTED, &[])
    }
    pub fn have(index: u32) -> Vec<u8> {
        frame(ID_HAVE, &index.to_be_bytes())
    }
    pub fn bitfield(bf: &[u8]) -> Vec<u8> {
        frame(ID_BITFIELD, bf)
    }
    pub fn piece(index: u32, begin: u32, block: &[u8]) -> Vec<u8> {
        let mut p = Vec::with_capacity(8 + block.len());
        p.extend_from_slice(&index.to_be_bytes());
        p.extend_from_slice(&begin.to_be_bytes());
        p.extend_from_slice(block);
        frame(ID_PIECE, &p)
    }
    pub fn reject(index: u32, begin: u32, length: u32) -> Vec<u8> {
        let mut p = Vec::with_capacity(12);
        p.extend_from_slice(&index.to_be_bytes());
        p.extend_from_slice(&begin.to_be_bytes());
        p.extend_from_slice(&length.to_be_bytes());
        frame(ID_REJECT, &p)
    }
    pub fn allowed_fast(index: u32) -> Vec<u8> {
        frame(ID_ALLOWED_FAST, &index.to_be_bytes())
    }
    pub fn port(port: u16) -> Vec<u8> {
        frame(ID_PORT, &port.to_be_bytes())
    }
    pub fn extended(ext_id: u8, body: &[u8]) -> Vec<u8> {
        let mut p = Vec::with_capacity(1 + body.len());
        p.push(ext_id);
        p.extend_from_slice(body);
        frame(ID_EXTENDED, &p)
    }

    pub fn handshake(info_hash: &[u8; 20], peer_id: &[u8; 20], reserved: &[u8; 8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(68);
        v.push(19);
        v.extend_from_slice(b"BitTorrent protocol");
        v.extend_from_slice(reserved);
        v.extend_from_slice(info_hash);
        v.extend_from_slice(peer_id);
        v
    }

    // ---- 解码（engine → seed），严格校验每个字段 ----

    /// 解析一个完整帧（含 4 字节长度前缀）。任何偏离规范 → Err(原因)。
    pub fn parse_frame(frame: &[u8]) -> Result<Msg, String> {
        if frame.len() < 4 {
            return Err(format!("帧短于长度前缀: {} 字节", frame.len()));
        }
        let len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
        if frame.len() != 4 + len {
            return Err(format!("帧长不一致: 声明 {len} 实际 {}", frame.len() - 4));
        }
        if len == 0 {
            return Ok(Msg::KeepAlive);
        }
        let id = frame[4];
        let p = &frame[5..];
        let need = |n: usize, what: &str| -> Result<(), String> {
            if p.len() != n {
                Err(format!("{what}(id={id}) payload 长度 {} != {n}", p.len()))
            } else {
                Ok(())
            }
        };
        let u32be = |off: usize| u32::from_be_bytes([p[off], p[off + 1], p[off + 2], p[off + 3]]);
        match id {
            ID_CHOKE => {
                need(0, "choke")?;
                Ok(Msg::Choke)
            }
            ID_UNCHOKE => {
                need(0, "unchoke")?;
                Ok(Msg::Unchoke)
            }
            ID_INTERESTED => {
                need(0, "interested")?;
                Ok(Msg::Interested)
            }
            ID_NOT_INTERESTED => {
                need(0, "not_interested")?;
                Ok(Msg::NotInterested)
            }
            ID_HAVE => {
                need(4, "have")?;
                Ok(Msg::Have(u32be(0)))
            }
            ID_BITFIELD => Ok(Msg::Bitfield(p.to_vec())),
            ID_REQUEST => {
                need(12, "request")?;
                Ok(Msg::Request {
                    index: u32be(0),
                    begin: u32be(4),
                    length: u32be(8),
                })
            }
            ID_PIECE => {
                if p.len() < 8 {
                    return Err(format!("piece payload 过短: {}", p.len()));
                }
                if p.len() == 8 {
                    return Err("piece 携带空数据块".into());
                }
                Ok(Msg::Piece {
                    index: u32be(0),
                    begin: u32be(4),
                    block: p[8..].to_vec(),
                })
            }
            ID_CANCEL => {
                need(12, "cancel")?;
                Ok(Msg::Cancel {
                    index: u32be(0),
                    begin: u32be(4),
                    length: u32be(8),
                })
            }
            ID_PORT => {
                need(2, "port")?;
                Ok(Msg::Port(u16::from_be_bytes([p[0], p[1]])))
            }
            ID_SUGGEST => {
                need(4, "suggest")?;
                Ok(Msg::Suggest(u32be(0)))
            }
            ID_HAVE_ALL => {
                need(0, "have_all")?;
                Ok(Msg::HaveAll)
            }
            ID_HAVE_NONE => {
                need(0, "have_none")?;
                Ok(Msg::HaveNone)
            }
            ID_REJECT => {
                need(12, "reject_request")?;
                Ok(Msg::Reject {
                    index: u32be(0),
                    begin: u32be(4),
                    length: u32be(8),
                })
            }
            ID_ALLOWED_FAST => {
                need(4, "allowed_fast")?;
                Ok(Msg::AllowedFast(u32be(0)))
            }
            ID_EXTENDED => {
                if p.is_empty() {
                    return Err("extended 消息缺少 ext_id".into());
                }
                Ok(Msg::Extended {
                    ext_id: p[0],
                    body: p[1..].to_vec(),
                })
            }
            other => Ok(Msg::Unknown {
                id: other,
                payload: p.to_vec(),
            }),
        }
    }
}

#[test]
fn spec_wire_golden_vectors() {
    // 握手布局（BEP 3）：pstrlen + pstr + 8 reserved + 20 info_hash + 20 peer_id
    let hs = spec::handshake(&[0xAB; 20], &[0xCD; 20], &spec::RESERVED_MODERN);
    assert_eq!(hs.len(), 68);
    assert_eq!(hs[0], 19);
    assert_eq!(&hs[1..20], b"BitTorrent protocol");
    assert_eq!(&hs[20..28], &[0, 0, 0, 0, 0, 0x10, 0, 0x05]);
    assert_eq!(&hs[28..48], &[0xAB; 20]);
    assert_eq!(&hs[48..68], &[0xCD; 20]);

    // 消息帧黄金向量
    assert_eq!(spec::keepalive(), [0, 0, 0, 0]);
    assert_eq!(spec::choke(), [0, 0, 0, 1, 0]);
    assert_eq!(spec::unchoke(), [0, 0, 0, 1, 1]);
    assert_eq!(spec::interested(), [0, 0, 0, 1, 2]);
    assert_eq!(spec::have(1), [0, 0, 0, 5, 4, 0, 0, 0, 1]);
    assert_eq!(
        spec::piece(1, 2, &[0xAB]),
        [0, 0, 0, 10, 7, 0, 0, 0, 1, 0, 0, 0, 2, 0xAB]
    );

    // 解析黄金向量（字面字节 → 消息）
    assert_eq!(
        spec::parse_frame(&[0, 0, 0, 1, 0x0E]).unwrap(),
        spec::Msg::HaveAll
    );
    assert_eq!(
        spec::parse_frame(&[0, 0, 0, 1, 0x0F]).unwrap(),
        spec::Msg::HaveNone
    );
    assert_eq!(
        spec::parse_frame(&[0, 0, 0, 5, 0x11, 0, 0, 0, 9]).unwrap(),
        spec::Msg::AllowedFast(9)
    );
    assert_eq!(
        spec::parse_frame(&[0, 0, 0, 3, 9, 0x1A, 0xE1]).unwrap(),
        spec::Msg::Port(6881)
    );

    // 解析黄金向量
    let req = [0, 0, 0, 13, 6, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4];
    assert_eq!(
        spec::parse_frame(&req).unwrap(),
        spec::Msg::Request {
            index: 2,
            begin: 3,
            length: 4
        }
    );
    // 长度非法的 request 必须被拒绝
    assert!(spec::parse_frame(&[0, 0, 0, 12, 6, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0]).is_err());
}

// ===========================================================================
// 帧读取器（手写增量读取，独立于 PeerReader）
// ===========================================================================

struct Framed {
    buf: Vec<u8>,
}

const MAX_FRAME: usize = 8 * 1024 * 1024;

impl Framed {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// 尝试取一个完整帧；数据不足返回 None，帧长超限返回 Err。
    fn try_take_frame(&mut self) -> Result<Option<Vec<u8>>, String> {
        if self.buf.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]])
            as usize;
        if len > MAX_FRAME {
            return Err(format!("帧长 {len} 超出上限 {MAX_FRAME}"));
        }
        if self.buf.len() < 4 + len {
            return Ok(None);
        }
        Ok(Some(self.buf.drain(..4 + len).collect()))
    }

    /// 取定长字节（握手用）。
    fn try_take_exact(&mut self, n: usize) -> Option<Vec<u8>> {
        if self.buf.len() < n {
            return None;
        }
        Some(self.buf.drain(..n).collect())
    }
}

async fn read_handshake(
    rd: &mut OwnedReadHalf,
    fr: &mut Framed,
) -> std::io::Result<Result<([u8; 8], [u8; 20], [u8; 20]), String>> {
    loop {
        if let Some(b) = fr.try_take_exact(68) {
            if b[0] != 19 {
                return Ok(Err(format!("握手 pstrlen={} != 19", b[0])));
            }
            if &b[1..20] != b"BitTorrent protocol" {
                return Ok(Err(format!("握手 pstr 非法: {:?}", &b[1..20])));
            }
            let mut reserved = [0u8; 8];
            reserved.copy_from_slice(&b[20..28]);
            let mut ih = [0u8; 20];
            ih.copy_from_slice(&b[28..48]);
            let mut pid = [0u8; 20];
            pid.copy_from_slice(&b[48..68]);
            return Ok(Ok((reserved, ih, pid)));
        }
        let mut tmp = [0u8; 4096];
        let n = rd.read(&mut tmp).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "握手阶段连接关闭",
            ));
        }
        fr.buf.extend_from_slice(&tmp[..n]);
    }
}

async fn next_msg(
    rd: &mut OwnedReadHalf,
    fr: &mut Framed,
) -> std::io::Result<Option<Result<spec::Msg, String>>> {
    loop {
        match fr.try_take_frame() {
            Ok(Some(frame)) => return Ok(Some(spec::parse_frame(&frame))),
            Ok(None) => {}
            Err(e) => return Ok(Some(Err(e))),
        }
        let mut tmp = [0u8; 65536];
        let n = rd.read(&mut tmp).await?;
        if n == 0 {
            return Ok(None);
        }
        fr.buf.extend_from_slice(&tmp[..n]);
    }
}

// ===========================================================================
// 违规日志与统计
// ===========================================================================

struct SeedLog {
    entries: Mutex<Vec<String>>,
}

impl SeedLog {
    fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }
    fn violation(&self, who: SocketAddr, msg: impl std::fmt::Display) {
        self.entries
            .lock()
            .unwrap()
            .push(format!("VIOLATION [{who}] {msg}"));
    }
    fn note(&self, who: SocketAddr, msg: impl std::fmt::Display) {
        self.entries.lock().unwrap().push(format!("NOTE [{who}] {msg}"));
    }
    fn violations(&self) -> Vec<String> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.starts_with("VIOLATION"))
            .cloned()
            .collect()
    }
    fn dump(&self) -> String {
        self.entries.lock().unwrap().join("\n")
    }
}

#[derive(Default)]
struct SeedStats {
    served_bytes: AtomicU64,
    requests_served: AtomicU64,
    requests_while_choked: AtomicU64,
    /// 引擎请求了本端不持有的片（选片无视 have 位图的诊断指标）。
    requests_not_had: AtomicU64,
}

// ===========================================================================
// RefSeeder：严格参考种子
// ===========================================================================

struct SeedCfg {
    label: &'static str,
    /// 本 seed 在握手中声明的能力位。
    reserved: [u8; 8],
    /// 收到 Interested 后延迟多久 Unchoke（真实客户端的典型门槛）。
    unchoke_delay: Duration,
    /// 每 N 秒 rechoke 一次（choke 2s 再恢复）：模拟 choking 算法轮换。
    rechoke_every: Option<Duration>,
    /// 连接建立后 N 秒断开（模拟掉线），仅首次连接生效。
    drop_after: Option<Duration>,
}

struct RefSeeder {
    cfg: SeedCfg,
    data: Arc<Vec<u8>>,
    piece_len: usize,
    /// 本片持有位图（部分种子场景：真实群中多数节点只有部分片）。
    have: Arc<Vec<bool>>,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    log: Arc<SeedLog>,
    stats: Arc<SeedStats>,
    dropped_once: AtomicBool,
    /// 磁力场景：本 seed 提供的元数据（info 字典原始字节，BEP 9 分片服务）。
    metadata: Option<Vec<u8>>,
    /// 本 seed 在扩展握手中为 ut_metadata 分配的 ext_id。
    /// 默认 2（与多数客户端巧合一致）；场景9 故意取 3，用于暴露
    /// 引擎「按对端 id 匹配回包」的 BEP 10 违规。
    ut_meta_id: u8,
}

impl RefSeeder {
    /// 监听并接受连接（引擎主动连入）。每连接独立任务（引擎可能并发连入）。
    async fn serve(self: Arc<Self>, listener: TcpListener) {
        loop {
            let Ok((stream, addr)) = listener.accept().await else {
                return;
            };
            let this = self.clone();
            tokio::spawn(async move {
                let session = this.clone_session();
                let _ = session.handle(stream, addr, false).await;
            });
        }
    }

    fn clone_session(&self) -> SeedSession<'_> {
        SeedSession {
            cfg: &self.cfg,
            data: &self.data,
            piece_len: self.piece_len,
            have: &self.have,
            info_hash: &self.info_hash,
            peer_id: &self.peer_id,
            log: &self.log,
            stats: &self.stats,
            dropped_once: &self.dropped_once,
            metadata: &self.metadata,
            ut_meta_id: self.ut_meta_id,
        }
    }
}

struct SeedSession<'a> {
    cfg: &'a SeedCfg,
    data: &'a Arc<Vec<u8>>,
    piece_len: usize,
    have: &'a Arc<Vec<bool>>,
    info_hash: &'a [u8; 20],
    peer_id: &'a [u8; 20],
    log: &'a Arc<SeedLog>,
    stats: &'a Arc<SeedStats>,
    dropped_once: &'a AtomicBool,
    metadata: &'a Option<Vec<u8>>,
    ut_meta_id: u8,
}

impl<'a> SeedSession<'a> {
    fn n_pieces(&self) -> u32 {
        self.data.len().div_ceil(self.piece_len) as u32
    }

    fn piece_len_of(&self, index: u32) -> usize {
        let total = self.data.len();
        let start = index as usize * self.piece_len;
        if start >= total {
            return 0;
        }
        (total - start).min(self.piece_len)
    }

    /// `we_initiate=true`：本端主动连接（引擎监听，测入站路径）。
    async fn handle(
        &self,
        stream: TcpStream,
        addr: SocketAddr,
        we_initiate: bool,
    ) -> std::io::Result<()> {
        let label = self.cfg.label;
        let (mut rd, wr) = stream.into_split();
        let wr = Arc::new(tokio::sync::Mutex::new(wr));
        let mut fr = Framed::new();

        // ---- 握手（BEP 3：发起方先发）----
        if we_initiate {
            wr.lock()
                .await
                .write_all(&spec::handshake(self.info_hash, self.peer_id, &self.cfg.reserved))
                .await?;
        }
        let (peer_reserved, peer_ih, _peer_pid) = match read_handshake(&mut rd, &mut fr).await? {
            Ok(v) => v,
            Err(e) => {
                self.log.violation(addr, format!("握手非法: {e}"));
                return Ok(());
            }
        };
        if &peer_ih != self.info_hash {
            self.log
                .violation(addr, "握手 info_hash 与种子不匹配（真实客户端会直接断开）");
            return Ok(());
        }
        if !we_initiate {
            wr.lock()
                .await
                .write_all(&spec::handshake(self.info_hash, self.peer_id, &self.cfg.reserved))
                .await?;
        }
        let peer_fast = spec::supports_fast(&peer_reserved);
        let peer_ext = spec::supports_ext(&peer_reserved);
        self.log.note(
            addr,
            format!("{label}: 握手成功 peer_fast={peer_fast} peer_ext={peer_ext}"),
        );

        // ---- 握后立即通告片集合（严格客户端行为，遵循 have 位图）----
        {
            let n = self.n_pieces();
            let all_have = self.have.iter().take(n as usize).all(|&h| h);
            let mut w = wr.lock().await;
            if spec::supports_fast(&self.cfg.reserved) && all_have {
                w.write_all(&[0, 0, 0, 1, spec::ID_HAVE_ALL]).await?;
            } else {
                // 部分持有或老式客户端：发 bitfield（BEP 3：高位在前，片 0 = 最高位）
                let mut bf = vec![0u8; n.div_ceil(8) as usize];
                for (i, &h) in self.have.iter().enumerate().take(n as usize) {
                    if h {
                        bf[i / 8] |= 0x80 >> (i % 8);
                    }
                }
                if !bf.is_empty() {
                    w.write_all(&spec::bitfield(&bf)).await?;
                }
            }
            // BEP 6：seeder 也会下发 AllowedFast 集合（给已持有的片 0）
            if spec::supports_fast(&self.cfg.reserved) && self.have.first().copied() == Some(true)
            {
                w.write_all(&spec::allowed_fast(0)).await?;
            }
            // BEP 5：声明 DHT 的客户端握后互发 Port 消息（引擎必须容忍且不断连）
            if self.cfg.reserved[7] & 0x01 != 0 {
                w.write_all(&spec::port(6881)).await?;
            }
        }

        // ---- 主消息循环 ----
        let mut unchoked = false;
        let mut interested = false;
        // 引擎为 ut_metadata 广告的 ext_id（从其扩展握手解析；BEP 10 回包必须用它）。
        let mut engine_ut_meta_id: u8 = 2;
        let mut rechoke_at = self
            .cfg
            .rechoke_every
            .map(|d| tokio::time::Instant::now() + d);
        let drop_at = self.cfg.drop_after.and_then(|d| {
            // 仅首个连接掉线
            if self.dropped_once.load(Ordering::Relaxed) {
                None
            } else {
                Some(tokio::time::Instant::now() + d)
            }
        });
        let mut keepalive_at = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut choked_req_count: u32 = 0;

        loop {
            let read_fut = next_msg(&mut rd, &mut fr);
            tokio::pin!(read_fut);

            tokio::select! {
                res = &mut read_fut => {
                    let Some(parsed) = res? else {
                        self.log.note(addr, format!("{label}: 对端关闭连接"));
                        return Ok(());
                    };
                    let msg = match parsed {
                        Ok(m) => m,
                        Err(e) => {
                            self.log.violation(addr, format!("消息帧违规: {e}"));
                            return Ok(());
                        }
                    };
                    match msg {
                        spec::Msg::KeepAlive => {}
                        spec::Msg::Interested => {
                            self.log.note(addr, format!("{label}: 收到 Interested"));
                            if !interested {
                                interested = true;
                                tokio::time::sleep(self.cfg.unchoke_delay).await;
                                wr.lock().await.write_all(&spec::unchoke()).await?;
                                unchoked = true;
                                self.log.note(addr, format!("{label}: 已 Unchoke"));
                            }
                        }
                        spec::Msg::NotInterested => {
                            self.log.note(addr, format!("{label}: 收到 NotInterested"));
                        }
                        spec::Msg::Request { index, begin, length } => {
                            if !unchoked {
                                // choked 期间：仅 AllowedFast 片（片 0）可服务，其余忽略
                                self.stats.requests_while_choked.fetch_add(1, Ordering::Relaxed);
                                choked_req_count += 1;
                                if choked_req_count > 64 {
                                    self.log.violation(addr, "choked 期间请求洪水（>64 条），真实客户端会断开");
                                    return Ok(());
                                }
                                if index == 0
                                    && spec::supports_fast(&self.cfg.reserved)
                                    && self.have.first().copied() == Some(true)
                                {
                                    self.serve_request(&wr, index, begin, length).await?;
                                } else {
                                    self.log.note(
                                        addr,
                                        format!("{label}: choked 期间忽略 request piece={index}"),
                                    );
                                }
                                continue;
                            }
                            // 部分种子：不持有该片 → Fast 对端回 Reject，老式对端静默忽略。
                            // 引擎若持续请求不持有的片（选片无视 have 位图），下载必然停滞。
                            if !self.have.get(index as usize).copied().unwrap_or(false) {
                                self.stats.requests_not_had.fetch_add(1, Ordering::Relaxed);
                                self.log.note(
                                    addr,
                                    format!("{label}: 拒绝不持有的片请求 piece={index}"),
                                );
                                if spec::supports_fast(&self.cfg.reserved) {
                                    wr.lock()
                                        .await
                                        .write_all(&spec::reject(index, begin, length))
                                        .await?;
                                }
                                continue;
                            }
                            if let Err(reason) = self.validate_request(index, begin, length) {
                                self.log.violation(
                                    addr,
                                    format!(
                                        "非法请求 piece={index} begin={begin} length={length}: {reason}"
                                    ),
                                );
                                if spec::supports_fast(&self.cfg.reserved) {
                                    wr.lock()
                                        .await
                                        .write_all(&spec::reject(index, begin, length))
                                        .await?;
                                }
                                continue;
                            }
                            self.serve_request(&wr, index, begin, length).await?;
                        }
                        spec::Msg::Have(_) | spec::Msg::Bitfield(_) | spec::Msg::Cancel { .. } => {
                            // 下载端上报，seeder 忽略
                        }
                        spec::Msg::Extended { ext_id, body } => {
                            if !spec::supports_ext(&self.cfg.reserved) {
                                self.log.violation(
                                    addr,
                                    "向未声明 BEP 10 的对端发送 Extended 消息（协议泄漏）",
                                );
                                return Ok(());
                            }
                            if ext_id == 0 {
                                self.log.note(
                                    addr,
                                    format!("{label}: 收到扩展握手 {} 字节", body.len()),
                                );
                                // 记录引擎为 ut_metadata 分配的 id（BEP 10：回包必须用它）
                                if let Some(id) = be_get_int(&body, b"ut_metadata") {
                                    engine_ut_meta_id = id as u8;
                                }
                                // 回复我方扩展握手：ut_metadata 用本 seed 分配的 id
                                let m = be_dict(vec![
                                    (b"ut_metadata".to_vec(), be_int(self.ut_meta_id as i64)),
                                    (b"ut_pex".to_vec(), be_int(1)),
                                ]);
                                let hs = be_dict(vec![
                                    (b"m".to_vec(), m),
                                    (b"v".to_vec(), be_bytes(b"spec-seed/1.0")),
                                ]);
                                wr.lock()
                                    .await
                                    .write_all(&spec::extended(0, &hs))
                                    .await?;
                            } else if ext_id == self.ut_meta_id {
                                // ut_metadata 请求（BEP 9 msg_type=0）
                                let msg_type = be_get_int(&body, b"msg_type").unwrap_or(-1);
                                let piece = be_get_int(&body, b"piece").unwrap_or(-1);
                                if msg_type != 0 || piece < 0 {
                                    self.log.violation(
                                        addr,
                                        format!(
                                            "ut_metadata 请求非法: msg_type={msg_type} piece={piece}"
                                        ),
                                    );
                                    continue;
                                }
                                let Some(md) = self.metadata.as_ref() else {
                                    // 不提供元数据：回 reject（msg_type=2）
                                    let d = be_dict(vec![
                                        (b"msg_type".to_vec(), be_int(2)),
                                        (b"piece".to_vec(), be_int(piece)),
                                    ]);
                                    wr.lock()
                                        .await
                                        .write_all(&spec::extended(engine_ut_meta_id, &d))
                                        .await?;
                                    continue;
                                };
                                let total = md.len();
                                let off = piece as usize * 16384;
                                if off >= total {
                                    self.log.violation(
                                        addr,
                                        format!("ut_metadata 请求越界 piece={piece}"),
                                    );
                                    continue;
                                }
                                let chunk = &md[off..(off + 16384).min(total)];
                                let mut payload = be_dict(vec![
                                    (b"msg_type".to_vec(), be_int(1)),
                                    (b"piece".to_vec(), be_int(piece)),
                                    (b"total_size".to_vec(), be_int(total as i64)),
                                ]);
                                payload.extend_from_slice(chunk);
                                // BEP 10：回包使用引擎广告的 ext_id
                                wr.lock()
                                    .await
                                    .write_all(&spec::extended(engine_ut_meta_id, &payload))
                                    .await?;
                                self.log.note(
                                    addr,
                                    format!(
                                        "{label}: 已服务 ut_metadata piece={piece}（{} 字节，回包 ext_id={engine_ut_meta_id}）",
                                        chunk.len()
                                    ),
                                );
                            } else {
                                self.log.note(
                                    addr,
                                    format!("{label}: 收到扩展消息 ext_id={ext_id}"),
                                );
                            }
                        }
                        spec::Msg::HaveAll
                        | spec::Msg::HaveNone
                        | spec::Msg::Suggest(_)
                        | spec::Msg::Reject { .. }
                        | spec::Msg::AllowedFast(_) => {
                            if !spec::supports_fast(&self.cfg.reserved) {
                                self.log.violation(
                                    addr,
                                    format!(
                                        "向未声明 BEP 6 的对端发送 Fast Extension 消息: {msg:?}"
                                    ),
                                );
                                return Ok(());
                            }
                            self.log.note(addr, format!("{label}: 收到 {msg:?}"));
                        }
                        spec::Msg::Port(p) => {
                            self.log.note(addr, format!("{label}: 收到 Port({p})"));
                        }
                        spec::Msg::Choke | spec::Msg::Unchoke => {
                            self.log.note(addr, format!("{label}: 收到 {msg:?}（下载端不应影响 seeder）"));
                        }
                        spec::Msg::Unknown { id, .. } => {
                            self.log.violation(addr, format!("未知消息 id=0x{id:02X}，真实客户端会断开"));
                            return Ok(());
                        }
                        spec::Msg::Piece { .. } => {
                            self.log.violation(addr, "下载端发送 Piece 消息（角色错乱）");
                            return Ok(());
                        }
                    }
                }
                _ = tokio::time::sleep_until(keepalive_at) => {
                    wr.lock().await.write_all(&spec::keepalive()).await?;
                    keepalive_at = tokio::time::Instant::now() + Duration::from_secs(30);
                }
                _ = async {
                    match rechoke_at {
                        Some(t) => tokio::time::sleep_until(t).await,
                        None => std::future::pending().await,
                    }
                }, if rechoke_at.is_some() && unchoked => {
                    // rechoke：choke 2 秒再恢复（真实 choking 算法轮换）。
                    // 期间 select 循环挂起，无请求会被处理，无需翻转 unchoked 标志。
                    wr.lock().await.write_all(&spec::choke()).await?;
                    self.log.note(addr, format!("{label}: rechoke（choke 2s）"));
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    wr.lock().await.write_all(&spec::unchoke()).await?;
                    self.log.note(addr, format!("{label}: rechoke 结束，已恢复 Unchoke"));
                    if let Some(d) = self.cfg.rechoke_every {
                        rechoke_at = Some(tokio::time::Instant::now() + d);
                    }
                }
                _ = async {
                    match drop_at {
                        Some(t) => tokio::time::sleep_until(t).await,
                        None => std::future::pending().await,
                    }
                }, if drop_at.is_some() => {
                    self.dropped_once.store(true, Ordering::Relaxed);
                    self.log.note(addr, format!("{label}: 模拟掉线，主动断开"));
                    return Ok(());
                }
            }
        }
    }

    /// 严格请求策略（libtorrent/Transmission 行为）：
    /// 16KiB 上限、16KiB 对齐（末块除外）、片内范围。
    fn validate_request(&self, index: u32, begin: u32, length: u32) -> Result<(), &'static str> {
        if index >= self.n_pieces() {
            return Err("piece 索引越界");
        }
        let plen = self.piece_len_of(index) as u32;
        if length == 0 {
            return Err("零长度请求");
        }
        if length > MAX_BLOCK {
            return Err("请求超过 16KiB（真实客户端拒绝/忽略，导致零速度）");
        }
        if begin % MAX_BLOCK != 0 {
            return Err("请求偏移未按 16KiB 对齐");
        }
        if begin >= plen {
            return Err("请求偏移超出片尾");
        }
        if begin.saturating_add(length) > plen {
            return Err("请求越过片尾");
        }
        if length != MAX_BLOCK && begin + length != plen {
            return Err("短块不在片尾（非法块划分）");
        }
        Ok(())
    }

    async fn serve_request(
        &self,
        wr: &Arc<tokio::sync::Mutex<OwnedWriteHalf>>,
        index: u32,
        begin: u32,
        length: u32,
    ) -> std::io::Result<()> {
        let off = index as usize * self.piece_len + begin as usize;
        let end = off + length as usize;
        if end > self.data.len() {
            return Ok(());
        }
        let block = &self.data[off..end];
        wr.lock()
            .await
            .write_all(&spec::piece(index, begin, block))
            .await?;
        self.stats.served_bytes.fetch_add(length as u64, Ordering::Relaxed);
        self.stats.requests_served.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

// ===========================================================================
// 引擎启动（生产形态：adaptive = true）
// ===========================================================================

async fn run_engine(
    meta: TorrentMeta,
    dir: &std::path::Path,
    adaptive: bool,
    listen_port: u16,
    timeout: Duration,
) -> Result<(), String> {
    // 与生产（xfer-engine manager.rs）一致：按 scheme 分流 udp:// 与 http(s)://
    let mut announce_urls = Vec::new();
    let mut udp_announce_urls = Vec::new();
    for url in meta
        .announce
        .iter()
        .cloned()
        .chain(meta.announce_list.iter().flat_map(|t| t.iter().cloned()))
    {
        if url.starts_with("udp://") {
            udp_announce_urls.push(url);
        } else {
            announce_urls.push(url);
        }
    }
    let cfg = TorrentConfig {
        dir: dir.to_path_buf(),
        peer_id: PeerId::azureus_prefix(&[7u8; 12]),
        listen_port,
        max_peers: 8,
        adaptive,
        numwant: 50,
        announce_urls,
        pipeline: 0,
        udp_announce_urls,
        enable_dht: false,
        dht_port: 0,
        encryption: xfer_bt::EncryptionMode::PlaintextOnly,
        bt_protocol: xfer_bt::BtProtocol::TcpOnly,
        download_limit: 0,
        upload_limit: 0,
        seed_mode: false,
        seed_duration: 0,
        selected_files: None,
    };
    let engine = TorrentEngine::new(meta, cfg).map_err(|e| e.to_string())?;
    tokio::time::timeout(timeout, engine.clone().run(CancellationToken::new()))
        .await
        .map_err(|_| "下载超时：严格规范种子下零速度（有节点无速度）".to_string())??;
    Ok(())
}

fn fresh_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("e2e-spec-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn make_seeder(
    data: Vec<u8>,
    piece_len: usize,
    have: Vec<bool>,
    info_hash: [u8; 20],
    cfg: SeedCfg,
) -> (Arc<RefSeeder>, Arc<SeedLog>, Arc<SeedStats>) {
    let log = Arc::new(SeedLog::new());
    let stats = Arc::new(SeedStats::default());
    let mut peer_id = [0u8; 20];
    peer_id[0..8].copy_from_slice(b"-SP0001-");
    let seeder = Arc::new(RefSeeder {
        cfg,
        data: Arc::new(data),
        piece_len,
        have: Arc::new(have),
        info_hash,
        peer_id,
        log: log.clone(),
        stats: stats.clone(),
        dropped_once: AtomicBool::new(false),
        metadata: None,
        ut_meta_id: 2,
    });
    (seeder, log, stats)
}

fn assert_no_violations(log: &SeedLog, ctx: &str) {
    let v = log.violations();
    assert!(
        v.is_empty(),
        "{ctx}：引擎对严格规范种子存在协议违规（真实环境会被断开/拒绝 → 零速度）:\n{}\n\n完整日志:\n{}",
        v.join("\n"),
        log.dump()
    );
}

async fn bind_random() -> (TcpListener, SocketAddr) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    (l, addr)
}

// ===========================================================================
// 场景 1：单个现代严格 seed（Fast + BEP10 + Interested 门槛），生产形态配置
// ===========================================================================

#[tokio::test]
async fn spec_modern_strict_seeder_full_download() {
    let dir = fresh_dir("modern");
    // 5 整片 + 非 16KiB 倍数尾部：覆盖末块短块策略
    let data: Vec<u8> = (0..(5 * PIECE_LEN + 12345))
        .map(|i| ((i * 251 + 37) % 256) as u8)
        .collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = build_torrent(&data, PIECE_LEN, &tracker_url);
    let meta = parse_torrent(&tb).unwrap();

    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];
    let (seeder, log, stats) = make_seeder(
        data.clone(),
        PIECE_LEN,
        vec![true; data.len().div_ceil(PIECE_LEN)],
        meta.info_hash,
        SeedCfg {
            label: "modern",
            reserved: spec::RESERVED_MODERN,
            unchoke_delay: Duration::from_millis(300),
            rechoke_every: None,
            drop_after: None,
        },
    );
    tokio::spawn(seeder.serve(sl));

    run_engine(meta, &dir, true, 0, Duration::from_secs(60))
        .await
        .unwrap_or_else(|e| panic!("现代严格种子下载失败: {e}\n种子端日志:\n{}", log.dump()));

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out.len(), data.len(), "下载长度不一致");
    assert_eq!(out, data, "下载内容与源数据不一致");
    assert_no_violations(&log, "场景1（现代严格 seed）");
    assert!(stats.requests_served.load(Ordering::Relaxed) > 0);
    let _ = std::fs::remove_dir_all(&dir);
}

// ===========================================================================
// 场景 2：4-seed 群 + rechoke 轮换 + 中途掉线 + 自适应调度（生产形态）
// ===========================================================================

#[tokio::test]
async fn spec_swarm_rechoke_dropout_adaptive() {
    let dir = fresh_dir("swarm");
    let data: Vec<u8> = (0..(8 * PIECE_LEN + 999))
        .map(|i| ((i * 131 + 11) % 256) as u8)
        .collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = build_torrent(&data, PIECE_LEN, &tracker_url);
    let meta = parse_torrent(&tb).unwrap();

    let mut addrs = Vec::new();
    let mut logs = Vec::new();
    for i in 0..4u8 {
        let (sl, saddr) = bind_random().await;
        addrs.push(saddr);
        let (seeder, log, _stats) = make_seeder(
            data.clone(),
            PIECE_LEN,
            vec![true; data.len().div_ceil(PIECE_LEN)],
            meta.info_hash,
            SeedCfg {
                label: "swarm",
                reserved: spec::RESERVED_MODERN,
                unchoke_delay: Duration::from_millis(150 + 100 * i as u64),
                rechoke_every: Some(Duration::from_secs(6)),
                // 首个 seed 在 8 秒后掉线一次
                drop_after: if i == 0 {
                    Some(Duration::from_secs(8))
                } else {
                    None
                },
            },
        );
        logs.push(log);
        tokio::spawn(seeder.serve(sl));
    }
    *seed_ref.write().unwrap() = addrs;

    run_engine(meta, &dir, true, 0, Duration::from_secs(90))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "群下载失败: {e}\n种子端日志:\n{}",
                logs.iter().map(|l| l.dump()).collect::<Vec<_>>().join("\n---\n")
            )
        });

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载内容与源数据不一致");
    for log in &logs {
        assert_no_violations(log, "场景2（群 + rechoke + 掉线）");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ===========================================================================
// 场景 3：入站连接（引擎监听，严格 seed 主动连入）——此前从未覆盖的路径
// ===========================================================================

#[tokio::test]
async fn spec_inbound_connection_download() {
    let dir = fresh_dir("inbound");
    let data: Vec<u8> = (0..(3 * PIECE_LEN + 777))
        .map(|i| ((i * 61 + 5) % 256) as u8)
        .collect();

    // tracker 返回空 peer 列表：下载只能靠入站连接完成
    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = build_torrent(&data, PIECE_LEN, &tracker_url);
    let meta = parse_torrent(&tb).unwrap();
    *seed_ref.write().unwrap() = Vec::new();

    // 选一个空闲端口给引擎监听
    let (probe, engine_addr) = bind_random().await;
    let engine_port = engine_addr.port();
    drop(probe);

    let (seeder, log, _stats) = make_seeder(
        data.clone(),
        PIECE_LEN,
        vec![true; data.len().div_ceil(PIECE_LEN)],
        meta.info_hash,
        SeedCfg {
            label: "inbound",
            reserved: spec::RESERVED_MODERN,
            unchoke_delay: Duration::from_millis(200),
            rechoke_every: None,
            drop_after: None,
        },
    );

    // seed 主动连接引擎监听端口（等引擎起来，最多重试 10 秒）
    let seeder2 = seeder.clone();
    tokio::spawn(async move {
        let mut connected = false;
        for _ in 0..50 {
            match TcpStream::connect(("127.0.0.1", engine_port)).await {
                Ok(stream) => {
                    let addr = stream.peer_addr().unwrap();
                    let _ = seeder2.clone_session().handle(stream, addr, true).await;
                    connected = true;
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        }
        if !connected {
            seeder2
                .log
                .violation("127.0.0.1:0".parse().unwrap(), format!("引擎监听端口 {engine_port} 始终无法连入"));
        }
    });

    run_engine(meta, &dir, true, engine_port, Duration::from_secs(60))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "入站连接下载失败: {e}（被动连接路径在真实网络承担大量数据流）\n种子端日志:\n{}",
                log.dump()
            )
        });

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载内容与源数据不一致");
    assert_no_violations(&log, "场景3（入站连接）");
    let _ = std::fs::remove_dir_all(&dir);
}

// ===========================================================================
// 场景 4：老式严格 seed（reserved 全零）——验证扩展消息零泄漏
// ===========================================================================

#[tokio::test]
async fn spec_legacy_seeder_no_extension_leak() {
    let dir = fresh_dir("legacy");
    let data: Vec<u8> = (0..(4 * PIECE_LEN + 4242))
        .map(|i| ((i * 97 + 13) % 256) as u8)
        .collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = build_torrent(&data, PIECE_LEN, &tracker_url);
    let meta = parse_torrent(&tb).unwrap();

    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];
    let (seeder, log, _stats) = make_seeder(
        data.clone(),
        PIECE_LEN,
        vec![true; data.len().div_ceil(PIECE_LEN)],
        meta.info_hash,
        SeedCfg {
            label: "legacy",
            reserved: spec::RESERVED_LEGACY,
            unchoke_delay: Duration::from_millis(300),
            rechoke_every: None,
            drop_after: None,
        },
    );
    tokio::spawn(seeder.serve(sl));

    run_engine(meta, &dir, true, 0, Duration::from_secs(60))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "老式严格种子下载失败: {e}（若因扩展消息泄漏被断开，见日志）\n种子端日志:\n{}",
                log.dump()
            )
        });

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载内容与源数据不一致");
    assert_no_violations(&log, "场景4（老式严格 seed，扩展零泄漏）");
    let _ = std::fs::remove_dir_all(&dir);
}

// ===========================================================================
// 场景 5：部分种子群（每节点只持有一半片）——真实群最常见形态。
// 引擎选片必须遵守各 peer 的 have 位图；向不持有者要片 = 永远等不到响应。
// ===========================================================================

#[tokio::test]
async fn spec_partial_swarm_have_aware_picking() {
    let dir = fresh_dir("partial");
    let data: Vec<u8> = (0..(8 * PIECE_LEN + 555))
        .map(|i| ((i * 173 + 29) % 256) as u8)
        .collect();
    let n_pieces = data.len().div_ceil(PIECE_LEN); // 9 片

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = build_torrent(&data, PIECE_LEN, &tracker_url);
    let meta = parse_torrent(&tb).unwrap();

    // seed A 持偶数片，seed B 持奇数片：任何单节点都无法独立完成下载
    let have_even: Vec<bool> = (0..n_pieces).map(|i| i % 2 == 0).collect();
    let have_odd: Vec<bool> = (0..n_pieces).map(|i| i % 2 == 1).collect();

    let mut addrs = Vec::new();
    let mut logs = Vec::new();
    let mut stats_all = Vec::new();
    for (idx, have) in [have_even, have_odd].into_iter().enumerate() {
        let (sl, saddr) = bind_random().await;
        addrs.push(saddr);
        let (seeder, log, stats) = make_seeder(
            data.clone(),
            PIECE_LEN,
            have,
            meta.info_hash,
            SeedCfg {
                label: "partial",
                reserved: spec::RESERVED_MODERN,
                unchoke_delay: Duration::from_millis(200),
                rechoke_every: None,
                drop_after: None,
            },
        );
        logs.push(log);
        stats_all.push(stats);
        let _ = idx;
        tokio::spawn(seeder.serve(sl));
    }
    *seed_ref.write().unwrap() = addrs;

    run_engine(meta, &dir, true, 0, Duration::from_secs(60))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "部分种子群下载失败: {e}（引擎可能向不持有该片的节点发请求 → 永远无响应）\n种子端日志:\n{}",
                logs.iter().map(|l| l.dump()).collect::<Vec<_>>().join("\n---\n")
            )
        });

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载内容与源数据不一致");
    for log in &logs {
        assert_no_violations(log, "场景5（部分种子群）");
    }
    // 两个节点都应有实际供块（否则说明只从一个节点拿到了所有片——不可能，位图互补）
    for (i, st) in stats_all.iter().enumerate() {
        assert!(
            st.requests_served.load(Ordering::Relaxed) > 0,
            "seed {i} 未服务任何请求（have 位图未被引擎正确利用）\n日志:\n{}",
            logs[i].dump()
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ===========================================================================
// 场景 6：1MiB 大片（64 个 16KiB 块/片）——生产 torrent 的典型片大小，
// 深度块流水线；测试既有的 256KB 片从未覆盖到的请求队列深度。
// ===========================================================================

#[tokio::test]
async fn spec_large_piece_deep_pipeline() {
    let dir = fresh_dir("largepiece");
    const LARGE_PIECE: usize = 1024 * 1024;
    // 3 整片 + 尾片：共 3*64 + 4 块
    let data: Vec<u8> = (0..(3 * LARGE_PIECE + 55555))
        .map(|i| ((i * 89 + 7) % 256) as u8)
        .collect();
    let n_pieces = data.len().div_ceil(LARGE_PIECE);

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = build_torrent(&data, LARGE_PIECE, &tracker_url);
    let meta = parse_torrent(&tb).unwrap();

    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];
    let (seeder, log, stats) = make_seeder(
        data.clone(),
        LARGE_PIECE,
        vec![true; n_pieces],
        meta.info_hash,
        SeedCfg {
            label: "largepiece",
            reserved: spec::RESERVED_MODERN,
            unchoke_delay: Duration::from_millis(200),
            rechoke_every: None,
            drop_after: None,
        },
    );
    tokio::spawn(seeder.serve(sl));

    run_engine(meta, &dir, true, 0, Duration::from_secs(60))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "1MiB 大片下载失败: {e}（深块流水线/末块策略问题）\n种子端日志:\n{}",
                log.dump()
            )
        });

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载内容与源数据不一致");
    assert_no_violations(&log, "场景6（1MiB 大片）");
    assert!(
        stats.requests_served.load(Ordering::Relaxed) >= 196,
        "请求块数异常：{}（预期 ≥196）",
        stats.requests_served.load(Ordering::Relaxed)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ===========================================================================
// 场景 7：纯 UDP tracker（BEP 15）下载——真实种子大量使用带 /announce 路径
// 后缀的 udp:// tracker URL。严格 tracker 逐字节校验 connect/announce。
// ===========================================================================

#[tokio::test]
async fn spec_udp_tracker_only_full_download() {
    let dir = fresh_dir("udp-only");
    let data: Vec<u8> = (0..(3 * PIECE_LEN + 7777))
        .map(|i| ((i * 97 + 5) % 256) as u8)
        .collect();

    // announce 位于 info 字典之外，占位构建先取 info_hash
    let tb0 = build_torrent(&data, PIECE_LEN, "udp://127.0.0.1:0/announce");
    let meta0 = parse_torrent(&tb0).unwrap();

    let tracker_log = Arc::new(SeedLog::new());
    let (sl, saddr) = bind_random().await;
    let (uaddr, udp_stats) =
        spec_udp::start(meta0.info_hash, vec![saddr], tracker_log.clone()).await;

    // 真实种子 URL 形态：带路径后缀
    let tracker_url = format!("udp://{uaddr}/announce");
    let tb = build_torrent(&data, PIECE_LEN, &tracker_url);
    let meta = parse_torrent(&tb).unwrap();
    assert_eq!(meta.info_hash, meta0.info_hash);

    let (seeder, seed_log, seed_stats) = make_seeder(
        data.clone(),
        PIECE_LEN,
        vec![true; data.len().div_ceil(PIECE_LEN)],
        meta.info_hash,
        SeedCfg {
            label: "udp",
            reserved: spec::RESERVED_MODERN,
            unchoke_delay: Duration::from_millis(200),
            rechoke_every: None,
            drop_after: None,
        },
    );
    tokio::spawn(seeder.serve(sl));

    run_engine(meta, &dir, true, 0, Duration::from_secs(60))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "纯 UDP tracker 下载失败: {e}\ntracker 日志:\n{}\n种子端日志:\n{}",
                tracker_log.dump(),
                seed_log.dump()
            )
        });

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载内容与源数据不一致");
    assert!(
        udp_stats.announces.load(Ordering::Relaxed) > 0,
        "UDP tracker 从未收到有效 announce（引擎未完成 BEP 15 流程或 URL 未被使用）\n日志:\n{}",
        tracker_log.dump()
    );
    assert_no_violations(&tracker_log, "场景7（BEP 15 UDP tracker）");
    assert_no_violations(&seed_log, "场景7（UDP tracker 下游 seed）");
    assert!(seed_stats.requests_served.load(Ordering::Relaxed) > 0);
    let _ = std::fs::remove_dir_all(&dir);
}

// ===========================================================================
// 场景 8：UDP tracker URL 为域名形态（udp://localhost:port/announce）。
// 真实 tracker 全是域名而非 IP 字面量；引擎必须做 DNS 解析，
// 否则全部 udp:// tracker 被静默跳过 → 零 peer → 零速度。
// ===========================================================================

#[tokio::test]
async fn spec_udp_tracker_hostname_url_download() {
    let dir = fresh_dir("udp-host");
    let data: Vec<u8> = (0..(2 * PIECE_LEN + 4321))
        .map(|i| ((i * 61 + 19) % 256) as u8)
        .collect();

    let tb0 = build_torrent(&data, PIECE_LEN, "udp://127.0.0.1:0/announce");
    let meta0 = parse_torrent(&tb0).unwrap();

    let tracker_log = Arc::new(SeedLog::new());
    let (sl, saddr) = bind_random().await;
    let (uaddr, udp_stats) =
        spec_udp::start(meta0.info_hash, vec![saddr], tracker_log.clone()).await;

    // 域名形态（localhost → 127.0.0.1）
    let tracker_url = format!("udp://localhost:{}/announce", uaddr.port());
    let tb = build_torrent(&data, PIECE_LEN, &tracker_url);
    let meta = parse_torrent(&tb).unwrap();
    assert_eq!(meta.info_hash, meta0.info_hash);

    let (seeder, seed_log, _seed_stats) = make_seeder(
        data.clone(),
        PIECE_LEN,
        vec![true; data.len().div_ceil(PIECE_LEN)],
        meta.info_hash,
        SeedCfg {
            label: "udp-host",
            reserved: spec::RESERVED_MODERN,
            unchoke_delay: Duration::from_millis(200),
            rechoke_every: None,
            drop_after: None,
        },
    );
    tokio::spawn(seeder.serve(sl));

    run_engine(meta, &dir, true, 0, Duration::from_secs(60))
        .await
        .unwrap_or_else(|e| {
            panic!(
                "域名形态 UDP tracker 下载失败: {e}\
                 （引擎缺少 DNS 解析则域名 tracker 全部被跳过）\n\
                 tracker 日志:\n{}\n种子端日志:\n{}",
                tracker_log.dump(),
                seed_log.dump()
            )
        });

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载内容与源数据不一致");
    assert!(
        udp_stats.announces.load(Ordering::Relaxed) > 0,
        "UDP tracker 从未收到有效 announce（域名未解析？）\n日志:\n{}",
        tracker_log.dump()
    );
    assert_no_violations(&tracker_log, "场景8（域名形态 UDP tracker）");
    assert_no_violations(&seed_log, "场景8（UDP tracker 下游 seed）");
    let _ = std::fs::remove_dir_all(&dir);
}

// ===========================================================================
// 场景 9：磁力链接（BEP 9 ut_metadata）端到端。
// 严格 seed 将 ut_metadata 广告在 ext_id=3（刻意不同于引擎广告的 2）：
// BEP 10 规定对端回包必须使用**我方**广告的 id。若引擎按对端 id 匹配
// 回包（常见实现错误），元数据永远收不到 → 磁力下载零速度。
// ===========================================================================

#[tokio::test]
async fn spec_magnet_ut_metadata_full_download() {
    let dir = fresh_dir("magnet");
    let data: Vec<u8> = (0..(2 * PIECE_LEN + 8080))
        .map(|i| ((i * 79 + 3) % 256) as u8)
        .collect();

    let info_bytes = build_info_dict(&data, PIECE_LEN);
    let info_hash = sha1_of(&info_bytes);

    // tracker 仅负责把引擎引到严格 seed（磁力链接的 peer 发现同样走 tracker）
    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");

    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];

    let log = Arc::new(SeedLog::new());
    let stats = Arc::new(SeedStats::default());
    let mut peer_id = [0u8; 20];
    peer_id[0..8].copy_from_slice(b"-SM0009-");
    let seeder = Arc::new(RefSeeder {
        cfg: SeedCfg {
            label: "magnet",
            reserved: spec::RESERVED_MODERN,
            unchoke_delay: Duration::from_millis(200),
            rechoke_every: None,
            drop_after: None,
        },
        data: Arc::new(data.clone()),
        piece_len: PIECE_LEN,
        have: Arc::new(vec![true; data.len().div_ceil(PIECE_LEN)]),
        info_hash,
        peer_id,
        log: log.clone(),
        stats: stats.clone(),
        dropped_once: AtomicBool::new(false),
        metadata: Some(info_bytes),
        ut_meta_id: 3, // 刻意 != 引擎的 2，暴露 BEP 10 回包方向错误
    });
    tokio::spawn(seeder.serve(sl));

    let cfg = TorrentConfig {
        dir: dir.clone(),
        peer_id: PeerId::azureus_prefix(&[9u8; 12]),
        listen_port: 0,
        max_peers: 8,
        adaptive: true,
        numwant: 50,
        announce_urls: vec![tracker_url],
        pipeline: 0,
        udp_announce_urls: Vec::new(),
        enable_dht: false,
        dht_port: 0,
        encryption: xfer_bt::EncryptionMode::PlaintextOnly,
        bt_protocol: xfer_bt::BtProtocol::TcpOnly,
        download_limit: 0,
        upload_limit: 0,
        seed_mode: false,
        seed_duration: 0,
        selected_files: None,
    };
    let engine = TorrentEngine::new_magnet(info_hash, cfg).unwrap();
    tokio::time::timeout(Duration::from_secs(90), engine.clone().run(CancellationToken::new()))
        .await
        .map_err(|_| "磁力下载超时（元数据获取或下载零速度）")
        .unwrap_or_else(|e| panic!("磁力下载失败: {e}\n种子端日志:\n{}", log.dump()))
        .unwrap_or_else(|e| panic!("磁力下载失败: {e}\n种子端日志:\n{}", log.dump()));

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载内容与源数据不一致");
    assert!(
        engine.has_metadata(),
        "引擎完成下载但元数据标记缺失（状态机不一致）"
    );
    assert_no_violations(&log, "场景9（磁力 ut_metadata）");
    assert!(
        stats.requests_served.load(Ordering::Relaxed) > 0,
        "元数据就绪后未发生任何 piece 请求"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
