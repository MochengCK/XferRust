//! UDP tracker 协议（BEP 15）。
//!
//! 协议：connect → announce/scrape，基于 UDP。
//! 重发策略（§7.7）：5s 首次重发、10s 放弃。
//! connect 响应 connection_id 有效期 60s（BEP 15 建议）。

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::time::timeout;
use xfer_types::{InfoHash, PeerId};

/// UDP tracker 事件类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum UdpEvent {
    None = 0,
    Completed = 1,
    Started = 2,
    Stopped = 3,
}

/// announce 请求参数。
#[derive(Debug, Clone)]
pub struct UdpAnnounceRequest {
    pub info_hash: InfoHash,
    pub peer_id: PeerId,
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub event: UdpEvent,
    pub numwant: u32,
}

/// announce 响应。
#[derive(Debug, Clone, Default)]
pub struct UdpTrackerResponse {
    pub interval: u32,
    pub leechers: u32,
    pub seeders: u32,
    pub peers: Vec<SocketAddr>,
}

/// UDP tracker 错误。
#[derive(Debug, thiserror::Error)]
pub enum UdpTrackerError {
    #[error("网络错误: {0}")]
    Network(String),
    #[error("协议错误: {0}")]
    Protocol(String),
    #[error("超时（10s 内无响应）")]
    Timeout,
    #[error("tracker 返回错误: {0}")]
    TrackerError(String),
}

/// action 常量（BEP 15）。
const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;
#[allow(dead_code)]
const ACTION_SCRAPE: u32 = 2;
const ACTION_ERROR: u32 = 3;

/// connection_id 有效期。
const CONNECTION_ID_TTL: Duration = Duration::from_secs(60);

/// UDP tracker 客户端。
pub struct UdpTracker {
    socket: UdpSocket,
    /// connection_id 缓存：(tracker_addr, id, 过期时间)。
    connection_id: Option<(SocketAddr, u64, Instant)>,
}

impl UdpTracker {
    /// 绑定 UDP socket（系统分配端口）。
    pub async fn new() -> Result<Self, UdpTrackerError> {
        let socket = UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(|e| UdpTrackerError::Network(e.to_string()))?;
        Ok(Self {
            socket,
            connection_id: None,
        })
    }

    /// 绑定到指定地址（测试用）。
    pub async fn bind(addr: &str) -> Result<Self, UdpTrackerError> {
        let socket = UdpSocket::bind(addr)
            .await
            .map_err(|e| UdpTrackerError::Network(e.to_string()))?;
        Ok(Self {
            socket,
            connection_id: None,
        })
    }

    /// 执行 announce（含 connect 握手 + 重发策略）。
    /// 重发策略（§7.7）：5s 首次重发、10s 放弃。
    pub async fn announce(
        &mut self,
        tracker_addr: SocketAddr,
        req: &UdpAnnounceRequest,
    ) -> Result<UdpTrackerResponse, UdpTrackerError> {
        // 1. 获取 connection_id（缓存有效则复用）
        let conn_id = self.get_connection_id(tracker_addr).await?;

        // 2. 发送 announce 请求（5s 重发，10s 放弃）
        let resp = self.announce_with_retry(tracker_addr, conn_id, req).await;

        // 如果 announce 失败且 connection_id 可能过期，重置后重试
        if let Err(UdpTrackerError::Protocol(ref msg)) = resp {
            if msg.contains("connection_id") || msg.contains("invalid") {
                self.connection_id = None;
                let conn_id = self.get_connection_id(tracker_addr).await?;
                return self.announce_with_retry(tracker_addr, conn_id, req).await;
            }
        }
        resp
    }

    /// 获取有效的 connection_id（缓存或新 connect）。
    async fn get_connection_id(
        &mut self,
        tracker_addr: SocketAddr,
    ) -> Result<u64, UdpTrackerError> {
        // 检查缓存
        if let Some((addr, id, expires)) = &self.connection_id {
            if *addr == tracker_addr && expires > &Instant::now() {
                return Ok(*id);
            }
        }

        // 发送 connect 请求（5s 重发，10s 放弃）
        let conn_id = self.connect_with_retry(tracker_addr).await?;
        self.connection_id = Some((tracker_addr, conn_id, Instant::now() + CONNECTION_ID_TTL));
        Ok(conn_id)
    }

    /// connect 请求（含重发策略）。
    async fn connect_with_retry(&self, tracker_addr: SocketAddr) -> Result<u64, UdpTrackerError> {
        let transaction_id: u32 = random_u32();
        let packet = build_connect_packet(transaction_id);

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut delay = Duration::from_millis(500);

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(UdpTrackerError::Timeout);
            }

            self.socket
                .send_to(&packet, tracker_addr)
                .await
                .map_err(|e| UdpTrackerError::Network(e.to_string()))?;

            let mut buf = vec![0u8; 64];
            match timeout(
                Duration::min(delay, remaining),
                self.socket.recv_from(&mut buf),
            )
            .await
            {
                Ok(Ok((n, _))) => {
                    return parse_connect_response(&buf[..n], transaction_id);
                }
                Ok(Err(e)) => return Err(UdpTrackerError::Network(e.to_string())),
                Err(_) => {
                    // 超时，加倍重试间隔
                    delay *= 2;
                    if delay > Duration::from_secs(5) {
                        delay = Duration::from_secs(5);
                    }
                    continue;
                }
            }
        }
    }

    /// announce 请求（含重发策略）。
    async fn announce_with_retry(
        &self,
        tracker_addr: SocketAddr,
        connection_id: u64,
        req: &UdpAnnounceRequest,
    ) -> Result<UdpTrackerResponse, UdpTrackerError> {
        let transaction_id: u32 = random_u32();
        let packet = build_announce_packet(connection_id, transaction_id, req);

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut delay = Duration::from_millis(500);

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(UdpTrackerError::Timeout);
            }

            self.socket
                .send_to(&packet, tracker_addr)
                .await
                .map_err(|e| UdpTrackerError::Network(e.to_string()))?;

            let mut buf = vec![0u8; 4096];
            match timeout(
                Duration::min(delay, remaining),
                self.socket.recv_from(&mut buf),
            )
            .await
            {
                Ok(Ok((n, _))) => {
                    return parse_announce_response(&buf[..n], transaction_id);
                }
                Ok(Err(e)) => return Err(UdpTrackerError::Network(e.to_string())),
                Err(_) => {
                    delay *= 2;
                    if delay > Duration::from_secs(5) {
                        delay = Duration::from_secs(5);
                    }
                    continue;
                }
            }
        }
    }
}

// ----------------------------------------------------------------------
// 包构造与解析
// ----------------------------------------------------------------------

/// 构造 connect 请求包。
/// 格式：[connection_id(8)][action=0(4)][transaction_id(4)]
fn build_connect_packet(transaction_id: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16);
    // protocol_id = 0x41727101980 (magic constant, BEP 15)
    buf.extend_from_slice(&0x41727101980u64.to_be_bytes());
    buf.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
    buf.extend_from_slice(&transaction_id.to_be_bytes());
    buf
}

/// 解析 connect 响应。
/// 格式：[action(4)][transaction_id(4)][connection_id(8)]
fn parse_connect_response(data: &[u8], expected_tid: u32) -> Result<u64, UdpTrackerError> {
    if data.len() < 16 {
        return Err(UdpTrackerError::Protocol("connect 响应过短".into()));
    }
    let action = u32::from_be_bytes(data[0..4].try_into().unwrap());
    let tid = u32::from_be_bytes(data[4..8].try_into().unwrap());

    if tid != expected_tid {
        return Err(UdpTrackerError::Protocol("transaction_id 不匹配".into()));
    }

    if action == ACTION_ERROR {
        let msg = String::from_utf8_lossy(&data[8..]).to_string();
        return Err(UdpTrackerError::TrackerError(msg));
    }

    if action != ACTION_CONNECT {
        return Err(UdpTrackerError::Protocol(format!(
            "期望 action=connect, 实际 action={action}"
        )));
    }

    let connection_id = u64::from_be_bytes(data[8..16].try_into().unwrap());
    Ok(connection_id)
}

/// 构造 announce 请求包。
/// 格式：[connection_id(8)][action=1(4)][transaction_id(4)]
///       [info_hash(20)][peer_id(20)][downloaded(8)][left(8)][uploaded(8)]
///       [event(4)][ip=0(4)][key(4)][numwant(4)][port(2)]
fn build_announce_packet(
    connection_id: u64,
    transaction_id: u32,
    req: &UdpAnnounceRequest,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(98);
    buf.extend_from_slice(&connection_id.to_be_bytes());
    buf.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
    buf.extend_from_slice(&transaction_id.to_be_bytes());
    buf.extend_from_slice(req.info_hash.as_bytes());
    buf.extend_from_slice(&req.peer_id.0);
    buf.extend_from_slice(&req.downloaded.to_be_bytes());
    buf.extend_from_slice(&req.left.to_be_bytes());
    buf.extend_from_slice(&req.uploaded.to_be_bytes());
    buf.extend_from_slice(&(req.event as u32).to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes()); // ip = 0 (default)
    buf.extend_from_slice(&random_u32().to_be_bytes()); // key
    buf.extend_from_slice(&req.numwant.to_be_bytes());
    buf.extend_from_slice(&req.port.to_be_bytes());
    buf
}

/// 解析 announce 响应。
/// 格式：[action(4)][transaction_id(4)][interval(4)][leechers(4)][seeders(4)]
///       [peers: 6 bytes each (4 IP + 2 port)]
fn parse_announce_response(
    data: &[u8],
    expected_tid: u32,
) -> Result<UdpTrackerResponse, UdpTrackerError> {
    if data.len() < 20 {
        return Err(UdpTrackerError::Protocol("announce 响应过短".into()));
    }
    let action = u32::from_be_bytes(data[0..4].try_into().unwrap());
    let tid = u32::from_be_bytes(data[4..8].try_into().unwrap());

    if tid != expected_tid {
        return Err(UdpTrackerError::Protocol("transaction_id 不匹配".into()));
    }

    if action == ACTION_ERROR {
        let msg = String::from_utf8_lossy(&data[8..]).to_string();
        return Err(UdpTrackerError::TrackerError(msg));
    }

    if action != ACTION_ANNOUNCE {
        return Err(UdpTrackerError::Protocol(format!(
            "期望 action=announce, 实际 action={action}"
        )));
    }

    let interval = u32::from_be_bytes(data[8..12].try_into().unwrap());
    let leechers = u32::from_be_bytes(data[12..16].try_into().unwrap());
    let seeders = u32::from_be_bytes(data[16..20].try_into().unwrap());

    let peers = parse_compact_peers(&data[20..]);

    Ok(UdpTrackerResponse {
        interval,
        leechers,
        seeders,
        peers,
    })
}

/// 解析 compact peer 列表：每 6 字节 = 4 IP + 2 port (BE)。
fn parse_compact_peers(data: &[u8]) -> Vec<SocketAddr> {
    let mut peers = Vec::with_capacity(data.len() / 6);
    for chunk in data.chunks_exact(6) {
        let ip = [chunk[0], chunk[1], chunk[2], chunk[3]];
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        peers.push(SocketAddr::from((ip, port)));
    }
    peers
}

/// 生成随机 u32。
fn random_u32() -> u32 {
    let mut buf = [0u8; 4];
    let _ = getrandom::fill(&mut buf);
    u32::from_be_bytes(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UdpSocket;

    #[test]
    fn connect_packet_format() {
        let packet = build_connect_packet(0x12345678);
        assert_eq!(packet.len(), 16);
        // protocol_id magic
        assert_eq!(
            u64::from_be_bytes(packet[0..8].try_into().unwrap()),
            0x41727101980
        );
        // action = 0 (connect)
        assert_eq!(
            u32::from_be_bytes(packet[8..12].try_into().unwrap()),
            ACTION_CONNECT
        );
        // transaction_id
        assert_eq!(
            u32::from_be_bytes(packet[12..16].try_into().unwrap()),
            0x12345678
        );
    }

    #[test]
    fn announce_packet_format() {
        let req = UdpAnnounceRequest {
            info_hash: InfoHash::from_bytes(&[0xAA; 20]),
            peer_id: PeerId([0xBB; 20]),
            port: 6881,
            uploaded: 100,
            downloaded: 200,
            left: 300,
            event: UdpEvent::Started,
            numwant: 50,
        };
        let packet = build_announce_packet(0xdead_beef_cafe_babe, 0x12345678, &req);
        assert_eq!(packet.len(), 98);
        assert_eq!(
            u64::from_be_bytes(packet[0..8].try_into().unwrap()),
            0xdead_beef_cafe_babe
        );
        assert_eq!(
            u32::from_be_bytes(packet[8..12].try_into().unwrap()),
            ACTION_ANNOUNCE
        );
        assert_eq!(
            u32::from_be_bytes(packet[12..16].try_into().unwrap()),
            0x12345678
        );
        assert_eq!(&packet[16..36], &[0xAA; 20]);
        assert_eq!(&packet[36..56], &[0xBB; 20]);
        assert_eq!(
            u64::from_be_bytes(packet[56..64].try_into().unwrap()),
            200 // downloaded
        );
        assert_eq!(
            u64::from_be_bytes(packet[64..72].try_into().unwrap()),
            300 // left
        );
        assert_eq!(
            u64::from_be_bytes(packet[72..80].try_into().unwrap()),
            100 // uploaded
        );
        assert_eq!(
            u32::from_be_bytes(packet[80..84].try_into().unwrap()),
            UdpEvent::Started as u32
        );
        assert_eq!(u16::from_be_bytes(packet[96..98].try_into().unwrap()), 6881);
    }

    #[test]
    fn parse_connect_response_valid() {
        let mut buf = vec![0u8; 16];
        buf[0..4].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
        buf[4..8].copy_from_slice(&0x12345678u32.to_be_bytes());
        buf[8..16].copy_from_slice(&0xdead_beef_cafe_babeu64.to_be_bytes());
        let conn_id = parse_connect_response(&buf, 0x12345678).unwrap();
        assert_eq!(conn_id, 0xdead_beef_cafe_babe);
    }

    #[test]
    fn parse_connect_response_wrong_tid() {
        let mut buf = vec![0u8; 16];
        buf[0..4].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
        buf[4..8].copy_from_slice(&0x99999999u32.to_be_bytes());
        buf[8..16].copy_from_slice(&0xdead_beef_cafe_babeu64.to_be_bytes());
        assert!(parse_connect_response(&buf, 0x12345678).is_err());
    }

    #[test]
    fn parse_announce_response_valid() {
        let mut buf = Vec::new();
        // action = announce
        buf.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
        // transaction_id
        buf.extend_from_slice(&0x12345678u32.to_be_bytes());
        // interval = 1800
        buf.extend_from_slice(&1800u32.to_be_bytes());
        // leechers = 5
        buf.extend_from_slice(&5u32.to_be_bytes());
        // seeders = 10
        buf.extend_from_slice(&10u32.to_be_bytes());
        // peers: 127.0.0.1:6881 + 10.0.0.1:51413
        buf.extend_from_slice(&[127, 0, 0, 1, 0x1A, 0xE1]);
        buf.extend_from_slice(&[10, 0, 0, 1, 0xC8, 0xD5]);

        let resp = parse_announce_response(&buf, 0x12345678).unwrap();
        assert_eq!(resp.interval, 1800);
        assert_eq!(resp.leechers, 5);
        assert_eq!(resp.seeders, 10);
        assert_eq!(resp.peers.len(), 2);
        assert_eq!(resp.peers[0], "127.0.0.1:6881".parse().unwrap());
        assert_eq!(resp.peers[1], "10.0.0.1:51413".parse().unwrap());
    }

    #[test]
    fn parse_error_response() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&ACTION_ERROR.to_be_bytes());
        buf.extend_from_slice(&0x12345678u32.to_be_bytes());
        buf.extend_from_slice(b"invalid torrent");
        let result = parse_announce_response(&buf, 0x12345678);
        assert!(matches!(result, Err(UdpTrackerError::TrackerError(_))));
    }

    #[test]
    fn compact_peers_parsing() {
        let data: Vec<u8> = vec![127, 0, 0, 1, 0x1A, 0xE1, 10, 0, 0, 1, 0xC8, 0xD5];
        let peers = parse_compact_peers(&data);
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].port(), 6881);
        assert_eq!(peers[1].port(), 51413);
    }

    /// 模拟 UDP tracker 服务器测试。
    #[tokio::test]
    async fn mock_udp_tracker_announce() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 128];
            // 1. 接收 connect 请求
            let (n, client_addr) = server.recv_from(&mut buf).await.unwrap();
            assert_eq!(n, 16);

            // 解析 transaction_id
            let tid = u32::from_be_bytes(buf[12..16].try_into().unwrap());

            // 发送 connect 响应
            let mut resp = Vec::with_capacity(16);
            resp.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
            resp.extend_from_slice(&tid.to_be_bytes());
            resp.extend_from_slice(&0xCAFE_BABE_u64.to_be_bytes());
            server.send_to(&resp, client_addr).await.unwrap();

            // 2. 接收 announce 请求
            let (n, _) = server.recv_from(&mut buf).await.unwrap();
            assert_eq!(n, 98);

            let announce_tid = u32::from_be_bytes(buf[12..16].try_into().unwrap());

            // 发送 announce 响应
            let mut announce_resp = Vec::new();
            announce_resp.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
            announce_resp.extend_from_slice(&announce_tid.to_be_bytes());
            announce_resp.extend_from_slice(&1800u32.to_be_bytes()); // interval
            announce_resp.extend_from_slice(&5u32.to_be_bytes()); // leechers
            announce_resp.extend_from_slice(&10u32.to_be_bytes()); // seeders
                                                                   // 2 个 peer
            announce_resp.extend_from_slice(&[127, 0, 0, 1, 0x1A, 0xE1]);
            announce_resp.extend_from_slice(&[192, 168, 1, 1, 0x1A, 0xE2]);
            server.send_to(&announce_resp, client_addr).await.unwrap();
        });

        let mut tracker = UdpTracker::bind("127.0.0.1:0").await.unwrap();
        let req = UdpAnnounceRequest {
            info_hash: InfoHash::from_bytes(&[0xAA; 20]),
            peer_id: PeerId([0xBB; 20]),
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 1024,
            event: UdpEvent::Started,
            numwant: 50,
        };
        let resp = tracker.announce(server_addr, &req).await.unwrap();
        assert_eq!(resp.interval, 1800);
        assert_eq!(resp.leechers, 5);
        assert_eq!(resp.seeders, 10);
        assert_eq!(resp.peers.len(), 2);
        assert_eq!(resp.peers[0], "127.0.0.1:6881".parse().unwrap());
        assert_eq!(resp.peers[1], "192.168.1.1:6882".parse().unwrap());

        server_task.await.unwrap();
    }
}
