//! Local Service Discovery（BEP 14）。
//!
//! LSD 通过 UDP 多播在本地网络发现 BT peer，无需 tracker 或 DHT。
//!
//! 多播组：`239.192.0.0:6771`（IPv4）/ `ff15::7465:7874:3674`（IPv6）
//! 消息格式（HTTP headers over UDP）：
//! ```text
//! BT-SEARCH * HTTP/1.1
//! Host: 239.192.0.0:6771
//! Port: <listening port>
//! Infohash: <40-char hex>
//! cookie: <random cookie>
//!
//! ```
//!
//! 每 5 分钟广播一次，最多收到 10 个 peer 后停止。

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;
use xfer_types::InfoHash;

/// LSD 多播地址（IPv4）。
pub const LSD_MULTICAST_ADDR: Ipv4Addr = Ipv4Addr::new(239, 192, 0, 0);
/// LSD 多播端口。
pub const LSD_PORT: u16 = 6771;
/// 广播间隔（BEP 14 建议 5 分钟）。
pub const LSD_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(300);
/// 监听超时。
pub const LSD_LISTEN_TIMEOUT: Duration = Duration::from_secs(5);

/// LSD 配置。
#[derive(Debug, Clone)]
pub struct LsdConfig {
    /// 本端监听端口（BT 监听端口）。
    pub listen_port: u16,
    /// cookie（随机标识，防止自身回环）。
    pub cookie: String,
    /// 多播 TTL（默认 1 = 本地链路）。
    pub multicast_ttl: u32,
}

impl Default for LsdConfig {
    fn default() -> Self {
        let mut cookie_buf = [0u8; 16];
        let _ = getrandom::fill(&mut cookie_buf);
        Self {
            listen_port: 0,
            cookie: hex::encode(cookie_buf),
            multicast_ttl: 1,
        }
    }
}

/// LSD 节点：发送多播 announce + 接收其他 peer 的多播消息。
pub struct Lsd {
    config: LsdConfig,
    socket: UdpSocket,
}

impl Lsd {
    /// 创建 LSD 节点并绑定到多播组。
    pub async fn new(config: LsdConfig) -> Result<Self, String> {
        let socket = UdpSocket::bind(("0.0.0.0", LSD_PORT))
            .await
            .map_err(|e| format!("LSD UDP 绑定失败: {e}"))?;

        // 加入多播组
        let multi_addr = IpAddr::V4(LSD_MULTICAST_ADDR);
        socket
            .join_multicast_v4(LSD_MULTICAST_ADDR, Ipv4Addr::UNSPECIFIED)
            .map_err(|e| format!("LSD join_multicast 失败: {e}"))?;

        // 设置多播 TTL
        socket
            .set_multicast_ttl_v4(config.multicast_ttl)
            .map_err(|e| format!("LSD set_multicast_ttl 失败: {e}"))?;

        tracing::info!(
            port = config.listen_port,
            multi_addr = %multi_addr,
            "LSD 节点已启动"
        );

        Ok(Self { config, socket })
    }

    /// 创建仅用于发送的 LSD（不监听多播，测试用）。
    pub async fn sender_only(config: LsdConfig) -> Result<Self, String> {
        let socket = UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(|e| format!("LSD sender UDP 绑定失败: {e}"))?;
        socket
            .set_multicast_ttl_v4(config.multicast_ttl)
            .map_err(|e| format!("LSD set_multicast_ttl 失败: {e}"))?;
        Ok(Self { config, socket })
    }

    /// 广播一次 LSD announce。
    pub async fn announce(&self, info_hash: &InfoHash) -> Result<(), String> {
        let msg = build_announce_message(&self.config, info_hash);
        let addr = SocketAddr::new(IpAddr::V4(LSD_MULTICAST_ADDR), LSD_PORT);
        self.socket
            .send_to(&msg, addr)
            .await
            .map_err(|e| format!("LSD announce 发送失败: {e}"))?;
        tracing::debug!(info_hash = %info_hash, "LSD announce 已发送");
        Ok(())
    }

    /// 监听 LSD 消息（阻塞直到超时或收到消息）。
    /// 返回发现的 peer 地址列表（port + info_hash 匹配时才加入）。
    pub async fn listen(
        &self,
        target_info_hash: &InfoHash,
        timeout_duration: Duration,
    ) -> Vec<SocketAddr> {
        let mut found = Vec::new();
        let mut buf = vec![0u8; 1024];

        let deadline = tokio::time::Instant::now() + timeout_duration;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(remaining, self.socket.recv_from(&mut buf)).await {
                Ok(Ok((n, from))) => {
                    if let Some(peer_port) =
                        parse_announce_message(&buf[..n], target_info_hash, &self.config.cookie)
                    {
                        let peer_addr = SocketAddr::new(from.ip(), peer_port);
                        if !found.contains(&peer_addr) {
                            found.push(peer_addr);
                        }
                        if found.len() >= 10 {
                            break; // BEP 14: 最多 10 个 peer
                        }
                    }
                }
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, "LSD recv 失败");
                    break;
                }
                Err(_) => break, // 超时
            }
        }
        found
    }

    /// 启动后台广播循环（每 5 分钟一次）。
    pub fn spawn_announce_loop(self: std::sync::Arc<Self>, info_hash: InfoHash) {
        tokio::spawn(async move {
            // 首次立即广播
            if let Err(e) = self.announce(&info_hash).await {
                tracing::warn!(error = %e, "LSD 首次广播失败");
            }
            let mut iv = tokio::time::interval(LSD_ANNOUNCE_INTERVAL);
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                iv.tick().await;
                if let Err(e) = self.announce(&info_hash).await {
                    tracing::warn!(error = %e, "LSD 广播失败");
                }
            }
        });
    }
}

/// 构建 LSD announce 消息（HTTP-over-UDP 格式）。
///
/// 注意：Rust `format!` 的 `\` 行续接会保留续行后的前导空格，导致 header
/// 名前出现多余空格（如 ` Host:` 而非 `Host:`）。BEP 14 要求标准 HTTP
/// header 格式，因此使用 `concat!` + `format!` 避免此问题。
fn build_announce_message(config: &LsdConfig, info_hash: &InfoHash) -> Vec<u8> {
    format!(
        concat!(
            "BT-SEARCH * HTTP/1.1\r\n",
            "Host: {}:{}\r\n",
            "Port: {}\r\n",
            "Infohash: {}\r\n",
            "cookie: {}\r\n",
            "\r\n",
            "\r\n",
        ),
        LSD_MULTICAST_ADDR,
        LSD_PORT,
        config.listen_port,
        info_hash.to_hex(),
        config.cookie,
    )
    .into_bytes()
}

/// 解析 LSD announce 消息，返回 peer 的监听端口（如果 info_hash 匹配且 cookie 不同）。
fn parse_announce_message(
    data: &[u8],
    target_info_hash: &InfoHash,
    my_cookie: &str,
) -> Option<u16> {
    let text = std::str::from_utf8(data).ok()?;

    // 检查首行
    if !text.starts_with("BT-SEARCH") {
        return None;
    }

    let mut port: Option<u16> = None;
    let mut info_hash_hex: Option<String> = None;
    let mut cookie: Option<String> = None;

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some((key, value)) = line.split_once(':') {
            let key = key.trim().to_lowercase();
            let value = value.trim();
            match key.as_str() {
                "port" => {
                    port = value.parse::<u16>().ok();
                }
                "infohash" => {
                    info_hash_hex = Some(value.to_lowercase());
                }
                "cookie" => {
                    cookie = Some(value.to_string());
                }
                _ => {}
            }
        }
    }

    // 检查 cookie：如果是自己的消息则忽略
    if let Some(c) = &cookie {
        if c == my_cookie {
            return None;
        }
    }

    // 检查 info_hash 是否匹配
    let ih = info_hash_hex?;
    let parsed_ih = InfoHash::from_hex(&ih)?;
    if &parsed_ih != target_info_hash {
        return None;
    }

    port
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn announce_message_format() {
        let config = LsdConfig {
            listen_port: 6881,
            cookie: "testcookie123".to_string(),
            multicast_ttl: 1,
        };
        let ih = InfoHash::from_bytes(&[0xAA; 20]);
        let msg = build_announce_message(&config, &ih);
        let text = std::str::from_utf8(&msg).unwrap();

        assert!(text.starts_with("BT-SEARCH * HTTP/1.1\r\n"));
        assert!(text.contains("Host: 239.192.0.0:6771"));
        assert!(text.contains("Port: 6881"));
        assert!(text.contains(&format!("Infohash: {}", ih.to_hex())));
        assert!(text.contains("cookie: testcookie123"));
    }

    #[test]
    fn parse_matching_message() {
        let ih = InfoHash::from_bytes(&[0xAA; 20]);
        let config = LsdConfig {
            listen_port: 6881,
            cookie: "my_cookie".to_string(),
            multicast_ttl: 1,
        };
        let msg = build_announce_message(&config, &ih);

        let port = parse_announce_message(&msg, &ih, "other_cookie");
        assert_eq!(port, Some(6881));
    }

    #[test]
    fn parse_ignores_self_message() {
        let ih = InfoHash::from_bytes(&[0xAA; 20]);
        let cookie = "my_cookie".to_string();
        let config = LsdConfig {
            listen_port: 6881,
            cookie: cookie.clone(),
            multicast_ttl: 1,
        };
        let msg = build_announce_message(&config, &ih);

        // 同 cookie → 忽略
        let port = parse_announce_message(&msg, &ih, &cookie);
        assert_eq!(port, None);
    }

    #[test]
    fn parse_ignores_wrong_infohash() {
        let ih1 = InfoHash::from_bytes(&[0xAA; 20]);
        let ih2 = InfoHash::from_bytes(&[0xBB; 20]);
        let config = LsdConfig {
            listen_port: 6881,
            cookie: "cookie1".to_string(),
            multicast_ttl: 1,
        };
        let msg = build_announce_message(&config, &ih1);

        // 不同的 info_hash → 不匹配
        let port = parse_announce_message(&msg, &ih2, "cookie2");
        assert_eq!(port, None);
    }

    #[test]
    fn parse_ignores_non_bt_search() {
        let ih = InfoHash::from_bytes(&[0xAA; 20]);
        let port = parse_announce_message(b"GET / HTTP/1.1", &ih, "cookie");
        assert_eq!(port, None);
    }

    #[tokio::test]
    async fn lsd_sender_receiver_roundtrip() {
        // 使用回环地址单播测试（CI 环境下多播可能不可用）
        let ih = InfoHash::from_bytes(&[0xAA; 20]);

        // 创建接收者（绑定到回环地址）
        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let receiver_addr = receiver.local_addr().unwrap();

        // 创建发送者（绑定到回环地址）
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        // 构建消息并发送
        let config = LsdConfig {
            listen_port: 6881,
            cookie: "sender_cookie".to_string(),
            multicast_ttl: 1,
        };
        let msg = build_announce_message(&config, &ih);
        sender.send_to(&msg, receiver_addr).await.unwrap();

        // 接收并解析
        let mut buf = vec![0u8; 1024];
        let (n, _) = receiver.recv_from(&mut buf).await.unwrap();
        let port = parse_announce_message(&buf[..n], &ih, "receiver_cookie");
        assert_eq!(port, Some(6881));
    }
}
