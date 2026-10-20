//! 协议第 1 节：UDP 发现。
//!
//! - 端口固定 41234，广播 discover（2000ms 一次，启动立即一次）；
//! - 收到别人的 discover：记录 + 单播回 hello；
//! - 收到 hello：只记录（回 hello 会导致两端无限对答）；
//! - 自己 id 的报文直接忽略；
//! - 8000ms 没新报文 → 移除。
//!
//! 41234 被占用时按协议第 4 节降级为"只发不收"。

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use tokio::net::UdpSocket;
use tokio::time::{interval, MissedTickBehavior};

use crate::protocol::DiscoverMsg;
use crate::{LanService, DISCOVERY_INTERVAL, DISCOVERY_PORT, PEER_TTL};

/// 单包上限（协议：< 1400 字节）。
const MAX_DATAGRAM: usize = 1400;

/// 绑定发现端口：成功 → (socket, true)；端口被占用 → 临时端口只发不收 → (socket, false)。
pub(crate) async fn bind_socket() -> Option<(UdpSocket, bool)> {
    match UdpSocket::bind(("0.0.0.0", DISCOVERY_PORT)).await {
        Ok(socket) => {
            if let Err(e) = socket.set_broadcast(true) {
                tracing::warn!(error = %e, "开启 UDP 广播失败");
            }
            Some((socket, true))
        }
        Err(e) => {
            tracing::warn!(
                port = DISCOVERY_PORT,
                error = %e,
                "发现端口被占用，按协议降级为只发不收"
            );
            match UdpSocket::bind(("0.0.0.0", 0u16)).await {
                Ok(socket) => {
                    let _ = socket.set_broadcast(true);
                    Some((socket, false))
                }
                Err(e) => {
                    tracing::warn!(error = %e, "创建发现套接字失败，发现功能不可用");
                    None
                }
            }
        }
    }
}

pub(crate) async fn run(svc: Arc<LanService>, udp: Option<(UdpSocket, bool)>) {
    let targets = directed_broadcasts();
    if let Some((socket, _)) = &udp {
        broadcast(&svc, socket, &targets).await;
    }
    let mut ticker = interval(DISCOVERY_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut buf = [0u8; 2048];
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                svc.prune_peers(PEER_TTL);
                if let Some((socket, _)) = &udp {
                    broadcast(&svc, socket, &targets).await;
                }
            }
            received = recv(&udp, &mut buf) => {
                if let Some((len, from)) = received {
                    // 拷贝出来：buf 要留给下一轮
                    let data = buf[..len].to_vec();
                    handle(&svc, &udp, &data, from).await;
                }
            }
            _ = svc.stop_notify.notified() => break,
        }
    }
}

/// 只发不收时永远挂起（select 不会空转）。
async fn recv(udp: &Option<(UdpSocket, bool)>, buf: &mut [u8]) -> Option<(usize, SocketAddr)> {
    match udp {
        Some((socket, true)) => socket.recv_from(buf).await.ok(),
        _ => std::future::pending().await,
    }
}

async fn broadcast(svc: &LanService, socket: &UdpSocket, targets: &[SocketAddr]) {
    let name = svc.device_name();
    let msg = DiscoverMsg::new(
        "discover",
        svc.device_id(),
        &name,
        svc.platform(),
        svc.port(),
    );
    let bytes = match serde_json::to_vec(&msg) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "序列化 discover 失败");
            return;
        }
    };
    if bytes.len() > MAX_DATAGRAM {
        tracing::warn!(len = bytes.len(), "discover 报文超过 1400 字节");
        return;
    }
    // 受限广播：所有网卡（由系统路由选择）
    let _ = socket
        .send_to(&bytes, ("255.255.255.255", DISCOVERY_PORT))
        .await;
    // 定向广播：多网卡（VPN/有线+无线）时每一张网卡都能收到
    for target in targets {
        let _ = socket.send_to(&bytes, target).await;
    }
}

async fn handle(
    svc: &Arc<LanService>,
    udp: &Option<(UdpSocket, bool)>,
    data: &[u8],
    from: SocketAddr,
) {
    let Ok(msg) = serde_json::from_slice::<DiscoverMsg>(data) else {
        return;
    };
    if !msg.is_valid() {
        return;
    }
    if msg.id == svc.device_id() {
        return; // 自己的广播
    }
    let is_discover = match msg.kind.as_str() {
        "discover" => true,
        "hello" => false,
        _ => return,
    };
    svc.upsert_peer(&msg, from.ip());
    if !is_discover {
        return; // hello 不再回复，否则两端会无限对答
    }
    let Some((socket, _)) = udp else {
        return;
    };
    let name = svc.device_name();
    let hello = DiscoverMsg::new(
        "hello",
        svc.device_id(),
        &name,
        svc.platform(),
        svc.port(),
    );
    if let Ok(bytes) = serde_json::to_vec(&hello) {
        // 协议：单播回 hello 到 rinfo.address:41234（不是 rinfo.port）
        let _ = socket
            .send_to(&bytes, SocketAddr::new(from.ip(), DISCOVERY_PORT))
            .await;
    }
}

/// 本机各网卡的 IPv4 地址（通过"连一下公网地址取 local_addr"获得，不实际发包）。
fn local_ipv4s() -> Vec<Ipv4Addr> {
    let mut out = Vec::new();
    for target in ["8.8.8.8:80", "1.1.1.1:80"] {
        if let Ok(socket) = std::net::UdpSocket::bind(("0.0.0.0", 0u16)) {
            if socket.connect(target).is_ok() {
                if let Ok(addr) = socket.local_addr() {
                    if let IpAddr::V4(v4) = addr.ip() {
                        if !v4.is_loopback() && !v4.is_unspecified() {
                            out.push(v4);
                        }
                    }
                }
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// 定向广播地址（`x.y.z.255:41234`）。
///
/// 不引入网卡枚举依赖：按 /24 估算——绝大多数局域网就是 /24，
/// 即使估算错，多发的 UDP 包也无害（受限广播仍会照发）。
fn directed_broadcasts() -> Vec<SocketAddr> {
    let mut out: Vec<SocketAddr> = local_ipv4s()
        .into_iter()
        .map(|ip| {
            let octets = ip.octets();
            SocketAddr::from((
                Ipv4Addr::new(octets[0], octets[1], octets[2], 255),
                DISCOVERY_PORT,
            ))
        })
        .filter(|addr| addr.ip() != IpAddr::V4(Ipv4Addr::UNSPECIFIED))
        .collect();
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LanConfig;

    #[test]
    fn directed_broadcasts_are_canonical() {
        for addr in directed_broadcasts() {
            assert_eq!(addr.port(), DISCOVERY_PORT);
            let IpAddr::V4(v4) = addr.ip() else {
                panic!("只应产生 IPv4 广播地址");
            };
            assert_eq!(v4.octets()[3], 255);
        }
    }

    /// 发现循环不应在"只发不收"的降级模式下空转：recv 永远挂起。
    #[test]
    fn send_only_recv_stays_pending() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut buf = [0u8; 16];
            let result =
                tokio::time::timeout(std::time::Duration::from_millis(50), recv(&None, &mut buf))
                    .await;
            assert!(result.is_err(), "只发不收时 recv 应立即挂起");
        });
    }

    /// 收到 discover 记录对端；自己 id / 协议标记或版本不符的报文一律忽略；超时移除。
    #[tokio::test]
    async fn records_peer_and_ignores_self_or_foreign() {
        let root = std::env::temp_dir().join(format!(
            "xfer-lan-disc-{}-{}",
            std::process::id(),
            crate::random_hex(4)
        ));
        let svc = crate::LanService::start(LanConfig::new(&root, root.join("data")))
            .await
            .unwrap();
        let from: SocketAddr = "10.1.2.3:41234".parse().unwrap();

        let discover = DiscoverMsg::new("discover", "peer-1", "Phone", "android", 40000);
        handle(&svc, &None, &serde_json::to_vec(&discover).unwrap(), from).await;
        let peer = svc
            .peers()
            .into_iter()
            .find(|p| p.id == "peer-1")
            .expect("应记录对端设备");
        assert_eq!(peer.address, "10.1.2.3");
        assert_eq!(peer.port, 40000);
        assert_eq!(peer.platform, "android");

        // 自己 id 的报文
        let own = DiscoverMsg::new("discover", svc.device_id(), "Self", "macos", 1);
        handle(&svc, &None, &serde_json::to_vec(&own).unwrap(), from).await;
        assert!(!svc.peers().iter().any(|p| p.id == svc.device_id()));

        // 协议标记 / 版本不符
        let mut foreign = discover.clone();
        foreign.proto = "other".into();
        foreign.platform = "other".into();
        handle(&svc, &None, &serde_json::to_vec(&foreign).unwrap(), from).await;
        // 已记录的 peer-1 不能被这条报文覆盖
        let peer = svc
            .peers()
            .into_iter()
            .find(|p| p.id == "peer-1")
            .expect("对端仍在列表里");
        assert_eq!(peer.platform, "android");
        let mut old = discover.clone();
        old.v = 1;
        old.id = "peer-2".into();
        handle(&svc, &None, &serde_json::to_vec(&old).unwrap(), from).await;
        assert!(!svc.peers().iter().any(|p| p.id == "peer-2"));

        // 8s 过期（这里直接把 TTL 设 0 验证移除逻辑）
        svc.prune_peers(std::time::Duration::from_millis(0));
        assert!(svc.peers().is_empty());

        svc.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }
}
