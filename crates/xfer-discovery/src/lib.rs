//! xfer-discovery：BT 网络发现层。
//!
//! - `udp_tracker`：UDP tracker（BEP 15），5s 首次重发、10s 放弃；
//! - `pex`：Peer Exchange（BEP 11），uTP 能力位 0x04；
//! - `lsd`：Local Service Discovery（BEP 14），多播 239.192.0.0:6771；
//! - `upnp`：UPnP/NAT-PMP 端口映射（TCP+UDP 双映射）。
//!
//! 关键正确性（§7 继承）：
//! - UDP tracker 5s 重发 / 10s 超时；
//! - PEX added.f 的 uTP 位是 0x04（不是 0x01）；
//! - UPnP/NAT-PMP 必须同时映射 TCP 和 UDP。

pub mod lsd;
pub mod pex;
pub mod udp_tracker;
pub mod upnp;

pub use lsd::{Lsd, LsdConfig};
pub use pex::{PexExchange, PexMessage, PexPeer};
pub use udp_tracker::{UdpTracker, UdpTrackerResponse};
pub use upnp::{NatPortMapping, PortMappingProtocol, UpnpClient};
