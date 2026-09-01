//! xfer-transport：TCP/UdpSocket 原语与 uTP(BEP 29) 完整状态机。
//!
//! - `utp_packet`：uTP 包格式（编解码、SACK 扩展、序列号比较）
//! - `utp_connection`：uTP 连接状态机（SYN/STATE/DATA/FIN/RESET、LEDBAT、SACK、快速重传）
//! - `utp_socket`：async UDP socket + 连接管理器（多路复用、tick 驱动、UtpStream）
//!
//! 关键正确性（§7 继承）：
//! - 发起方所有出站包恒用自选 id C，响应方恒用 C+1
//! - LEDBAT 窗口增长按"新确认字节数"等比，接收窗口 ≥1MB
//! - 包尺寸 ≤1400 防 IP 分片
//! - uTP 与 TCP 监听同端口（§7.6）
//! - 独立于引擎，配规范参考对端互操作测试

pub mod utp_connection;
pub mod utp_packet;
pub mod utp_socket;

pub use utp_socket::{UtpManager, UtpManagerHandle, UtpPhase, UtpStream};
