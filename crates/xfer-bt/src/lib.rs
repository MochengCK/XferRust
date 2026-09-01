//! xfer-bt：BitTorrent 下载引擎。
//!
//! - `message`：peer wire 握手/消息编解码（BEP 3）；
//! - `mse`：MSE 加密协议协商（BEP 8）；
//! - `tracker`：HTTP tracker announce（compact/非 compact）；
//! - `engine`：下载控制器（tracker + 多 peer 并行 + rarest-first + 校验落盘）。
//!
//! M4 新增：MSE（BEP 8）与 uTP（BEP 29）传输层。

pub mod engine;
pub mod message;
pub mod mse;
pub mod resume;
pub mod scheduler;
pub mod tracker;

pub use engine::{
    BtProtocol, EncryptionMode, PeerInfo, PeerSource, PeerStream, TorrentConfig, TorrentEngine,
    TorrentProgress,
};
pub use message::{
    decode_handshake, encode_handshake, supports_dht, supports_extension, supports_fast_extension,
    Handshake, Message, PeerReader, RESERVED,
};
pub use mse::{pe_handshake_initiator, pe_handshake_responder, EncryptedStream, PeOutcome};
pub use scheduler::{PeerSample, PeerScheduler, PeerSchedulerConfig, ScheduleAction};
