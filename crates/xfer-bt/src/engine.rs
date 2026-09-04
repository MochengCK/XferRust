//! BT 下载引擎（M2/M3/M4/M5/M7）：
//! tracker announce（HTTP + UDP）+ 多 peer 并行下载 +
//! rarest-first 选片 + piece 校验落盘 + 阻塞语义（BEP 3）+ DHT peer 发现 +
//! MSE 加密集成（BEP 8）+ 精细调度（自适应流水线/冷启动突发/慢速节点淘汰/限速/seed 模式）。
//!
//! 设计：单控制器（TorrentEngine）+ 每 peer 一个 tokio 任务。
//! 一片同一时刻只分配给一个 peer（独占至完成/断开），rarest-first 选择；
//! 块级请求流水线自适应 16→256；被 choke 时整片释放重新排队。
//! M5 新增：请求流水线自适应、30s 连接超时、冷启动突发连接、
//! 慢速节点淘汰、限速、seed 模式、MSE 集成、uTP-first 探测。
//! M7 新增：Fast Extension (BEP 6) 完整集成、Extension Protocol (BEP 10) + PEX (BEP 11)、
//! Choking 算法（Leecher/Seeder）、Keep-alive 定时优化、Have 广播、
//! 消息洪泛检测、非活跃连接断开、Allowed Fast Set 计算。

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use xfer_bencode::TorrentMeta;
use xfer_dht::{Dht, DhtConfig};
use xfer_discovery::UdpTracker;
use xfer_storage::{PieceLayout, PieceMap, PieceStore};
use xfer_transport::{UtpManager, UtpManagerHandle, UtpStream};
use xfer_types::{InfoHash, PeerId, ENGINE_NAME, ENGINE_VERSION};

use crate::message::{
    encode_handshake, request_blocks, supports_dht, supports_extension, supports_fast_extension,
    Handshake, Message, PeerReader, BLOCK_SIZE,
};
use crate::mse::EncryptedStream;
use crate::scheduler::{PeerSample, PeerScheduler, PeerSchedulerConfig, ScheduleAction};
use crate::tracker::{announce, AnnounceRequest, AnnounceResponse};

/// 统一 peer 流：明文 TCP 或 MSE 加密流。
/// EncryptedStream 已实现 AsyncRead + AsyncWrite，可直接传递给 PeerReader。
#[allow(clippy::large_enum_variant)]
pub enum PeerStream {
    Plain(TcpStream),
    Encrypted(EncryptedStream<TcpStream>),
    PlainUtp(UtpStream),
    EncryptedUtp(EncryptedStream<UtpStream>),
}

/// 对端连接的传输协议（PeerInfo.protocol 展示 + 拨号策略）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Tcp,
    Utp,
}

impl TransportKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TransportKind::Tcp => "tcp",
            TransportKind::Utp => "utp",
        }
    }
}

/// 把具体传输（TcpStream / UtpStream）包装成统一的 [`PeerStream`]。
///
/// run_peer 泛型于传输类型，握手完成后经此 trait 收敛到 PeerStream 枚举，
/// 后续读写/消息循环对传输类型无感。
trait IntoPeerStream: Sized {
    const TRANSPORT: TransportKind;
    fn into_plain(self) -> PeerStream;
    fn into_encrypted(enc: EncryptedStream<Self>) -> PeerStream;
}

impl IntoPeerStream for TcpStream {
    const TRANSPORT: TransportKind = TransportKind::Tcp;
    fn into_plain(self) -> PeerStream {
        PeerStream::Plain(self)
    }
    fn into_encrypted(enc: EncryptedStream<Self>) -> PeerStream {
        PeerStream::Encrypted(enc)
    }
}

impl IntoPeerStream for UtpStream {
    const TRANSPORT: TransportKind = TransportKind::Utp;
    fn into_plain(self) -> PeerStream {
        PeerStream::PlainUtp(self)
    }
    fn into_encrypted(enc: EncryptedStream<Self>) -> PeerStream {
        PeerStream::EncryptedUtp(enc)
    }
}

/// 实现 AsyncRead：委托给内部流（加密流自动解密）。
impl AsyncRead for PeerStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            PeerStream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            PeerStream::Encrypted(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            PeerStream::PlainUtp(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            PeerStream::EncryptedUtp(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl PeerStream {
    /// 写入数据（加密流自动加密）。
    pub async fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        match self {
            PeerStream::Plain(s) => s.write_all(data).await,
            PeerStream::Encrypted(s) => s.write_encrypted(data).await,
            PeerStream::PlainUtp(s) => s.write_all(data).await,
            PeerStream::EncryptedUtp(s) => s.write_encrypted(data).await,
        }
    }
}

/// 流水线起始深度（§7.8）。
const PIPELINE_MIN: usize = 16;
/// 流水线最大深度（§7.8）。
const PIPELINE_MAX: usize = 256;
/// 单 peer 片队列容量下限：多片并行才能填满带宽延迟积
/// （单片 256KiB 在途 → 200ms RTT 下吞吐被限死在 ~1.3MB/s）。
/// 实际容量按「打满流水线窗口所需片数」动态计算，此为下限。
const MAX_QUEUED_PIECES: usize = 4;
/// 片队列容量硬上限：防止极小片（16KiB）导致占片数爆炸。
const MAX_QUEUED_PIECES_HARD: usize = 64;
/// 连接阶段短超时（§7.8：30s → 10s：冷启动时 tracker 返回的 peer 大量为死地址，
/// 缩短等待让连接槽快速周转，减少"占着槽等超时"的冷启动拖尾）。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// MSE/PE 握手超时：防止对端只连接不发数据时握手无限挂起。
/// 15s → 8s：多数 MSE 协商在 2~3 个 RTT 内完成，缩短让明文对端更快回退。
const MSE_TIMEOUT: Duration = Duration::from_secs(8);
/// uTP 拨号握手等待上限：超时/失败即回退 TCP（保证快速换路）。
/// 5s → 2s：对端不支持 uTP 时白等 5s 才回退 TCP，冷启动大量 peer 依次卡 5s。
const UTP_DIAL_TIMEOUT: Duration = Duration::from_secs(2);
/// 冷启动突发倍数（§7.8：首轮 3 倍突发）。
const COLD_START_BURST: usize = 3;
/// 冷启动爬坡周期（§7.8：1s）。
const COLD_START_RAMP: Duration = Duration::from_secs(1);
/// 慢速节点淘汰周期。
const SLOW_PEER_INTERVAL: Duration = Duration::from_secs(10);
/// 慢速节点淘汰阈值（bytes/s，低于此速率且有空闲候选时淘汰）。
const SLOW_PEER_THRESHOLD: u64 = 1024;
/// peer 无活动超时（比 KEEPALIVE_INTERVAL 多 60s 余量，避免误杀正常 peer）。
const PEER_IDLE_TIMEOUT: Duration = Duration::from_secs(180);
/// 限速令牌桶 refill 间隔。
const RATE_LIMIT_INTERVAL: Duration = Duration::from_millis(100);
/// Keep-alive 间隔（BEP 3：建议 2 分钟）。
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(120);
/// Choking 算法执行间隔（BEP 3：10 秒一轮）。
const CHOKE_INTERVAL: Duration = Duration::from_secs(10);
/// 消息洪泛检测间隔。
const FLOODING_CHECK_INTERVAL: Duration = Duration::from_secs(30);
/// 消息洪泛阈值（检查区间内 choke/unchoke 或 keep-alive 次数）。
const FLOODING_THRESHOLD: u32 = 10;
/// 非活跃连接断开超时（无数据传输）。
const INACTIVE_TIMEOUT: Duration = Duration::from_secs(120);
/// 互不感兴趣断开超时。
const NO_INTEREST_TIMEOUT: Duration = Duration::from_secs(120);
/// 在途请求块重发超时：发出 request 后超过该时长未收到块，
/// 清空在途表触发重发（aria2 request slot 超时语义）。
/// 覆盖对端静默丢弃请求的场景（无 RejectRequest 可依赖）。
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// 续传控制文件节流间隔：片完成频繁时最多每秒写一次；暂停/停止强制写入。
const RESUME_SAVE_INTERVAL: Duration = Duration::from_secs(1);
/// PEX 发送间隔（BEP 11：约 1 分钟）。
const PEX_INTERVAL: Duration = Duration::from_secs(60);

/// 已断开 peer 快照保留上限（getPeers 的 disconnected 分组）。
const MAX_DISCONNECTED_PEERS: usize = 32;
/// Allowed Fast Set 大小。
const ALLOWED_FAST_SET_SIZE: usize = 10;
/// 同一地址连续拨号/会话失败的重试上限——超过后不再回填 pending，
/// 等 tracker/PEX/DHT 重新发现该地址时清零计数。
const MAX_DIAL_RETRIES: u32 = 3;

// ---- ut_metadata（BEP 9） ----
/// 本端为 ut_metadata 分配的扩展消息 ID（BEP 10 扩展握手 "m" 字典）。
const UT_METADATA_EXT_ID: u8 = 2;
/// 元数据分片大小（BEP 9：固定 16KB，最后一片可能不足）。
const METADATA_PIECE_SIZE: usize = 16 * 1024;
/// 磁力链接获取元数据的整体超时。
const METADATA_TIMEOUT: Duration = Duration::from_secs(60);
/// 元数据 total_size 上限（主流客户端元数据普遍 < 1MB，
/// 4MB 覆盖极端多文件种子的 info 字典；恶意声明更大会被直接拒绝）。
const MAX_METADATA_SIZE: usize = 4 * 1024 * 1024;
/// 磁力元数据获取阶段的 announce 补充间隔（秒）。
const METADATA_ANNOUNCE_INTERVAL: u64 = 5;
/// 元数据分片流水线深度：一次向同一 peer 在途请求多个分片（BEP 9 允许）。
/// 避免大 metadata 逐片串行请求（每片一个 RTT），冷启动大幅提速。
const METADATA_PIPELINE: usize = 8;

/// ut_metadata 消息类型（BEP 9 msg_type 字段）。
const UT_METADATA_REQUEST: i64 = 0;
const UT_METADATA_DATA: i64 = 1;
const UT_METADATA_REJECT: i64 = 2;

/// 无元数据阶段（磁力链接）的元数据分片累积器。
#[derive(Debug, Default)]
struct MetadataAccum {
    /// 已收到的分片：piece 索引 → 原始字节。
    pieces: BTreeMap<usize, Vec<u8>>,
    /// 元数据总大小（第一个 data 消息的 total_size 提供）。
    expected_size: Option<usize>,
    /// 已请求过的分片索引（避免重复请求）。
    requested: HashSet<usize>,
}

impl MetadataAccum {
    /// 从期望大小计算分片数。
    fn piece_count(&self) -> Option<usize> {
        self.expected_size
            .map(|sz| sz.div_ceil(METADATA_PIECE_SIZE))
    }

    /// 清空累积器（解析失败 / info_hash 不匹配 / total_size 冲突时），
    /// 防止残留分片毒化后续会话的重新收集。
    fn reset(&mut self) {
        self.pieces.clear();
        self.expected_size = None;
        self.requested.clear();
    }

    /// 完整收集后拼接并返回元数据原始字节。
    fn assemble(&self) -> Option<Vec<u8>> {
        let count = self.piece_count()?;
        if self.pieces.len() != count {
            return None;
        }
        let mut out = Vec::with_capacity(self.expected_size.unwrap_or(0));
        for i in 0..count {
            out.extend_from_slice(self.pieces.get(&i)?);
        }
        Some(out)
    }
}

/// 由引擎配置派生 BT 智能调度器配置（预分配连接数即调度上限）。
fn scheduler_config(cfg: &TorrentConfig) -> PeerSchedulerConfig {
    PeerSchedulerConfig {
        max_peers: cfg.max_peers.max(1),
        min_peers: 2,
        ..Default::default()
    }
}

/// 构造 ut_metadata 请求消息（BEP 9 msg_type=0），以对端协商的 ext_id 发送。
fn ut_metadata_request(peer_meta_id: u8, piece: usize) -> Vec<u8> {
    use std::collections::BTreeMap;
    use xfer_bencode::{encode, Value};
    let mut d = BTreeMap::new();
    d.insert(b"msg_type".to_vec(), Value::Int(UT_METADATA_REQUEST));
    d.insert(b"piece".to_vec(), Value::Int(piece as i64));
    Message::Extended {
        ext_id: peer_meta_id,
        payload: encode(&Value::Dict(d)),
    }
    .encode()
}

/// 判断 ut_metadata payload 是否为 REQUEST（BEP 9 msg_type=0）。
/// 元数据交换阶段收到 REQUEST 走 REJECT 回复而非报错断开。
fn payload_is_metadata_request(payload: &[u8]) -> bool {
    xfer_bencode::decode_prefix(payload)
        .ok()
        .and_then(|(v, _)| {
            v.as_dict().and_then(|d| {
                d.get(b"msg_type".as_slice())
                    .and_then(xfer_bencode::Value::as_int)
                    .map(|t| t == UT_METADATA_REQUEST)
            })
        })
        .unwrap_or(false)
}

/// 加密模式（PE/MSE 策略，对应 aria2 bt-force-encryption 语义扩展为三档）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EncryptionMode {
    /// 优先加密：先尝试 MSE，失败后回退明文（默认，等价旧 `enable_mse: true`）。
    PreferEncryption,
    /// 强制加密：出站不提供明文、失败不回退；入站拒绝明文握手。
    ForceEncryption,
    /// 仅明文：跳过 MSE（等价旧 `enable_mse: false`）。
    PlaintextOnly,
}

impl Default for EncryptionMode {
    fn default() -> Self {
        EncryptionMode::PreferEncryption
    }
}

impl EncryptionMode {
    /// 解析全局选项值：adaptive|prefer / force / plain|plaintext。
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "adaptive" | "prefer" | "auto" => Some(EncryptionMode::PreferEncryption),
            "force" | "forced" => Some(EncryptionMode::ForceEncryption),
            "plain" | "plaintext" | "none" => Some(EncryptionMode::PlaintextOnly),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            EncryptionMode::PreferEncryption => "adaptive",
            EncryptionMode::ForceEncryption => "force",
            EncryptionMode::PlaintextOnly => "plain",
        }
    }

    fn code(self) -> u8 {
        match self {
            EncryptionMode::PreferEncryption => 0,
            EncryptionMode::ForceEncryption => 1,
            EncryptionMode::PlaintextOnly => 2,
        }
    }

    fn from_code(c: u8) -> Self {
        match c {
            1 => EncryptionMode::ForceEncryption,
            2 => EncryptionMode::PlaintextOnly,
            _ => EncryptionMode::PreferEncryption,
        }
    }
}

/// BT peer 传输协议。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BtProtocol {
    /// TCP + uTP：出站 uTP 优先、TCP 兜底；入站两者都接受（默认）。
    TcpAndUtp,
    /// 仅 TCP。
    TcpOnly,
    /// 仅 uTP。
    UtpOnly,
}

impl Default for BtProtocol {
    fn default() -> Self {
        BtProtocol::TcpAndUtp
    }
}

impl BtProtocol {
    /// 解析全局选项值：tcp+utp|both / tcp / utp。
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "tcp+utp" | "utp+tcp" | "both" | "tcp,utp" => Some(BtProtocol::TcpAndUtp),
            "tcp" => Some(BtProtocol::TcpOnly),
            "utp" => Some(BtProtocol::UtpOnly),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            BtProtocol::TcpAndUtp => "tcp+utp",
            BtProtocol::TcpOnly => "tcp",
            BtProtocol::UtpOnly => "utp",
        }
    }

    pub fn allows_tcp(self) -> bool {
        self != BtProtocol::UtpOnly
    }

    pub fn allows_utp(self) -> bool {
        self != BtProtocol::TcpOnly
    }

    fn code(self) -> u8 {
        match self {
            BtProtocol::TcpAndUtp => 0,
            BtProtocol::TcpOnly => 1,
            BtProtocol::UtpOnly => 2,
        }
    }

    fn from_code(c: u8) -> Self {
        match c {
            1 => BtProtocol::TcpOnly,
            2 => BtProtocol::UtpOnly,
            _ => BtProtocol::TcpAndUtp,
        }
    }
}

/// 引擎配置。
#[derive(Debug, Clone)]
pub struct TorrentConfig {
    /// 下载根目录。
    pub dir: PathBuf,
    /// 本端 peer id。
    pub peer_id: PeerId,
    /// 监听端口（0 = 不监听）。
    pub listen_port: u16,
    /// 最大并发 peer 连接数（预分配连接数，来自 `bt-max-peers`）。
    ///
    /// 与 HTTP 的 `split` 相互独立：BT 的连接对象是对等节点而非 Range 分片。
    pub max_peers: usize,
    /// 是否启用 BT 智能调度（自适应连接数，来自 `bt-adaptive`，默认开）。
    ///
    /// 启用后由 [`crate::scheduler::PeerScheduler`] 按吞吐边际收益动态增减目标
    /// 连接数；关闭则退化为「连满 max_peers + 固定阈值淘汰慢节点」。
    pub adaptive: bool,
    /// tracker announce 的 numwant。
    pub numwant: u32,
    /// HTTP tracker URL 列表（按顺序尝试）。
    pub announce_urls: Vec<String>,
    /// UDP tracker URL 列表（udp:// 前缀）。
    pub udp_announce_urls: Vec<String>,
    /// 单 peer 在途请求流水线深度（0 = 自适应 16→256）。
    pub pipeline: usize,
    /// 是否启用 DHT（磁力链接冷启动需要）。
    pub enable_dht: bool,
    /// DHT 监听端口（0 = 系统分配）。
    pub dht_port: u16,
    /// 加密模式（BEP 8 / PE 策略，默认优先加密）。
    pub encryption: EncryptionMode,
    /// peer 传输协议（TCP / uTP / 两者，默认两者）。
    pub bt_protocol: BtProtocol,
    /// 下载限速（bytes/s，0 = 不限制）。
    pub download_limit: u64,
    /// 上传限速（bytes/s，0 = 不限制）。
    pub upload_limit: u64,
    /// 下载完成后是否切换为 seed 模式（继续做种）。
    pub seed_mode: bool,
    /// seed 模式持续时间（秒，0 = 永久）。
    pub seed_duration: u64,
    /// 文件选择（None = 全部文件；Some = 仅下载这些文件索引）。
    /// 磁力链接解析出文件列表后由用户勾选，未选文件的片不请求。
    pub selected_files: Option<Vec<usize>>,
}

impl Default for TorrentConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("."),
            peer_id: PeerId::azureus_prefix(&[0u8; 12]),
            listen_port: 0,
            max_peers: 50,
            adaptive: true,
            numwant: 50,
            announce_urls: Vec::new(),
            udp_announce_urls: Vec::new(),
            pipeline: 0, // 0 = 自适应
            enable_dht: false,
            dht_port: 0,
            encryption: EncryptionMode::default(),
            bt_protocol: BtProtocol::default(),
            download_limit: 0,
            upload_limit: 0,
            seed_mode: false,
            seed_duration: 0,
            selected_files: None,
        }
    }
}

/// 对端运行时状态（共享于 peer 任务与分配器）。
#[derive(Debug)]
pub struct PeerState {
    pub peer_id: Option<PeerId>,
    /// 对端已拥有片集合。
    pub have: PieceMap,
    /// 磁力模式：元数据未就绪时暂存的对端 bitfield（就绪后应用到 `have`）。
    ///
    /// 元数据未就绪时 `have` 的片数为 0，无法承载 bitfield，必须先暂存，
    /// 否则转下载后对端"一片都没有"→ 选片失败 → 速度恒为 0。
    pub pending_bitfield: Option<Vec<u8>>,
    /// 磁力模式：元数据未就绪时收到的单点 Have 索引。
    pub pending_haves: Vec<u32>,
    /// 磁力模式：元数据未就绪时收到 HaveAll（对端是 seed）。
    pub pending_have_all: bool,
    /// 对端是否 choke 了我们。
    pub choked: bool,
    /// 我们是否已发送 interested。
    pub we_interested: bool,
    pub last_activity: Instant,
    pub is_seed: bool,
    /// 是否通过 MSE 加密连接。
    pub encrypted: bool,
    /// 连接建立时间（用于慢速节点淘汰评估）。
    pub connected_at: Instant,
    /// 最近一次下载速率采样（bytes/s）。
    pub recent_speed: u64,
    /// 我们是否 choke 了对端（seed 模式用）。
    pub we_choked: bool,
    /// 对端是否对我们 interested。
    pub peer_interested: bool,
    /// 对端是否支持 Fast Extension (BEP 6)。
    pub fast_extension: bool,
    /// 对端是否支持扩展协议 (BEP 10)。
    pub extended_messaging: bool,
    /// 对端是否支持 DHT (BEP 5)。
    pub dht_enabled: bool,
    /// 对端 ut_pex 扩展消息 ID（0 = 未协商）。
    pub ut_pex_id: u8,
    /// 本端为对端分配的 ut_pex 扩展消息 ID。
    pub our_ut_pex_id: u8,
    /// 对端 ut_metadata 扩展消息 ID（BEP 9，0 = 未协商）。
    pub ut_metadata_id: u8,
    /// 本端为对端分配的 ut_metadata 扩展消息 ID。
    pub our_ut_metadata_id: u8,
    /// Allowed Fast 集合（对端允许我们快速下载的片索引）。
    pub allowed_fast_set: HashSet<u32>,
    /// 本端已向对端发送的 Allowed Fast 集合。
    pub am_allowed_fast_set: HashSet<u32>,
    /// 最近一次数据传输时间（用于非活跃检测）。
    pub last_data_transfer: Instant,
    /// 消息洪泛统计：choke/unchoke 切换次数。
    pub choke_unchoke_count: u32,
    /// 消息洪泛统计：keep-alive 次数。
    pub keepalive_count: u32,
    /// 洪泛检查的起始时间。
    pub flooding_check_at: Instant,
    /// 最近一次发送 keep-alive 的时间。
    pub last_keepalive: Instant,
    /// 最近一次被本端 unchoke 的时间（做种模式上传轮转的排序依据——
    /// 全员速率为 0 时按「最久未服务优先」选出常规 unchoke 集合）。
    pub last_unchoke: Instant,
    /// chokingRequired：choking 算法标记此 peer 应被 choke。
    pub choking_required: bool,
    /// optUnchoking：此 peer 被乐观 unchoke 选中。
    pub opt_unchoking: bool,
    /// 对端 BEP 10 扩展握手上报的客户端版本（"v" 字段，如
    /// "qBittorrent/4.6.0"）。比 peer_id 前缀解析更精确，显示优先。
    pub client_version: Option<String>,
}

/// Peer 来源（发现渠道）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum PeerSource {
    /// Tracker announce 获取的 peer。
    Tracker,
    /// DHT get_peers 获取的 peer。
    Dht,
    /// PEX（Peer Exchange，BEP 11）从其他 peer 交换获得。
    Pex,
    /// 被动入站连接（对端主动连入）。
    Incoming,
}

impl PeerSource {
    pub fn as_str(self) -> &'static str {
        match self {
            PeerSource::Tracker => "tracker",
            PeerSource::Dht => "dht",
            PeerSource::Pex => "pex",
            PeerSource::Incoming => "incoming",
        }
    }
}

/// 乐观 unchoke 引擎级状态。
#[derive(Debug, Clone, Copy)]
struct OptimisticUnchoke {
    addr: SocketAddr,
    /// 选中时所处的 10s 窗口序号。每逢 `窗口 % 3 == 0` 且序号不同即重掷，
    /// lucky peer 保持 unchoke 约 30s（3 轮）后回收轮换。
    window: u64,
}

/// 出站拨号结果（失败地址回填 pending 重试语义）。
enum DialOutcome {
    /// 拨号失败（uTP+TCP 均未建立会话）或会话因错误中断——
    /// 引擎未停机且未主动淘汰时回填 pending 重试。
    Failed,
    /// 会话正常结束（含引擎停机 / 引擎主动淘汰）——不重试。
    Done,
}

/// 对端公开信息（RPC getPeers 用）。
#[derive(Debug, Clone, serde::Serialize)]
pub struct PeerInfo {
    pub addr: String,
    pub peer_id: Option<String>,
    /// 客户端及版本：BEP 10 "v" 字段优先（"qBittorrent/4.6.0"），
    /// 否则 peer_id 前缀解析（"qBittorrent 4.5.7"）。
    pub client: String,
    pub choked: bool,
    pub interested: bool,
    pub seed: bool,
    pub downloaded: u64,
    pub encrypted: bool,
    /// 是否当前在线（false = 本会话已断开，保留最后快照，参考 C++ 版
    /// Peer::releaseSessionResource 的 disconnected 分组语义）。
    pub connected: bool,
    /// peer 来源（tracker / dht / pex / incoming）。
    pub source: PeerSource,
    /// 本端已向该对端上传的字节数（serve_block 逐块累计）。
    pub uploaded: u64,
    /// 传输协议。当前引擎 peer 连接恒为 "tcp"，为将来 uTP 预留。
    pub protocol: String,
    /// 连接持续时长（秒）。断开快照中为断开时刻的时长。
    pub connected_secs: u64,
    /// 对端下载进度百分比（0-100）。磁力元数据未就绪、位图未知时为 None。
    pub progress: Option<f32>,
}

struct PeerCell {
    addr: SocketAddr,
    state: Mutex<PeerState>,
    downloaded: AtomicU64,
    /// 本端已向该对端上传的字节数（与引擎级 `uploaded_bytes` 同步累计）。
    uploaded: AtomicU64,
    /// 上次速率采样时的 downloaded 快照。
    prev_downloaded: Mutex<u64>,
    /// 本 peer 占用的片队列（头 = 正在请求的片）。
    /// 单片上限 = 片大小（常见 256KiB），高延迟链路上远小于带宽延迟积，
    /// 吞吐被钉死（实测 ~1MB/s）；多片并行才能填满流水线。
    queued: Mutex<Vec<u32>>,
    /// 自适应流水线深度。
    pipeline: Mutex<usize>,
    /// 最近一次收到 block 的时间（用于 RTT 估算）。
    last_block_at: Mutex<Instant>,
    /// peer 来源（发现渠道）。
    source: PeerSource,
    /// 连接传输协议（TCP/uTP），连接建立时确定。
    transport: Mutex<TransportKind>,
    /// 待发送给该 peer 的 Have 片号队列：
    /// `broadcast_have` 入队，该 peer 的消息循环每轮出队发送。
    have_out_queue: Mutex<Vec<u32>>,
    /// 逐 peer 停机信号：淘汰/剪除/注销时置位，会话立即退出。
    /// 缺了它，被注销的会话仍在后台下载（选片不查 peers 表）——
    /// 淘汰只是"除名"不是"断开"，连接泄漏且调度换血全部空转。
    kill: CancellationToken,
}

/// 每 peer 任务持有的下载上下文。
struct PeerCtx {
    cell: Arc<PeerCell>,
    /// 在途请求集合：(片号, 块偏移)。
    in_flight: HashSet<(u32, u32)>,
    /// 已收到块缓存：(片号, 块偏移) → 数据。
    blocks: HashMap<(u32, u32), Vec<u8>>,
    /// 片 → 应收块数（入队时按片长登记）。
    block_need: HashMap<u32, u32>,
    /// 片 → 已收块数（首次收到的块计数；重发块去重不计）。
    block_have: HashMap<u32, u32>,
    /// 最近一次发送请求的时间（用于 RTT 估算）。
    last_request_at: Option<Instant>,
    /// 连续无进展计数（用于检测卡死的 peer）。
    stale_count: u32,
}

/// 进度快照。
#[derive(Debug, Clone, Copy)]
pub struct TorrentProgress {
    pub done: u64,
    pub total: u64,
}

/// 限速桶容量下限：桶容量 = max(rate, 该值)。
/// 块大小固定 16 KiB（下载）/ 请求上限 64 KiB（上传）；若桶容量 = rate，
/// 当 rate < 块长时 `try_consume(块长)` 永远凑不齐令牌 → 限速循环死循环
/// （容量至少能装下一个最大块）。
const RATE_LIMITER_MIN_BURST: u64 = 64 * 1024;

/// 令牌桶限速器。
struct RateLimiter {
    /// 每秒令牌数（bytes/s）。
    rate: u64,
    /// 当前可用令牌。
    tokens: u64,
    /// 上次 refill 时间。
    last_refill: Instant,
}

impl RateLimiter {
    fn new(rate: u64) -> Self {
        Self {
            rate,
            tokens: rate,
            last_refill: Instant::now(),
        }
    }

    /// 桶容量：保证不小于最大块长，否则低速率下永远无法消费整块。
    fn capacity(&self) -> u64 {
        self.rate.max(RATE_LIMITER_MIN_BURST)
    }

    /// 尝试消费 `n` 字节；若不足则返回需要等待的时间。
    fn try_consume(&mut self, n: u64) -> Option<Duration> {
        self.refill();
        if self.rate == 0 {
            return None; // 不限制
        }
        if self.tokens >= n {
            self.tokens -= n;
            None
        } else {
            // 计算等待时间
            let deficit = n - self.tokens;
            let wait_ms = (deficit * 1000) / self.rate;
            Some(Duration::from_millis(wait_ms.max(1)))
        }
    }

    /// 按时间流逝补充令牌。
    fn refill(&mut self) {
        let now = Instant::now();
        if self.rate == 0 {
            return;
        }
        let elapsed = now.duration_since(self.last_refill);
        if elapsed < RATE_LIMIT_INTERVAL {
            return;
        }
        let new_tokens = (self.rate as u128 * elapsed.as_millis()) / 1000;
        self.tokens = (self.tokens + new_tokens as u64).min(self.capacity());
        self.last_refill = now;
    }

    /// 运行时调整限速值（全局选项变更时下发到活动引擎）。
    fn set_rate(&mut self, rate: u64) {
        self.refill();
        self.rate = rate;
        self.tokens = self.tokens.min(self.capacity().max(1));
    }
}

/// BT 下载引擎。
pub struct TorrentEngine {
    /// 元数据（磁力模式在 ut_metadata 获取后从 None 变为 Some）。
    meta: RwLock<Option<TorrentMeta>>,
    config: TorrentConfig,
    client: reqwest::Client,
    peer_id: PeerId,
    /// 固定的 info_hash（磁力模式来自链接解析；.torrent 模式来自元信息）。
    info_hash: [u8; 20],
    /// 实际监听端口（spawn_listener 后从 bind 获取，0 = 未监听）。
    actual_listen_port: std::sync::atomic::AtomicU16,
    /// 文件存储（元数据就绪前为 None）。
    store: Mutex<Option<PieceStore>>,
    /// 磁力模式的元数据分片累积器。
    metadata: Mutex<MetadataAccum>,
    /// 已分配未完成片。
    assigned: Mutex<HashSet<u32>>,
    /// 待连接 peer 地址及其来源。
    pending: Mutex<HashMap<SocketAddr, PeerSource>>,
    peers: RwLock<HashMap<SocketAddr, Arc<PeerCell>>>,
    done_bytes: AtomicU64,
    total_bytes: AtomicU64,
    finished: AtomicBool,
    /// 最近一次成功 announce 的 interval（秒）。
    last_interval: AtomicU64,
    /// 全局下载限速器。
    rate_limiter: Mutex<RateLimiter>,
    /// 全局上传限速器（响应 peer 请求发送块前消费令牌）。
    upload_limiter: Mutex<RateLimiter>,
    /// 当前下载限速值（bytes/s，0 = 不限）：运行时可经
    /// [`Self::set_rate_limits`] 调整，热路径无锁读。
    dl_limit: AtomicU64,
    /// 当前上传限速值（bytes/s，0 = 不限）。
    ul_limit: AtomicU64,
    /// 累计上传字节数（实际发出的 piece 数据）。
    uploaded_bytes: AtomicU64,
    /// 冷启动突发标记（首轮连接后清除）。
    cold_start_done: AtomicBool,
    /// BT 智能调度器（按吞吐边际收益动态调整目标连接数）。
    scheduler: Mutex<PeerScheduler>,
    /// choking 决策纪元（轮次由墙钟推导 `(elapsed/10s)%3`，所有会话在
    /// 同一 10s 窗口内看到一致的轮次——原先的全局计数器会被每个 peer 的
    /// 10s tick 各推一次（50 peer = 每 10s 推 50 次），乐观 unchoke 节奏失真）。
    choke_epoch: Instant,
    /// 乐观 unchoke 当前选中项（引擎级共享状态。lucky peer 自己的 tick
    /// 算出的 unchoke_set 必然包含自己 → 由其会话真正发出 Unchoke——原先
    /// 只写 `opt_unchoking` 标志却无任何代码读取，乐观 unchoke 名存实亡）。
    optimistic_unchoke: Mutex<Option<OptimisticUnchoke>>,
    /// 出站拨号失败/会话异常中断计数（失败地址回填 pending 重试，
    /// 超过次数放弃，等 tracker/PEX 重新发现时清零）。
    dial_failures: Mutex<HashMap<SocketAddr, u32>>,
    /// 本会话已断开 peer 的最后快照（getPeers 的 disconnected 分组），
    /// 上限 [`MAX_DISCONNECTED_PEERS`]，超出淘汰最旧。
    disconnected_peers: Mutex<Vec<PeerInfo>>,
    /// 续传控制文件上次写入时间（节流：片完成时最多 1s 写一次）。
    last_resume_save: Mutex<Instant>,
    /// 引擎级停机信号：暂停/取消/完成时置位，所有后台任务
    /// （peer 会话、监听器、连接派发）必须监听它立即退出，
    /// 否则暂停后僵尸任务继续下载并更新续传文件 → 恢复时进度跳变。
    shutdown: CancellationToken,
    /// run() 中创建的 DHT 实例（停机时同步关闭，避免跨暂停泄漏）。
    dht: Mutex<Option<Arc<Dht>>>,
    /// uTP 管理器（与 TCP 同端口监听，出站拨号 + 入站连接复用）。
    utp: Mutex<Option<UtpManagerHandle>>,
    /// 当前加密模式（运行时可经 [`Self::set_bt_modes`] 热切换，拨号/握手路径无锁读）。
    encryption_mode: std::sync::atomic::AtomicU8,
    /// 当前传输协议模式（运行时可热切换，拨号路径无锁读）。
    bt_protocol_mode: std::sync::atomic::AtomicU8,
    /// 运行时动态注入的 announce URL（订阅源刷新 / 用户热添加）：
    /// 与 `config` 的静态列表合并去重后使用。引擎启动后 announce
    /// 列表被克隆进配置快照，若无此机制，新增 tracker 只能等
    /// 暂停/恢复重建引擎后才生效。
    dynamic_announces: Mutex<DynamicAnnounces>,
    /// 运行时文件选择（None = 全部文件；Some = 仅下载这些文件索引）。
    /// 初始值来自 [`TorrentConfig::selected_files`]，可经
    /// [`Self::set_selected_files`] 热更新。
    selected_files: Mutex<Option<Vec<usize>>>,
    /// 由 `selected_files` 推导的所需片位图（None = 全量）。
    /// 元数据就绪 / 文件选择变更时重算。
    wanted: Mutex<Option<PieceMap>>,
}

/// 动态 announce 列表：按协议分流（http / udp）。
#[derive(Default)]
struct DynamicAnnounces {
    http: Vec<String>,
    udp: Vec<String>,
}

impl TorrentEngine {
    /// 创建引擎：解析布局、打开存储、续传标记。
    pub fn new(meta: TorrentMeta, config: TorrentConfig) -> Result<Arc<Self>, String> {
        let files: Vec<(Vec<String>, u64)> = meta
            .info
            .files
            .iter()
            .map(|f| (f.path.clone(), f.length))
            .collect();
        let layout = PieceLayout::new(meta.info.piece_length, files);
        let total_bytes = layout.total_length();
        let mut store = PieceStore::open(&config.dir, &meta.info.name, layout)
            .map_err(|e| format!("打开 piece 存储失败: {e}"))?;

        // 续传：优先从续传控制文件恢复已校验片位图（暂停/重启续传）；
        // 无控制文件但文件已全部完整（手动拷贝/旧任务）→ 按长度全部标记完成。
        let data_path = config.dir.join(&meta.info.name);
        let restored = restore_resume(
            &xfer_storage::ctrl_path(&data_path),
            &meta.info_hash,
            &config.dir,
            &meta.info.name,
            &mut store,
        );
        if !restored {
            let complete = meta.info.files.iter().all(|f| {
                let path = if meta.info.files.len() == 1 {
                    config.dir.join(&meta.info.name)
                } else {
                    config
                        .dir
                        .join(&meta.info.name)
                        .join(f.path.iter().collect::<PathBuf>())
                };
                std::fs::metadata(&path)
                    .map(|m| m.len() >= f.length)
                    .unwrap_or(false)
            });
            if complete {
                store.mark_all_done();
            }
        }

        let peer_id = config.peer_id;
        let download_limit = config.download_limit;
        let upload_limit = config.upload_limit;
        let selected_files_init = config.selected_files.clone();
        let info_hash = meta.info_hash;
        let sched_cfg = scheduler_config(&config);
        let encryption_code = config.encryption.code();
        let protocol_code = config.bt_protocol.code();
        let engine = Arc::new(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .connect_timeout(Duration::from_secs(10))
                .user_agent(concat!("XferRust/", env!("CARGO_PKG_VERSION")))
                .build()
                .map_err(|e| format!("HTTP 客户端构建失败: {e}"))?,
            meta: RwLock::new(Some(meta)),
            config,
            peer_id,
            info_hash,
            actual_listen_port: std::sync::atomic::AtomicU16::new(0),
            store: Mutex::new(Some(store)),
            metadata: Mutex::new(MetadataAccum::default()),
            assigned: Mutex::new(HashSet::new()),
            pending: Mutex::new(HashMap::new()),
            peers: RwLock::new(HashMap::new()),
            done_bytes: AtomicU64::new(0),
            total_bytes: AtomicU64::new(total_bytes),
            finished: AtomicBool::new(false),
            last_interval: AtomicU64::new(60),
            rate_limiter: Mutex::new(RateLimiter::new(download_limit)),
            upload_limiter: Mutex::new(RateLimiter::new(upload_limit)),
            dl_limit: AtomicU64::new(download_limit),
            ul_limit: AtomicU64::new(upload_limit),
            uploaded_bytes: AtomicU64::new(0),
            cold_start_done: AtomicBool::new(false),
            scheduler: Mutex::new(PeerScheduler::new(sched_cfg)),
            choke_epoch: Instant::now(),
            optimistic_unchoke: Mutex::new(None),
            dial_failures: Mutex::new(HashMap::new()),
            disconnected_peers: Mutex::new(Vec::new()),
            last_resume_save: Mutex::new(Instant::now() - Duration::from_secs(60)),
            shutdown: CancellationToken::new(),
            dht: Mutex::new(None),
            utp: Mutex::new(None),
            encryption_mode: std::sync::atomic::AtomicU8::new(encryption_code),
            bt_protocol_mode: std::sync::atomic::AtomicU8::new(protocol_code),
            dynamic_announces: Mutex::new(DynamicAnnounces::default()),
            selected_files: Mutex::new(selected_files_init),
            wanted: Mutex::new(None),
        });
        engine.refresh_done_bytes();
        engine.apply_selection();
        Ok(engine)
    }

    /// 从磁力链接（仅 info_hash）创建引擎：无元数据，先经 ut_metadata（BEP 9）
    /// 从 peer 获取元数据，再初始化存储进入正常下载。
    pub fn new_magnet(info_hash: [u8; 20], config: TorrentConfig) -> Result<Arc<Self>, String> {
        let peer_id = config.peer_id;
        let download_limit = config.download_limit;
        let upload_limit = config.upload_limit;
        let selected_files_init = config.selected_files.clone();
        let sched_cfg = scheduler_config(&config);
        let encryption_code = config.encryption.code();
        let protocol_code = config.bt_protocol.code();
        let engine = Arc::new(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .connect_timeout(Duration::from_secs(10))
                .user_agent(concat!("XferRust/", env!("CARGO_PKG_VERSION")))
                .build()
                .map_err(|e| format!("HTTP 客户端构建失败: {e}"))?,
            meta: RwLock::new(None),
            config,
            peer_id,
            info_hash,
            actual_listen_port: std::sync::atomic::AtomicU16::new(0),
            store: Mutex::new(None),
            metadata: Mutex::new(MetadataAccum::default()),
            assigned: Mutex::new(HashSet::new()),
            pending: Mutex::new(HashMap::new()),
            peers: RwLock::new(HashMap::new()),
            done_bytes: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            finished: AtomicBool::new(false),
            last_interval: AtomicU64::new(60),
            rate_limiter: Mutex::new(RateLimiter::new(download_limit)),
            upload_limiter: Mutex::new(RateLimiter::new(upload_limit)),
            dl_limit: AtomicU64::new(download_limit),
            ul_limit: AtomicU64::new(upload_limit),
            uploaded_bytes: AtomicU64::new(0),
            cold_start_done: AtomicBool::new(false),
            scheduler: Mutex::new(PeerScheduler::new(sched_cfg)),
            choke_epoch: Instant::now(),
            optimistic_unchoke: Mutex::new(None),
            dial_failures: Mutex::new(HashMap::new()),
            disconnected_peers: Mutex::new(Vec::new()),
            last_resume_save: Mutex::new(Instant::now() - Duration::from_secs(60)),
            shutdown: CancellationToken::new(),
            dht: Mutex::new(None),
            utp: Mutex::new(None),
            encryption_mode: std::sync::atomic::AtomicU8::new(encryption_code),
            bt_protocol_mode: std::sync::atomic::AtomicU8::new(protocol_code),
            dynamic_announces: Mutex::new(DynamicAnnounces::default()),
            selected_files: Mutex::new(selected_files_init),
            wanted: Mutex::new(None),
        });
        Ok(engine)
    }

    /// 元数据是否已就绪。
    pub fn has_metadata(&self) -> bool {
        self.meta.read().unwrap().is_some()
    }

    /// 元数据就绪后安装：初始化 piece 存储与总大小，续传标记。
    pub fn install_metadata(&self, meta: TorrentMeta) -> Result<(), String> {
        // 校验 info_hash 与磁力链接一致
        if meta.info_hash != self.info_hash {
            return Err("元数据 info_hash 与磁力链接不一致".into());
        }
        let files: Vec<(Vec<String>, u64)> = meta
            .info
            .files
            .iter()
            .map(|f| (f.path.clone(), f.length))
            .collect();
        let layout = PieceLayout::new(meta.info.piece_length, files);
        let total_bytes = layout.total_length();
        let mut store = PieceStore::open(&self.config.dir, &meta.info.name, layout)
            .map_err(|e| format!("打开 piece 存储失败: {e}"))?;

        // 续传：优先从续传控制文件恢复已校验片位图；
        // 无控制文件但文件已全部完整 → 按长度全部标记完成。
        let data_path = self.config.dir.join(&meta.info.name);
        let restored = restore_resume(
            &xfer_storage::ctrl_path(&data_path),
            &meta.info_hash,
            &self.config.dir,
            &meta.info.name,
            &mut store,
        );
        if !restored {
            let complete = meta.info.files.iter().all(|f| {
                let path = if meta.info.files.len() == 1 {
                    self.config.dir.join(&meta.info.name)
                } else {
                    self.config
                        .dir
                        .join(&meta.info.name)
                        .join(f.path.iter().collect::<PathBuf>())
                };
                std::fs::metadata(&path)
                    .map(|m| m.len() >= f.length)
                    .unwrap_or(false)
            });
            if complete {
                store.mark_all_done();
            }
        }
        *self.store.lock().unwrap() = Some(store);
        self.total_bytes.store(total_bytes, Ordering::Relaxed);
        {
            let mut m = self.meta.write().unwrap();
            *m = Some(meta);
        }
        // 片数已知后，把各 peer 在元数据阶段暂存的 bitfield/have 落到 have，
        // 否则这些 peer 的片集合仍为空 → 选片失败 → "有节点无速度"。
        self.apply_pending_peer_bitfields();
        self.refresh_done_bytes();
        // 文件选择（磁力解析后勾选）：就绪后立即生效
        self.apply_selection();
        tracing::info!(total_bytes, "磁力元数据就绪，进入正常下载");
        Ok(())
    }

    /// 按当前 `selected_files` 重算所需片位图、总量与已完成字节数。
    ///
    /// None（全量）时位图保持 None、总量为全部文件长度；有选择时总量
    /// 收缩为所选文件长度，未覆盖文件的片不参与选片/完成判定。
    fn apply_selection(&self) {
        let sel = self.selected_files.lock().unwrap().clone();
        let mask = {
            let guard = self.store.lock().unwrap();
            match guard.as_ref() {
                Some(store) => store.layout().wanted_piece_mask(sel.as_deref()),
                None => return,
            }
        };
        let total = {
            let guard = self.store.lock().unwrap();
            guard
                .as_ref()
                .map(|s| s.layout().selected_length(sel.as_deref()))
                .unwrap_or(0)
        };
        *self.wanted.lock().unwrap() = mask;
        self.total_bytes.store(total, Ordering::Relaxed);
        self.refresh_done_bytes();
    }

    /// 某片是否需要下载（无选择 = 全部需要）。
    fn piece_wanted(&self, idx: u32) -> bool {
        match self.wanted.lock().unwrap().as_ref() {
            None => true,
            Some(m) => m.is_set(idx),
        }
    }

    /// 所需片是否全部完成（文件选择下的完成判定）。
    fn all_wanted_done(&self) -> bool {
        let guard = self.store.lock().unwrap();
        let Some(store) = guard.as_ref() else {
            return false;
        };
        self.all_wanted_done_with(store)
    }

    /// [`Self::all_wanted_done`] 的无 store 加锁版本：调用方已持 store 锁时
    /// 使用（std Mutex 不可重入，重复加锁自死锁）。
    fn all_wanted_done_with(&self, store: &PieceStore) -> bool {
        let wanted = self.wanted.lock().unwrap();
        match wanted.as_ref() {
            None => store.map().all_done(),
            Some(m) => (0..m.count()).all(|i| !m.is_set(i) || store.map().is_set(i)),
        }
    }

    /// 运行时更新文件选择（None = 全部文件）。
    ///
    /// 立即重算位图/总量/已完成字节数；已在途的未选片请求会自然完成
    /// 落盘（幂等无害），后续选片不再分配未选文件的片。
    pub fn set_selected_files(&self, files: Option<Vec<usize>>) {
        *self.selected_files.lock().unwrap() = files;
        self.apply_selection();
    }

    /// 启动监听（端口 0 = 系统自动分配，避免多任务冲突）。
    async fn spawn_listener(self: &Arc<Self>) -> Result<(), String> {
        match TcpListener::bind(("0.0.0.0", self.config.listen_port)).await {
            Ok(listener) => {
                let port = listener.local_addr().map(|a| a.port()).unwrap_or(0);
                self.actual_listen_port.store(port, Ordering::Relaxed);
                let engine = self.clone();
                let shutdown = self.shutdown.clone();
                tokio::spawn(async move {
                    loop {
                        // 停机信号优先：暂停/完成后不再接受新连接
                        let accepted = tokio::select! {
                            _ = shutdown.cancelled() => break,
                            r = listener.accept() => r,
                        };
                        match accepted {
                            Ok((stream, addr)) => {
                                let e = engine.clone();
                                tokio::spawn(async move {
                                    if let Err(e2) = e.run_peer(addr, stream, false, None).await {
                                        tracing::debug!(peer = %addr, error = %e2, "被动连接结束");
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "accept 失败");
                                tokio::time::sleep(Duration::from_millis(200)).await;
                            }
                        }
                    }
                });
                tracing::info!(port, "BT 监听已启动");

                // uTP 同端口监听（§7.6）。始终绑定，模式只决定是否接受/拨号，
                // 以便运行时热切换协议；UDP 绑定失败则本引擎禁用 uTP。
                match UtpManager::bind("0.0.0.0", port).await {
                    Ok((handle, incoming_rx)) => {
                        *self.utp.lock().unwrap() = Some(handle);
                        let engine = self.clone();
                        let shutdown = self.shutdown.clone();
                        tokio::spawn(async move {
                            engine.utp_incoming_loop(incoming_rx, shutdown).await;
                        });
                        tracing::info!(port, "uTP 监听已启动");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "uTP 端口绑定失败，本引擎禁用 uTP");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "BT 监听端口绑定失败，降级为不监听");
            }
        }
        Ok(())
    }

    /// uTP 入站连接循环：从管理器接收新连接，按当前协议模式过滤后交给
    /// [`Self::run_peer`]（与 TCP accept 路径等价）。
    async fn utp_incoming_loop(
        self: Arc<Self>,
        mut incoming_rx: tokio::sync::mpsc::Receiver<UtpStream>,
        shutdown: CancellationToken,
    ) {
        loop {
            let stream = tokio::select! {
                _ = shutdown.cancelled() => break,
                s = incoming_rx.recv() => s,
            };
            let Some(stream) = stream else { break };
            // 协议模式不含 uTP 时不接受入站（热切换实时生效）
            if !self.bt_protocol().allows_utp() {
                continue;
            }
            let addr = stream.remote_addr();
            let e = self.clone();
            tokio::spawn(async move {
                if let Err(e2) = e.run_peer(addr, stream, false, None).await {
                    tracing::debug!(peer = %addr, error = %e2, "uTP 被动连接结束");
                }
            });
        }
    }

    /// 运行直到下载完成或取消。
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) -> Result<(), String> {
        self.spawn_listener().await?;

        // DHT 初始化（如果启用）
        let dht = if self.config.enable_dht {
            match Dht::new(DhtConfig {
                listen_port: self.config.dht_port,
                bind_addr: Some("0.0.0.0".into()),
                ..Default::default()
            })
            .await
            {
                Ok(dht) => {
                    *self.dht.lock().unwrap() = Some(dht.clone());
                    dht.spawn_background();
                    tracing::info!("DHT 已启动，后台 bootstrap");
                    let dht_clone = dht.clone();
                    tokio::spawn(async move {
                        if let Err(e) = dht_clone.bootstrap().await {
                            tracing::warn!(error = %e, "DHT bootstrap 失败");
                        }
                    });
                    Some(dht)
                }
                Err(e) => {
                    tracing::warn!(error = %e, "DHT 初始化失败，将仅依赖 tracker");
                    None
                }
            }
        } else {
            None
        };

        // 首次 announce（started）
        let mut last_announce = Instant::now() - Duration::from_secs(120);
        if let Some(r) = self.announce_all(Some("started")).await {
            self.add_peers(r.peers, PeerSource::Tracker).await;
            last_announce = Instant::now();
        }

        // 首次 DHT get_peers（后台执行，不阻塞主流程）
        let mut last_dht = Instant::now() - Duration::from_secs(120);
        if let Some(d) = dht.as_ref() {
            // DHT bootstrap 可能需要时间，不阻塞 cold_start
            let engine_ref = self.clone();
            let dht_clone = d.clone();
            tokio::spawn(async move {
                engine_ref.dht_get_peers(&dht_clone).await;
            });
            // 给 DHT 一点 bootstrap 时间
            tokio::time::sleep(Duration::from_millis(100)).await;
            last_dht = Instant::now();
        }

        // 冷启动突发连接：首轮 3 倍突发
        self.cold_start_burst_connect().await;

        // 磁力模式：元数据未就绪时先经 ut_metadata 获取（冷启动连接已在路上）
        if !self.has_metadata() {
            if let Err(e) = self.fetch_metadata(&cancel).await {
                self.stop_background();
                return Err(e);
            }
        }

        let mut tick = tokio::time::interval(COLD_START_RAMP);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut slow_peer_timer = Instant::now();
        let mut speed_sample_timer = Instant::now();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    // 先停掉所有后台任务（peer 会话/监听器/连接派发），
                    // 再持久化续传控制文件：否则僵尸任务在暂停后继续下载、
                    // 更新控制文件 → 恢复时进度凭空跳变
                    self.stop_background();
                    self.save_resume(true);
                    if let Some(store) = self.store.lock().unwrap().as_mut() {
                        store.flush_all().ok();
                    }
                    self.announce_all_sync(Some("stopped"));
                    return Err("BT 任务已取消".into());
                }
                _ = tick.tick() => {
                    if self.is_done() {
                        self.finish();
                        // seed 模式：下载完成后继续做种
                        if self.config.seed_mode {
                            if let Err(e) = self.run_seed_mode(&cancel).await {
                                self.stop_background();
                                return Err(e);
                            }
                        }
                        self.stop_background();
                        return Ok(());
                    }
                    // 低水位重试：无活跃 peer 时缩短 announce 间隔
                    let active = self.peers.read().unwrap().len();
                    let interval = self.last_interval.load(Ordering::Relaxed).max(15);
                    let eff = if active == 0 { 5 } else { interval };
                    if last_announce.elapsed() >= Duration::from_secs(eff) {
                        // announce 最长可阻塞 15s：取消优先，暂停不被 tracker I/O 挡住
                        let r = tokio::select! {
                            biased;
                            _ = cancel.cancelled() => None,
                            r = self.announce_all(None) => r,
                        };
                        if cancel.is_cancelled() {
                            continue; // 下一轮进入取消收尾分支
                        }
                        if let Some(r) = r {
                            self.add_peers(r.peers, PeerSource::Tracker).await;
                        }
                        last_announce = Instant::now();
                    }
                    // DHT get_peers 重试（§7.8：5s/30s/2min 三档）
                    if let Some(dht) = &dht {
                        let active = self.peers.read().unwrap().len();
                        let dht_interval = if active == 0 { 5 } else if active < 5 { 30 } else { 120 };
                        if last_dht.elapsed() >= Duration::from_secs(dht_interval) {
                            // 后台执行，不阻塞主循环
                            let engine_ref = self.clone();
                            let dht_clone = dht.clone();
                            tokio::spawn(async move {
                                engine_ref.dht_get_peers(&dht_clone).await;
                            });
                            last_dht = Instant::now();
                        }
                    }
                    self.connect_pending().await;

                    // 速度采样 + 慢速节点淘汰 / 智能调度
                    if speed_sample_timer.elapsed() >= Duration::from_secs(5) {
                        self.sample_peer_speeds();
                        speed_sample_timer = Instant::now();
                    }
                    if slow_peer_timer.elapsed() >= SLOW_PEER_INTERVAL {
                        if self.config.adaptive {
                            self.run_peer_schedule().await;
                        } else {
                            self.evict_slow_peers();
                        }
                        slow_peer_timer = Instant::now();
                    }
                    self.prune_dead_peers();
                }
            }
        }
    }

    /// 磁力模式：持续 announce + 连接候选 peer，等待 ut_metadata 就绪。
    ///
    /// 后台各 run_peer 任务在元数据未就绪时只做元数据交换，
    /// 收集完成后由 `install_metadata` 置位，本循环随即返回。
    async fn fetch_metadata(self: &Arc<Self>, cancel: &CancellationToken) -> Result<(), String> {
        let deadline = Instant::now() + METADATA_TIMEOUT;
        tracing::info!(
            "磁力模式：等待从 peer 获取元数据（上限 {:?}）",
            METADATA_TIMEOUT
        );
        // announce 节流：每轮循环都发会形成 tracker 洪水，统一按 5s 间隔补充
        let mut last_announce = Instant::now() - Duration::from_secs(METADATA_ANNOUNCE_INTERVAL);
        loop {
            if cancel.is_cancelled() {
                return Err("BT 任务已取消".into());
            }
            if self.has_metadata() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("获取元数据超时：无法从 peer 获取 .torrent 元数据".into());
            }
            // 补充 announce 拿新 peer（低水位加速）
            if last_announce.elapsed() >= Duration::from_secs(METADATA_ANNOUNCE_INTERVAL) {
                if let Some(r) = self.announce_all(None).await {
                    self.add_peers(r.peers, PeerSource::Tracker).await;
                }
                last_announce = Instant::now();
            }
            self.connect_pending().await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// 冷启动突发连接：首轮 3 倍并发连接。
    async fn cold_start_burst_connect(self: &Arc<Self>) {
        if self.cold_start_done.load(Ordering::Relaxed) {
            return;
        }
        self.cold_start_done.store(true, Ordering::Relaxed);

        let target = (self.config.max_peers * COLD_START_BURST).min(self.pending_count());
        tracing::info!(target, "冷启动突发连接");

        let mut spawned = 0;
        for _ in 0..target {
            let next = {
                let pending = self.pending.lock().unwrap();
                pending
                    .iter()
                    .find(|(a, _)| !is_unroutable(a) && a.port() != 0)
                    .map(|(a, s)| (*a, *s))
            };
            let Some((addr, source)) = next else { break };
            self.pending.lock().unwrap().remove(&addr);

            let cell = self.register_peer(addr, source);
            let e2 = self.clone();
            let e3 = self.clone();
            spawned += 1;
            tokio::spawn(async move {
                let outcome = e2.dial_and_run(addr, cell.clone()).await;
                e3.unregister_peer(&addr, cell.clone());
                // 失败地址回填 pending 重试（引擎主动淘汰不重试）
                if matches!(outcome, DialOutcome::Failed) && !cell.kill.is_cancelled() {
                    e3.retry_later(addr, cell.source);
                }
            });
        }
        tracing::info!(spawned, "冷启动突发连接已派发");
    }

    fn pending_count(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    /// 并发 announce 全部 tracker（HTTP + UDP），聚合所有成功响应的 peers。
    ///
    /// 对 `stopped`/`completed` 事件，只需首个成功响应（避免重复状态报告）；
    /// `started` 与周期性 announce（None）则聚合全部 tracker 的 peers ——
    /// 冷启动首轮必须尽量拿全 peer 池，只取第一个 tracker 会显著缩小候选集。
    ///
    /// 所有 tracker 请求并发发送，总体超时 15 秒，避免串行等待不可用的 tracker 阻塞主循环。
    async fn announce_all(&self, event: Option<&str>) -> Option<AnnounceResponse> {
        let done = self.done_bytes.load(Ordering::Relaxed);
        let left = self
            .total_bytes
            .load(Ordering::Relaxed)
            .saturating_sub(done);
        let info_hash = InfoHash::from_bytes(&self.info_hash);
        // 仅 stopped/completed 需要"首个成功即返回"；started/周期 announce 聚合全部
        let first_only = matches!(event, Some("stopped") | Some("completed"));
        let mut all_peers: Vec<SocketAddr> = Vec::new();
        let mut best_interval: u64 = 0;
        let mut any_success = false;

        // 并发 HTTP tracker announce：静态配置 + 运行时动态注入（合并去重）
        let http_urls: Vec<String> = {
            let dyn_list = self.dynamic_announces.lock().unwrap();
            self.config
                .announce_urls
                .iter()
                .filter(|url| !url.starts_with("wss://") && !url.starts_with("ws://"))
                .chain(dyn_list.http.iter())
                .cloned()
                .collect()
        };

        // 并发 announce 全部 tracker（HTTP + UDP 同一 JoinSet，总超时 15 秒）。
        // UDP 原先串行（每个最长 ~10s），多 tracker 时冷启动被逐个阻塞。
        let mut join_set: tokio::task::JoinSet<Result<AnnounceResponse, (String, String)>> =
            tokio::task::JoinSet::new();

        for url in &http_urls {
            let url = url.clone();
            let client = self.client.clone();
            let peer_id = self.peer_id;
            let port = self.actual_listen_port.load(Ordering::Relaxed);
            let numwant = self.config.numwant;
            let event_owned = event.map(|s| s.to_string());
            // 引擎统计了 uploaded_bytes 却上报 0 —— 私有 tracker 的
            // 分享率恒为 0 可能被判作弊封号
            let uploaded_now = self.uploaded_bytes.load(Ordering::Relaxed);
            join_set.spawn(async move {
                let req = AnnounceRequest {
                    info_hash: &info_hash,
                    peer_id: &peer_id,
                    port,
                    uploaded: uploaded_now,
                    downloaded: done,
                    left,
                    event: event_owned.as_deref(),
                    numwant,
                };
                match announce(&client, &url, &req).await {
                    Ok(r) => Ok(r),
                    Err(e) => Err((url, e)),
                }
            });
        }

        // UDP tracker（§7.7：5s 重发 / 10s 超时；与 HTTP 并行）：
        // 静态配置 + 运行时动态注入
        let udp_urls: Vec<String> = {
            let dyn_list = self.dynamic_announces.lock().unwrap();
            self.config
                .udp_announce_urls
                .iter()
                .chain(dyn_list.udp.iter())
                .cloned()
                .collect()
        };
        for url in &udp_urls {
            let url = url.clone();
            let peer_id = self.peer_id;
            let port = self.actual_listen_port.load(Ordering::Relaxed);
            let numwant = self.config.numwant;
            let uploaded_now = self.uploaded_bytes.load(Ordering::Relaxed);
            let udp_event = match event {
                Some("started") => xfer_discovery::udp_tracker::UdpEvent::Started,
                Some("completed") => xfer_discovery::udp_tracker::UdpEvent::Completed,
                Some("stopped") => xfer_discovery::udp_tracker::UdpEvent::Stopped,
                _ => xfer_discovery::udp_tracker::UdpEvent::None,
            };
            join_set.spawn(async move {
                let Some(addr) = resolve_udp_tracker_url(&url).await else {
                    return Err((url, "URL 无法解析（缺端口或域名解析失败）".into()));
                };
                let mut tracker = UdpTracker::new()
                    .await
                    .map_err(|e| (url.clone(), format!("初始化失败: {e}")))?;
                let req = xfer_discovery::udp_tracker::UdpAnnounceRequest {
                    info_hash,
                    peer_id,
                    port,
                    uploaded: uploaded_now,
                    downloaded: done,
                    left,
                    event: udp_event,
                    numwant,
                };
                match tracker.announce(addr, &req).await {
                    Ok(r) => Ok(AnnounceResponse {
                        interval: r.interval as u64,
                        min_interval: None,
                        peers: r.peers,
                        failure: None,
                        complete: Some(r.seeders as u64),
                        incomplete: Some(r.leechers as u64),
                    }),
                    Err(e) => Err((url, format!("announce 失败: {e}"))),
                }
            });
        }

        // 并发等待所有 tracker：stopped/completed 首个成功即返回；
        // started/周期 announce 聚合，但 started 用 3s 短窗口——冷启动不能被
        // 某个慢 tracker 拖住（慢 tracker 12s 响应会让冷启动整体卡 12s）。
        let overall_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let started_agg = matches!(event, Some("started"));
        let agg_deadline = if started_agg {
            Some(tokio::time::Instant::now() + Duration::from_secs(3))
        } else {
            None
        };
        loop {
            // 聚合模式下每轮重新取 min(整体截止, 聚合窗口)；首轮窗口到期即收尾
            let deadline = match (agg_deadline, overall_deadline) {
                (Some(agg), overall) => {
                    if tokio::time::Instant::now() >= agg && any_success {
                        break; // 首轮 3s 窗口已过且有成功响应 → 带已收集 peers 返回
                    }
                    agg.min(overall)
                }
                (None, overall) => overall,
            };
            match tokio::time::timeout_at(deadline, join_set.join_next()).await {
                Ok(Some(task_result)) => match task_result {
                    Ok(Ok(r)) => {
                        if r.failure.is_none() {
                            tracing::debug!(
                                peers = r.peers.len(),
                                interval = r.interval,
                                "tracker announce 成功"
                            );
                            self.last_interval
                                .store(r.interval.max(1), Ordering::Relaxed);
                            if r.interval > best_interval {
                                best_interval = r.interval;
                            }
                            any_success = true;
                            if first_only {
                                return Some(r);
                            }
                            all_peers.extend(r.peers);
                        } else {
                            tracing::warn!(reason = %r.failure.unwrap_or_default(), "tracker 拒绝");
                        }
                    }
                    Ok(Err((url, e))) => {
                        tracing::warn!(url, error = %e, "tracker announce 失败");
                    }
                    Err(join_err) => {
                        tracing::warn!(error = %join_err, "tracker announce 任务异常");
                    }
                },
                Ok(None) => break, // JoinSet 已空
                Err(_) => break,   // 超时，剩余请求由 JoinSet drop 自动取消
            }
        }

        if any_success {
            // 去重
            all_peers.sort();
            all_peers.dedup();
            Some(AnnounceResponse {
                interval: best_interval.max(1),
                min_interval: None,
                peers: all_peers,
                failure: None,
                complete: None,
                incomplete: None,
            })
        } else {
            None
        }
    }

    /// 通过 DHT get_peers 获取 peer 列表。
    async fn dht_get_peers(&self, dht: &Arc<Dht>) {
        let info_hash = InfoHash::from_bytes(&self.info_hash);
        match dht.get_peers(info_hash).await {
            Ok(result) => {
                if !result.peers.is_empty() {
                    tracing::info!(peers = result.peers.len(), "DHT get_peers 返回 peer");
                    self.add_peers(result.peers, PeerSource::Dht).await;

                    let dht_clone = dht.clone();
                    let port = self.actual_listen_port.load(Ordering::Relaxed);
                    tokio::spawn(async move {
                        let _ = dht_clone
                            .announce_peer(info_hash, port, &result.announce_nodes)
                            .await;
                    });
                } else {
                    tracing::debug!("DHT get_peers 未找到 peer");
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "DHT get_peers 失败");
            }
        }
    }

    /// 异步派发的 stopped/completed announce（run 结束后尽力通知）。
    fn announce_all_sync(&self, event: Option<&'static str>) {
        let client = self.client.clone();
        let peer_id = self.peer_id;
        let info_hash = InfoHash::from_bytes(&self.info_hash);
        let port = self.actual_listen_port.load(Ordering::Relaxed);
        let done = self.done_bytes.load(Ordering::Relaxed);
        let total = self.total_bytes.load(Ordering::Relaxed);
        // 分享率统计必须包含上传量
        let uploaded = self.uploaded_bytes.load(Ordering::Relaxed);
        let urls: Vec<String> = {
            let dyn_list = self.dynamic_announces.lock().unwrap();
            self.config
                .announce_urls
                .iter()
                .chain(dyn_list.http.iter())
                .cloned()
                .collect()
        };
        // 原先只拼 HTTP 列表，UDP tracker 收不到 stopped/completed
        // ——部分 tracker 记 incomplete 不释放。与 announce_all 相同的分流逻辑。
        let udp_urls: Vec<String> = {
            let dyn_list = self.dynamic_announces.lock().unwrap();
            self.config
                .udp_announce_urls
                .iter()
                .chain(dyn_list.udp.iter())
                .cloned()
                .collect()
        };
        let udp_event = match event {
            Some("started") => xfer_discovery::udp_tracker::UdpEvent::Started,
            Some("completed") => xfer_discovery::udp_tracker::UdpEvent::Completed,
            Some("stopped") => xfer_discovery::udp_tracker::UdpEvent::Stopped,
            _ => xfer_discovery::udp_tracker::UdpEvent::None,
        };
        tokio::spawn(async move {
            for url in urls {
                let req = AnnounceRequest {
                    info_hash: &info_hash,
                    peer_id: &peer_id,
                    port,
                    uploaded,
                    downloaded: done,
                    left: total.saturating_sub(done),
                    event,
                    numwant: 0,
                };
                let _ = announce(&client, &url, &req).await;
            }
            // UDP tracker（fire-and-forget，尽力通知）
            for url in udp_urls {
                let Some(addr) = resolve_udp_tracker_url(&url).await else {
                    continue;
                };
                let Ok(mut tracker) = UdpTracker::new().await else {
                    continue;
                };
                let req = xfer_discovery::udp_tracker::UdpAnnounceRequest {
                    info_hash,
                    peer_id,
                    port,
                    uploaded,
                    downloaded: done,
                    left: total.saturating_sub(done),
                    event: udp_event,
                    numwant: 0,
                };
                let _ = tracker.announce(addr, &req).await;
            }
        });
    }

    /// 去重加入待连接 peers。
    ///
    /// 过滤回环地址、本端监听端口和不可路由地址——tracker/DHT/PEX 会把
    /// 本端自己的地址回传给 announce（自连），自连 peer 拥有本端自己的
    /// 空 have 位图，永远无法贡献数据却占用连接槽，还会被调度器当成
    /// "慢节点"引发误判换血。
    async fn add_peers(&self, addrs: Vec<SocketAddr>, source: PeerSource) {
        let mut fresh: Vec<SocketAddr> = Vec::new();
        {
            let mut pending = self.pending.lock().unwrap();
            for a in addrs {
                // 过滤不可路由地址与 0 端口（部分 tracker 会返回，永远连不通）。
                // 回环地址保留：本机可能有 seed（本地测试/同机部署），
                // 自连回声由下面的监听端口检查兜底。
                if is_unroutable(&a) || a.port() == 0 {
                    continue;
                }
                if a.port() == self.actual_listen_port.load(Ordering::Relaxed) {
                    continue;
                }
                if pending.len() >= self.config.max_peers * 2 {
                    break;
                }
                if pending.contains_key(&a) || self.peers.read().unwrap().contains_key(&a) {
                    continue;
                }
                pending.insert(a, source);
                fresh.push(a);
            }
        }
        // 重新发现 = 新的重试预算（清零失败计数；注意 dial_failures 与
        // pending 不嵌套持锁，与 retry_later 的加锁顺序一致避免 AB-BA）
        if !fresh.is_empty() {
            let mut fails = self.dial_failures.lock().unwrap();
            for a in fresh {
                fails.remove(&a);
            }
        }
    }

    /// 建立新连接直至达到 max_peers（§7.8：30s 连接超时）。
    /// 建立新连接直到达到连接上限。
    async fn connect_pending(self: &Arc<Self>) {
        self.connect_pending_limited(usize::MAX).await
    }

    /// 建立新连接，本轮最多连 `limit` 条（智能调度按决策值限流）。
    async fn connect_pending_limited(self: &Arc<Self>, limit: usize) {
        // 连接上限：启用智能调度时取调度器目标（不超过预分配连接数），
        // 否则直接用预分配连接数（旧行为：连满为止）。
        let cap = if self.config.adaptive {
            self.scheduler
                .lock()
                .unwrap()
                .target()
                .min(self.config.max_peers)
        } else {
            self.config.max_peers
        };
        let mut new_peers: Vec<(SocketAddr, Arc<PeerCell>)> = Vec::new();
        {
            let connected = self.peers.read().unwrap().len();
            let need = cap.saturating_sub(connected).min(limit);
            if need == 0 {
                return;
            }
            let mut pending = self.pending.lock().unwrap();
            // 从 pending 中取 need 个（跳过不可路由/0 端口地址）
            let taken: Vec<(SocketAddr, PeerSource)> = pending
                .iter()
                .filter(|(a, _)| !is_unroutable(a) && a.port() != 0)
                .take(need)
                .map(|(a, s)| (*a, *s))
                .collect();
            for (a, _) in &taken {
                pending.remove(a);
            }
            for (a, source) in taken {
                let cell = self.register_peer(a, source);
                new_peers.push((a, cell));
            }
        }
        for (addr, cell) in new_peers {
            let e2 = self.clone();
            let e3 = self.clone();
            tokio::spawn(async move {
                let outcome = e2.dial_and_run(addr, cell.clone()).await;
                e3.unregister_peer(&addr, cell.clone());
                // 失败地址回填 pending 重试（引擎主动淘汰不重试）
                if matches!(outcome, DialOutcome::Failed) && !cell.kill.is_cancelled() {
                    e3.retry_later(addr, cell.source);
                }
            });
        }
    }

    /// 为对端创建共享状态。
    fn register_peer(&self, addr: SocketAddr, source: PeerSource) -> Arc<PeerCell> {
        let now = Instant::now();
        // 无元数据阶段（磁力）piece_count 为 0，元数据就绪后按需重建
        let piece_count = self
            .store
            .lock()
            .unwrap()
            .as_ref()
            .map(|s| s.piece_count())
            .unwrap_or(0);
        let cell = Arc::new(PeerCell {
            addr,
            state: Mutex::new(PeerState {
                peer_id: None,
                have: PieceMap::new(piece_count),
                pending_bitfield: None,
                pending_haves: Vec::new(),
                pending_have_all: false,
                choked: true,
                we_interested: false,
                last_activity: now,
                is_seed: false,
                encrypted: false,
                connected_at: now,
                recent_speed: 0,
                we_choked: true,
                peer_interested: false,
                fast_extension: false,
                extended_messaging: false,
                dht_enabled: false,
                ut_pex_id: 0,
                our_ut_pex_id: 0,
                ut_metadata_id: 0,
                our_ut_metadata_id: UT_METADATA_EXT_ID,
                allowed_fast_set: HashSet::new(),
                am_allowed_fast_set: HashSet::new(),
                last_data_transfer: now,
                choke_unchoke_count: 0,
                keepalive_count: 0,
                flooding_check_at: now,
                last_keepalive: now,
                last_unchoke: now,
                choking_required: true,
                opt_unchoking: false,
                client_version: None,
            }),
            downloaded: AtomicU64::new(0),
            uploaded: AtomicU64::new(0),
            prev_downloaded: Mutex::new(0),
            queued: Mutex::new(Vec::new()),
            pipeline: Mutex::new(self.config.pipeline.max(PIPELINE_MIN)),
            last_block_at: Mutex::new(now),
            source,
            transport: Mutex::new(TransportKind::Tcp),
            have_out_queue: Mutex::new(Vec::new()),
            kill: CancellationToken::new(),
        });
        self.peers.write().unwrap().insert(addr, cell.clone());
        cell
    }

    fn unregister_peer(&self, addr: &SocketAddr, cell: Arc<PeerCell>) {
        // 停机信号先行：淘汰/剪除必须真正终止会话——选片不查 peers 表，
        // 只除名不断开的"淘汰"会继续下载，连接只增不减，换血全部空转。
        cell.kill.cancel();
        // 释放占用的片
        self.release_all_pieces(&cell);
        // 只移除属于本次会话的登记项：同地址可能已注册新会话
        // （重连/入站并发），按地址盲删会把新登记误删。
        let was_present = {
            let mut peers = self.peers.write().unwrap();
            match peers.get(addr) {
                Some(cur) if Arc::ptr_eq(cur, &cell) => peers.remove(addr).is_some(),
                _ => false,
            }
        };
        if was_present {
            let st = cell.state.lock().unwrap();
            tracing::info!(
                peer = %addr,
                peer_id = ?st.peer_id,
                choked = st.choked,
                interested = st.we_interested,
                downloaded = cell.downloaded.load(Ordering::Relaxed),
                "peer 已断开注册"
            );
        }
        // 保留断开前的最后快照（getPeers 的 disconnected 分组，参考 C++ 版语义）
        let st = cell.state.lock().unwrap();
        let info = PeerInfo {
            addr: cell.addr.to_string(),
            peer_id: st
                .peer_id
                .map(|p| String::from_utf8_lossy(&p.0).to_string()),
            client: peer_client_desc(&st),
            choked: st.choked,
            interested: st.we_interested,
            seed: st.is_seed,
            downloaded: cell.downloaded.load(Ordering::Relaxed),
            encrypted: st.encrypted,
            connected: false,
            source: cell.source,
            uploaded: cell.uploaded.load(Ordering::Relaxed),
            protocol: cell.transport.lock().unwrap().as_str().to_string(),
            connected_secs: st.connected_at.elapsed().as_secs(),
            progress: peer_progress(&st),
        };
        drop(st);
        let mut dc = self.disconnected_peers.lock().unwrap();
        if let Some(pos) = dc.iter().position(|p| p.addr == info.addr) {
            dc[pos] = info;
        } else {
            dc.push(info);
            while dc.len() > MAX_DISCONNECTED_PEERS {
                dc.remove(0);
            }
        }
    }

    /// 清理长时间无活动 peer（§7.8：缩短为 60s）。
    fn prune_dead_peers(&self) {
        let dead: Vec<(SocketAddr, Arc<PeerCell>)> = self
            .peers
            .read()
            .unwrap()
            .iter()
            .filter(|(_, c)| c.state.lock().unwrap().last_activity.elapsed() > PEER_IDLE_TIMEOUT)
            .map(|(a, c)| (*a, c.clone()))
            .collect();
        for (addr, cell) in dead {
            self.unregister_peer(&addr, cell);
        }
    }

    /// 采样所有 peer 的下载速率（用于慢速节点淘汰）。
    fn sample_peer_speeds(&self) {
        let peers = self.peers.read().unwrap();
        for c in peers.values() {
            let downloaded = c.downloaded.load(Ordering::Relaxed);
            let mut prev = c.prev_downloaded.lock().unwrap();
            let delta = downloaded.saturating_sub(*prev);
            *prev = downloaded;
            // 5 秒采样窗口 → bytes/s
            c.state.lock().unwrap().recent_speed = delta / 5;
        }
    }

    /// BT 智能调度一轮：按吞吐边际收益决定扩张 / 换血 / 维持。
    ///
    /// 与 HTTP 的自适应调度同思路：加连接还能换来吞吐增长就扩张，
    /// 停滞则停止扩张并淘汰慢节点换血，始终不超过预分配连接数。
    async fn run_peer_schedule(self: &Arc<Self>) {
        let samples: Vec<PeerSample> = {
            let peers = self.peers.read().unwrap();
            peers
                .values()
                .map(|c| {
                    let st = c.state.lock().unwrap();
                    PeerSample {
                        addr: c.addr,
                        speed: st.recent_speed,
                        connected_for: st.connected_at.elapsed(),
                    }
                })
                .collect()
        };
        let pending = self.pending.lock().unwrap().len();
        let action = self.scheduler.lock().unwrap().evaluate(&samples, pending);
        match action {
            ScheduleAction::Expand(n) => {
                if n > 0 {
                    tracing::debug!(n, target = %self.scheduler.lock().unwrap().target(), "智能调度：扩张连接");
                    self.connect_pending_limited(n).await;
                }
            }
            ScheduleAction::Replace(victims) => {
                for addr in &victims {
                    let cell = self.peers.read().unwrap().get(addr).cloned();
                    if let Some(cell) = cell {
                        tracing::info!(peer = %addr, "智能调度：淘汰慢节点换血");
                        self.unregister_peer(addr, cell);
                    }
                }
                // 腾出的槽位立即用候选补上
                self.connect_pending_limited(victims.len()).await;
            }
            ScheduleAction::Hold => {}
        }
    }

    /// 慢速节点淘汰：当有空闲连接槽时，淘汰速率低于阈值的 peer。
    fn evict_slow_peers(&self) {
        let peers = self.peers.read().unwrap();
        let mut candidates: Vec<(SocketAddr, Arc<PeerCell>, u64, Instant)> = peers
            .values()
            .map(|c| {
                let st = c.state.lock().unwrap();
                (c.addr, c.clone(), st.recent_speed, st.connected_at)
            })
            .collect();
        drop(peers);

        // 按速率升序排序
        candidates.sort_by_key(|(_, _, speed, _)| *speed);

        // 只有当有空闲连接槽可被新 peer 填补时才淘汰
        let active = self.peers.read().unwrap().len();
        let pending = self.pending.lock().unwrap().len();
        if active == 0 || pending == 0 {
            return;
        }

        // 淘汰底部 10% 的慢速 peer（至少留一个）
        let evict_count = (active / 10).max(1).min(candidates.len().saturating_sub(1));
        let now = Instant::now();
        for (addr, cell, speed, connected_at) in candidates.iter().take(evict_count) {
            // 新连接给 15s 宽限期
            if now.duration_since(*connected_at) < Duration::from_secs(15) {
                continue;
            }
            if *speed < SLOW_PEER_THRESHOLD {
                tracing::info!(
                    peer = %addr,
                    speed_bytes = speed,
                    "淘汰慢速节点"
                );
                self.unregister_peer(addr, cell.clone());
            }
        }
    }

    pub fn is_done(&self) -> bool {
        self.all_wanted_done()
    }

    fn finish(&self) {
        self.finished.store(true, Ordering::Relaxed);
        {
            let mut guard = self.store.lock().unwrap();
            if let Some(store) = guard.as_mut() {
                store.flush_all().ok();
            }
        }
        // 下载完成后控制文件即失效：删除（aria2 .aria2 语义）。
        // 「完成后重启」由文件长度兜底检查覆盖。
        if let Some(ctrl) = self.resume_ctrl_path() {
            let _ = std::fs::remove_file(&ctrl);
        }
        self.announce_all_sync(Some("completed"));
        tracing::info!("BT 下载完成");
    }

    pub fn progress(&self) -> TorrentProgress {
        TorrentProgress {
            done: self.done_bytes.load(Ordering::Relaxed),
            total: self.total_bytes.load(Ordering::Relaxed),
        }
    }

    /// 累计上传字节数（实际发出的 piece 数据）。
    pub fn uploaded(&self) -> u64 {
        self.uploaded_bytes.load(Ordering::Relaxed)
    }

    /// 实际监听端口（0 = 监听器尚未就绪）。
    pub fn listen_port(&self) -> u16 {
        self.actual_listen_port.load(Ordering::Relaxed)
    }

    /// 运行时调整上传/下载限速（bytes/s，0 = 不限制）。
    /// 全局选项变更时由 Manager 下发，立即生效，无需重启任务。
    pub fn set_rate_limits(&self, download: u64, upload: u64) {
        self.dl_limit.store(download, Ordering::Relaxed);
        self.ul_limit.store(upload, Ordering::Relaxed);
        self.rate_limiter.lock().unwrap().set_rate(download);
        self.upload_limiter.lock().unwrap().set_rate(upload);
    }

    /// 当前加密模式（无锁读，拨号/握手热路径用）。
    pub fn encryption(&self) -> EncryptionMode {
        EncryptionMode::from_code(self.encryption_mode.load(Ordering::Relaxed))
    }

    /// 当前传输协议模式。
    pub fn bt_protocol(&self) -> BtProtocol {
        BtProtocol::from_code(self.bt_protocol_mode.load(Ordering::Relaxed))
    }

    /// 运行时热切换加密/协议模式（None 项保持不变）。
    ///
    /// 新值对后续拨号与新建连接立即生效；已建立的连接不受影响。
    pub fn set_bt_modes(
        &self,
        encryption: Option<EncryptionMode>,
        protocol: Option<BtProtocol>,
    ) -> Result<(), String> {
        if let Some(p) = protocol {
            self.bt_protocol_mode.store(p.code(), Ordering::Relaxed);
        }
        if let Some(e) = encryption {
            self.encryption_mode.store(e.code(), Ordering::Relaxed);
        }
        Ok(())
    }

    /// 运行时注入 announce URL（订阅源刷新 / 用户热添加）：
    /// 按 scheme 分流（`udp://` → UDP 列表，`ws://`/`wss://` 暂不支持，
    /// 其余 → HTTP 列表），与静态配置列表去重。下一轮 announce 立即
    /// 生效，无需暂停/恢复重建引擎。
    pub fn add_announce_urls(&self, urls: &[String]) {
        let mut dyn_list = self.dynamic_announces.lock().unwrap();
        for url in urls {
            let url = url.trim();
            if url.is_empty() {
                continue;
            }
            if url.starts_with("udp://") {
                if !dyn_list.udp.iter().any(|u| u == url)
                    && !self.config.udp_announce_urls.iter().any(|u| u == url)
                {
                    dyn_list.udp.push(url.to_string());
                }
            } else if url.starts_with("ws://") || url.starts_with("wss://") {
                // WebSocket tracker 暂不支持（与静态列表过滤一致）
            } else if !dyn_list.http.iter().any(|u| u == url)
                && !self.config.announce_urls.iter().any(|u| u == url)
            {
                dyn_list.http.push(url.to_string());
            }
        }
    }

    /// 对端列表（getPeers 用）：当前在线 + 本会话已断开的最后快照。
    pub fn peers_info(&self) -> Vec<PeerInfo> {
        let mut out: Vec<PeerInfo> = self
            .peers
            .read()
            .unwrap()
            .values()
            .map(|c| {
                let st = c.state.lock().unwrap();
                let progress = peer_progress(&st);
                PeerInfo {
                    addr: c.addr.to_string(),
                    peer_id: st
                        .peer_id
                        .map(|p| String::from_utf8_lossy(&p.0).to_string()),
                    client: peer_client_desc(&st),
                    choked: st.choked,
                    interested: st.we_interested,
                    seed: st.is_seed,
                    downloaded: c.downloaded.load(Ordering::Relaxed),
                    encrypted: st.encrypted,
                    connected: true,
                    source: c.source,
                    uploaded: c.uploaded.load(Ordering::Relaxed),
                    protocol: c.transport.lock().unwrap().as_str().to_string(),
                    connected_secs: st.connected_at.elapsed().as_secs(),
                    progress,
                }
            })
            .collect();
        let dc = self.disconnected_peers.lock().unwrap();
        for info in dc.iter() {
            if !out.iter().any(|p| p.addr == info.addr) {
                out.push(info.clone());
            }
        }
        out
    }

    pub fn info_hash(&self) -> InfoHash {
        InfoHash::from_bytes(&self.info_hash)
    }

    /// 当前元数据（磁力模式在 ut_metadata 获取完成前为 None）。
    pub fn meta(&self) -> Option<TorrentMeta> {
        self.meta.read().unwrap().clone()
    }

    /// 重新统计 done_bytes（启动/续传后/文件选择变更）。
    /// 有文件选择时只统计所需片的字节（总量同口径收缩）。
    fn refresh_done_bytes(&self) {
        let guard = self.store.lock().unwrap();
        let Some(store) = guard.as_ref() else {
            return;
        };
        let wanted = self.wanted.lock().unwrap().clone();
        let mut n = 0u64;
        for i in 0..store.piece_count() {
            let wanted_i = wanted.as_ref().is_none_or(|m| m.is_set(i));
            if wanted_i && store.have_piece(i) {
                n += store.piece_len(i);
            }
        }
        self.done_bytes.store(n, Ordering::Relaxed);
    }

    // ------------------------------------------------------------------
    // seed 模式
    // ------------------------------------------------------------------

    /// seed 模式：下载完成后继续做种。
    async fn run_seed_mode(self: &Arc<Self>, cancel: &CancellationToken) -> Result<(), String> {
        tracing::info!("进入 seed 模式");

        let seed_duration = if self.config.seed_duration > 0 {
            Some(Duration::from_secs(self.config.seed_duration))
        } else {
            None
        };
        // seed_duration 之前被完全忽略（`let _ = dur;`），任务只能
        // 靠外部 cancel 停止——与文档「seed 模式持续时间（秒，0 = 永久）」不符
        let started_at = Instant::now();
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // 在 seed 模式下定期 announce
        let mut last_announce = Instant::now();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    self.announce_all_sync(Some("stopped"));
                    return Err("seed 模式已取消".into());
                }
                _ = tick.tick() => {
                    // 定期 announce 保持在线（取消优先：暂停不被 tracker I/O 挡住）
                    let interval = self.last_interval.load(Ordering::Relaxed).max(30);
                    if last_announce.elapsed() >= Duration::from_secs(interval) {
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => {}
                            _ = self.announce_all(None) => {}
                        }
                        if cancel.is_cancelled() {
                            continue; // 下一轮进入取消分支
                        }
                        last_announce = Instant::now();
                    }
                    // 接受新连接（被动连接由 spawn_listener 处理）
                    // 主动连接待连接队列中的 peer
                    self.connect_pending_seed().await;
                    self.prune_dead_peers();
                    // 检查 seed 超时：到时正常退出（stopped announce 由
                    // 调用方 stop_background 前后的收尾路径处理）
                    if let Some(dur) = seed_duration {
                        if started_at.elapsed() >= dur {
                            tracing::info!(
                                secs = self.config.seed_duration,
                                "seed 时长已到，正常退出做种"
                            );
                            self.announce_all_sync(Some("stopped"));
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    /// seed 模式下主动连接 peer（作为 seeder 主动提供数据）。
    async fn connect_pending_seed(self: &Arc<Self>) {
        // 只在有空闲槽时连接
        let connected = self.peers.read().unwrap().len();
        if connected >= self.config.max_peers {
            return;
        }
        let need = self.config.max_peers - connected;
        let mut new_peers: Vec<(SocketAddr, Arc<PeerCell>)> = Vec::new();
        {
            let mut pending = self.pending.lock().unwrap();
            let taken: Vec<(SocketAddr, PeerSource)> =
                pending.iter().take(need).map(|(a, s)| (*a, *s)).collect();
            for (a, _) in &taken {
                pending.remove(a);
            }
            for (a, source) in taken {
                let cell = self.register_peer(a, source);
                new_peers.push((a, cell));
            }
        }
        for (addr, cell) in new_peers {
            let e2 = self.clone();
            let e3 = self.clone();
            tokio::spawn(async move {
                let outcome = e2.dial_and_run(addr, cell.clone()).await;
                e3.unregister_peer(&addr, cell.clone());
                // 失败地址回填 pending 重试（引擎主动淘汰不重试）
                if matches!(outcome, DialOutcome::Failed) && !cell.kill.is_cancelled() {
                    e3.retry_later(addr, cell.source);
                }
            });
        }
    }

    // ------------------------------------------------------------------
    // peer 会话
    // ------------------------------------------------------------------

    /// 按协议模式拨号并运行会话：uTP 优先（握手等待 [`UTP_DIAL_TIMEOUT`]），
    /// 失败/超时回退 TCP；`TcpOnly` 直连 TCP，`UtpOnly` 仅 uTP。
    /// 内联 await `run_peer`，调用方在返回后再 `unregister_peer`。
    ///
    /// 返回 [`DialOutcome`]：调用方据此决定是否把地址回填 pending 重试
    /// （uTP 会话夭折/拨号失败后原先要等下一轮 announce/PEX——
    /// interval 可能 30 分钟——才有重试机会）。
    async fn dial_and_run(self: &Arc<Self>, addr: SocketAddr, cell: Arc<PeerCell>) -> DialOutcome {
        let proto = self.bt_protocol();
        // 磁力冷启动：元数据未就绪时优先 TCP —— uTP 探测（最多 2s）对
        // metadata 交换无增益，反而让每个不支持 uTP 的对端白等一轮；
        // metadata 就绪后再走 uTP 优先（出站拨号路径无锁读原子量）。
        let prefer_tcp_cold_start = !self.has_metadata();
        // uTP 优先
        if proto.allows_utp() && !prefer_tcp_cold_start {
            let handle_opt = self.utp.lock().unwrap().clone();
            if let Some(handle) = handle_opt {
                match handle.connect_established(addr, UTP_DIAL_TIMEOUT).await {
                    Ok(stream) => {
                        if !self.shutdown.is_cancelled() {
                            if let Err(err) =
                                self.clone().run_peer(addr, stream, true, Some(cell)).await
                            {
                                tracing::debug!(peer = %addr, error = %err, "uTP 出站连接结束");
                                // 会话错误中断（非停机）→ 可重试；调用方会再
                                // 过滤 kill（引擎主动淘汰）的情况
                                return DialOutcome::Failed;
                            }
                            self.dial_failures.lock().unwrap().remove(&addr);
                            return DialOutcome::Done;
                        }
                        return DialOutcome::Done;
                    }
                    Err(err) => {
                        tracing::debug!(peer = %addr, error = %err, "uTP 拨号失败，回退 TCP");
                    }
                }
            }
        }
        // TCP 回退（或仅 TCP）
        if proto.allows_tcp() {
            match timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
                Ok(Ok(stream)) => {
                    if !self.shutdown.is_cancelled() {
                        if let Err(err) = self.clone().run_peer(addr, stream, true, Some(cell)).await {
                            tracing::debug!(peer = %addr, error = %err, "TCP 出站连接结束");
                            return DialOutcome::Failed;
                        }
                        self.dial_failures.lock().unwrap().remove(&addr);
                        return DialOutcome::Done;
                    }
                    return DialOutcome::Done;
                }
                Ok(Err(err)) => tracing::debug!(peer = %addr, error = %err, "TCP 拨号失败"),
                Err(_) => tracing::debug!(peer = %addr, "TCP 拨号超时({:?})", CONNECT_TIMEOUT),
            }
        }
        DialOutcome::Failed
    }

    /// 会话失败后的地址回填——计入失败次数并塞回 pending，
    /// 给地址有限次数的重试机会（超过即放弃，等 tracker/PEX 重新发现，
    /// 重新发现会清零计数）。引擎停机或主动淘汰（kill）不回填。
    fn retry_later(self: &Arc<Self>, addr: SocketAddr, source: PeerSource) {
        if self.shutdown.is_cancelled() {
            return;
        }
        let attempts = {
            let mut fails = self.dial_failures.lock().unwrap();
            let e = fails.entry(addr).or_insert(0);
            *e += 1;
            *e
        };
        if attempts > MAX_DIAL_RETRIES {
            tracing::debug!(peer = %addr, attempts, "连续失败放弃重试，等待重新发现");
            return;
        }
        let mut pending = self.pending.lock().unwrap();
        if pending.len() < self.config.max_peers * 2
            && !pending.contains_key(&addr)
            && !self.peers.read().unwrap().contains_key(&addr)
        {
            pending.insert(addr, source);
        }
    }

    /// 运行一个 peer 会话（主动或被动）。
    ///
    /// 收尾纪律：
    /// - 主动连接：调用方先 `register_peer` 并在返回后负责 `unregister_peer`；
    /// - 被动连接：本方法在此注册，会话结束（含握手早退）后**必须**注销，
    ///   否则死 peer 残留 peers 表 + 占用的片留在全局 assigned 集合，
    ///   其他 peer 拿不到这些片，最长停滞 PEER_IDLE_TIMEOUT。
    async fn run_peer<S>(
        self: Arc<Self>,
        addr: SocketAddr,
        stream: S,
        we_initiate: bool,
        existing_cell: Option<Arc<PeerCell>>,
    ) -> Result<(), String>
    where
        S: IntoPeerStream
            + tokio::io::AsyncReadExt
            + tokio::io::AsyncWriteExt
            + Unpin
            + Send
            + 'static,
    {
        match existing_cell {
            // 主动连接：cell 由调用方持有，收尾也由调用方执行
            Some(cell) => {
                self.run_peer_inner(addr, stream, we_initiate, cell)
                    .await
            }
            // 被动连接：自注册 + 会话结束后自注销（与出站路径对称的收尾）
            None => {
                let cell = self.register_peer(addr, PeerSource::Incoming);
                let result = self
                    .clone()
                    .run_peer_inner(addr, stream, we_initiate, cell.clone())
                    .await;
                self.unregister_peer(&addr, cell);
                result
            }
        }
    }

    /// M6：MSE 加密流完整集成 — 握手成功后后续所有 I/O 走 EncryptedStream。
    ///
    /// `cell`：会话登记项（主动连接由调用方注册，被动连接由 [`Self::run_peer`] 注册）。
    async fn run_peer_inner<S>(
        self: Arc<Self>,
        addr: SocketAddr,
        stream: S,
        we_initiate: bool,
        cell: Arc<PeerCell>,
    ) -> Result<(), String>
    where
        S: IntoPeerStream
            + tokio::io::AsyncReadExt
            + tokio::io::AsyncWriteExt
            + Unpin
            + Send
            + 'static,
    {
        *cell.transport.lock().unwrap() = S::TRANSPORT;
        let info_hash = InfoHash::from_bytes(&self.info_hash);
        let our_id = self.peer_id;

        // MSE/PE 加密握手（如果启用）：真实标准线格式，可与任意主流客户端互通。
        // PeerReader 提前创建：明文回退时需把已识别字节回填读缓冲。
        let mut reader = PeerReader::new();
        // 加密模式运行时可热切换：握手时刻读原子量
        let enc_mode = self.encryption();
        let (mut peer_stream, peer_hs_opt, encrypted) = if enc_mode == EncryptionMode::PlaintextOnly
        {
            (S::into_plain(stream), None, false)
        } else {
            let outcome = match timeout(
                MSE_TIMEOUT,
                self.mse_handshake(stream, &info_hash, we_initiate, enc_mode),
            )
            .await
            {
                Ok(res) => res,
                Err(_) => Err("MSE 握手超时".to_string()),
            };
            match outcome {
                Ok(crate::mse::PeOutcome::Encrypted {
                    stream: enc_stream,
                    peer_ia,
                }) => {
                    cell.state.lock().unwrap().encrypted = true;
                    tracing::debug!(peer = %addr, "MSE 加密连接已建立");
                    // 从 peer_ia 解析 BT 握手
                    let peer_hs = if peer_ia.len() >= 68 {
                        Some(
                            crate::message::decode_handshake(&peer_ia)
                                .map_err(|e| format!("MSE 捎带 BT 握手解析失败: {e}"))?,
                        )
                    } else {
                        None
                    };
                    (S::into_encrypted(enc_stream), peer_hs, true)
                }
                Ok(crate::mse::PeOutcome::Plaintext {
                    stream,
                    pending,
                    peer_ia,
                }) => {
                    if peer_ia.len() >= 68 {
                        // 加密握手后协商为明文流：BT 握手已经过 IA 互换
                        let peer_hs = Some(
                            crate::message::decode_handshake(&peer_ia)
                                .map_err(|e| format!("MSE 明文流 BT 握手解析失败: {e}"))?,
                        );
                        // 握手扫描越界读入的对端早期数据必须回注，否则丢包
                        if !pending.is_empty() {
                            reader.preload(pending);
                        }
                        (S::into_plain(stream), peer_hs, false)
                    } else {
                        // 被动方识别到明文 BT 握手：回填已读字节，走标准握手路径
                        reader.preload(pending);
                        (S::into_plain(stream), None, false)
                    }
                }
                Err(e) => {
                    if we_initiate && enc_mode == EncryptionMode::PreferEncryption {
                        // 对端可能不支持加密：重连一次走明文（libtorrent pe_enabled 语义）；
                        // 强制加密模式不重连，直接失败
                        tracing::debug!(peer = %addr, error = %e, "MSE 协商失败，尝试明文重连");
                        match timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
                            Ok(Ok(s)) => (PeerStream::Plain(s), None, false),
                            Ok(Err(e2)) => return Err(format!("明文重连失败: {e2}")),
                            Err(_) => return Err("明文重连超时".into()),
                        }
                    } else {
                        tracing::warn!(peer = %addr, error = %e, "MSE 握手失败");
                        return Err(e);
                    }
                }
            }
        };

        // BT 握手（如果 MSE 未捎带或未启用 MSE）
        // 握手后 reader 传递给 peer_message_loop，避免丢失对端握手后已发送的消息
        let peer_hs: Handshake = if let Some(hs) = peer_hs_opt {
            // MSE 捎带已获取对端 BT 握手
            hs
        } else {
            // 明文模式：执行标准 BT 握手
            if we_initiate {
                peer_stream
                    .write_all(&encode_handshake(&info_hash, &our_id))
                    .await
                    .map_err(|e| format!("握手发送失败: {e}"))?;
            }
            let hs: Handshake = match timeout(
                Duration::from_secs(8),
                reader.read_handshake(&mut peer_stream),
            )
            .await
            {
                Ok(Ok(Some(hs))) => hs,
                Ok(Ok(None)) => return Err("对端关闭连接（握手阶段）".into()),
                Ok(Err(e)) => return Err(format!("握手读取失败: {e}")),
                Err(_) => return Err("握手超时".into()),
            };
            // 回显己方握手前先校验：不向无效种子泄露握手
            if hs.info_hash != info_hash {
                tracing::warn!(peer = %addr, "对端 info_hash 不匹配，断开");
                return Err("对端 info_hash 不匹配".into());
            }
            if !we_initiate {
                peer_stream
                    .write_all(&encode_handshake(&info_hash, &our_id))
                    .await
                    .map_err(|e| format!("握手发送失败: {e}"))?;
            }
            hs
        };

        if peer_hs.info_hash != info_hash {
            tracing::warn!(peer = %addr, "对端 info_hash 不匹配，断开");
            return Err("对端 info_hash 不匹配".into());
        }

        tracing::info!(
            peer = %addr,
            fast_ext = supports_fast_extension(&peer_hs.reserved),
            ext_msg = supports_extension(&peer_hs.reserved),
            "BT 握手成功"
        );

        {
            let mut st = cell.state.lock().unwrap();
            st.peer_id = Some(peer_hs.peer_id);
            st.last_activity = Instant::now();
            st.encrypted = encrypted;
            // 检测对端能力（从 reserved bytes）
            st.fast_extension = supports_fast_extension(&peer_hs.reserved);
            st.extended_messaging = supports_extension(&peer_hs.reserved);
            st.dht_enabled = supports_dht(&peer_hs.reserved);
        }

        // 注意：tracing 字段中不能多次 lock 同一个 Mutex（参数临时值存活到语句末尾，重入死锁），
        // 先块内取出快照再记录。
        let (fast_ext, ext_msg, dht_enabled) = {
            let st = cell.state.lock().unwrap();
            (st.fast_extension, st.extended_messaging, st.dht_enabled)
        };
        tracing::debug!(
            peer = %addr,
            fast_ext,
            ext_msg,
            dht = dht_enabled,
            "对端能力协商完成"
        );

        // 磁力模式：元数据未就绪时先用本连接获取元数据。
        // 成功后 **继续** 常规下载流程复用该连接 —— 不能在此断开，否则真实网络下
        // 必须重新 announce 并等下一轮 tracker interval 才能重连（长时间无速度）。
        let mut ext_handshake_sent = false;
        if !self.has_metadata() {
            if let Err(e) = self
                .run_metadata_exchange(&mut peer_stream, &mut reader, &cell)
                .await
            {
                // 本连接请求过但未收到的分片必须释放，否则永久占用全局
                // requested 集合 → 其他 peer 永不重试 → metadata 收不齐。
                // 释放只会造成重复请求（幂等覆盖），不会造成数据丢失。
                self.release_unreceived_metadata_pieces();
                return Err(e);
            }
            if !self.has_metadata() {
                self.release_unreceived_metadata_pieces();
                return Err("元数据获取失败".into());
            }
            // 元数据交换阶段已发过扩展握手，下面不再重复发送
            ext_handshake_sent = true;
        }

        // 发送我方 bitfield（Fast Extension: 优先 HaveAll/HaveNone）
        let all_done = self
            .store
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .map()
            .all_done();
        let any_done = {
            let guard = self.store.lock().unwrap();
            let store = guard.as_ref().unwrap();
            store.piece_count() > 0 && (0..store.piece_count()).any(|p| store.have_piece(p))
        };

        let fast_ext = cell.state.lock().unwrap().fast_extension;
        if fast_ext {
            if all_done {
                peer_stream
                    .write_all(&Message::HaveAll.encode())
                    .await
                    .map_err(|e| format!("HaveAll 发送失败: {e}"))?;
            } else if !any_done {
                peer_stream
                    .write_all(&Message::HaveNone.encode())
                    .await
                    .map_err(|e| format!("HaveNone 发送失败: {e}"))?;
            } else {
                let bf = self.store.lock().unwrap().as_ref().unwrap().bitfield();
                peer_stream
                    .write_all(&Message::Bitfield(bf).encode())
                    .await
                    .map_err(|e| format!("bitfield 发送失败: {e}"))?;
            }
        } else {
            // 非 Fast Extension: 仅在有片时发送 bitfield
            if any_done {
                let bf = self.store.lock().unwrap().as_ref().unwrap().bitfield();
                peer_stream
                    .write_all(&Message::Bitfield(bf).encode())
                    .await
                    .map_err(|e| format!("bitfield 发送失败: {e}"))?;
            }
        }

        // BEP 10: 扩展协议握手（对端支持扩展协议且元数据阶段未发送过）
        if cell.state.lock().unwrap().extended_messaging && !ext_handshake_sent {
            let ext_handshake = self.build_extension_handshake();
            peer_stream
                .write_all(&ext_handshake)
                .await
                .map_err(|e| format!("扩展握手发送失败: {e}"))?;
        }

        // 磁力模式：对端的 Unchoke 可能已在元数据阶段被消费掉，主循环不会再收到，
        // 若不在此时主动声明感兴趣，连接会一直空转（表现为有节点但速度恒为 0）。
        if ext_handshake_sent {
            self.ensure_interested(&mut peer_stream, &cell).await?;
        }

        // BEP 5: DHT Port 消息（双方都支持 DHT 时通告端口）
        if cell.state.lock().unwrap().dht_enabled && self.config.dht_port > 0 {
            peer_stream
                .write_all(&Message::Port(self.config.dht_port).encode())
                .await
                .map_err(|e| format!("DHT port 消息发送失败: {e}"))?;
        }

        // BEP 6: Allowed Fast Set（对端支持 Fast Extension 时发送）
        if cell.state.lock().unwrap().fast_extension {
            let fast_set = self.compute_allowed_fast_set(&addr);
            if !fast_set.is_empty() {
                {
                    let mut st = cell.state.lock().unwrap();
                    for &idx in &fast_set {
                        st.am_allowed_fast_set.insert(idx);
                    }
                }
                for idx in fast_set {
                    peer_stream
                        .write_all(&Message::AllowedFast(idx).encode())
                        .await
                        .map_err(|e| format!("AllowedFast 发送失败: {e}"))?;
                }
            }
        }

        let mut ctx = PeerCtx::new(cell.clone());
        self.peer_message_loop(&mut peer_stream, &mut ctx, &mut reader)
            .await
    }

    /// MSE/PE 握手包装器：按主动/被动角色调用对应协商。
    ///
    /// 返回 `PeOutcome::Encrypted` 表示 RC4 加密流建立成功；
    /// `PeOutcome::Plaintext` 表示被动方识别到明文 BT 握手，
    /// 或加密握手后协商为明文流。
    async fn mse_handshake<S>(
        &self,
        stream: S,
        info_hash: &InfoHash,
        we_initiate: bool,
        mode: EncryptionMode,
    ) -> Result<crate::mse::PeOutcome<S>, String>
    where
        S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + Unpin,
    {
        // BT 握手作为 PE 的初始载荷（发起方 IA / 响应方回复）
        let bt_handshake = encode_handshake(info_hash, &self.peer_id);
        let result = if we_initiate {
            // 强制加密只提供 RC4；优先加密两者皆可（响应方仍优选 RC4）
            let provide = if mode == EncryptionMode::ForceEncryption {
                xfer_crypto::CRYPTO_RC4
            } else {
                xfer_crypto::CRYPTO_RC4 | xfer_crypto::CRYPTO_PLAINTEXT
            };
            crate::mse::pe_handshake_initiator(stream, info_hash, &bt_handshake, provide).await
        } else {
            crate::mse::pe_handshake_responder(
                stream,
                info_hash,
                &bt_handshake,
                mode == EncryptionMode::ForceEncryption,
            )
            .await
        };
        result.map_err(|e| format!("MSE 握手失败: {e}"))
    }

    /// peer 消息主循环。
    /// M7：集成 Fast Extension、Extension Protocol、PEX、Choking、Flooding 检测。
    ///
    /// `reader` 由调用方传入（复用握手阶段的 PeerReader，保留握手后对端可能已发送的 bitfield 等消息）。
    async fn peer_message_loop(
        self: &Arc<Self>,
        stream: &mut PeerStream,
        ctx: &mut PeerCtx,
        reader: &mut PeerReader,
    ) -> Result<(), String> {
        // BEP 3：keep-alive 间隔 120s
        let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
        keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Choking 算法每 10s 执行一轮
        let mut choke_timer = tokio::time::interval(CHOKE_INTERVAL);
        choke_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // PEX 每 60s 发送一次
        let mut pex_timer = tokio::time::interval(PEX_INTERVAL);
        pex_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let piece_count = self.store.lock().unwrap().as_ref().unwrap().piece_count();
        let mut pex_exchange = xfer_discovery::pex::PexExchange::new();

        // 进入循环前先灌一次请求流水线。
        // Unchoke / bitfield 可能在握手或磁力元数据阶段就已被消费掉，
        // 若只等循环内事件触发首次填充，连接会一直空转（有节点但速度恒为 0）。
        if !ctx.cell.state.lock().unwrap().choked {
            self.fill_pipeline(stream, ctx).await?;
        }

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    // 引擎暂停/完成/取消：立即结束本会话，
                    // 不再请求块、不再写盘、不再更新续传文件
                    return Err("引擎已停止".into());
                }
                _ = ctx.cell.kill.cancelled() => {
                    // 被引擎淘汰/剪除（慢节点换血、空闲超时等）：立即断开
                    return Err("连接已被引擎终止".into());
                }
                _ = keepalive.tick() => {
                    // BEP 3：keep-alive
                    stream.write_all(&Message::KeepAlive.encode())
                        .await.map_err(|e| e.to_string())?;
                    let mut st = ctx.cell.state.lock().unwrap();
                    st.last_keepalive = Instant::now();
                    // 注意：不计入 keepalive_count，避免洪泛误判
                }
                _ = choke_timer.tick() => {
                    // Choking 算法决策
                    self.decide_choking(stream, &ctx.cell).await?;
                    // 出队并发送 broadcast_have 积攒的 Have 消息
                    let haves: Vec<u32> =
                        std::mem::take(&mut *ctx.cell.have_out_queue.lock().unwrap());
                    for p in haves {
                        stream
                            .write_all(&Message::Have(p).encode())
                            .await
                            .map_err(|e| format!("Have 发送失败: {e}"))?;
                    }
                    // 每轮兜底补发请求（aria2 的 decideInterest/每轮补请求语义）。
                    // 事件触发式 fill 存在漏触发场景：unchoke 时所有片都被其他
                    // peer 占用 → assign_piece 返回 None → 之后片被释放也无人
                    // 唤醒本连接 → 永久空转（有节点无速度）。每 10s 兜底一次。
                    // fill_pipeline 幂等（已有分配/在途请求时不会重复发请求）。
                    //
                    // 停滞缩窗：有在途请求但长时间收不到块 → 递减窗口
                    // （旧实现的减半分支放在收块路径里，永远不会触发）
                    self.shrink_stalled_pipeline(ctx);
                    // 请求超时重发：有在途请求但长时间收不到任何块（对端静默
                    // 丢弃请求，无 RejectRequest 可依赖）时，清空在途表，让
                    // 紧随的 fill_pipeline 补发缺失块（已收块保留在 ctx.blocks）。
                    {
                        let timed_out = ctx.last_request_at.is_some()
                            && !ctx.in_flight.is_empty()
                            && ctx.cell.last_block_at.lock().unwrap().elapsed()
                                > REQUEST_TIMEOUT;
                        if timed_out {
                            tracing::debug!(
                                peer = %ctx.cell.addr,
                                count = ctx.in_flight.len(),
                                "在途请求超时未应答，重发"
                            );
                            ctx.in_flight.clear();
                            ctx.last_request_at = None;
                        }
                    }
                    if !ctx.cell.state.lock().unwrap().choked {
                        self.fill_pipeline(stream, ctx).await?;
                    }
                }
                _ = pex_timer.tick() => {
                    // PEX 消息发送（BEP 11）
                    self.send_pex_message(stream, &ctx.cell, &mut pex_exchange).await?;
                }
                msg = reader.read_message(stream) => {
                    let Some(msg) = msg.map_err(|e| format!("读取失败: {e}"))? else {
                        return Err("连接关闭".into());
                    };
                    {
                        let mut st = ctx.cell.state.lock().unwrap();
                        st.last_activity = Instant::now();
                    }
                    match msg {
                        Message::Bitfield(bf) => {
                            // peer 若注册于元数据就绪前，have 尺寸可能仍为 0，
                            // 先重建位图再合并（否则 set 全部空转 → 零速度）
                            self.ensure_have_capacity(&ctx.cell);
                            {
                                let mut st = ctx.cell.state.lock().unwrap();
                                st.have.set_from_bitfield(&bf);
                                st.is_seed = st.have.done_count() as usize >= piece_count as usize;
                            }
                            self.ensure_interested(stream, &ctx.cell).await?;
                            // bitfield 后若未被 choke，立即灌一次请求流水线
                            // （磁力路径对端 unchoke 可能已被元数据阶段消费）
                            if !ctx.cell.state.lock().unwrap().choked {
                                self.fill_pipeline(stream, ctx).await?;
                            }
                        }
                        Message::Have(p) => {
                            self.ensure_have_capacity(&ctx.cell);
                            {
                                let mut st = ctx.cell.state.lock().unwrap();
                                st.have.set(p);
                                st.is_seed = st.have.done_count() as usize >= piece_count as usize;
                            }
                            self.ensure_interested(stream, &ctx.cell).await?;
                            // 与 Bitfield 分支同因：乐观 unchoke 场景下 Unchoke 可能
                            // 先于片信息到达（此时 fill 因 have 为空而无片可分），
                            // Have 到达后必须主动补发请求，否则连接空转（有节点无速度）。
                            if !ctx.cell.state.lock().unwrap().choked {
                                self.fill_pipeline(stream, ctx).await?;
                            }
                        }
                        // ---- Fast Extension (BEP 6) ----
                        Message::HaveAll => {
                            self.ensure_have_capacity(&ctx.cell);
                            {
                                let mut st = ctx.cell.state.lock().unwrap();
                                if !st.fast_extension {
                                    tracing::warn!(peer = %ctx.cell.addr, "收到 HaveAll 但对端未声明 Fast Extension");
                                }
                                st.have.set_all();
                                st.is_seed = true;
                            }
                            self.ensure_interested(stream, &ctx.cell).await?;
                            // 如果双方都是 seeder，断开
                            if self.is_done() {
                                return Err("双方都是 seeder，断开连接".into());
                            }
                            // 与 Bitfield 分支同因：磁力路径 unchoke 可能已被
                            // 元数据阶段消费，此处立即灌一次请求流水线
                            if !ctx.cell.state.lock().unwrap().choked {
                                self.fill_pipeline(stream, ctx).await?;
                            }
                        }
                        Message::HaveNone => {
                            {
                                let mut st = ctx.cell.state.lock().unwrap();
                                if !st.fast_extension {
                                    tracing::warn!(peer = %ctx.cell.addr, "收到 HaveNone 但对端未声明 Fast Extension");
                                }
                                st.have.clear();
                                st.is_seed = false;
                            }
                        }
                        Message::RejectRequest { index, begin, length: _ } => {
                            // Fast Extension: 对端拒绝了我们的请求
                            ctx.in_flight.remove(&(index, begin));
                            ctx.stale_count = ctx.stale_count.saturating_add(1);
                            tracing::debug!(
                                peer = %ctx.cell.addr,
                                piece = index,
                                begin,
                                "请求被拒绝 (RejectRequest)"
                            );
                        }
                        Message::AllowedFast(index) => {
                            // Fast Extension: 对端允许我们快速下载该片
                            let mut st = ctx.cell.state.lock().unwrap();
                            st.allowed_fast_set.insert(index);
                            tracing::debug!(
                                peer = %ctx.cell.addr,
                                piece = index,
                                "收到 AllowedFast"
                            );
                        }
                        Message::SuggestPiece(index) => {
                            // Fast Extension: 对端建议我们下载该片
                            tracing::debug!(
                                peer = %ctx.cell.addr,
                                piece = index,
                                "收到 SuggestPiece"
                            );
                            // 如果该片未分配且我们缺失，加入该 peer 的片队列
                            if !self.is_done() {
                                let (missing, plen) = {
                                    let guard = self.store.lock().unwrap();
                                    let store = guard.as_ref().unwrap();
                                    (!store.have_piece(index), store.piece_len(index) as u32)
                                };
                                if missing {
                                    let room = {
                                        let q = ctx.cell.queued.lock().unwrap();
                                        q.len() < MAX_QUEUED_PIECES && !q.contains(&index)
                                    };
                                    if room {
                                        let mut assigned = self.assigned.lock().unwrap();
                                        if !assigned.contains(&index) {
                                            assigned.insert(index);
                                            drop(assigned);
                                            ctx.block_need
                                                .entry(index)
                                                .or_insert(plen.div_ceil(BLOCK_SIZE));
                                            ctx.cell.queued.lock().unwrap().push(index);
                                        }
                                    }
                                }
                            }
                        }
                        // ---- DHT (BEP 5) ----
                        Message::Port(port) => {
                            tracing::debug!(
                                peer = %ctx.cell.addr,
                                port,
                                "收到 DHT Port 消息"
                            );
                            // 记录对端 DHT 端口用于后续 DHT 操作
                            // （DHT 逻辑由 xfer-dht crate 独立处理）
                        }
                        // ---- Extension Protocol (BEP 10) ----
                        Message::Extended { ext_id, payload } => {
                            if ext_id == UT_METADATA_EXT_ID {
                                // 服务对端 ut_metadata 请求（我方在扩展握手中
                                // 广告了 ut_metadata=2 却从不响应 = 蜂群"坏公民"）
                                self.serve_ut_metadata(stream, &payload, &ctx.cell)
                                    .await?;
                            } else {
                                self.handle_extension_message(ext_id, &payload, &ctx.cell);
                            }
                        }
                        Message::Choke => {
                            {
                                let mut st = ctx.cell.state.lock().unwrap();
                                let was_choked = st.choked;
                                st.choked = true;
                                // 洪泛检测：仅当状态从 unchoke → choke 变化时计数
                                if !was_choked {
                                    st.choke_unchoke_count =
                                        st.choke_unchoke_count.saturating_add(1);
                                }
                            }
                            self.release_all_pieces(&ctx.cell);
                            ctx.in_flight.clear();
                            ctx.blocks.clear();
                            ctx.block_need.clear();
                            ctx.block_have.clear();
                        }
                        Message::Unchoke => {
                            {
                                let mut st = ctx.cell.state.lock().unwrap();
                                let was_choked = st.choked;
                                st.choked = false;
                                // 洪泛检测
                                if was_choked {
                                    st.choke_unchoke_count = st.choke_unchoke_count.saturating_add(1);
                                }
                                st.last_data_transfer = Instant::now();
                            }
                            self.ensure_interested(stream, &ctx.cell).await?;
                            self.fill_pipeline(stream, ctx).await?;
                        }
                        Message::Interested => {
                            ctx.cell.state.lock().unwrap().peer_interested = true;
                            // seed 模式或 choking 算法管理 unchoke
                            if self.finished.load(Ordering::Relaxed) {
                                self.maybe_unchoke_peer(stream, &ctx.cell).await?;
                            }
                        }
                        Message::NotInterested => {
                            ctx.cell.state.lock().unwrap().peer_interested = false;
                        }
                        Message::Request { index, begin, length } => {
                            // seed 模式或 choking：响应上传请求
                            let (we_choked, fast_ext, am_allowed) = {
                                let st = ctx.cell.state.lock().unwrap();
                                (st.we_choked, st.fast_extension, st.am_allowed_fast_set.contains(&index))
                            };
                            if !we_choked || am_allowed {
                                // 我们有该片 → 发送数据
                                self.serve_block(stream, &ctx.cell, index, begin, length).await?;
                            } else if fast_ext {
                                // Fast Extension: 发送 RejectRequest
                                stream.write_all(
                                    &Message::RejectRequest { index, begin, length }.encode()
                                ).await.map_err(|e| e.to_string())?;
                            }
                            // 非 Fast Extension 且被 choke：忽略请求
                        }
                        Message::Piece { index, begin, block } => {
                            if !ctx.cell.queued.lock().unwrap().contains(&index) {
                                continue; // 非本连接下载中的片，丢弃
                            }
                            // 限速检查（读动态限额，运行时可通过 set_rate_limits 调整）。
                            // 循环直到消费成功：旧实现单次等待封顶 500ms 且不扣令牌，
                            // 低于「块大小/500ms」的限速值形同虚设。
                            if self.dl_limit.load(Ordering::Relaxed) > 0 {
                                let block_len = block.len() as u64;
                                loop {
                                    let wait =
                                        self.rate_limiter.lock().unwrap().try_consume(block_len);
                                    match wait {
                                        None => break,
                                        Some(dur) => {
                                            if self.shutdown.is_cancelled() {
                                                break;
                                            }
                                            tokio::time::sleep(
                                                dur.min(Duration::from_millis(500)),
                                            )
                                            .await;
                                        }
                                    }
                                }
                            }
                            let blen = block.len() as u64;
                            // 完成计数：仅首次收到的块计数（请求超时重发的重复块
                            // 覆盖缓存但不计数，否则计数越过应收数、片永不"完成"）
                            if ctx.blocks.insert((index, begin), block).is_none() {
                                *ctx.block_have.entry(index).or_insert(0) += 1;
                            }
                            ctx.in_flight.remove(&(index, begin));
                            // 累计该 peer 下载量：速度采样（sample_peer_speeds）、
                            // 慢节点淘汰/智能调度都依赖它。缺了它所有 peer 的
                            // recent_speed 恒为 0 → 调度把正常下载的 seed 当死
                            // 节点批量断开换血 → 0 字节 + 连接数被砍到 1。
                            ctx.cell.downloaded.fetch_add(blen, Ordering::Relaxed);
                            *ctx.cell.last_block_at.lock().unwrap() = Instant::now();
                            ctx.last_request_at = None;
                            ctx.stale_count = 0;
                            {
                                let mut st = ctx.cell.state.lock().unwrap();
                                st.last_data_transfer = Instant::now();
                            }

                            // 自适应流水线深度调整
                            self.adjust_pipeline(ctx);

                            if self.piece_complete(ctx, index) {
                                let plen = self.store.lock().unwrap().as_ref().unwrap().piece_len(index);
                                let mut data = vec![0u8; plen as usize];
                                let mut ok = true;
                                for (&(pi, off), blk) in &ctx.blocks {
                                    if pi != index {
                                        continue;
                                    }
                                    let off = off as usize;
                                    if off + blk.len() > data.len() {
                                        ok = false;
                                        break;
                                    }
                                    data[off..off + blk.len()].copy_from_slice(blk);
                                }
                                let expected = self
                                    .meta
                                    .read()
                                    .unwrap()
                                    .as_ref()
                                    .expect("元数据未就绪")
                                    .info
                                    .pieces[index as usize];
                                if ok && self.accept_piece(index, &data, &expected) {
                                    // 广播 Have 给所有 peer（BEP 3）
                                    self.broadcast_have(index);
                                }
                                // 仅释放该片的槽位与缓存；其余在途片继续下载
                                self.release_piece(&ctx.cell, index);
                                ctx.blocks.retain(|&(pi, _), _| pi != index);
                                ctx.in_flight.retain(|&(pi, _)| pi != index);
                                ctx.block_need.remove(&index);
                                ctx.block_have.remove(&index);
                            }
                            self.fill_pipeline(stream, ctx).await?;
                        }
                        Message::Cancel { index, begin, length } => {
                            // seed 模式：对端取消请求，忽略（已在发送中或已发送）
                            tracing::debug!(
                                peer = %ctx.cell.addr,
                                piece = index, begin, length,
                                "收到 Cancel"
                            );
                        }
                        Message::Unknown { id, .. } => {
                            tracing::debug!(id, peer = %ctx.cell.addr, "未知消息");
                        }
                        Message::KeepAlive => {
                            // 仅统计对端发来的 keep-alive（洪泛检测）
                            let mut st = ctx.cell.state.lock().unwrap();
                            st.keepalive_count = st.keepalive_count.saturating_add(1);
                        }
                    }

                    // 消息洪泛检测
                    self.detect_flooding(&ctx.cell)?;

                    // 非活跃连接检测
                    self.check_active_interaction(&ctx.cell)?;
                }
            }
        }
    }

    /// 构建 BEP 10 扩展协议握手消息（ext_id=0）。
    ///
    /// 消息体为 bencode 字典，包含:
    /// - "m": { "ut_pex": <id> } — 本端支持的扩展
    /// - "v": 客户端版本字符串
    /// - "p": 本端监听端口
    fn build_extension_handshake(&self) -> Vec<u8> {
        use std::collections::BTreeMap;
        use xfer_bencode::{encode, Value};

        let mut m = BTreeMap::new();
        // 分配 ut_pex ext_id = 1
        m.insert(b"ut_pex".to_vec(), Value::Int(1));
        // 磁力链接支持：声明 ut_metadata（BEP 9）
        m.insert(
            b"ut_metadata".to_vec(),
            Value::Int(UT_METADATA_EXT_ID as i64),
        );
        let mut dict = BTreeMap::new();
        dict.insert(b"m".to_vec(), Value::Dict(m));
        dict.insert(
            b"v".to_vec(),
            Value::Bytes(format!("{ENGINE_NAME}/{ENGINE_VERSION}").into_bytes()),
        );
        let port = self.actual_listen_port.load(Ordering::Relaxed);
        if port > 0 {
            dict.insert(b"p".to_vec(), Value::Int(port as i64));
        }
        let payload = encode(&Value::Dict(dict));
        Message::Extended {
            ext_id: 0, // ext_id=0 是握手消息
            payload,
        }
        .encode()
    }

    /// 处理收到的 BEP 10 扩展消息。
    fn handle_extension_message(&self, ext_id: u8, payload: &[u8], cell: &Arc<PeerCell>) {
        use xfer_bencode::Value;
        if ext_id == 0 {
            // 扩展握手响应：解析对端支持的扩展
            if let Ok(v) = xfer_bencode::decode(payload) {
                if let Some(d) = v.as_dict() {
                    // 解析 "v"：对端自报客户端版本（比 peer_id 前缀更精确）
                    if let Some(Value::Bytes(ver)) = d.get(b"v".as_slice()) {
                        let s = String::from_utf8_lossy(ver).trim().to_string();
                        if !s.is_empty() {
                            cell.state.lock().unwrap().client_version = Some(s);
                        }
                    }
                    // 解析 "m" 字典获取对端 ut_pex / ut_metadata ext_id
                    if let Some(Value::Dict(m)) = d.get(b"m".as_slice()) {
                        if let Some(Value::Int(id)) = m.get(b"ut_pex".as_slice()) {
                            let mut st = cell.state.lock().unwrap();
                            st.ut_pex_id = *id as u8;
                            st.our_ut_pex_id = 1; // 我们分配的 ut_pex id
                        }
                        if let Some(Value::Int(id)) = m.get(b"ut_metadata".as_slice()) {
                            let mut st = cell.state.lock().unwrap();
                            st.ut_metadata_id = *id as u8;
                            st.our_ut_metadata_id = UT_METADATA_EXT_ID;
                        }
                        let st = cell.state.lock().unwrap();
                        tracing::debug!(
                            peer = %cell.addr,
                            ut_pex_id = st.ut_pex_id,
                            ut_metadata_id = st.ut_metadata_id,
                            "对端扩展握手完成"
                        );
                    }
                }
            }
        } else if ext_id == 1 {
            // ut_pex 消息（PEX，BEP 11）
            // 在此简化处理：解析并添加新 peer（过滤回环地址）
            if let Ok(msg) = xfer_discovery::pex::PexMessage::decode(payload) {
                let new_peers: Vec<SocketAddr> = msg
                    .added
                    .iter()
                    .map(|p| p.addr)
                    .filter(|a| !is_loopback(a))
                    .collect();
                if !new_peers.is_empty() {
                    tracing::debug!(
                        peer = %cell.addr,
                        new_peers = new_peers.len(),
                        "PEX 收到新 peer"
                    );
                    // 直接添加 peer（同步插入，不 spawn）
                    let max = self.config.max_peers * 2;
                    let mut p = self.pending.lock().unwrap();
                    for addr in new_peers {
                        if p.len() >= max {
                            break;
                        }
                        p.insert(addr, PeerSource::Pex);
                    }
                }
            }
        }
    }

    /// 磁力模式：与单个 peer 交换 ut_metadata（BEP 9），成功即安装元数据。
    ///
    /// 本函数在 `run_peer` 能力协商后、元数据未就绪时被调用；
    /// 只做元数据请求/接收，不参与常规 piece 下载。
    /// 磁力模式：与单个 peer 交换 ut_metadata（BEP 9），成功即安装元数据。
    ///
    /// 返回 `Ok` 表示元数据已就绪（可能由**其他** peer 提供）。调用方应 **继续**
    /// 走常规下载流程复用本连接 —— 若在此处断开，真实网络下必须重新 announce
    /// 并等待下一轮 tracker interval 才能重连，表现为"有节点但长时间速度 0"。
    /// 注意：`reader` 必须是 run_peer 创建的**同一个**实例 —— 元数据阶段读到的
    /// 字节若留在另一个 reader 的缓冲里被丢弃，转入下载后主循环会读到错乱数据
    /// （表现为连接报错关闭、反复重连）。
    async fn run_metadata_exchange(
        self: &Arc<Self>,
        peer_stream: &mut PeerStream,
        reader: &mut PeerReader,
        cell: &Arc<PeerCell>,
    ) -> Result<(), String> {
        // 元数据已由其他 peer 就绪 → 本连接直接转入常规下载流程
        if self.has_metadata() {
            return Ok(());
        }

        // 发送我方扩展握手（对端支持扩展协议时）
        if cell.state.lock().unwrap().extended_messaging {
            let ext_handshake = self.build_extension_handshake();
            peer_stream
                .write_all(&ext_handshake)
                .await
                .map_err(|e| format!("扩展握手发送失败: {e}"))?;
        }

        // 等待并解析对端扩展握手，取得对端 ut_metadata ext_id
        // （对端可能在 BT 握手后已先行发出，直接从读循环中取）
        let mut peer_meta_id = cell.state.lock().unwrap().ut_metadata_id;
        if peer_meta_id == 0 {
            // 8s：扩展握手只需 1~2 个 RTT，纯老客户端（不支持 BEP 10）不回包，
            // 缩短判定时间让冷启动更快放弃不支持 ut_metadata 的对端。
            let hs_deadline = Instant::now() + Duration::from_secs(8);
            while peer_meta_id == 0 {
                if self.shutdown.is_cancelled() {
                    return Err("引擎已停止".into());
                }
                if self.has_metadata() {
                    return Ok(());
                }
                if Instant::now() >= hs_deadline {
                    return Err("对端未协商 ut_metadata".into());
                }
                let msg = match timeout(Duration::from_secs(8), reader.read_message(peer_stream))
                    .await
                {
                    Ok(Ok(Some(m))) => m,
                    Ok(Ok(None)) => return Err("对端关闭连接（元数据交换阶段）".into()),
                    Ok(Err(e)) => return Err(format!("读取消息失败: {e}")),
                    Err(_) => return Err("扩展握手读超时".into()),
                };
                match msg {
                    Message::Extended { ext_id: 0, payload } => {
                        self.handle_extension_message(0, &payload, cell);
                    }
                    other => self.note_metadata_phase_message(&other, cell),
                }
                peer_meta_id = cell.state.lock().unwrap().ut_metadata_id;
            }
        }
        if peer_meta_id == 0 {
            return Err("对端不支持 ut_metadata".into());
        }

        let deadline = Instant::now() + METADATA_TIMEOUT;

        // 流水线请求：同一 peer 在途请求多个分片（BEP 9 允许并发请求）。
        // 大 metadata（几百 KB = 几十片）逐片串行要几十个 RTT，流水线后
        // 并行度上限为 METADATA_PIPELINE，冷启动等待大幅缩短。
        let mut in_flight: usize = 0;
        // 首片请求：未知 total_size 时从 piece 0 开始，收到 data 后计算分片数。
        // 0 未收到时总是请求——即使已在途也冗余请求：请求 0 的 peer 断开后，
        // requested 里的 0 不会自动释放，若非冗余则 0 永远没人再请求 → 卡死。
        // REJECT 分支会从 requested 移除 0，让后续 peer 可重试。
        {
            let mut md = self.metadata.lock().unwrap();
            if !md.pieces.contains_key(&0) {
                md.requested.insert(0);
                in_flight += 1;
            }
        }
        if in_flight > 0 {
            peer_stream
                .write_all(&ut_metadata_request(peer_meta_id, 0))
                .await
                .map_err(|e| format!("metadata 请求发送失败: {e}"))?;
        }
        // 已知道分片数则立即填满流水线
        while in_flight < METADATA_PIPELINE {
            if let Some(next) = self.next_metadata_piece() {
                peer_stream
                    .write_all(&ut_metadata_request(peer_meta_id, next))
                    .await
                    .map_err(|e| format!("metadata 请求发送失败: {e}"))?;
                in_flight += 1;
            } else {
                break;
            }
        }

        loop {
            if self.shutdown.is_cancelled() {
                return Err("引擎已停止".into());
            }
            if self.has_metadata() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("获取元数据超时".into());
            }
            let msg = match timeout(Duration::from_secs(15), reader.read_message(peer_stream)).await
            {
                Ok(Ok(Some(m))) => m,
                Ok(Ok(None)) => return Err("对端关闭连接（元数据交换阶段）".into()),
                Ok(Err(e)) => return Err(format!("读取消息失败: {e}")),
                Err(_) => return Err("元数据交换读超时".into()),
            };
            match msg {
                // BEP 10：对端用**我方**在扩展握手中广告的 ext_id 发送 ut_metadata
                // 数据（而非对端自己广告的 id——此前误用 peer_meta_id 匹配，
                // 仅当双方恰好都分配 2 时才能收到数据，属「双方都错」侥幸通过）。
                Message::Extended { ext_id, payload } if ext_id == UT_METADATA_EXT_ID => {
                    // 对端也可能向我们发 REQUEST（元数据交换阶段我方必然
                    // 还没有元数据）—— 回 REJECT 并继续等待，不再报错断开
                    // 会话（旧实现"非 data 消息"→ Err → 整条连接报废）。
                    if payload_is_metadata_request(&payload) {
                        let _ = self
                            .serve_ut_metadata(peer_stream, &payload, cell)
                            .await;
                        continue;
                    }
                    match self.handle_metadata_payload(&payload) {
                        Ok(()) => {
                            if self.has_metadata() {
                                return Ok(());
                            }
                            // 收到一片，流水线补发一片（保持 METADATA_PIPELINE 在途）
                            in_flight = in_flight.saturating_sub(1);
                            if in_flight < METADATA_PIPELINE {
                                if let Some(next) = self.next_metadata_piece() {
                                    peer_stream
                                        .write_all(&ut_metadata_request(peer_meta_id, next))
                                        .await
                                        .map_err(|e| format!("metadata 请求发送失败: {e}"))?;
                                    in_flight += 1;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::debug!(
                                peer = %cell.addr,
                                error = %e,
                                "metadata 数据解析失败，放弃该 peer"
                            );
                            return Err(e);
                        }
                    }
                }
                // 其余消息不能丢弃：对端 bitfield/unchoke 决定后续能否选片下载
                other => self.note_metadata_phase_message(&other, cell),
            }
        }
    }

    /// 元数据获取阶段处理对端的非 Extended 消息。
    ///
    /// bitfield / have 需等元数据就绪（片数已知）才能落到 `have`，此刻先暂存；
    /// choke 状态与片数无关，即时生效。
    fn note_metadata_phase_message(&self, msg: &Message, cell: &Arc<PeerCell>) {
        // 元数据可能已被其他 peer 装好（竞态）：此时 have 若仍是 0 尺寸
        // 必须立即重建，否则消息被塞进 pending 后再无人应用
        // （install_metadata 只跑一次）→ 该 peer 永远"无片可分"→ 零速度。
        self.ensure_have_capacity(cell);
        let mut st = cell.state.lock().unwrap();
        match msg {
            Message::Bitfield(bf) => {
                if st.have.count() > 0 {
                    st.have.set_from_bitfield(bf);
                } else {
                    st.pending_bitfield = Some(bf.clone());
                }
            }
            Message::HaveAll => {
                st.is_seed = true;
                if st.have.count() > 0 {
                    st.have.set_all();
                } else {
                    st.pending_have_all = true;
                }
            }
            Message::HaveNone => {}
            Message::Have(idx) => {
                if *idx < st.have.count() {
                    st.have.set(*idx);
                } else {
                    st.pending_haves.push(*idx);
                }
            }
            Message::Unchoke => st.choked = false,
            Message::Choke => st.choked = true,
            _ => {}
        }
    }

    /// have 位图尺寸与真实片数不一致（peer 注册于元数据就绪前）时重建，
    /// 并顺带应用暂存的 bitfield/have/have_all。
    ///
    /// 典型竞态：peer A 装好元数据触发过一次 apply_pending（本 peer 当时
    /// 还没收到 bitfield），本 peer 之后才收到对端 bitfield——若不在此
    /// 重建尺寸，`set`/`is_set` 在 0 尺寸位图上全部空转，该 peer 有连接
    /// 但永远选不出片（表现为速度恒为 0）。
    fn ensure_have_capacity(&self, cell: &Arc<PeerCell>) {
        let piece_count = self
            .store
            .lock()
            .unwrap()
            .as_ref()
            .map(|s| s.piece_count())
            .unwrap_or(0);
        if piece_count == 0 {
            return; // 元数据未就绪，继续走 pending 暂存
        }
        let mut st = cell.state.lock().unwrap();
        if st.have.count() == piece_count {
            return;
        }
        st.have = PieceMap::new(piece_count);
        if st.pending_have_all {
            st.have.set_all();
            st.is_seed = true;
        } else if let Some(bf) = st.pending_bitfield.take() {
            st.have.set_from_bitfield(&bf);
        }
        for idx in std::mem::take(&mut st.pending_haves) {
            if idx < piece_count {
                st.have.set(idx);
            }
        }
        st.pending_have_all = false;
    }

    /// 元数据就绪后，把各 peer 在元数据阶段暂存的片信息应用到 `have`。
    fn apply_pending_peer_bitfields(&self) {
        let piece_count = self
            .store
            .lock()
            .unwrap()
            .as_ref()
            .map(|s| s.piece_count())
            .unwrap_or(0);
        if piece_count == 0 {
            return;
        }
        for cell in self.peers.read().unwrap().values() {
            let mut st = cell.state.lock().unwrap();
            if st.have.count() != piece_count {
                st.have = PieceMap::new(piece_count);
            }
            if st.pending_have_all {
                st.have.set_all();
            } else if let Some(bf) = st.pending_bitfield.take() {
                st.have.set_from_bitfield(&bf);
            }
            // 先取出再落盘，避免同一 &mut 上的重入借用
            let pending_haves = std::mem::take(&mut st.pending_haves);
            for idx in pending_haves {
                if idx < piece_count {
                    st.have.set(idx);
                }
            }
            st.pending_have_all = false;
        }
    }

    /// 处理收到的 ut_metadata data 消息（BEP 9 msg_type=1）。
    ///
    /// 分片收集完整后拼接 → 解析 info 字典 → [`Self::install_metadata`]。
    ///
    /// 
    /// - `total_size` 上限 [`MAX_METADATA_SIZE`]，且所有分片必须一致，
    ///   不一致即清空累积器重新收集（防止假 total_size 撑爆内存/毒化拼接）；
    /// - piece 索引必须 `< piece_count`（防任意大索引塞 BTreeMap）；
    /// - `parse_info_bytes` / info_hash 校验失败时**清空整个累积器** ——
    ///   否则分片残留，此后每个新会话补齐任意一片就再次 assemble →
    ///   再次失败 → 磁力任务在超时前永久无法拿到真元数据。
    fn handle_metadata_payload(&self, payload: &[u8]) -> Result<(), String> {
        use xfer_bencode::{decode_prefix, Value};
        let (v, consumed) =
            decode_prefix(payload).map_err(|e| format!("metadata 头解析失败: {e}"))?;
        let dict = v
            .as_dict()
            .ok_or_else(|| "metadata 头必须是字典".to_string())?;
        let msg_type = dict
            .get(b"msg_type".as_slice())
            .and_then(Value::as_int)
            .unwrap_or(-1);
        if msg_type == UT_METADATA_REJECT {
            // 流水线模式下，一片被拒后必须从 requested 集合移除，
            // 否则其他 peer 永远不会再请求该片 → metadata 永久收不齐。
            // 从拒绝消息中解析 piece 索引并解除占用。
            if let Some(p) = dict.get(b"piece".as_slice()).and_then(Value::as_int) {
                let mut md = self.metadata.lock().unwrap();
                md.requested.remove(&(p as usize));
            }
            return Err("对端拒绝提供元数据".into());
        }
        if msg_type != UT_METADATA_DATA {
            // 元数据交换阶段对端可能发来 REQUEST（我们还没有元数据），
            // 旧实现在此报错断开会话；非 data 消息一律忽略（REJECT 由
            // run_metadata_exchange 显式回复）。
            return Ok(());
        }
        let piece = dict
            .get(b"piece".as_slice())
            .and_then(Value::as_int)
            .ok_or_else(|| "缺少 piece".to_string())? as usize;
        let total_size = dict
            .get(b"total_size".as_slice())
            .and_then(Value::as_int)
            .ok_or_else(|| "缺少 total_size".to_string())? as usize;
        // total_size 上限校验（恶意 peer 可声明 16MB 撑爆 with_capacity）
        if total_size == 0 || total_size > MAX_METADATA_SIZE {
            return Err(format!("元数据总大小非法: {total_size}"));
        }
        let info_bytes = &payload[consumed..];
        if info_bytes.is_empty() {
            return Err("空元数据分片".into());
        }
        let piece_count = total_size.div_ceil(METADATA_PIECE_SIZE);
        // piece 索引无界校验
        if piece >= piece_count {
            return Err(format!("元数据分片索引越界: {piece} >= {piece_count}"));
        }
        // 分片长度必须与 piece 位置一致（最后一片除外）——
        // 短分片会在 assemble 时拼出小于 expected_size 的数据
        let expected_len = METADATA_PIECE_SIZE.min(total_size - piece * METADATA_PIECE_SIZE);
        if info_bytes.len() != expected_len {
            return Err(format!(
                "元数据分片长度错误: {} != {expected_len}",
                info_bytes.len()
            ));
        }
        {
            let mut md = self.metadata.lock().unwrap();
            // 所有分片的 total_size 必须一致，不一致说明 peer 声明冲突
            // （可能被投毒）—— 清空重置，从声明一致的 peer 重新收集
            if md.expected_size != Some(total_size) {
                md.expected_size = Some(total_size);
                md.pieces.clear();
                md.requested.clear();
            }
            md.pieces.insert(piece, info_bytes.to_vec());
            if let Some(assembled) = md.assemble() {
                drop(md);
                let meta = match xfer_bencode::parse_info_bytes(&assembled) {
                    Ok(m) => m,
                    // 解析失败必须清空累积器，否则残留分片毒化后续会话
                    Err(e) => {
                        self.metadata.lock().unwrap().reset();
                        return Err(format!("元数据解析失败: {e}"));
                    }
                };
                // info_hash 不匹配（拼出的是别的内容）同样清空
                if meta.info_hash != self.info_hash {
                    self.metadata.lock().unwrap().reset();
                    return Err("元数据 info_hash 不匹配".into());
                }
                self.install_metadata(meta)?;
                return Ok(());
            }
        }
        Ok(())
    }

    /// 服务对端的 ut_metadata 请求（BEP 9 服务端语义）。
    ///
    /// 我方在扩展握手中声明 `ut_metadata=2`，对端（尤其磁力用户）可能向我们
    /// 请求元数据。store 就绪时按 16 KiB 分片回 DATA（携带 info 字典原始字节，
    /// [`TorrentMeta::raw_info`]）；未就绪或索引越界回 REJECT。
    /// 回复必须使用**对端**广告的 ut_metadata ext_id（BEP 10 语义）。
    async fn serve_ut_metadata(
        &self,
        stream: &mut PeerStream,
        payload: &[u8],
        cell: &Arc<PeerCell>,
    ) -> Result<(), String> {
        use std::collections::BTreeMap;
        use xfer_bencode::{decode_prefix, encode, Value};

        let reply_ext_id = cell.state.lock().unwrap().ut_metadata_id;
        if reply_ext_id == 0 {
            // 对端未在扩展握手中声明 ut_metadata：无法构造它能识别的回复
            return Ok(());
        }

        let v = match decode_prefix(payload) {
            Ok((v, _)) => v,
            Err(_) => return Ok(()),
        };
        let dict = match v.as_dict() {
            Some(d) => d,
            None => return Ok(()),
        };
        let msg_type = dict
            .get(b"msg_type".as_slice())
            .and_then(Value::as_int)
            .unwrap_or(-1);
        // 只处理 REQUEST；data/reject 属客户端路径，不在此响应
        if msg_type != UT_METADATA_REQUEST {
            return Ok(());
        }
        let piece = match dict.get(b"piece".as_slice()).and_then(Value::as_int) {
            Some(p) if p >= 0 => p as usize,
            _ => return Ok(()),
        };

        let mut d = BTreeMap::new();
        let info = self
            .meta
            .read()
            .unwrap()
            .as_ref()
            .and_then(|m| m.raw_info.clone());
        match info {
            Some(info) => {
                let count = info.len().div_ceil(METADATA_PIECE_SIZE);
                if piece < count {
                    let start = piece * METADATA_PIECE_SIZE;
                    let end = (start + METADATA_PIECE_SIZE).min(info.len());
                    d.insert(b"msg_type".to_vec(), Value::Int(UT_METADATA_DATA));
                    d.insert(b"piece".to_vec(), Value::Int(piece as i64));
                    d.insert(b"total_size".to_vec(), Value::Int(info.len() as i64));
                    let mut body = encode(&Value::Dict(d));
                    body.extend_from_slice(&info[start..end]);
                    stream
                        .write_all(
                            &Message::Extended {
                                ext_id: reply_ext_id,
                                payload: body,
                            }
                            .encode(),
                        )
                        .await
                        .map_err(|e| format!("metadata DATA 发送失败: {e}"))?;
                } else {
                    // 索引越界：REJECT
                    d.insert(b"msg_type".to_vec(), Value::Int(UT_METADATA_REJECT));
                    d.insert(b"piece".to_vec(), Value::Int(piece as i64));
                    stream
                        .write_all(
                            &Message::Extended {
                                ext_id: reply_ext_id,
                                payload: encode(&Value::Dict(d)),
                            }
                            .encode(),
                        )
                        .await
                        .map_err(|e| format!("metadata REJECT 发送失败: {e}"))?;
                }
            }
            None => {
                // 元数据未就绪（磁力模式自身还在收集）：REJECT
                d.insert(b"msg_type".to_vec(), Value::Int(UT_METADATA_REJECT));
                d.insert(b"piece".to_vec(), Value::Int(piece as i64));
                stream
                    .write_all(
                        &Message::Extended {
                            ext_id: reply_ext_id,
                            payload: encode(&Value::Dict(d)),
                        }
                        .encode(),
                    )
                    .await
                    .map_err(|e| format!("metadata REJECT 发送失败: {e}"))?;
            }
        }
        Ok(())
    }

    /// 下一个缺失且未请求的元数据分片索引（全部已请求则 None）。
    fn next_metadata_piece(&self) -> Option<usize> {
        let mut md = self.metadata.lock().unwrap();
        let count = md.piece_count()?;
        for i in 0..count {
            if !md.requested.contains(&i) && !md.pieces.contains_key(&i) {
                md.requested.insert(i);
                return Some(i);
            }
        }
        None
    }

    /// 释放「已请求但未收到」的元数据分片占用：peer 会话失败/断开时调用。
    ///
    /// 流水线模式下，本连接请求过的分片若在收到 data 前断开，会永久留在全局
    /// `requested` 集合 → 其他 peer 的 `next_metadata_piece` 永远不会再选它 →
    /// metadata 收不齐直到整体超时。释放只可能造成重复请求（幂等覆盖），
    /// 不会丢失已收到的分片，因此多 peer 并发下也是安全的。
    fn release_unreceived_metadata_pieces(&self) {
        let mut md = self.metadata.lock().unwrap();
        let Some(count) = md.piece_count() else {
            // 未知 total_size（首片未收到）：此时只有 piece 0 可能在途，
            // 全部释放（0 会由后续 peer 的冗余首片请求重新拾起）。
            if !md.pieces.contains_key(&0) {
                md.requested.remove(&0);
            }
            return;
        };
        for i in 0..count {
            if md.requested.contains(&i) && !md.pieces.contains_key(&i) {
                md.requested.remove(&i);
            }
        }
    }

    /// 计算 Allowed Fast Set (BEP 6)。
    ///
    /// 基于 BEP 6 规范：使用对端 IP + info_hash 计算 SHA1 哈希链，
    /// 取前 N 个不同的片索引。
    fn compute_allowed_fast_set(&self, peer_addr: &SocketAddr) -> Vec<u32> {
        let ip = match peer_addr.ip() {
            IpAddr::V4(v4) => v4.octets(),
            IpAddr::V6(_) => return Vec::new(), // Fast Set 仅支持 IPv4
        };
        let num_pieces = self.store.lock().unwrap().as_ref().unwrap().piece_count();
        if num_pieces == 0 {
            return Vec::new();
        }
        let fast_set_size = (ALLOWED_FAST_SET_SIZE as u32).min(num_pieces);
        let info_hash = &self.info_hash;

        // 构造初始输入: 4 bytes IP (masked) + 20 bytes info_hash = 24 bytes
        let mut tx = [0u8; 24];
        tx[0..4].copy_from_slice(&ip);
        // BEP 6: mask last bytes of IP
        if (tx[0] & 0x80) == 0 || (tx[0] & 0x40) == 0 {
            tx[2] = 0;
            tx[3] = 0;
        } else {
            tx[3] = 0;
        }
        tx[4..24].copy_from_slice(info_hash);

        let mut x = [0u8; 20];
        // SHA1(tx) → x
        use sha1::{Digest, Sha1};
        let mut hasher = Sha1::new();
        hasher.update(tx);
        x.copy_from_slice(&hasher.finalize());

        let mut fast_set = Vec::new();
        while fast_set.len() < fast_set_size as usize {
            for i in 0..5 {
                if fast_set.len() >= fast_set_size as usize {
                    break;
                }
                let j = i * 4;
                let ny = u32::from_be_bytes([x[j], x[j + 1], x[j + 2], x[j + 3]]);
                let index = ny % num_pieces;
                if !fast_set.contains(&index) {
                    fast_set.push(index);
                }
            }
            // SHA1(x) → x (hash chain)
            let mut h2 = Sha1::new();
            h2.update(x);
            x.copy_from_slice(&h2.finalize());
        }
        fast_set
    }

    /// 广播 Have 消息给所有已连接的对端（BEP 3）。
    ///
    /// 每 peer 有独立 stream，无法在此直接写出。这里把片号入队各 peer 的
    /// `have_out_queue`，由各自消息循环在下一轮（choke_timer）出队发送。
    /// 不发送 Have 会导致对端永远不知道我们完成了哪些片 → 无法回传兴趣/请求。
    fn broadcast_have(&self, piece: u32) {
        let peers = self.peers.read().unwrap();
        for cell in peers.values() {
            cell.have_out_queue.lock().unwrap().push(piece);
        }
    }

    /// Choking 算法决策（BEP 3 Choking Algorithm）。
    ///
    /// 每 peer 会话在自己的 10s tick 里调用，但决策所需的全局状态
    /// （轮次、乐观 unchoke 选中项）由墙钟 + 引擎级共享字段推导，
    /// 因此所有会话在同一窗口内得出一致的全局视角：
    /// - Leecher 模式：按下载速率 unchoke 前 3 + 1 个乐观 unchoke
    /// - Seeder 模式（全员速率为 0）：按「最久未服务优先」轮转
    ///
    /// 修复要点：① 轮次不再由 per-peer tick 推进全局计数器（50 peer =
    /// 每 10s 推 50 次，「每 3 轮一次乐观 unchoke」完全失真）；② 乐观
    /// unchoke 选中项改为引擎级共享状态，lucky peer 自己的 tick 必然
    /// 把自己算进 unchoke_set → 真正发出 Unchoke（原先只写标志无人读）；
    /// ③ 做种时全员速率 0，按速率排序退化为 HashMap 迭代序随机选——
    /// 改为 last_unchoke 升序轮转。
    async fn decide_choking(
        self: &Arc<Self>,
        stream: &mut PeerStream,
        cell: &Arc<PeerCell>,
    ) -> Result<(), String> {
        // 全局 choking 轮次：由墙钟推导，全局一致（0/1/2 循环，窗口 10s）
        let window = Instant::now().duration_since(self.choke_epoch).as_secs() / 10;
        let round = (window % 3) as u32;

        // 收集所有活跃 peer 的速率信息（addr, 速率, interested, we_choked, last_unchoke）
        let mut peer_entries: Vec<(SocketAddr, u64, bool, bool, Instant)> = {
            let peers = self.peers.read().unwrap();
            let mut entries: Vec<(SocketAddr, u64, bool, bool, Instant)> = Vec::new();
            for (addr, c) in peers.iter() {
                let st = c.state.lock().unwrap();
                entries.push((
                    *addr,
                    st.recent_speed,
                    st.peer_interested,
                    st.we_choked,
                    st.last_unchoke,
                ));
            }
            entries
        };

        // 排序：有下载贡献时按速率降序；全员速率为 0（做种/冷启动）时按
        // 「最近一次被 unchoke」升序——最久未服务的优先，上传轮转公平
        let total_download: u64 = peer_entries.iter().map(|e| e.1).sum();
        if total_download == 0 {
            peer_entries.sort_by_key(|e| e.4);
        } else {
            peer_entries.sort_by_key(|e| std::cmp::Reverse(e.1));
        }

        // 常规 unchoke：选择排序最前的 N 个 interested peer
        let regular_unchoke_count = 3;
        let mut unchoke_set: HashSet<SocketAddr> = HashSet::new();
        for (addr, _, interested, _, _) in peer_entries.iter() {
            if unchoke_set.len() >= regular_unchoke_count {
                break;
            }
            if *interested {
                unchoke_set.insert(*addr);
            }
        }

        // 乐观 unchoke（引擎级共享状态）：每逢 round==0 的窗口重掷一次，
        // 窗口间保持不变（lucky peer 保持 unchoke 约 30s 后回收轮换）
        {
            let mut opt = self.optimistic_unchoke.lock().unwrap();
            let peers = self.peers.read().unwrap();
            let current = opt.filter(|o| peers.contains_key(&o.addr));
            // 重掷条件：无有效选中项，或进入新的 round==0 窗口
            let should_roll = match current {
                Some(o) => round == 0 && o.window != window,
                None => true,
            };
            let mut entry = current;
            if should_roll {
                // 回收旧 lucky 的标志
                if let Some(o) = current {
                    if let Some(c) = peers.get(&o.addr) {
                        c.state.lock().unwrap().opt_unchoking = false;
                    }
                }
                // 从被 choke 的 interested peer 中随机选一个
                let candidates: Vec<SocketAddr> = peer_entries
                    .iter()
                    .filter(|(addr, _, interested, we_choked, _)| {
                        *interested && *we_choked && !unchoke_set.contains(addr)
                    })
                    .map(|(addr, _, _, _, _)| *addr)
                    .collect();
                entry = None;
                if !candidates.is_empty() {
                    let mut rand_buf = [0u8; 4];
                    let _ = getrandom::fill(&mut rand_buf);
                    let idx = u32::from_le_bytes(rand_buf) as usize % candidates.len();
                    let lucky = candidates[idx];
                    if let Some(c) = peers.get(&lucky) {
                        c.state.lock().unwrap().opt_unchoking = true;
                    }
                    entry = Some(OptimisticUnchoke {
                        addr: lucky,
                        window,
                    });
                }
                *opt = entry;
            }
            if let Some(o) = entry {
                unchoke_set.insert(o.addr);
            }
        }

        // 应用 choking 决策到当前 peer
        let should_choke = !unchoke_set.contains(&cell.addr);
        {
            let mut st = cell.state.lock().unwrap();
            st.choking_required = should_choke;
        }

        // 发送 choke/unchoke 消息
        let current_choked = cell.state.lock().unwrap().we_choked;
        if should_choke && !current_choked {
            cell.state.lock().unwrap().we_choked = true;
            stream
                .write_all(&Message::Choke.encode())
                .await
                .map_err(|e| e.to_string())?;
        } else if !should_choke && current_choked {
            cell.state.lock().unwrap().we_choked = false;
            cell.state.lock().unwrap().last_unchoke = Instant::now();
            stream
                .write_all(&Message::Unchoke.encode())
                .await
                .map_err(|e| e.to_string())?;
        }

        Ok(())
    }

    /// 发送 PEX 消息（BEP 11）。
    async fn send_pex_message(
        &self,
        stream: &mut PeerStream,
        cell: &Arc<PeerCell>,
        pex_exchange: &mut xfer_discovery::pex::PexExchange,
    ) -> Result<(), String> {
        let ut_pex_id = cell.state.lock().unwrap().ut_pex_id;
        if ut_pex_id == 0 {
            return Ok(()); // 对端不支持 PEX
        }

        // 收集当前已连接的 peer 列表（过滤回环地址，不通过 PEX 通告给远端）
        let current_peers: Vec<(SocketAddr, PeerId, u8)> = {
            let peers = self.peers.read().unwrap();
            peers
                .values()
                .filter(|c| c.addr != cell.addr) // 不包含目标 peer 自身
                .filter(|c| !is_loopback(&c.addr)) // 不通告回环地址
                .map(|c| {
                    let st = c.state.lock().unwrap();
                    // 本引擎只走 TCP，不能带 UTP 标志，否则对端会尝试
                    // 永远连不通的 uTP 连接
                    let flags = if st.encrypted {
                        xfer_discovery::pex::flags::ENCRYPTION
                    } else {
                        0
                    };
                    (c.addr, st.peer_id.unwrap_or(PeerId([0; 20])), flags)
                })
                .collect()
        };

        let pex_msg = pex_exchange.generate_message(&current_peers);
        if pex_msg.added.is_empty() && pex_msg.dropped.is_empty() {
            return Ok(());
        }

        let payload = pex_msg.encode();
        stream
            .write_all(
                &Message::Extended {
                    ext_id: ut_pex_id,
                    payload,
                }
                .encode(),
            )
            .await
            .map_err(|e| format!("PEX 发送失败: {e}"))?;
        Ok(())
    }

    /// 消息洪泛检测（参考 C++ detectMessageFlooding）。
    ///
    /// 在 5 秒区间内，如果 choke/unchoke 切换 ≥ 2 次或 keep-alive ≥ 2 次，
    /// 则判定为洪泛，断开连接。
    fn detect_flooding(&self, cell: &Arc<PeerCell>) -> Result<(), String> {
        let mut st = cell.state.lock().unwrap();
        let elapsed = Instant::now().duration_since(st.flooding_check_at);
        if elapsed >= FLOODING_CHECK_INTERVAL {
            if st.choke_unchoke_count >= FLOODING_THRESHOLD
                || st.keepalive_count >= FLOODING_THRESHOLD
            {
                return Err(format!(
                    "检测到消息洪泛 (choke/unchoke={}, keep-alive={})",
                    st.choke_unchoke_count, st.keepalive_count
                ));
            }
            st.choke_unchoke_count = 0;
            st.keepalive_count = 0;
            st.flooding_check_at = Instant::now();
        }
        Ok(())
    }

    /// 非活跃连接检测（参考 C++ checkActiveInteraction）。
    ///
    /// - 互不感兴趣超过 120s → 断开
    /// - 无数据传输超过 120s → 断开
    /// - 双方都是 seeder → 断开
    fn check_active_interaction(&self, cell: &Arc<PeerCell>) -> Result<(), String> {
        // 先在 state 锁内取快照并释放，再调用 is_done()（取 PieceStore 锁）。
        // 若持 state 锁调 is_done，会与「持 store 锁取 state 锁」的选片路径
        // 构成 AB-BA 环，整个 runtime 会在真实蜂群上数秒内冻死。
        let (we_interested, peer_interested, is_seed, inactive, connected_for) = {
            let st = cell.state.lock().unwrap();
            let now = Instant::now();
            (
                st.we_interested,
                st.peer_interested,
                st.is_seed,
                now.duration_since(st.last_data_transfer),
                now.duration_since(st.connected_at),
            )
        };

        // 互不感兴趣超时（仅在双方都不感兴趣时触发）
        if !we_interested && !peer_interested && inactive >= NO_INTEREST_TIMEOUT {
            return Err(format!("互不感兴趣超过 {NO_INTEREST_TIMEOUT:?}，断开连接"));
        }

        // 非活跃超时（仅在已连接足够久且无任何数据传输时触发）
        if inactive >= INACTIVE_TIMEOUT && connected_for >= INACTIVE_TIMEOUT {
            return Err(format!("无数据传输超过 {INACTIVE_TIMEOUT:?}，断开连接"));
        }

        // 双 seeder 检测。用 finished 原子而非 is_done()：本函数每条
        // 消息都执行，连上 seed 时每收一个块都会走这里，is_done()
        // 会与选片/落盘路径争抢 store 锁。finished 在 finish() 置位，
        // 语义等价（下载完成后才需要断开双 seeder）。
        if is_seed && self.finished.load(Ordering::Relaxed) {
            return Err("双方都是 seeder，断开连接".into());
        }

        Ok(())
    }

    /// 自适应流水线深度调整：窗口接近打满且刚收到新块 → 乘性扩容。
    ///
    /// 加法增长（+1）追不上带宽时延积；以窗口利用率作增长信号
    /// （类 TCP 慢启动），与 RTT 无关，高延迟链路同样有效。
    /// 只在新块到达时调用，因此只做增长；「停滞减半」由
    /// peer_message_loop 的 choke tick 检测（那里才可能观察到
    /// 长时间无新块——本函数被调用本身就意味着刚收到了块）。
    fn adjust_pipeline(&self, ctx: &mut PeerCtx) {
        let mut pl = ctx.cell.pipeline.lock().unwrap();
        let current = *pl;
        if ctx.in_flight.len() * 4 >= current * 3 {
            *pl = (current + (current / 4).max(1)).min(PIPELINE_MAX);
        }
    }

    /// 流水线停滞减半：有在途请求但长时间收不到块时递减窗口。
    ///
    /// 与请求超时重发（`REQUEST_TIMEOUT`）配合：先缩窗减少无效
    /// 在途，再由超时机制补发。仅在 choke tick 调用——那里才能
    /// 观察到「长时间无新块」。
    fn shrink_stalled_pipeline(&self, ctx: &mut PeerCtx) {
        if ctx.in_flight.is_empty() {
            return;
        }
        if ctx.cell.last_block_at.lock().unwrap().elapsed() <= Duration::from_secs(5) {
            return;
        }
        ctx.stale_count += 1;
        if ctx.stale_count > 3 {
            let mut pl = ctx.cell.pipeline.lock().unwrap();
            *pl = (*pl / 2).max(PIPELINE_MIN);
            ctx.stale_count = 0;
        }
    }

    /// seed 模式：unchoke 对端（简单策略：立即 unchoke）。
    async fn maybe_unchoke_peer(
        &self,
        stream: &mut PeerStream,
        cell: &Arc<PeerCell>,
    ) -> Result<(), String> {
        let should_unchoke = {
            let mut st = cell.state.lock().unwrap();
            if st.we_choked && st.peer_interested {
                st.we_choked = false;
                true
            } else {
                false
            }
        };
        if should_unchoke {
            stream
                .write_all(&Message::Unchoke.encode())
                .await
                .map_err(|e| format!("unchoke 发送失败: {e}"))?;
        }
        Ok(())
    }

    /// seed 模式：响应上传请求，从存储读取块并发送。
    ///
    /// 请求参数来自对端，必须校验：恶意构造的巨大 `length` 会触发
    /// 巨量分配（OOM）；64 KiB 是响应请求的生态上限（旧引擎 aria2
    /// MAX_BLOCK_LENGTH 语义，§7.11）。
    async fn serve_block(
        &self,
        stream: &mut PeerStream,
        cell: &PeerCell,
        index: u32,
        begin: u32,
        length: u32,
    ) -> Result<(), String> {
        const MAX_SERVE_BLOCK: u32 = 64 * 1024;
        if length == 0 || length > MAX_SERVE_BLOCK {
            tracing::debug!(
                peer = %cell.addr,
                index,
                begin,
                length,
                "拒绝非法请求长度"
            );
            return Ok(());
        }
        let data = {
            let mut guard = self.store.lock().unwrap();
            let store = guard.as_mut().unwrap();
            if !store.have_piece(index) {
                return Ok(()); // 我们没有这片
            }
            // 钳制到片内范围：越界请求不产生数据（对端容忍截断，
            // 但越界读可能 read_exact 失败）
            let plen = store.piece_len(index) as u32;
            if begin >= plen {
                return Ok(());
            }
            let length = length.min(plen - begin);
            store
                .read_block(index, begin, length)
                .map_err(|e| format!("读取块失败: {e}"))?
        };
        // 上传限速：发送前消费令牌，循环直到扣减成功（与下载限速同语义；
        // 单次封顶 500ms 的旧实现对低限速值形同虚设）
        if self.ul_limit.load(Ordering::Relaxed) > 0 {
            let n = data.len() as u64;
            loop {
                let wait = self.upload_limiter.lock().unwrap().try_consume(n);
                match wait {
                    None => break,
                    Some(dur) => {
                        if self.shutdown.is_cancelled() {
                            break;
                        }
                        tokio::time::sleep(dur.min(Duration::from_millis(500))).await;
                    }
                }
            }
        }
        let sent = data.len() as u64;
        stream
            .write_all(
                &Message::Piece {
                    index,
                    begin,
                    block: data,
                }
                .encode(),
            )
            .await
            .map_err(|e| format!("piece 发送失败: {e}"))?;
        self.uploaded_bytes.fetch_add(sent, Ordering::Relaxed);
        cell.uploaded.fetch_add(sent, Ordering::Relaxed);
        Ok(())
    }

    /// 指定片是否已收齐全部块：应收/已收计数比较，O(1) 无锁。
    /// （旧实现每收一块都锁 store + 分配 request_blocks 全量扫描。）
    fn piece_complete(&self, ctx: &PeerCtx, index: u32) -> bool {
        match (ctx.block_need.get(&index), ctx.block_have.get(&index)) {
            (Some(&need), Some(&have)) => need > 0 && have >= need,
            _ => false,
        }
    }

    /// 我们缺片且对端有 → 发送 interested（幂等）。
    async fn ensure_interested(
        &self,
        stream: &mut PeerStream,
        cell: &Arc<PeerCell>,
    ) -> Result<(), String> {
        // 快速路径：兴趣已声明时直接返回。Have 密集的场景下每条消息都
        // 走本函数，不做此短路会每条消息锁 store + 全片表扫描。
        if cell.state.lock().unwrap().we_interested {
            return Ok(());
        }
        let (want, has, already) = {
            let guard = self.store.lock().unwrap();
            let store = guard.as_ref().unwrap();
            let count = store.piece_count();
            let want = !self.all_wanted_done_with(store);
            let mut st = cell.state.lock().unwrap();
            // 对端拥有的片中，存在我们尚未下载且需要下载的片
            let has = (0..count)
                .any(|p| st.have.is_set(p) && !store.have_piece(p) && self.piece_wanted(p));
            let already = st.we_interested;
            if want && has && !already {
                st.we_interested = true;
            }
            (want, has, already)
        };
        if want && has && !already {
            stream
                .write_all(&Message::Interested.encode())
                .await
                .map_err(|e| format!("interested 发送失败: {e}"))?;
        }
        Ok(())
    }

    /// 释放该 peer 占用的**全部**片（断开 / 被 choke 时）。
    fn release_all_pieces(&self, cell: &Arc<PeerCell>) {
        // 两把锁分开取：先取出再释放，避免锁环
        let taken = std::mem::take(&mut *cell.queued.lock().unwrap());
        if !taken.is_empty() {
            let mut g = self.assigned.lock().unwrap();
            for p in taken {
                g.remove(&p);
            }
        }
    }

    /// 释放该 peer 队列中指定的一片（片完成或校验失败时）。
    fn release_piece(&self, cell: &Arc<PeerCell>, piece: u32) {
        cell.queued.lock().unwrap().retain(|&p| p != piece);
        self.assigned.lock().unwrap().remove(&piece);
    }

    /// 校验并落盘一片（成功时更新进度）。
    fn accept_piece(&self, index: u32, data: &[u8], expected: &[u8; 20]) -> bool {
        let mut guard = self.store.lock().unwrap();
        let store = guard.as_mut().unwrap();
        // 重复完成去重（淘汰换血后同片被双路下载等竞态）：
        // 不去重会让 done_bytes 双计、进度虚高甚至提前报完成。
        if store.have_piece(index) {
            return false;
        }
        match store.accept_piece(index, data, expected) {
            Ok(true) => {
                let n = store.piece_len(index);
                drop(guard);
                self.done_bytes.fetch_add(n, Ordering::Relaxed);
                tracing::debug!(piece = index, "片完成");
                self.save_resume(false);
                true
            }
            Ok(false) => {
                tracing::warn!(piece = index, "片哈希校验失败，重新下载");
                false
            }
            Err(e) => {
                tracing::warn!(piece = index, error = %e, "片落盘失败");
                false
            }
        }
    }

    /// 停止全部后台任务（peer 会话、监听器、连接派发、DHT）。
    ///
    /// run() 的每个退出路径都必须调用：否则僵尸任务在暂停后
    /// 继续下载、写盘并更新续传控制文件，恢复时进度会凭空跳变。
    fn stop_background(&self) {
        self.shutdown.cancel();
        if let Some(d) = self.dht.lock().unwrap().take() {
            d.shutdown();
        }
        if let Some(h) = self.utp.lock().unwrap().take() {
            // 管理器自带 1ms tick 任务，须显式停止避免跨暂停泄漏
            tokio::spawn(async move {
                h.shutdown().await;
            });
        }
    }

    /// 续传控制文件路径（磁力元数据未就绪时为 None）。
    /// 与任务管理器的 `ctrl_path(数据路径)` 键派生一致：
    /// 移除任务时的控制文件清理由此自动覆盖 BT。
    fn resume_ctrl_path(&self) -> Option<PathBuf> {
        let meta = self.meta.read().unwrap();
        let m = meta.as_ref()?;
        Some(xfer_storage::ctrl_path(&self.config.dir.join(&m.info.name)))
    }

    /// 持久化续传控制文件：片完成时节流写入，暂停/停止强制写入。
    ///
    /// 节流写入走阻塞线程池：同步 `std::fs` 直接在 async 上下文
    /// 执行会卡住 tokio 工作线程（巨种子位图可达几百 KB）。强制
    /// 写入保持同步，保证暂停/停止时控制文件一定落盘。
    fn save_resume(&self, force: bool) {
        let Some(ctrl) = self.resume_ctrl_path() else {
            return;
        };
        {
            let mut last = self.last_resume_save.lock().unwrap();
            if !force && last.elapsed() < RESUME_SAVE_INTERVAL {
                return;
            }
            *last = Instant::now();
        }
        let (bf, count) = {
            let guard = self.store.lock().unwrap();
            let Some(store) = guard.as_ref() else {
                return;
            };
            (store.bitfield(), store.piece_count())
        };
        let info_hash = self.info_hash;
        if force {
            if let Err(e) = crate::resume::save(&ctrl, &info_hash, count, &bf) {
                tracing::warn!(error = %e, "续传控制文件写入失败");
            }
        } else {
            tokio::task::spawn_blocking(move || {
                if let Err(e) = crate::resume::save(&ctrl, &info_hash, count, &bf) {
                    tracing::warn!(error = %e, "续传控制文件写入失败");
                }
            });
        }
    }

    /// 请求流水线填充：为该 peer 的片队列按序补发请求（保持 pipeline 在途）。
    ///
    /// 多片模型：每 peer 的片队列容量按「打满流水线窗口所需片数」
    /// 动态计算（下限 `MAX_QUEUED_PIECES`，硬上限 `MAX_QUEUED_PIECES_HARD`），
    /// 在途窗口上限为自适应 pipeline 深度（§7.8：16→256）。
    /// 单片模型下每连接在途至多一片（256KiB），吞吐被
    /// 带宽时延积限死（200ms RTT ≈ 1.3MB/s）；多片并行后
    /// 在途字节 = pipeline × 16KiB，可达 4MB/连接。
    async fn fill_pipeline(
        self: &Arc<Self>,
        stream: &mut PeerStream,
        ctx: &mut PeerCtx,
    ) -> Result<(), String> {
        {
            let st = ctx.cell.state.lock().unwrap();
            if st.choked || !st.we_interested {
                return Ok(());
            }
        }
        let pipeline = *ctx.cell.pipeline.lock().unwrap();
        if ctx.in_flight.len() >= pipeline {
            return Ok(());
        }
        // 队列快照 + 块序列一次取齐：旧实现每发一个请求就克隆一次
        // queued 并重新持 store 锁扫一遍，窗口打满时代价随
        // pipeline² 增长；块序列按片预计算后扫描零分配。
        // 片长元数据就绪后不变，快照安全。
        let mut queued: Vec<u32> = ctx.cell.queued.lock().unwrap().clone();
        let mut pieces: Vec<(u32, Vec<(u32, u32)>)> = {
            let guard = self.store.lock().unwrap();
            let store = guard.as_ref().unwrap();
            queued
                .iter()
                .map(|&p| (p, request_blocks(store.piece_len(p) as u32, BLOCK_SIZE)))
                .collect()
        };
        // 登记应收块数（完成判定用；幂等——重复进入不覆盖）
        for (p, blocks) in &pieces {
            ctx.block_need.entry(*p).or_insert(blocks.len() as u32);
        }
        let mut sent = false;
        loop {
            if ctx.in_flight.len() >= pipeline {
                break;
            }
            // 在队列中找下一个待发块（队首片优先，让先开始的片尽早完成）
            let mut next: Option<(u32, u32, u32)> = None;
            'outer: for &(piece, ref blocks) in &pieces {
                for &(begin, len) in blocks {
                    let key = (piece, begin);
                    if !ctx.blocks.contains_key(&key) && !ctx.in_flight.contains(&key) {
                        next = Some((piece, begin, len));
                        break 'outer;
                    }
                }
            }
            let (piece, begin, len) = match next {
                Some(t) => t,
                None => {
                    // 队列内所有块都已在途/已收：有空位则再领一片。
                    // 容量按「打满窗口所需片数」动态计算，下限 4、硬上限 64。
                    // 队首片尺寸（片小则队列需装更多片才能打满窗口）
                    let blocks_per_piece = pieces
                        .first()
                        .map(|(_, blocks)| blocks.len().max(1))
                        .unwrap_or(16);
                    let cap = (pipeline.div_ceil(blocks_per_piece))
                        .max(MAX_QUEUED_PIECES)
                        .min(MAX_QUEUED_PIECES_HARD);
                    if queued.len() >= cap {
                        break;
                    }
                    let Some(p) = self.assign_piece(&ctx.cell) else {
                        break;
                    };
                    let blocks = {
                        let guard = self.store.lock().unwrap();
                        request_blocks(guard.as_ref().unwrap().piece_len(p) as u32, BLOCK_SIZE)
                    };
                    ctx.block_need.entry(p).or_insert(blocks.len() as u32);
                    queued.push(p);
                    pieces.push((p, blocks));
                    ctx.cell.queued.lock().unwrap().push(p);
                    continue;
                }
            };
            stream
                .write_all(
                    &Message::Request {
                        index: piece,
                        begin,
                        length: len,
                    }
                    .encode(),
                )
                .await
                .map_err(|e| format!("request 发送失败: {e}"))?;
            ctx.in_flight.insert((piece, begin));
            ctx.last_request_at = Some(Instant::now());
            sent = true;
        }
        if sent {
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    /// rarest-first 选片：对端拥有、我方缺失、未被分配中，
    /// 稀有度 = 当前连接 peers 中拥有该片的人数（越少越优先）。
    ///
    /// 快照式实现：各把锁单独短持取快照，无锁计算，最后在 `assigned`
    /// 锁内原子注册选中的片。避免与持 PeerState 锁访问 store 的路径
    /// 构成锁环，也避免长时间持有 store 锁阻塞其他 peer。
    fn assign_piece(&self, cell: &Arc<PeerCell>) -> Option<u32> {
        let (count, store_have) = {
            let guard = self.store.lock().unwrap();
            let store = guard.as_ref().unwrap();
            (store.piece_count(), store.map().clone())
        };
        let peer_have = cell.state.lock().unwrap().have.clone();
        let peer_haves: Vec<PieceMap> = {
            let peers = self.peers.read().unwrap();
            peers
                .values()
                .map(|p| p.state.lock().unwrap().have.clone())
                .collect()
        };

        // (稀有度, 片号)，按稀有度升序
        let mut candidates: Vec<(u32, u32)> = Vec::new();
        for idx in 0..count {
            if !peer_have.is_set(idx) || store_have.is_set(idx) || !self.piece_wanted(idx) {
                continue;
            }
            let rarity = peer_haves.iter().filter(|h| h.is_set(idx)).count() as u32;
            candidates.push((rarity, idx));
        }
        candidates.sort_unstable();

        // 原子注册：在 assigned 锁内取第一个未被占用的片，
        // 避免多个 peer 同时选中同一片造成重复下载
        let mut assigned = self.assigned.lock().unwrap();
        for (_, idx) in candidates {
            if !assigned.contains(&idx) {
                assigned.insert(idx);
                return Some(idx);
            }
        }
        None
    }
}

/// 从续传控制文件恢复已完成片位图。
///
/// 只信任「片的字节区间被磁盘现有长度完全覆盖」的位——
/// 防御控制文件写入后文件被截断/部分删除的情形。
/// 成功恢复（至少一片）时应用到 store 并返回 true。
fn restore_resume(
    ctrl: &Path,
    info_hash: &[u8; 20],
    dir: &Path,
    name: &str,
    store: &mut PieceStore,
) -> bool {
    let count = store.piece_count();
    let Some(mut bf) = crate::resume::load(ctrl, info_hash, count) else {
        return false;
    };
    // 各文件现有磁盘长度
    let layout = store.layout();
    let single = layout.files.len() == 1 && layout.files[0].path.len() == 1;
    let base = dir.join(name);
    let lens: Vec<u64> = layout
        .files
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let path = if single && i == 0 {
                base.clone()
            } else {
                base.join(f.path.iter().collect::<PathBuf>())
            };
            std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0)
        })
        .collect();
    // 掩码：未完全覆盖的位清除
    let mut restored = 0u32;
    for idx in 0..count {
        let byte = (idx / 8) as usize;
        let mask = 0x80u8 >> (idx % 8);
        if bf[byte] & mask == 0 {
            continue;
        }
        let covered = layout
            .piece_segments(idx)
            .iter()
            .all(|&(fi, off, len)| lens[fi] >= off + len);
        if covered {
            restored += 1;
        } else {
            bf[byte] &= !mask;
        }
    }
    if restored == 0 {
        return false;
    }
    store.set_bitfield(&bf);
    tracing::info!(pieces = restored, count, "续传控制文件恢复已完成片");
    true
}

impl PeerCtx {
    fn new(cell: Arc<PeerCell>) -> Self {
        Self {
            cell,
            in_flight: HashSet::new(),
            blocks: HashMap::new(),
            block_need: HashMap::new(),
            block_have: HashMap::new(),
            last_request_at: None,
            stale_count: 0,
        }
    }
}

/// 解析 UDP tracker URL（`udp://host:port[/path]` → SocketAddr）。
///
/// 真实种子的 udp:// URL 几乎都带路径后缀（如
/// `udp://tracker.example.com:6969/announce`）且 host 多为域名；
/// 仅接受 `IP:port` 字面量会导致这些 tracker 全部被静默跳过（零 peer）。
/// BEP 15 compact peers 仅 IPv4，DNS 解析优先取 IPv4 结果。
async fn resolve_udp_tracker_url(url: &str) -> Option<SocketAddr> {
    let rest = url.strip_prefix("udp://")?;
    let hostport = rest.split('/').next()?;
    if hostport.is_empty() {
        return None;
    }
    if let Ok(addr) = hostport.parse::<SocketAddr>() {
        return Some(addr);
    }
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host(hostport).await.ok()?.collect();
    addrs
        .iter()
        .find(|a| a.is_ipv4())
        .copied()
        .or_else(|| addrs.first().copied())
}

/// 对端客户端描述（PeerInfo.client）：BEP 10 扩展握手自报的 "v"
/// 优先（如 "qBittorrent/4.6.0"），否则退回 peer_id 前缀解析
/// （带版本号，如 "qBittorrent 4.5.7"）。
fn peer_client_desc(st: &PeerState) -> String {
    if let Some(v) = &st.client_version {
        return v.clone();
    }
    client_name_from_peer_id(st.peer_id)
}

/// 对端下载进度百分比（PeerInfo.progress）。
///
/// - 对端是 seed（HaveAll/完成）→ 100；
/// - 位图已知（片数 > 0）→ done/count 百分比；
/// - 磁力元数据未就绪（位图 0 片）→ None（前端显示 "-"）。
fn peer_progress(st: &PeerState) -> Option<f32> {
    if st.is_seed {
        return Some(100.0);
    }
    let total = st.have.count();
    if total == 0 {
        return None;
    }
    Some(st.have.done_count() as f32 / total as f32 * 100.0)
}

/// Azureus 前缀的版本段解析：`-qB4570-` → "4.5.7"。
/// 每字节一个数字（BEP 20），尾零省略。
fn azureus_version(b: &[u8]) -> Option<String> {
    if b.len() < 4 || !b[..4].iter().all(u8::is_ascii_digit) {
        return None;
    }
    let digits: Vec<u32> = b[..4].iter().map(|&d| (d - b'0') as u32).collect();
    // 去掉尾部零（4.5.7.0 → 4.5.7；2.0.0.0 → 2）
    let mut end = digits.len();
    while end > 1 && digits[end - 1] == 0 {
        end -= 1;
    }
    let parts: Vec<String> = digits[..end].iter().map(|d| d.to_string()).collect();
    Some(parts.join("."))
}

/// 从 peer_id 解析 BT 客户端名称与版本（Azureus 风格前缀 `-XX####-`）。
///
/// 常见前缀映射参考 BEP 20 及各客户端文档；版本号取前缀中的 4 位数字。
fn client_name_from_peer_id(peer_id: Option<PeerId>) -> String {
    let Some(pid) = peer_id else {
        return String::new();
    };
    let b = &pid.0;
    // Azureus 风格：-XX####-  (2 字母 + 4 数字 + '-')
    if b.len() >= 8 && b[0] == b'-' && b[7] == b'-' {
        let code = &b[1..3];
        let name = match code {
            b"AZ" => "Azureus",
            b"BT" => "BitTorrent",
            b"qB" => "qBittorrent",
            b"TR" => "Transmission",
            b"UT" => "uTorrent",
            b"UM" => "uTorrent Mac",
            b"DE" => "Deluge",
            b"XL" => "Xunlei",
            b"XR" => "XferRust",
            b"lt" => "libtorrent (Rasterbar)",
            b"LT" => "libtorrent (Arvid)",
            b"BC" => "BitComet",
            b"BJ" => "BitJuggler",
            b"BM" => "BitMagnet",
            b"BO" => "BitOrb",
            b"BP" => "BitTorrent Pro",
            b"BS" => "BTSlave",
            b"BX" => "Bittorrent X",
            b"CB" => "Shareaza Plus",
            b"CO" => "Coincidence",
            b"CT" => "CTorrent",
            b"DP" => "Propagate Data Client",
            b"EB" => "EBit",
            b"FC" => "FileCroc",
            b"FG" => "FlashGet",
            b"FL" => "Folx",
            b"FT" => "FoxTorrent",
            b"FX" => "FreeBox BitTorrent",
            b"GS" => "BT Next",
            b"HK" => "Hekate",
            b"HL" => "Halite",
            b"HN" => "Hydranode",
            b"IL" => "ILCorum",
            b"JS" => "Justseed.it",
            b"JT" => "JavaTorrent",
            b"KG" => "KGet",
            b"KT" => "KTorrent",
            b"LC" => "LeechCraft",
            b"LH" => "LH-ABC",
            b"LP" => "Lphant",
            b"MK" => "Meerkat",
            b"MO" => "MonoTorrent",
            b"MP" => "MooPolice",
            b"MR" => "Miro",
            b"MT" => "MoonlightTorrent",
            b"NB" => "Net::BitTorrent",
            b"NE" => "BT Next",
            b"NX" => "Net Transport",
            b"OS" => "OspreyPermaseed",
            b"OT" => "OmegaTorrent",
            b"PB" => "Protocol::BitTorrent",
            b"PD" => "Pando",
            b"PE" => "PicoTorrent",
            b"PT" => "BT Next",
            b"QD" => "QQDownload",
            b"RT" => "Retriever",
            b"RZ" => "RezTorrent",
            b"SD" => "Thunder",
            b"SM" => "SoMud",
            b"SP" => "BitSpirit",
            b"SS" => "SwarmScope",
            b"ST" => "SymTorrent",
            b"st" => "sharktorrent",
            b"SZ" => "Shareaza",
            b"TB" => "Torch",
            b"TG" => "Tuge",
            b"TK" => "TorrentKeeper",
            b"TL" => "Tlcn",
            b"TN" => "TorrentDotNET",
            b"TS" => "Torrentstorm",
            b"TT" => "TuoTu",
            b"UE" => "uTorrent Embedded",
            b"UL" => "uLeecher",
            b"VG" => "Vagaa",
            b"WD" => "WebTorrent",
            b"WT" => "BitLet",
            b"WW" => "WebTorrent",
            b"WY" => "FireTorrent",
            b"XS" => "XSwifter",
            b"XT" => "XanTorrent",
            b"XX" => "Xtorrent",
            b"ZT" => "ZipTorrent",
            _ => "",
        };
        if !name.is_empty() {
            // Transmission 特殊：4 位数字是 major(2).minor(1).patch(1)，
            // 如 -TR0403- → 4.0.3（major 前导零省略）
            if code == b"TR" && b[3..7].iter().all(u8::is_ascii_digit) {
                let major = (b[3] - b'0') as u32 * 10 + (b[4] - b'0') as u32;
                let minor = (b[5] - b'0') as u32;
                let patch = (b[6] - b'0') as u32;
                return format!("{name} {major}.{minor}.{patch}");
            }
            // 版本段在 b[3..7]（如 -qB4570- → 4.5.7）
            if let Some(v) = azureus_version(&b[3..]) {
                return format!("{name} {v}");
            }
            return name.to_string();
        }
        // 未知前缀：显示原始 2 字母代码（带版本，若可解析）
        let code_str = String::from_utf8_lossy(code).to_string();
        if let Some(v) = azureus_version(&b[3..]) {
            return format!("Unknown({code_str} {v})");
        }
        return format!("Unknown({code_str})");
    }
    // Shadow's 风格：前 6 字节为大写 ASCII 客户端名
    if b.len() >= 6 && b[0].is_ascii_uppercase() {
        let name = String::from_utf8_lossy(&b[..6]);
        let name = name.trim_end();
        if !name.is_empty() {
            return name.to_string();
        }
    }
    String::new()
}

/// 判断地址是否不可路由（unspecified/broadcast），应从 peer 队列中过滤。
/// 注意：回环和私有地址在本地 BT 场景中可能有效，不全局过滤。
fn is_unroutable(addr: &SocketAddr) -> bool {
    let ip = addr.ip();
    match ip {
        std::net::IpAddr::V4(v4) => v4.is_unspecified() || v4.is_broadcast(),
        std::net::IpAddr::V6(v6) => v6.is_unspecified(),
    }
}

/// 判断地址是否为回环地址（PEX 通告中应过滤，避免远端 peer 误连本地）。
fn is_loopback(addr: &SocketAddr) -> bool {
    addr.ip().is_loopback()
}

#[cfg(test)]
mod tests {
    use super::*;
    use xfer_bencode::{bytes, encode, int, Value};

    /// 构造单文件 info 字典原始字节（piece 哈希用占位，结构合法即可）。
    fn make_info_bytes(data_len: u64) -> Vec<u8> {
        let piece_length = 16_384u64;
        let pieces_count = data_len.div_ceil(piece_length).max(1) as usize;
        let mut d = BTreeMap::new();
        d.insert(b"name".to_vec(), bytes(b"magnet-test.bin"));
        d.insert(b"piece length".to_vec(), int(piece_length as i64));
        d.insert(b"length".to_vec(), int(data_len as i64));
        d.insert(b"pieces".to_vec(), bytes(vec![0u8; pieces_count * 20]));
        encode(&Value::Dict(d))
    }

    #[test]
    fn metadata_accum_piece_count_and_assemble() {
        let mut acc = MetadataAccum::default();
        assert_eq!(acc.piece_count(), None);
        acc.expected_size = Some(METADATA_PIECE_SIZE * 2 + 10);
        assert_eq!(acc.piece_count(), Some(3));
        let p0 = vec![0u8; METADATA_PIECE_SIZE];
        let p1 = vec![1u8; METADATA_PIECE_SIZE];
        let p2 = vec![2u8; 10];
        acc.pieces.insert(0, p0.clone());
        assert!(acc.assemble().is_none()); // 未收全
        acc.pieces.insert(1, p1.clone());
        acc.pieces.insert(2, p2.clone());
        let out = acc.assemble().unwrap();
        assert_eq!(out.len(), METADATA_PIECE_SIZE * 2 + 10);
        assert_eq!(&out[..METADATA_PIECE_SIZE], &p0[..]);
        assert_eq!(&out[METADATA_PIECE_SIZE..METADATA_PIECE_SIZE * 2], &p1[..]);
    }

    #[test]
    fn extension_handshake_declares_ut_metadata() {
        let engine = TorrentEngine::new_magnet([7u8; 20], TorrentConfig::default()).unwrap();
        let hs = engine.build_extension_handshake();
        // 握手体（bencode）应包含 ut_metadata 声明
        assert!(
            hs.windows(11).any(|w| w == b"ut_metadata"),
            "扩展握手应声明 ut_metadata"
        );
        assert!(hs.windows(6).any(|w| w == b"ut_pex"));
    }

    /// 构造零值 PeerState（测试用）。
    fn empty_peer_state() -> PeerState {
        let now = Instant::now();
        PeerState {
            peer_id: None,
            have: PieceMap::new(0),
            pending_bitfield: None,
            pending_haves: Vec::new(),
            pending_have_all: false,
            choked: true,
            we_interested: false,
            last_activity: now,
            is_seed: false,
            encrypted: false,
            connected_at: now,
            recent_speed: 0,
            we_choked: true,
            peer_interested: false,
            fast_extension: false,
            extended_messaging: false,
            dht_enabled: false,
            ut_pex_id: 0,
            our_ut_pex_id: 0,
            ut_metadata_id: 0,
            our_ut_metadata_id: UT_METADATA_EXT_ID,
            allowed_fast_set: HashSet::new(),
            am_allowed_fast_set: HashSet::new(),
            last_data_transfer: now,
            choke_unchoke_count: 0,
            keepalive_count: 0,
            flooding_check_at: now,
            last_keepalive: now,
            last_unchoke: now,
            choking_required: true,
            opt_unchoking: false,
            client_version: None,
        }
    }

    /// peer_id 前缀解析应带版本号（BEP 20 Azureus 风格）。
    #[test]
    fn client_name_includes_version() {
        let mk = |prefix: &[u8]| {
            let mut id = [0u8; 20];
            id[..8].copy_from_slice(prefix);
            PeerId(id)
        };
        assert_eq!(
            client_name_from_peer_id(Some(mk(b"-qB4570-"))),
            "qBittorrent 4.5.7"
        );
        assert_eq!(
            client_name_from_peer_id(Some(mk(b"-TR0403-"))),
            "Transmission 4.0.3"
        );
        // 尾零省略：-UT2200- → uTorrent 2.2
        assert_eq!(client_name_from_peer_id(Some(mk(b"-UT2200-"))), "uTorrent 2.2");
        // 版本段非数字 → 仅名称
        assert_eq!(
            client_name_from_peer_id(Some(mk(b"-AZxxxx-"))),
            "Azureus"
        );
        // PeerInfo 优先显示 BEP 10 v 字段
        let st = PeerState {
            peer_id: Some(mk(b"-qB4570-")),
            client_version: Some("qBittorrent/4.6.0".into()),
            ..empty_peer_state()
        };
        assert_eq!(peer_client_desc(&st), "qBittorrent/4.6.0");
    }

    #[test]
    fn magnet_metadata_install_via_payload() {
        let dir = std::env::temp_dir().join(format!("xfer-magnet-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let data_len = 100_000u64;
        let info_bytes = make_info_bytes(data_len);
        let info_hash: [u8; 20] = {
            use sha1::{Digest, Sha1};
            let mut h = Sha1::new();
            h.update(&info_bytes);
            h.finalize().into()
        };
        let cfg = TorrentConfig {
            dir: dir.clone(),
            ..Default::default()
        };
        let engine = TorrentEngine::new_magnet(info_hash, cfg).unwrap();
        assert!(!engine.has_metadata());

        // 单分片 data 消息（info < 16KB）：bencode 头 + info 原始字节
        let mut head = BTreeMap::new();
        head.insert(b"msg_type".to_vec(), Value::Int(UT_METADATA_DATA));
        head.insert(b"piece".to_vec(), Value::Int(0));
        head.insert(b"total_size".to_vec(), Value::Int(info_bytes.len() as i64));
        let mut payload = encode(&Value::Dict(head));
        payload.extend_from_slice(&info_bytes);

        engine.handle_metadata_payload(&payload).unwrap();
        assert!(engine.has_metadata());
        assert_eq!(engine.progress().total, data_len);
        let meta = engine.meta().unwrap();
        assert_eq!(meta.info.name, "magnet-test.bin");
        assert_eq!(meta.info_hash, info_hash);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 构造多文件 info 字典原始字节（测试用）：name=multi-test，
    /// 每个文件 `<根>/f<i>.bin`，长度取 sizes。
    fn make_multi_file_info_bytes(sizes: &[u64]) -> Vec<u8> {
        let piece_length = 16_384u64;
        let total: u64 = sizes.iter().sum();
        let pieces_count = total.div_ceil(piece_length).max(1) as usize;
        let files: Vec<Value> = sizes
            .iter()
            .enumerate()
            .map(|(i, &len)| {
                let mut fd = BTreeMap::new();
                fd.insert(
                    b"path".to_vec(),
                    Value::List(vec![bytes(format!("f{i}.bin").as_bytes())]),
                );
                fd.insert(b"length".to_vec(), int(len as i64));
                Value::Dict(fd)
            })
            .collect();
        let mut d = BTreeMap::new();
        d.insert(b"name".to_vec(), bytes(b"multi-test"));
        d.insert(b"piece length".to_vec(), int(piece_length as i64));
        d.insert(b"files".to_vec(), Value::List(files));
        d.insert(b"pieces".to_vec(), bytes(vec![0u8; pieces_count * 20]));
        encode(&Value::Dict(d))
    }

    #[test]
    fn file_selection_limits_pieces_and_totals() {
        use xfer_bencode::parse_info_bytes;

        let dir = std::env::temp_dir().join(format!("xfer-sel-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // 3 文件 × 40_000，片长 16_384 → 8 片；片 2、5 为跨文件边界片
        let info = make_multi_file_info_bytes(&[40_000, 40_000, 40_000]);
        let meta = parse_info_bytes(&info).unwrap();
        assert_eq!(meta.info.piece_count(), 8);

        let cfg = TorrentConfig {
            dir: dir.clone(),
            selected_files: Some(vec![1]),
            ..Default::default()
        };
        let engine = TorrentEngine::new(meta, cfg).unwrap();
        // 总量收缩为所选文件，完成判定为假（所选片未下载）
        assert_eq!(engine.progress().total, 40_000);
        assert!(!engine.is_done());

        // 模拟一个全量 seed：assign_piece 只应分配覆盖文件 1 的片
        // 片布局：0-1→f0；2=f0+f1 边界；3→f1；4=f1+f2 边界；5-7→f2
        let cell = engine
            .register_peer("127.0.0.1:59001".parse().unwrap(), PeerSource::Dht);
        {
            let mut st = cell.state.lock().unwrap();
            st.have.set_all();
        }
        let mut got = std::collections::HashSet::new();
        for _ in 0..3 {
            let p = engine.assign_piece(&cell).expect("应能分配到所需片");
            assert!((2..=4).contains(&p), "只应分配所选文件的片，实际 {p}");
            assert!(got.insert(p), "片 {p} 不应重复分配");
        }
        // 3 个所需片全部分配完毕后无片可分
        assert!(engine.assign_piece(&cell).is_none());
        for p in &got {
            engine.release_piece(&cell, *p);
        }

        // 热切换：改为选文件 0+2 → 分配范围随之变化，总量回升
        engine.set_selected_files(Some(vec![0, 2]));
        assert_eq!(engine.progress().total, 80_000);
        // 可分配片 = 0,1,2,4,5,6,7（片 3 只属于文件 1，不分配）
        let mut got2 = std::collections::HashSet::new();
        for _ in 0..7 {
            let p = engine.assign_piece(&cell).expect("应能分配到所需片");
            assert_ne!(p, 3, "文件 1 独占片不应再分配");
            assert!(got2.insert(p));
        }
        assert!(engine.assign_piece(&cell).is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn magnet_rejects_wrong_info_hash() {
        let dir = std::env::temp_dir().join(format!("xfer-magnet-{}-x", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let info_bytes = make_info_bytes(1024);
        let engine = TorrentEngine::new_magnet([9u8; 20], TorrentConfig::default()).unwrap();
        let mut head = BTreeMap::new();
        head.insert(b"msg_type".to_vec(), Value::Int(UT_METADATA_DATA));
        head.insert(b"piece".to_vec(), Value::Int(0));
        head.insert(b"total_size".to_vec(), Value::Int(info_bytes.len() as i64));
        let mut payload = encode(&Value::Dict(head));
        payload.extend_from_slice(&info_bytes);
        assert!(engine.handle_metadata_payload(&payload).is_err());
        assert!(!engine.has_metadata());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 构造仅用于 `peer_progress` 测试的最小 PeerState。
    fn test_peer_state(have: PieceMap, is_seed: bool) -> PeerState {
        let now = Instant::now();
        PeerState {
            peer_id: None,
            have,
            pending_bitfield: None,
            pending_haves: Vec::new(),
            pending_have_all: false,
            choked: true,
            we_interested: false,
            last_activity: now,
            is_seed,
            encrypted: false,
            connected_at: now,
            recent_speed: 0,
            we_choked: true,
            peer_interested: false,
            fast_extension: false,
            extended_messaging: false,
            dht_enabled: false,
            ut_pex_id: 0,
            our_ut_pex_id: 0,
            ut_metadata_id: 0,
            our_ut_metadata_id: 0,
            allowed_fast_set: HashSet::new(),
            am_allowed_fast_set: HashSet::new(),
            last_data_transfer: now,
            choke_unchoke_count: 0,
            keepalive_count: 0,
            flooding_check_at: now,
            last_keepalive: now,
            last_unchoke: now,
            choking_required: false,
            opt_unchoking: false,
            client_version: None,
        }
    }

    #[test]
    fn peer_progress_semantics() {
        // seed → 100%
        let st = test_peer_state(PieceMap::new(8), true);
        assert_eq!(peer_progress(&st), Some(100.0));
        // 位图未知（0 片，磁力元数据未就绪）→ None
        let st = test_peer_state(PieceMap::new(0), false);
        assert_eq!(peer_progress(&st), None);
        // 部分拥有 → 百分比
        let mut have = PieceMap::new(4);
        have.set(0);
        have.set(1);
        let st = test_peer_state(have, false);
        assert_eq!(peer_progress(&st), Some(50.0));
        // 全部拥有（非 seed 标记）→ 100%
        let mut have = PieceMap::new(4);
        have.set_all();
        let st = test_peer_state(have, false);
        assert_eq!(peer_progress(&st), Some(100.0));
    }
    /// 运行时注入 announce URL：按 scheme 分流、与静态配置去重、
    /// 批内去重、空白行忽略、ws/wss 不支持（跳过）。
    #[test]
    fn add_announce_urls_classifies_and_dedups() {
        let dir = std::env::temp_dir().join(format!("xfer-ann-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let cfg = TorrentConfig {
            dir: dir.clone(),
            announce_urls: vec!["http://static.example/announce".into()],
            udp_announce_urls: vec!["udp://static.example:80/announce".into()],
            ..Default::default()
        };
        let engine = TorrentEngine::new_magnet([1u8; 20], cfg).unwrap();

        engine.add_announce_urls(&[
            "http://dyn.example/a".into(),
            "http://dyn.example/a".into(),            // 批内重复
            "http://static.example/announce".into(),  // 与静态重复
            "udp://dyn.example:6969/a".into(),
            "wss://dyn.example/ws".into(),            // 暂不支持 → 跳过
            "  ".into(),                              // 空白 → 跳过
        ]);

        let dyn_list = engine.dynamic_announces.lock().unwrap();
        assert_eq!(
            dyn_list.http,
            vec!["http://dyn.example/a".to_string()],
            "HTTP 动态列表：仅新增一条（批内/静态去重）"
        );
        assert_eq!(
            dyn_list.udp,
            vec!["udp://dyn.example:6969/a".to_string()],
            "UDP 动态列表：仅新增一条"
        );
        drop(dyn_list);

        let _ = std::fs::remove_dir_all(&dir);
    }
}

