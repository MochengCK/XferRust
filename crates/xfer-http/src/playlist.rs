//! HLS（M3U8）播放列表下载：清单解析 → 分片并发获取 → **严格顺序**拼接落盘。
//!
//! 设计要点：
//!
//! - **清单解析**：主清单（`#EXT-X-STREAM-INF`）自动选流；媒体清单读取
//!   `#EXTINF` 分片、`#EXT-X-MAP` 初始化段（fMP4 的 `init.mp4`）、
//!   `#EXT-X-BYTERANGE` 区间分片、`#EXT-X-KEY` AES-128 加密（支持密钥
//!   轮换与 `IV` 缺省时按媒体序号推导）。
//! - **顺序拼接**：分片在网络上并发获取，写盘严格按清单顺序串行追加，
//!   产物即最终文件，无需二次合并。`futures::buffered` 保证"最多 N 个
//!   在飞、按序产出"。
//! - **总长可知**：先对未知大小的分片做 `Range: bytes=0-0` 预探测，
//!   全部命中时给出精确总长（进度条/剩余时间可用）；服务器不支持
//!   Range 时立刻放弃探测，退化为"总长未知"。
//! - **断点续传**：控制文件只记录**已 fsync 的连续前缀**（段序号 +
//!   字节数）与清单指纹。清单变化（直播滑窗）指纹不匹配即重新开始，
//!   绝不把两个不同清单的字节拼在一起。
//!
//! 明确不支持：`SAMPLE-AES`（报错，不做静默降级）；直播清单
//! （无 `#EXT-X-ENDLIST`）按"当前窗口快照"一次性下载；`#EXT-X-MEDIA`
//! 的独立音轨组不会被打包进产物。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::rate::RateLimiter;
use crate::{apply_headers, HttpError, RequestHeaders};

/// 清单文本上限（防止把"看起来像清单"的大文件整体读进内存）。
const MANIFEST_MAX_BYTES: usize = 8 * 1024 * 1024;
/// 主清单 → 变体清单的最大跳转层数。
const MAX_VARIANT_HOPS: usize = 3;
/// 分片大小预探测的分片数上限：超过则跳过（观测单条清单没这么多分片）。
/// 预探测的**唯一**用途是给小清单一个精确总长：分片数不超过它才逐个探测。
/// 超过就完全不探测（756 个分片 = 756 个额外请求 + 几十秒到几分钟的"没反应"），
/// 改由 [`PlaylistStats::estimated_total`] 按已下字节实时外推。
const SIZE_PROBE_SAMPLE: usize = 32;
/// 预探测并发上限：探测是轻量请求，不需要跟着下载并发走。
const SIZE_PROBE_CONCURRENCY: usize = 8;
/// 控制文件落盘节流（与分片下载一致：1s 一次 fsync + 原子写）。
const CTRL_SAVE_INTERVAL: Duration = Duration::from_secs(1);
/// 单个分片的瞬时失败重试默认次数。
pub const DEFAULT_SEGMENT_RETRIES: u32 = 3;

/// "已下好但还没轮到写盘"的分片最多允许占用的字节数。
///
/// 分片必须按清单顺序拼成一个文件，所以慢分片会挡住后面分片的落盘。
/// 若把"下好待写"的分片也算进在飞名额（`futures::buffered` 的语义：
/// 完成的分片留在队列里、不再补新分片），队头一慢连接就集体空转 ——
/// CDN 抖动越大损失越大，实测是 HLS 吞吐的主要瓶颈之一。
///
/// 因此这里给"待写"单独设一个内存预算：预算没用完就继续补新分片下载，
/// 写盘只在连续前缀可用时推进。预算用完说明队头已经慢了整整一段距离，
/// 再往前跑只是囤内存。
const MAX_UNWRITTEN_BYTES: u64 = 32 * 1024 * 1024;

/// 分片读空闲超时：**连续这么久没读到任何数据**就断开重连（重试）。
///
/// 与 `split.rs` 的读空闲同级（那套是 10s / 尾声 3s），比客户端全局
/// `read_timeout`（30s）灵敏得多 —— 分片是有序落盘的，一条僵死连接会顶住
/// 后面所有已下好的分片，30s × 重试足够把"走走停停"拉成"停几分钟"。
const SEGMENT_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// 分片等响应头的上限（连上但不给响应，同样是假死）。
const SEGMENT_HEADER_TIMEOUT: Duration = Duration::from_secs(15);

/// `#EXT-X-KEY` 描述的分片密钥（AES-128 整段加密）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentKey {
    /// 密钥地址（已相对清单 URL 解析为绝对地址）。
    pub url: String,
    /// 初始向量（`IV` 缺省时按媒体序号大端 16 字节推导）。
    pub iv: [u8; 16],
}

/// 一个待下载的分片（含 fMP4 初始化段）。
#[derive(Debug, Clone)]
pub struct Segment {
    /// 绝对地址。
    pub url: String,
    /// `#EXT-X-BYTERANGE` 指定的区间 `(offset, len)`；None = 整段。
    pub range: Option<(u64, u64)>,
    /// 加密密钥；None = 明文。
    pub key: Option<SegmentKey>,
    /// 已知字节数（BYTERANGE 直接给出，其余由预探测补齐）。
    pub size: Option<u64>,
    /// `#EXTINF` 声明的时长（秒）。初始化段为 0。
    ///
    /// **用途是把"已下多少字节"换算成"大概下了多少比例"**：大清单绝不
    /// 为了精确总长去逐个探测（756 个分片要打 756 个请求，用户看到的就是
    /// "半天没反应"），改成用「已下字节 ÷ 已覆盖时长」实时外推总长。
    pub duration: f64,
}

/// 已解析的下载计划（引擎据此决定文件名、总长与分片位图粒度）。
#[derive(Debug, Clone)]
pub struct PlaylistPlan {
    /// 最终清单地址（重定向后）。
    pub source: String,
    /// 主清单 → 变体的选流链路（诊断用）。
    pub chain: Vec<String>,
    /// fMP4 初始化段（`#EXT-X-MAP`），必须排在所有分片之前。
    pub init: Option<Segment>,
    /// 媒体分片（按播放顺序）。
    pub segments: Vec<Segment>,
    /// 清单是否没有 `#EXT-X-ENDLIST`（直播/滚动窗口）。
    pub live: bool,
    /// 是否为 fMP4（决定默认扩展名 `.mp4` / `.ts`）。
    pub fmp4: bool,
    /// 全部分片大小已知时的总字节数（精确）。大清单通常为 `None`，
    /// 由 [`PlaylistStats::estimated_total`] 提供实时估算。
    pub total: Option<u64>,
    /// 媒体时长合计（`#EXTINF` 之和，秒）。总长估算的分母。
    pub duration_secs: f64,
    /// 选中变体的声明码率（`#EXT-X-STREAM-INF:BANDWIDTH`，bit/s）。
    ///
    /// 单层清单（没有主清单）时为 `None`。有它就能在**下载开始之前**给出
    /// 一个像样的总长（`bitrate / 8 × duration_secs`）—— 否则进度条要等到
    /// 第一个分片落盘才有总长，而单连接被限速的站点上一个分片要几十秒。
    pub bitrate: Option<u64>,
}

impl PlaylistPlan {
    /// 待拼接的段总数（含初始化段）。
    pub fn segment_count(&self) -> usize {
        self.segments.len() + usize::from(self.init.is_some())
    }
}

/// 下载选项（由引擎层从任务/全局选项合成）。
#[derive(Debug, Clone)]
pub struct PlaylistOptions {
    /// 分片并发数（在飞请求上限）。
    pub concurrency: usize,
    /// 单个分片的瞬时失败重试次数（含首次共 `retries` 次尝试）。
    pub retries: u32,
    /// 是否做分片大小预探测（关闭则总长未知，进度为不确定态）。
    pub probe_sizes: bool,
    /// 主清单选流偏好：true = 取最低码率（默认取最高）。
    pub prefer_worst: bool,
    /// 全局限速器（所有连接共享）。
    pub limiter: Option<Arc<RateLimiter>>,
    /// 逐任务自定义请求头（`Referer` / `Cookie` / `User-Agent`）。
    ///
    /// 清单、密钥、每个分片都带同一组头——受保护地址缺任意一处必然 403。
    pub headers: RequestHeaders,
    /// **顺序落盘**：true = 边下边按清单顺序直接写产物文件（下载中途的产物
    /// 就已经是能播的完整前缀）；false（默认）= **乱序落盘**，每个分片先写
    /// 自己的段文件、连续前缀就位后再拼进产物。
    ///
    /// 为什么默认选乱序：顺序落盘时"下好但还没轮到写"的分片必须留在内存里，
    /// 于是有两笔硬开销 —— ① 内存预算（[`MAX_UNWRITTEN_BYTES`]）用满后
    /// **不再派发新分片**，队头慢的时候其余连接成片空转（实测某视频站单连接
    /// ~65KB/s，一个 1.45MB 的分片要 20 秒，那 20 秒里总吞吐会从 3MB/s 掉到
    /// 单路）；② 进度只能吸附"光标那一段"的速度，总速度与进度显示对不上
    /// （用户报"速度几 MB、文件却几 KB 几 KB 地涨"）。乱序落盘把每个分片
    /// 直接写进磁盘（不占内存），进度就是磁盘上的真实字节数，与速度一致。
    pub ordered_write: bool,
}

impl Default for PlaylistOptions {
    fn default() -> Self {
        Self {
            concurrency: 16,
            retries: DEFAULT_SEGMENT_RETRIES,
            probe_sizes: true,
            prefer_worst: false,
            limiter: None,
            headers: Vec::new(),
            // 默认乱序：见字段说明（内存/吞吐/进度三者都更好）
            ordered_write: false,
        }
    }
}

/// 引擎轮询的进度句柄（无锁原子 + 一组分片计数槽位）。
pub struct PlaylistStats {
    /// 已交给文件的字节数（含 `FileSink` 那 512KB 写回缓冲——取消/失败收尾
    /// 会 `flush()` 把它落盘，所以这些字节**不会被丢弃**），含续传基线。
    pub completed: AtomicU64,
    /// 已从网络读到的字节数（**含还没轮到写盘的在飞分片**），含续传基线。
    ///
    /// 只作诊断与**速度**来源（见 [`Self::progress`]），不参与进度：
    /// 这里面有一大截还在内存里，取消/暂停时整段丢弃。
    pub received: AtomicU64,
    /// 当前在飞分片数。
    pub connections: AtomicUsize,
    /// 下一个待写分片的绝对序号（`#EXT-X-MAP` 初始化段算 0）。
    pub cursor: AtomicUsize,
    /// `cursor` 那一分片的已收字节**是否计入进度**：由"取消时这截会不会被
    /// 段内续传保住"决定 —— 未加密 = 会（计入），`AES-128` 加密 = 不会。
    pub cursor_counted: AtomicBool,
    /// 总字节数的**实时估算**（0 = 还没法估）。
    ///
    /// 两个来源，按可用性递进：① 主清单的 `BANDWIDTH × 总时长`（下载一开始
    /// 就有值，误差 ±10% 量级）；② 实测「已下字节 ÷ 已覆盖时长 × 总时长」
    /// 外推（写完第一段后接管，越来越准）。`plan.total` 已知时（小清单探测
    /// 过）这里始终为 0，由调用方优先用精确值。
    pub estimated_total: AtomicU64,
    /// **乱序落盘**模式下，已经写进各段文件、但还没拼进产物的字节总数。
    ///
    /// 这些字节躺在磁盘上（取消也不会丢），进度直接算作已下载 —— 所以乱序
    /// 模式的进度**恰好等于磁盘上的真实字节**，与 `speed` 对得上。拼接时它们
    /// 只是"转移"（`spilled` 减该段长度、`completed` 加同样多），进度单调不减。
    pub spilled: AtomicU64,
    /// 是否乱序落盘（决定 [`Self::progress`] 的口径）。
    unordered: AtomicBool,
    /// 每个分片各自的已接收字节（按绝对序号，按需增长）。
    ///
    /// 顺序落盘时进度只取 [`Self::cursor`] 处那一个：见 [`Self::progress`]。
    slots: Mutex<Vec<Arc<AtomicU64>>>,
}

impl PlaylistStats {
    pub fn new(baseline: u64) -> Arc<Self> {
        Arc::new(Self {
            completed: AtomicU64::new(baseline),
            received: AtomicU64::new(baseline),
            connections: AtomicUsize::new(0),
            cursor: AtomicUsize::new(0),
            cursor_counted: AtomicBool::new(true),
            estimated_total: AtomicU64::new(0),
            spilled: AtomicU64::new(0),
            unordered: AtomicBool::new(false),
            slots: Mutex::new(Vec::new()),
        })
    }

    /// 切到**乱序落盘**口径（进度 = 产物已拼字节 + 各段文件字节）。
    pub fn mark_unordered(&self) {
        self.unordered.store(true, Ordering::Relaxed);
    }

    /// 某个分片的计数槽位（不存在就按需建，槽位只增不删）。
    pub fn slot(&self, index: usize) -> Arc<AtomicU64> {
        let mut slots = self.slots.lock().unwrap();
        if index >= slots.len() {
            slots.resize_with(index + 1, || Arc::new(AtomicU64::new(0)));
        }
        slots[index].clone()
    }

    /// 推进待写光标（写完第 `written` 段之后调用）。
    ///
    /// **必须在更新 `completed` 之前调用**：顺序反过来会有一个窗口，此刻
    /// `completed` 已含刚写完的整段、而光标还指着它（槽位里也是整段字节），
    /// 进度会凭空多出一段。
    pub fn set_cursor(&self, written: usize, counted: bool) {
        self.cursor_counted.store(counted, Ordering::Relaxed);
        self.cursor.store(written, Ordering::Relaxed);
    }

    /// 进度 = **已交给文件的字节 + 当前待写分片已经收到的字节**。
    ///
    /// 为什么是这两项：分片整段下完才按顺序追加，所以
    ///
    /// - **已落盘部分**（`completed`，含写回缓冲）取消时一定在文件里，算它没有风险；
    /// - **当前待写分片**（光标处）的已收字节取消时会被**段内续传**追加进文件
    ///   （未加密时），同样不会白算。没有它，进度就只能按"分片落盘"跳 —— 单连接
    ///   被限速的站点上，一个 1.5MB 的分片要 20 秒才下完，界面 20 秒才动一格，
    ///   看着就是"进度不动"，而磁盘/网络其实一直在走。
    ///
    /// 为什么**只**算光标那一个：排在它后面的分片虽然也下好了，但它们的字节
    /// 停在内存里、取消即丢弃（写盘严格按序，轮不到它们）。把这些算进去就是
    /// 曾经的"界面显示 40MB、一暂停回落成磁盘上的 1.4MB"。加密分片（`AES-128`）
    /// 不做段内续传，光标处也不计入，退化成"按分片跳"。
    pub fn progress(&self) -> u64 {
        let done = self
            .completed
            .load(Ordering::Relaxed)
            .saturating_add(self.spilled.load(Ordering::Relaxed));
        if self.unordered.load(Ordering::Relaxed) {
            // 乱序：段文件里的字节全在磁盘上（取消也不丢），一个不落地算数 ——
            // 进度因此与 speed 完全同步，"速度几 MB、进度几 KB"不会再有。
            return done;
        }
        if !self.cursor_counted.load(Ordering::Relaxed) {
            return done;
        }
        let cursor = self.cursor.load(Ordering::Relaxed);
        let slots = self.slots.lock().unwrap();
        match slots.get(cursor) {
            Some(n) => done.saturating_add(n.load(Ordering::Relaxed)),
            None => done,
        }
    }
}

/// 下载结果。
#[derive(Debug, Clone, Copy)]
pub struct PlaylistDone {
    /// 产物总字节数。
    pub bytes: u64,
    /// 已拼接的分片数（含初始化段）。
    pub segments: usize,
    /// 清单声明的分片总数。
    pub total_segments: usize,
}

// ---------------------------------------------------------------------------
// 清单解析
// ---------------------------------------------------------------------------

/// 主清单中的一个码率变体。
#[derive(Debug, Clone)]
struct Variant {
    url: String,
    bandwidth: u64,
    area: u64,
}

/// 解析 `KEY=VALUE,KEY=VALUE` 属性串（尊重引号内的逗号）。
fn split_attributes(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    for ch in s.chars() {
        match ch {
            '"' => {
                quoted = !quoted;
                cur.push(ch);
            }
            ',' if !quoted => parts.push(std::mem::take(&mut cur)),
            _ => cur.push(ch),
        }
    }
    if !cur.trim().is_empty() {
        parts.push(cur);
    }
    parts
}

/// 解析属性列表为 `(名称大写, 值)`。
fn parse_attributes(s: &str) -> Vec<(String, String)> {
    split_attributes(s)
        .into_iter()
        .filter_map(|part| {
            let (k, v) = part.split_once('=')?;
            let k = k.trim().to_ascii_uppercase();
            if k.is_empty() {
                return None;
            }
            let v = v.trim().trim_matches('"').to_string();
            Some((k, v))
        })
        .collect()
}

fn attr<'a>(attrs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    attrs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// 去掉 UTF-8 BOM 与首尾空白。
fn clean_text(s: &str) -> &str {
    s.trim_start_matches('\u{feff}').trim()
}

/// 是否为 HLS 清单（首行必须是 `#EXTM3U`）。
fn looks_like_manifest(text: &str) -> bool {
    clean_text(text).starts_with("#EXTM3U")
}

/// 是否为主清单（含码率变体声明）。
fn is_master(text: &str) -> bool {
    clean_text(text)
        .lines()
        .any(|l| l.trim_start().starts_with("#EXT-X-STREAM-INF"))
}

/// 解析主清单中的码率变体（不含独立音轨组）。
fn parse_variants(text: &str, base: &Url) -> Vec<Variant> {
    let text = clean_text(text);
    let mut out = Vec::new();
    let mut pending: Option<(u64, u64)> = None;
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("#EXT-X-STREAM-INF:") {
            let attrs = parse_attributes(rest);
            let bandwidth = attr(&attrs, "BANDWIDTH")
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(0);
            let area = attr(&attrs, "RESOLUTION")
                .and_then(|v| {
                    let (w, h) = v.split_once('x')?;
                    Some(w.trim().parse::<u64>().ok()? * h.trim().parse::<u64>().ok()?)
                })
                .unwrap_or(0);
            pending = Some((bandwidth, area));
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        if let Some((bandwidth, area)) = pending.take() {
            if let Some(url) = resolve_url(base, line) {
                out.push(Variant {
                    url,
                    bandwidth,
                    area,
                });
            }
        }
    }
    out
}

/// 选流：默认取码率最高（同码率取分辨率最大），`worst` 时取最低。
fn choose_variant<'a>(variants: &'a [Variant], prefer_worst: bool) -> Option<&'a Variant> {
    variants.iter().max_by_key(|v| {
        let key = (v.bandwidth, v.area);
        if prefer_worst {
            (u64::MAX - key.0, u64::MAX - key.1)
        } else {
            key
        }
    })
}

/// 相对地址解析（失败时原样返回 None，交由调用方报错）。
fn resolve_url(base: &Url, uri: &str) -> Option<String> {
    let uri = uri.trim();
    if uri.is_empty() {
        return None;
    }
    match base.join(uri) {
        Ok(u) => Some(u.to_string()),
        Err(_) => uri.starts_with("http").then(|| uri.to_string()),
    }
}

/// 媒体清单解析结果。
#[derive(Debug, Default, Clone)]
struct MediaPlaylist {
    segments: Vec<Segment>,
    init: Option<Segment>,
    live: bool,
}

/// `#EXT-X-BYTERANGE:<len>[@<offset>]`。
fn parse_byterange(v: &str) -> Option<(u64, Option<u64>)> {
    let v = v.trim();
    match v.split_once('@') {
        Some((len, off)) => Some((len.trim().parse().ok()?, Some(off.trim().parse().ok()?))),
        None => Some((v.parse().ok()?, None)),
    }
}

/// 十六进制 IV → 16 字节（右侧对齐，缺位左补 0）。
fn parse_iv(v: &str) -> Option<[u8; 16]> {
    let hex = v.trim().trim_start_matches("0x").trim_start_matches("0X");
    if hex.is_empty() || hex.len() > 32 || hex.len() % 2 != 0 {
        return None;
    }
    let mut out = [0u8; 16];
    let bytes = hex.as_bytes();
    for i in 0..hex.len() / 2 {
        let hi = (bytes[i * 2] as char).to_digit(16)?;
        let lo = (bytes[i * 2 + 1] as char).to_digit(16)?;
        let idx = 16 - hex.len() / 2 + i;
        out[idx] = (hi * 16 + lo) as u8;
    }
    Some(out)
}

/// 媒体序号 → 缺省 IV（大端 16 字节）。
fn iv_from_sequence(seq: u64) -> [u8; 16] {
    let mut iv = [0u8; 16];
    iv[8..].copy_from_slice(&seq.to_be_bytes());
    iv
}

/// 解析中的密钥（`IV` 可能缺省，需按分片序号在遇到分片时确定）。
#[derive(Debug, Clone)]
struct PendingKey {
    url: String,
    iv: Option<[u8; 16]>,
}

/// 解析媒体清单。
fn parse_media(text: &str, base: &Url) -> Result<MediaPlaylist, String> {
    // BOM 必须先去：带 BOM 的首行 `\u{feff}#EXTM3U` 不以 '#' 开头，
    // 会被当成一个分片地址混进清单。
    let text = clean_text(text);
    let mut out = MediaPlaylist::default();
    let mut key: Option<PendingKey> = None;
    let mut sequence: u64 = 0;
    let mut next_range: Option<(u64, u64)> = None;
    // 最近一条 `#EXTINF` 的时长，跟随下一个 URI 行（初始化段不吃它）
    let mut next_duration: f64 = 0.0;
    // BYTERANGE 省略 offset 时接续同地址的上一段末尾
    let mut last_range_end: Option<(String, u64)> = None;
    let mut endlist = false;

    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:") {
            sequence = rest.trim().parse().unwrap_or(0);
            continue;
        }
        if let Some(rest) = line.strip_prefix("#EXT-X-KEY:") {
            let attrs = parse_attributes(rest);
            let method = attr(&attrs, "METHOD").unwrap_or("NONE").trim().to_ascii_uppercase();
            match method.as_str() {
                "NONE" => key = None,
                "AES-128" => {
                    let uri = attr(&attrs, "URI")
                        .ok_or_else(|| "EXT-X-KEY(AES-128) 缺少 URI".to_string())?;
                    let url = resolve_url(base, uri).ok_or_else(|| "密钥地址无法解析".to_string())?;
                    // IV 缺省时**不能在解析 KEY 标签时定值**：规范要求按
                    // 该分片自己的媒体序号推导，而序号随分片推进。
                    let iv = match attr(&attrs, "IV") {
                        Some(v) => Some(parse_iv(v).ok_or_else(|| "EXT-X-KEY 的 IV 非法".to_string())?),
                        None => None,
                    };
                    key = Some(PendingKey { url, iv });
                }
                other => {
                    return Err(format!(
                        "暂不支持 {other} 加密的播放列表（仅支持 AES-128 与明文）"
                    ))
                }
            }
            continue;
        }
        if let Some(rest) = line.strip_prefix("#EXT-X-MAP:") {
            let attrs = parse_attributes(rest);
            let uri = attr(&attrs, "URI").ok_or_else(|| "EXT-X-MAP 缺少 URI".to_string())?;
            let url = resolve_url(base, uri).ok_or_else(|| "初始化段地址无法解析".to_string())?;
            let range = match attr(&attrs, "BYTERANGE") {
                Some(v) => {
                    let (len, off) = parse_byterange(v).ok_or_else(|| "EXT-X-MAP BYTERANGE 非法".to_string())?;
                    Some((off.unwrap_or(0), len))
                }
                None => None,
            };
            // 初始化段按规范不受 AES-128 影响（只有 SAMPLE-AES 才作用于它）
            out.init = Some(Segment {
                url,
                range,
                key: None,
                size: range.map(|(_, len)| len),
                duration: 0.0,
            });
            continue;
        }
        if let Some(rest) = line.strip_prefix("#EXTINF:") {
            // `#EXTINF:<时长>,<标题>` —— 时长取逗号前那段（解析不出就是 0，
            // 只会让总长估算更保守，不影响下载）
            let secs = rest.split(',').next().unwrap_or("").trim();
            next_duration = secs.parse::<f64>().unwrap_or(0.0).max(0.0);
            continue;
        }
        if let Some(rest) = line.strip_prefix("#EXT-X-BYTERANGE:") {
            let (len, off) = parse_byterange(rest)
                .ok_or_else(|| "EXT-X-BYTERANGE 非法".to_string())?;
            next_range = Some((len, off.unwrap_or(u64::MAX)));
            continue;
        }
        if line == "#EXT-X-ENDLIST" {
            endlist = true;
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        // URI 行 = 一个分片（非 URI 行之外的 #EXTINF 只是元数据）
        let Some(url) = resolve_url(base, line) else {
            return Err(format!("分片地址无法解析: {line}"));
        };
        let duration = std::mem::take(&mut next_duration);
        let range = next_range.take().map(|(len, off)| {
            let start = if off == u64::MAX {
                match &last_range_end {
                    Some((u, end)) if *u == url => *end,
                    _ => 0,
                }
            } else {
                off
            };
            last_range_end = Some((url.clone(), start + len));
            (start, len)
        });
        out.segments.push(Segment {
            url,
            range,
            key: key.as_ref().map(|k| SegmentKey {
                url: k.url.clone(),
                iv: k.iv.unwrap_or_else(|| iv_from_sequence(sequence)),
            }),
            size: range.map(|(_, len)| len),
            duration,
        });
        sequence += 1;
    }
    out.live = !endlist;
    Ok(out)
}

// ---------------------------------------------------------------------------
// 清单获取
// ---------------------------------------------------------------------------

/// 读取清单文本（限制长度，UTF-8 lossy 解码）。
async fn fetch_manifest(
    client: &reqwest::Client,
    url: &str,
    cancel: &CancellationToken,
    headers: &[(String, String)],
) -> Result<(String, String), HttpError> {
    let resp = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(HttpError::Cancelled),
        r = apply_headers(client.get(url), headers).send() => {
            r.map_err(|e| HttpError::from_reqwest(&e))?
        }
    };
    let status = resp.status();
    if !status.is_success() {
        return Err(HttpError::Http(status.as_u16()));
    }
    let final_url = resp.url().to_string();
    let mut buf: Vec<u8> = Vec::new();
    let mut stream = resp.bytes_stream();
    loop {
        let chunk = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(HttpError::Cancelled),
            c = stream.next() => match c {
                Some(Ok(c)) => c,
                Some(Err(e)) => return Err(HttpError::from_reqwest(&e)),
                None => break,
            },
        };
        buf.extend_from_slice(&chunk);
        if buf.len() > MANIFEST_MAX_BYTES {
            return Err(HttpError::Protocol("清单文件过大（>8MiB），已放弃".into()));
        }
    }
    Ok((String::from_utf8_lossy(&buf).to_string(), final_url))
}

/// 抓取并解析清单，得到可下载的计划（含选流与分片大小预探测）。
pub async fn fetch_plan(
    client: &reqwest::Client,
    url: &str,
    cancel: &CancellationToken,
    headers: &[(String, String)],
    opts: &PlaylistOptions,
) -> Result<PlaylistPlan, HttpError> {
    let mut cur = url.to_string();
    let mut chain: Vec<String> = Vec::new();
    // 选中变体的声明码率（主清单才有）：用于"下载一开始就有总长"
    let mut bitrate: Option<u64> = None;
    for _ in 0..MAX_VARIANT_HOPS {
        let (text, final_url) = fetch_manifest(client, &cur, cancel, headers).await?;
        if !looks_like_manifest(&text) {
            // 不是清单：调用方决定是回退普通 HTTP 下载还是报错
            return Err(HttpError::NotPlaylist);
        }
        let base = Url::parse(&final_url)
            .or_else(|_| Url::parse(&cur))
            .map_err(|e| HttpError::Protocol(format!("清单地址非法: {e}")))?;

        if is_master(&text) {
            let variants = parse_variants(&text, &base);
            if variants.is_empty() {
                return Err(HttpError::Protocol("主清单中没有可用的码率变体".into()));
            }
            let chosen = choose_variant(&variants, opts.prefer_worst)
                .expect("变体列表非空")
                .clone();
            tracing::debug!(variant = %chosen.url, bandwidth = chosen.bandwidth, "主清单选流");
            bitrate = Some(chosen.bandwidth).filter(|b| *b > 0);
            chain.push(cur.clone());
            cur = chosen.url;
            continue;
        }

        let media = parse_media(&text, &base).map_err(HttpError::Protocol)?;
        let duration_secs: f64 = media.segments.iter().map(|s| s.duration).sum();
        let mut plan = PlaylistPlan {
            source: final_url,
            chain,
            fmp4: media.init.is_some()
                || media
                    .segments
                    .iter()
                    .any(|s| is_mp4_like(&s.url)),
            init: media.init,
            segments: media.segments,
            live: media.live,
            total: None,
            duration_secs,
            bitrate,
        };
        if plan.segment_count() == 0 {
            return Err(HttpError::Protocol("播放列表中没有可下载的分片".into()));
        }
        if opts.probe_sizes {
            probe_segment_sizes(client, &mut plan, cancel, headers, opts.concurrency).await;
        } else {
            plan.total = sum_sizes(&plan);
        }
        return Ok(plan);
    }
    Err(HttpError::Protocol("播放列表层级过深（主清单嵌套超过 3 层）".into()))
}

fn is_mp4_like(url: &str) -> bool {
    let path = url.split(['?', '#']).next().unwrap_or(url).to_ascii_lowercase();
    path.ends_with(".mp4") || path.ends_with(".m4s") || path.ends_with(".m4v") || path.ends_with(".cmfv")
}

fn sum_sizes(plan: &PlaylistPlan) -> Option<u64> {
    let mut total = 0u64;
    if let Some(i) = &plan.init {
        total = total.checked_add(i.size?)?;
    }
    for s in &plan.segments {
        total = total.checked_add(s.size?)?;
    }
    Some(total)
}

/// 分片大小预探测：**只对小清单做**，命中全部才给出精确总长。
///
/// 大清单（一部电影动辄 700+ 个分片）**不探测**：每个分片一个
/// `Range: bytes=0-0` 请求，756 个分片就是 756 个额外请求（实测单次 ~600ms、
/// 8 并发约 10 次/秒 → 75 秒起步，服务器一被压就变成几分钟），而这期间
/// 一个字节都还没下、进度条纹丝不动 —— 用户看到的就是"卡住了"，紧接着
/// 下载因为刚被打过一轮而变慢、走走停停。大清单改用
/// [`PlaylistStats::estimated_total`]（已下字节 ÷ 已覆盖时长外推），零额外请求。
///
/// 服务器对 `Range` 不配合（返回 200）时立即停止探测——继续探测等于
/// 把每个分片整段多下一次，代价远大于"总长未知"。
async fn probe_segment_sizes(
    client: &reqwest::Client,
    plan: &mut PlaylistPlan,
    cancel: &CancellationToken,
    headers: &[(String, String)],
    concurrency: usize,
) {
    // 初始化段通常很小，单独探测一次就够（它必须进产物，值得确知大小）
    if plan.init.as_ref().map(|i| i.size.is_none()).unwrap_or(false) {
        if let Some(i) = plan.init.as_mut() {
            if let Ok(p) = crate::probe_with(client, &i.url, cancel, headers).await {
                if !p.accepts_ranges {
                    plan.total = sum_sizes(plan);
                    return;
                }
                i.size = p.total_len;
            }
        }
    }
    let unknown: Vec<usize> = plan
        .segments
        .iter()
        .enumerate()
        .filter(|(_, s)| s.size.is_none())
        .map(|(i, _)| i)
        .collect();
    if unknown.is_empty() || unknown.len() > SIZE_PROBE_SAMPLE {
        plan.total = sum_sizes(plan);
        return;
    }
    let conn = concurrency.clamp(1, SIZE_PROBE_CONCURRENCY);
    let urls: Vec<String> = unknown.iter().map(|i| plan.segments[*i].url.clone()).collect();
    let mut results: Vec<Option<u64>> = Vec::with_capacity(urls.len());
    let mut range_unsupported = false;
    {
        let mut stream = futures_util::stream::iter(urls.into_iter())
            .map(|url| {
                let url = url.clone();
                async move {
                    match crate::probe_with(client, &url, cancel, headers).await {
                        Ok(p) if !p.accepts_ranges => {
                            // 整段回 200：后续探测都会变成"多下一次整片"
                            (None, true)
                        }
                        Ok(p) => (p.total_len.filter(|n| *n > 0), false),
                        Err(_) => (None, false),
                    }
                }
            })
            .buffered(conn);
        while let Some((size, bad)) = stream.next().await {
            if bad {
                range_unsupported = true;
                break;
            }
            results.push(size);
        }
    }
    if range_unsupported {
        plan.total = sum_sizes(plan);
        return;
    }
    for (slot, size) in unknown.iter().zip(results.iter()) {
        plan.segments[*slot].size = *size;
    }
    plan.total = sum_sizes(plan);
}

// ---------------------------------------------------------------------------
// 控制文件
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct PlaylistCtrl {
    /// 标记文件种类，避免误读分片下载（split）的控制文件
    kind: String,
    v: u32,
    /// 清单指纹（分片地址集合）
    fp: String,
    /// 最终清单地址
    url: String,
    /// 段总数
    segs: usize,
    /// 已持久化的连续前缀（段数，含初始化段）
    prefix: usize,
    /// 「完整前缀」部分的字节数（文件里 `0..bytes` 这一段由完整的段拼成）
    bytes: u64,
    /// **段内续传**：第 `prefix` 段（下一个待写的那段）已经有这么多字节落在
    /// `bytes` 之后。0 = 该段一个字节都没写。
    ///
    /// 有它才能"暂停不丢在飞分片"：暂停时把该段已收到的部分直接追加进文件，
    /// 恢复时对这一段发 `Range: bytes=part-` 接着下，而不是整段重来。
    /// 老的 v1 控制文件没有这个字段 → 缺省 0，语义与从前一致（只认完整前缀）。
    #[serde(default)]
    part: u64,
    /// 落盘方式：`"unordered"` = 乱序落盘（各段先落自己的段文件、再按序拼接）。
    /// 空字符串 = 顺序落盘（老控制文件没有这个字段，语义就是顺序）。
    ///
    /// 两种模式的产物结构、续传方式都不一样，所以**互相不认**：模式不匹配
    /// 一律丢开重来，绝不把两种模式的中间状态拼在一起。
    #[serde(default)]
    mode: String,
    /// 乱序落盘：哪些分片已经**完整**（hex 位图，最高位对应第 0 段）。
    ///
    /// 段文件长度只能说明"下到哪了"，说明不了"下完了没有"（大清单不预探测、
    /// 段大小未知），所以"完整"必须记在这里。
    #[serde(default)]
    mask: String,
}

/// 乱序落盘的模式标记（控制文件里的 `mode` 字段）。
const CTRL_MODE_UNORDERED: &str = "unordered";

fn is_unordered_ctrl(c: &PlaylistCtrl) -> bool {
    c.mode == CTRL_MODE_UNORDERED
}

/// 段文件目录：与产物同级、以产物名加后缀（一眼能看出是谁的）。
fn seg_dir(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "out".to_string());
    path.with_file_name(format!("{name}.hlseg"))
}

/// 第 `i` 段的段文件路径（文件名用 6 位数字，便于按序浏览）。
fn seg_file(path: &Path, i: usize) -> PathBuf {
    seg_dir(path).join(format!("{i:06}"))
}

/// 把段文件追加进产物，然后删掉它。返回搬运的字节数。
fn splice_seg(seg: &Path, sink: &mut xfer_storage::FileSink) -> Result<u64, HttpError> {
    use std::io::Read;
    let mut f = std::fs::File::open(seg).map_err(|e| HttpError::Io(e.to_string()))?;
    let mut buf = vec![0u8; 256 * 1024];
    let mut moved = 0u64;
    loop {
        let n = f.read(&mut buf).map_err(|e| HttpError::Io(e.to_string()))?;
        if n == 0 {
            break;
        }
        sink.write(&buf[..n]).map_err(|e| HttpError::Io(e.to_string()))?;
        moved += n as u64;
    }
    let _ = std::fs::remove_file(seg);
    Ok(moved)
}

/// `Vec<bool>` → hex 位图（最高位对应第 0 段）。
fn mask_to_hex(mask: &[bool]) -> String {
    let mut out = String::with_capacity(mask.len().div_ceil(8) * 2);
    let mut byte = 0u8;
    for (i, d) in mask.iter().enumerate() {
        if *d {
            byte |= 1 << (7 - (i % 8));
        }
        if i % 8 == 7 {
            out.push_str(&format!("{byte:02x}"));
            byte = 0;
        }
    }
    if mask.len() % 8 != 0 {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// hex 位图 → `Vec<bool>`（长度不足补 false，超长忽略）。
fn mask_from_hex(hex: &str, len: usize) -> Vec<bool> {
    let bytes: Vec<u8> = (0..hex.len() / 2)
        .filter_map(|i| u8::from_str_radix(hex.get(i * 2..i * 2 + 2)?, 16).ok())
        .collect();
    (0..len)
        .map(|i| bytes.get(i / 8).is_some_and(|b| (b >> (7 - (i % 8))) & 1 == 1))
        .collect()
}

const CTRL_KIND: &str = "hls-playlist";

/// 清单指纹：分片地址 + 区间 + 密钥地址。
fn fingerprint(plan: &PlaylistPlan) -> String {
    let mut h = Sha256::new();
    if let Some(i) = &plan.init {
        h.update(i.url.as_bytes());
        if let Some((o, l)) = i.range {
            h.update(format!("{o}:{l}").as_bytes());
        }
    }
    for s in &plan.segments {
        h.update(s.url.as_bytes());
        if let Some((o, l)) = s.range {
            h.update(format!("{o}:{l}").as_bytes());
        }
        if let Some(k) = &s.key {
            h.update(k.url.as_bytes());
        }
    }
    hex::encode(&h.finalize()[..8])
}

fn load_ctrl(path: &Path, plan: &PlaylistPlan) -> Option<PlaylistCtrl> {
    let raw = std::fs::read_to_string(path).ok()?;
    let c: PlaylistCtrl = serde_json::from_str(&raw).ok()?;
    if c.kind != CTRL_KIND || c.v != 1 {
        return None;
    }
    if c.fp != fingerprint(plan) || c.url != plan.source || c.segs != plan.segment_count() {
        return None;
    }
    Some(c)
}

fn save_ctrl(path: &Path, c: &PlaylistCtrl) {
    let Some(parent) = path.parent() else { return };
    if std::fs::create_dir_all(parent).is_err() {
        return;
    }
    let Ok(json) = serde_json::to_vec(c) else { return };
    let tmp: PathBuf = {
        let mut s = path.as_os_str().to_os_string();
        s.push(".tmp");
        PathBuf::from(s)
    };
    if std::fs::write(&tmp, &json).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    }
}

/// 续传现场：文件里已经持久化的内容。
///
/// 关键不变式：**文件 = 「完整前缀」+ 「第 prefix 段的开头 part 字节」**，
/// 所以文件长度恒等于 `prefix_bytes + part`；要丢弃尾巴（例如服务器不支持
/// `Range`、续不了段内）时截断到 `prefix_bytes` 即可。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResumeState {
    /// 「完整前缀」部分的字节数（文件里 `0..prefix_bytes` 是完整段拼出来的）
    pub prefix_bytes: u64,
    /// 已完成的段数（含初始化段）
    pub prefix: usize,
    /// 第 `prefix` 段已经落在文件里的字节数（0 = 该段从未写过）
    pub part: u64,
}

impl ResumeState {
    pub(crate) const NONE: ResumeState = ResumeState {
        prefix_bytes: 0,
        prefix: 0,
        part: 0,
    };

    /// 文件当前应有的长度（= 完整前缀 + 段内已落部分）。
    pub(crate) fn file_bytes(&self) -> u64 {
        self.prefix_bytes.saturating_add(self.part)
    }
}

/// 读控制文件并核对磁盘，得到可安全续传的现场。
///
/// 三种退化：文件长度够 `prefix_bytes + part` → 原样续传；只够 `prefix_bytes`
/// （段内那截被截掉了/没落盘）→ 退化成整段重下；连前缀都不够 → 从头来。
fn resume_state(path: &Path, plan: &PlaylistPlan) -> ResumeState {
    let ctrl = xfer_storage::ctrl_path(path);
    let Some(c) = load_ctrl(&ctrl, plan) else {
        return ResumeState::NONE;
    };
    // 乱序落盘的控制文件不属于这条路径（产物结构完全不同），别拿来续
    if is_unordered_ctrl(&c) {
        return ResumeState::NONE;
    }
    if c.prefix > plan.segment_count() {
        return ResumeState::NONE;
    }
    let Ok(m) = std::fs::metadata(path) else {
        return ResumeState::NONE;
    };
    if m.len() >= c.bytes.saturating_add(c.part) {
        return ResumeState {
            prefix_bytes: c.bytes,
            prefix: c.prefix,
            part: c.part,
        };
    }
    if m.len() >= c.bytes {
        return ResumeState {
            prefix_bytes: c.bytes,
            prefix: c.prefix,
            part: 0,
        };
    }
    ResumeState::NONE
}

/// 续传水位：`(已持久化字节数, 已持久化段数)`。
///
/// 引擎在启动下载前用它回填进度与分片位图基线；`download_playlist`
/// 内部再算一次（纯函数，结果一致）。字节数含段内已落的那一截
/// （即磁盘上的真实长度）。
pub fn resume_point(path: &Path, plan: &PlaylistPlan) -> (u64, usize) {
    let ctrl = xfer_storage::ctrl_path(path);
    if let Some(c) = load_ctrl(&ctrl, plan) {
        if is_unordered_ctrl(&c) {
            // 乱序落盘：已下载字节 = 产物里已拼好的 + 各段文件里还没拼的。
            // 引擎拿它当进度基线（从这儿接着显示，不从头开始数）。
            let mut total = c.bytes;
            for i in 0..plan.segment_count() {
                if let Ok(m) = std::fs::metadata(seg_file(path, i)) {
                    total = total.saturating_add(m.len());
                }
            }
            return (total, c.prefix.min(plan.segment_count()));
        }
    }
    let st = resume_state(path, plan);
    (st.file_bytes(), st.prefix)
}

// ---------------------------------------------------------------------------
// 下载
// ---------------------------------------------------------------------------

struct Ctx<'a> {
    client: &'a reqwest::Client,
    cancel: &'a CancellationToken,
    limiter: Option<&'a RateLimiter>,
    /// 逐任务请求头（借用）。
    headers: &'a [(String, String)],
    retries: u32,
    stats: &'a PlaylistStats,
    keys: Mutex<HashMap<String, Arc<Vec<u8>>>>,
}

impl Ctx<'_> {
    fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
}

/// 取密钥（同一地址只下一次）。
async fn fetch_key(ctx: &Ctx<'_>, url: &str) -> Result<Arc<Vec<u8>>, HttpError> {
    if let Some(k) = ctx.keys.lock().unwrap().get(url).cloned() {
        return Ok(k);
    }
    let resp = tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => return Err(HttpError::Cancelled),
        r = apply_headers(ctx.client.get(url), ctx.headers).send() => {
            r.map_err(|e| HttpError::from_reqwest(&e))?
        }
    };
    if !resp.status().is_success() {
        return Err(HttpError::Http(resp.status().as_u16()));
    }
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| HttpError::from_reqwest(&e))?;
    if bytes.is_empty() {
        return Err(HttpError::Protocol("密钥内容为空".into()));
    }
    let arc = Arc::new(bytes.to_vec());
    ctx.keys.lock().unwrap().insert(url.to_string(), arc.clone());
    Ok(arc)
}

/// AES-128-CBC + PKCS7 解密。
fn decrypt_segment(key: &[u8], iv: &[u8; 16], data: &[u8]) -> Result<Vec<u8>, HttpError> {
    use aes::cipher::{BlockDecryptMut, KeyIvInit};
    type Dec = cbc::Decryptor<aes::Aes128>;
    if key.len() != 16 {
        return Err(HttpError::Protocol(format!(
            "AES-128 密钥长度应为 16 字节，实际 {}",
            key.len()
        )));
    }
    if data.is_empty() {
        return Ok(Vec::new());
    }
    if data.len() % 16 != 0 {
        return Err(HttpError::Protocol(format!(
            "加密分片长度 {} 不是 16 的整数倍",
            data.len()
        )));
    }
    let mut buf = data.to_vec();
    let dec = Dec::new(key.into(), iv.into());
    let plain = dec
        .decrypt_padded_mut::<aes::cipher::block_padding::Pkcs7>(&mut buf)
        .map_err(|e| HttpError::Protocol(format!("AES-128 解密失败: {e}")))?;
    Ok(plain.to_vec())
}

/// 在飞分片计数守卫：分片 future 被丢弃（取消/失败提前收尾）时也要
/// 把计数降回去，否则 `connections` 只增不减。
struct ConnGuard<'a>(&'a AtomicUsize);

impl ConnGuard<'_> {
    fn new(c: &AtomicUsize) -> ConnGuard<'_> {
        c.fetch_add(1, Ordering::Relaxed);
        ConnGuard(c)
    }
}

impl Drop for ConnGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// 一个分片取回来的数据。
struct SegBody {
    /// 段内续传之后再取到的**剩余部分**（从 `Range` 起点开始）。
    ///
    /// 只有 [`SegTarget::Mem`] 才会填这里；[`SegTarget::Disk`] 的数据已经
    /// 直接写进段文件了（不经过内存）。
    data: Vec<u8>,
    /// 服务器无视我们发的 `Range`、回了整段（数据从**段首**开始）。
    ///
    /// 这时已有的"段内续传那截"必须丢掉，否则会把同一段开头写两遍。
    /// `Disk` 目标下由写入器自己截断，这里的值只作诊断。
    whole: bool,
    /// `Disk` 目标：这一趟写进段文件的字节数（进度统计用）。
    on_disk: u64,
}

/// 分片数据的落点。
enum SegTarget {
    /// 收进内存（**顺序落盘**模式；加密段也走这里 —— 解密要整段）。
    Mem(Arc<Mutex<Vec<u8>>>),
    /// **边收边追加写进段文件**（乱序落盘的明文段）。
    ///
    /// 不占内存、取消时半截直接留在文件里（恢复按文件长度发 `Range` 接着下），
    /// 而且落盘字节立刻计入进度 —— 这是"进度与速度对得上"的关键。
    Disk(Arc<Mutex<SegFile>>),
}

/// 段文件的写入器：直接写（无写回缓冲），所以 `written` 恒等于磁盘长度。
struct SegFile {
    file: std::fs::File,
    written: u64,
}

impl SegFile {
    /// 打开段文件。`truncate=true` 时清空重写（首下 / 服务器无视 Range），
    /// 否则沿用已有长度（走 `Range` 接着下）。
    fn open(path: &Path, truncate: bool) -> Result<Self, HttpError> {
        use std::io::Seek;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| HttpError::Io(e.to_string()))?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(truncate)
            .open(path)
            .map_err(|e| HttpError::Io(e.to_string()))?;
        let written = if truncate {
            0
        } else {
            // **必须把写位置挪到文件末尾**：`File` 默认从 0 开始写，续传时
            // 会从段首覆盖旧数据（文件长度不变、而 `written` 照累加），
            // 产物于是缺一截
            let len = file.metadata().map(|m| m.len()).unwrap_or(0);
            file.seek(std::io::SeekFrom::End(0))
                .map_err(|e| HttpError::Io(e.to_string()))?;
            len
        };
        Ok(Self { file, written })
    }

    fn append(&mut self, data: &[u8]) -> Result<(), HttpError> {
        use std::io::Write;
        self.file
            .write_all(data)
            .map_err(|e| HttpError::Io(e.to_string()))?;
        // 先落地再记账：进度绝不能领先磁盘（取消时这些字节必须在文件里）
        self.written += data.len() as u64;
        Ok(())
    }

    /// 服务器无视 `Range` 回了整段：丢掉已有内容从头写。返回被丢弃的字节数
    /// （调用方要从 `spilled` 里扣掉）。
    fn reset(&mut self) -> Result<u64, HttpError> {
        let old = self.written;
        self.file
            .set_len(0)
            .map_err(|e| HttpError::Io(e.to_string()))?;
        self.written = 0;
        Ok(old)
    }

    fn finish(&mut self) -> Result<u64, HttpError> {
        use std::io::Write;
        self.file
            .flush()
            .map_err(|e| HttpError::Io(e.to_string()))?;
        let _ = self.file.sync_data();
        Ok(self.written)
    }
}

/// 分片字节计数守卫：每读到一个 chunk 就计入几个计数器；**没走到"成功拿到
/// 整段"就 Drop**（失败重试、取消、future 被直接丢弃）时把这一趟的字节扣回去。
///
/// 计数器分工：
/// - `total`（`stats.received`）：网络已收到多少 —— **速度**的唯一来源；
/// - `disk`（`stats.spilled`）：落进段文件的字节 —— **乱序模式的进度**来源；
/// - `slot`（该分片的槽位）：顺序模式下只有"光标那一段"计入进度。
///
/// 只在 `Err` 分支扣数的写法漏掉了 future 被 drop 的路径 —— 暂停、任务提前
/// 收尾、`futures` 队列被丢弃时走不到那段清理代码，计数会永久虚高。
struct SegCount<'a> {
    total: &'a AtomicU64,
    disk: Option<&'a AtomicU64>,
    slot: Option<&'a AtomicU64>,
    counted: u64,
    keep: bool,
    /// 取消/失败时是否把这一趟的计数扣回去。
    ///
    /// 内存目标要扣（数据还在内存里、取消即丢，留着就是虚报）；**段文件目标
    /// 不能扣** —— 那些字节已经写进磁盘（取消也不会丢），它们本来就是"已下
    /// 载"的一部分。这一条正是"乱序模式下进度等于磁盘真实字节"的前提。
    rollback: bool,
}

impl<'a> SegCount<'a> {
    fn new(
        total: &'a AtomicU64,
        disk: Option<&'a AtomicU64>,
        slot: Option<&'a AtomicU64>,
        rollback: bool,
    ) -> Self {
        Self {
            total,
            disk,
            slot,
            counted: 0,
            keep: false,
            rollback,
        }
    }

    fn add(&mut self, n: u64) {
        self.counted += n;
        self.total.fetch_add(n, Ordering::Relaxed);
        if let Some(d) = self.disk {
            d.fetch_add(n, Ordering::Relaxed);
        }
        if let Some(s) = self.slot {
            s.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// 整段到手，计数保留。
    fn keep(&mut self) {
        self.keep = true;
    }
}

impl Drop for SegCount<'_> {
    fn drop(&mut self) {
        if !self.keep && self.rollback && self.counted > 0 {
            self.total.fetch_sub(self.counted, Ordering::Relaxed);
            if let Some(d) = self.disk {
                d.fetch_sub(self.counted, Ordering::Relaxed);
            }
            if let Some(s) = self.slot {
                s.fetch_sub(self.counted, Ordering::Relaxed);
            }
        }
    }
}

/// 单次分片请求（区间请求 + 流式读取 + 限速 + 长度校验）。
///
/// 两个**空闲**超时（不是总时长超时，大分片正常下多久都行）：等响应头
/// [`SEGMENT_HEADER_TIMEOUT`]、读到两段数据之间 [`SEGMENT_IDLE_TIMEOUT`]。
/// 服务器"接受连接后不吭声"是线上最常见的假死形态，靠客户端全局的
/// read_timeout（30s）兜底太钝；这里超时即断开重连（错误可重试），恢复快得多。
async fn fetch_body(
    ctx: &Ctx<'_>,
    seg: &Segment,
    target: &SegTarget,
    resume_from: u64,
    slot: Option<&AtomicU64>,
) -> Result<SegBody, HttpError> {
    let mut count = match target {
        // 内存目标：取消/失败要把这一趟的计数扣回去（数据会丢）
        SegTarget::Mem(_) => SegCount::new(&ctx.stats.received, None, slot, true),
        // 乱序：字节直接落段文件，按**磁盘**记账（进度即磁盘真实字节）；
        // 而且**不回滚** —— 那些字节确实在磁盘上
        SegTarget::Disk(_) => {
            SegCount::new(&ctx.stats.received, Some(&ctx.stats.spilled), None, false)
        }
    };
    let r = fetch_body_inner(ctx, seg, target, resume_from, &mut count).await;
    if r.is_ok() {
        count.keep();
    }
    r
}

async fn fetch_body_inner(
    ctx: &Ctx<'_>,
    seg: &Segment,
    target: &SegTarget,
    resume_from: u64,
    count: &mut SegCount<'_>,
) -> Result<SegBody, HttpError> {
    // 区间起点：清单里的 `#EXT-X-BYTERANGE` 偏移 + 段内续传偏移
    let base_off = seg.range.map(|(o, _)| o).unwrap_or(0);
    let end = seg.range.map(|(o, l)| o + l.saturating_sub(1));
    let start = base_off.saturating_add(resume_from);
    let mut req = apply_headers(ctx.client.get(&seg.url), ctx.headers);
    if resume_from > 0 || end.is_some() {
        let range = match end {
            Some(e) => format!("bytes={start}-{e}"),
            None => format!("bytes={start}-"),
        };
        req = req.header("Range", range);
    }
    let resp = tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => return Err(HttpError::Cancelled),
        r = tokio::time::timeout(SEGMENT_HEADER_TIMEOUT, req.send()) => match r {
            Ok(sent) => sent.map_err(|e| HttpError::from_reqwest(&e))?,
            // 连上了但迟迟不给响应头：当作瞬时故障重试
            Err(_) => return Err(HttpError::Timeout),
        },
    };
    let status = resp.status();
    if !status.is_success() {
        return Err(HttpError::Http(status.as_u16()));
    }
    // 要了区间却回整段（200 而非 206）：段内续传被无视，数据是从段首开始的
    let whole = resume_from > 0 && status != reqwest::StatusCode::PARTIAL_CONTENT;
    if whole {
        // 段文件里已有的那截作废 —— 否则同一段的开头会被写两遍
        if let SegTarget::Disk(f) = target {
            let mut f = f.lock().unwrap();
            let dropped = f.reset()?;
            if dropped > 0 {
                ctx.stats.spilled.fetch_sub(dropped, Ordering::Relaxed);
            }
        }
    }
    if let SegTarget::Mem(buf) = target {
        buf.lock()
            .unwrap()
            .reserve(seg.size.unwrap_or(1 << 20) as usize);
    }
    let mut stream = resp.bytes_stream();
    loop {
        let chunk = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => return Err(HttpError::Cancelled),
            c = tokio::time::timeout(SEGMENT_IDLE_TIMEOUT, stream.next()) => match c {
                Ok(next) => match next {
                    Some(Ok(c)) => c,
                    Some(Err(e)) => return Err(HttpError::from_reqwest(&e)),
                    None => break,
                },
                // 期间一个字节都没到：断开重连（重试）
                Err(_) => return Err(HttpError::Timeout),
            },
        };
        if chunk.is_empty() {
            continue;
        }
        if let Some(l) = ctx.limiter {
            l.acquire(chunk.len()).await;
        }
        // 先落数据再计数：反过来会在暂停瞬间出现"计数已加、数据还没落"，
        // 收尾按实际长度落盘后进度反而偏低。
        match target {
            SegTarget::Mem(buf) => {
                buf.lock().unwrap().extend_from_slice(&chunk);
                count.add(chunk.len() as u64);
            }
            SegTarget::Disk(f) => {
                f.lock().unwrap().append(&chunk)?;
                count.add(chunk.len() as u64);
            }
        }
    }
    let (data, on_disk, got) = match target {
        SegTarget::Mem(buf) => {
            let data = std::mem::take(&mut *buf.lock().unwrap());
            let got = data.len() as u64;
            (data, 0u64, got)
        }
        SegTarget::Disk(f) => {
            let got = f.lock().unwrap().finish()?;
            (Vec::new(), got, got)
        }
    };
    if let Some(total) = seg.size {
        match target {
            // 段文件长度就是"这一段到目前为止的完整长度"（续传那截也在里面）
            SegTarget::Disk(_) => {
                if got != total {
                    return Err(HttpError::ShortRead);
                }
            }
            // 内存里存的要么是整段（服务器无视 Range），要么是续传剩下的部分
            SegTarget::Mem(_) => {
                let expected = if whole {
                    total
                } else {
                    total.saturating_sub(resume_from)
                };
                if got != expected {
                    return Err(HttpError::ShortRead);
                }
            }
        }
    }
    Ok(SegBody {
        data,
        whole,
        on_disk,
    })
}

/// 拉取单个分片（含重试、限速、解密）。
///
/// `Disk` 目标的续传起点**以段文件当前长度为准**（不是入参）：重试时那部分
/// 已经落盘的数据是有效的，直接接着 `Range` 下即可、不必重下；服务器无视
/// `Range` 回了整段时，`fetch_body_inner` 会把段文件截回 0 再写，产物不会
/// 把同一段开头写两遍。
async fn fetch_segment(
    ctx: &Ctx<'_>,
    seg: &Segment,
    target: &SegTarget,
    mut resume_from: u64,
    slot: Option<&AtomicU64>,
) -> Result<SegBody, HttpError> {
    let mut attempt = 1u32;
    loop {
        if ctx.is_cancelled() {
            return Err(HttpError::Cancelled);
        }
        match target {
            SegTarget::Mem(buf) => buf.lock().unwrap().clear(),
            // 每趟按段文件**当前**长度续（重试不丢已下部分）
            SegTarget::Disk(f) => resume_from = f.lock().unwrap().written,
        }
        let r = async {
            let _conn = ConnGuard::new(&ctx.stats.connections);
            let key = match &seg.key {
                Some(k) => Some(fetch_key(ctx, &k.url).await?),
                None => None,
            };
            let body = fetch_body(ctx, seg, target, resume_from, slot).await?;
            match (&key, &seg.key) {
                // 加密段一律走内存目标（调用方保证）：解密要整段，没法边收边写
                (Some(k), Some(sk)) => decrypt_segment(k, &sk.iv, &body.data).map(|d| SegBody {
                    data: d,
                    whole: false,
                    on_disk: 0,
                }),
                _ => Ok(body),
            }
        }
        .await;
        match r {
            Ok(body) => return Ok(body),
            Err(e) => {
                if e.is_retryable() && attempt < ctx.retries.max(1) {
                    let backoff = Duration::from_millis(400 * u64::from(attempt));
                    tokio::select! {
                        biased;
                        _ = ctx.cancel.cancelled() => return Err(HttpError::Cancelled),
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    attempt += 1;
                    continue;
                }
                return Err(e);
            }
        }
    }
}

/// 按计划下载分片并拼成 `path`。
///
/// 落盘方式由 [`PlaylistOptions::ordered_write`] 决定：
/// - `false`（默认）：**乱序落盘**（[`download_playlist_unordered`]）—— 各段先写
///   自己的段文件、连续前缀一就位就拼进产物；
/// - `true`：**顺序落盘**（[`download_playlist_ordered`]）—— 边下边按清单顺序
///   直接写产物，下载中途的产物就是一个能播的完整前缀（想边下边看时选它）。
///
/// 两条路径的产物**逐字节相同**，差别只在"字节先落在哪儿"。
pub async fn download_playlist(
    client: &reqwest::Client,
    path: &Path,
    plan: &PlaylistPlan,
    opts: &PlaylistOptions,
    cancel: &CancellationToken,
    stats: Arc<PlaylistStats>,
) -> Result<PlaylistDone, HttpError> {
    // 总长初值（两条路径共用）：大清单不做分片预探测，先用主清单的
    // `BANDWIDTH × 总时长` 给个像样的值 —— 界面从第一秒起就有总大小与百分比，
    // 随后被实测外推接管。
    if plan.total.is_none() && plan.duration_secs > 0.0 {
        if let Some(bw) = plan.bitrate {
            let est = (bw as f64 / 8.0 * plan.duration_secs) as u64;
            if est > 0 {
                stats.estimated_total.fetch_max(est, Ordering::Relaxed);
            }
        }
    }
    if opts.ordered_write {
        download_playlist_ordered(client, path, plan, opts, cancel, stats).await
    } else {
        download_playlist_unordered(client, path, plan, opts, cancel, stats).await
    }
}

/// **乱序落盘 + 顺序拼接**：每个分片边收边写进自己的段文件（不占内存），连续
/// 前缀一就位就拼进产物、删掉段文件。
///
/// 与 [`download_playlist_ordered`] 的差别只在字节先落在哪儿，却带来三件事：
/// 1. **吞吐**：不必把"下好但还没轮到写"的分片攒在内存里，也就没有"内存预算
///    用满 → 停止派发新分片"那道闸门 —— 队头慢时其余连接照常满负荷（顺序模式
///    下它们会集体空转，总吞吐掉到单路）；
/// 2. **进度**：落进段文件的字节立刻计入已下载，进度 = 磁盘上的真实字节数，
///    与 `speed` 同步（顺序模式只能吸附光标那一段，用户看到"速度几 MB、进度
///    却几 KB 几 KB 地涨"）；
/// 3. **暂停/退出零损失**：所有已下字节都在段文件里（含半截），恢复时逐段按
///    文件长度发 `Range` 接着下，一个字节都不重下。
///
/// 代价是完成时把段文件拼进产物（本地顺序读写，GB 级约 1~2 秒）；拼一段删一段，
/// 磁盘峰值 ≈ 产物 + 在飞段，不会翻倍。
async fn download_playlist_unordered(
    client: &reqwest::Client,
    path: &Path,
    plan: &PlaylistPlan,
    opts: &PlaylistOptions,
    cancel: &CancellationToken,
    stats: Arc<PlaylistStats>,
) -> Result<PlaylistDone, HttpError> {
    use xfer_storage::FileSink;

    let all: Vec<Segment> = plan
        .init
        .clone()
        .into_iter()
        .chain(plan.segments.iter().cloned())
        .collect();
    let total_segments = plan.segment_count();
    if all.is_empty() {
        return Err(HttpError::Protocol("播放列表中没有可下载的分片".into()));
    }

    let ctrl_path = xfer_storage::ctrl_path(path);
    let fp = fingerprint(plan);
    let dir = seg_dir(path);

    // 续传：只有"本模式 + 清单指纹"都吻合的控制文件才认；否则段目录整个丢掉
    // （绝不复用来历不明的半截文件）
    let resumed = load_ctrl(&ctrl_path, plan).filter(is_unordered_ctrl);
    let (mut written, mut done) = match &resumed {
        Some(c) => (c.prefix.min(all.len()), mask_from_hex(&c.mask, all.len())),
        None => {
            let _ = std::fs::remove_dir_all(&dir);
            (0usize, vec![false; all.len()])
        }
    };
    // 位图说"完整"、段文件却不在 → 那段得重下（段目录被清 / 文件被手工删）
    for i in 0..all.len() {
        if done[i] && !seg_file(path, i).is_file() {
            done[i] = false;
        }
    }
    // 产物文件长度：磁盘上的可能比控制文件长（上次没 flush 的尾巴），以控制
    // 文件为准截断；短了说明产物被动过 → 已拼前缀作废，重新拼
    let disk_len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let spliced = match &resumed {
        Some(c) if disk_len >= c.bytes => c.bytes,
        _ => 0,
    };
    if resumed.is_some() && spliced == 0 {
        written = 0;
    }
    stats.mark_unordered();
    stats.completed.store(spliced, Ordering::Relaxed);
    // 段文件里的字节也算已下载（进度 = 磁盘上真实存在的字节总数）
    let mut spilled0 = 0u64;
    for i in 0..all.len() {
        if !done[i] {
            if let Ok(m) = std::fs::metadata(seg_file(path, i)) {
                spilled0 = spilled0.saturating_add(m.len());
            }
        }
    }
    stats.spilled.store(spilled0, Ordering::Relaxed);

    let mut sink = if spliced > 0 {
        if let Some(p) = path.parent() {
            let _ = std::fs::create_dir_all(p);
        }
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .and_then(|f| f.set_len(spliced))
            .map_err(|e| HttpError::Io(e.to_string()))?;
        FileSink::append_at(path, spliced).map_err(|e| HttpError::Io(e.to_string()))?
    } else {
        FileSink::create(path).map_err(|e| HttpError::Io(e.to_string()))?
    };

    let ctx = Ctx {
        client,
        cancel,
        limiter: opts.limiter.as_deref(),
        headers: &opts.headers,
        retries: opts.retries.max(1),
        stats: &stats,
        keys: Mutex::new(HashMap::new()),
    };
    let conn = opts.concurrency.clamp(1, 64);
    let ctrl_of = |written: usize, bytes: u64, done: &[bool]| PlaylistCtrl {
        kind: CTRL_KIND.to_string(),
        v: 1,
        fp: fp.clone(),
        url: plan.source.clone(),
        segs: total_segments,
        prefix: written,
        bytes,
        part: 0,
        mode: CTRL_MODE_UNORDERED.to_string(),
        mask: mask_to_hex(done),
    };
    // 已完成段的时长合计：乱序下"已覆盖时长"不能用前缀算（段是乱序完成的），
    // 用已完成段的时长和 —— 段大小/时长的比例在整条流上是稳定的，够用
    let mut done_dur: f64 = (0..all.len())
        .filter(|i| done[*i])
        .map(|i| all[i].duration)
        .sum();
    let mut last_save = std::time::Instant::now() - CTRL_SAVE_INTERVAL;
    let mut in_flight = 0usize;
    // 派发从"还没拼进产物的第一段"开始：`written` 之前的段已经躺在产物里了，
    // 再派发一次就是白下（续传时最容易踩到）
    let mut next_spawn = written;
    let mut handles: HashMap<usize, Arc<Mutex<SegFile>>> = HashMap::new();
    let mut futs: FuturesUnordered<BoxFuture<'_, (usize, Result<SegBody, HttpError>)>> =
        FuturesUnordered::new();
    let mut failure: Option<HttpError> = None;

    loop {
        // 1) 把连续前缀拼进产物（拼一段删一段，磁盘不翻倍）
        while written < all.len() && done[written] {
            let f = seg_file(path, written);
            match splice_seg(&f, &mut sink) {
                Ok(moved) => {
                    done[written] = false;
                    written += 1;
                    // 进度"转移"：先记产物侧、再减段文件侧 —— 中途只会略偏高一点，
                    // 绝不会回落
                    stats.completed.store(sink.position(), Ordering::Relaxed);
                    let _ = stats
                        .spilled
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                            Some(v.saturating_sub(moved))
                        });
                }
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            }
        }
        if failure.is_some() || written == all.len() {
            break;
        }
        // 2) 补足在飞下载：**没有内存闸门**（字节直接落段文件），连接始终满载
        while in_flight < conn && next_spawn < all.len() {
            let i = next_spawn;
            next_spawn += 1;
            if done[i] {
                continue;
            }
            let sf = seg_file(path, i);
            let existing = std::fs::metadata(&sf).map(|m| m.len()).unwrap_or(0);
            let file = match SegFile::open(&sf, existing == 0) {
                Ok(f) => f,
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            };
            in_flight += 1;
            let target = if all[i].key.is_some() {
                // 加密段：解密要整段，先收进内存（收完由主循环写段文件）
                SegTarget::Mem(Arc::new(Mutex::new(Vec::new())))
            } else {
                let h = Arc::new(Mutex::new(file));
                handles.insert(i, h.clone());
                SegTarget::Disk(h)
            };
            let seg = &all[i];
            let ctx = &ctx;
            futs.push(Box::pin(async move {
                let r = fetch_segment(ctx, seg, &target, 0, None).await;
                (i, r)
            }));
        }
        if failure.is_some() {
            break;
        }
        // 3) 等一个下载结果
        match futs.next().await {
            Some((i, Ok(body))) => {
                in_flight -= 1;
                handles.remove(&i);
                if body.on_disk == 0 {
                    // 加密段：把解出来的明文写进段文件 —— 到这一刻才算落到盘上
                    let sf = seg_file(path, i);
                    let w = SegFile::open(&sf, true).and_then(|mut f| {
                        f.append(&body.data)?;
                        f.finish()
                    });
                    match w {
                        Ok(n) => {
                            stats.spilled.fetch_add(n, Ordering::Relaxed);
                        }
                        Err(e) => {
                            failure = Some(e);
                            break;
                        }
                    }
                }
                done[i] = true;
                done_dur += all[i].duration;
                // 实测外推总长：已下字节 ÷ 已覆盖时长 × 总时长（只增不减，
                // 免得进度条往回跳）。初值由分发器按清单码率给。
                if plan.total.is_none() && done_dur > 0.0 && plan.duration_secs > 0.0 {
                    let got = stats
                        .completed
                        .load(Ordering::Relaxed)
                        .saturating_add(stats.spilled.load(Ordering::Relaxed));
                    let est = (got as f64 / done_dur * plan.duration_secs) as u64;
                    if est > 0 {
                        stats.estimated_total.fetch_max(est, Ordering::Relaxed);
                    }
                }
                if last_save.elapsed() >= CTRL_SAVE_INTERVAL {
                    if let Err(e) = sink.flush() {
                        failure = Some(HttpError::Io(e.to_string()));
                        break;
                    }
                    save_ctrl(&ctrl_path, &ctrl_of(written, sink.position(), &done));
                    last_save = std::time::Instant::now();
                }
            }
            Some((_, Err(e))) => {
                failure = Some(e);
                break;
            }
            None => {
                if failure.is_none() && written < all.len() {
                    failure = Some(HttpError::Protocol(format!(
                        "播放列表未下载完整: {written}/{total_segments} 段"
                    )));
                }
                break;
            }
        }
    }

    // 收尾：丢掉在飞请求，并把刚好凑齐的连续前缀补拼完
    drop(futs);
    drop(handles);
    while failure.is_none() && written < all.len() && done[written] {
        let f = seg_file(path, written);
        match splice_seg(&f, &mut sink) {
            Ok(moved) => {
                done[written] = false;
                written += 1;
                stats.completed.store(sink.position(), Ordering::Relaxed);
                let _ = stats
                    .spilled
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                        Some(v.saturating_sub(moved))
                    });
            }
            Err(e) => failure = Some(e),
        }
    }
    let flush = sink.flush().map_err(|e| HttpError::Io(e.to_string()));
    if let Err(e) = flush {
        if failure.is_none() {
            failure = Some(e);
        }
    }
    match (&failure, written == total_segments) {
        (Some(_), _) => {
            save_ctrl(&ctrl_path, &ctrl_of(written, sink.position(), &done));
            Err(failure.unwrap())
        }
        (None, true) => {
            // 全部拼进去了：段目录是空的，顺手收掉它和控制文件
            let _ = std::fs::remove_dir_all(&dir);
            let _ = std::fs::remove_file(&ctrl_path);
            Ok(PlaylistDone {
                bytes: sink.position(),
                segments: written,
                total_segments,
            })
        }
        (None, false) => {
            save_ctrl(&ctrl_path, &ctrl_of(written, sink.position(), &done));
            Err(HttpError::Protocol(format!(
                "播放列表未下载完整: {written}/{total_segments} 段"
            )))
        }
    }
}

/// **顺序落盘**：边下边按清单顺序直接写产物文件。
///
/// 取消时返回 [`HttpError::Cancelled`]：已持久化的内容（完整前缀 + 「下一
/// 待写段」已经收到的开头部分）留在文件与控制文件里，下次调用从该处续传，
/// 段内那截用 `Range` 接着下，不必整段重来。
async fn download_playlist_ordered(
    client: &reqwest::Client,
    path: &Path,
    plan: &PlaylistPlan,
    opts: &PlaylistOptions,
    cancel: &CancellationToken,
    stats: Arc<PlaylistStats>,
) -> Result<PlaylistDone, HttpError> {
    use xfer_storage::FileSink;

    let all: Vec<Segment> = plan
        .init
        .clone()
        .into_iter()
        .chain(plan.segments.iter().cloned())
        .collect();
    let total_segments = plan.segment_count();
    if all.is_empty() {
        return Err(HttpError::Protocol("播放列表中没有可下载的分片".into()));
    }

    let ctrl_path = xfer_storage::ctrl_path(path);
    let fp = fingerprint(plan);

    // 续传现场：文件 = 「完整前缀」+ 「第 prefix 段的开头 part 字节」
    let st = resume_state(path, plan);
    let prefix = st.prefix.min(all.len());
    let mut complete_bytes = st.prefix_bytes; // 完整前缀的字节数
    // 段内续传的偏移只对明文段有效：AES-128-CBC 分段从中间接不上（IV 链断）
    let mut pending_resume = if prefix < all.len() && all[prefix].key.is_none() {
        st.part
    } else {
        0
    };
    // 段内续传**不预先探测** `Range` 支持：直接按续传位置发请求，服务器若无视
    // `Range` 回了整段，`SegBody::whole` 那条回退会把文件截回完整前缀再整段写，
    // 结果一样正确。省掉一次探测请求（还避开"暂停后连接半死、探测结论不可信"）。
    let have_bytes = complete_bytes.saturating_add(pending_resume);

    // 落位：续传时截断到已持久化水位（丢弃上次未 fsync 的尾巴），否则重建
    let mut sink = if have_bytes > 0 {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .and_then(|f| f.set_len(have_bytes))
            .map_err(|e| HttpError::Io(e.to_string()))?;
        FileSink::append_at(path, have_bytes).map_err(|e| HttpError::Io(e.to_string()))?
    } else {
        FileSink::create(path).map_err(|e| HttpError::Io(e.to_string()))?
    };
    stats.completed.store(have_bytes, Ordering::Relaxed);
    // 文件当前长度（终局保存水位时用；`complete_bytes` 只到最后一个完整段）
    let mut file_bytes = have_bytes;

    let ctx = Ctx {
        client,
        cancel,
        limiter: opts.limiter.as_deref(),
        headers: &opts.headers,
        retries: opts.retries.max(1),
        stats: &stats,
        keys: Mutex::new(HashMap::new()),
    };

    let conn = opts.concurrency.clamp(1, 64);
    let mut written = prefix;

    // 总长实时估算的记账（估算只在 plan.total 未知时需要）：
    //   done_bytes/done_dur/done_segs 都从续传基线起步，
    //   估算 = max(按字节密度外推, 已下字节 + 平均分片大小 × 剩余分片数)
    let need_estimate = plan.total.is_none();
    // 总长先给一个"清单里就有"的粗估：`BANDWIDTH × 总时长 / 8`。大清单不做分片
    // 预探测，实测外推又要等第一个分片落盘 —— 单连接被限速的站点上一个 1.5MB
    // 分片要几十秒，那段时间里界面连"总大小"都显示不出来。有了它，进度条从
    // 下载第一秒起就有分母（误差 ±10% 量级），随后被实测外推接管。
    if need_estimate && plan.duration_secs > 0.0 {
        if let Some(bw) = plan.bitrate {
            let est = (bw as f64 / 8.0 * plan.duration_secs) as u64;
            if est > 0 {
                stats
                    .estimated_total
                    .store(est.max(have_bytes), Ordering::Relaxed);
            }
        }
    }
    let mut est_done_bytes: u64 = have_bytes;
    let mut est_done_dur: f64 = all[..prefix].iter().map(|s| s.duration).sum();
    let publish_estimate = |stats: &PlaylistStats,
                            est_done_bytes: u64,
                            est_done_dur: f64,
                            done_segs: usize| {
        if !need_estimate || done_segs == 0 || est_done_dur <= 0.0 || plan.duration_secs <= 0.0 {
            return;
        }
        let avg = est_done_bytes as f64 / done_segs as f64;
        let rest = all.len().saturating_sub(done_segs) as f64;
        let density = est_done_bytes as f64 / est_done_dur * plan.duration_secs;
        let est = density.max(est_done_bytes as f64 + avg * rest);
        if est.is_finite() && est > 0.0 {
            stats.estimated_total.store(est as u64, Ordering::Relaxed);
        }
    };
    publish_estimate(&stats, est_done_bytes, est_done_dur, prefix);
    let mut last_save = std::time::Instant::now() - CTRL_SAVE_INTERVAL;
    // `bytes` = 完整前缀的字节数；`part` = 紧跟着的段内续传那截
    // `mode` 留空 = 顺序落盘（乱序那条路径用 "unordered"），两种模式互不认
    let ctrl_of = |prefix: usize, bytes: u64, part: u64| PlaylistCtrl {
        kind: CTRL_KIND.to_string(),
        v: 1,
        fp: fp.clone(),
        url: plan.source.clone(),
        segs: total_segments,
        prefix,
        bytes,
        part,
        mode: String::new(),
        mask: String::new(),
    };

    // 重排窗口：**在飞下载**与**已下好待写**分开计。
    //
    // `futures::buffered(conn)` 把"下好但排在慢分片之后"的分片一直留在队列里，
    // 占着在飞名额、不再补新分片 —— 队头一慢，其余连接全部空转。这里改成：
    // 并发上限只管真正在跑的请求，"已下好待写"另有一个内存预算
    // [`MAX_UNWRITTEN_BYTES`]，预算内继续往前跑，写盘只在连续前缀可用时推进。
    let mut ready: HashMap<usize, Vec<u8>> = HashMap::new();
    let mut ready_bytes: u64 = 0;
    let mut in_flight: usize = 0;
    let mut next_spawn = prefix;
    // 在飞分片的共享缓冲（暂停时"下一待写段"的那截靠它落盘续传）
    let mut partials: HashMap<usize, Arc<Mutex<Vec<u8>>>> = HashMap::new();
    let mut futs: FuturesUnordered<BoxFuture<'_, (usize, Result<SegBody, HttpError>)>> =
        FuturesUnordered::new();

    let mut failure: Option<HttpError> = None;
    // 待写光标：进度里唯一允许算进"在飞字节"的那一段（见 `PlaylistStats::progress`）。
    // 续传起步时它就是 `prefix`（接下来要写的那一段）。
    stats.set_cursor(prefix, all.get(prefix).is_none_or(|s| s.key.is_none()));
    loop {
        // 1) 连续前缀能写多少写多少（写完即释放"待写"内存预算）
        while written < all.len() {
            let Some(data) = ready.remove(&written) else {
                break;
            };
            ready_bytes = ready_bytes.saturating_sub(data.len() as u64);
            let seg_dur = all[written].duration;
            if let Err(e) = sink.write(&data) {
                failure = Some(HttpError::Io(e.to_string()));
                break;
            }
            written += 1;
            est_done_bytes = sink.position();
            complete_bytes = est_done_bytes; // 刚写完的是一整段，文件里没有半截尾巴
            est_done_dur += seg_dur;
            // 光标前移**必须早于** `completed` 更新：顺序反了会有一个窗口，
            // 此刻 `completed` 已含刚写完的整段、而光标还指着它（槽位里也是
            // 整段字节），进度会凭空多出一段。
            stats.set_cursor(written, all.get(written).is_none_or(|s| s.key.is_none()));
            stats.completed.store(est_done_bytes, Ordering::Relaxed);
            publish_estimate(&stats, est_done_bytes, est_done_dur, written);
            if last_save.elapsed() >= CTRL_SAVE_INTERVAL {
                // 先 fsync 再记水位：控制文件绝不领先磁盘（同上层的续传不变式）
                if let Err(e) = sink.flush() {
                    failure = Some(HttpError::Io(e.to_string()));
                    break;
                }
                save_ctrl(&ctrl_path, &ctrl_of(written, complete_bytes, 0));
                last_save = std::time::Instant::now();
            }
        }
        if failure.is_some() || written == all.len() {
            break;
        }
        // 2) 补足在飞下载：并发上限 + 待写内存预算双闸门
        while in_flight < conn
            && next_spawn < all.len()
            && ready_bytes < MAX_UNWRITTEN_BYTES
        {
            let i = next_spawn;
            next_spawn += 1;
            in_flight += 1;
            // 上一次没收完的"下一待写段"：这一段要按段内续传的偏移接着下
            let resume_from = if i == written { std::mem::take(&mut pending_resume) } else { 0 };
            let seg = &all[i];
            let slot = stats.slot(i);
            let buf = Arc::new(Mutex::new(Vec::new()));
            partials.insert(i, buf.clone());
            // 顺序落盘：数据先收进内存（"下好但没轮到写"的分片就攒在这里，
            // 受 `MAX_UNWRITTEN_BYTES` 约束）
            let target = SegTarget::Mem(buf);
            let ctx = &ctx;
            futs.push(Box::pin(async move {
                let r = fetch_segment(ctx, seg, &target, resume_from, Some(&slot)).await;
                (i, r)
            }));
        }
        // 3) 等一个下载结果（拿到就先回到 1) 写盘）
        match futs.next().await {
            Some((i, Ok(body))) => {
                in_flight -= 1;
                partials.remove(&i);
                // 这一段的槽位"转正"：整段已收完、排在 `ready` 里等写盘，
                // 落盘之前它没有资格算进进度（进度只认光标那一个）——清零即可。
                stats.slot(i).store(0, Ordering::Relaxed);
                let data = body.data;
                if body.whole && sink.position() > complete_bytes {
                    // 服务器无视 Range 回了整段：把文件里那截续传数据丢掉再写，
                    // 否则同一段的开头会被写两遍
                    tracing::debug!(seg = i, "分片服务器不支持 Range，丢弃段内续传数据");
                    if let Err(e) = sink.flush() {
                        failure = Some(HttpError::Io(e.to_string()));
                        break;
                    }
                    if let Ok(f) = std::fs::OpenOptions::new().write(true).open(path) {
                        let _ = f.set_len(complete_bytes);
                    }
                    match FileSink::append_at(path, complete_bytes) {
                        Ok(s) => sink = s,
                        Err(e) => {
                            failure = Some(HttpError::Io(e.to_string()));
                            break;
                        }
                    }
                    stats.completed.store(complete_bytes, Ordering::Relaxed);
                }
                ready_bytes = ready_bytes.saturating_add(data.len() as u64);
                ready.insert(i, data);
            }
            Some((_, Err(e))) => {
                failure = Some(e);
                break;
            }
            None => {
                // 在飞任务清空却没写完：清单被服务端截断（正常路径不可达，
                // 因为每个分片都被派发过）
                if failure.is_none() && written < all.len() {
                    failure = Some(HttpError::Protocol(format!(
                        "播放列表未下载完整: {written}/{total_segments} 段"
                    )));
                }
                break;
            }
        }
    }
    // 收尾：丢掉仍在飞的请求（守卫会把它们这趟读到的字节从计数里扣回去）
    drop(futs);

    // **段内续传**：把"下一待写段"已经收到的那截追加进文件 —— 它不是白下的，
    // 下次对这一段发 `Range: bytes=part-` 就接着下（暂停不再整段重来）。
    // 只有"下一待写段"能续：写盘严格按序，别的段都要等它。
    if failure.is_some() {
        let encrypted = all.get(written).map(|s| s.key.is_some()).unwrap_or(true);
        let tail: Vec<u8> = partials
            .remove(&written)
            .map(|b| std::mem::take(&mut *b.lock().unwrap()))
            .unwrap_or_default();
        if !encrypted && !tail.is_empty() && sink.write(&tail).is_ok() {
            est_done_bytes = sink.position();
            // 这截已经转正落进文件、算进 `completed` 了：槽位清零，否则
            // `progress()` 会把它再加一遍（差一截 `part`）。
            stats.slot(written).store(0, Ordering::Relaxed);
            stats.completed.store(est_done_bytes, Ordering::Relaxed);
        }
    }

    // 终局：刷盘 + 保存水位；全部完成时删除控制文件
    let flush = sink.flush().map_err(|e| HttpError::Io(e.to_string()));
    if let Err(e) = flush {
        if failure.is_none() {
            failure = Some(e);
        }
    } else {
        file_bytes = sink.position();
    }
    // 文件尾部多出来的这截就是段内续传部分（完整前缀之外的内容）
    let part = file_bytes.saturating_sub(complete_bytes);
    match (&failure, written == total_segments) {
        (Some(_), _) => {
            save_ctrl(&ctrl_path, &ctrl_of(written, complete_bytes, part));
            return Err(failure.unwrap());
        }
        (None, true) => {
            let _ = std::fs::remove_file(&ctrl_path);
            return Ok(PlaylistDone {
                bytes: file_bytes,
                segments: written,
                total_segments,
            });
        }
        (None, false) => {
            // 分片流提前结束（清单被服务端截断）：保留水位，按失败上报
            save_ctrl(&ctrl_path, &ctrl_of(written, complete_bytes, part));
            return Err(HttpError::Protocol(format!(
                "播放列表未下载完整: {written}/{total_segments} 段"
            )));
        }
    }
}

/// 默认产物文件名：清单地址末段去扩展名，通用名则回退父目录名。
pub fn default_filename(manifest_url: &str, fmp4: bool) -> String {
    let ext = if fmp4 { "mp4" } else { "ts" };
    let raw = crate::filename_from_url(manifest_url).unwrap_or_default();
    let stem = strip_playlist_ext(&raw);
    let generic = is_generic_stem(&stem);
    let name = if !stem.is_empty() && !generic {
        stem
    } else {
        // 末段是 index/playlist 这类通用名时用父目录名（站点常把真实
        // 名称放在目录上，如 /videos/我的影片/index.m3u8）
        let no_query = manifest_url.split(['?', '#']).next().unwrap_or(manifest_url);
        let parent = no_query
            .rsplit('/')
            .nth(1)
            .map(|s| xfer_types::text::decode_percent_text(s))
            .unwrap_or_default();
        if !parent.is_empty() && !is_generic_stem(&parent) {
            parent
        } else {
            "video".to_string()
        }
    };
    format!("{name}.{ext}")
}

fn strip_playlist_ext(name: &str) -> String {
    let lower = name.to_ascii_lowercase();
    for ext in [".m3u8", ".m3u"] {
        if let Some(stripped) = lower.strip_suffix(ext) {
            return name[..stripped.len()].to_string();
        }
    }
    name.to_string()
}

fn is_generic_stem(s: &str) -> bool {
    const GENERIC: [&str; 16] = [
        "index",
        "playlist",
        "master",
        "manifest",
        "main",
        "media",
        "hls",
        "video",
        "out",
        "output",
        "stream",
        "chunklist",
        "prog_index",
        "play",
        "hlsr",
        "dash",
    ];
    s.is_empty() || GENERIC.contains(&s.to_ascii_lowercase().as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Url {
        Url::parse("https://cdn.example.com/videos/abc/index.m3u8").unwrap()
    }

    #[test]
    fn parses_attributes_with_quotes() {
        let a = parse_attributes(r#"BANDWIDTH=800000,RESOLUTION=1280x720,CODECS="avc1,mp4a",NAME="中 文""#);
        assert_eq!(attr(&a, "BANDWIDTH"), Some("800000"));
        assert_eq!(attr(&a, "RESOLUTION"), Some("1280x720"));
        assert_eq!(attr(&a, "CODECS"), Some("avc1,mp4a"));
        assert_eq!(attr(&a, "NAME"), Some("中 文"));
    }

    #[test]
    fn parses_media_segments_with_map_and_byterange() {
        let text = "\u{feff}#EXTM3U\n#EXT-X-TARGETDURATION:10\n#EXT-X-MEDIA-SEQUENCE:3\n\
                    #EXT-X-MAP:URI=\"init.mp4\",BYTERANGE=\"720@0\"\n\
                    #EXTINF:9.0,\nseg0.m4s\n#EXTINF:9.0,\nseg1.m4s\n\
                    #EXT-X-BYTERANGE:1000@0\n#EXTINF:1.0,\nsub.ts\n\
                    #EXT-X-BYTERANGE:500\n#EXTINF:1.0,\nsub.ts\n#EXT-X-ENDLIST\n";
        let m = parse_media(text, &base()).unwrap();
        assert!(!m.live);
        let init = m.init.unwrap();
        assert_eq!(init.url, "https://cdn.example.com/videos/abc/init.mp4");
        assert_eq!(init.range, Some((0, 720)));
        assert_eq!(m.segments.len(), 4);
        assert_eq!(m.segments[0].url, "https://cdn.example.com/videos/abc/seg0.m4s");
        assert_eq!(m.segments[0].size, None);
        assert_eq!(m.segments[2].range, Some((0, 1000)));
        // 省略 offset：接续同地址上一段的末尾
        assert_eq!(m.segments[3].range, Some((1000, 500)));
        assert_eq!(m.segments[3].size, Some(500));
    }

    #[test]
    fn parses_aes128_key_with_iv_rotation() {
        let text = "#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:7\n\
                    #EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\",IV=0x00000000000000000000000000000007\n\
                    #EXTINF:4,\na.ts\n#EXT-X-KEY:METHOD=AES-128,URI=\"../k2.bin\"\n\
                    #EXTINF:4,\nb.ts\n#EXT-X-KEY:METHOD=NONE\n#EXTINF:4,\nc.ts\n#EXT-X-ENDLIST\n";
        let m = parse_media(text, &base()).unwrap();
        let k0 = m.segments[0].key.clone().unwrap();
        assert_eq!(k0.url, "https://cdn.example.com/videos/abc/key.bin");
        assert_eq!(k0.iv, iv_from_sequence(7));
        // 第二个密钥无 IV：按该分片的媒体序号推导（序号 8）
        let k1 = m.segments[1].key.clone().unwrap();
        assert_eq!(k1.url, "https://cdn.example.com/videos/k2.bin");
        assert_eq!(k1.iv, iv_from_sequence(8));
        // METHOD=NONE 之后恢复明文
        assert!(m.segments[2].key.is_none());
    }

    #[test]
    fn rejects_sample_aes() {
        let text = "#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"k\"\n#EXTINF:4,\na.ts\n#EXT-X-ENDLIST\n";
        assert!(parse_media(text, &base()).is_err());
    }

    #[test]
    fn master_playlist_picks_highest_bandwidth() {
        let text = "#EXTM3U\n\
            #EXT-X-STREAM-INF:BANDWIDTH=500000,RESOLUTION=640x360\nlow.m3u8\n\
            #EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=1920x1080\nhigh.m3u8\n\
            #EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=1280x720\nmid.m3u8\n";
        assert!(is_master(text));
        let v = parse_variants(text, &base());
        assert_eq!(v.len(), 3);
        let best = choose_variant(&v, false).unwrap();
        assert!(best.url.ends_with("high.m3u8"));
        let worst = choose_variant(&v, true).unwrap();
        assert!(worst.url.ends_with("low.m3u8"));
    }

    #[test]
    fn default_filename_uses_parent_dir_for_generic_stems() {
        assert_eq!(
            default_filename("https://x/videos/my_movie/index.m3u8?t=1", false),
            "my_movie.ts"
        );
        assert_eq!(
            default_filename("https://x/a/episode01.m3u8", true),
            "episode01.mp4"
        );
        assert_eq!(
            default_filename("https://x/%E4%B8%AD%E6%96%87/index.m3u8", false),
            "中文.ts"
        );
    }

    #[test]
    fn fingerprint_changes_with_segment_list() {
        let mk = |u: &str| PlaylistPlan {
            source: "https://x/i.m3u8".into(),
            chain: vec![],
            bitrate: None,
            init: None,
            segments: vec![Segment {
                url: u.into(),
                range: None,
                key: None,
                size: None,
                duration: 4.0,
            }],
            live: true,
            fmp4: false,
            total: None,
            duration_secs: 4.0,
        };
        assert_eq!(fingerprint(&mk("a.ts")), fingerprint(&mk("a.ts")));
        assert_ne!(fingerprint(&mk("a.ts")), fingerprint(&mk("b.ts")));
    }

    #[test]
    fn iv_parsing_is_right_aligned() {
        assert_eq!(parse_iv("0x01"), Some(iv_from_sequence(1)));
        assert_eq!(parse_iv("ff"), Some({
            let mut v = [0u8; 16];
            v[15] = 0xff;
            v
        }));
        assert_eq!(parse_iv("0xabc"), None); // 奇数长度非法
        assert_eq!(parse_iv(""), None);
    }

    #[test]
    fn aes128_cbc_roundtrip() {
        use aes::cipher::{BlockEncryptMut, KeyIvInit};
        type Enc = cbc::Encryptor<aes::Aes128>;
        let key = [7u8; 16];
        let iv = iv_from_sequence(42);
        let plain = b"hello hls segment payload".to_vec();
        // PKCS7 需要额外一个块的填充空间
        let mut buf = vec![0u8; plain.len() + 16];
        buf[..plain.len()].copy_from_slice(&plain);
        let enc = Enc::new(&key.into(), &iv.into());
        let ct = enc
            .encrypt_padded_mut::<aes::cipher::block_padding::Pkcs7>(&mut buf, plain.len())
            .unwrap()
            .to_vec();
        assert_ne!(ct, plain);
        assert_eq!(decrypt_segment(&key, &iv, &ct).unwrap(), plain);
        // 非法密文长度必须报错而不是静默截断
        assert!(decrypt_segment(&key, &iv, &ct[..ct.len() - 1]).is_err());
    }

    #[test]
    fn ctrl_kind_is_distinct_from_split() {
        // split 的控制文件没有 kind 字段，互相不能误读
        let split_like = r#"{"v":1,"total":100,"base":0,"url":"u","segs":[{"s":0,"w":0,"e":100}]}"#;
        assert!(serde_json::from_str::<PlaylistCtrl>(split_like).is_err());
    }

    // ------------------------------------------------------------------
    // 集成测试：本地 axum 服务（支持 Range，可注入失败）
    // ------------------------------------------------------------------

    use std::sync::atomic::AtomicUsize;

    /// 位置敏感数据：任何错位/重复写都会暴露。
    fn sample(tag: u8, len: usize) -> Vec<u8> {
        (0..len).map(|i| (i as u8).wrapping_add(tag)).collect()
    }

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("xfer-hls-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        // 控制文件目录隔离：走全进程一次的初始化，避免与并行执行的
        // 其它用例（分片下载）互相改写环境变量，详见 `crate::testutil`
        crate::testutil::init_ctrl_dir();
        d
    }

    struct TestServer {
        base: String,
        hits: HashMap<String, Arc<AtomicUsize>>,
        /// 每个请求记一条：(路径, `Range` 头)
        requests: Arc<Mutex<Vec<(String, Option<String>)>>>,
    }

    impl TestServer {
        fn url(&self, p: &str) -> String {
            format!("{}{}", self.base, p)
        }
        fn hits(&self, p: &str) -> usize {
            self.hits.get(p).map(|c| c.load(Ordering::SeqCst)).unwrap_or(0)
        }
        /// 某个路径上收到的全部 `Range` 头（按时间顺序）。
        fn ranges(&self, p: &str) -> Vec<Option<String>> {
            self.requests
                .lock()
                .unwrap()
                .iter()
                .filter(|(path, _)| path == p)
                .map(|(_, r)| r.clone())
                .collect()
        }
    }

    /// 测试服务端行为开关（默认全关）。
    #[derive(Default, Clone)]
    struct ServerOpts {
        /// 前 N 次请求返回 500（模拟瞬时故障）
        fail_first: HashMap<String, usize>,
        /// 响应前的固定延迟（毫秒），模拟 CDN 抖动
        delays: HashMap<String, u64>,
        /// 分块慢发 `(块字节, 块间隔毫秒)`：用来制造"段内只下到一半"的场景
        trickle: HashMap<String, (usize, u64)>,
        /// 只认"从 0 开始"的 `Range`：非 0 起点的区间请求一律回整段（200）。
        /// 这是线上真实存在的 CDN 怪癖，用来验证段内续传被拒时的回退。
        range_only_from_zero: std::collections::HashSet<String>,
    }

    /// 起一个支持 Range 的静态文件服务；`fail_first` 里列出的路径前 N 次
    /// 请求返回 500（模拟瞬时故障，用于验证续传）。
    async fn start_server(
        files: HashMap<String, Vec<u8>>,
        fail_first: HashMap<String, usize>,
    ) -> TestServer {
        start_server_opts(
            files,
            ServerOpts {
                fail_first,
                ..Default::default()
            },
        )
        .await
    }

    /// 同上，另可按路径注入"响应前延迟"（毫秒）——模拟 CDN 抖动，
    /// 用于验证慢分片不阻塞后续分片、也不虚报进度。
    async fn start_server_with_delay(
        files: HashMap<String, Vec<u8>>,
        fail_first: HashMap<String, usize>,
        delays: HashMap<String, u64>,
    ) -> TestServer {
        start_server_opts(
            files,
            ServerOpts {
                fail_first,
                delays,
                ..Default::default()
            },
        )
        .await
    }

    async fn start_server_opts(files: HashMap<String, Vec<u8>>, opts: ServerOpts) -> TestServer {
        use axum::http::{header, HeaderValue, StatusCode};
        use axum::response::IntoResponse;
        use axum::routing::get;
        use axum::Router;

        let mut hits: HashMap<String, Arc<AtomicUsize>> = HashMap::new();
        let requests: Arc<Mutex<Vec<(String, Option<String>)>>> = Arc::new(Mutex::new(Vec::new()));
        let mut app = Router::new();
        for (path, body) in files {
            let data = Arc::new(body);
            let hit = Arc::new(AtomicUsize::new(0));
            let fail = opts
                .fail_first
                .get(&path)
                .map(|n| Arc::new(AtomicUsize::new(*n)));
            let delay = opts.delays.get(&path).copied().unwrap_or(0);
            let trickle = opts.trickle.get(&path).copied();
            let zero_only = opts.range_only_from_zero.contains(&path);
            let requests = requests.clone();
            let route = path.clone();
            hits.insert(path.clone(), hit.clone());
            app = app.route(
                &path,
                get(move |headers: axum::http::HeaderMap| {
                    let data = data.clone();
                    let hit = hit.clone();
                    let fail = fail.clone();
                    let requests = requests.clone();
                    let route = route.clone();
                    async move {
                        let seen = hit.fetch_add(1, Ordering::SeqCst);
                        let range_hdr = headers
                            .get(header::RANGE)
                            .and_then(|v| v.to_str().ok())
                            .map(|s| s.to_string());
                        requests.lock().unwrap().push((route.clone(), range_hdr.clone()));
                        if delay > 0 {
                            tokio::time::sleep(Duration::from_millis(delay)).await;
                        }
                        if let Some(f) = &fail {
                            if seen < f.load(Ordering::SeqCst) {
                                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                            }
                        }
                        let total = data.len();
                        let range = range_hdr.clone().unwrap_or_default();
                        let (from, to) = match range
                            .strip_prefix("bytes=")
                            .and_then(|r| r.split_once('-'))
                        {
                            Some((f, t)) => (
                                f.trim().parse::<usize>().unwrap_or(0),
                                t.trim().parse::<usize>().unwrap_or(total),
                            ),
                            None => (0, total),
                        };
                        let from = from.min(total);
                        let to = (to + 1).min(total).max(from);
                        // CDN 怪癖：非 0 起点的区间请求直接回整段（200），
                        // 客户端必须能识别并回退
                        let (from, to) = if zero_only && from > 0 {
                            (0, total)
                        } else {
                            (from, to)
                        };
                        let partial = from > 0 || to < total;
                        let body = data[from..to].to_vec();
                        let status = if partial {
                            StatusCode::PARTIAL_CONTENT
                        } else {
                            StatusCode::OK
                        };
                        let mut resp = match trickle {
                            // 分块慢发：制造"段内只收到一半"的现场
                            Some((chunk, gap)) => {
                                let stream = futures_util::stream::unfold(
                                    (body, 0usize),
                                    move |(b, off)| async move {
                                        if off >= b.len() {
                                            return None;
                                        }
                                        let end = (off + chunk).min(b.len());
                                        let piece = b[off..end].to_vec();
                                        tokio::time::sleep(Duration::from_millis(gap)).await;
                                        Some((
                                            Ok::<_, std::io::Error>(bytes::Bytes::from(piece)),
                                            (b, end),
                                        ))
                                    },
                                );
                                axum::response::Response::new(axum::body::Body::from_stream(stream))
                            }
                            None => axum::response::Response::new(axum::body::Body::from(body)),
                        };
                        *resp.status_mut() = status;
                        if partial {
                            resp.headers_mut().insert(
                                header::CONTENT_RANGE,
                                HeaderValue::from_str(&format!(
                                    "bytes {}-{}/{}",
                                    from,
                                    to.saturating_sub(1),
                                    total
                                ))
                                .unwrap(),
                            );
                        }
                        resp
                    }
                }),
            );
        }
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(l, app).await;
        });
        TestServer {
            base: format!("http://{addr}"),
            hits,
            requests,
        }
    }

    fn media_playlist(paths: &[&str]) -> String {
        let mut s = String::from("#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:0\n");
        for p in paths {
            s.push_str(&format!("#EXTINF:4.0,\n{p}\n"));
        }
        s.push_str("#EXT-X-ENDLIST\n");
        s
    }

    /// 大清单**一个探测请求都不许发**，总长改用实时估算。
    ///
    /// 这是"下载前卡几分钟 + 走走停停"的根因回归测试：756 个分片的清单要是
    /// 逐个 `Range: bytes=0-0` 探测，就是 756 个额外请求（实测 75 秒起步），
    /// 这期间一个字节都没下、界面上什么都不会动。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn large_playlist_skips_probe_and_estimates_total() {
        let dir = tmpdir("hls-big");
        // 40 > SIZE_PROBE_SAMPLE(32) → 走"不探测"分支
        let count = 40usize;
        let names: Vec<String> = (1..=count).map(|i| format!("s{i}.ts")).collect();
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let seg_size = 16 * 1024usize;
        let mut files = HashMap::new();
        files.insert("/m/index.m3u8".to_string(), media_playlist(&refs).into_bytes());
        for (i, n) in names.iter().enumerate() {
            files.insert(format!("/m/{n}"), sample(i as u8 + 1, seg_size));
        }
        let srv = start_server(files, HashMap::new()).await;
        let expected_bytes = (count * seg_size) as u64;

        let client = crate::build_client();
        let cancel = CancellationToken::new();
        let opts = PlaylistOptions {
            concurrency: 4,
            ..Default::default()
        };
        let plan = fetch_plan(&client, &srv.url("/m/index.m3u8"), &cancel, &[], &opts)
            .await
            .expect("取清单");

        // 不探测：总长保持未知，界面由估算值驱动
        assert_eq!(plan.total, None, "大清单不应为了精确总长去逐个探测");
        assert_eq!(plan.segment_count(), count);
        assert!(
            (plan.duration_secs - (count as f64) * 4.0).abs() < 0.01,
            "应累计 #EXTINF 时长（估算的分母），实际 {}",
            plan.duration_secs
        );
        // 一个分片请求都不该发生
        for n in &names {
            assert_eq!(srv.hits(&format!("/m/{n}")), 0, "探测阶段不得请求分片 {n}");
        }

        let path = dir.join("out.ts");
        let stats = PlaylistStats::new(0);
        let done = download_playlist(&client, &path, &plan, &opts, &cancel, stats.clone())
            .await
            .expect("下载");
        assert_eq!(done.bytes, expected_bytes);

        // 下载完成后估算应收敛到真实总长（相等分片 → 误差极小）
        let est = stats.estimated_total.load(Ordering::Relaxed);
        let diff = (est as i64 - expected_bytes as i64).abs();
        assert!(
            diff * 100 <= (expected_bytes as i64) * 10,
            "估算总长 {est} 与真实 {expected_bytes} 相差超过 10%"
        );
        // 没有重试时"已接收"必须正好等于产物大小：多一个字节就是把丢弃的
        // 分片算进了进度（界面会虚高），少一个则是漏记（界面会停住）
        assert_eq!(
            stats.received.load(Ordering::Relaxed),
            expected_bytes,
            "已接收字节数应与产物大小一致"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn downloads_segments_in_order_with_known_total() {
        let dir = tmpdir("order");
        let segs: Vec<Vec<u8>> = (1..=4).map(|i| sample(i, 32 * 1024 + i as usize)).collect();
        let expected: Vec<u8> = segs.iter().flatten().copied().collect();
        let mut files = HashMap::new();
        files.insert("/m/index.m3u8".to_string(), media_playlist(&["s1.ts", "s2.ts", "s3.ts", "s4.ts"]).into_bytes());
        for (i, s) in segs.iter().enumerate() {
            files.insert(format!("/m/s{}.ts", i + 1), s.clone());
        }
        let srv = start_server(files, HashMap::new()).await;

        let client = crate::build_client();
        let cancel = CancellationToken::new();
        let opts = PlaylistOptions {
            concurrency: 4,
            ..Default::default()
        };
        let plan = fetch_plan(&client, &srv.url("/m/index.m3u8"), &cancel, &[], &opts)
            .await
            .expect("取清单");
        // 预探测拿到精确总长（进度条可用的前提）
        assert_eq!(plan.total, Some(expected.len() as u64));
        assert_eq!(plan.segment_count(), 4);
        assert!(!plan.fmp4);

        let path = dir.join("out.ts");
        let stats = PlaylistStats::new(0);
        let done = download_playlist(&client, &path, &plan, &opts, &cancel, stats.clone())
            .await
            .expect("下载");
        assert_eq!(done.bytes, expected.len() as u64);
        assert_eq!(stats.completed.load(Ordering::Relaxed), expected.len() as u64);
        assert_eq!(std::fs::read(&path).unwrap(), expected, "分片必须按清单顺序拼接");
        // 完成后控制文件删除
        assert!(!xfer_storage::ctrl_path(&path).exists());
    }

    /// 单个分片瞬时失败 → 任务失败但**保留连续前缀**；再次下载从该处续传，
    /// 前缀分片不会被重新请求。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn resumes_from_durable_prefix_after_failure() {
        let dir = tmpdir("resume");
        let segs: Vec<Vec<u8>> = (1..=4).map(|i| sample(i, 24 * 1024)).collect();
        let expected: Vec<u8> = segs.iter().flatten().copied().collect();
        let mut files = HashMap::new();
        files.insert("/m/index.m3u8".to_string(), media_playlist(&["s1.ts", "s2.ts", "s3.ts", "s4.ts"]).into_bytes());
        for (i, s) in segs.iter().enumerate() {
            files.insert(format!("/m/s{}.ts", i + 1), s.clone());
        }
        // 第 3 段前 3 次请求失败（= 默认重试预算），第 4 次起正常
        let mut fail = HashMap::new();
        fail.insert("/m/s3.ts".to_string(), 3usize);
        let srv = start_server(files, fail).await;

        let client = crate::build_client();
        let cancel = CancellationToken::new();
        // 并发 1：分片按序在飞，失败点之后的分片不会被提前请求。
        // 关闭大小预探测：预探测会替下载先消耗掉注入的失败次数
        let opts = PlaylistOptions {
            concurrency: 1,
            probe_sizes: false,
            // 这些用例断言的正是**顺序落盘**的语义（控制文件里前缀 + part、
            // 进度只认光标那一段），显式选它
            ordered_write: true,
            ..Default::default()
        };
        let plan = fetch_plan(&client, &srv.url("/m/index.m3u8"), &cancel, &[], &opts)
            .await
            .expect("取清单");
        let path = dir.join("out.ts");

        let err = download_playlist(&client, &path, &plan, &opts, &cancel, PlaylistStats::new(0))
            .await
            .expect_err("第 3 段失败应上报");
        assert!(matches!(err, HttpError::Http(500)), "实际: {err:?}");
        let prefix_bytes = (segs[0].len() + segs[1].len()) as u64;
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            prefix_bytes,
            "失败时必须保留已 fsync 的连续前缀"
        );
        assert!(xfer_storage::ctrl_path(&path).exists(), "控制文件应保留");

        // 续传：前缀分片不再请求
        let done = download_playlist(&client, &path, &plan, &opts, &cancel, PlaylistStats::new(0))
            .await
            .expect("续传应成功");
        assert_eq!(done.bytes, expected.len() as u64);
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        assert_eq!(srv.hits("/m/s1.ts"), 1, "已持久化的分片不应重下");
        assert_eq!(srv.hits("/m/s2.ts"), 1, "已持久化的分片不应重下");
        assert!(!xfer_storage::ctrl_path(&path).exists());
    }

    /// 清单变化（直播滑窗）→ 指纹不匹配 → 不得复用旧前缀。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn changed_playlist_invalidates_prefix() {
        let dir = tmpdir("fingerprint");
        let segs: Vec<Vec<u8>> = (1..=2).map(|i| sample(i, 8 * 1024)).collect();
        let mut files = HashMap::new();
        files.insert("/m/index.m3u8".to_string(), media_playlist(&["a.ts"]).into_bytes());
        files.insert("/m/a.ts".to_string(), segs[0].clone());
        files.insert("/m/b.ts".to_string(), segs[1].clone());
        let srv = start_server(files, HashMap::new()).await;

        let client = crate::build_client();
        let cancel = CancellationToken::new();
        let opts = PlaylistOptions::default();
        let path = dir.join("out.ts");
        // 第一次：清单只有 a.ts 且完整下载 → 完成后无控制文件
        let plan_a = fetch_plan(&client, &srv.url("/m/index.m3u8"), &cancel, &[], &opts).await.unwrap();
        assert_eq!(plan_a.segment_count(), 1);
        download_playlist(&client, &path, &plan_a, &opts, &cancel, PlaylistStats::new(0))
            .await
            .unwrap();
        // 伪造一份"另一条清单"的续传水位：必须被判为不匹配
        let plan_b = PlaylistPlan {
            segments: vec![
                plan_a.segments[0].clone(),
                Segment { url: srv.url("/m/b.ts"), range: None, key: None, size: None, duration: 4.0 },
            ],
            ..plan_a.clone()
        };
        assert_eq!(resume_point(&path, &plan_b), (0, 0));
    }

    /// AES-128 加密分片：按 IV 解密后与明文一致（含 PKCS7 去填充）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn decrypts_aes128_segments() {
        use aes::cipher::{BlockEncryptMut, KeyIvInit};
        type Enc = cbc::Encryptor<aes::Aes128>;

        let dir = tmpdir("aes");
        let key = [9u8; 16];
        let plain: Vec<Vec<u8>> = (1..=3).map(|i| sample(i, 20 * 1024 + 7)).collect();
        let mut files = HashMap::new();
        files.insert("/m/index.m3u8".to_string(), {
            let mut s = String::from("#EXTM3U\n");
            s.push_str("#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\n");
            for i in 1..=3 {
                s.push_str(&format!("#EXTINF:4.0,\ns{i}.ts\n"));
            }
            s.push_str("#EXT-X-ENDLIST\n");
            s.into_bytes()
        });
        files.insert("/m/key.bin".to_string(), key.to_vec());
        for (i, p) in plain.iter().enumerate() {
            // IV 缺省 = 媒体序号（第 i 段序号 i）
            let iv = iv_from_sequence(i as u64);
            let mut buf = vec![0u8; p.len() + 16];
            buf[..p.len()].copy_from_slice(p);
            let enc = Enc::new(&key.into(), &iv.into());
            let ct = enc
                .encrypt_padded_mut::<aes::cipher::block_padding::Pkcs7>(&mut buf, p.len())
                .unwrap()
                .to_vec();
            files.insert(format!("/m/s{}.ts", i + 1), ct);
        }
        let srv = start_server(files, HashMap::new()).await;

        let client = crate::build_client();
        let cancel = CancellationToken::new();
        let opts = PlaylistOptions::default();
        let plan = fetch_plan(&client, &srv.url("/m/index.m3u8"), &cancel, &[], &opts)
            .await
            .expect("取清单");
        // 加密分片长度与明文不同 → 预探测出的总长是密文总长（进度够用）
        assert!(plan.segments.iter().all(|s| s.key.is_some()));
        let path = dir.join("out.ts");
        let done = download_playlist(&client, &path, &plan, &opts, &cancel, PlaylistStats::new(0))
            .await
            .expect("下载");
        let expected: Vec<u8> = plain.iter().flatten().copied().collect();
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        assert_eq!(done.bytes, expected.len() as u64);
    }

    /// `#EXT-X-BYTERANGE`：同一文件的不同区间按清单顺序拼出来。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn downloads_byterange_segments() {
        let dir = tmpdir("range");
        let blob = sample(3, 48 * 1024);
        let (a, b, c) = (8 * 1024, 12 * 1024, 6 * 1024);
        let mut files = HashMap::new();
        files.insert(
            "/m/index.m3u8".to_string(),
            format!(
                "#EXTM3U\n#EXT-X-BYTERANGE:{a}@0\n#EXTINF:4.0,\nall.ts\n\
                 #EXT-X-BYTERANGE:{b}\n#EXTINF:4.0,\nall.ts\n\
                 #EXT-X-BYTERANGE:{c}\n#EXTINF:4.0,\nall.ts\n#EXT-X-ENDLIST\n"
            )
            .into_bytes(),
        );
        files.insert("/m/all.ts".to_string(), blob.clone());
        let srv = start_server(files, HashMap::new()).await;

        let client = crate::build_client();
        let cancel = CancellationToken::new();
        let opts = PlaylistOptions::default();
        let plan = fetch_plan(&client, &srv.url("/m/index.m3u8"), &cancel, &[], &opts)
            .await
            .expect("取清单");
        assert_eq!(
            plan.segments.iter().map(|s| s.range).collect::<Vec<_>>(),
            vec![Some((0, a as u64)), Some((a as u64, b as u64)), Some(((a + b) as u64, c as u64))]
        );
        // BYTERANGE 直接给出大小 → 无需探测即知总长
        assert_eq!(plan.total, Some((a + b + c) as u64));
        let path = dir.join("out.ts");
        download_playlist(&client, &path, &plan, &opts, &cancel, PlaylistStats::new(0))
            .await
            .expect("下载");
        assert_eq!(std::fs::read(&path).unwrap(), blob[..a + b + c]);
        assert_eq!(srv.hits("/m/all.ts"), 3, "每个区间各一次带 Range 的请求");
    }

    /// fMP4：`#EXT-X-MAP` 必须排在所有分片之前。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn prepends_init_segment() {
        let dir = tmpdir("fmp4");
        let init = sample(10, 1024);
        let segs: Vec<Vec<u8>> = (1..=2).map(|i| sample(i, 4 * 1024)).collect();
        let mut files = HashMap::new();
        files.insert(
            "/m/index.m3u8".to_string(),
            b"#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:4.0,\ns1.m4s\n#EXTINF:4.0,\ns2.m4s\n#EXT-X-ENDLIST\n".to_vec(),
        );
        files.insert("/m/init.mp4".to_string(), init.clone());
        files.insert("/m/s1.m4s".to_string(), segs[0].clone());
        files.insert("/m/s2.m4s".to_string(), segs[1].clone());
        let srv = start_server(files, HashMap::new()).await;

        let client = crate::build_client();
        let cancel = CancellationToken::new();
        let opts = PlaylistOptions::default();
        let plan = fetch_plan(&client, &srv.url("/m/index.m3u8"), &cancel, &[], &opts)
            .await
            .expect("取清单");
        assert!(plan.fmp4, "含 EXT-X-MAP 应判定为 fMP4（产物扩展名 .mp4）");
        assert_eq!(plan.segment_count(), 3);
        let path = dir.join("out.mp4");
        let done = download_playlist(&client, &path, &plan, &opts, &cancel, PlaylistStats::new(0))
            .await
            .expect("下载");
        let mut expected = init.clone();
        expected.extend_from_slice(&segs[0]);
        expected.extend_from_slice(&segs[1]);
        assert_eq!(std::fs::read(&path).unwrap(), expected);
        assert_eq!(done.segments, 3);
    }

    /// 分片字节计数的 Drop 兜底：没走到"整段到手"就必须扣回去。
    ///
    /// 只清 `Err` 分支的写法漏掉 future 被 drop 的路径（暂停、提前收尾），
    /// 槽位与"已接收"会永久虚高（进度/速度再也对不上真实值）。
    #[test]
    fn seg_count_guard_reverts_bytes_on_drop() {
        let slot = AtomicU64::new(0);
        let total = AtomicU64::new(100);
        let mut c = SegCount::new(&total, None, Some(&slot), true);
        c.add(64);
        assert_eq!(slot.load(Ordering::Relaxed), 64);
        assert_eq!(total.load(Ordering::Relaxed), 164);
        drop(c);
        assert_eq!(slot.load(Ordering::Relaxed), 0, "被丢弃的分片槽位必须归零");
        assert_eq!(total.load(Ordering::Relaxed), 100, "被丢弃的分片不得留在已接收里");

        // 整段到手（keep）后不再扣
        let mut c2 = SegCount::new(&total, None, Some(&slot), true);
        c2.add(32);
        c2.keep();
        drop(c2);
        assert_eq!(slot.load(Ordering::Relaxed), 32);
        assert_eq!(total.load(Ordering::Relaxed), 132);
    }

    /// 进度 = 已落盘 + **当前待写段**的在飞字节；其它在飞段一律不算。
    ///
    /// 这条口径是三个现场问题的共同解：① 单连接被限速的站点上一个分片要
    /// 几十秒，只按"分片落盘"跳会让界面几十秒不动；② 只算落盘字节时数字
    /// 会落后于网络实际进展；③ 把**所有**在飞字节算进去又是虚报（暂停即丢）。
    #[test]
    fn progress_counts_only_the_pending_segment() {
        let stats = PlaylistStats::new(1000);
        // cursor 处（第 0 段）收了 512，其它段收得再多也不算
        stats.slot(0).store(512, Ordering::Relaxed);
        stats.slot(1).store(4096, Ordering::Relaxed);
        stats.slot(2).store(4096, Ordering::Relaxed);
        assert_eq!(stats.progress(), 1512, "只该算 `completed` + 光标那一段");
        assert_eq!(
            stats.received.load(Ordering::Relaxed),
            1000,
            "received 不受槽位影响"
        );

        // 写完第 0 段：completed 前移 + 光标前移（槽位清零），进度只前进
        stats.set_cursor(1, true);
        stats.slot(0).store(0, Ordering::Relaxed);
        stats.completed.store(1000 + 8192, Ordering::Relaxed);
        assert_eq!(stats.progress(), 1000 + 8192 + 4096);

        // 加密段（不做段内续传）：光标处的字节不算，退化成"按分片跳"
        stats.set_cursor(2, false);
        assert_eq!(
            stats.progress(),
            1000 + 8192,
            "加密分片取消时会整段丢弃，不能算进进度"
        );
    }

    /// 队头慢分片：**后续分片必须继续下载**（连接不得空转），
    /// 且进度不得把"还在内存里、暂停就被丢掉"的字节算成已下载。
    ///
    /// 老实现（`buffered(conn)`）把"下好但排在慢分片之后"的分片一直留在
    /// 在飞队列里：队头一慢，其余连接全部空转，同时这些字节被当进度上报
    /// —— 界面能显示 40MB、一暂停回落成磁盘上的 1.4MB。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn slow_head_does_not_stall_pipeline_or_inflate_progress() {
        let dir = tmpdir("hls-head");
        let count = 12usize;
        let seg_size = 16 * 1024usize;
        let names: Vec<String> = (0..count).map(|i| format!("s{i}.ts")).collect();
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let mut files = HashMap::new();
        files.insert(
            "/m/index.m3u8".to_string(),
            media_playlist(&refs).into_bytes(),
        );
        for (i, n) in names.iter().enumerate() {
            files.insert(format!("/m/{n}"), sample(i as u8 + 1, seg_size));
        }
        // 队头（第 0 片）1.5s 后才给响应，其余分片正常
        let mut delays = HashMap::new();
        delays.insert("/m/s0.ts".to_string(), 1500);
        let srv = start_server_with_delay(files, HashMap::new(), delays).await;

        let client = crate::build_client();
        let cancel = CancellationToken::new();
        // 关闭预探测：否则 hits 计数会混进探测请求
        let opts = PlaylistOptions {
            concurrency: 4,
            probe_sizes: false,
            // 「重排窗口不空转 + 进度不虚高」是**顺序落盘**的两个特性
            ordered_write: true,
            ..Default::default()
        };
        let plan = fetch_plan(&client, &srv.url("/m/index.m3u8"), &cancel, &[], &opts)
            .await
            .expect("取清单");
        let path = dir.join("out.ts");
        let stats = PlaylistStats::new(0);
        let dl = tokio::spawn({
            let client = client.clone();
            let path = path.clone();
            let plan = plan.clone();
            let opts = opts.clone();
            let cancel = cancel.clone();
            let stats = stats.clone();
            async move { download_playlist(&client, &path, &plan, &opts, &cancel, stats).await }
        });

        // 队头开始下载后再给它 0.7s：这段时间里后面的分片早就该下完了
        let mut waited = 0u64;
        while srv.hits("/m/s0.ts") == 0 && waited < 3000 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            waited += 20;
        }
        tokio::time::sleep(Duration::from_millis(700)).await;

        assert_eq!(
            std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0),
            0,
            "队头未完成前不应有内容落盘"
        );
        let received = stats.received.load(Ordering::Relaxed);
        assert!(
            received >= 8 * seg_size as u64,
            "队头慢时后续分片应已下好等在内存里（received={received}）"
        );
        // 队头一个字节都没给、也没写下任何东西 → 进度必须是 0：
        // 那些"已下好等在内存里"的分片会被暂停整段丢掉，进度不能为它们背书
        assert_eq!(
            stats.progress(),
            0,
            "进度把在飞/待写的字节算进去了（进度一旦回落，界面就是「虚报」）"
        );
        assert!(
            srv.hits("/m/s11.ts") >= 1,
            "队头慢时后续分片仍应被派发（老实现里连接会空转）"
        );

        let done = dl.await.unwrap().expect("下载");
        assert_eq!(done.bytes, (count * seg_size) as u64);
        let expected: Vec<u8> = (0..count)
            .flat_map(|i| sample(i as u8 + 1, seg_size))
            .collect();
        assert_eq!(std::fs::read(&path).unwrap(), expected, "分片必须按清单顺序拼接");
    }
    /// 造一个"下到第 2 段中途被取消"的现场：返回 (服务端, 输出路径, 清单, 取消时落盘的长度)。
    async fn cancel_mid_second_segment(
        tag: &str,
        opts: ServerOpts,
    ) -> (TestServer, PathBuf, PlaylistPlan, u64) {
        // 每个用例各自的目录：`tmpdir` 会删同名的旧目录，两个用例并行跑时
        // 共用目录会互相清掉对方的产物与控制文件（表现为间歇性"续传没生效"）
        let dir = tmpdir(tag);
        let seg_len = 64 * 1024usize;
        let segs: Vec<Vec<u8>> = (1..=4).map(|i| sample(i, seg_len)).collect();
        let mut files = HashMap::new();
        files.insert(
            "/m/index.m3u8".to_string(),
            media_playlist(&["s1.ts", "s2.ts", "s3.ts", "s4.ts"]).into_bytes(),
        );
        for (i, s) in segs.iter().enumerate() {
            files.insert(format!("/m/s{}.ts", i + 1), s.clone());
        }
        let srv = start_server_opts(files, opts).await;

        let client = crate::build_client();
        let cancel = CancellationToken::new();
        // 并发 1：严格按序，取消时"下一待写段"就是 s2
        let po = PlaylistOptions {
            concurrency: 1,
            probe_sizes: false,
            // 这些用例断言的正是**顺序落盘**的语义（控制文件里前缀 + part、
            // 进度只认光标那一段），显式选它
            ordered_write: true,
            ..Default::default()
        };
        let plan = fetch_plan(&client, &srv.url("/m/index.m3u8"), &cancel, &[], &po)
            .await
            .expect("取清单");
        let path = dir.join("out.ts");
        let dl = tokio::spawn({
            let client = client.clone();
            let path = path.clone();
            let plan = plan.clone();
            let po = po.clone();
            let cancel = cancel.clone();
            async move {
                download_playlist(&client, &path, &plan, &po, &cancel, PlaylistStats::new(0)).await
            }
        });

        // 等 s2 真的开始发（trickle 已经在往缓冲写），再给它 150ms
        let mut waited = 0u64;
        while srv.hits("/m/s2.ts") == 0 && waited < 5000 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            waited += 20;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
        cancel.cancel();
        let err = dl.await.unwrap().expect_err("取消应上报");
        assert!(matches!(err, HttpError::Cancelled), "实际: {err:?}");
        let file_len = std::fs::metadata(&path).unwrap().len();
        (srv, path, plan, file_len)
    }

    /// **段内续传**：取消时把"下一待写段"已经收到的那截留在文件里，
    /// 恢复时对这一段发 `Range: bytes=part-` 接着下，而不是整段重来。
    ///
    /// 旧行为：在飞分片的字节整段丢弃，恢复时重下 —— 一次暂停能白下几十 MB。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn resumes_within_segment_using_range() {
        let seg_len = 64 * 1024u64;
        let mut opts = ServerOpts::default();
        // s2 慢发：6KB 一块、每块 60ms → 整段约 640ms，够在中途取消
        opts.trickle.insert("/m/s2.ts".to_string(), (6 * 1024, 60));
        let (srv, path, plan, file_len) =
            cancel_mid_second_segment("hls-partial-range", opts).await;

        // 取消后：第 1 段完整落盘 + 第 2 段的半截
        assert!(file_len > seg_len, "取消时第 1 段应已落盘：{file_len}");
        let part = file_len - seg_len;
        assert!(
            part > 0 && part < seg_len,
            "取消时应保留第 2 段的半截：已落 {part} 字节"
        );
        let ctrl: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(xfer_storage::ctrl_path(&path)).unwrap())
                .unwrap();
        assert_eq!(ctrl["prefix"], 1, "第 1 段是完整前缀");
        assert_eq!(ctrl["bytes"], seg_len, "完整前缀的字节数");
        assert_eq!(ctrl["part"], part, "段内续传位置应记进控制文件");
        assert_eq!(
            resume_point(&path, &plan),
            (file_len, 1),
            "续传水位应等于磁盘真实长度（含段内那截）"
        );

        // 恢复：只补下第 2 段剩下的部分
        let client = crate::build_client();
        let po = PlaylistOptions {
            concurrency: 1,
            probe_sizes: false,
            // 这些用例断言的正是**顺序落盘**的语义（控制文件里前缀 + part、
            // 进度只认光标那一段），显式选它
            ordered_write: true,
            ..Default::default()
        };
        let done = download_playlist(
            &client,
            &path,
            &plan,
            &po,
            &CancellationToken::new(),
            PlaylistStats::new(file_len),
        )
        .await
        .expect("续传应成功");
        let expected: Vec<u8> = (1..=4).flat_map(|i| sample(i as u8, seg_len as usize)).collect();
        assert_eq!(done.bytes, expected.len() as u64);
        assert_eq!(
            std::fs::read(&path).unwrap(),
            expected,
            "段内续传必须严丝合缝（多写一遍开头就会错位）"
        );
        // 探测（`bytes=0-0`）不算真实抓取，滤掉后应该只有"首次 + 续传"两次
        let rs: Vec<Option<String>> = srv
            .ranges("/m/s2.ts")
            .into_iter()
            .filter(|r| r.as_deref() != Some("bytes=0-0"))
            .collect();
        assert_eq!(
            rs.len(),
            2,
            "s2 只应有两次真实请求（首次 + 续传，不该整段重下）：{rs:?}"
        );
        assert_eq!(
            rs.last().cloned().flatten(),
            Some(format!("bytes={part}-")),
            "续传请求必须从段内续传位置接着下：{rs:?}"
        );
    }

    /// 服务器只认"0 起点"的 `Range`（CDN 线上真实怪癖）：探测（`bytes=0-0`）
    /// 说支持，真正的续传请求却被回了整段 —— 必须丢掉文件里那截续传数据、
    /// 按整段重写，否则同一段的前半会被写两遍。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn resume_drops_partial_when_server_ignores_range() {
        let seg_len = 64 * 1024u64;
        let mut opts = ServerOpts::default();
        opts.trickle.insert("/m/s2.ts".to_string(), (6 * 1024, 60));
        opts.range_only_from_zero.insert("/m/s2.ts".to_string());
        let (srv, path, plan, file_len) =
            cancel_mid_second_segment("hls-partial-norange", opts).await;
        assert!(file_len > seg_len, "取消时应留下第 2 段的半截：{file_len}");

        let client = crate::build_client();
        let po = PlaylistOptions {
            concurrency: 1,
            probe_sizes: false,
            // 这些用例断言的正是**顺序落盘**的语义（控制文件里前缀 + part、
            // 进度只认光标那一段），显式选它
            ordered_write: true,
            ..Default::default()
        };
        let done = download_playlist(
            &client,
            &path,
            &plan,
            &po,
            &CancellationToken::new(),
            PlaylistStats::new(file_len),
        )
        .await
        .expect("续传应成功（回退成整段重下）");
        let expected: Vec<u8> = (1..=4).flat_map(|i| sample(i as u8, seg_len as usize)).collect();
        assert_eq!(done.bytes, expected.len() as u64);
        assert_eq!(
            std::fs::read(&path).unwrap(),
            expected,
            "服务器无视 Range 时必须丢掉半截重写，不能重复写开头"
        );
        assert!(
            srv.ranges("/m/s2.ts").len() >= 2,
            "续传仍应发出带 Range 的请求（被服务器无视）"
        );
    }
    /// **下载一开始就有总长**：主清单的 `BANDWIDTH × 总时长`。
    ///
    /// 大清单不做分片预探测，实测外推又要等第一个分片落盘 —— 单连接被限速的
    /// 站点上一个 1.5MB 的分片要几十秒，那段时间里界面连「总大小」都显示不
    /// 出来（用户现场：已经在下、左下角却既没有大小也没有百分比）。这条用例
    /// 钉住：`estimated_total` 在**第一个分片落盘之前**就已经是像样的值。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn master_bandwidth_yields_total_before_first_segment() {
        let dir = tmpdir("hls-bitrate");
        // 40 > SIZE_PROBE_SAMPLE(32) → 不预探测，总长只能靠估算
        let count = 40usize;
        let seg_size = 16 * 1024usize;
        let names: Vec<String> = (1..=count).map(|i| format!("s{i}.ts")).collect();
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let mut files = HashMap::new();
        files.insert(
            "/m/master.m3u8".to_string(),
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=1280x720\nv1.m3u8\n"
                .as_bytes()
                .to_vec(),
        );
        files.insert("/m/v1.m3u8".to_string(), media_playlist(&refs).into_bytes());
        for (i, n) in names.iter().enumerate() {
            files.insert(format!("/m/{n}"), sample(i as u8 + 1, seg_size));
        }
        // 首片慢发，保证观察时它还没落盘
        let mut sopts = ServerOpts::default();
        sopts.trickle.insert("/m/s1.ts".to_string(), (4 * 1024, 150));
        let srv = start_server_opts(files, sopts).await;

        let client = crate::build_client();
        let cancel = CancellationToken::new();
        let po = PlaylistOptions {
            concurrency: 2,
            ..Default::default()
        };
        let plan = fetch_plan(&client, &srv.url("/m/master.m3u8"), &cancel, &[], &po)
            .await
            .expect("取清单");
        assert_eq!(plan.bitrate, Some(800_000), "应记下选中变体的码率");
        let expect_est = 800_000u64 / 8 * (count as u64) * 4; // 每段 4 秒

        let path = dir.join("out.ts");
        let stats = PlaylistStats::new(0);
        let dl = tokio::spawn({
            let client = client.clone();
            let path = path.clone();
            let plan = plan.clone();
            let po = po.clone();
            let cancel = cancel.clone();
            let stats = stats.clone();
            async move { download_playlist(&client, &path, &plan, &po, &cancel, stats).await }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            stats.completed.load(Ordering::Relaxed),
            0,
            "此刻首片应还没落盘（否则这条用例没测到「一开始就有总长」）"
        );
        let est = stats.estimated_total.load(Ordering::Relaxed);
        assert!(
            est > 0,
            "下载刚开始就该有个总长给界面（BANDWIDTH × 时长），实际 {est}"
        );
        let diff = (est as i64 - expect_est as i64).abs();
        assert!(
            diff * 100 <= expect_est as i64 * 20,
            "码率估算 {est} 偏离 {expect_est} 超过 20%"
        );
        cancel.cancel();
        let _ = dl.await;
    }
}
