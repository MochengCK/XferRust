//! HTTP-FLV 直播录制：把一条**永不结束**的 FLV over HTTP 响应持续录成产物。
//!
//! HTTP-FLV 直播的传输形态是"一个 chunked 长连接"：服务器不声明
//! `Content-Length`、也永远不会主动收尾。按普通 HTTP 下载会得到一条
//! 永不完成的任务（进度条永远在跑、任务永远不结束）；这里按"录制"处理：
//!
//! - 持续把响应字节落盘，逐 tag 解析出**时间戳**（[`FlvStream`]）；
//! - 时间轴**实时归零 + 断口闭合**：每一段连接内的时间戳统一平移，
//!   段与段首尾相接 —— 产物从 0 开始、连续可播，暂停/断线的时间不进
//!   产物（"已录时长" = 产物能播多久，不是"从开始录制过去了多久"）；
//! - 断线自动重连续录（EOF、读静默、连接错误都只当作一次尝试失败）；
//! - 收尾条件：连续一段时间没有新数据（[`FlvOptions::stall_timeout`]）、
//!   地址持续 404/410（直播已下线）、有限资源读到 EOF，或用户暂停/删除；
//! - 断点现场（控制文件）：产物水位（一定是**完整 tag 的末尾**）+
//!   已录时长；暂停 / 失败都保留，恢复时截断到水位继续录。
//!
//! 与 `playlist::record_playlist`（HLS 直播录制）是同一套设计语言：产物
//! 时间轴归零、断点续录、停滞收尾。两处的差别只在"流怎么读进来"：
//! HLS 是反复拉清单 + 下分片，FLV 是一条连接一直读。

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::rate::RateLimiter;
use crate::{apply_headers, filename_from_url, HttpError, RequestHeaders};

/// 控制文件种类标记（与分片下载 / HLS 的控制文件区分开）。
const CTRL_KIND: &str = "http-flv";
/// 直播录制现场的模式标记。
const CTRL_MODE_LIVE: &str = "live";
/// 控制文件落盘节流（与分片下载 / HLS 一致：1s 一次原子写）。
const CTRL_SAVE_INTERVAL: Duration = Duration::from_secs(1);
/// 连接建立后首字节的等待上限（建连 / TLS / 服务端首包）。
const FIRST_BYTE_TIMEOUT: Duration = Duration::from_secs(15);
/// 读静默阈值：连续这么久没有收到任何字节，断开重连。
///
/// 直播流每秒都有多个 tag 在流动，静默这么久基本可以断定这条连接
/// 已经不干活了；断开重连比干等（客户端全局 read_timeout 是 30s）灵敏。
const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(5);
/// 重连退避：0.5s 起步翻倍，上限 8s。
const RECONNECT_BACKOFF_MS: u64 = 500;
const RECONNECT_BACKOFF_MAX_SHIFT: u32 = 4;
/// 连续这么多次 404 / 410 视为直播已经下线，按录制结束收尾。
const GONE_LIMIT: u32 = 6;
/// 连续这么多次"连上了却一个字节都没收到"（200 + 立即 EOF）视为流已结束。
const EMPTY_LIMIT: u32 = 3;
/// 404/410 之间的检查间隔（比常规重连退避快，下线收尾要利落）。
const GONE_RETRY_DELAY: Duration = Duration::from_millis(500);

/// FLV 直播录制的进度统计（无锁原子，与 [`crate::PlaylistStats`] 同用
/// 一套采样方式：采样器 200ms 一跳读进任务的原子计数器）。
pub struct FlvStats {
    /// 已录字节（产物文件长度；只记**完整 tag 的末尾**，暂停/恢复时
    /// 截断到它不会有半截 tag）。
    pub completed: AtomicU64,
    /// 已从网络收到的字节（含还没凑成完整 tag 的部分，供速度显示）。
    pub received: AtomicU64,
    /// 当前是否有连接在收数据（0/1）。
    pub connections: AtomicUsize,
    /// 已录媒体时长（毫秒）= 产物连续时间轴的长度（能播多久）。
    pub live_ms: AtomicU64,
}

impl FlvStats {
    /// `baseline` = 续录起点（产物已有字节数），进度/速度都从这里接着数。
    pub fn new(baseline: u64) -> Arc<Self> {
        Arc::new(Self {
            completed: AtomicU64::new(baseline),
            received: AtomicU64::new(baseline),
            connections: AtomicUsize::new(0),
            live_ms: AtomicU64::new(0),
        })
    }

    /// 当前进度（已落盘可保住的字节数）。
    pub fn progress(&self) -> u64 {
        self.completed.load(Ordering::Relaxed)
    }

    /// 已录媒体时长（毫秒）。
    pub fn live_recorded_ms(&self) -> u64 {
        self.live_ms.load(Ordering::Relaxed)
    }
}

/// FLV 录制选项。
pub struct FlvOptions {
    /// 逐任务自定义请求头（与探测用的是同一组）。
    pub headers: RequestHeaders,
    /// 全局限速器（None / rate 0 = 不限）。
    pub limiter: Option<Arc<RateLimiter>>,
    /// 停滞判定：连续这么久没有任何新数据就按"流已结束"收尾。
    /// `Duration::ZERO` = 不按停滞自动收尾（一直录到下线 / 用户暂停）。
    pub stall_timeout: Duration,
}

/// 一次录制的产物信息。
#[derive(Debug, Clone, Copy)]
pub struct FlvDone {
    /// 产物字节数。
    pub bytes: u64,
    /// 已录媒体时长（毫秒）。
    pub live_ms: u64,
}

/// 产物旁边的**直播录制**控制文件是否存在。
///
/// 引擎用它决定"暂停后恢复 / 重启恢复"的任务继续走录制路径，而不是
/// 因为"服务器这次带了 Content-Length"之类的外在变化退化成普通下载。
pub fn has_live_ctrl(path: &Path) -> bool {
    load_ctrl(path).is_some()
}

/// 录制的续录水位：`(已落盘字节, 已录媒体时长毫秒)`。
///
/// 与 [`record_flv`] 内部的初始化**共用同一份判定**（引擎侧进度基线
/// 与录制器起点必须逐字节一致，否则恢复第一帧就会倒退）。
pub fn live_resume_point(path: &Path, url: &str) -> Option<(u64, u64)> {
    let c = load_ctrl(path)?;
    if !ctrl_matches(&c, url) {
        return None;
    }
    let disk_len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    if disk_len < c.bytes {
        // 产物比控制文件短：现场被动过 → 整份丢掉重来
        return None;
    }
    Some((c.bytes, c.live_ms))
}

/// 默认产物文件名：URL 末段，扩展名归一为 `.flv`。
///
/// 内容的容器是确定的（FLV），名字就该说真话 —— `stream` 补成
/// `stream.flv`，`live.php?token=…` 这类直出接口也归成 `.flv`。
pub fn default_filename(url: &str) -> String {
    let raw = filename_from_url(url).unwrap_or_default();
    let stem = match raw.rsplit_once('.') {
        Some((s, _)) if !s.trim().is_empty() => s.to_string(),
        _ => raw,
    };
    let stem = stem.trim();
    if stem.is_empty() {
        "live.flv".to_string()
    } else {
        format!("{stem}.flv")
    }
}

// ---------------------------------------------------------------------------
// 控制文件
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct FlvCtrl {
    /// 标记文件种类，避免误读分片 / HLS 的控制文件
    kind: String,
    v: u32,
    /// 流地址（稳定形态比对：令牌/签名轮换不影响"还是这条流"的判定）
    url: String,
    /// 产物水位（字节数；一定是**完整 tag 的末尾**）
    bytes: u64,
    /// 已录媒体时长（毫秒）= 产物连续时间轴的长度
    live_ms: u64,
    /// `"live"`（直播录制的现场）
    mode: String,
    /// 录制第一个媒体 tag 的原始前缀（hex），重播检测的参照跨进程续用
    #[serde(default)]
    probe: String,
}

/// 控制文件路径 → 现场；文件损坏 / 种类不符的返回 None。
///
/// 入参是**产物路径**（控制文件路径由它派生）。
fn load_ctrl(path: &Path) -> Option<FlvCtrl> {
    let raw = std::fs::read_to_string(xfer_storage::ctrl_path(path)).ok()?;
    let c: FlvCtrl = serde_json::from_str(&raw).ok()?;
    if c.kind != CTRL_KIND || c.v != 1 || c.mode != CTRL_MODE_LIVE {
        return None;
    }
    Some(c)
}

fn save_ctrl(path: &Path, c: &FlvCtrl) {
    let ctrl = xfer_storage::ctrl_path(path);
    let Some(parent) = ctrl.parent() else { return };
    if std::fs::create_dir_all(parent).is_err() {
        return;
    }
    let Ok(json) = serde_json::to_vec(c) else { return };
    let tmp: PathBuf = {
        let mut s = ctrl.as_os_str().to_os_string();
        s.push(".tmp");
        PathBuf::from(s)
    };
    if std::fs::write(&tmp, &json).is_ok() {
        let _ = std::fs::rename(&tmp, &ctrl);
    }
}

/// 现场是不是同一条流：地址的**稳定形态**一致即可（直播地址的令牌、
/// 签名参数每轮都可能换，逐字比对必然失败）。
fn ctrl_matches(c: &FlvCtrl, url: &str) -> bool {
    !c.url.is_empty() && crate::playlist::stable_url(&c.url) == crate::playlist::stable_url(url)
}

// ---------------------------------------------------------------------------
// FLV 字节流
// ---------------------------------------------------------------------------

/// 重播检测的比对长度：录制的**第一个媒体 tag** 的原始前缀（含时间戳）。
///
/// 直播重连时服务端从"当前时刻"继续发，首个媒体 tag 与录制起点不可能
/// 逐字节相同；而"流结束后改为从头发同一份内容"（归档 / 有限资源被
/// 当成直播重连）会逐字节相同 —— 靠它把后者识别出来收尾，免得把
/// 同一份内容在产物里重复追加。
const REPLAY_PROBE_LEN: usize = 1024;

/// "段开头暂存"的字节上限：正常情况下只有文件头 + onMetaData（几百字节），
/// 超过它说明服务端在疯狂灌脚本 tag —— 直接照常落盘，不再等媒体 tag
/// （放弃这一段的头比对是安全的，重播检测只在"开头"有意义）。
const OPEN_HOLD_LIMIT: usize = 1024 * 1024;

/// FLV 字节流状态机：喂入网络块，吐出**可直接落盘**的完整结构
/// （文件头 + 完整 tag），并在吐出前完成时间戳映射。
///
/// 结构切分（增量、跨块）：
/// - 文件头 `FLV\x01` + dataOffset + `PreviousTagSize0`：只认一次，
///   **之后每次重连带的头一律跳过**（产物是一份连续文件，不能把
///   第二个文件头写进中间 —— 解析器会把它误读成 tag）；
/// - 每个 tag：11 字节头（含 4 字节时间戳）+ 数据 + 4 字节
///   `PreviousTagSize`。数据不解析，只走位。
///
/// 时间戳映射（直播录制的时间轴）：
/// - 每一次连接算一个「录制段」；段内所有 tag 的时间戳统一减去一个
///   位移，使本段在产物里**紧接着上一段的末尾**排（暂停、断线期间
///   在产物里是"没有时间"的，产物是一段连续可播的内容）；
/// - 第一段的位移 = 第一个**媒体** tag 的时间戳 → 产物从 0 开始
///   （"开播以来"的时间戳被清零，播放器显示的长度 = 实际录制长度）；
/// - 服务端换流重启（时间戳回退）也不会算出负数。
struct FlvStream {
    /// 未消费的原始字节（文件头 + 至多一个不完整的 tag）
    buf: Vec<u8>,
    /// 文件头已校验（后续按 tag 走）
    started: bool,
    /// 文件头是否已经写进产物（重连带的头要跳过）
    header_written: bool,
    /// 当前段的位移；None = 本段还没见到媒体 tag
    shift: Option<i64>,
    /// 已写出的最大（映射后）时间戳 = 产物的可播长度
    mapped_max: u64,
    /// 录制的第一个媒体 tag 的原始前缀（重播检测的参照；续录时从
    /// 控制文件恢复）
    begin_probe: Option<Vec<u8>>,
    /// 本段是否已经见过媒体 tag（重播检测只看段内第一个）
    seg_media_seen: bool,
    /// 本段像是对**产品开头**的重播（首个媒体 tag 逐字节相同）
    replay: bool,
}

impl FlvStream {
    /// `resume_ms` = 续录起点（现场记录的已录时长；新录为 0），
    /// `resume_bytes` = 产物已有字节（> 0 说明文件头已经在产物开头了）。
    fn new(resume_ms: u64, resume_bytes: u64, begin_probe: Option<Vec<u8>>) -> Self {
        Self {
            buf: Vec::new(),
            started: false,
            header_written: resume_bytes > 0,
            shift: None,
            mapped_max: resume_ms,
            begin_probe,
            seg_media_seen: false,
            replay: false,
        }
    }

    /// 一次连接开始：下一个媒体 tag 重新锚定到当前水位之后
    /// （断线/暂停的时间差从产物时间轴上剔除），并准备好重新校验
    /// 这条连接自己的文件头（**跳过头本身、不跳校验**：不是 FLV 的
    /// 响应仍要被识破）。上一条连接的残尾（半个 tag）直接丢弃 ——
    /// 它已经不可能凑齐了。
    fn begin_segment(&mut self) {
        self.buf.clear();
        self.started = false;
        self.shift = None;
        self.seg_media_seen = false;
        self.replay = false;
    }

    /// 喂入一块原始数据，把**完整结构**追加到 `out`（时间戳已映射）。
    ///
    /// 不完整的尾巴留在内部缓冲，等下一块；返回 `Err(NotFlv)` 表示
    /// 开头不是 FLV（调用方据此回退普通下载或当作一次失败尝试）。
    fn feed(&mut self, data: &[u8], out: &mut Vec<u8>) -> Result<(), HttpError> {
        self.buf.extend_from_slice(data);
        let mut pos = 0usize;
        loop {
            if !self.started {
                if self.buf.len() < 9 {
                    break;
                }
                if &self.buf[..3] != b"FLV" {
                    return Err(HttpError::NotFlv);
                }
                let off = u32::from_be_bytes([
                    self.buf[5],
                    self.buf[6],
                    self.buf[7],
                    self.buf[8],
                ]) as usize;
                // 常规 dataOffset = 9；留余量容忍带附加数据的头
                if !(9..=4096).contains(&off) {
                    return Err(HttpError::NotFlv);
                }
                if self.buf.len() < off + 4 {
                    break;
                }
                if !self.header_written {
                    out.extend_from_slice(&self.buf[..off + 4]);
                    self.header_written = true;
                }
                pos = off + 4;
                self.started = true;
                continue;
            }
            if self.buf.len() - pos < 11 {
                break;
            }
            let mut hdr = [0u8; 11];
            hdr.copy_from_slice(&self.buf[pos..pos + 11]);
            let size =
                ((hdr[1] as usize) << 16) | ((hdr[2] as usize) << 8) | (hdr[3] as usize);
            let end = pos + 11 + size + 4;
            if self.buf.len() < end {
                break;
            }
            let ts = ((hdr[4] as u32) << 16)
                | ((hdr[5] as u32) << 8)
                | (hdr[6] as u32)
                | ((hdr[7] as u32) << 24);
            // 重播检测：只认**本段第一个媒体 tag**（原始字节，打时间戳
            // 补丁之前）。第一次见到的记作参照；此后每段的首个媒体 tag
            // 与参照逐字节比对，相同就说明服务端在从头重发同一份内容
            if (hdr[0] == 8 || hdr[0] == 9) && !self.seg_media_seen {
                self.seg_media_seen = true;
                let cut = (pos + REPLAY_PROBE_LEN).min(end);
                if self.begin_probe.is_none() {
                    self.begin_probe = Some(self.buf[pos..cut].to_vec());
                } else {
                    let same_len = self
                        .begin_probe
                        .as_ref()
                        .is_some_and(|p| p.len() == cut - pos);
                    if same_len && self.begin_probe.as_deref() == Some(&self.buf[pos..cut]) {
                        self.replay = true;
                    }
                }
            }
            let mapped = self.map_ts(hdr[0], ts) as u32;
            // 时间戳就地改写（3 字节 + 扩展字节；长度不变，tag 结构不动）
            self.buf[pos + 4] = (mapped >> 16) as u8;
            self.buf[pos + 5] = (mapped >> 8) as u8;
            self.buf[pos + 6] = mapped as u8;
            self.buf[pos + 7] = (mapped >> 24) as u8;
            out.extend_from_slice(&self.buf[pos..end]);
            pos = end;
        }
        if pos > 0 {
            self.buf.drain(..pos);
        }
        Ok(())
    }

    /// 单个 tag 的时间戳映射（见类型文档）。
    fn map_ts(&mut self, tag_type: u8, ts: u32) -> u64 {
        let media = tag_type == 8 || tag_type == 9;
        if media && self.shift.is_none() {
            // 本段第一个媒体 tag：本段正好从上一段的末尾接排（第一段就是
            // 从 0 起；服务端时间戳回退时位移为负也照样成立）
            self.shift = Some(ts as i64 - self.mapped_max as i64);
        }
        let mapped = match self.shift {
            Some(s) => (ts as i64 - s).max(0) as u64,
            // 还没见到媒体 tag（如流头的 onMetaData 脚本 tag）：压在
            // 当前水位上，不把产物时间轴往回拉
            None => self.mapped_max,
        };
        if mapped > self.mapped_max {
            self.mapped_max = mapped;
        }
        mapped.min(u32::MAX as u64)
    }
}

// ---------------------------------------------------------------------------
// 录制
// ---------------------------------------------------------------------------

/// 一次尝试的结局。
enum Exit {
    /// 有限资源读到 EOF（响应带 Content-Length）：录完即结束
    Ended,
    /// 长时间没有新数据：按"流已结束"收尾
    Stalled,
    /// 地址持续 404/410 或连上后始终没有数据：直播已下线
    Gone,
    /// 用户取消（暂停/删除）：保留现场
    Cancelled,
}

/// 重连退避：0.5s 起步翻倍、上限 8s；取消立即返回。
async fn backoff(cancel: &CancellationToken, attempt: u32) -> Result<(), HttpError> {
    let shift = attempt.saturating_sub(1).min(RECONNECT_BACKOFF_MAX_SHIFT);
    let ms = RECONNECT_BACKOFF_MS.saturating_mul(1u64 << shift);
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(ms)) => Ok(()),
        _ = cancel.cancelled() => Err(HttpError::Cancelled),
    }
}

/// 限速取令牌 + 落盘 + 进度记账（录制循环的唯一写入口）。
async fn write_out(
    sink: &mut xfer_storage::FileSink,
    stats: &FlvStats,
    limiter: Option<&RateLimiter>,
    data: &[u8],
) -> Result<(), HttpError> {
    if let Some(l) = limiter {
        l.acquire(data.len()).await;
    }
    sink.write(data).map_err(|e| HttpError::Io(e.to_string()))?;
    stats.completed.store(sink.position(), Ordering::Relaxed);
    Ok(())
}

/// **HTTP-FLV 直播录制**：把一条长连接持续录成产物（设计见模块文档）。
///
/// `path` = 产物路径；`url` = 用户给出的地址（每轮重连走它）。
pub async fn record_flv(
    client: &reqwest::Client,
    path: &Path,
    url: &str,
    opts: &FlvOptions,
    cancel: &CancellationToken,
    stats: Arc<FlvStats>,
) -> Result<FlvDone, HttpError> {
    use xfer_storage::FileSink;

    // 续录现场：同一条流 + 产物不小于控制文件水位（与 live_resume_point 同判据）
    let resume = load_ctrl(path).filter(|c| ctrl_matches(c, url)).filter(|c| {
        let disk_len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        if disk_len >= c.bytes {
            true
        } else {
            tracing::warn!("FLV 录制现场与产物不一致，重新开始录制");
            false
        }
    });
    let (bytes0, ms0) = match &resume {
        Some(c) => (c.bytes, c.live_ms),
        None => (0, 0),
    };
    let mut sink = if let Some(c) = &resume {
        // 恢复：截断到水位（丢掉上次未 flush 的尾巴）
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .and_then(|f| f.set_len(c.bytes))
            .map_err(|e| HttpError::Io(e.to_string()))?;
        FileSink::append_at(path, c.bytes).map_err(|e| HttpError::Io(e.to_string()))?
    } else {
        FileSink::create(path).map_err(|e| HttpError::Io(e.to_string()))?
    };
    stats.completed.store(bytes0, Ordering::Relaxed);
    stats.received.store(bytes0, Ordering::Relaxed);
    stats.live_ms.store(ms0, Ordering::Relaxed);

    let begin_probe = resume.as_ref().and_then(|c| {
        if c.probe.is_empty() {
            None
        } else {
            hex::decode(&c.probe).ok()
        }
    });
    let mut parser = FlvStream::new(ms0, bytes0, begin_probe);
    let mut buf: Vec<u8> = Vec::with_capacity(256 * 1024);
    // 段开头暂存（文件头 / onMetaData）：确认本段不是"重播"后才落盘
    let mut pending_open: Vec<u8> = Vec::new();
    let mut last_data = Instant::now();
    let mut last_save = Instant::now() - CTRL_SAVE_INTERVAL;
    let mut dirty = false;
    let mut gone_streak = 0u32;
    let mut empty_streak = 0u32;
    let mut attempt = 0u32;
    let mut last_err: Option<HttpError> = None;

    let ctrl_of = |bytes: u64, live_ms: u64, probe: Option<&Vec<u8>>| FlvCtrl {
        kind: CTRL_KIND.to_string(),
        v: 1,
        url: url.to_string(),
        bytes,
        live_ms,
        mode: CTRL_MODE_LIVE.to_string(),
        probe: probe.map(hex::encode).unwrap_or_default(),
    };
    let save_now = |sink: &mut FileSink, mapped_max: u64, probe: Option<&Vec<u8>>| -> Result<(), HttpError> {
        sink.flush_buf().map_err(|e| HttpError::Io(e.to_string()))?;
        save_ctrl(path, &ctrl_of(sink.position(), mapped_max, probe));
        Ok(())
    };

    let outcome: Result<Exit, HttpError> = 'record: loop {
        if cancel.is_cancelled() {
            break Ok(Exit::Cancelled);
        }
        // 停滞收尾：连续这么久没有任何新数据
        if !opts.stall_timeout.is_zero() && last_data.elapsed() >= opts.stall_timeout {
            tracing::info!(
                stall_s = opts.stall_timeout.as_secs(),
                bytes = sink.position(),
                "FLV 直播流连续 {} 秒没有数据，按录制结束收尾",
                opts.stall_timeout.as_secs()
            );
            break Ok(Exit::Stalled);
        }
        if dirty && last_save.elapsed() >= CTRL_SAVE_INTERVAL {
            save_now(&mut sink, parser.mapped_max, parser.begin_probe.as_ref())?;
            dirty = false;
            last_save = Instant::now();
        }

        // —— 建立连接 ——
        parser.begin_segment();
        // 上一段没有产生过媒体 tag 的"开头"（只有脚本 tag 就断了）：
        // 它已经不完整，丢掉（新段的开头会重新攒）
        pending_open.clear();
        let req = apply_headers(client.get(url), &opts.headers);
        let resp = tokio::select! {
            biased;
            _ = cancel.cancelled() => break 'record Ok(Exit::Cancelled),
            r = tokio::time::timeout(FIRST_BYTE_TIMEOUT, req.send()) => r,
        };
        let resp = match resp {
            Err(_) => {
                last_err = Some(HttpError::Timeout);
                tracing::warn!("FLV 连接超时，退避重连");
                attempt = attempt.saturating_add(1);
                backoff(cancel, attempt).await?;
                continue;
            }
            Ok(Err(e)) => {
                let err = HttpError::from_reqwest(&e);
                if !err.is_retryable() {
                    break Err(err);
                }
                last_err = Some(err.clone());
                tracing::warn!(err = %err, "FLV 连接失败，退避重连");
                attempt = attempt.saturating_add(1);
                backoff(cancel, attempt).await?;
                continue;
            }
            Ok(Ok(r)) => r,
        };
        let status = resp.status();
        if matches!(status.as_u16(), 404 | 410) {
            // 直播下线的典型形态（nginx-rtmp / SRS 在流结束后 404）
            gone_streak += 1;
            if gone_streak >= GONE_LIMIT {
                tracing::info!("FLV 地址持续不可用，按录制结束收尾");
                break Ok(Exit::Gone);
            }
            tokio::select! {
                _ = tokio::time::sleep(GONE_RETRY_DELAY) => {}
                _ = cancel.cancelled() => break 'record Ok(Exit::Cancelled),
            }
            continue;
        }
        if !status.is_success() {
            let err = HttpError::Http(status.as_u16());
            if !err.is_retryable() {
                break Err(err);
            }
            last_err = Some(err.clone());
            tracing::warn!(err = %err, "FLV 响应异常，退避重连");
            attempt = attempt.saturating_add(1);
            backoff(cancel, attempt).await?;
            continue;
        }
        gone_streak = 0;

        // 服务器声明了总长 = 有限资源。录制现场还在（产物已有内容）时
        // 说明这条直播已经下线、地址变成了点播文件 —— 继续追加只会把
        // 文件从头发一遍，保留已录内容收尾；全新任务则读完整份后收尾。
        let declared_len = resp.content_length();
        if declared_len.is_some() && sink.position() > 0 {
            tracing::info!(
                bytes = sink.position(),
                "FLV 地址已变为有限资源（响应带总长），保留已录内容收尾"
            );
            break Ok(Exit::Gone);
        }
        let finite = declared_len.is_some();

        stats.connections.store(1, Ordering::Relaxed);
        let mut run_bytes = 0u64;
        let mut stream = resp.bytes_stream();
        loop {
            let chunk = tokio::select! {
                biased;
                _ = cancel.cancelled() => break 'record Ok(Exit::Cancelled),
                c = tokio::time::timeout(READ_IDLE_TIMEOUT, stream.next()) => match c {
                    // 读静默：断开重连（数据在流动时每秒都有多个 tag）
                    Err(_) => {
                        last_err = Some(HttpError::Timeout);
                        break;
                    }
                    Ok(None) => break,
                    Ok(Some(Ok(c))) => c,
                    Ok(Some(Err(e))) => {
                        let err = HttpError::from_reqwest(&e);
                        if !err.is_retryable() {
                            stats.connections.store(0, Ordering::Relaxed);
                            break 'record Err(err);
                        }
                        last_err = Some(err);
                        break;
                    }
                },
            };
            if chunk.is_empty() {
                continue;
            }
            run_bytes += chunk.len() as u64;
            stats
                .received
                .fetch_add(chunk.len() as u64, Ordering::Relaxed);
            last_data = Instant::now();
            buf.clear();
            if let Err(e) = parser.feed(&chunk, &mut buf) {
                stats.connections.store(0, Ordering::Relaxed);
                // 全新任务 + 一个字节都没录：不是 FLV，交回普通下载
                if sink.position() == 0 && resume.is_none() {
                    return Err(e);
                }
                // 录制中的连接返回了非 FLV 内容（错误页等）：按一次
                // 失败尝试处理，重连换现场（保留水位与已录内容）
                last_err = Some(HttpError::Protocol(format!("续录连接返回的内容不是 FLV: {e}")));
                parser = FlvStream::new(parser.mapped_max, 1, parser.begin_probe.clone());
                break;
            }
            // 服务端从头重发同一份内容（直播结束后的归档 / 有限资源被
            // 当成直播重连）：这一整遍都不能要，就此收尾
            if parser.replay {
                tracing::info!(
                    bytes = sink.position(),
                    "FLV 重连后从头发送同一内容（重播），按录制结束收尾"
                );
                break 'record Ok(Exit::Ended);
            }
            if !buf.is_empty() {
                // 本段开头（文件头 / onMetaData）先攒着：如果它是"重播"
                // 的开头，这些字节也不能要（上面紧接着就是重播判定）。
                // 见到本段第一个媒体 tag 后一切照常直接落盘。
                if parser.seg_media_seen || pending_open.len() >= OPEN_HOLD_LIMIT {
                    if !pending_open.is_empty() {
                        write_out(&mut sink, &stats, opts.limiter.as_deref(), &pending_open)
                            .await?;
                        pending_open.clear();
                    }
                    write_out(&mut sink, &stats, opts.limiter.as_deref(), &buf).await?;
                } else {
                    pending_open.extend_from_slice(&buf);
                }
                stats.live_ms.store(parser.mapped_max, Ordering::Relaxed);
                dirty = true;
            }
            if finite && declared_len.is_some_and(|t| run_bytes >= t) {
                break;
            }
        }
        stats.connections.store(0, Ordering::Relaxed);

        if finite {
            // 有限资源：读到 EOF（或读满总长）就是结束
            break Ok(Exit::Ended);
        }
        if !cancel.is_cancelled() {
            if run_bytes == 0 {
                // 连上了却一个字节都没有：反复如此说明流已结束
                empty_streak += 1;
                if empty_streak >= EMPTY_LIMIT {
                    tracing::info!("FLV 重连后始终没有数据，按录制结束收尾");
                    break Ok(Exit::Gone);
                }
            } else {
                empty_streak = 0;
            }
            attempt = if run_bytes > 0 { 0 } else { attempt.saturating_add(1) };
            backoff(cancel, attempt).await?;
        }
    };

    // —— 收尾 ——
    let flush = sink.flush().map_err(|e| HttpError::Io(e.to_string()));
    stats.completed.store(sink.position(), Ordering::Relaxed);
    stats.live_ms.store(parser.mapped_max, Ordering::Relaxed);
    stats.connections.store(0, Ordering::Relaxed);

    match outcome {
        Err(e) => {
            // 失败也保留现场：下次恢复从断点续录
            if flush.is_ok() {
                save_ctrl(
                    path,
                    &ctrl_of(sink.position(), parser.mapped_max, parser.begin_probe.as_ref()),
                );
            }
            Err(e)
        }
        Ok(Exit::Cancelled) => {
            if let Err(e) = flush {
                save_ctrl(
                    path,
                    &ctrl_of(sink.position(), parser.mapped_max, parser.begin_probe.as_ref()),
                );
                return Err(e);
            }
            save_ctrl(
                path,
                &ctrl_of(sink.position(), parser.mapped_max, parser.begin_probe.as_ref()),
            );
            tracing::info!(
                bytes = sink.position(),
                live_ms = parser.mapped_max,
                "FLV 直播录制暂停（现场已保存，恢复后继续）"
            );
            Err(HttpError::Cancelled)
        }
        Ok(Exit::Stalled) | Ok(Exit::Gone) | Ok(Exit::Ended) => {
            if let Err(e) = flush {
                save_ctrl(
                    path,
                    &ctrl_of(sink.position(), parser.mapped_max, parser.begin_probe.as_ref()),
                );
                return Err(e);
            }
            // 一个字节都没录到（且不是续录）：这不是"录完了"，是没录成 ——
            // 报错（调用方会把它当失败 / 换地址），不冒充"录制完成"
            if sink.position() == 0 {
                return Err(last_err.take().unwrap_or_else(|| {
                    HttpError::Protocol("FLV 直播流没有任何数据".into())
                }));
            }
            // 录到干净：现场收掉（产物时间轴在写入时已经归零/接续）
            let ctrl = xfer_storage::ctrl_path(path);
            if ctrl.exists() {
                let _ = std::fs::remove_file(&ctrl);
            }
            let bytes = sink.position();
            tracing::info!(bytes, live_ms = parser.mapped_max, "FLV 直播录制结束");
            Ok(FlvDone {
                bytes,
                live_ms: parser.mapped_max,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 造一段 FLV：文件头 + `(tag_type, ts, data_len)` 列表。
    fn flv_bytes(tags: &[(u8, u32, usize)]) -> Vec<u8> {
        let mut out = vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
        for (t, ts, len) in tags {
            out.push(*t);
            out.push((len >> 16) as u8);
            out.push((len >> 8) as u8);
            out.push(*len as u8);
            out.push((ts >> 16) as u8);
            out.push((ts >> 8) as u8);
            out.push(*ts as u8);
            out.push((ts >> 24) as u8);
            out.extend_from_slice(&[0, 0, 0]); // streamId
            out.extend(std::iter::repeat(0xAB).take(*len));
            out.extend_from_slice(&[0, 0, 0, 0]); // PreviousTagSize
        }
        out
    }

    /// 逐 tag 读回产物里的时间戳（顺带验证结构严格自洽）。
    ///
    /// 产物可能**没有** FLV 文件头（续录时只在第一次写头，见 `FlvStream`）。
    fn read_timestamps(data: &[u8]) -> Vec<(u8, u32)> {
        let mut pos = if &data[..3] == b"FLV" {
            u32::from_be_bytes([data[5], data[6], data[7], data[8]]) as usize + 4
        } else {
            0
        };
        let mut out = Vec::new();
        while pos < data.len() {
            let typ = data[pos];
            let size = ((data[pos + 1] as usize) << 16)
                | ((data[pos + 2] as usize) << 8)
                | (data[pos + 3] as usize);
            let ts = ((data[pos + 4] as u32) << 16)
                | ((data[pos + 5] as u32) << 8)
                | (data[pos + 6] as u32)
                | ((data[pos + 7] as u32) << 24);
            out.push((typ, ts));
            pos += 11 + size + 4;
        }
        assert_eq!(pos, data.len(), "产物结构必须严格自洽（长度对得上）");
        out
    }

    fn feed_all(parser: &mut FlvStream, data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        parser.feed(data, &mut out).unwrap();
        out
    }

    #[test]
    fn timestamps_start_at_zero_and_advance() {
        let src = flv_bytes(&[(18, 0, 8), (9, 3_600_000, 16), (8, 3_600_040, 12), (9, 3_600_080, 16)]);
        let mut p = FlvStream::new(0, 0, None);
        let out = feed_all(&mut p, &src);
        let ts = read_timestamps(&out);
        // 脚本 tag 压在 0；媒体 tag 从 0 起（开播以来的时间戳被清零）
        assert_eq!(ts[0].1, 0);
        assert_eq!(ts[1].1, 0);
        assert_eq!(ts[2].1, 40);
        assert_eq!(ts[3].1, 80);
        assert_eq!(p.mapped_max, 80);
        assert_eq!(out.len(), src.len());
    }

    #[test]
    fn chunk_split_anywhere_is_equivalent() {
        let src = flv_bytes(&[(9, 1000, 7), (8, 1020, 5), (9, 1040, 9)]);
        let whole = feed_all(&mut FlvStream::new(0, 0, None), &src);
        for step in [1usize, 2, 3, 5, 8, 13, 64] {
            let mut p = FlvStream::new(0, 0, None);
            let mut got = Vec::new();
            let mut buf = Vec::new();
            for part in src.chunks(step) {
                buf.clear();
                p.feed(part, &mut buf).unwrap();
                got.extend_from_slice(&buf);
            }
            assert_eq!(got, whole, "按 {step} 字节分块结果必须一致");
        }
    }

    #[test]
    fn reconnect_continues_after_previous_end() {
        let mut p = FlvStream::new(0, 0, None);
        let a = flv_bytes(&[(9, 200_000, 4), (9, 200_040, 4)]);
        let mut out = feed_all(&mut p, &a);
        assert_eq!(p.mapped_max, 40);
        // 断线 5 秒后重连：服务端时间戳跳到 205000，产物必须接着 40 排
        p.begin_segment();
        let b = flv_bytes(&[(9, 205_000, 4), (8, 205_020, 4)]);
        out.extend(feed_all(&mut p, &b));
        // 重连带的文件头被跳过：产物里恰好一个文件头
        assert_eq!(&out[..3], b"FLV");
        assert_eq!(out.windows(3).filter(|w| *w == b"FLV").count(), 1);
        let ts = read_timestamps(&out);
        assert_eq!(
            ts.iter().map(|x| x.1).collect::<Vec<_>>(),
            vec![0, 40, 40, 60]
        );
        assert_eq!(p.mapped_max, 60);
    }

    #[test]
    fn resume_anchor_handles_server_timeline_reset() {
        // 续录：现场已录 50000ms；服务端换流后时间戳从 0 重新计
        let mut p = FlvStream::new(50_000, 1234, None);
        p.begin_segment();
        let src = flv_bytes(&[(9, 0, 4), (9, 40, 4)]);
        let out = feed_all(&mut p, &src);
        let ts = read_timestamps(&out);
        assert_eq!(ts[0].1, 50_000);
        assert_eq!(ts[1].1, 50_040);
        assert_eq!(p.mapped_max, 50_040);
        // 续录时产物里不会再写第二个文件头
        assert!(out.windows(3).all(|w| w != b"FLV"));
    }

    #[test]
    fn non_flv_is_rejected() {
        let mut p = FlvStream::new(0, 0, None);
        let mut out = Vec::new();
        assert!(matches!(
            p.feed(b"<html>not flv at all", &mut out),
            Err(HttpError::NotFlv)
        ));
        assert!(out.is_empty());
    }

    #[test]
    fn replay_from_the_beginning_is_detected() {
        let a = flv_bytes(&[(9, 700_000, 32), (9, 700_040, 32)]);
        let mut p = FlvStream::new(0, 0, None);
        let _ = feed_all(&mut p, &a);
        assert!(!p.replay);
        p.begin_segment();
        // 服务端从头发同一份内容（时间戳与内容逐字节相同）→ 重播
        let mut out = Vec::new();
        p.feed(&a, &mut out).unwrap();
        assert!(p.replay, "从头重发同一内容必须被识别");
        // 直播续传（内容前进）不能被误判
        let mut q = FlvStream::new(0, 0, None);
        let _ = feed_all(&mut q, &a);
        q.begin_segment();
        let b = flv_bytes(&[(9, 700_080, 32)]);
        let _ = feed_all(&mut q, &b);
        assert!(!q.replay, "正常的直播续传不能判成重播");
        // 换流重启（时间戳回退、内容不同）同样是续传，不是重播
        let mut r = FlvStream::new(0, 0, None);
        let _ = feed_all(&mut r, &flv_bytes(&[(9, 700_000, 32)]));
        r.begin_segment();
        let _ = feed_all(&mut r, &flv_bytes(&[(9, 0, 32), (9, 40, 32)]));
        assert!(!r.replay, "服务端时间戳回退的换流不能判成重播");
    }

    #[test]
    fn ctrl_roundtrip_and_resume_point() {
        let dir = std::env::temp_dir().join(format!("xfer-flv-unit-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let ctrl_dir = std::env::temp_dir().join(format!("xfer-flv-ctrl-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&ctrl_dir);
        std::env::set_var("XFER_CTRL_DIR", &ctrl_dir);
        let product = dir.join("live.flv");
        std::fs::write(&product, vec![0u8; 4096]).unwrap();
        save_ctrl(
            &product,
            &FlvCtrl {
                kind: CTRL_KIND.into(),
                v: 1,
                url: "https://live.example.com/app/stream.flv?token=abc".into(),
                bytes: 3000,
                live_ms: 12_345,
                mode: CTRL_MODE_LIVE.into(),
                probe: hex::encode(b"probe-bytes"),
            },
        );
        assert!(has_live_ctrl(&product));
        // 令牌轮换不影响"还是这条流"
        let r = live_resume_point(&product, "https://live.example.com/app/stream.flv?token=zzz");
        assert_eq!(r, Some((3000, 12_345)));
        // 换了一条流 → 不认
        assert_eq!(
            live_resume_point(&product, "https://live.example.com/app/other.flv"),
            None
        );
        // 产物比水位短 → 不认
        std::fs::write(&product, vec![0u8; 10]).unwrap();
        assert_eq!(
            live_resume_point(&product, "https://live.example.com/app/stream.flv?t=1"),
            None
        );
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&ctrl_dir);
    }

    #[test]
    fn default_filename_normalizes_ext() {
        assert_eq!(default_filename("https://h/app/stream.flv"), "stream.flv");
        assert_eq!(default_filename("https://h/app/stream"), "stream.flv");
        assert_eq!(
            default_filename("https://h/live/ch1.php?token=x"),
            "ch1.flv"
        );
        assert_eq!(default_filename("https://h/"), "live.flv");
    }
}
