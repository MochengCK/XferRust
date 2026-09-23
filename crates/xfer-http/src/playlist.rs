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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
const SIZE_PROBE_MAX_SEGMENTS: usize = 5000;
/// 预探测并发上限：探测是轻量请求，不需要跟着下载并发走。
const SIZE_PROBE_CONCURRENCY: usize = 8;
/// 控制文件落盘节流（与分片下载一致：1s 一次 fsync + 原子写）。
const CTRL_SAVE_INTERVAL: Duration = Duration::from_secs(1);
/// 单个分片的瞬时失败重试默认次数。
pub const DEFAULT_SEGMENT_RETRIES: u32 = 3;

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
    /// 全部分片大小已知时的总字节数。
    pub total: Option<u64>,
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
        }
    }
}

/// 引擎轮询的进度句柄（无锁原子）。
pub struct PlaylistStats {
    /// 已落盘（fsync）字节数，含续传基线。
    pub completed: AtomicU64,
    /// 当前在飞分片数。
    pub connections: AtomicUsize,
}

impl PlaylistStats {
    pub fn new(baseline: u64) -> Arc<Self> {
        Arc::new(Self {
            completed: AtomicU64::new(baseline),
            connections: AtomicUsize::new(0),
        })
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
            });
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
            chain.push(cur.clone());
            cur = chosen.url;
            continue;
        }

        let media = parse_media(&text, &base).map_err(HttpError::Protocol)?;
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

/// 分片大小预探测：全部命中才能给出精确总长。
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
    let unknown: Vec<usize> = plan
        .segments
        .iter()
        .enumerate()
        .filter(|(_, s)| s.size.is_none())
        .map(|(i, _)| i)
        .collect();
    let init_unknown = plan.init.as_ref().map(|i| i.size.is_none()).unwrap_or(false);
    if plan.init.is_some() && init_unknown {
        // 初始化段通常很小，单独探测保证它一定进产物
        if let Some(i) = plan.init.as_mut() {
            if let Ok(p) = crate::probe_with(client, &i.url, cancel, headers).await {
                if !p.accepts_ranges {
                    return; // 服务器不支持 Range，放弃全部探测
                }
                i.size = p.total_len;
            }
        }
    }
    if unknown.is_empty() || unknown.len() > SIZE_PROBE_MAX_SEGMENTS {
        plan.total = sum_sizes(plan);
        return;
    }
    let conn = concurrency.clamp(1, SIZE_PROBE_CONCURRENCY);
    let urls: Vec<String> = unknown.iter().map(|i| plan.segments[*i].url.clone()).collect();
    let mut results: Vec<Option<u64>> = Vec::with_capacity(urls.len());
    {
        let mut stream = futures_util::stream::iter(urls.into_iter())
            .map(|url| {
                let url = url.clone();
                async move {
                    match crate::probe_with(client, &url, cancel, headers).await {
                        Ok(p) => p.total_len.filter(|n| *n > 0),
                        Err(_) => None,
                    }
                }
            })
            .buffered(conn);
        while let Some(size) = stream.next().await {
            results.push(size);
        }
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
    /// 已持久化字节数（= 对应文件长度）
    bytes: u64,
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

/// 续传水位：`(已持久化字节数, 已持久化段数)`。
///
/// 引擎在启动下载前用它回填进度与分片位图基线；`download_playlist`
/// 内部再算一次（纯函数，结果一致）。
pub fn resume_point(path: &Path, plan: &PlaylistPlan) -> (u64, usize) {
    let ctrl = xfer_storage::ctrl_path(path);
    let Some(c) = load_ctrl(&ctrl, plan) else {
        return (0, 0);
    };
    match std::fs::metadata(path) {
        Ok(m) if m.len() >= c.bytes && c.prefix <= plan.segment_count() => (c.bytes, c.prefix),
        _ => (0, 0),
    }
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

/// 拉取单个分片（含重试、限速、解密）。
async fn fetch_segment(ctx: &Ctx<'_>, seg: &Segment) -> Result<Vec<u8>, HttpError> {
    let mut attempt = 1u32;
    loop {
        if ctx.is_cancelled() {
            return Err(HttpError::Cancelled);
        }
        let r = async {
            let _conn = ConnGuard::new(&ctx.stats.connections);
            let key = match &seg.key {
                Some(k) => Some(fetch_key(ctx, &k.url).await?),
                None => None,
            };
            let body = fetch_body(ctx, seg).await?;
            match (&key, &seg.key) {
                (Some(k), Some(sk)) => decrypt_segment(k, &sk.iv, &body),
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

/// 单次分片请求（区间请求 + 流式读取 + 限速 + 长度校验）。
async fn fetch_body(ctx: &Ctx<'_>, seg: &Segment) -> Result<Vec<u8>, HttpError> {
    let mut req = apply_headers(ctx.client.get(&seg.url), ctx.headers);
    if let Some((off, len)) = seg.range {
        req = req.header("Range", format!("bytes={}-{}", off, off + len.saturating_sub(1)));
    }
    let resp = tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => return Err(HttpError::Cancelled),
        r = req.send() => r.map_err(|e| HttpError::from_reqwest(&e))?,
    };
    let status = resp.status();
    if !status.is_success() {
        return Err(HttpError::Http(status.as_u16()));
    }
    let mut buf: Vec<u8> = Vec::with_capacity(seg.size.unwrap_or(1 << 20) as usize);
    let mut stream = resp.bytes_stream();
    loop {
        let chunk = tokio::select! {
            biased;
            _ = ctx.cancel.cancelled() => return Err(HttpError::Cancelled),
            c = stream.next() => match c {
                Some(Ok(c)) => c,
                Some(Err(e)) => return Err(HttpError::from_reqwest(&e)),
                None => break,
            },
        };
        if chunk.is_empty() {
            continue;
        }
        if let Some(l) = ctx.limiter {
            l.acquire(chunk.len()).await;
        }
        buf.extend_from_slice(&chunk);
    }
    if let Some(expected) = seg.size {
        if buf.len() as u64 != expected {
            return Err(HttpError::ShortRead);
        }
    }
    Ok(buf)
}

/// 按计划下载分片并顺序拼接写入 `path`。
///
/// 取消时返回 [`HttpError::Cancelled`]，已持久化的连续前缀保留在控制
/// 文件中，下次调用从该处续传。
pub async fn download_playlist(
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
    // resume_point 返回 (已持久化字节数, 已持久化段数)
    let (mut bytes, mut prefix) = resume_point(path, plan);
    if prefix > 0 && bytes == 0 {
        prefix = 0;
    }

    // 落位：续传时截断到已持久化水位（丢弃上次未 fsync 的尾巴），否则重建
    let mut sink = if prefix > 0 {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .and_then(|f| f.set_len(bytes))
            .map_err(|e| HttpError::Io(e.to_string()))?;
        FileSink::append_at(path, bytes).map_err(|e| HttpError::Io(e.to_string()))?
    } else {
        FileSink::create(path).map_err(|e| HttpError::Io(e.to_string()))?
    };
    stats.completed.store(bytes, Ordering::Relaxed);

    let ctx = Ctx {
        client,
        cancel,
        limiter: opts.limiter.as_deref(),
        headers: &opts.headers,
        retries: opts.retries.max(1),
        stats: &stats,
        keys: Mutex::new(HashMap::new()),
    };

    let pending: Vec<Segment> = all[prefix..].to_vec();
    let conn = opts.concurrency.clamp(1, 64);
    let mut written = prefix;
    let mut last_save = std::time::Instant::now() - CTRL_SAVE_INTERVAL;
    let ctrl_of = |prefix: usize, bytes: u64| PlaylistCtrl {
        kind: CTRL_KIND.to_string(),
        v: 1,
        fp: fp.clone(),
        url: plan.source.clone(),
        segs: total_segments,
        prefix,
        bytes,
    };

    let mut stream = futures_util::stream::iter(pending.into_iter())
        .map(|seg| {
            let ctx = &ctx;
            async move { fetch_segment(ctx, &seg).await }
        })
        .buffered(conn);

    let mut failure: Option<HttpError> = None;
    while let Some(item) = stream.next().await {
        let data = match item {
            Ok(d) => d,
            Err(e) => {
                failure = Some(e);
                break;
            }
        };
        if let Err(e) = sink.write(&data) {
            failure = Some(HttpError::Io(e.to_string()));
            break;
        }
        written += 1;
        stats
            .completed
            .store(sink.position(), Ordering::Relaxed);
        if last_save.elapsed() >= CTRL_SAVE_INTERVAL {
            // 先 fsync 再记水位：控制文件绝不领先磁盘（同上层的续传不变式）
            if let Err(e) = sink.flush() {
                failure = Some(HttpError::Io(e.to_string()));
                break;
            }
            bytes = sink.position();
            save_ctrl(&ctrl_path, &ctrl_of(written, bytes));
            last_save = std::time::Instant::now();
        }
    }
    drop(stream);

    // 终局：刷盘 + 保存水位；全部完成时删除控制文件
    let flush = sink.flush().map_err(|e| HttpError::Io(e.to_string()));
    if let Err(e) = flush {
        if failure.is_none() {
            failure = Some(e);
        }
    } else {
        bytes = sink.position();
    }
    match (&failure, written == total_segments) {
        (Some(_), _) => {
            save_ctrl(&ctrl_path, &ctrl_of(written, bytes));
            return Err(failure.unwrap());
        }
        (None, true) => {
            let _ = std::fs::remove_file(&ctrl_path);
            return Ok(PlaylistDone {
                bytes,
                segments: written,
                total_segments,
            });
        }
        (None, false) => {
            // 分片流提前结束（清单被服务端截断）：保留水位，按失败上报
            save_ctrl(&ctrl_path, &ctrl_of(written, bytes));
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
            init: None,
            segments: vec![Segment {
                url: u.into(),
                range: None,
                key: None,
                size: None,
            }],
            live: true,
            fmp4: false,
            total: None,
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
    }

    impl TestServer {
        fn url(&self, p: &str) -> String {
            format!("{}{}", self.base, p)
        }
        fn hits(&self, p: &str) -> usize {
            self.hits.get(p).map(|c| c.load(Ordering::SeqCst)).unwrap_or(0)
        }
    }

    /// 起一个支持 Range 的静态文件服务；`fail_first` 里列出的路径前 N 次
    /// 请求返回 500（模拟瞬时故障，用于验证续传）。
    async fn start_server(
        files: HashMap<String, Vec<u8>>,
        fail_first: HashMap<String, usize>,
    ) -> TestServer {
        use axum::http::{header, HeaderValue, StatusCode};
        use axum::response::IntoResponse;
        use axum::routing::get;
        use axum::Router;

        let mut hits: HashMap<String, Arc<AtomicUsize>> = HashMap::new();
        let mut app = Router::new();
        for (path, body) in files {
            let data = Arc::new(body);
            let hit = Arc::new(AtomicUsize::new(0));
            let fail = fail_first.get(&path).map(|n| Arc::new(AtomicUsize::new(*n)));
            hits.insert(path.clone(), hit.clone());
            app = app.route(
                &path,
                get(move |headers: axum::http::HeaderMap| {
                    let data = data.clone();
                    let hit = hit.clone();
                    let fail = fail.clone();
                    async move {
                        let seen = hit.fetch_add(1, Ordering::SeqCst);
                        if let Some(f) = &fail {
                            if seen < f.load(Ordering::SeqCst) {
                                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                            }
                        }
                        let total = data.len();
                        let range = headers
                            .get(header::RANGE)
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or_default()
                            .to_string();
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
                        let body = data[from..to].to_vec();
                        if from > 0 || to < total {
                            let mut r = axum::response::Response::new(axum::body::Body::from(body));
                            *r.status_mut() = StatusCode::PARTIAL_CONTENT;
                            r.headers_mut().insert(
                                header::CONTENT_RANGE,
                                HeaderValue::from_str(&format!(
                                    "bytes {}-{}/{}",
                                    from,
                                    to.saturating_sub(1),
                                    total
                                ))
                                .unwrap(),
                            );
                            r
                        } else {
                            axum::response::Response::new(axum::body::Body::from(body))
                        }
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
                Segment { url: srv.url("/m/b.ts"), range: None, key: None, size: None },
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
}
