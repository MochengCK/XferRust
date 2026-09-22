//! HTTP(S) 下载：探测（总长度 / 文件名 / Range 支持）、单连接流式下载
//! 与多连接分片下载（见 [`split`] 模块：单写线程调度 + 工作窃取对冲
//! + 段级控制文件断点续传）。HLS（M3U8）播放列表下载见 [`playlist`]。

mod adaptive;
mod playlist;
mod rate;
mod split;

pub use adaptive::{AdaptiveConfig, AdaptiveScheduler, ConnPerf, ScheduleAction};
pub use playlist::{
    default_filename as playlist_default_filename, download_playlist, fetch_plan,
    resume_point as playlist_resume_point, PlaylistDone, PlaylistOptions, PlaylistPlan,
    PlaylistStats, Segment as PlaylistSegment, SegmentKey as PlaylistKey,
    DEFAULT_SEGMENT_RETRIES,
};
pub use rate::RateLimiter;
pub use split::{
    ctrl_path, download_split, PieceSnapshot, PieceTrack, SplitDone, SplitOptions, SplitStats,
};

use std::time::Duration;

use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;

use xfer_types::{text as charset_text, ENGINE_NAME, ENGINE_VERSION};

/// 构建 HTTP 客户端（全局共享）。
///
/// - UA 按引擎名/版本派生（可通过 `user-agent` 全局选项覆盖）；
/// - 可选 HTTP 代理（`all-proxy` 全局选项，空 = 直连）；
/// - 不启用自动解压（保证 Content-Length 与线上字节一致）；
/// - 连接 10s、读 30s 超时；无整体超时（大文件流式下载）；
/// - TCP_NODELAY：流式分块传输关闭 Nagle，避免小块合并延迟。
pub fn build_client() -> reqwest::Client {
    build_client_with(None, None, None)
}

/// 按用户配置构建 HTTP 客户端：`user_agent`（None = 引擎默认 UA）、
/// `proxy`（None/空 = 直连，否则为 `http://host:port` 形式代理地址）、
/// `no_proxy`（None/空 = 不过滤，逗号分隔的直连主机/网段，支持
/// `.example.com` 后缀与 `192.168.0.0/16` 网段写法）。
///
/// `no-proxy` 作用于 `all-proxy` 配置的代理；走环境变量代理
/// （`HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY`，reqwest 默认读取）时由
/// `NO_PROXY` 环境变量本身控制。
pub fn build_client_with(
    user_agent: Option<&str>,
    proxy: Option<&str>,
    no_proxy: Option<&str>,
) -> reqwest::Client {
    let mut builder = reqwest::Client::builder()
        .user_agent(user_agent
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{ENGINE_NAME}/{ENGINE_VERSION}")))
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::default())
        .tcp_nodelay(true);
    if let Some(p) = proxy.filter(|s| !s.trim().is_empty()) {
        if let Ok(mut pr) = reqwest::Proxy::all(p) {
            // 直连例外（no-proxy）：此前该选项只被存储从未生效，代理环境里
            // 局域网/回环地址也被塞进代理，本可直连的地址反而失败
            if let Some(np) = no_proxy
                .filter(|s| !s.trim().is_empty())
                .and_then(reqwest::NoProxy::from_string)
            {
                pr = pr.no_proxy(Some(np));
            }
            builder = builder.proxy(pr);
        }
    }
    builder.build().expect("构建 HTTP 客户端失败")
}

/// 逐任务自定义请求头（`(名称, 值)`，名称大小写按调用方给出）。
///
/// 用于把浏览器侧的真实请求上下文（`Referer` / `Cookie` / `User-Agent`）
/// 带进下载请求——需要请求头校验的地址缺少它们必然 403。
pub type RequestHeaders = Vec<(String, String)>;

/// 会被丢弃的请求头：要么破坏下载语义，要么与客户端自身行为冲突。
///
/// - `range`：区间由引擎按分段进度计算，覆盖会让续传位图与磁盘错位
/// - `host`：虚拟主机由客户端按 URL 决定（覆盖会导致请求打错站点）
/// - `content-length`：请求体长度，引擎不发请求体
/// - `connection` / `accept-encoding`：连接与压缩编码由客户端接管
///
/// 注意**不包含 `origin`**：是否携带由调用方决定。浏览器对跨域 GET
/// 不会带 `Origin`，伪造 `Origin` 是 CDN/WAF 判定伪造请求的典型特征，
/// 会被直接 403——调用方应自行避免下发该头。
const DROPPED_REQUEST_HEADERS: [&str; 5] = [
    "range",
    "host",
    "content-length",
    "connection",
    "accept-encoding",
];

/// 解析 `Name: value` 形式的请求头行（忽略空行）。
///
/// - 无冒号、名称为空、值含 CR/LF 或非可见 ASCII → 整行丢弃
/// - 命中 [`DROPPED_REQUEST_HEADERS`] → 丢弃
/// - 同名重复（大小写不敏感）：后出现的覆盖先出现的，保留首次出现的位置
pub fn parse_header_lines<I, S>(lines: I) -> RequestHeaders
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut out: RequestHeaders = Vec::new();
    for line in lines {
        let line = line.as_ref().trim_end_matches(['\r', '\n']);
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim();
        let value = value.trim();
        if name.is_empty() || value.is_empty() {
            continue;
        }
        if reqwest::header::HeaderName::from_bytes(name.as_bytes()).is_err()
            || reqwest::header::HeaderValue::from_str(value).is_err()
        {
            continue;
        }
        if DROPPED_REQUEST_HEADERS.contains(&name.to_ascii_lowercase().as_str()) {
            continue;
        }
        match out
            .iter_mut()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
        {
            Some(slot) => slot.1 = value.to_string(),
            None => out.push((name.to_string(), value.to_string())),
        }
    }
    out
}

/// 把自定义请求头逐条挂到请求上（入参应已经过 [`parse_header_lines`]）。
pub(crate) fn apply_headers(
    mut req: reqwest::RequestBuilder,
    headers: &[(String, String)],
) -> reqwest::RequestBuilder {
    for (name, value) in headers {
        req = req.header(name.as_str(), value.as_str());
    }
    req
}

/// 下载相关错误。`Cancelled` 表示主动暂停/移除，不是任务失败。
#[derive(Debug, Clone, thiserror::Error)]
pub enum HttpError {
    #[error("连接服务器失败: {0}")]
    Connect(String),
    #[error("连接超时")]
    Timeout,
    #[error("资源不存在或不可访问 (HTTP {0})")]
    Http(u16),
    #[error("服务器响应异常: {0}")]
    Protocol(String),
    /// 响应体短于请求区间（服务器中途断流，干净 EOF）——瞬时、
    /// 可再生，重试即可，不视为协议违约（线上"99% 卡死"根因：
    /// 尾段短读风暴把整个失败预算耗尽，任务被误杀）。
    #[error("响应体短于请求区间")]
    ShortRead,
    /// 服务器探测时支持 Range，但实际分段请求不配合——
    /// 调用方应回退单连接模式（此时本地文件已被截断为连续前缀）。
    #[error("服务器不支持分段下载: {0}")]
    NotSplittable(String),
    /// 目标地址返回的内容不是 M3U8 清单（内容嗅探落空）。
    ///
    /// 调用方据此回退普通 HTTP 下载：把"看起来像清单、实际是普通
    /// 资源"的地址当播放列表处理会把整个响应体下载成产物。
    #[error("目标内容不是 M3U8 播放列表")]
    NotPlaylist,
    #[error("本地写入失败: {0}")]
    Io(String),
    #[error("已取消")]
    Cancelled,
}

impl HttpError {
    /// 从 reqwest 错误归类。
    pub fn from_reqwest(e: &reqwest::Error) -> Self {
        if e.is_timeout() {
            Self::Timeout
        } else if e.is_connect() {
            Self::Connect(e.to_string())
        } else {
            Self::Protocol(e.to_string())
        }
    }

    /// 是否为可重试的瞬时失败（连接类/超时/中途断流/5xx）。
    ///
    /// 4xx、本地 IO、取消都不重试：重试只会重复同一个确定性结果。
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Timeout | Self::Connect(_) | Self::Protocol(_) | Self::ShortRead => true,
            Self::Http(code) => *code >= 500,
            Self::NotSplittable(_) | Self::NotPlaylist | Self::Io(_) | Self::Cancelled => false,
        }
    }

    /// 映射到线上协议的任务错误码。
    pub fn error_code(&self) -> i64 {
        match self {
            HttpError::Timeout => 2,
            HttpError::Http(_) => 3,
            HttpError::Connect(_)
            | HttpError::Protocol(_)
            | HttpError::NotSplittable(_)
            | HttpError::NotPlaylist => 5,
            HttpError::Io(_) => 1,
            HttpError::Cancelled => 0,
            HttpError::ShortRead => 5,
        }
    }
}

/// 资源探测结果。
#[derive(Debug, Clone)]
pub struct Probe {
    /// 服务器可知的总长度（未知 / chunked 为 None）。
    pub total_len: Option<u64>,
    /// 服务器建议的文件名（Content-Disposition）。
    pub filename: Option<String>,
    /// 服务器是否支持 Range 请求（决定能否断点续传）。
    pub accepts_ranges: bool,
    /// 重定向后的最终 URL（文件名兜底解析用）。
    pub final_url: String,
    /// 响应声明的 MIME 类型（小写，含参数前的部分）。
    pub content_type: Option<String>,
}

impl Probe {
    /// 是否**像是** HLS 播放列表（MIME 命中或文件名以 `.m3u8` 结尾）。
    ///
    /// 只是嗅探：`m3u8` 的 MIME 在线上五花八门（`application/vnd.apple.mpegurl`、
    /// `application/x-mpegurl`、`audio/mpegurl`、甚至 `text/plain`），因此
    /// 命中后仍必须读正文确认首行是 `#EXTM3U`（见 [`crate::fetch_plan`]
    /// 的 [`HttpError::NotPlaylist`]）。
    pub fn is_playlist_hint(&self) -> bool {
        if let Some(ct) = &self.content_type {
            let ct = ct.to_ascii_lowercase();
            if ct.contains("mpegurl") || ct.contains("m3u8") || ct.contains("vnd.apple") {
                return true;
            }
        }
        let path = self
            .final_url
            .split(['?', '#'])
            .next()
            .unwrap_or(&self.final_url)
            .to_ascii_lowercase();
        path.ends_with(".m3u8") || path.ends_with(".m3u")
    }
}

/// 探测资源：`GET` + `Range: bytes=0-0`。
///
/// 206 → 支持 Range，总长取自 Content-Range；
/// 200 → 不支持 Range，总长取自 Content-Length；
/// 416 + `Content-Range: bytes */N` → 空资源（起点 0 不可满足），总长为 N。
pub async fn probe(
    client: &reqwest::Client,
    url: &str,
    cancel: &CancellationToken,
) -> Result<Probe, HttpError> {
    probe_with(client, url, cancel, &[]).await
}

/// 同 [`probe`]，但携带逐任务自定义请求头（`Referer` / `Cookie` /
/// `User-Agent` 等，见 [`RequestHeaders`]）。
///
/// 探测与随后的下载必须带同一组头：受保护地址若只在下载时带头、
/// 探测时不带，会拿到 403 且总长/文件名全部判错。
pub async fn probe_with(
    client: &reqwest::Client,
    url: &str,
    cancel: &CancellationToken,
    headers: &[(String, String)],
) -> Result<Probe, HttpError> {
    if cancel.is_cancelled() {
        return Err(HttpError::Cancelled);
    }
    let resp = apply_headers(client.get(url), headers)
        .header("Range", "bytes=0-0")
        .send()
        .await
        .map_err(|e| HttpError::from_reqwest(&e))?;
    let status = resp.status();
    let headers = resp.headers().clone();
    let final_url = resp.url().to_string();
    // 排空响应体再释放：连接（与 TLS 会话）随即可被后续下载请求复用；
    // 直接 drop 未读完的响应会让探测请求白白多付一次握手往返。
    let mut resp = resp;
    while let Ok(Some(_)) = resp.chunk().await {}

    // 416：bytes=0-0 对零长资源不可满足，Content-Range 携带真实总长。
    if status.as_u16() == 416 {
        let total_len = headers
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split_once("*/"))
            .and_then(|(_, n)| n.trim().parse::<u64>().ok());
        if let Some(n) = total_len {
            return Ok(Probe {
                total_len: Some(n),
                filename: probe_filename(&headers, &final_url),
                accepts_ranges: true,
                final_url,
                content_type: probe_content_type(&headers),
            });
        }
    }

    if !status.is_success() && status.as_u16() != 206 && !status.is_redirection() {
        return Err(HttpError::Http(status.as_u16()));
    }
    let accepts_ranges = status.as_u16() == 206;

    let total_len = if accepts_ranges {
        headers
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .and_then(total_from_content_range)
    } else {
        headers
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.trim().parse().ok())
    };

    let filename = probe_filename(&headers, &final_url);
    let content_type = probe_content_type(&headers);

    Ok(Probe {
        total_len,
        filename,
        accepts_ranges,
        final_url,
        content_type,
    })
}

/// 响应 MIME（小写；去掉 `;charset=...` 参数）。
fn probe_content_type(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let raw = headers.get("content-type")?.to_str().ok()?;
    let mime = raw.split(';').next().unwrap_or(raw).trim().to_ascii_lowercase();
    (!mime.is_empty()).then_some(mime)
}

/// 文件名解析：Content-Disposition 优先，URL 路径兜底。
fn probe_filename(headers: &reqwest::header::HeaderMap, final_url: &str) -> Option<String> {
    headers
        .get("content-disposition")
        .and_then(|v| v.to_str().ok())
        .and_then(filename_from_content_disposition)
        .or_else(|| filename_from_url(final_url))
}

/// 一次传输的结果。
#[derive(Debug)]
pub struct TransferDone {
    /// 本次传输的字节数（不含起点 offset）。
    pub transferred: u64,
    /// 已知的总长度。
    pub total_len: Option<u64>,
    /// 服务器忽略了 Range 请求（调用方需要截断本地文件从 0 重写）。
    pub restarted_from_zero: bool,
}

/// 传输落盘回调：由引擎实现，负责本地文件的打开/续写/截断与进度记账。
pub trait TransferSink: Send {
    /// 响应头解析后调用一次。
    ///
    /// `restarted` = 服务器忽略 Range、从 0 开始（sink 应截断重建）；
    /// 返回本连接写入前的基线偏移（用于进度修正）。
    fn begin(&mut self, restarted: bool) -> std::io::Result<u64>;
    /// 逐块写入。
    fn write_chunk(&mut self, data: &[u8]) -> std::io::Result<()>;
    /// 传输正常结束：刷盘并返回最终位置。
    fn finish(&mut self) -> std::io::Result<u64>;
}

/// 流式下载：从 `start` 偏移请求，逐块经 `sink` 落盘。
///
/// `limiter`：全局限速器（None / rate 0 = 不限），每块落盘前消费令牌。
///
/// 取消令牌触发时返回 [`HttpError::Cancelled`]（已写入部分由调用方保留）。
pub async fn download(
    client: &reqwest::Client,
    url: &str,
    start: u64,
    cancel: &CancellationToken,
    sink: &mut dyn TransferSink,
    limiter: Option<&RateLimiter>,
) -> Result<TransferDone, HttpError> {
    download_with(client, url, start, cancel, sink, limiter, &[]).await
}

/// 同 [`download`]，但携带逐任务自定义请求头（见 [`RequestHeaders`]）。
#[allow(clippy::too_many_arguments)]
pub async fn download_with(
    client: &reqwest::Client,
    url: &str,
    start: u64,
    cancel: &CancellationToken,
    sink: &mut dyn TransferSink,
    limiter: Option<&RateLimiter>,
    headers: &[(String, String)],
) -> Result<TransferDone, HttpError> {
    if cancel.is_cancelled() {
        return Err(HttpError::Cancelled);
    }
    let mut req = apply_headers(client.get(url), headers);
    if start > 0 {
        req = req.header("Range", format!("bytes={start}-"));
    }
    let resp = req.send().await.map_err(|e| HttpError::from_reqwest(&e))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(HttpError::Http(status.as_u16()));
    }

    let restarted_from_zero = start > 0 && status.as_u16() != 206;
    let total_len = if status.as_u16() == 206 {
        resp.headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .and_then(total_from_content_range)
    } else {
        resp.content_length()
    };

    sink.begin(restarted_from_zero)
        .map_err(|e| HttpError::Io(e.to_string()))?;
    let mut transferred: u64 = 0;
    let mut stream = resp.bytes_stream();
    loop {
        // 取消优先：服务器静默时每块间隔检查最坏要等读超时（30s）
        // 才能感知暂停，select 使取消立即生效。
        let chunk = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(HttpError::Cancelled),
            c = stream.next() => match c {
                Some(Ok(c)) => c,
                Some(Err(e)) => return Err(HttpError::from_reqwest(&e)),
                None => break,
            },
        };
        if chunk.is_empty() {
            continue;
        }
        // 全局限速：落盘前消费令牌，不足时等待（TCP 背压收敛速率）
        if let Some(l) = limiter {
            l.acquire(chunk.len()).await;
        }
        sink.write_chunk(&chunk)
            .map_err(|e| HttpError::Io(e.to_string()))?;
        transferred += chunk.len() as u64;
    }
    sink.finish().map_err(|e| HttpError::Io(e.to_string()))?;
    Ok(TransferDone {
        transferred,
        total_len,
        restarted_from_zero,
    })
}

/// 解析 `Content-Range: bytes 0-1/1234` 的总长度；`*` 返回 None。
fn total_from_content_range(value: &str) -> Option<u64> {
    value
        .split('/')
        .nth(1)
        .map(|s| s.trim())
        .and_then(|s| s.parse().ok())
}

/// 解析 `Content-Disposition` 中的文件名（`filename*` 优先于 `filename`）。
fn filename_from_content_disposition(cd: &str) -> Option<String> {
    let mut plain = None;
    let mut extended = None;
    for seg in cd.split(';') {
        let seg = seg.trim();
        if let Some(rest) = strip_ci(seg, "filename*=") {
            // RFC 5987: charset'lang'percent-encoded。charset 必须尊重：
            // 部分站点声明 gb2312/gbk，按 UTF-8 解会得到一串 U+FFFD
            let mut parts = rest.splitn(3, '\'');
            let charset = parts.next();
            let value = parts.nth(1).unwrap_or("");
            let decoded = charset_text::decode_percent_text_with_charset(value, charset);
            extended = sanitize_filename(&decoded);
        } else if let Some(rest) = strip_ci(seg, "filename=") {
            let value = rest.trim().trim_matches('"');
            let unescaped = value.replace("\\\"", "\"").replace("\\\\", "\\");
            plain = sanitize_filename(&unescaped);
        }
    }
    extended.or(plain)
}

/// 从 URL 路径解析文件名（percent 解码后取最后一段）。
pub(crate) fn filename_from_url(url: &str) -> Option<String> {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let last = path.rsplit('/').next().unwrap_or("");
    if last.is_empty() {
        return None;
    }
    // URL 路径里的中文常被站点按 GBK 百分号编码（%CF%C2%D4%D8 → 下载），
    // 按 UTF-8 lossy 解会得到 `����`，交给字符集探测解码
    let decoded = charset_text::decode_percent_text(last);
    sanitize_filename(&decoded)
}

/// 去掉路径分隔符与控制字符；结果为空返回 None。
fn sanitize_filename(name: &str) -> Option<String> {
    let cleaned: String = name
        .chars()
        .filter(|&c| c != '/' && c != '\\' && c != '\0' && !c.is_control())
        .collect();
    let trimmed = cleaned.trim().trim_matches('.');
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn strip_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&s[prefix.len()..])
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cd(value: &str) -> Option<String> {
        filename_from_content_disposition(value)
    }

    #[test]
    fn parses_content_disposition() {
        assert_eq!(
            cd(r#"attachment; filename="foo.zip""#),
            Some("foo.zip".into())
        );
        assert_eq!(
            cd("attachment; filename=bar.tar.gz"),
            Some("bar.tar.gz".into())
        );
        // filename* 优先
        assert_eq!(
            cd("attachment; filename=\"a.zip\"; filename*=UTF-8''%E4%B8%AD%E6%96%87.zip"),
            Some("中文.zip".into())
        );
        // 引号内转义
        assert_eq!(
            cd(r#"attachment; filename="we \"quote\" it.zip""#),
            Some("we \"quote\" it.zip".into())
        );
        // 危险字符过滤
        assert_eq!(
            cd(r#"attachment; filename="../../etc/passwd""#),
            Some("etcpasswd".into())
        );
        assert_eq!(cd("attachment; filename="), None);
    }

    /// 回归：`filename*` 的 charset 必须被尊重。中文下载站常声明
    /// gb2312/gbk，按 UTF-8 lossy 解会得到一串 U+FFFD。
    #[test]
    fn parses_content_disposition_gbk_charset() {
        // %B2%E2%CA%D4 是 GBK 的「测试」
        assert_eq!(
            cd("attachment; filename*=gb2312''%B2%E2%CA%D4.zip"),
            Some("测试.zip".into())
        );
        // 声明为 utf-8 的照旧按 UTF-8
        assert_eq!(
            cd("attachment; filename*=UTF-8''%E6%B5%8B%E8%AF%95.zip"),
            Some("测试.zip".into())
        );
    }

    #[test]
    fn parses_url_filename() {
        assert_eq!(
            filename_from_url("http://x/a/b/file.zip?token=1"),
            Some("file.zip".into())
        );
        assert_eq!(
            filename_from_url("http://x/%E4%B8%AD.zip"),
            Some("中.zip".into())
        );
        // 回归：GBK 百分号编码的 URL 路径不能解成一串 U+FFFD
        // （%CF%C2%D4%D8 是 GBK 的「下载」）
        assert_eq!(
            filename_from_url("http://x/%CF%C2%D4%D8.zip?token=1"),
            Some("下载.zip".into())
        );
        assert_eq!(filename_from_url("http://x/dir/"), None);
        assert_eq!(filename_from_url("http://x/"), None);
    }

    #[test]
    fn parses_content_range() {
        assert_eq!(total_from_content_range("bytes 0-1/12345"), Some(12345));
        assert_eq!(total_from_content_range("bytes 100-199/*"), None);
        assert_eq!(total_from_content_range("garbage"), None);
    }

    /// 端到端：本地 axum 服务，验证 206 续传与 200 重启语义。
    #[tokio::test]
    async fn download_with_range_semantics() {
        use axum::http::{header, HeaderValue, StatusCode};
        /// 测试用内存 sink。
        struct VecSink {
            buf: Vec<u8>,
        }
        impl TransferSink for VecSink {
            fn begin(&mut self, restarted: bool) -> std::io::Result<u64> {
                if restarted {
                    self.buf.clear();
                }
                Ok(self.buf.len() as u64)
            }
            fn write_chunk(&mut self, data: &[u8]) -> std::io::Result<()> {
                self.buf.extend_from_slice(data);
                Ok(())
            }
            fn finish(&mut self) -> std::io::Result<u64> {
                Ok(self.buf.len() as u64)
            }
        }

        let data = std::sync::Arc::new(vec![7u8; 1024]);
        let data_range = data.clone();
        let data_plain = data.clone();

        let app = axum::Router::new()
            .route(
                "/file.bin",
                axum::routing::get(move |headers: axum::http::HeaderMap| {
                    let data = data_range.clone();
                    async move {
                        let range = headers
                            .get(header::RANGE)
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or_default()
                            .to_string();
                        if range == "bytes=0-0" {
                            let mut r =
                                axum::response::Response::new(axum::body::Body::from(vec![
                                    data[0],
                                ]));
                            *r.status_mut() = StatusCode::PARTIAL_CONTENT;
                            r.headers_mut().insert(
                                header::CONTENT_RANGE,
                                HeaderValue::from_str(&format!("bytes 0-1/{}", data.len()))
                                    .unwrap(),
                            );
                            return r;
                        }
                        if let Some(start) = range
                            .strip_prefix("bytes=")
                            .and_then(|r| r.trim_end_matches('-').parse::<usize>().ok())
                        {
                            if start < data.len() {
                                let mut r = axum::response::Response::new(axum::body::Body::from(
                                    data[start..].to_vec(),
                                ));
                                *r.status_mut() = StatusCode::PARTIAL_CONTENT;
                                r.headers_mut().insert(
                                    header::CONTENT_RANGE,
                                    HeaderValue::from_str(&format!(
                                        "bytes {}-{}/{}",
                                        start,
                                        data.len() - 1,
                                        data.len()
                                    ))
                                    .unwrap(),
                                );
                                return r;
                            }
                        }
                        axum::response::Response::new(axum::body::Body::from(data.as_ref().clone()))
                    }
                }),
            )
            .route(
                "/no-range.bin",
                axum::routing::get(move || {
                    let data = data_plain.clone();
                    async move {
                        axum::response::Response::new(axum::body::Body::from(data.as_ref().clone()))
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });

        let client = build_client();
        let cancel = CancellationToken::new();

        // 探测：支持 Range
        let p = probe(&client, &format!("http://{addr}/file.bin"), &cancel)
            .await
            .unwrap();
        assert!(p.accepts_ranges);
        assert_eq!(p.total_len, Some(1024));

        // 从 512 续传
        let mut sink = VecSink {
            buf: vec![7u8; 512],
        };
        let done = download(
            &client,
            &format!("http://{addr}/file.bin"),
            512,
            &cancel,
            &mut sink,
            None,
        )
        .await
        .unwrap();
        assert_eq!(done.transferred, 512);
        assert_eq!(done.total_len, Some(1024));
        assert!(!done.restarted_from_zero);
        assert_eq!(sink.buf.len(), 1024);
        assert_eq!(&sink.buf[508..512], &[7, 7, 7, 7]);

        // 不支持 Range 的服务器：带 start 请求返回 200 → 重启语义（sink 被截断）
        let mut sink = VecSink {
            buf: vec![0u8; 512],
        };
        let done = download(
            &client,
            &format!("http://{addr}/no-range.bin"),
            512,
            &cancel,
            &mut sink,
            None,
        )
        .await
        .unwrap();
        assert!(done.restarted_from_zero);
        assert_eq!(done.transferred, 1024);
        assert_eq!(sink.buf.len(), 1024);

        // 取消令牌
        let cancel2 = CancellationToken::new();
        cancel2.cancel();
        assert!(matches!(
            download(
                &client,
                &format!("http://{addr}/file.bin"),
                0,
                &cancel2,
                &mut VecSink { buf: vec![] },
                None,
            )
            .await,
            Err(HttpError::Cancelled)
        ));

        // 404
        let app404 = axum::Router::new().route(
            "/missing",
            axum::routing::get(|| async { StatusCode::NOT_FOUND }),
        );
        let l2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a2 = l2.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l2, app404).await });
        assert!(matches!(
            probe(&client, &format!("http://{a2}/missing"), &cancel).await,
            Err(HttpError::Http(404))
        ));
    }

    /// no-proxy 生效：代理地址不可达时，列表内的主机仍能直连成功。
    ///
    /// 回归场景——代理环境（企业网/校园网强制代理）里 `all-proxy` 配了代理，
    /// `no-proxy` 列出局域网/回环地址；此前 no-proxy 只被存储从不生效，
    /// 这些本该直连的地址被塞进代理后请求必然失败。
    #[tokio::test]
    async fn no_proxy_bypasses_unreachable_proxy() {
        let app = axum::Router::new().route(
            "/probe",
            axum::routing::get(|| async { axum::http::StatusCode::OK }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });

        let cancel = CancellationToken::new();

        // 代理指向必然连不上的端口：只有 no-proxy 放行 127.0.0.1 才可能成功
        let client = build_client_with(None, Some("http://127.0.0.1:1"), Some("127.0.0.1"));
        let p = probe(&client, &format!("http://{addr}/probe"), &cancel).await;
        assert!(p.is_ok(), "no-proxy 未生效，请求走了不可达代理: {p:?}");
    }

    // ------------------------------------------------------------------
    // 逐任务自定义请求头：Referer / Cookie / User-Agent
    // ------------------------------------------------------------------

    #[test]
    fn parse_header_lines_keeps_real_headers_only() {
        let parsed = parse_header_lines([
            "Referer: https://example.com/watch?v=1",
            "cookie: sid=abc; t=1",
            "User-Agent: LerxuTest/1.0",
            // 以下都必须被丢弃
            "Range: bytes=0-1",       // 覆盖分段区间会让位图与磁盘错位
            "Host: evil.example",     // 虚拟主机由客户端按 URL 决定
            "Content-Length: 999",    // 引擎不发请求体
            "Connection: close",      // 连接管理归客户端
            "Accept-Encoding: gzip",  // 压缩编码归客户端
            "X-Empty:",               // 空值
            "BadHeaderLine",          // 无冒号
            "",
        ]);
        let names: Vec<&str> = parsed.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(names, vec!["Referer", "cookie", "User-Agent"]);
        assert_eq!(parsed[0].1, "https://example.com/watch?v=1");
        assert_eq!(parsed[1].1, "sid=abc; t=1");
        assert_eq!(parsed[2].1, "LerxuTest/1.0");
    }

    #[test]
    fn parse_header_lines_later_value_wins() {
        let parsed = parse_header_lines(["Referer: https://a", "referer: https://b"]);
        assert_eq!(parsed.len(), 1, "同名头只保留一条");
        assert_eq!(parsed[0].0, "Referer");
        assert_eq!(parsed[0].1, "https://b");
    }

    /// 头注入防护：值里带 CR/LF 的行必须整行丢弃，否则可以伪造出
    /// 第二条请求头（例如偷偷把 Cookie 覆盖掉）。
    #[test]
    fn parse_header_lines_rejects_crlf_injection() {
        assert!(parse_header_lines(["X-Injected: a\r\nCookie: evil=1"]).is_empty());
        assert!(parse_header_lines(["X-Injected: a\nCookie: evil=1"]).is_empty());
    }

    /// 自定义请求头必须真的发到线上（探测与下载两条路径都要带），
    /// 且不得覆盖引擎自己算的 Range。
    #[tokio::test]
    async fn custom_headers_reach_the_wire() {
        /// 内存 sink（断言下载字节数即可）。
        struct BufSink {
            buf: Vec<u8>,
        }
        impl TransferSink for BufSink {
            fn begin(&mut self, restarted: bool) -> std::io::Result<u64> {
                if restarted {
                    self.buf.clear();
                }
                Ok(self.buf.len() as u64)
            }
            fn write_chunk(&mut self, data: &[u8]) -> std::io::Result<()> {
                self.buf.extend_from_slice(data);
                Ok(())
            }
            fn finish(&mut self) -> std::io::Result<u64> {
                Ok(self.buf.len() as u64)
            }
        }

        type Seen = std::sync::Arc<std::sync::Mutex<Vec<(String, String, String, String)>>>;
        let seen: Seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let app = axum::Router::new().route(
            "/file.bin",
            axum::routing::get({
                let seen = seen.clone();
                move |headers: axum::http::HeaderMap| {
                    let seen = seen.clone();
                    async move {
                        let get = |n: &str| {
                            headers
                                .get(n)
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or_default()
                                .to_string()
                        };
                        seen.lock().unwrap().push((
                            get("referer"),
                            get("cookie"),
                            get("user-agent"),
                            get("range"),
                        ));
                        let body = vec![7u8; 1024];
                        let mut resp =
                            axum::response::Response::new(axum::body::Body::from(body));
                        if get("range").is_empty() {
                            *resp.status_mut() = axum::http::StatusCode::OK;
                        } else {
                            *resp.status_mut() = axum::http::StatusCode::PARTIAL_CONTENT;
                            resp.headers_mut().insert(
                                "content-range",
                                "bytes 0-1023/1024".parse().unwrap(),
                            );
                        }
                        resp
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });

        let headers = parse_header_lines([
            "Referer: https://example.com/page",
            "Cookie: sid=1; t=2",
            "User-Agent: LerxuTest/1.0",
        ]);
        let cancel = CancellationToken::new();
        let client = build_client();

        // 探测路径
        let p = probe_with(&client, &format!("http://{addr}/file.bin"), &cancel, &headers)
            .await
            .expect("带头的探测应成功");
        assert_eq!(p.total_len, Some(1024));

        // 单连接下载路径
        let mut sink = BufSink { buf: vec![] };
        let done = download_with(
            &client,
            &format!("http://{addr}/file.bin"),
            0,
            &cancel,
            &mut sink,
            None,
            &headers,
        )
        .await
        .expect("带头的下载应成功");
        assert_eq!(done.transferred, 1024);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2, "探测 + 下载各一次请求");
        for (referer, cookie, ua, _) in seen.iter() {
            assert_eq!(referer, "https://example.com/page");
            assert_eq!(cookie, "sid=1; t=2");
            assert_eq!(ua, "LerxuTest/1.0");
        }
        // 探测自带 Range（引擎算的），下载 start=0 不带 Range
        assert_eq!(seen[0].3, "bytes=0-0");
        assert_eq!(seen[1].3, "");
    }
}
