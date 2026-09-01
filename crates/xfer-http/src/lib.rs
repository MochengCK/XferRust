//! HTTP(S) 下载：探测（总长度 / 文件名 / Range 支持）、单连接流式下载
//! 与多连接分片下载（见 [`split`] 模块：单写线程调度 + 工作窃取对冲
//! + 段级控制文件断点续传）。

mod adaptive;
mod split;

pub use adaptive::{AdaptiveConfig, AdaptiveScheduler, ConnPerf, ScheduleAction};
pub use split::{ctrl_path, download_split, SplitDone, SplitOptions, SplitStats};

use std::time::Duration;

use futures_util::StreamExt;
use percent_encoding::percent_decode_str;
use tokio_util::sync::CancellationToken;

use xfer_types::{ENGINE_NAME, ENGINE_VERSION};

/// 构建 HTTP 客户端（全局共享）。
///
/// - UA 按引擎名/版本派生；
/// - 不启用自动解压（保证 Content-Length 与线上字节一致）；
/// - 连接 10s、读 30s 超时；无整体超时（大文件流式下载）；
/// - TCP_NODELAY：流式分块传输关闭 Nagle，避免小块合并延迟。
pub fn build_client() -> reqwest::Client {
    reqwest::Client::builder()
        .user_agent(format!("{ENGINE_NAME}/{ENGINE_VERSION}"))
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::default())
        .tcp_nodelay(true)
        .build()
        .expect("构建 HTTP 客户端失败")
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
    #[error("本地写入失败: {0}")]
    Io(String),
    #[error("已取消")]
    Cancelled,
}

impl HttpError {
    /// 从 reqwest 错误归类。
    fn from_reqwest(e: &reqwest::Error) -> Self {
        if e.is_timeout() {
            Self::Timeout
        } else if e.is_connect() {
            Self::Connect(e.to_string())
        } else {
            Self::Protocol(e.to_string())
        }
    }

    /// 映射到线上协议的任务错误码。
    pub fn error_code(&self) -> i64 {
        match self {
            HttpError::Timeout => 2,
            HttpError::Http(_) => 3,
            HttpError::Connect(_) | HttpError::Protocol(_) | HttpError::NotSplittable(_) => 5,
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
    if cancel.is_cancelled() {
        return Err(HttpError::Cancelled);
    }
    let resp = client
        .get(url)
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

    Ok(Probe {
        total_len,
        filename,
        accepts_ranges,
        final_url,
    })
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
/// 取消令牌触发时返回 [`HttpError::Cancelled`]（已写入部分由调用方保留）。
pub async fn download(
    client: &reqwest::Client,
    url: &str,
    start: u64,
    cancel: &CancellationToken,
    sink: &mut dyn TransferSink,
) -> Result<TransferDone, HttpError> {
    if cancel.is_cancelled() {
        return Err(HttpError::Cancelled);
    }
    let mut req = client.get(url);
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
            // RFC 5987: charset'lang'percent-encoded
            let value = rest.splitn(3, '\'').nth(2).unwrap_or("");
            let decoded = percent_decode_str(value).decode_utf8_lossy().to_string();
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
fn filename_from_url(url: &str) -> Option<String> {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let last = path.rsplit('/').next().unwrap_or("");
    if last.is_empty() {
        return None;
    }
    let decoded = percent_decode_str(last).decode_utf8_lossy().to_string();
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
                &mut VecSink { buf: vec![] }
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
}
