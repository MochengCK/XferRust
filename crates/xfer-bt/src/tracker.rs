//! HTTP tracker announce（BEP 3）：请求构造与响应解析（compact/非 compact）。

use std::net::SocketAddr;

use xfer_bencode::{decode, Value};
use xfer_types::{InfoHash, PeerId};

/// announce 请求参数。
pub struct AnnounceRequest<'a> {
    pub info_hash: &'a InfoHash,
    pub peer_id: &'a PeerId,
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    /// started / stopped / completed；None 为周期性 announce。
    pub event: Option<&'a str>,
    pub numwant: u32,
}

/// announce 响应。
#[derive(Debug, Clone, Default)]
pub struct AnnounceResponse {
    pub interval: u64,
    pub min_interval: Option<u64>,
    pub peers: Vec<SocketAddr>,
    pub failure: Option<String>,
    pub complete: Option<u64>,
    pub incomplete: Option<u64>,
}

/// 执行一次 announce。返回 Err 表示网络/解析失败（调用方按重试策略处理）。
pub async fn announce(
    client: &reqwest::Client,
    url: &str,
    req: &AnnounceRequest<'_>,
) -> Result<AnnounceResponse, String> {
    let mut query = format!(
        "info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact=1&numwant={}",
        percent_encode(req.info_hash.as_bytes()),
        percent_encode(&req.peer_id.0),
        req.port,
        req.uploaded,
        req.downloaded,
        req.left,
        req.numwant,
    );
    if let Some(ev) = req.event {
        query.push_str("&event=");
        query.push_str(ev);
    }
    let sep = if url.contains('?') { '&' } else { '?' };
    let full = format!("{url}{sep}{query}");
    let ua = format!("XferRust/{}", xfer_types::ENGINE_VERSION);
    let mut resp = client
        .get(&full)
        .header("User-Agent", &ua)
        .header("Accept", "text/plain, application/x-bencode")
        .send()
        .await
        .map_err(|e| format!("announce 请求失败: {e}"))?;
    let status = resp.status();
    // 限制响应体大小：正常 announce 响应仅几 KB（numwant 上限决定
    // peers 数量），失控/恶意 tracker 不得借响应体放大内存
    const MAX_BODY: usize = 4 * 1024 * 1024;
    let mut body = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| format!("announce 响应读取失败: {e}"))?
    {
        body.extend_from_slice(&chunk);
        if body.len() > MAX_BODY {
            return Err(format!("tracker {url} 响应体超过 {MAX_BODY} 字节，已截断读取"));
        }
    }
    if !status.is_success() {
        return Err(format!(
            "tracker {url} 返回 HTTP {status}（{} 字节）",
            body.len()
        ));
    }
    // 检测 HTML 响应（某些 tracker 对不规范请求返回错误页面）
    if !body.is_empty() && body[0] == b'<' {
        let snippet = String::from_utf8_lossy(&body[..body.len().min(200)]);
        return Err(format!(
            "tracker {url} 返回非 bencode 响应（HTML）：{snippet}"
        ));
    }
    parse_response(&body)
}

fn parse_response(body: &[u8]) -> Result<AnnounceResponse, String> {
    let v = decode(body).map_err(|e| format!("announce 响应 bencode 解析失败: {e}"))?;
    let d = v
        .as_dict()
        .ok_or_else(|| "announce 响应顶层必须是字典".to_string())?;
    let mut out = AnnounceResponse::default();

    if let Some(f) = d.get(b"failure reason".as_slice()).and_then(Value::as_str) {
        out.failure = Some(f.to_string());
        return Ok(out);
    }
    out.interval = d
        .get(b"interval".as_slice())
        .and_then(Value::as_int)
        .unwrap_or(1800)
        .max(0) as u64;
    out.min_interval = d
        .get(b"min interval".as_slice())
        .and_then(Value::as_int)
        .map(|n| n.max(0) as u64);
    out.complete = d
        .get(b"complete".as_slice())
        .and_then(Value::as_int)
        .map(|n| n.max(0) as u64);
    out.incomplete = d
        .get(b"incomplete".as_slice())
        .and_then(Value::as_int)
        .map(|n| n.max(0) as u64);

    match d.get(b"peers".as_slice()) {
        Some(Value::Bytes(b)) => out.peers = parse_compact(b)?,
        Some(Value::List(items)) => {
            for it in items {
                let Some(pd) = it.as_dict() else {
                    continue;
                };
                let ip = pd
                    .get(b"ip".as_slice())
                    .and_then(Value::as_str)
                    .ok_or_else(|| "非 compact peers 条目缺少 ip".to_string())?;
                let port = pd
                    .get(b"port".as_slice())
                    .and_then(Value::as_int)
                    .ok_or_else(|| "非 compact peers 条目缺少 port".to_string())?;
                if let Ok(addr) = format!("{ip}:{port}").parse::<SocketAddr>() {
                    out.peers.push(addr);
                }
            }
        }
        _ => {} // 无 peers 字段
    }
    Ok(out)
}

/// compact peers：每 6 字节 = 4 字节 IP + 2 字节端口（BE）。
///
/// 长度非 6 倍数时截断取整而非整体作废：真实 tracker 偶有尾部
/// 填充字节，为这几个坏字节丢掉整份几百个候选会直接拖慢冷启动。
fn parse_compact(b: &[u8]) -> Result<Vec<SocketAddr>, String> {
    if b.len() % 6 != 0 {
        tracing::warn!(
            len = b.len(),
            usable = b.len() / 6 * 6,
            "compact peers 长度非 6 的倍数，截断尾部"
        );
    }
    let mut out = Vec::with_capacity(b.len() / 6);
    for c in b.chunks_exact(6) {
        let ip = [c[0], c[1], c[2], c[3]];
        let port = u16::from_be_bytes([c[4], c[5]]);
        out.push(SocketAddr::from((ip, port)));
    }
    Ok(out)
}

/// 百分号编码（tracker 查询参数要求）。
fn percent_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 3);
    for &b in bytes {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_peers_parsed() {
        // 127.0.0.1:6881 + 10.0.0.1:51413 (0xC8D5)
        let raw = [127, 0, 0, 1, 0x1A, 0xE1, 10, 0, 0, 1, 0xC8, 0xD5];
        let addrs = parse_compact(&raw).unwrap();
        assert_eq!(addrs.len(), 2);
        assert_eq!(addrs[0], "127.0.0.1:6881".parse().unwrap());
        assert_eq!(addrs[1], "10.0.0.1:51413".parse().unwrap());
        // 不足一条 → 空列表而非报错
        assert!(parse_compact(&[1, 2, 3]).unwrap().is_empty());
    }

    #[test]
    fn compact_peers_truncates_ragged_tail() {
        // 1 个有效条目 + 3 字节尾部填充：应保留有效条目
        let raw = [127, 0, 0, 1, 0x1A, 0xE1, 0xAA, 0xBB, 0xCC];
        let addrs = parse_compact(&raw).unwrap();
        assert_eq!(addrs, vec!["127.0.0.1:6881".parse().unwrap()]);
    }

    #[test]
    fn response_with_failure() {
        use std::collections::BTreeMap;
        use xfer_bencode::{bytes, dict, encode};
        let v = dict(BTreeMap::from([(
            b"failure reason".to_vec(),
            bytes("unregistered torrent"),
        )]));
        let r = parse_response(&encode(&v)).unwrap();
        assert!(r.failure.is_some());
        assert!(r.peers.is_empty());
    }

    #[test]
    fn response_compact_roundtrip() {
        use std::collections::BTreeMap;
        use xfer_bencode::{bytes, dict, encode, int};
        let peers = [127, 0, 0, 1, 0x1A, 0xE1];
        let v = dict(BTreeMap::from([
            (b"interval".to_vec(), int(60)),
            (b"complete".to_vec(), int(3)),
            (b"peers".to_vec(), bytes(peers.to_vec())),
        ]));
        let r = parse_response(&encode(&v)).unwrap();
        assert_eq!(r.interval, 60);
        assert_eq!(r.complete, Some(3));
        assert_eq!(r.peers, vec!["127.0.0.1:6881".parse().unwrap()]);
    }

    #[test]
    fn percent_encoding() {
        assert_eq!(percent_encode(b"AZaz09-_.~"), "AZaz09-_.~");
        assert_eq!(percent_encode(&[0x00]), "%00");
        assert_eq!(percent_encode(&[0xFF, 0xAB]), "%FF%AB");
    }
}
