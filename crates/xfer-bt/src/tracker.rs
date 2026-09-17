//! HTTP tracker announce（BEP 3）：请求构造与响应解析（compact/非 compact）。

use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::time::Duration;

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
                let Some(ip) = pd.get(b"ip".as_slice()).and_then(Value::as_str) else {
                    continue;
                };
                let Some(port) = pd.get(b"port".as_slice()).and_then(Value::as_int) else {
                    continue;
                };
                if !(0..=65535).contains(&port) {
                    continue;
                }
                // 非 compact 的 ip 是裸字面量（IPv6 不带方括号）。拼成
                // "ip:port" 再 parse 对 IPv6 必然失败——"2a02:2479:44:8f00::1:6965"
                // 会被当成合法的 8 组 IPv6 地址（端口被吞进地址），于是整条
                // IPv6 peer 被静默丢弃。Ubuntu tracker 对 IPv6 源返回的正是
                // 非 compact 字典列表，丢弃它等于丢掉全部公网 IPv6 seeder。
                if let Ok(ip) = ip.parse::<IpAddr>() {
                    out.peers.push(SocketAddr::new(ip, port as u16));
                }
            }
        }
        _ => {} // 无 peers 字段
    }
    // BEP 32：部分 tracker 用独立字段 peers6（18 字节/条 compact）返回 IPv6 peer
    if let Some(Value::Bytes(b)) = d.get(b"peers6".as_slice()) {
        out.peers.extend(parse_compact6(b));
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

/// peers6（BEP 32 compact）：每 18 字节 = 16 字节 IPv6 + 2 字节端口（BE）。
fn parse_compact6(b: &[u8]) -> Vec<SocketAddr> {
    if b.len() % 18 != 0 {
        tracing::warn!(
            len = b.len(),
            usable = b.len() / 18 * 18,
            "compact peers6 长度非 18 的倍数，截断尾部"
        );
    }
    let mut out = Vec::with_capacity(b.len() / 18);
    for c in b.chunks_exact(18) {
        let mut ip = [0u8; 16];
        ip.copy_from_slice(&c[..16]);
        let port = u16::from_be_bytes([c[16], c[17]]);
        if port != 0 {
            out.push(SocketAddr::from((Ipv6Addr::from(ip), port)));
        }
    }
    out
}

/// 强制经 IPv6 追加一次 announce（BEP 7/32）。
///
/// tracker 域名通常同时有 A/AAAA 记录，而系统解析顺序是 IPv4 优先，
/// 默认 HTTP 客户端必然经 IPv4 访问；tracker 只把 IPv6 peer 返回给
/// IPv6 来源（BEP 7），于是 IPv6 seeder 一个都拿不到。这里用 `resolve`
/// 把域名钉到该主机的 IPv6 地址上再 announce 一次，与 IPv4 结果合并。
pub async fn announce_via_ipv6(
    url: &str,
    req: &AnnounceRequest<'_>,
) -> Result<AnnounceResponse, String> {
    let parsed = reqwest::Url::parse(url).map_err(|e| format!("tracker URL 非法: {e}"))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| "tracker URL 缺少主机名".to_string())?
        .to_string();
    if host.parse::<IpAddr>().is_ok() {
        // IP 直连：announce 已在该地址族上，无需附加
        return Err("tracker 使用 IP 直连，跳过 IPv6 附加 announce".into());
    }
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| "tracker URL 缺少端口".to_string())?;
    let addrs = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|e| format!("tracker 域名解析失败: {e}"))?;
    let v6 = addrs
        .filter(|a| a.is_ipv6())
        .next()
        .ok_or_else(|| "tracker 无 IPv6 地址".to_string())?;
    let ua = format!("XferRust/{}", xfer_types::ENGINE_VERSION);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(10))
        .user_agent(ua)
        .resolve(&host, v6)
        .build()
        .map_err(|e| format!("IPv6 HTTP 客户端构建失败: {e}"))?;
    announce(&client, url, req).await
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
    fn non_compact_ipv6_peers_parsed() {
        // Ubuntu tracker 对 IPv6 源返回的形态：非 compact 字典列表，ip 为裸
        // IPv6 字面量（无方括号），必须能解析出 IPv6 peer（含端口）
        use std::collections::BTreeMap;
        use xfer_bencode::{bytes, dict, encode, int, list};
        let entry = |ip: &str, port: i64| {
            dict(BTreeMap::from([
                (b"ip".to_vec(), bytes(ip.to_string())),
                (b"port".to_vec(), int(port)),
                (b"peer id".to_vec(), bytes("-lt0D80-012345678901")),
            ]))
        };
        let v = dict(BTreeMap::from([
            (b"interval".to_vec(), int(1800)),
            (
                b"peers".to_vec(),
                list(vec![
                    entry("185.125.190.59", 6893),
                    entry("2001:41d0:700:2413::1", 6769),
                    entry("not-an-ip", 1234),
                ]),
            ),
        ]));
        let r = parse_response(&encode(&v)).unwrap();
        assert_eq!(
            r.peers,
            vec![
                "185.125.190.59:6893".parse().unwrap(),
                "[2001:41d0:700:2413::1]:6769".parse().unwrap(),
            ]
        );
    }

    #[test]
    fn peers6_compact_parsed() {
        use std::collections::BTreeMap;
        use xfer_bencode::{bytes, dict, encode, int};
        let mut compact = Vec::new();
        compact.extend_from_slice(&"2001:41d0:700:2413::1".parse::<Ipv6Addr>().unwrap().octets());
        compact.extend_from_slice(&6769u16.to_be_bytes());
        let v = dict(BTreeMap::from([
            (b"interval".to_vec(), int(1800)),
            (b"peers6".to_vec(), bytes(compact.clone())),
        ]));
        let r = parse_response(&encode(&v)).unwrap();
        assert_eq!(r.peers, vec!["[2001:41d0:700:2413::1]:6769".parse().unwrap()]);
        // 端口 0 与长度非 18 倍数的尾部都应被丢弃
        assert!(parse_compact6(&[0u8; 18]).is_empty());
        let mut ragged = compact.clone();
        ragged.extend_from_slice(&[1, 2, 3]);
        assert_eq!(parse_compact6(&ragged).len(), 1);
    }

    #[test]
    fn percent_encoding() {
        assert_eq!(percent_encode(b"AZaz09-_.~"), "AZaz09-_.~");
        assert_eq!(percent_encode(&[0x00]), "%00");
        assert_eq!(percent_encode(&[0xFF, 0xAB]), "%FF%AB");
    }
}
