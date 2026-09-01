//! 磁力链接解析（BEP 9 的 xt 参数 + 常见参数）。

/// 解析结果。
#[derive(Debug, Clone)]
pub struct Magnet {
    pub info_hash: [u8; 20],
    pub display_name: Option<String>,
    pub trackers: Vec<String>,
}

/// 解析磁力链接。
///
/// 支持 `magnet:?xt=urn:btih:<40hex>`（小写十六进制）与 base32 编码
/// （32 位不含 padding 的 RFC 4648 变体，btih 常见 32 字符形式）。
pub fn parse_magnet(uri: &str) -> Result<Magnet, String> {
    let rest = uri
        .strip_prefix("magnet:")
        .ok_or_else(|| "不是 magnet 链接".to_string())?;
    let rest = match rest.strip_prefix('?') {
        Some(r) => r,
        None => rest,
    };
    let mut info_hash: Option<[u8; 20]> = None;
    let mut display_name: Option<String> = None;
    let mut trackers = Vec::new();

    for part in rest.split('&') {
        let Some((k, v)) = part.split_once('=') else {
            continue;
        };
        let v = percent_decode(v);
        match k {
            "xt" => {
                if let Some(h) = v.strip_prefix("urn:btih:") {
                    info_hash = Some(parse_btih(h)?);
                }
            }
            "dn" => display_name = Some(v),
            "tr" if !v.is_empty() => trackers.push(v),
            _ => {}
        }
    }
    let info_hash = info_hash.ok_or_else(|| "缺少 xt=urn:btih:<hash>".to_string())?;
    Ok(Magnet {
        info_hash,
        display_name,
        trackers,
    })
}

fn parse_btih(s: &str) -> Result<[u8; 20], String> {
    // 40 hex
    if s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        let bytes = hex::decode(s).map_err(|_| "btih hex 非法".to_string())?;
        let mut out = [0u8; 20];
        out.copy_from_slice(&bytes);
        return Ok(out);
    }
    // 32 base32（RFC 4648 无 padding）
    if s.len() == 32 {
        if let Some(bytes) = base32_decode(s) {
            return Ok(bytes);
        }
    }
    Err("btih 哈希必须是 40 位 hex 或 32 位 base32".into())
}

/// 百分号解码（无效转义按原样保留）。
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push(h << 4 | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// RFC 4648 base32 解码（无 padding），失败返回 None。
fn base32_decode(s: &str) -> Option<[u8; 20]> {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut bits: u64 = 0;
    let mut nbits = 0u32;
    let mut out = Vec::with_capacity(20);
    for c in s.bytes() {
        let v = ALPHABET.iter().position(|&a| a == c)? as u64;
        bits = (bits << 5) | v;
        nbits += 5;
        if nbits >= 8 {
            nbits -= 8;
            out.push((bits >> nbits) as u8);
            bits &= (1u64 << nbits) - 1;
        }
    }
    if out.len() != 20 {
        return None;
    }
    let mut a = [0u8; 20];
    a.copy_from_slice(&out);
    Some(a)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hex_btih() {
        let m = parse_magnet(
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=foo&tr=http://t/ann",
        )
        .unwrap();
        assert_eq!(
            m.info_hash,
            hex::decode("0123456789abcdef0123456789abcdef01234567").unwrap()[..]
        );
        assert_eq!(m.display_name.as_deref(), Some("foo"));
        assert_eq!(m.trackers, vec!["http://t/ann".to_string()]);
    }

    #[test]
    fn parse_base32_btih() {
        // "0123456789abcdef0123456789abcdef01234567" 的 base32（无 padding）
        let hex_hash = "0123456789abcdef0123456789abcdef01234567";
        let b32 = base32_encode(hex::decode(hex_hash).unwrap().as_slice());
        let m = parse_magnet(&format!("magnet:?xt=urn:btih:{b32}")).unwrap();
        assert_eq!(m.info_hash, hex::decode(hex_hash).unwrap()[..]);
    }

    #[test]
    fn percent_decoding() {
        let m = parse_magnet(
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=%E6%B5%8B%E8%AF%95",
        )
        .unwrap();
        assert_eq!(m.display_name.as_deref(), Some("测试"));
    }

    #[test]
    fn rejects_bad() {
        assert!(parse_magnet("http://example.com").is_err());
        assert!(parse_magnet("magnet:?dn=no-hash").is_err());
        assert!(parse_magnet("magnet:?xt=urn:btih:zz").is_err());
    }

    fn base32_encode(data: &[u8]) -> String {
        const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        let mut out = String::new();
        let mut bits: u64 = 0;
        let mut nbits = 0u32;
        for &b in data {
            bits = (bits << 8) | b as u64;
            nbits += 8;
            while nbits >= 5 {
                nbits -= 5;
                out.push(ALPHABET[((bits >> nbits) & 0x1F) as usize] as char);
            }
            bits &= (1u64 << nbits) - 1;
        }
        if nbits > 0 {
            out.push(ALPHABET[((bits << (5 - nbits)) & 0x1F) as usize] as char);
        }
        out
    }
}
