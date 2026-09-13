//! 百分号编码 / 非 UTF-8 字节的字符集友好解码。
//!
//! 中文站点（BT 站、网盘、下载站）习惯把中文按 **GBK/GB2312** 做百分号编码，
//! 例如 `magnet:?dn=%B2%E2%CA%D4`（GBK 的「测试」）或
//! `http://host/%CF%C2%D4%D8.zip`。这类字节不是合法 UTF-8，直接
//! `String::from_utf8_lossy` 会把每个字节变成一个 U+FFFD，界面上就是一串
//! `����`（既不可读也不可搜索），`std::str::from_utf8` 则直接报错。
//!
//! 本模块按下面的顺序解码，只在前面全部失败时才退回 lossy：
//!   1. 显式声明的字符集（RFC 5987 `filename*=gb2312'zh'%XX`）；
//!   2. 严格 UTF-8（现代站点与 BEP 3 要求的编码，命中最常见）；
//!   3. GB18030（GBK/GB2312 的超集，中文站点的实际编码）。
//!
//! 注意：GB18030 覆盖面很广，非 UTF-8 的其它单/双字节编码（如 Shift_JIS、
//! Latin-1）也可能被"解成功"成别的字符——这是无法完全避免的猜测，但与
//! 现状（保证输出 `����`）相比仍是严格改进，且中文站点的绝大多数输入是 GBK。

use encoding_rs::Encoding;

/// 解码字节为文本：严格 UTF-8 → GB18030 → lossy。
pub fn decode_text(bytes: &[u8]) -> String {
    if let Ok(s) = std::str::from_utf8(bytes) {
        return s.to_string();
    }
    if let Some(s) = decode_with(bytes, encoding_rs::GB18030) {
        return s;
    }
    String::from_utf8_lossy(bytes).into_owned()
}

/// 解码字节为文本，`charset` 为显式声明的字符集标签（如 `gb2312`/`utf-8`）
/// 时优先按声明解码；标签未知或按声明解不出（含非法字节）时回退到
/// [`decode_text`] 的探测顺序。
pub fn decode_text_with_charset(bytes: &[u8], charset: Option<&str>) -> String {
    if let Some(label) = charset {
        let label = label.trim();
        if !label.is_empty() {
            // WHATWG 标签映射：gb2312/gbk/cp936 → GBK，utf-8 → UTF-8
            if let Some(enc) = Encoding::for_label(label.as_bytes()) {
                if let Some(s) = decode_with(bytes, enc) {
                    return s;
                }
            }
        }
    }
    decode_text(bytes)
}

/// 百分号解码（`%XX` → 字节；无效转义按原样保留字节），再按字符集探测解码。
pub fn decode_percent_text(input: &str) -> String {
    decode_text(&percent_to_bytes(input))
}

/// 同 [`decode_percent_text`]，但带显式声明的字符集标签。
pub fn decode_percent_text_with_charset(input: &str, charset: Option<&str>) -> String {
    decode_text_with_charset(&percent_to_bytes(input), charset)
}

/// `%XX` → 字节；无效转义与普通字符按原字节保留。
pub fn percent_to_bytes(input: &str) -> Vec<u8> {
    let bytes = input.as_bytes();
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
    out
}

/// 按指定编码解码；出现非法字节序列（malformed）时返回 None 交由调用方回退。
fn decode_with(bytes: &[u8], enc: &'static Encoding) -> Option<String> {
    let (text, _, malformed) = enc.decode(bytes);
    if malformed {
        None
    } else {
        Some(text.into_owned())
    }
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_percent_kept() {
        // 现代站点：UTF-8 百分号编码（%E6%B5%8B%E8%AF%95 = 测试）
        assert_eq!(decode_percent_text("%E6%B5%8B%E8%AF%95"), "测试");
        assert_eq!(decode_percent_text("Plain%20Name"), "Plain Name");
    }

    #[test]
    fn gbk_percent_decoded() {
        // 中文站点：GBK 百分号编码（%B2%E2%CA%D4 = 测试）
        assert_eq!(decode_percent_text("%B2%E2%CA%D4"), "测试");
        // 混合大小写与其它 GBK 字样（%CF%C2%D4%D8 = 下载）
        assert_eq!(decode_percent_text("%cf%c2%d4%d8"), "下载");
        // 明文中文直接透传
        assert_eq!(decode_percent_text("直接中文"), "直接中文");
    }

    #[test]
    fn explicit_charset_wins() {
        assert_eq!(
            decode_percent_text_with_charset("%B2%E2%CA%D4", Some("gb2312")),
            "测试"
        );
        assert_eq!(
            decode_percent_text_with_charset("%E6%B5%8B%E8%AF%95", Some("utf-8")),
            "测试"
        );
        // 标签未知：回退到探测
        assert_eq!(
            decode_percent_text_with_charset("%B2%E2%CA%D4", Some("x-unknown")),
            "测试"
        );
    }

    #[test]
    fn gbk_bytes_without_percent() {
        // 种子里的 name/path 是原始字节（未百分号编码），编码同样可能是 GBK
        assert_eq!(decode_text(&[0xB2, 0xE2, 0xCA, 0xD4]), "测试");
        assert_eq!(decode_text(b"plain-ascii.bin"), "plain-ascii.bin");
    }

    #[test]
    fn invalid_escape_kept_and_lossy_fallback() {
        // 无效转义按原样保留，不会丢字符
        assert_eq!(decode_percent_text("100%ok"), "100%ok");
        assert_eq!(decode_percent_text("%zz"), "%zz");
        // 完全无法解码的字节不 panic，退回 lossy（而不是报错）
        let s = decode_text(&[0xFF, 0xFE, 0x00]);
        assert!(!s.is_empty());
    }
}
