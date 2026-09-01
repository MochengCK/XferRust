//! uTP (BEP 29) 包格式：编解码 + SACK 扩展。
//!
//! 头部固定 20 字节，大端：
//! ```text
//! 0       4       8              16             24             32
//! +-------+-------+---------------+---------------+---------------+
//! | type  | ver   | extension     | connection_id                 |
//! +-------+-------+---------------+---------------+---------------+
//! | timestamp_microseconds                                         |
//! +---------------+---------------+---------------+---------------+
//! | timestamp_difference_microseconds                             |
//! +---------------+---------------+---------------+---------------+
//! | wnd_size                                                       |
//! +---------------+---------------+---------------+---------------+
//! | seq_nr                         | ack_nr                         |
//! +---------------+---------------+---------------+---------------+
//! ```
//!
//! SACK 扩展 (type=1)：payload 至少 4 字节（32 位 bitmap），
//! 字节序为"逆序"——第一个字节 LSB = ack_nr+2，MSB = ack_nr+9。

/// uTP 协议版本 1。
pub const PROTOCOL_VERSION: u8 = 1;

/// 固定头部长度（字节）。
pub const HEADER_LEN: usize = 20;

/// 包类型。
pub mod packet_type {
    pub const ST_DATA: u8 = 0;
    pub const ST_FIN: u8 = 1;
    pub const ST_STATE: u8 = 2;
    pub const ST_RESET: u8 = 3;
    pub const ST_SYN: u8 = 4;
}

/// 扩展类型。
pub mod ext_type {
    pub const EXT_NONE: u8 = 0;
    pub const EXT_SACK: u8 = 1;
}

/// uTP 包头部。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PacketHeader {
    /// 包类型（高 4 位）。
    pub type_: u8,
    /// 版本（低 4 位）。固定为 1。
    pub version: u8,
    /// 第一个扩展类型（0 = 无扩展）。
    pub extension: u8,
    /// 连接 ID（发送方的接收 ID；SYN 也携带它）。
    pub connection_id: u16,
    /// 时间戳（微秒，发送方时钟）。
    pub timestamp: u32,
    /// 时间戳差值（微秒，单向延迟回显）。
    pub timestamp_diff: u32,
    /// 接收窗口（字节）。
    pub wnd_size: u32,
    /// 包序列号。
    pub seq_nr: u16,
    /// 确认号。
    pub ack_nr: u16,
}

/// 扩展头 (type, payload)。
pub type Extension = (u8, Vec<u8>);

/// 解析一个 uTP 包。
///
/// 返回 `(header, extensions, payload_offset)`：
/// - `extensions`：扩展链（按线上顺序）
/// - `payload_offset`：数据负载在 `data` 中的起始偏移
///
/// 版本不匹配或长度不足时返回 `None`。
pub fn parse_packet(data: &[u8]) -> Option<(PacketHeader, Vec<Extension>, usize)> {
    if data.len() < HEADER_LEN {
        return None;
    }
    let type_ver = data[0];
    let type_ = (type_ver >> 4) & 0x0F;
    let version = type_ver & 0x0F;
    if version != PROTOCOL_VERSION {
        return None;
    }
    let header = PacketHeader {
        type_,
        version,
        extension: data[1],
        connection_id: u16::from_be_bytes([data[2], data[3]]),
        timestamp: u32::from_be_bytes([data[4], data[5], data[6], data[7]]),
        timestamp_diff: u32::from_be_bytes([data[8], data[9], data[10], data[11]]),
        wnd_size: u32::from_be_bytes([data[12], data[13], data[14], data[15]]),
        seq_nr: u16::from_be_bytes([data[16], data[17]]),
        ack_nr: u16::from_be_bytes([data[18], data[19]]),
    };

    // 遍历扩展链
    let mut extensions = Vec::new();
    let mut off = HEADER_LEN;
    let mut ext_type = header.extension;
    while ext_type != ext_type::EXT_NONE {
        if off + 2 > data.len() {
            return None;
        }
        let next_type = data[off];
        let ext_len = data[off + 1] as usize;
        off += 2;
        if off + ext_len > data.len() {
            return None;
        }
        extensions.push((ext_type, data[off..off + ext_len].to_vec()));
        off += ext_len;
        ext_type = next_type;
    }

    Some((header, extensions, off))
}

/// 编码头部到 `out` 的前 `HEADER_LEN` 字节。
pub fn encode_header(out: &mut [u8], header: &PacketHeader) {
    assert!(out.len() >= HEADER_LEN);
    out[0] = ((header.type_ & 0x0F) << 4) | (header.version & 0x0F);
    out[1] = header.extension;
    out[2..4].copy_from_slice(&header.connection_id.to_be_bytes());
    out[4..8].copy_from_slice(&header.timestamp.to_be_bytes());
    out[8..12].copy_from_slice(&header.timestamp_diff.to_be_bytes());
    out[12..16].copy_from_slice(&header.wnd_size.to_be_bytes());
    out[16..18].copy_from_slice(&header.seq_nr.to_be_bytes());
    out[18..20].copy_from_slice(&header.ack_nr.to_be_bytes());
}

/// 解码 SACK bitmap（至少 4 字节）。
///
/// 返回 32 位 bitmap：bit 0 = ack_nr+2, bit 1 = ack_nr+3, ..., bit 31 = ack_nr+33。
/// 字节序为"逆序"：第一个字节的 LSB = ack_nr+2。
pub fn decode_sack_bits(payload: &[u8]) -> u32 {
    if payload.len() < 4 {
        return 0;
    }
    let mut bits = 0u32;
    for (i, b) in payload.iter().take(4).enumerate() {
        bits |= (*b as u32) << (8 * i);
    }
    bits
}

/// 追加 SACK 扩展到 `out`（在 `off` 处写入，返回新的偏移）。
///
/// 格式：`[next_type=0][len=4][4-byte bitmap LE]`
pub fn append_sack_extension(out: &mut Vec<u8>, off: usize, sack_bits: u32) -> usize {
    out.push(0); // next_type = EXT_NONE
    out.push(4); // len
    for i in 0..4 {
        out.push(((sack_bits >> (8 * i)) & 0xFF) as u8);
    }
    off + 6
}

/// 序列号比较工具函数（wrap-safe，u16 空间）：
/// `a` 是否在 `b` 之后（严格 after），wrap-safe。
#[inline]
pub fn seq_after(a: u16, b: u16) -> bool {
    (a.wrapping_sub(b) as i16) > 0
}

/// `a` 是否在 `b` 之前或等于（before-or-equal），wrap-safe。
#[inline]
pub fn seq_leq(a: u16, b: u16) -> bool {
    (a.wrapping_sub(b) as i16) <= 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_header_roundtrip() {
        let h = PacketHeader {
            type_: packet_type::ST_DATA,
            version: PROTOCOL_VERSION,
            extension: ext_type::EXT_NONE,
            connection_id: 0x1234,
            timestamp: 0xDEADBEEF,
            timestamp_diff: 0xCAFEBABE,
            wnd_size: 0x100000,
            seq_nr: 42,
            ack_nr: 17,
        };
        let mut buf = vec![0u8; HEADER_LEN];
        encode_header(&mut buf, &h);
        let (parsed, exts, payload_off) = parse_packet(&buf).unwrap();
        assert_eq!(parsed, h);
        assert!(exts.is_empty());
        assert_eq!(payload_off, HEADER_LEN);
    }

    #[test]
    fn parse_rejects_wrong_version() {
        let mut buf = vec![0u8; HEADER_LEN];
        buf[0] = (packet_type::ST_DATA << 4) | 2; // version=2
        assert!(parse_packet(&buf).is_none());
    }

    #[test]
    fn parse_rejects_short_packet() {
        let buf = vec![0u8; 10];
        assert!(parse_packet(&buf).is_none());
    }

    #[test]
    fn sack_bits_decode() {
        // 第一字节 0x01 = bit0 set = ack_nr+2 已收到
        let payload = [0x01, 0x00, 0x00, 0x00];
        assert_eq!(decode_sack_bits(&payload), 1);

        // 第一字节 0x80 = bit7 set = ack_nr+9 已收到
        let payload = [0x80, 0x00, 0x00, 0x00];
        assert_eq!(decode_sack_bits(&payload), 0x80);

        // 第二字节 0x01 = bit8 set = ack_nr+10
        let payload = [0x00, 0x01, 0x00, 0x00];
        assert_eq!(decode_sack_bits(&payload), 0x100);
    }

    #[test]
    fn sack_extension_roundtrip() {
        let mut pkt = vec![0u8; HEADER_LEN];
        // 设置 type=ST_DATA(0), version=1 → byte0 = (0<<4)|1 = 0x01
        pkt[0] = (packet_type::ST_DATA << 4) | PROTOCOL_VERSION;
        // 设置 extension = EXT_SACK
        pkt[1] = ext_type::EXT_SACK;
        let off = HEADER_LEN;
        let bits = 0x80000001; // bit0 + bit31
        let new_off = append_sack_extension(&mut pkt, off, bits);
        assert_eq!(new_off, HEADER_LEN + 6);
        assert_eq!(pkt.len(), HEADER_LEN + 6);

        let (hdr, exts, payload_off) = parse_packet(&pkt).unwrap();
        assert_eq!(hdr.extension, ext_type::EXT_SACK);
        assert_eq!(exts.len(), 1);
        assert_eq!(exts[0].0, ext_type::EXT_SACK);
        assert_eq!(decode_sack_bits(&exts[0].1), bits);
        assert_eq!(payload_off, HEADER_LEN + 6);
    }

    #[test]
    fn seq_comparison_wraps() {
        // 0xFFFF 之后是 0
        assert!(seq_after(0, 0xFFFF));
        assert!(!seq_after(0xFFFF, 0));
        assert!(seq_leq(0xFFFF, 0xFFFF));
        // 0 在 0xFFFF 之后（wrap 后），所以 0 不在 0xFFFF 之前
        assert!(!seq_leq(0, 0xFFFF));
        assert!(seq_leq(0xFFFF, 0xFFFF));
        // 1 在 2 之前
        assert!(seq_leq(1, 2));
        assert!(!seq_leq(2, 1));
    }

    #[test]
    fn syn_packet_format() {
        let h = PacketHeader {
            type_: packet_type::ST_SYN,
            version: PROTOCOL_VERSION,
            extension: ext_type::EXT_NONE,
            connection_id: 0xBEEF,
            timestamp: 123456,
            timestamp_diff: 0, // SYN 时无延迟样本
            wnd_size: 0x7FFFFFFF,
            seq_nr: 1,
            ack_nr: 0,
        };
        let mut buf = vec![0u8; HEADER_LEN];
        encode_header(&mut buf, &h);
        // 第一个字节: type=4(高4位) | version=1(低4位) = 0x41
        assert_eq!(buf[0], 0x41);
        let (parsed, _, _) = parse_packet(&buf).unwrap();
        assert_eq!(parsed.type_, packet_type::ST_SYN);
        assert_eq!(parsed.connection_id, 0xBEEF);
        assert_eq!(parsed.seq_nr, 1);
    }

    #[test]
    fn data_packet_with_payload() {
        let payload = b"hello world";
        let h = PacketHeader {
            type_: packet_type::ST_DATA,
            version: PROTOCOL_VERSION,
            extension: ext_type::EXT_NONE,
            connection_id: 0x4242,
            timestamp: 999,
            timestamp_diff: 42,
            wnd_size: 1024 * 1024,
            seq_nr: 10,
            ack_nr: 5,
        };
        let mut buf = vec![0u8; HEADER_LEN + payload.len()];
        encode_header(&mut buf, &h);
        buf[HEADER_LEN..].copy_from_slice(payload);

        let (parsed, exts, payload_off) = parse_packet(&buf).unwrap();
        assert_eq!(parsed.type_, packet_type::ST_DATA);
        assert!(exts.is_empty());
        assert_eq!(payload_off, HEADER_LEN);
        assert_eq!(&buf[payload_off..], payload);
    }
}
