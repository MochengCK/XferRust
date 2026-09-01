//! peer wire 协议（BEP 3）：握手、消息编解码与增量读取。

use tokio::io::{AsyncRead, AsyncReadExt};
use xfer_types::{InfoHash, PeerId};

/// 协议标识串。
pub const PSTR: &[u8] = b"BitTorrent protocol";
/// 握手保留字节：声明扩展协议 (BEP 10)、Fast Extension (BEP 6)、DHT (BEP 5)。
///
/// - reserved[5] bit 0x10 = 扩展协议 (BEP 10)
/// - reserved[7] bit 0x04 = Fast Extension (BEP 6)
/// - reserved[7] bit 0x01 = DHT (BEP 5)
pub const RESERVED: [u8; 8] = {
    let mut r = [0u8; 8];
    r[5] |= 0x10; // 扩展协议 (BEP 10)
    r[7] |= 0x04; // Fast Extension (BEP 6)
    r[7] |= 0x01; // DHT (BEP 5)
    r
};

/// 检查对端 reserved bytes 是否声明了扩展协议 (BEP 10)。
pub fn supports_extension(reserved: &[u8; 8]) -> bool {
    reserved[5] & 0x10 != 0
}

/// 检查对端 reserved bytes 是否声明了 Fast Extension (BEP 6)。
pub fn supports_fast_extension(reserved: &[u8; 8]) -> bool {
    reserved[7] & 0x04 != 0
}

/// 检查对端 reserved bytes 是否声明了 DHT (BEP 5)。
pub fn supports_dht(reserved: &[u8; 8]) -> bool {
    reserved[7] & 0x01 != 0
}

/// 请求块大小（字节）：生态事实标准 2^14 = 16KiB。
///
/// 主流客户端对更大的 request 直接拒绝或忽略：Transmission
/// （MAX_BLOCK_SIZE = 16384）、libtorrent（qBittorrent/Deluge）等。
/// 若用 64KB 发请求，真实种子永不应答 → 「有节点无速度」。
///
/// 注意区分：64KB（旧引擎 aria2 的 MAX_BLOCK_LENGTH）是「响应」对端请求的
/// 上限，不是发起请求的块大小；旧引擎请求端同样固定 16KiB
/// （aria2 Piece::BLOCK_LENGTH）。
pub const BLOCK_SIZE: u32 = 16 * 1024;

/// 握手。
#[derive(Debug, Clone)]
pub struct Handshake {
    pub info_hash: InfoHash,
    pub peer_id: PeerId,
    /// 对端保留字节（扩展位等，M2 仅记录）。
    pub reserved: [u8; 8],
}

/// peer wire 消息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// 4 字节 0 长度的 keep-alive。
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    /// have：对端已有该片。
    Have(u32),
    /// bitfield：对端已有片集合。
    Bitfield(Vec<u8>),
    /// request：请求一块（index, begin, length）。
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    /// piece：一块数据（index, begin, block）。
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    /// cancel：取消请求。
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    // ---- Fast Extension (BEP 6) ----
    /// 0x09 HaveAll：对端拥有全部片（Fast Extension）。
    HaveAll,
    /// 0x0A HaveNone：对端没有片（Fast Extension）。
    HaveNone,
    /// 0x0B RejectRequest：拒绝请求（Fast Extension）。
    RejectRequest {
        index: u32,
        begin: u32,
        length: u32,
    },
    /// 0x0C AllowedFast：允许快速下载的片索引（Fast Extension）。
    AllowedFast(u32),
    /// 0x0D SuggestPiece：建议下载的片索引（Fast Extension）。
    SuggestPiece(u32),
    // ---- DHT (BEP 5) ----
    /// 0x09 Port：通告 DHT 监听端口。
    /// 注意：Port 的消息 id 是 9，但 BEP 5 使用 reserved[7] bit 0x01。
    /// 实际上 Port 消息 id = 9 在旧规范中与 HaveAll 冲突，
    /// 但 BEP 6 Fast Extension 使用 0x09 = HaveAll。
    /// BEP 5 Port 消息的 id 是 9（在非 Fast Extension 对端上），
    /// 在 Fast Extension 对端上不发送 Port 消息（DHT 通过 reserved bits 协商）。
    Port(u16),
    // ---- Extension Protocol (BEP 10) ----
    /// 0x14 Extended：扩展协议消息 (id=20, payload = ext_id + body)。
    Extended {
        ext_id: u8,
        payload: Vec<u8>,
    },
    /// 未知消息 id（忽略但记录，保持流同步）。
    Unknown {
        id: u8,
        payload: Vec<u8>,
    },
}

impl Message {
    /// 消息 id（keep-alive 无 id，返回 None）。
    pub fn id(&self) -> Option<u8> {
        match self {
            Message::KeepAlive => None,
            Message::Choke => Some(0),
            Message::Unchoke => Some(1),
            Message::Interested => Some(2),
            Message::NotInterested => Some(3),
            Message::Have(_) => Some(4),
            Message::Bitfield(_) => Some(5),
            Message::Request { .. } => Some(6),
            Message::Piece { .. } => Some(7),
            Message::Cancel { .. } => Some(8),
            Message::Port(_) => Some(0x09),
            Message::SuggestPiece(_) => Some(0x0D),
            Message::HaveAll => Some(0x0E),
            Message::HaveNone => Some(0x0F),
            Message::RejectRequest { .. } => Some(0x10),
            Message::AllowedFast(_) => Some(0x11),
            Message::Extended { .. } => Some(0x14),
            Message::Unknown { id, .. } => Some(*id),
        }
    }

    /// 消息体长度（不含 4 字节长度前缀，含 id 字节）。用于精确预分配。
    fn payload_len(&self) -> usize {
        match self {
            Message::KeepAlive => 0,
            Message::Choke
            | Message::Unchoke
            | Message::Interested
            | Message::NotInterested
            | Message::HaveAll
            | Message::HaveNone => 1,
            Message::Have(_)
            | Message::Port(_)
            | Message::SuggestPiece(_)
            | Message::AllowedFast(_) => 5,
            Message::Bitfield(bf) => 1 + bf.len(),
            Message::Request { .. } | Message::Cancel { .. } | Message::RejectRequest { .. } => {
                13
            }
            Message::Piece { block, .. } => 9 + block.len(),
            Message::Extended { payload, .. } => 2 + payload.len(),
            Message::Unknown { payload, .. } => 1 + payload.len(),
        }
    }

    /// 序列化为 wire 格式（keep-alive → 4 字节 0）。
    ///
    /// 单缓冲 + 回填长度前缀：旧实现先建 payload 再整体拷入第二个
    /// Vec，seed 供块时每个 16KiB 块要拷贝两遍。
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Message::KeepAlive => vec![0, 0, 0, 0],
            other => {
                let mut out = Vec::with_capacity(4 + other.payload_len());
                out.extend_from_slice(&[0, 0, 0, 0]); // 长度前缀占位，最后回填
                out.push(other.id().unwrap());
                match other {
                    Message::Have(i) => out.extend_from_slice(&i.to_be_bytes()),
                    Message::Bitfield(bf) => out.extend_from_slice(bf),
                    Message::Request {
                        index,
                        begin,
                        length,
                    }
                    | Message::Cancel {
                        index,
                        begin,
                        length,
                    }
                    | Message::RejectRequest {
                        index,
                        begin,
                        length,
                    } => {
                        out.extend_from_slice(&index.to_be_bytes());
                        out.extend_from_slice(&begin.to_be_bytes());
                        out.extend_from_slice(&length.to_be_bytes());
                    }
                    Message::Piece {
                        index,
                        begin,
                        block,
                    } => {
                        out.extend_from_slice(&index.to_be_bytes());
                        out.extend_from_slice(&begin.to_be_bytes());
                        out.extend_from_slice(block);
                    }
                    Message::Port(port) => {
                        out.extend_from_slice(&port.to_be_bytes());
                    }
                    Message::SuggestPiece(index) | Message::AllowedFast(index) => {
                        out.extend_from_slice(&index.to_be_bytes());
                    }
                    Message::Extended {
                        ext_id,
                        payload: ext_payload,
                    } => {
                        out.push(*ext_id);
                        out.extend_from_slice(ext_payload);
                    }
                    Message::Unknown { payload, .. } => {
                        out.extend_from_slice(payload);
                    }
                    // KeepAlive / Choke / Unchoke / Interested / NotInterested /
                    // HaveAll / HaveNone — 无附加 payload
                    _ => {}
                }
                let len = (out.len() - 4) as u32;
                out[..4].copy_from_slice(&len.to_be_bytes());
                out
            }
        }
    }
}

/// 组装握手字节。
pub fn encode_handshake(info_hash: &InfoHash, peer_id: &PeerId) -> Vec<u8> {
    let mut out = Vec::with_capacity(68);
    out.push(PSTR.len() as u8);
    out.extend_from_slice(PSTR);
    out.extend_from_slice(&RESERVED);
    out.extend_from_slice(info_hash.as_bytes());
    out.extend_from_slice(&peer_id.0);
    out
}

/// 从原始字节解析 BT 握手（68 字节）。
/// 用于 MSE 捎带的 BT 握手解析。
pub fn decode_handshake(data: &[u8]) -> std::io::Result<Handshake> {
    if data.len() < 68 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("握手数据不足: {} < 68", data.len()),
        ));
    }
    let pstrlen = data[0] as usize;
    if pstrlen != PSTR.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("握手 pstrlen 异常: {pstrlen}"),
        ));
    }
    if &data[1..1 + pstrlen] != PSTR {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "协议标识串不匹配",
        ));
    }
    let mut reserved = [0u8; 8];
    reserved.copy_from_slice(&data[1 + pstrlen..9 + pstrlen]);
    let mut ih = [0u8; 20];
    ih.copy_from_slice(&data[9 + pstrlen..29 + pstrlen]);
    let mut pid = [0u8; 20];
    pid.copy_from_slice(&data[29 + pstrlen..49 + pstrlen]);
    Ok(Handshake {
        info_hash: InfoHash::from_bytes(&ih),
        peer_id: PeerId(pid),
        reserved,
    })
}

/// 增量读取器：维护接收缓冲，逐个吐出消息。
pub struct PeerReader {
    buf: Vec<u8>,
    /// 已解析对端握手（读取第一条消息前置位）。
    handshake: Option<Handshake>,
}

impl Default for PeerReader {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerReader {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(16 * 1024),
            handshake: None,
        }
    }

    /// 回填已读出的字节到读缓冲头部（如 MSE 明文识别时已消费的对端握手前缀）。
    pub fn preload(&mut self, bytes: Vec<u8>) {
        if bytes.is_empty() {
            return;
        }
        let mut merged = bytes;
        merged.extend_from_slice(&self.buf);
        self.buf = merged;
    }

    /// 从流中读取完整握手。未读满时返回 Ok(None)。
    pub async fn read_handshake<R: AsyncRead + Unpin>(
        &mut self,
        stream: &mut R,
    ) -> std::io::Result<Option<Handshake>> {
        if self.handshake.is_some() {
            return Ok(self.handshake.clone());
        }
        // BT 握手 = 1 (pstrlen) + 19 (pstr) + 8 (reserved) + 20 (info_hash) + 20 (peer_id) = 68
        const HS_LEN: usize = 68;
        // 循环读取直到凑齐 68 字节
        while self.buf.len() < HS_LEN {
            let need = HS_LEN - self.buf.len();
            let n = fill(stream, &mut self.buf, need).await?;
            if n == 0 {
                // EOF
                if self.buf.is_empty() {
                    return Ok(None);
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "握手数据不完整",
                ));
            }
        }
        let pstrlen = self.buf[0] as usize;
        if pstrlen != PSTR.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("握手 pstrlen 异常: {pstrlen}"),
            ));
        }
        if &self.buf[1..1 + pstrlen] != PSTR {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "协议标识串不匹配",
            ));
        }
        let mut reserved = [0u8; 8];
        reserved.copy_from_slice(&self.buf[1 + pstrlen..9 + pstrlen]);
        let mut ih = [0u8; 20];
        ih.copy_from_slice(&self.buf[9 + pstrlen..29 + pstrlen]);
        let mut pid = [0u8; 20];
        pid.copy_from_slice(&self.buf[29 + pstrlen..49 + pstrlen]);
        // 移除已消费的握手字节
        self.buf.drain(..HS_LEN);
        let hs = Handshake {
            info_hash: InfoHash::from_bytes(&ih),
            peer_id: PeerId(pid),
            reserved,
        };
        self.handshake = Some(hs.clone());
        Ok(Some(hs))
    }

    /// 从流中读取一条消息。流 EOF 返回 Ok(None)。
    ///
    /// 正确处理 TCP 部分读取：循环调用 read 直到凑齐完整消息，
    /// 或 read 返回 0 字节（真正的 EOF）时返回 None。
    pub async fn read_message<R: AsyncRead + Unpin>(
        &mut self,
        stream: &mut R,
    ) -> std::io::Result<Option<Message>> {
        // 读取 4 字节长度前缀
        while self.buf.len() < 4 {
            let need = 4 - self.buf.len();
            let n = fill(stream, &mut self.buf, need).await?;
            if n == 0 {
                // 真正的 EOF：流已关闭
                if self.buf.is_empty() {
                    return Ok(None);
                }
                // 缓冲区有残留但流已断开 → 不完整消息
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "消息长度前缀不完整",
                ));
            }
        }

        let len = u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]) as usize;
        if len > 2 * 1024 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("消息长度异常: {len}"),
            ));
        }
        let total = 4 + len;

        // 读取消息体
        while self.buf.len() < total {
            let need = total - self.buf.len();
            let n = fill(stream, &mut self.buf, need).await?;
            if n == 0 {
                // 流断开，消息体不完整
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "消息体不完整",
                ));
            }
        }

        let msg = parse_message(&self.buf[4..total])?;
        self.buf.drain(..total);
        Ok(Some(msg))
    }
}

/// 从流中读取最多 n 字节，直接写入 buf 的预留空间。返回实际读取的字节数（0 = EOF）。
///
/// 不分配临时缓冲：热路径上每个 16KiB 块至少走一次，旧实现的
/// `vec![0; n]` + `extend_from_slice` 意味着每消息一次分配加一次拷贝。
async fn fill<R: AsyncRead + Unpin>(
    stream: &mut R,
    buf: &mut Vec<u8>,
    n: usize,
) -> std::io::Result<usize> {
    buf.reserve(n);
    let spare = buf.spare_capacity_mut();
    // SAFETY: MaybeUninit<u8> 与 u8 布局相同；只写入不读取未初始化内容，
    // set_len 只覆盖 read 实际写入的前 read 字节。
    let dst =
        unsafe { std::slice::from_raw_parts_mut(spare.as_mut_ptr().cast::<u8>(), spare.len()) };
    let read = stream.read(dst).await?;
    unsafe { buf.set_len(buf.len() + read) };
    Ok(read)
}

fn parse_message(payload: &[u8]) -> std::io::Result<Message> {
    if payload.is_empty() {
        return Ok(Message::KeepAlive);
    }
    let id = payload[0];
    let body = &payload[1..];
    let u32_at = |o: usize| -> std::io::Result<u32> {
        let b = body
            .get(o..o + 4)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "消息体不足"))?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    };
    Ok(match id {
        0 => Message::Choke,
        1 => Message::Unchoke,
        2 => Message::Interested,
        3 => Message::NotInterested,
        4 => Message::Have(u32_at(0)?),
        5 => Message::Bitfield(body.to_vec()),
        6 => Message::Request {
            index: u32_at(0)?,
            begin: u32_at(4)?,
            length: u32_at(8)?,
        },
        7 => Message::Piece {
            index: u32_at(0)?,
            begin: u32_at(4)?,
            block: body[8..].to_vec(),
        },
        8 => Message::Cancel {
            index: u32_at(0)?,
            begin: u32_at(4)?,
            length: u32_at(8)?,
        },
        0x09 => {
            // Port (BEP 5): 2 字节端口号
            if body.len() < 2 {
                return Ok(Message::Unknown {
                    id,
                    payload: body.to_vec(),
                });
            }
            Message::Port(u16::from_be_bytes([body[0], body[1]]))
        }
        0x0D => Message::SuggestPiece(u32_at(0)?),
        0x0E => Message::HaveAll,
        0x0F => Message::HaveNone,
        0x10 => Message::RejectRequest {
            index: u32_at(0)?,
            begin: u32_at(4)?,
            length: u32_at(8)?,
        },
        0x11 => Message::AllowedFast(u32_at(0)?),
        0x14 => {
            // Extended (BEP 10): ext_id (1 byte) + payload
            if body.is_empty() {
                return Ok(Message::Unknown {
                    id,
                    payload: body.to_vec(),
                });
            }
            Message::Extended {
                ext_id: body[0],
                payload: body[1..].to_vec(),
            }
        }
        other => Message::Unknown {
            id: other,
            payload: body.to_vec(),
        },
    })
}

/// 计算一片的请求块序列：(begin, length)。
pub fn request_blocks(piece_len: u32, block_size: u32) -> Vec<(u32, u32)> {
    let mut out = Vec::new();
    let mut begin = 0u32;
    while begin < piece_len {
        let len = (piece_len - begin).min(block_size);
        out.push((begin, len));
        begin += len;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use xfer_types::ENGINE_NAME;

    #[test]
    fn handshake_roundtrip() {
        let ih = InfoHash::from_bytes(&[7u8; 20]);
        let pid = PeerId([9u8; 20]);
        let wire = encode_handshake(&ih, &pid);
        assert_eq!(wire.len(), 68);
        assert_eq!(wire[0], 19);
        assert_eq!(&wire[1..20], PSTR);
        // RESERVED bytes now declare Fast Extension, Extension Protocol, DHT
        assert_eq!(&wire[20..28], &RESERVED);
        assert_eq!(&wire[28..48], &[7u8; 20]);
        assert_eq!(&wire[48..68], &[9u8; 20]);
    }

    #[test]
    fn message_encode_decode() {
        let cases = vec![
            Message::KeepAlive,
            Message::Choke,
            Message::Unchoke,
            Message::Interested,
            Message::NotInterested,
            Message::Have(1234),
            Message::Bitfield(vec![0xFF, 0x80]),
            Message::Request {
                index: 1,
                begin: 2,
                length: 65536,
            },
            Message::Piece {
                index: 3,
                begin: 4,
                block: vec![0xAB; 100],
            },
            Message::Cancel {
                index: 5,
                begin: 6,
                length: 16384,
            },
            // Fast Extension (BEP 6)
            Message::HaveAll,
            Message::HaveNone,
            Message::RejectRequest {
                index: 7,
                begin: 8,
                length: 16384,
            },
            Message::AllowedFast(42),
            Message::SuggestPiece(99),
            // DHT Port (BEP 5)
            Message::Port(6881),
            // Extension Protocol (BEP 10)
            Message::Extended {
                ext_id: 0,
                payload: format!("d1:md6:ut_pexi1ee1:v{}:{}e", ENGINE_NAME.len(), ENGINE_NAME)
                    .into_bytes(),
            },
        ];
        for c in cases {
            let wire = c.encode();
            // 完整消息 = 4 字节长度 + payload
            let len = u32::from_be_bytes([wire[0], wire[1], wire[2], wire[3]]) as usize;
            assert_eq!(4 + len, wire.len());
            let parsed = parse_message(&wire[4..]).unwrap();
            assert_eq!(parsed, c, "消息往返失败: {c:?}");
        }
    }

    #[test]
    fn keepalive_is_zero_length() {
        assert_eq!(Message::KeepAlive.encode(), vec![0, 0, 0, 0]);
        assert_eq!(parse_message(&[]).unwrap(), Message::KeepAlive);
    }

    #[test]
    fn request_blocks_splitting() {
        assert_eq!(request_blocks(10, 4), vec![(0, 4), (4, 4), (8, 2)]);
        assert_eq!(request_blocks(0, 4), vec![]);
        assert_eq!(request_blocks(65536, 65536), vec![(0, 65536)]);
    }

    #[tokio::test]
    async fn reader_parses_from_buffer() {
        let wire = Message::Piece {
            index: 3,
            begin: 4,
            block: vec![0xAB; 100],
        }
        .encode();
        let mut reader = PeerReader::new();
        reader.buf = wire;
        let msg = reader
            .read_message(&mut tokio::io::empty())
            .await
            .unwrap()
            .expect("应读到一条消息");
        assert_eq!(
            msg,
            Message::Piece {
                index: 3,
                begin: 4,
                block: vec![0xAB; 100]
            }
        );
        // EOF（empty 流返回 0 字节）→ 未读满返回 None
        assert!(reader
            .read_message(&mut tokio::io::empty())
            .await
            .unwrap()
            .is_none());

        // 超长消息拒绝
        let mut r3 = PeerReader::new();
        r3.buf = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x01];
        assert!(r3.read_message(&mut tokio::io::empty()).await.is_err());
    }
}
