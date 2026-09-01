//! MSE/PE (Message Stream Encryption) 协商 — 真实标准客户端线格式。
//!
//! 协议事实依据：与 libtorrent（qBittorrent/Deluge）和 rTorrent 源码逐字段对照。
//! 完整握手流程（A 发起、B 响应）：
//!
//! ```text
//! A → B: Ya(96) || PadA(0..512 随机)
//! B → A: Yb(96) || PadB(0..512 随机)
//! A → B: SHA1("req1"||S) || SHA1("req2"||SKEY)⊕SHA1("req3"||S)
//!        || ENC_A(VC(8×0) || crypto_provide(4) || len(PadC)(2) || PadC || len(IA)(2))
//!        || ENC_A(IA)
//! B → A: ENC_B(VC || crypto_select(4) || len(PadD)(2) || PadD) || ENC_B(BT握手(68))
//! ```
//!
//! - DH 公钥**先于** padding 发送，定长 96 字节读取；padding 跟在后面，
//!   由对端通过同步标记扫描（≤512 字节）跳过
//! - ENC_A = RC4(SHA1("keyA"||S||SKEY))，ENC_B = RC4(SHA1("keyB"||S||SKEY))，
//!   均丢弃前 1024 字节密钥流；加密段从 VC 开始，之前的字节不过 RC4
//! - 响应方通过明文 SHA1("req1"||S) 定位加密段起点，再用
//!   SHA1("req2"||SKEY)⊕SHA1("req3"||S) 识别种子
//! - 发起方通过扫描"加密后的 VC"（接收流加密 8 个零字节的线上形态）定位响应段
//! - 被动方识别明文 BT 握手（0x13 + "BitTorrent protocol"）→ 回退明文路径
//! - crypto_select 选择明文时，协商段仍是加密的，仅协商完成后的流为明文

use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use xfer_crypto::{
    derive_rc4_streams, is_degenerate_key, obfuscated_skey_hash, req1_hash,
    skey_matches_obfuscated, DhKeyPair, MseRole, Rc4, CRYPTO_PLAINTEXT, CRYPTO_RC4, VC,
};
use xfer_types::InfoHash;

/// DH 公钥线上长度（768-bit 群，96 字节大端）。
const KEY_LEN: usize = 96;

/// PadA/PadB/PadC/PadD 最大长度。
const MAX_PAD: usize = 512;

/// 同步标记（req1 哈希 / 加密 VC）的最大扫描字节数。
const SYNC_SCAN_LIMIT: usize = MAX_PAD;

/// BT 握手固定长度（响应方回复的握手无长度前缀，按定长读取）。
const BT_HANDSHAKE_LEN: usize = 68;

/// IA（发起方初始载荷）长度上限。
const MAX_IA: usize = 636;

/// PE 协商结果。
pub enum PeOutcome<S> {
    /// RC4 加密流已建立；`peer_ia` 为对端捎带的初始数据（BT 握手）。
    Encrypted {
        stream: EncryptedStream<S>,
        peer_ia: Vec<u8>,
    },
    /// 明文路径：
    /// - `pending` 非空 → 被动方检测到明文 BT 握手，`pending` 为已读出的字节，
    ///   调用方需将其回填读缓冲再走标准握手；
    /// - `peer_ia` 非空 → 加密握手后协商为明文流，BT 握手已经过 IA 互换。
    Plaintext {
        stream: S,
        pending: Vec<u8>,
        peer_ia: Vec<u8>,
    },
}

/// PE 发起方：执行完整加密握手。
///
/// `skey` 为目标种子的 info_hash；`initial_payload` 为 IA（通常为 68 字节 BT 握手）；
/// `provide` 为 `crypto_provide` 位掩码（优先加密传 `CRYPTO_RC4|CRYPTO_PLAINTEXT`，
/// 强制加密仅传 `CRYPTO_RC4`）。
pub async fn pe_handshake_initiator<S>(
    mut stream: S,
    skey: &InfoHash,
    initial_payload: &[u8],
    provide: u32,
) -> io::Result<PeOutcome<S>>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    // 1. Ya(96) + PadA(0..512)
    let dh = DhKeyPair::generate();
    let pad_a = random_pad();
    let mut out = Vec::with_capacity(KEY_LEN + pad_a.len());
    out.extend_from_slice(&dh.public_key());
    out.extend_from_slice(&pad_a);
    stream.write_all(&out).await?;

    // 2. 读对端公钥 Yb（定长 96；PadB 在其后，由 VC 扫描跳过）
    let mut yb = [0u8; KEY_LEN];
    stream.read_exact(&mut yb).await?;
    if is_degenerate_key(&yb) {
        return Err(invalid_data("对端 DH 公钥退化"));
    }

    // 3. 共享密钥 + 双向 RC4 流（发起方：发 keyA 收 keyB）
    let shared = dh.compute_shared_secret(&yb);
    if is_degenerate_key(&shared) {
        return Err(invalid_data("DH 共享密钥退化"));
    }
    let (mut send_stream, mut recv_stream) =
        derive_rc4_streams(&shared, skey.as_bytes(), MseRole::Initiator);

    // 4. 明文同步/识别哈希：SHA1("req1"||S) + SHA1("req2"||SKEY)⊕SHA1("req3"||S)
    let mut plain = Vec::with_capacity(40);
    plain.extend_from_slice(&req1_hash(&shared));
    plain.extend_from_slice(&obfuscated_skey_hash(&shared, skey.as_bytes()));
    stream.write_all(&plain).await?;

    // 5. 加密协商段：VC + crypto_provide + len(PadC) + PadC + len(IA)
    let pad_c = random_pad();
    let mut enc = Vec::with_capacity(8 + 4 + 2 + pad_c.len() + 2);
    enc.extend_from_slice(&VC);
    enc.extend_from_slice(&provide.to_be_bytes());
    enc.extend_from_slice(&(pad_c.len() as u16).to_be_bytes());
    enc.extend_from_slice(&pad_c);
    enc.extend_from_slice(&(initial_payload.len() as u16).to_be_bytes());
    send_stream.process(&mut enc);
    stream.write_all(&enc).await?;

    // IA（BT 握手）紧随其后，同样加密发送
    let mut ia = initial_payload.to_vec();
    send_stream.process(&mut ia);
    stream.write_all(&ia).await?;

    // 6. 在 ≤512 字节 PadB 后扫描加密 VC。
    // 加密 VC 的线上形态 = 用接收流加密 8 个零字节（同时消耗 8 字节密钥流，
    // 后续解密恰好从真实 VC 之后继续）。
    let mut vc_wire = VC.to_vec();
    recv_stream.process(&mut vc_wire);
    let leftover = scan_sync(&mut stream, &vc_wire).await?;
    let mut r = RawBuf {
        stream: &mut stream,
        pending: leftover,
    };

    // 7. crypto_select(4) + len(PadD)(2) + PadD
    let mut hdr = [0u8; 6];
    r.read_exact_into(&mut hdr).await?;
    recv_stream.process(&mut hdr);
    let crypto_select = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
    let pad_d_len = u16::from_be_bytes([hdr[4], hdr[5]]) as usize;
    if pad_d_len > MAX_PAD {
        return Err(invalid_data("PadD 长度异常"));
    }
    let mut pad_d = vec![0u8; pad_d_len];
    r.read_exact_into(&mut pad_d).await?;
    recv_stream.process(&mut pad_d);

    // 8. 对端 BT 握手（定长 68，无长度前缀；明文/密文取决于 crypto_select）
    let mut peer_hs = vec![0u8; BT_HANDSHAKE_LEN];
    r.read_exact_into(&mut peer_hs).await?;
    // 扫描越界可能已读入对端握手后立即发出的数据——必须保留，丢弃即丢包
    let leftover = std::mem::take(&mut r.pending);
    drop(r);

    if crypto_select & CRYPTO_RC4 != 0 {
        recv_stream.process(&mut peer_hs);
        Ok(PeOutcome::Encrypted {
            stream: EncryptedStream::new(stream, send_stream, recv_stream)
                .with_pending_read(leftover),
            peer_ia: peer_hs,
        })
    } else if crypto_select & CRYPTO_PLAINTEXT != 0 && provide & CRYPTO_PLAINTEXT != 0 {
        // 明文流：协商段已消费完毕，后续全部明文
        Ok(PeOutcome::Plaintext {
            stream,
            pending: leftover,
            peer_ia: peer_hs,
        })
    } else {
        Err(invalid_data(&format!(
            "crypto_select 异常: {crypto_select:#x}"
        )))
    }
}

/// PE 响应方：执行完整加密握手；识别到明文 BT 握手时回退明文路径。
///
/// `skey` 为本引擎种子的 info_hash；`our_handshake` 为回复用的 68 字节 BT 握手；
/// `force_encrypted` 为强制加密模式：拒绝明文 BT 握手、仅接受 RC4 协商。
pub async fn pe_handshake_responder<S>(
    mut stream: S,
    skey: &InfoHash,
    our_handshake: &[u8],
    force_encrypted: bool,
) -> io::Result<PeOutcome<S>>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    // 1-2. 明文 BT 握手识别：必须匹配完整 20 字节（0x13 + "BitTorrent protocol"）。
    // 只匹配首字节会把 1/256 概率首字节恰为 0x13 的真实加密连接误判为明文
    // （rTorrent/libtorrent 均匹配完整协议串后才分流）。
    let mut head = [0u8; 20];
    stream.read_exact(&mut head).await?;
    if head[0] == 0x13 && &head[1..] == b"BitTorrent protocol" {
        if force_encrypted {
            return Err(invalid_data("强制加密模式：拒绝明文连接"));
        }
        return Ok(PeOutcome::Plaintext {
            stream,
            pending: head.to_vec(),
            peer_ia: Vec::new(),
        });
    }

    // 发起方 DH 公钥 Ya（前 20 字节已读，续读剩余 76 字节）
    let mut ya = [0u8; KEY_LEN];
    ya[..20].copy_from_slice(&head);
    stream.read_exact(&mut ya[20..]).await?;
    if is_degenerate_key(&ya) {
        return Err(invalid_data("对端 DH 公钥退化"));
    }

    // 3. 生成己方密钥对，回复 Yb + PadB（S 的计算不依赖发送顺序，先回后算）
    let dh = DhKeyPair::generate();
    let pad_b = random_pad();
    let mut out = Vec::with_capacity(KEY_LEN + pad_b.len());
    out.extend_from_slice(&dh.public_key());
    out.extend_from_slice(&pad_b);
    stream.write_all(&out).await?;

    let shared = dh.compute_shared_secret(&ya);
    if is_degenerate_key(&shared) {
        return Err(invalid_data("DH 共享密钥退化"));
    }

    // 4. 扫描明文同步哈希 SHA1("req1"||S)（跳过 PadA，≤512 字节）
    let sync = req1_hash(&shared);
    let leftover = scan_sync(&mut stream, &sync).await?;
    let mut r = RawBuf {
        stream: &mut stream,
        pending: leftover,
    };

    // 5. 种子识别：skeyhash ⊕ SHA1("req3"||S) == SHA1("req2"||SKEY)
    let mut skey_hash = [0u8; 20];
    r.read_exact_into(&mut skey_hash).await?;
    if !skey_matches_obfuscated(&skey_hash, &shared, skey.as_bytes()) {
        return Err(invalid_data("种子识别失败（SKEY 不匹配）"));
    }

    // 6. 双向 RC4 流（响应方：发 keyB 收 keyA），校验 VC
    let (mut send_stream, mut recv_stream) =
        derive_rc4_streams(&shared, skey.as_bytes(), MseRole::Responder);
    let mut vc = [0u8; 8];
    r.read_exact_into(&mut vc).await?;
    recv_stream.process(&mut vc);
    if vc != VC {
        return Err(invalid_data("VC 校验失败"));
    }

    // 7. crypto_provide(4) + len(PadC)(2) + PadC + len(IA)(2) + IA
    let mut hdr = [0u8; 6];
    r.read_exact_into(&mut hdr).await?;
    recv_stream.process(&mut hdr);
    let crypto_provide = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
    let pad_c_len = u16::from_be_bytes([hdr[4], hdr[5]]) as usize;
    if pad_c_len > MAX_PAD {
        return Err(invalid_data("PadC 长度异常"));
    }
    let mut pad_c = vec![0u8; pad_c_len];
    r.read_exact_into(&mut pad_c).await?;
    recv_stream.process(&mut pad_c);

    let mut ia_len_buf = [0u8; 2];
    r.read_exact_into(&mut ia_len_buf).await?;
    recv_stream.process(&mut ia_len_buf);
    let ia_len = u16::from_be_bytes(ia_len_buf) as usize;
    if ia_len > MAX_IA {
        return Err(invalid_data("IA 长度异常"));
    }
    let mut peer_ia = vec![0u8; ia_len];
    r.read_exact_into(&mut peer_ia).await?;
    recv_stream.process(&mut peer_ia);
    // 越界读入的对端早期数据（协议因果上罕见，但防御性保留，丢弃即丢包）
    let leftover = std::mem::take(&mut r.pending);
    drop(r);

    // 8. 选择加密方式（优先 RC4）并回复：
    //    ENC(VC + crypto_select + len(PadD) + PadD) + BT 握手
    //    强制加密模式仅接受 RC4。
    let (select, encrypted_after) = if crypto_provide & CRYPTO_RC4 != 0 {
        (CRYPTO_RC4, true)
    } else if !force_encrypted && crypto_provide & CRYPTO_PLAINTEXT != 0 {
        (CRYPTO_PLAINTEXT, false)
    } else {
        return Err(invalid_data("对端未提供可用加密方式"));
    };

    let pad_d = random_pad();
    let mut resp = Vec::with_capacity(8 + 4 + 2 + pad_d.len());
    resp.extend_from_slice(&VC);
    resp.extend_from_slice(&select.to_be_bytes());
    resp.extend_from_slice(&(pad_d.len() as u16).to_be_bytes());
    resp.extend_from_slice(&pad_d);
    send_stream.process(&mut resp);
    stream.write_all(&resp).await?;

    if encrypted_after {
        let mut hs = our_handshake.to_vec();
        send_stream.process(&mut hs);
        stream.write_all(&hs).await?;
        Ok(PeOutcome::Encrypted {
            stream: EncryptedStream::new(stream, send_stream, recv_stream)
                .with_pending_read(leftover),
            peer_ia,
        })
    } else {
        stream.write_all(our_handshake).await?;
        Ok(PeOutcome::Plaintext {
            stream,
            pending: leftover,
            peer_ia,
        })
    }
}

// ---------------------------------------------------------------------------
// 内部工具
// ---------------------------------------------------------------------------

fn invalid_data(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

/// 生成随机 padding（长度 0..512，与主流实现一致）。
fn random_pad() -> Vec<u8> {
    let mut len_buf = [0u8; 2];
    let _ = getrandom::fill(&mut len_buf);
    let len = (u16::from_le_bytes(len_buf) as usize) % MAX_PAD;
    let mut pad = vec![0u8; len];
    let _ = getrandom::fill(&mut pad);
    pad
}

/// 在流中扫描 `marker`，最多扫描 `SYNC_SCAN_LIMIT` 字节后放弃。
///
/// 返回 marker 之后的剩余字节（读取可能越过 marker，不能塞回流中）。
async fn scan_sync<S: AsyncRead + Unpin>(stream: &mut S, marker: &[u8]) -> io::Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    let mut scanned = 0usize;
    let mut chunk = [0u8; 128];
    loop {
        if let Some(pos) = buf.windows(marker.len()).position(|w| w == marker) {
            return Ok(buf.split_off(pos + marker.len()));
        }
        // 保留末尾 marker.len()-1 字节以支持跨读取边界匹配，
        // 其余前缀已确认不含 marker 起点
        let keep = marker.len().saturating_sub(1);
        if buf.len() > keep {
            let safe_end = buf.len() - keep;
            scanned += safe_end;
            buf.drain(..safe_end);
        }
        if scanned > SYNC_SCAN_LIMIT {
            return Err(invalid_data("同步标记扫描超限"));
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(invalid_data("同步扫描期间连接关闭"));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// 小型缓冲读取器：先消费扫描阶段带回的剩余字节，再从流中读取。
struct RawBuf<'a, S> {
    stream: &'a mut S,
    pending: Vec<u8>,
}

impl<S: AsyncRead + Unpin> RawBuf<'_, S> {
    async fn read_exact_into(&mut self, out: &mut [u8]) -> io::Result<()> {
        let mut filled = 0;
        if !self.pending.is_empty() {
            let n = out.len().min(self.pending.len());
            out[..n].copy_from_slice(&self.pending[..n]);
            self.pending.drain(..n);
            filled = n;
        }
        while filled < out.len() {
            let n = self.stream.read(&mut out[filled..]).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "协商数据不完整",
                ));
            }
            filled += n;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 加密流
// ---------------------------------------------------------------------------

/// 加密流包装器：在读写时自动 RC4 加密/解密。
///
/// 实现 `AsyncRead` + `AsyncWrite`，可透明替换底层流。
pub struct EncryptedStream<S> {
    inner: S,
    send_cipher: Rc4,
    recv_cipher: Rc4,
    /// 写入时的加密缓冲：部分写入时保留未发出的密文，
    /// 绝不可重新加密或丢弃（密钥流位置已前进）。
    write_buf: Vec<u8>,
    /// 握手扫描阶段越界读入的密文（对端在握手刚完成时就发出的数据）。
    /// 必须在读取内层流之前先行消费，否则字节永久丢失且密钥流失步。
    read_pending: Vec<u8>,
}

impl<S> EncryptedStream<S>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    pub fn new(inner: S, send_cipher: Rc4, recv_cipher: Rc4) -> Self {
        Self {
            inner,
            send_cipher,
            recv_cipher,
            write_buf: Vec::new(),
            read_pending: Vec::new(),
        }
    }

    /// 附带握手扫描阶段越界读入的密文（按线上顺序，尚未解密）。
    pub fn with_pending_read(mut self, bytes: Vec<u8>) -> Self {
        self.read_pending = bytes;
        self
    }

    /// 加密写入数据。
    pub async fn write_encrypted(&mut self, data: &[u8]) -> io::Result<()> {
        let mut buf = data.to_vec();
        self.send_cipher.process(&mut buf);
        self.inner.write_all(&buf).await
    }

    /// 读取并解密数据。
    pub async fn read_decrypted(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.read_pending.is_empty() {
            let n = buf.len().min(self.read_pending.len());
            buf[..n].copy_from_slice(&self.read_pending[..n]);
            self.read_pending.drain(..n);
            self.recv_cipher.process(&mut buf[..n]);
            return Ok(n);
        }
        let n = self.inner.read(buf).await?;
        if n > 0 {
            self.recv_cipher.process(&mut buf[..n]);
        }
        Ok(n)
    }

    /// 读取精确长度的解密数据。
    pub async fn read_exact_decrypted(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let mut filled = 0;
        if !self.read_pending.is_empty() {
            let n = buf.len().min(self.read_pending.len());
            buf[..n].copy_from_slice(&self.read_pending[..n]);
            self.read_pending.drain(..n);
            self.recv_cipher.process(&mut buf[..n]);
            filled = n;
        }
        if filled < buf.len() {
            self.inner.read_exact(&mut buf[filled..]).await?;
            self.recv_cipher.process(&mut buf[filled..]);
        }
        Ok(())
    }

    /// 获取内层流的引用。
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// 获取内层流的可变引用。
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }
}

impl<S> AsyncRead for EncryptedStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let this = self.get_mut();
        if !this.read_pending.is_empty() {
            let n = buf.remaining().min(this.read_pending.len());
            buf.put_slice(&this.read_pending[..n]);
            this.read_pending.drain(..n);
            let start = buf.filled().len() - n;
            this.recv_cipher.process(&mut buf.filled_mut()[start..]);
            return std::task::Poll::Ready(Ok(()));
        }
        let before = buf.filled().len();
        match std::pin::Pin::new(&mut this.inner).poll_read(cx, buf) {
            std::task::Poll::Ready(Ok(())) => {
                let n = buf.filled().len() - before;
                if n > 0 {
                    let start = buf.filled().len() - n;
                    this.recv_cipher.process(&mut buf.filled_mut()[start..]);
                }
                std::task::Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<S> AsyncWrite for EncryptedStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        let this = self.get_mut();
        // 只有缓冲为空时才加密新数据；部分写入后的遗留密文必须原样发出
        if this.write_buf.is_empty() {
            this.write_buf.extend_from_slice(buf);
            this.send_cipher.process(&mut this.write_buf);
        }
        match std::pin::Pin::new(&mut this.inner).poll_write(cx, &this.write_buf) {
            std::task::Poll::Ready(Ok(n)) => {
                this.write_buf.drain(..n);
                std::task::Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use xfer_types::PeerId;

    /// 完整 PE 握手回环：发起方↔响应方，BT 握手作为 IA，
    /// 之后双向加密流量必须可互通。
    #[tokio::test]
    async fn pe_full_handshake_loopback() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let info_hash = InfoHash::from_bytes(&[0xAAu8; 20]);
        let init_id = PeerId::azureus_prefix(&[1u8; 12]);
        let resp_id = PeerId::azureus_prefix(&[2u8; 12]);
        let init_hs = crate::message::encode_handshake(&info_hash, &init_id);
        let resp_hs = crate::message::encode_handshake(&info_hash, &resp_id);

        let ih2 = info_hash;
        let resp_hs2 = resp_hs.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            pe_handshake_responder(stream, &ih2, &resp_hs2, false)
                .await
                .unwrap()
        });

        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let init_outcome = pe_handshake_initiator(client, &info_hash, &init_hs, CRYPTO_RC4 | CRYPTO_PLAINTEXT)
            .await
            .unwrap();
        let resp_outcome = server.await.unwrap();

        // 双方都应是加密结果，且 IA 互换正确
        let (PeOutcome::Encrypted {
            stream: mut enc_client,
            peer_ia: client_got,
        },
        PeOutcome::Encrypted {
            stream: mut enc_server,
            peer_ia: server_got,
        }) = (init_outcome, resp_outcome) else {
            panic!("双方都应协商为 RC4 加密流");
        };
        assert_eq!(client_got, resp_hs, "发起方应收到响应方 BT 握手");
        assert_eq!(server_got, init_hs, "响应方应收到发起方 BT 握手");

        // 双向加密通信
        enc_client.write_encrypted(b"hello_encrypted").await.unwrap();
        let mut buf = [0u8; 64];
        enc_server
            .read_exact_decrypted(&mut buf[..15])
            .await
            .unwrap();
        assert_eq!(&buf[..15], b"hello_encrypted");

        enc_server.write_encrypted(b"reply").await.unwrap();
        enc_client.read_exact_decrypted(&mut buf[..5]).await.unwrap();
        assert_eq!(&buf[..5], b"reply");
    }

    /// 明文 BT 握手识别：对端直接发送标准 BT 握手时，
    /// 响应方必须回退明文路径并回填已读字节。
    #[tokio::test]
    async fn pe_responder_detects_plaintext_bt() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let info_hash = InfoHash::from_bytes(&[0x77u8; 20]);
        let peer_id = PeerId::azureus_prefix(&[3u8; 12]);
        let plain_hs = crate::message::encode_handshake(&info_hash, &peer_id);

        let ih2 = info_hash;
        let our_hs = plain_hs.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            pe_handshake_responder(stream, &ih2, &our_hs, false).await.unwrap()
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        // 只发前 20 字节（握手的其余部分稍后才发），检测路径必须回填这 20 字节
        client.write_all(&plain_hs[..20]).await.unwrap();

        let outcome = server.await.unwrap();
        let PeOutcome::Plaintext { pending, peer_ia, .. } = outcome else {
            panic!("应识别为明文 BT 握手");
        };
        assert_eq!(pending, plain_hs[..20].to_vec(), "已读字节必须完整回填");
        assert!(peer_ia.is_empty());
    }

    /// 负验证：响应方持有不同种子（SKEY 不同）时，
    /// 发起方握手必须失败（SKEY 哈希对不上，响应方断开）。
    #[tokio::test]
    async fn pe_wrong_skey_fails_handshake() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let initiator_skey = InfoHash::from_bytes(&[0x11u8; 20]);
        let responder_skey = InfoHash::from_bytes(&[0x22u8; 20]);

        let hs = crate::message::encode_handshake(
            &initiator_skey,
            &PeerId::azureus_prefix(&[4u8; 12]),
        );

        let rs = responder_skey;
        let hs2 = hs.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            pe_handshake_responder(stream, &rs, &hs2, false).await
        });

        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let init_result = pe_handshake_initiator(client, &initiator_skey, &hs, CRYPTO_RC4 | CRYPTO_PLAINTEXT).await;
        assert!(init_result.is_err(), "SKEY 不匹配时发起方握手必须失败");

        // 响应方也应报错（SKEY 识别失败）
        let resp_result = server.await.unwrap();
        assert!(resp_result.is_err(), "响应方应拒绝未知种子的加密握手");
    }

    /// 加密流双向通信（不经过握手，直接构造）。
    #[tokio::test]
    async fn encrypted_stream_bidirectional() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let key_a = b"key-a-stream";
        let key_b = b"key-b-stream";

        let mut c = EncryptedStream::new(
            client,
            Rc4::new(key_a, 1024),
            Rc4::new(key_b, 1024),
        );
        let mut s = EncryptedStream::new(
            server,
            Rc4::new(key_b, 1024),
            Rc4::new(key_a, 1024),
        );

        c.write_encrypted(b"ping_over_rc4").await.unwrap();
        let mut buf = [0u8; 32];
        s.read_exact_decrypted(&mut buf[..13]).await.unwrap();
        assert_eq!(&buf[..13], b"ping_over_rc4");

        s.write_encrypted(b"pong").await.unwrap();
        c.read_exact_decrypted(&mut buf[..4]).await.unwrap();
        assert_eq!(&buf[..4], b"pong");
    }

    /// 慢写者：每次 poll_write 只接受 cap 字节，模拟 TCP 写背压。
    struct DribbleWriter {
        cap: usize,
        out: Vec<u8>,
    }

    impl AsyncRead for DribbleWriter {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(())) // EOF
        }
    }

    impl AsyncWrite for DribbleWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            let this = self.get_mut();
            let n = buf.len().min(this.cap);
            this.out.extend_from_slice(&buf[..n]);
            std::task::Poll::Ready(Ok(n))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// 部分写入不得丢密文：背压下内层每次只收几个字节，
    /// 遗留密文必须原样排空，否则 RC4 流失步、对端解出乱码。
    #[tokio::test]
    async fn partial_writes_do_not_desync_rc4() {
        let key = b"desync-regression-key";
        let mut enc = EncryptedStream::new(
            DribbleWriter {
                cap: 3,
                out: Vec::new(),
            },
            Rc4::new(key, 1024),
            Rc4::new(b"unused-recv", 1024),
        );
        let plaintext: Vec<u8> = (0u8..=255).cycle().take(1000).collect();
        enc.write_all(&plaintext).await.unwrap();

        let mut ct = std::mem::take(&mut enc.inner_mut().out);
        assert_eq!(ct.len(), plaintext.len(), "密文长度应等于明文长度");
        let mut dec = Rc4::new(key, 1024);
        dec.process(&mut ct);
        assert_eq!(ct, plaintext, "部分写入后 RC4 流必须保持同步");
    }
}
