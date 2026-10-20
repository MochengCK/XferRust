//! 协议 2.1 / 2.2：会话密钥派生（ECDH P-256 + HKDF-SHA256）与 AES-256-GCM 分帧。
//!
//! 这一层的字节语义必须与对端（桌面端 Node / 安卓 Kotlin）逐字节一致：
//! salt / info 是**应用身份常量**，常量值一改两端就握不上手。

use std::io;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use p256::elliptic_curve::sec1::ToEncodedPoint;
use p256::{PublicKey, SecretKey};
use sha2::Sha256;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// HKDF salt（ASCII，协议 2.1；26 字节）。
pub(crate) const SALT: &[u8] = b"lerxu-lan-transfer-v1-salt";
/// HKDF info（ASCII，协议 2.1）。
pub(crate) const INFO: &[u8] = b"lerxu-lan-transfer-v1";

/// AES-256 密钥长度。
pub(crate) const KEY_LEN: usize = 32;
/// GCM nonce 长度。
pub(crate) const NONCE_LEN: usize = 12;
/// GCM tag 长度。
pub(crate) const TAG_LEN: usize = 16;
/// 单帧明文上限（协议 2.2）。
pub(crate) const MAX_PLAINTEXT: usize = 262_144;
/// 单帧负载上限 = 明文 + nonce + tag（协议 2.2，262172）。
pub(crate) const MAX_FRAME: usize = MAX_PLAINTEXT + NONCE_LEN + TAG_LEN;

/// 生成一条连接用的临时 P-256 私钥。
///
/// 不用 `rand_core::OsRng`，直接取系统随机源再交给 `SecretKey::from_slice`
/// 校验（标量越界会重试，概率可忽略）。
pub(crate) fn new_secret_key() -> SecretKey {
    loop {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).expect("系统随机源不可用");
        if let Ok(sk) = SecretKey::from_slice(&bytes) {
            return sk;
        }
    }
}

/// 公钥的线上编码：未压缩点 `04 || X(32) || Y(32)`，共 65 字节。
pub(crate) fn encode_public(sk: &SecretKey) -> [u8; 65] {
    let point = sk.public_key().to_encoded_point(false);
    let mut out = [0u8; 65];
    out.copy_from_slice(point.as_bytes());
    out
}

/// 解析对端公钥：必须是 65 字节未压缩点，且落在曲线上。
pub(crate) fn parse_public(bytes: &[u8]) -> Option<PublicKey> {
    if bytes.len() != 65 || bytes[0] != 0x04 {
        return None;
    }
    PublicKey::from_sec1_bytes(bytes).ok()
}

/// ECDH → 会话密钥。
pub(crate) fn derive_session_key(secret: &SecretKey, peer: &PublicKey) -> [u8; KEY_LEN] {
    let shared = p256::ecdh::diffie_hellman(secret.to_nonzero_scalar(), peer.as_affine());
    // ecdh 的 raw_secret_bytes 就是共享点的 X 坐标（32 字节定长）
    hkdf_key(&shared.raw_secret_bytes()[..])
}

/// HKDF-SHA256(ikm = ECDH 共享 X 坐标, salt, info) → 32 字节。
pub(crate) fn hkdf_key(shared_x: &[u8]) -> [u8; KEY_LEN] {
    let hkdf = Hkdf::<Sha256>::new(Some(SALT), shared_x);
    let mut okm = [0u8; KEY_LEN];
    hkdf.expand(INFO, &mut okm)
        .expect("32 字节输出不会超出 HKDF 上限");
    okm
}

/// 一条连接上的加密上下文（AES-256-GCM，逐帧随机 nonce，无状态计数器）。
pub(crate) struct SessionCipher {
    cipher: Aes256Gcm,
}

impl SessionCipher {
    pub(crate) fn new(key: [u8; KEY_LEN]) -> Self {
        Self {
            cipher: Aes256Gcm::new_from_slice(&key).expect("AES-256 密钥必为 32 字节"),
        }
    }

    /// 加密一个分片 → `nonce(12) || ciphertext || tag(16)`（即帧的 L 字节负载）。
    pub(crate) fn seal(&self, plaintext: &[u8]) -> io::Result<Vec<u8>> {
        if plaintext.len() > MAX_PLAINTEXT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("分片 {} 字节超过上限 {MAX_PLAINTEXT}", plaintext.len()),
            ));
        }
        let mut nonce = [0u8; NONCE_LEN];
        getrandom::fill(&mut nonce).map_err(|e| io::Error::other(format!("随机数失败：{e}")))?;
        let ciphertext = self
            .cipher
            .encrypt(Nonce::from_slice(&nonce), plaintext)
            .map_err(|_| io::Error::other("加密失败"))?;
        let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// 解密一个帧负载；tag 不符（密钥不对/被篡改）返回错误。
    pub(crate) fn open(&self, payload: &[u8]) -> io::Result<Vec<u8>> {
        if payload.len() < NONCE_LEN + TAG_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "帧负载过短（缺 nonce/tag）",
            ));
        }
        let (nonce, ciphertext) = payload.split_at(NONCE_LEN);
        self.cipher
            .decrypt(Nonce::from_slice(nonce), ciphertext)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "解密失败（密钥不符或数据被篡改）",
                )
            })
    }
}

/// 写一个加密帧：`4 字节大端 L || L 字节负载`。
pub(crate) async fn write_frame<W>(
    writer: &mut W,
    cipher: &SessionCipher,
    plaintext: &[u8],
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let payload = cipher.seal(plaintext)?;
    let mut head = [0u8; 4];
    head.copy_from_slice(&(payload.len() as u32).to_be_bytes());
    writer.write_all(&head).await?;
    writer.write_all(&payload).await?;
    Ok(())
}

/// 写结束帧（`L = 0`，不带 nonce/tag）。
pub(crate) async fn write_eof<W>(writer: &mut W) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(&0u32.to_be_bytes()).await
}

/// 读一个帧，返回负载（仍是密文）；`None` 表示结束帧。
///
/// 读到一半断开、长度超限都返回错误——调用方据此判失败，绝不把半截当完整。
pub(crate) async fn read_frame<R>(reader: &mut R) -> io::Result<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let mut head = [0u8; 4];
    reader.read_exact(&mut head).await?;
    let len = u32::from_be_bytes(head) as usize;
    if len == 0 {
        return Ok(None);
    }
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("帧长度 {len} 超过上限 {MAX_FRAME}"),
        ));
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    Ok(Some(payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 已知答案：ikm = 0x00..0x1f，salt/info 取协议常量，
    /// 期望值由独立实现（Python hmac/hashlib）算出。
    #[test]
    fn hkdf_known_answer() {
        let ikm: Vec<u8> = (0u8..32).collect();
        let key = hkdf_key(&ikm);
        assert_eq!(
            key.to_vec(),
            hex_bytes("ec888712509e156756073d0c815216ee83462f0a36833f7033944968db1ac2e4")
        );
    }

    /// 全 0x0b 的 ikm 是 RFC5869 常见测试向量形状，同样对独立实现算出的值。
    #[test]
    fn hkdf_known_answer_repeated_ikm() {
        let key = hkdf_key(&[0x0b; 32]);
        assert_eq!(
            key.to_vec(),
            hex_bytes("ac502eccb4c50091b9d122933bba760cff6defad77c228a12d0d15477f022896")
        );
    }

    /// 两端各自生成临时密钥，ECDH 必须导出一模一样的会话密钥。
    #[test]
    fn ecdh_both_sides_derive_same_key() {
        let a = new_secret_key();
        let b = new_secret_key();
        let a_pub = parse_public(&encode_public(&a)).expect("自己的公钥应可解析");
        let b_pub = parse_public(&encode_public(&b)).expect("自己的公钥应可解析");
        assert_eq!(
            derive_session_key(&a, &b_pub),
            derive_session_key(&b, &a_pub)
        );
        // 换一条连接（另一把临时密钥）必须得到不同的密钥
        let c = new_secret_key();
        assert_ne!(
            derive_session_key(&a, &b_pub),
            derive_session_key(&a, &c.public_key())
        );
    }

    /// 公钥编码必须是 65 字节未压缩点。
    #[test]
    fn public_key_is_uncompressed() {
        let sk = new_secret_key();
        let bytes = encode_public(&sk);
        assert_eq!(bytes.len(), 65);
        assert_eq!(bytes[0], 0x04);
        assert_eq!(parse_public(&bytes[..64]), None);
        let mut bad = bytes;
        bad[64] ^= 0xff; // 不在曲线上的点
        assert_eq!(parse_public(&bad), None);
    }

    #[test]
    fn frame_roundtrip() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let cipher = SessionCipher::new([7u8; KEY_LEN]);
            let reader_cipher = SessionCipher::new([7u8; KEY_LEN]);
            let plaintext: Vec<u8> = (0..MAX_PLAINTEXT).map(|i| (i % 251) as u8).collect();
            let (mut a, mut b) = tokio::io::duplex(64 * 1024);
            let payload = plaintext.clone();
            let writer = tokio::spawn(async move {
                write_frame(&mut a, &cipher, &payload).await?;
                write_eof(&mut a).await
            });
            let frame = read_frame(&mut b).await.unwrap().expect("第一帧");
            assert_eq!(frame.len(), MAX_PLAINTEXT + NONCE_LEN + TAG_LEN);
            let plain = reader_cipher.open(&frame).unwrap();
            assert_eq!(plain, plaintext);
            assert_eq!(read_frame(&mut b).await.unwrap(), None);
            writer.await.unwrap().unwrap();
        });
    }

    /// 负载任意一个字节被改动（nonce / 密文 / tag）都必须解不开。
    #[test]
    fn tampered_frame_fails() {
        let cipher = SessionCipher::new([9u8; KEY_LEN]);
        let payload = cipher.seal(b"hello lerxu").unwrap();
        assert_eq!(cipher.open(&payload).unwrap(), b"hello lerxu");
        for i in [0usize, NONCE_LEN, payload.len() - 1] {
            let mut bad = payload.clone();
            bad[i] ^= 0x01;
            assert!(cipher.open(&bad).is_err(), "第 {i} 字节被篡改却解开了");
        }
        // 截断（丢 tag 的一部分）也必须失败
        assert!(cipher.open(&payload[..payload.len() - 1]).is_err());
    }

    /// 密钥不同（对端不是同一会话）也解不开。
    #[test]
    fn wrong_key_fails() {
        let a = SessionCipher::new([1u8; KEY_LEN]);
        let b = SessionCipher::new([2u8; KEY_LEN]);
        let payload = a.seal(b"secret").unwrap();
        assert!(b.open(&payload).is_err());
    }

    fn hex_bytes(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }
}
