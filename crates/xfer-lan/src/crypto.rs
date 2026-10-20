//! 协议 2.1 / 2.2：会话密钥派生（ECDH P-256 + HKDF-SHA256）与 AES-256-GCM 分帧。
//!
//! 这一层的字节语义必须与对端（任何按协议文档实现的客户端）逐字节一致。
//! v2 起**不再使用任何应用私有常量**：HKDF salt 由双方临时公钥派生，
//! info 是公布的协议标签，因此第三方实现只要照文档写就能算出同一把密钥。

use std::io;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use p256::elliptic_curve::sec1::ToEncodedPoint;
use p256::{PublicKey, SecretKey};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// HKDF info：会话密钥（ASCII，协议 2.1）。
pub(crate) const INFO_KEY: &[u8] = b"lan-transfer-v2";
/// HKDF info：短认证串 SAS（ASCII，协议 2.1）。
pub(crate) const INFO_SAS: &[u8] = b"lan-transfer-v2-sas";

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

fn encode_public_point(public: &PublicKey) -> [u8; 65] {
    let point = public.to_encoded_point(false);
    let mut out = [0u8; 65];
    out.copy_from_slice(point.as_bytes());
    out
}

/// HKDF salt：`SHA-256(min(pkA,pkB) || max(pkA,pkB))`。
///
/// 排序按 65 字节编码的**字节序字典序**，两端必然得到同一个顺序；
/// 两端各自算出的两个公钥集合相同，因此 salt 相同。salt 里绑定了双方
/// 临时公钥，中间人换掉任意一把公钥都会导致 SAS 不同（见协议第 2.1 节）。
pub(crate) fn session_salt(a: &[u8; 65], b: &[u8; 65]) -> [u8; 32] {
    let (first, second) = if a.as_slice() <= b.as_slice() {
        (a, b)
    } else {
        (b, a)
    };
    let mut hasher = Sha256::new();
    hasher.update(first);
    hasher.update(second);
    hasher.finalize().into()
}

/// HKDF-SHA256：ikm = ECDH 共享 X 坐标，salt/info 由协议给定。
fn hkdf_expand(ikm: &[u8], salt: &[u8], info: &[u8], out: &mut [u8]) {
    let hkdf = Hkdf::<Sha256>::new(Some(salt), ikm);
    hkdf.expand(info, out).expect("输出长度不会超出 HKDF 上限");
}

/// HKDF(shared, salt, info="lan-transfer-v2") → 32 字节 AES-256-GCM 密钥。
pub(crate) fn hkdf_key(shared_x: &[u8], salt: &[u8]) -> [u8; KEY_LEN] {
    let mut key = [0u8; KEY_LEN];
    hkdf_expand(shared_x, salt, INFO_KEY, &mut key);
    key
}

/// 短认证串：HKDF(shared, salt, info="lan-transfer-v2-sas") 前 4 字节
/// 按大端解释后对 1_000_000 取模，左补零成 6 位数字。
pub(crate) fn sas_code(shared_x: &[u8], salt: &[u8]) -> String {
    let mut material = [0u8; 4];
    hkdf_expand(shared_x, salt, INFO_SAS, &mut material);
    format!("{:06}", u32::from_be_bytes(material) % 1_000_000)
}

/// 一次握手派生出的会话材料：加密上下文 + 供用户比对的 SAS。
pub(crate) struct SessionKeys {
    pub cipher: SessionCipher,
    pub sas: String,
}

/// ECDH → salt → 会话密钥 + SAS（两端输入同一对临时公钥，结果必然相同）。
fn session_material(secret: &SecretKey, peer: &PublicKey) -> ([u8; KEY_LEN], String) {
    let own_public = encode_public(secret);
    let peer_public = encode_public_point(peer);
    let salt = session_salt(&own_public, &peer_public);
    let shared = p256::ecdh::diffie_hellman(secret.to_nonzero_scalar(), peer.as_affine());
    // ecdh 的 raw_secret_bytes 就是共享点的 X 坐标（32 字节定长）
    let shared_x = &shared.raw_secret_bytes()[..];
    (hkdf_key(shared_x, &salt), sas_code(shared_x, &salt))
}

/// 建立一条连接的会话：会话密钥决定加解密，SAS 供两端用户比对防中间人。
pub(crate) fn establish(secret: &SecretKey, peer: &PublicKey) -> SessionKeys {
    let (key, sas) = session_material(secret, peer);
    SessionKeys {
        cipher: SessionCipher::new(key),
        sas,
    }
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

    /// salt = SHA-256(排序后拼接的两把 65 字节公钥)，与顺序无关。
    /// 期望值由独立实现（Python hashlib）算出。
    #[test]
    fn session_salt_binds_both_keys_and_is_order_independent() {
        let a = [b'A'; 65];
        let b = [b'B'; 65];
        let expected =
            hex_bytes("3726a4cc77593117c68a0d8a5aea37152991b653030bf502008efcd010d858e6");
        assert_eq!(session_salt(&a, &b).to_vec(), expected);
        assert_eq!(session_salt(&b, &a).to_vec(), expected);
    }

    /// 已知答案：ikm = 0x00..0x1f、salt = SHA-256(65*A || 65*B)、
    /// info = "lan-transfer-v2"。期望值由独立 Python 实现算出。
    #[test]
    fn hkdf_known_answer() {
        let ikm: Vec<u8> = (0u8..32).collect();
        let salt = session_salt(&[b'A'; 65], &[b'B'; 65]);
        assert_eq!(
            hkdf_key(&ikm, &salt).to_vec(),
            hex_bytes("9d002bd300504637df2be5e7fbc16a97f21dacc097147ecce7d61c39d293c151")
        );
    }

    /// SAS 已知答案：同一 ikm/salt，info = "lan-transfer-v2-sas"。
    #[test]
    fn sas_known_answer_and_shape() {
        let ikm: Vec<u8> = (0u8..32).collect();
        let salt = session_salt(&[b'A'; 65], &[b'B'; 65]);
        assert_eq!(sas_code(&ikm, &salt), "605002");
        assert_eq!(sas_code(&ikm, &salt).len(), 6);
        assert!(sas_code(&ikm, &salt).chars().all(|c| c.is_ascii_digit()));
        // 换 salt（等于换掉任意一把临时公钥）SAS 必须变
        assert_ne!(sas_code(&ikm, &salt), sas_code(&ikm, &[0u8; 32]));
    }

    /// 两端各自生成临时密钥，ECDH 必须导出一模一样的会话密钥与 SAS。
    #[test]
    fn ecdh_both_sides_derive_same_key_and_sas() {
        let a = new_secret_key();
        let b = new_secret_key();
        let a_pub = parse_public(&encode_public(&a)).expect("自己的公钥应可解析");
        let b_pub = parse_public(&encode_public(&b)).expect("自己的公钥应可解析");
        let (key_a, sas_a) = session_material(&a, &b_pub);
        let (key_b, sas_b) = session_material(&b, &a_pub);
        assert_eq!(key_a, key_b);
        assert_eq!(sas_a, sas_b);
        assert_eq!(sas_a.len(), 6);
        // 换一条连接（另一把临时密钥）必须得到不同的密钥
        let c = new_secret_key();
        let (key_c, _) = session_material(&a, &c.public_key());
        assert_ne!(key_a, key_c);
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
        let payload = cipher.seal(b"hello lan").unwrap();
        assert_eq!(cipher.open(&payload).unwrap(), b"hello lan");
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
