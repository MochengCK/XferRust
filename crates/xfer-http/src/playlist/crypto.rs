//! HLS 分片解密：密钥模型、整段 AES-CBC / AES-CTR、样本级 SAMPLE-AES，以及 DRM 的识别。
//!
//! 线上真实存在的加密形态比 RFC 8216 写的那一种多得多，这一层的目标很明确：
//! **能解的一律解开，解不开的给一句能看懂的话**（而不是解出一堆垃圾字节让用户
//! 拿到一个"能播但花屏"的文件）。
//!
//! ## 支持面
//!
//! | `METHOD` | 处理方式 |
//! |---|---|
//! | `NONE` | 明文 |
//! | `AES-128` | 整段 AES-128-CBC（[`decrypt_cbc`]） |
//! | `AES-256` | 整段 AES-256-CBC（非标准，但线上有；密钥 32 字节） |
//! | `AES-128-CTR` / `AES-CTR` | 整段 AES-128-CTR（非标准，少数站点在用） |
//! | `SAMPLE-AES` | 样本级加密：MPEG-TS 见 [`super::ts`]，fMP4（cbcs）见 [`super::mp4`] |
//! | `SAMPLE-AES-CTR` | fMP4 的 cenc 形态，同样见 [`super::mp4`] |
//!
//! ## 为什么 DRM 要单独识别
//!
//! `KEYFORMAT` 是 FairPlay / Widevine / PlayReady 时，密钥根本不在这条链路上 ——
//! 它要拿 `skd://` / `data:` 里的授权信息去**授权服务器**换，没有用户自己的账号
//! 与设备证书是换不到的。这类流不是"没实现"，而是"实现不了"，所以这里给一条
//! 点明原因的错（用户至少知道该换条路，而不是以为引擎坏了）。

use base64::Engine as _;

/// `#EXT-X-KEY:METHOD` 的取值（归一化后）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyMethod {
    /// 明文（`METHOD=NONE`，或整条清单里就没有 KEY 标签）。
    None,
    /// 整段 AES-128-CBC。
    Aes128,
    /// 整段 AES-256-CBC（非标准：少数站点用 32 字节密钥配 `METHOD=AES-128`）。
    Aes256,
    /// 整段 AES-128-CTR（非标准）。
    Aes128Ctr,
    /// 样本级加密，CBC 形态（Apple `SAMPLE-AES`）。
    SampleAes,
    /// 样本级加密，CTR 形态（fMP4 `cenc`，即 `SAMPLE-AES-CTR`）。
    SampleAesCtr,
}

impl KeyMethod {
    /// 从 `METHOD` 属性值解析（大小写不敏感，容忍前后空白）。
    ///
    /// 认不出的取值**不报错**而是回 [`KeyMethod::None`]：线上偶尔能看到
    /// 拼错的 `METHOD`，按明文处理至少能把片子下下来（真加密的流解出来一眼
    /// 就能看出问题，比整个任务失败好）。同时返回是否认得，供调用方记日志。
    pub fn parse(raw: &str) -> (Self, bool) {
        let v = raw.trim().to_ascii_uppercase();
        match v.as_str() {
            "" | "NONE" => (Self::None, true),
            "AES-128" => (Self::Aes128, true),
            "AES-256" => (Self::Aes256, true),
            "AES-128-CTR" | "AES-CTR" => (Self::Aes128Ctr, true),
            "SAMPLE-AES" => (Self::SampleAes, true),
            "SAMPLE-AES-CTR" => (Self::SampleAesCtr, true),
            _ => (Self::None, false),
        }
    }

    /// 是否**需要**下载密钥。
    pub fn needs_key(&self) -> bool {
        !matches!(self, Self::None)
    }

    /// 是否为样本级加密（要按容器拆样本，而不是整段一起解）。
    pub fn is_sample_level(&self) -> bool {
        matches!(self, Self::SampleAes | Self::SampleAesCtr)
    }
}

/// `#EXT-X-KEY:KEYFORMAT` 的归一化取值：决定这把钥匙**能不能拿到**。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyFormat {
    /// 标准：密钥就是 URI 指向的那串字节。
    Identity,
    /// Apple FairPlay（`com.apple.streamingkeydelivery`，URI 是 `skd://`）。
    FairPlay,
    /// Google Widevine（`urn:uuid:edef8ba9-…` 或 `com.widevine`）。
    Widevine,
    /// Microsoft PlayReady（`com.microsoft.playready` 或 `urn:uuid:9a04f079-…`）。
    PlayReady,
    /// 其它（原样留一份，报错时写进消息里）。
    Other(String),
}

impl KeyFormat {
    /// 从 `KEYFORMAT` 属性值解析。缺省 = `identity`（RFC 8216 的规定）。
    pub fn parse(raw: Option<&str>) -> Self {
        let Some(v) = raw else { return Self::Identity };
        let v = v.trim();
        let lower = v.to_ascii_lowercase();
        if lower.is_empty() || lower == "identity" {
            return Self::Identity;
        }
        if lower.contains("streamingkeydelivery") || lower.contains("fairplay") {
            return Self::FairPlay;
        }
        if lower.contains("edef8ba9") || lower.contains("widevine") {
            return Self::Widevine;
        }
        if lower.contains("playready") || lower.contains("9a04f079") {
            return Self::PlayReady;
        }
        Self::Other(v.to_string())
    }

    /// 是不是 DRM（拿不到密钥）。是的话返回一句给用户看的原因。
    pub fn drm_reason(&self) -> Option<&'static str> {
        match self {
            Self::FairPlay => Some("Apple FairPlay"),
            Self::Widevine => Some("Google Widevine"),
            Self::PlayReady => Some("Microsoft PlayReady"),
            Self::Identity | Self::Other(_) => None,
        }
    }
}

/// 一条 `#EXT-X-KEY` 描述的密钥（作用范围：直到下一条 KEY 标签）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentKey {
    /// 密钥地址（已相对清单 URL 解析为绝对地址）。
    pub url: String,
    /// 初始向量（`IV` 缺省时按媒体序号大端 16 字节推导）。
    pub iv: [u8; 16],
    /// 加密方式。
    pub method: KeyMethod,
    /// 密钥形态（DRM 判定用）。
    pub key_format: KeyFormat,
}

impl SegmentKey {
    /// 这把密钥能不能真的用上；不能就给出**面向用户**的原因。
    ///
    /// 三类拒绝：DRM（密钥不在链路上）、未知 `METHOD`（不知道该怎么解）、
    /// 以及样本级加密但容器不认识（TS / fMP4 之外的封装，例如裸 ES）。
    pub fn unsupported_reason(&self) -> Option<String> {
        if let Some(drm) = self.key_format.drm_reason() {
            return Some(format!(
                "该视频使用 {drm} 数字版权保护（DRM），密钥由授权服务器下发，\
                 无法在不登录的情况下解密。请改用能播放它的官方客户端。"
            ));
        }
        match self.method {
            KeyMethod::None => None,
            KeyMethod::Aes128 | KeyMethod::Aes256 | KeyMethod::Aes128Ctr => None,
            // 样本级加密的容器判定要等拿到分片首字节，这里不算"不支持"
            KeyMethod::SampleAes | KeyMethod::SampleAesCtr => None,
        }
    }
}

/// 从密钥响应体里取出密钥字节。
///
/// 线上至少三种发法，**都要认**（早先只认第一种，于是大量站点的密钥被判成
/// "长度不对"，任务直接失败）：
///
/// 1. 裸二进制：长度正好等于 `need`；
/// 2. 十六进制文本：`need * 2` 个 hex 字符（`hexdump` 出来的那种）；
/// 3. Base64 文本：`need` 字节编码后的 24 / 44 个字符。
///
/// 都不是时按"裸密钥 + 尾部杂字节"处理（截前 `need` 字节）——很多 CDN 会在
/// 密钥后追加一个换行或一段注释，为此整条任务失败不值得。
pub fn decode_key_body(body: &[u8], need: usize) -> Result<Vec<u8>, String> {
    if body.len() == need {
        return Ok(body.to_vec());
    }
    if let Ok(text) = std::str::from_utf8(body) {
        let t = text.trim();
        if t.len() == need * 2 && t.bytes().all(|b| b.is_ascii_hexdigit()) {
            if let Ok(v) = hex::decode(t) {
                return Ok(v);
            }
        }
        if !t.is_empty() {
            if let Ok(v) = base64::engine::general_purpose::STANDARD.decode(t) {
                if v.len() == need {
                    return Ok(v);
                }
            }
        }
    }
    if body.len() > need {
        return Ok(body[..need].to_vec());
    }
    Err(format!(
        "密钥长度应为 {need} 字节，服务器实际给了 {} 字节",
        body.len()
    ))
}

/// AES-CBC 解密（整段），容忍**没有按规范填充**的站点。
///
/// 规范要求 PKCS7 填充，但线上确实有服务器不做填充、或者最后一段是短段
/// （长度不是 16 的整数倍）。三种都按"能解多少解多少"处理：
/// 先解完整分组，尾部落单的字节原样保留；填充只在**看起来确实是填充**时才剥。
pub fn decrypt_cbc(key: &[u8], iv: &[u8; 16], data: &[u8]) -> Result<Vec<u8>, String> {
    let mut cipher = AesCbc::new(key, iv)?;
    let mut out = data.to_vec();
    let whole = out.len() - out.len() % 16;
    if whole == 0 {
        // 不足一个分组：没有任何可解的内容，原样返回（短到这种程度的多半是空段）
        return Ok(out);
    }
    cipher.decrypt_in_place(&mut out[..whole]);
    // 尾部落单的字节（非 16 整数倍）说明这段不是规范加密的：不要再去剥填充
    if whole == out.len() {
        strip_pkcs7(&mut out);
    }
    Ok(out)
}

/// AES-CTR 解密（整段）：计数器就是 IV 本身，128 位大端递增（cenc 的口径）。
pub fn decrypt_ctr(key: &[u8], iv: &[u8; 16], data: &[u8]) -> Result<Vec<u8>, String> {
    let mut cipher = AesEcb::new(key)?;
    let mut out = data.to_vec();
    let mut counter = *iv;
    for chunk in out.chunks_mut(16) {
        let mut ks = counter;
        cipher.encrypt_block(&mut ks);
        for (b, k) in chunk.iter_mut().zip(ks.iter()) {
            *b ^= *k;
        }
        // 128 位大端 +1（进位跨字节；整块计数器循环一周等于 2^128 块，实际到不了）
        for i in (0..16).rev() {
            counter[i] = counter[i].wrapping_add(1);
            if counter[i] != 0 {
                break;
            }
        }
    }
    Ok(out)
}

/// 剥掉 PKCS7 填充（只在**确实像填充**时才剥）。
///
/// 判据与规范一致：末字节 n ∈ 1..=16 且末尾 n 个字节都等于 n。不满足就原样保留 ——
/// 宁可留几个字节的填充，也不能把用户的正片数据当填充切掉。
fn strip_pkcs7(buf: &mut Vec<u8>) {
    let Some(&last) = buf.last() else { return };
    let n = last as usize;
    if n == 0 || n > 16 || n > buf.len() {
        return;
    }
    if buf[buf.len() - n..].iter().all(|&b| b == last) {
        buf.truncate(buf.len() - n);
    }
}

// ---------------------------------------------------------------------------
// 原始分组操作（供整段解密与样本级解密共用）
// ---------------------------------------------------------------------------

/// 按密钥长度分派的 AES 分组解密（ECB 单块语义，CBC 的链式由调用方维护）。
enum AesCbc {
    A128(aes::Aes128, [u8; 16]),
    A256(aes::Aes256, [u8; 16]),
}

impl AesCbc {
    fn new(key: &[u8], iv: &[u8; 16]) -> Result<Self, String> {
        use aes::cipher::KeyInit;
        match key.len() {
            16 => {
                let k = aes::Aes128::new(key.into());
                Ok(Self::A128(k, *iv))
            }
            32 => {
                let k = aes::Aes256::new(key.into());
                Ok(Self::A256(k, *iv))
            }
            other => Err(format!(
                "AES 密钥长度应为 16 或 32 字节，实际 {other} 字节"
            )),
        }
    }

    /// 就地解一段**连续**数据，CBC 链式贯穿整段（IV 取自构造时的值）。
    fn decrypt_in_place(&mut self, data: &mut [u8]) {
        use aes::cipher::BlockDecryptMut;
        let mut prev = match self {
            Self::A128(_, iv) | Self::A256(_, iv) => *iv,
        };
        for chunk in data.chunks_exact_mut(16) {
            let mut block = [0u8; 16];
            block.copy_from_slice(chunk);
            let ct = block;
            match self {
                Self::A128(c, _) => c.decrypt_block_mut((&mut block).into()),
                Self::A256(c, _) => c.decrypt_block_mut((&mut block).into()),
            }
            for i in 0..16 {
                block[i] ^= prev[i];
            }
            prev = ct;
            chunk.copy_from_slice(&block);
        }
    }
}

/// AES 分组**加密**（CTR 的密钥流生成只需要加密方向）。
enum AesEcb {
    A128(aes::Aes128),
    A256(aes::Aes256),
}

impl AesEcb {
    fn new(key: &[u8]) -> Result<Self, String> {
        use aes::cipher::KeyInit;
        match key.len() {
            16 => Ok(Self::A128(aes::Aes128::new(key.into()))),
            32 => Ok(Self::A256(aes::Aes256::new(key.into()))),
            other => Err(format!(
                "AES 密钥长度应为 16 或 32 字节，实际 {other} 字节"
            )),
        }
    }

    fn encrypt_block(&mut self, block: &mut [u8; 16]) {
        use aes::cipher::BlockEncryptMut;
        match self {
            Self::A128(c) => c.encrypt_block_mut(block.into()),
            Self::A256(c) => c.encrypt_block_mut(block.into()),
        }
    }
}

// ---------------------------------------------------------------------------
// 样本级解密：SAMPLE-AES（TS 的 CBC 形态 / fMP4 的 cbcs 形态）
// ---------------------------------------------------------------------------

/// 样本级解密的**逐样本**驱动：把一个 protected block 里的加密区按
/// `[16 字节加密][至多 skip 字节明文]` 的节奏解开。
///
/// Apple 规范（"MPEG-2 Stream Encryption Format for HTTP Live Streaming" §2.1）：
/// - CBC 只在**单个 protected block 内部**链式，**每个新 protected block 开始时
///   IV 复位**；
/// - H.264 用 10% 跳加密（16 加密 + 144 明文）；AAC 是"头之后整段加密，无跳过"。
///
/// 两者只差 `skip`（144 与 0）……**但还差一条，而且这条踩过坑**：剩余正好 16 字节
/// 那一块要不要解，两种格式的规范写法不一样 ——
///
/// - H.264 的循环是 `if (bytes_remaining() > 16)`：**最后那个整块不解**
///   （"NAL ≤ 48 字节时完全明文"就是这条推出来的）；
/// - AAC 的循环是 `while (bytes_remaining() >= 16)`：**每个整块都要解**。
///
/// 用一个 `> 16` 通吃会让 AAC 的最后一块留在密文状态（整帧最后一个 16 字节块花屏），
/// 所以 `trailing_full_block` 必须由调用方按格式给。
pub(crate) struct SampleDecryptor {
    cipher: AesCbc,
    /// 每块开始时的 IV（每个 protected block 复位到它）。
    base_iv: [u8; 16],
}

impl SampleDecryptor {
    pub(crate) fn new(key: &[u8], iv: &[u8; 16]) -> Result<Self, String> {
        Ok(Self {
            cipher: AesCbc::new(key, iv)?,
            base_iv: *iv,
        })
    }

    /// 解开一个 protected block：`data` 是**去掉起始明文头之后**的整段
    /// （即加密区起点），`skip` 是每个加密块之后要跳过的明文字节数，
    /// `trailing_full_block` 见类型说明（H.264 = false、AAC = true）。
    pub(crate) fn decrypt_block(&mut self, data: &mut [u8], skip: usize, trailing_full_block: bool) {
        use aes::cipher::BlockDecryptMut;
        let mut prev = self.base_iv;
        let mut pos = 0usize;
        while pos < data.len() {
            let rem = data.len() - pos;
            let encrypted = if trailing_full_block { rem >= 16 } else { rem > 16 };
            if encrypted {
                let mut block = [0u8; 16];
                block.copy_from_slice(&data[pos..pos + 16]);
                let ct = block;
                match &mut self.cipher {
                    AesCbc::A128(c, _) => c.decrypt_block_mut((&mut block).into()),
                    AesCbc::A256(c, _) => c.decrypt_block_mut((&mut block).into()),
                }
                for i in 0..16 {
                    block[i] ^= prev[i];
                }
                prev = ct;
                data[pos..pos + 16].copy_from_slice(&block);
            }
            pos += 16;
            pos += skip.min(data.len().saturating_sub(pos));
        }
    }
}

/// 去掉 H.264 字节流里的**起始码防竞争字节**（`00 00 03` → `00 00`）。
///
/// 规范要求：SAMPLE-AES 的加密区是按"去掉防竞争字节之后"的 NAL 定位的，
/// 所以解密前必须先去一遍；解完再补回去（见 [`add_emulation_prevention`]）。
pub(crate) fn remove_emulation_prevention(nal: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(nal.len());
    let mut zeros = 0u32;
    for &b in nal {
        if zeros >= 2 && b == 0x03 {
            // 这个 0x03 是插入的防竞争字节：丢掉，并把零计数清零（下一个字节从新算）
            zeros = 0;
            continue;
        }
        if b == 0x00 {
            zeros += 1;
        } else {
            zeros = 0;
        }
        out.push(b);
    }
    out
}

/// 把起始码防竞争字节补回去（加密后必须重做，见规范 §2.2）。
///
/// 输出长度可能**大于**输入 —— 这正是 TS 的 SAMPLE-AES 必须重新封装的原因
/// （见 [`super::ts`]）。
pub(crate) fn add_emulation_prevention(nal: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(nal.len() + 8);
    let mut zeros = 0u32;
    for &b in nal {
        if zeros >= 2 && b <= 0x03 {
            out.push(0x03);
            zeros = 0;
        }
        if b == 0x00 {
            zeros += 1;
        } else {
            zeros = 0;
        }
        out.push(b);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc_cbc(key: &[u8], iv: &[u8; 16], data: &[u8]) -> Vec<u8> {
        use aes::cipher::{BlockEncryptMut, KeyIvInit};
        let mut buf = vec![0u8; data.len() + 16];
        buf[..data.len()].copy_from_slice(data);
        if key.len() == 16 {
            let e = cbc::Encryptor::<aes::Aes128>::new(key.into(), iv.into());
            e.encrypt_padded_mut::<aes::cipher::block_padding::Pkcs7>(&mut buf, data.len())
                .unwrap()
                .to_vec()
        } else {
            let e = cbc::Encryptor::<aes::Aes256>::new(key.into(), iv.into());
            e.encrypt_padded_mut::<aes::cipher::block_padding::Pkcs7>(&mut buf, data.len())
                .unwrap()
                .to_vec()
        }
    }

    #[test]
    fn method_parse_covers_market_spellings() {
        assert_eq!(KeyMethod::parse("AES-128").0, KeyMethod::Aes128);
        assert_eq!(KeyMethod::parse(" aes-128 ").0, KeyMethod::Aes128);
        assert_eq!(KeyMethod::parse("AES-256").0, KeyMethod::Aes256);
        assert_eq!(KeyMethod::parse("AES-128-CTR").0, KeyMethod::Aes128Ctr);
        assert_eq!(KeyMethod::parse("SAMPLE-AES").0, KeyMethod::SampleAes);
        assert_eq!(KeyMethod::parse("SAMPLE-AES-CTR").0, KeyMethod::SampleAesCtr);
        assert_eq!(KeyMethod::parse("NONE").0, KeyMethod::None);
        // 认不出的取值：按明文处理，但明确告知调用方"没认出"
        let (m, known) = KeyMethod::parse("AES-192-WEIRD");
        assert_eq!(m, KeyMethod::None);
        assert!(!known);
    }

    #[test]
    fn key_format_detects_drm() {
        assert_eq!(KeyFormat::parse(None), KeyFormat::Identity);
        assert_eq!(KeyFormat::parse(Some("identity")), KeyFormat::Identity);
        assert_eq!(
            KeyFormat::parse(Some("com.apple.streamingkeydelivery")),
            KeyFormat::FairPlay
        );
        assert_eq!(
            KeyFormat::parse(Some("urn:uuid:edef8ba9-79d6-4ace-a3c8-27dcd51d21ed")),
            KeyFormat::Widevine
        );
        assert_eq!(
            KeyFormat::parse(Some("com.microsoft.playready")),
            KeyFormat::PlayReady
        );
        assert!(KeyFormat::parse(Some("com.microsoft.playready")).drm_reason().is_some());
        // 认不出的 KEYFORMAT：没有 DRM 结论（照常按 identity 去取密钥）
        assert!(KeyFormat::Other("com.example.custom".into()).drm_reason().is_none());
        assert!(KeyFormat::Identity.drm_reason().is_none());
    }

    #[test]
    fn drm_key_is_reported_not_silently_broken() {
        let k = SegmentKey {
            url: "skd://asset".into(),
            iv: [0; 16],
            method: KeyMethod::SampleAes,
            key_format: KeyFormat::FairPlay,
        };
        let msg = k.unsupported_reason().expect("DRM 必须给出原因");
        assert!(msg.contains("FairPlay"));
    }

    #[test]
    fn key_body_accepts_raw_hex_and_base64() {
        let raw: Vec<u8> = (0u8..16).collect();
        assert_eq!(decode_key_body(&raw, 16).unwrap(), raw);

        let hex_text = hex::encode(&raw);
        assert_eq!(decode_key_body(hex_text.as_bytes(), 16).unwrap(), raw);

        let b64 = base64::engine::general_purpose::STANDARD.encode(&raw);
        assert_eq!(decode_key_body(b64.as_bytes(), 16).unwrap(), raw);

        // 尾部带换行/杂字节：按裸密钥截断
        let mut padded = raw.clone();
        padded.extend_from_slice(b"\n");
        assert_eq!(decode_key_body(&padded, 16).unwrap(), raw);

        // 32 字节密钥（AES-256 的 need=32）
        let raw32: Vec<u8> = (0u8..32).collect();
        assert_eq!(decode_key_body(&raw32, 32).unwrap(), raw32);

        // 太短：明确报错
        assert!(decode_key_body(&raw[..8], 16).is_err());
    }

    #[test]
    fn cbc_roundtrip_128_and_256() {
        let iv = [7u8; 16];
        for key_len in [16usize, 32] {
            let key: Vec<u8> = (0..key_len).map(|i| i as u8 + 1).collect();
            let plain: Vec<u8> = (0..200u32).map(|i| (i % 251) as u8).collect();
            let ct = enc_cbc(&key, &iv, &plain);
            assert_eq!(ct.len() % 16, 0);
            assert_eq!(decrypt_cbc(&key, &iv, &ct).unwrap(), plain);
        }
    }

    #[test]
    fn cbc_tolerates_missing_padding() {
        // 无填充的密文（长度正好是 16 的整数倍）：解出来必须保留全部明文
        use aes::cipher::{BlockEncryptMut, KeyInit};
        let key = [3u8; 16];
        let iv = [9u8; 16];
        let plain = [0xABu8; 32];
        let mut buf = plain.to_vec();
        // 手工做一遍无填充 CBC（cbc crate 的加密入口都要填充）
        let mut cipher = aes::Aes128::new(&key.into());
        let mut prev = iv;
        for chunk in buf.chunks_exact_mut(16) {
            let mut block = [0u8; 16];
            block.copy_from_slice(chunk);
            for i in 0..16 {
                block[i] ^= prev[i];
            }
            cipher.encrypt_block_mut((&mut block).into());
            prev = block;
            chunk.copy_from_slice(&block);
        }
        let got = decrypt_cbc(&key, &iv, &buf).unwrap();
        assert_eq!(got, plain.to_vec());
    }

    #[test]
    fn cbc_keeps_trailing_partial_block() {
        // 长度不是 16 整数倍：完整分组解出来、尾字节原样留（不做填充剥离）
        let key = [5u8; 16];
        let iv = [6u8; 16];
        let plain: Vec<u8> = (0..40u32).map(|i| i as u8).collect();
        // PKCS7 会把 40 字节补到 48
        let mut ct = enc_cbc(&key, &iv, &plain);
        assert_eq!(ct.len(), 48);
        ct.extend_from_slice(&[0xEE, 0xFF, 0x11]);
        let got = decrypt_cbc(&key, &iv, &ct).unwrap();
        assert_eq!(got.len(), 51);
        assert_eq!(&got[..40], &plain[..]);
        // 长度不是 16 的整数倍说明这段不是规范加密的：填充**不剥**（宁可多留几字节，
        // 也不能把正片数据当填充切掉），落单的尾字节原样保留
        assert_eq!(&got[48..], &[0xEE, 0xFF, 0x11]);
    }

    #[test]
    fn ctr_roundtrip() {
        let key = [0x11u8; 16];
        let iv = [0x22u8; 16];
        let plain: Vec<u8> = (0..100u32).map(|i| (i * 7 % 253) as u8).collect();
        // CTR 自反：加密与解密同一运算
        let ct = decrypt_ctr(&key, &iv, &plain).unwrap();
        assert_ne!(ct, plain);
        assert_eq!(decrypt_ctr(&key, &iv, &ct).unwrap(), plain);
    }

    #[test]
    fn ctr_counter_is_128_bit_big_endian() {
        // 计数器只在最低字节进位、跨字节进位都要正确：
        // 用一段刚好跨过 0xFF→0x00 的数据验证不会出现密钥流重复
        let key = [0x33u8; 16];
        let mut iv = [0u8; 16];
        iv[15] = 0xFE;
        let plain = vec![0u8; 48];
        let ct = decrypt_ctr(&key, &iv, &plain).unwrap();
        // 三个 16 字节块的密钥流必须互不相同
        assert_ne!(ct[0..16], ct[16..32]);
        assert_ne!(ct[16..32], ct[32..48]);
        assert_eq!(decrypt_ctr(&key, &iv, &ct).unwrap(), plain);
    }

    #[test]
    fn emulation_prevention_roundtrip() {
        // 干净 NAL（未转义，含多处 00 00 0X 模式）→ 加防竞争 → 去防竞争必须逐字节还原。
        // 注意：入参必须是**干净**数据 —— 已转义的流里那个 0x03 是插入的，
        // 去一遍就没了，拿它当输入当然对不上（这正是第一次写错的地方）
        let clean = vec![
            0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x65, 0x00, 0x00, 0x01, 0xFF,
        ];
        let escaped = add_emulation_prevention(&clean);
        assert_ne!(escaped, clean, "有 00 00 0X 就必须插入防竞争字节");
        assert!(escaped.len() > clean.len());
        assert_eq!(remove_emulation_prevention(&escaped), clean);
        // 没有 00 00 0X 的数据不该被改动
        let plain = vec![0x65u8, 0x11, 0x22, 0x00, 0x33];
        assert_eq!(add_emulation_prevention(&plain), plain);
        assert_eq!(remove_emulation_prevention(&plain), plain);
    }

    #[test]
    fn sample_decryptor_matches_spec_pattern() {
        // 手工按规范加密一段（16 加密 + 144 明文循环），再用 SampleDecryptor 解回
        use aes::cipher::{BlockEncryptMut, KeyInit};
        let key = [0x42u8; 16];
        let iv = [0x24u8; 16];
        let skip = 144usize;
        let plain: Vec<u8> = (0..400u32).map(|i| (i % 256) as u8).collect();

        // 加密：逐块 ECB 加密后与前一块密文（或 IV）异或
        let mut cipher = aes::Aes128::new(&key.into());
        let mut ct = plain.clone();
        let mut prev = iv;
        let mut pos = 0usize;
        while pos < ct.len() {
            if ct.len() - pos > 16 {
                let mut block = [0u8; 16];
                block.copy_from_slice(&ct[pos..pos + 16]);
                for i in 0..16 {
                    block[i] ^= prev[i];
                }
                cipher.encrypt_block_mut((&mut block).into());
                ct[pos..pos + 16].copy_from_slice(&block);
                prev = block;
            }
            pos += 16;
            pos += skip.min(ct.len().saturating_sub(pos));
        }

        let mut dec = SampleDecryptor::new(&key, &iv).unwrap();
        let mut got = ct.clone();
        dec.decrypt_block(&mut got, skip, false);
        assert_eq!(got, plain);
    }

    #[test]
    fn sample_decryptor_resets_iv_per_block() {
        // 两个 protected block 各自从同一个 IV 开始：内容相同的两块解出来必须相同
        let key = [0x55u8; 16];
        let iv = [0x66u8; 16];
        let mut dec = SampleDecryptor::new(&key, &iv).unwrap();
        let mut a = vec![0xAAu8; 160];
        let mut b = vec![0xAAu8; 160];
        dec.decrypt_block(&mut a, 144, false);
        dec.decrypt_block(&mut b, 144, false);
        assert_eq!(a, b);
    }

    #[test]
    fn sample_decryptor_trailing_block_differs_between_formats() {
        // 剩余正好 16 字节那一块：H.264(false) 不解、AAC(true) 要解。
        // 这是两个格式规范写法的差异，混用会让 AAC 帧尾一个块留在密文状态
        let key = [0x77u8; 16];
        let iv = [0x88u8; 16];
        let data = [0x11u8; 16];

        let mut h264 = SampleDecryptor::new(&key, &iv).unwrap();
        let mut a = data;
        h264.decrypt_block(&mut a, 144, false);
        assert_eq!(a, data, "H.264：整块不解");

        let mut aac = SampleDecryptor::new(&key, &iv).unwrap();
        let mut b = data;
        aac.decrypt_block(&mut b, 0, true);
        assert_ne!(b, data, "AAC：整块要解");
    }
}
