//! xfer-crypto：SHA-1、RC4（含 MSE 密钥流丢弃前 1024 字节）、768-bit MSE DH 群。
//!
//! 纯函数 crate。协议事实依据：与 libtorrent（qBittorrent/Deluge）和 rTorrent
//! 源码逐字段对照，即真实标准客户端使用的 Protocol Encryption (PE/MSE) 线格式：
//!
//! - DH：768-bit 固定素数 P（与 libtorrent/rTorrent 常量逐字节一致），G = 2；
//!   公钥/共享密钥 S 均为 **96 字节大端**；私钥为 160-bit 随机
//! - RC4 密钥：发起方发送 = SHA1("keyA" || S || SKEY)，响应方发送 = SHA1("keyB" || S || SKEY)，
//!   SKEY = info_hash；两条流都丢弃前 1024 字节密钥流
//! - VC = 8 个零字节；crypto_provide/select：0x01 明文、0x02 RC4
//! - 同步/识别：发起方发送明文 SHA1("req1" || S) 定位加密段起点，
//!   再发 SHA1("req2" || SKEY) ⊕ SHA1("req3" || S) 供响应方识别种子
//! - 线上公钥**不做** mod P 混淆（主流实现均发送原始公钥）

use sha1::{Digest, Sha1};

// ---------------------------------------------------------------------------
// SHA-1
// ---------------------------------------------------------------------------

/// 计算 SHA-1 摘要（20 字节）。
pub fn sha1_digest(data: &[u8]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut out = [0u8; 20];
    out.copy_from_slice(&result);
    out
}

/// 计算 SHA-1(info_hash + peer_id) — BT 握手校验用。
pub fn sha1_info_peer(info_hash: &[u8; 20], peer_id: &[u8; 20]) -> [u8; 20] {
    let mut data = Vec::with_capacity(40);
    data.extend_from_slice(info_hash);
    data.extend_from_slice(peer_id);
    sha1_digest(&data)
}

// ---------------------------------------------------------------------------
// RC4
// ---------------------------------------------------------------------------

/// RC4 加密/解密流（对称，同一密钥用于加解密）。
///
/// MSE 特殊：密钥流前 1024 字节必须丢弃。
pub struct Rc4 {
    /// S-box（256 字节状态）。
    s: [u8; 256],
    /// i 索引。
    i: u8,
    /// j 索引。
    j: u8,
}

impl Rc4 {
    /// 用密钥初始化 RC4 流。
    ///
    /// `discard` 为要丢弃的初始密钥流字节数（MSE = 1024）。
    pub fn new(key: &[u8], discard: usize) -> Self {
        let mut s: [u8; 256] = std::array::from_fn(|i| i as u8);
        // KSA (Key Scheduling Algorithm)
        let mut j: u8 = 0;
        #[allow(clippy::needless_range_loop)]
        for k in 0..256 {
            j = j.wrapping_add(s[k]).wrapping_add(key[k % key.len()]);
            s.swap(k, j as usize);
        }

        let mut rc4 = Rc4 { s, i: 0, j: 0 };

        // 丢弃前 `discard` 字节
        for _ in 0..discard {
            rc4.next_byte();
        }

        rc4
    }

    /// 生成下一个密钥流字节。
    #[inline]
    fn next_byte(&mut self) -> u8 {
        self.i = self.i.wrapping_add(1);
        self.j = self.j.wrapping_add(self.s[self.i as usize]);
        self.s.swap(self.i as usize, self.j as usize);
        // RC4 标准：t = (S[i] + S[j]) mod 256
        let t = self.s[self.i as usize].wrapping_add(self.s[self.j as usize]);
        self.s[t as usize]
    }

    /// 加密/解密数据（就地 XOR）。
    pub fn process(&mut self, data: &mut [u8]) {
        for b in data.iter_mut() {
            *b ^= self.next_byte();
        }
    }

    /// 加密/解密数据（返回新 Vec）。
    pub fn process_vec(&mut self, data: &[u8]) -> Vec<u8> {
        let mut out = data.to_vec();
        self.process(&mut out);
        out
    }
}

// ---------------------------------------------------------------------------
// 768-bit MSE DH
// ---------------------------------------------------------------------------

/// MSE DH 素数 P（768-bit，与 libtorrent/rTorrent 常量逐字节一致）。
const DH_P_HEX: &str = "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD1\
29024E088A67CC74020BBEA63B139B22514A08798E3404DD\
EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245\
E485B576625E7EC6F44C42E9A63A36210000000000090563";

/// MSE DH 生成元 G。
const DH_G: u64 = 2;

/// DH 公钥/共享密钥线上长度（96 字节大端）。
pub const DH_KEY_LEN: usize = 96;

/// VC：8 个零字节（加密协商校验常量）。
pub const VC: [u8; 8] = [0u8; 8];

/// crypto_provide/select：明文流。
pub const CRYPTO_PLAINTEXT: u32 = 0x01;

/// crypto_provide/select：RC4 加密流。
pub const CRYPTO_RC4: u32 = 0x02;

/// 发起方 RC4 密钥标签。
pub const LABEL_KEY_A: &[u8; 4] = b"keyA";

/// 响应方 RC4 密钥标签。
pub const LABEL_KEY_B: &[u8; 4] = b"keyB";

/// 768-bit DH 密钥对。
pub struct DhKeyPair {
    /// 私钥（160-bit 随机，与 libtorrent 一致）。
    private_key: num_bigint::BigUint,
    /// 公钥 Ya = G^Xa mod P（96 字节大端）。
    public_key: [u8; DH_KEY_LEN],
}

impl DhKeyPair {
    /// 生成新的 DH 密钥对。
    pub fn generate() -> Self {
        let p = dh_prime();
        let g = num_bigint::BigUint::from(DH_G);

        // 真实实现（libtorrent/rTorrent）使用 160-bit 随机私钥
        let mut key_bytes = [0u8; 20];
        loop {
            let _ = getrandom::fill(&mut key_bytes);
            if key_bytes.iter().any(|&b| b != 0) {
                break;
            }
        }

        let private_key = num_bigint::BigUint::from_bytes_be(&key_bytes);
        let public_bn = g.modpow(&private_key, &p);
        let public_key = to_be_96(&public_bn);

        DhKeyPair {
            private_key,
            public_key,
        }
    }

    /// 获取公钥（96 字节大端）。
    pub fn public_key(&self) -> [u8; DH_KEY_LEN] {
        self.public_key
    }

    /// 计算共享密钥 S = Yb^Xa mod P（96 字节大端）。
    ///
    /// `peer_public_key` 为对端公钥（96 字节大端，原始值，无 mod P 混淆）。
    pub fn compute_shared_secret(&self, peer_public_key: &[u8; DH_KEY_LEN]) -> [u8; DH_KEY_LEN] {
        let p = dh_prime();
        let peer_bn = num_bigint::BigUint::from_bytes_be(peer_public_key);
        let shared_bn = peer_bn.modpow(&self.private_key, &p);
        to_be_96(&shared_bn)
    }
}

/// 大端序列化到 96 字节（高位补零）。
fn to_be_96(n: &num_bigint::BigUint) -> [u8; DH_KEY_LEN] {
    let bytes = n.to_bytes_be();
    debug_assert!(bytes.len() <= DH_KEY_LEN);
    let mut out = [0u8; DH_KEY_LEN];
    out[DH_KEY_LEN - bytes.len()..].copy_from_slice(&bytes);
    out
}

/// 退化公钥/共享密钥检测（0 或 1）：这类值会让共享密钥坍缩，必须拒绝。
pub fn is_degenerate_key(key: &[u8]) -> bool {
    if key.iter().all(|&b| b == 0) {
        return true;
    }
    *key.last().unwrap_or(&0) == 1 && key[..key.len() - 1].iter().all(|&b| b == 0)
}

/// 获取 DH 素数 P。
fn dh_prime() -> num_bigint::BigUint {
    let hex_clean: String = DH_P_HEX.chars().filter(|c| *c != '\\').collect();
    let hex_trimmed = hex_clean.trim();
    num_bigint::BigUint::parse_bytes(hex_trimmed.as_bytes(), 16).unwrap()
}

// ---------------------------------------------------------------------------
// MSE 密钥派生与同步哈希
// ---------------------------------------------------------------------------

/// MSE 密钥角色。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MseRole {
    /// 发起方：用 keyA 派生发送流，用 keyB 派生接收流。
    Initiator,
    /// 响应方：用 keyB 派生发送流，用 keyA 派生接收流。
    Responder,
}

/// 标签 + 数据的 SHA-1（label 在前）。
fn sha1_salted(label: &[u8], data: &[&[u8]]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(label);
    for d in data {
        hasher.update(d);
    }
    let result = hasher.finalize();
    let mut out = [0u8; 20];
    out.copy_from_slice(&result);
    out
}

/// MSE RC4 密钥 = SHA1(label || S || SKEY)。
///
/// label 为 `LABEL_KEY_A`（发起方发送/响应方接收）或 `LABEL_KEY_B`（反之）。
pub fn mse_rc4_key(label: &[u8; 4], shared_secret: &[u8; DH_KEY_LEN], skey: &[u8; 20]) -> [u8; 20] {
    sha1_salted(label, &[shared_secret, skey])
}

/// 从共享密钥 S 派生发送/接收 RC4 流（各丢弃前 1024 字节密钥流）。
///
/// `skey` 为种子的 info_hash。
pub fn derive_rc4_streams(
    shared_secret: &[u8; DH_KEY_LEN],
    skey: &[u8; 20],
    role: MseRole,
) -> (Rc4, Rc4) {
    let key_a = mse_rc4_key(LABEL_KEY_A, shared_secret, skey);
    let key_b = mse_rc4_key(LABEL_KEY_B, shared_secret, skey);

    let (send_key, recv_key) = match role {
        MseRole::Initiator => (key_a, key_b), // 发起方：发 keyA，收 keyB
        MseRole::Responder => (key_b, key_a), // 响应方：发 keyB，收 keyA
    };

    // RC4 流丢弃前 1024 字节
    const RC4_DISCARD: usize = 1024;
    let send_stream = Rc4::new(&send_key, RC4_DISCARD);
    let recv_stream = Rc4::new(&recv_key, RC4_DISCARD);

    (send_stream, recv_stream)
}

/// 同步哈希 = SHA1("req1" || S)。
///
/// 发起方明文发送，响应方扫描它以定位加密段起点（跳过 PadA）。
pub fn req1_hash(shared_secret: &[u8; DH_KEY_LEN]) -> [u8; 20] {
    sha1_salted(b"req1", &[shared_secret])
}

/// 种子识别哈希 = SHA1("req2" || SKEY)。
pub fn req2_hash(skey: &[u8; 20]) -> [u8; 20] {
    sha1_salted(b"req2", &[skey])
}

/// 混淆掩码 = SHA1("req3" || S)。
pub fn req3_hash(shared_secret: &[u8; DH_KEY_LEN]) -> [u8; 20] {
    sha1_salted(b"req3", &[shared_secret])
}

/// 混淆后的种子哈希 = SHA1("req2" || SKEY) ⊕ SHA1("req3" || S)。
///
/// 发起方明文发送；响应方用本地 SKEY 反混淆并识别目标种子。
pub fn obfuscated_skey_hash(shared_secret: &[u8; DH_KEY_LEN], skey: &[u8; 20]) -> [u8; 20] {
    let mut out = req2_hash(skey);
    let mask = req3_hash(shared_secret);
    for (o, m) in out.iter_mut().zip(mask.iter()) {
        *o ^= *m;
    }
    out
}

/// 响应方校验：收到的混淆种子哈希是否与本地 SKEY 匹配。
pub fn skey_matches_obfuscated(
    received: &[u8; 20],
    shared_secret: &[u8; DH_KEY_LEN],
    skey: &[u8; 20],
) -> bool {
    obfuscated_skey_hash(shared_secret, skey) == *received
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_known_vector() {
        // SHA1("abc") = a9993e364706816aba3e25717850c26c9cd0d89d
        let result = sha1_digest(b"abc");
        assert_eq!(
            hex::encode(result),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
    }

    #[test]
    fn sha1_empty() {
        // SHA1("") = da39a3ee5e6b4b0d3255bfef95601890afd80709
        let result = sha1_digest(b"");
        assert_eq!(
            hex::encode(result),
            "da39a3ee5e6b4b0d3255bfef95601890afd80709"
        );
    }

    #[test]
    fn sha1_info_peer_combined() {
        let ih = [1u8; 20];
        let pid = [2u8; 20];
        let result = sha1_info_peer(&ih, &pid);

        // 手动验证
        let mut data = Vec::new();
        data.extend_from_slice(&ih);
        data.extend_from_slice(&pid);
        assert_eq!(result, sha1_digest(&data));
    }

    #[test]
    fn rc4_known_vector() {
        // RC4 key="Key", plaintext="Plaintext"
        // ciphertext = BBF316E8D940AF0AD3
        let key = b"Key";
        let plaintext = b"Plaintext";
        let mut rc4 = Rc4::new(key, 0); // 不丢弃
        let ciphertext = rc4.process_vec(plaintext);
        assert_eq!(hex::encode(&ciphertext), "bbf316e8d940af0ad3");
    }

    #[test]
    fn rc4_symmetric() {
        // 加密再解密应该恢复原文
        let key = b"test_key_12345";
        let plaintext = b"Hello MSE World! This is a test message.";
        let mut enc = Rc4::new(key, 1024);
        let ciphertext = enc.process_vec(plaintext);

        let mut dec = Rc4::new(key, 1024);
        let recovered = dec.process_vec(&ciphertext);
        assert_eq!(&recovered[..], plaintext);
    }

    #[test]
    fn rc4_discard_differs() {
        // 丢弃 1024 字节后与不丢弃产生不同结果
        let key = b"key";
        let plaintext = b"data";
        let mut rc4_no_discard = Rc4::new(key, 0);
        let ct1 = rc4_no_discard.process_vec(plaintext);

        let mut rc4_discard = Rc4::new(key, 1024);
        let ct2 = rc4_discard.process_vec(plaintext);

        assert_ne!(ct1, ct2);
    }

    #[test]
    fn rc4_stream_continuity() {
        // 分段处理 = 整体处理
        let key = b"stream_test";
        let data = b"0123456789ABCDEF";
        let mut rc4_whole = Rc4::new(key, 1024);
        let whole = rc4_whole.process_vec(data);

        let mut rc4_parts = Rc4::new(key, 1024);
        let mut part1 = rc4_parts.process_vec(&data[..8]);
        let part2 = rc4_parts.process_vec(&data[8..]);
        part1.extend_from_slice(&part2);

        assert_eq!(whole, part1);
    }

    #[test]
    fn dh_key_exchange() {
        // Alice 和 Bob 各自生成密钥对
        let alice = DhKeyPair::generate();
        let bob = DhKeyPair::generate();

        // 交换公钥，计算共享密钥
        let alice_shared = alice.compute_shared_secret(&bob.public_key());
        let bob_shared = bob.compute_shared_secret(&alice.public_key());

        // 双方共享密钥必须一致
        assert_eq!(
            hex::encode(alice_shared),
            hex::encode(bob_shared),
            "DH 交换后共享密钥必须一致"
        );

        // 共享密钥必须是 96 字节
        assert_eq!(alice_shared.len(), DH_KEY_LEN);
    }

    #[test]
    fn dh_public_key_is_96_bytes() {
        let pair = DhKeyPair::generate();
        let pubkey = pair.public_key();
        assert_eq!(pubkey.len(), DH_KEY_LEN);
    }

    #[test]
    fn dh_multiple_exchanges() {
        // 多次 DH 交换都应产生一致的共享密钥
        for _ in 0..5 {
            let alice = DhKeyPair::generate();
            let bob = DhKeyPair::generate();
            let alice_shared = alice.compute_shared_secret(&bob.public_key());
            let bob_shared = bob.compute_shared_secret(&alice.public_key());
            assert_eq!(alice_shared, bob_shared);
        }
    }

    #[test]
    fn degenerate_key_detection() {
        assert!(is_degenerate_key(&[0u8; 96]));
        let mut one = [0u8; 96];
        one[95] = 1;
        assert!(is_degenerate_key(&one));
        let mut two = [0u8; 96];
        two[95] = 2;
        assert!(!is_degenerate_key(&two));
        let randomish = [0xABu8; 96];
        assert!(!is_degenerate_key(&randomish));
    }

    #[test]
    fn mse_rc4_key_is_label_first() {
        // 关键正确性：密钥 = SHA1(label || S || SKEY)，label 在最前
        let shared = [0x42u8; 96];
        let skey = [0x99u8; 20];

        let mut data = Vec::new();
        data.extend_from_slice(b"keyA");
        data.extend_from_slice(&shared);
        data.extend_from_slice(&skey);
        assert_eq!(mse_rc4_key(LABEL_KEY_A, &shared, &skey), sha1_digest(&data));

        let mut data_b = Vec::new();
        data_b.extend_from_slice(b"keyB");
        data_b.extend_from_slice(&shared);
        data_b.extend_from_slice(&skey);
        assert_eq!(mse_rc4_key(LABEL_KEY_B, &shared, &skey), sha1_digest(&data_b));
    }

    #[test]
    fn mse_key_derivation_consistency() {
        // 发起方的发送密钥 = 响应方的接收密钥
        // 发起方的接收密钥 = 响应方的发送密钥
        let shared = [0x42u8; 96];
        let skey = [0x77u8; 20];

        let (mut init_send, mut init_recv) =
            derive_rc4_streams(&shared, &skey, MseRole::Initiator);
        let (mut resp_send, mut resp_recv) =
            derive_rc4_streams(&shared, &skey, MseRole::Responder);

        // 发起方发送 → 响应方接收
        let plaintext = b"MSE test message";
        let ciphertext = init_send.process_vec(plaintext);
        let recovered = resp_recv.process_vec(&ciphertext);
        assert_eq!(&recovered[..], plaintext);

        // 响应方发送 → 发起方接收
        let plaintext2 = b"Reply message";
        let ciphertext2 = resp_send.process_vec(plaintext2);
        let recovered2 = init_recv.process_vec(&ciphertext2);
        assert_eq!(&recovered2[..], plaintext2);
    }

    #[test]
    fn req_hashes_are_label_first_sha1() {
        let shared = [0x11u8; 96];
        let skey = [0x22u8; 20];

        let mut d1 = b"req1".to_vec();
        d1.extend_from_slice(&shared);
        assert_eq!(req1_hash(&shared), sha1_digest(&d1));

        let mut d2 = b"req2".to_vec();
        d2.extend_from_slice(&skey);
        assert_eq!(req2_hash(&skey), sha1_digest(&d2));

        let mut d3 = b"req3".to_vec();
        d3.extend_from_slice(&shared);
        assert_eq!(req3_hash(&shared), sha1_digest(&d3));
    }

    #[test]
    fn obfuscated_skey_hash_roundtrip() {
        let shared = [0x33u8; 96];
        let skey = [0x44u8; 20];

        let obf = obfuscated_skey_hash(&shared, &skey);
        // obf = req2(skey) ⊕ req3(shared)
        let expected: Vec<u8> = req2_hash(&skey)
            .iter()
            .zip(req3_hash(&shared).iter())
            .map(|(a, b)| a ^ b)
            .collect();
        assert_eq!(obf.as_slice(), expected.as_slice());

        // 响应方识别：正确 SKEY 匹配，错误 SKEY 不匹配
        assert!(skey_matches_obfuscated(&obf, &shared, &skey));
        let wrong_skey = [0x45u8; 20];
        assert!(!skey_matches_obfuscated(&obf, &shared, &wrong_skey));
        let wrong_shared = [0x34u8; 96];
        assert!(!skey_matches_obfuscated(&obf, &wrong_shared, &skey));
    }

    #[test]
    fn vc_is_eight_zero_bytes() {
        assert_eq!(VC.len(), 8);
        assert!(VC.iter().all(|&b| b == 0));
    }

    #[test]
    fn mse_full_handshake_simulation() {
        // 模拟完整 PE 密钥层：DH → S → req1/req2⊕req3 识别 → 双向 RC4
        let alice = DhKeyPair::generate();
        let bob = DhKeyPair::generate();

        let alice_shared = alice.compute_shared_secret(&bob.public_key());
        let bob_shared = bob.compute_shared_secret(&alice.public_key());
        assert_eq!(alice_shared, bob_shared);

        let skey = [0x55u8; 20];

        // 同步/识别哈希双方可独立计算且一致
        assert_eq!(req1_hash(&alice_shared), req1_hash(&bob_shared));
        let obf_a = obfuscated_skey_hash(&alice_shared, &skey);
        assert!(skey_matches_obfuscated(&obf_a, &bob_shared, &skey));

        // 双向加密通信
        let (mut alice_send, mut alice_recv) =
            derive_rc4_streams(&alice_shared, &skey, MseRole::Initiator);
        let (mut bob_send, mut bob_recv) =
            derive_rc4_streams(&bob_shared, &skey, MseRole::Responder);

        let msg_a_to_b = b"Hello from Alice to Bob via MSE!";
        let ciphertext = alice_send.process_vec(msg_a_to_b);
        let recovered = bob_recv.process_vec(&ciphertext);
        assert_eq!(&recovered[..], msg_a_to_b);

        let msg_b_to_a = b"Hi Alice, Bob received your message!";
        let ciphertext2 = bob_send.process_vec(msg_b_to_a);
        let recovered2 = alice_recv.process_vec(&ciphertext2);
        assert_eq!(&recovered2[..], msg_b_to_a);
    }
}
