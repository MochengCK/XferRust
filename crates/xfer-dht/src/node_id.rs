//! 节点 ID：160-bit，与 info_hash 同空间。
//!
//! XOR 距离是 DHT 路由的核心度量：distance(A, B) = A XOR B。

use sha1::{Digest, Sha1};
use xfer_types::InfoHash;

/// 160-bit 节点 ID。
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub [u8; 20]);

impl NodeId {
    pub fn from_bytes(b: &[u8; 20]) -> Self {
        Self(*b)
    }

    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    /// 随机生成一个节点 ID。
    pub fn random() -> Self {
        let mut buf = [0u8; 20];
        getrandom::fill(&mut buf).expect("系统随机源不可用");
        Self(buf)
    }

    /// 从 info_hash 创建（用于 get_peers 查询目标）。
    pub fn from_info_hash(ih: InfoHash) -> Self {
        Self(*ih.as_bytes())
    }

    /// XOR 距离（返回 20 字节）。
    pub fn xor(&self, other: &NodeId) -> [u8; 20] {
        let mut out = [0u8; 20];
        for (out, (a, b)) in out.iter_mut().zip(self.0.iter().zip(other.0.iter())) {
            *out = a ^ b;
        }
        out
    }

    /// XOR 距离的前导零位数（用于 K-bucket 定位）。
    /// 返回 0..160，值越大表示距离越远。
    pub fn leading_zero_bits(xor: &[u8; 20]) -> usize {
        for (i, &b) in xor.iter().enumerate() {
            if b != 0 {
                return i * 8 + b.leading_zeros() as usize;
            }
        }
        160
    }

    /// 从 hex 字符串解析。
    pub fn from_hex(s: &str) -> Option<Self> {
        let v = hex::decode(s).ok()?;
        if v.len() != 20 {
            return None;
        }
        let mut out = [0u8; 20];
        out.copy_from_slice(&v);
        Some(Self(out))
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeId({})", self.to_hex())
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// 从随机数据派生节点 ID（用于确定性测试）。
#[allow(dead_code)]
pub fn node_id_from_seed(seed: &[u8]) -> NodeId {
    let mut h = Sha1::new();
    h.update(seed);
    let r: [u8; 20] = h.finalize().into();
    NodeId(r)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xor_distance_symmetric() {
        let a = node_id_from_seed(b"alice");
        let b = node_id_from_seed(b"bob");
        assert_eq!(a.xor(&b), b.xor(&a));
    }

    #[test]
    fn xor_with_self_is_zero() {
        let a = NodeId::random();
        let d = a.xor(&a);
        assert!(d.iter().all(|&b| b == 0));
        assert_eq!(NodeId::leading_zero_bits(&d), 160);
    }

    #[test]
    fn leading_zero_bits_basic() {
        // 0xFF → 0 前导零
        assert_eq!(NodeId::leading_zero_bits(&[0xFF; 20]), 0);
        // 0x7F → 1 前导零
        let mut x = [0u8; 20];
        x[0] = 0x7F;
        assert_eq!(NodeId::leading_zero_bits(&x), 1);
        // 0x01 → 7 前导零
        x[0] = 0x01;
        assert_eq!(NodeId::leading_zero_bits(&x), 7);
        // 全 0 → 160
        assert_eq!(NodeId::leading_zero_bits(&[0u8; 20]), 160);
    }

    #[test]
    fn hex_roundtrip() {
        let id = NodeId::random();
        let h = id.to_hex();
        assert_eq!(h.len(), 40);
        assert_eq!(NodeId::from_hex(&h), Some(id));
        assert!(NodeId::from_hex("short").is_none());
    }

    #[test]
    fn random_is_unique() {
        let a = NodeId::random();
        let b = NodeId::random();
        assert_ne!(a, b);
    }
}
