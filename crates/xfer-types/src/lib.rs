//! 公共基础类型：InfoHash、PeerId、引擎版本常量。
//!
//! 依赖方向的根：所有 crate 都可以依赖本 crate，本 crate 不依赖任何兄弟 crate。

/// 引擎版本。与 XferRust 的 Cargo.toml version 保持同步，
/// RPC 版本查询与 UA/peer-id 前缀派生均以此为唯一源。
pub const ENGINE_VERSION: &str = "0.2.0";

/// 引擎名称（UA、RPC feature 列表使用）。
pub const ENGINE_NAME: &str = "XferRust";

/// BT info-hash（SHA-1，20 字节）。
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct InfoHash(pub [u8; 20]);

impl InfoHash {
    pub fn from_bytes(b: &[u8; 20]) -> Self {
        Self(*b)
    }

    /// 从 40 字符 hex 解析。
    pub fn from_hex(s: &str) -> Option<Self> {
        let v = hex::decode(s).ok()?;
        if v.len() != 20 {
            return None;
        }
        let mut out = [0u8; 20];
        out.copy_from_slice(&v);
        Some(Self(out))
    }

    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl std::fmt::Display for InfoHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl std::fmt::Debug for InfoHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InfoHash({self})")
    }
}

/// BT peer-id（20 字节）。Azureus 风格前缀 `-XR{major}{minor}{micro}0-`
/// 由 [`PeerId::azureus_prefix`] 从引擎版本派生。
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerId(pub [u8; 20]);

/// 任务 GID：16 位小写 hex 字符串标识（线上协议字段，纯 ASCII）。
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Gid(pub String);

impl Gid {
    /// 生成随机 GID（8 随机字节 → 16 hex 字符）。
    pub fn generate() -> Self {
        let mut buf = [0u8; 8];
        getrandom::fill(&mut buf).expect("系统随机源不可用");
        Self(hex::encode(buf))
    }

    /// 解析外部传入的 GID 字符串（16 位 hex）。
    pub fn parse(s: &str) -> Option<Self> {
        if s.len() == 16 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
            Some(Self(s.to_ascii_lowercase()))
        } else {
            None
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Gid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::fmt::Debug for Gid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Gid({})", self.0)
    }
}

impl<'a> From<&'a str> for Gid {
    fn from(s: &'a str) -> Self {
        Self(s.to_string())
    }
}

impl PeerId {
    /// 按引擎版本生成 Azureus 风格前缀（如 0.2.0 → `-XR0200-`），
    /// 尾部以随机字节填满 20 字节。
    pub fn azureus_prefix(random: &[u8; 12]) -> Self {
        let (maj, min, mic) = version_tuple();
        let prefix = format!("-XR{maj}{min}{mic}0-");
        let mut out = [0u8; 20];
        let p = prefix.as_bytes();
        debug_assert_eq!(p.len(), 8, "version 前缀长度必须为 8");
        out[..p.len()].copy_from_slice(p);
        out[p.len()..].copy_from_slice(random);
        Self(out)
    }

    /// 解析 Azureus 风格前缀出 (major, minor, micro)，非该风格返回 None。
    pub fn parse_azureus(&self) -> Option<(u8, u8, u8)> {
        let p = &self.0;
        if p[0] != b'-' || p[1] != b'X' || p[2] != b'R' || p[7] != b'-' {
            return None;
        }
        let d = |b: u8| (b as char).is_ascii_digit().then(|| b - b'0');
        Some((d(p[3])?, d(p[4])?, d(p[5])?))
    }
}

impl std::fmt::Debug for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PeerId({})", String::from_utf8_lossy(&self.0))
    }
}

/// 解析 [`ENGINE_VERSION`] 为 (major, minor, micro)。
/// 版本号各位必须是一位数（`-XR%d%d%d0-` 编码要求）。
fn version_tuple() -> (u8, u8, u8) {
    let mut it = ENGINE_VERSION
        .split('.')
        .map(|s| s.parse::<u8>().expect("版本号非法"));
    let maj = it.next().expect("major");
    let min = it.next().expect("minor");
    let mic = it.next().expect("micro");
    debug_assert!(
        maj < 10 && min < 10 && mic < 10,
        "Azureus 前缀编码仅支持个位版本"
    );
    (maj, min, mic)
}

/// 当前用户主目录（跨平台）：
/// Unix/macOS 读 `HOME`；Windows 读 `USERPROFILE`（部分环境也有
/// `HOME`，作为回退）。两者皆无返回 None（调用方决定退路）。
/// 数据目录（会话/控制文件）的定位必须经此函数，直接读 `HOME`
/// 会在 Windows 上失效。
pub fn home_dir() -> Option<std::path::PathBuf> {
    let var = if cfg!(windows) {
        std::env::var_os("USERPROFILE").or_else(|| std::env::var_os("HOME"))
    } else {
        std::env::var_os("HOME")
    };
    var.map(std::path::PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn gid_format_and_uniqueness() {
        let mut seen = HashSet::new();
        for _ in 0..1000 {
            let g = Gid::generate();
            assert_eq!(g.0.len(), 16);
            assert!(g.0.bytes().all(|b| b.is_ascii_hexdigit()));
            assert!(g
                .0
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
            assert!(seen.insert(g.0));
        }
    }

    #[test]
    fn gid_parse() {
        assert!(Gid::parse("0123456789abcdef").is_some());
        assert!(Gid::parse("0123456789ABCDEF").is_some());
        assert!(Gid::parse("0123456789abcde").is_none());
        assert!(Gid::parse("0123456789abcdeg").is_none());
        assert!(Gid::parse("").is_none());
    }

    #[test]
    fn infohash_roundtrip() {
        let ih = InfoHash::from_hex("0123456789abcdef0123456789abcdef01234567").unwrap();
        assert_eq!(ih.to_hex(), "0123456789abcdef0123456789abcdef01234567");
        assert!(InfoHash::from_hex("zz").is_none());
        assert!(InfoHash::from_hex(&"0".repeat(39)).is_none());
    }

    #[test]
    fn peer_id_prefix_matches_engine_version() {
        // 期望值从 ENGINE_VERSION 派生，版本升级时无需改测试
        let (maj, min, mic) = version_tuple();
        let pid = PeerId::azureus_prefix(&[0x41; 12]);
        let exp = format!("-XR{maj}{min}{mic}0-");
        assert_eq!(&pid.0[..8], exp.as_bytes());
        assert_eq!(pid.parse_azureus(), Some((maj, min, mic)));
        // 非本客户端风格
        let mut other = [0u8; 20];
        other[..8].copy_from_slice(b"-UT3600-");
        assert_eq!(PeerId(other).parse_azureus(), None);
    }
}
