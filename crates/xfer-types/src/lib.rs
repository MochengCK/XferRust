//! 公共基础类型：InfoHash、PeerId、引擎版本常量。
//!
//! 依赖方向的根：所有 crate 都可以依赖本 crate，本 crate 不依赖任何兄弟 crate。

pub mod text;

/// 引擎版本。与 XferRust 的 Cargo.toml version 保持同步，
/// RPC 版本查询与 UA/peer-id 前缀派生均以此为唯一源。
pub const ENGINE_VERSION: &str = "0.3.3";

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
        Self(azureus_prefix_from(maj, min, mic, random))
    }

    /// 解析 Azureus 风格前缀出 (major, minor, micro)，非该风格返回 None。
    pub fn parse_azureus(&self) -> Option<(u8, u8, u8)> {
        let p = &self.0;
        if p[0] != b'-' || p[1] != b'X' || p[2] != b'R' || p[7] != b'-' {
            return None;
        }
        Some((
            version_digit_value(p[3])?,
            version_digit_value(p[4])?,
            version_digit_value(p[5])?,
        ))
    }
}

/// 构造 Azureus 风格 peer-id：`-XR####-`（**定长 8 字节**）+ 12 字节随机段。
///
/// 定长构造是刻意的：此前用 `format!("-XR{maj}{min}{mic}0-")` 拼字符串，
/// 版本号一旦出现两位数（如 0.10.0）就拼出 9 字节前缀，`out[p.len()..]`
/// 只剩 11 字节而随机段是 12 字节 → `copy_from_slice` 长度不匹配 panic；
/// 而 `debug_assert` 仅在 debug 生效，release 里没有护栏，升到 0.10.x
/// 就是每个任务第一次生成 peer-id 时直接崩。
fn azureus_prefix_from(maj: u8, min: u8, mic: u8, random: &[u8; 12]) -> [u8; 20] {
    let mut out = [0u8; 20];
    // BEP 20 的 4 个版本字符位：major / minor / micro / 固定 0
    out[..8].copy_from_slice(&[
        b'-',
        b'X',
        b'R',
        version_digit(maj),
        version_digit(min),
        version_digit(mic),
        b'0',
        b'-',
    ]);
    out[8..].copy_from_slice(random);
    out
}

/// 版本号单个字符位的编码：0-9 → `'0'`-`'9'`，10-35 → `'A'`-`'Z'`。
///
/// 前缀只有 4 个字符位，两位数版本塞不进单个十进制字符，改用字母承载
///（客户端对 Azureus 前缀本来就是按字符解析的）。超出 35 的分量饱和到
/// `'Z'`——宁可少报版本号，也不能 panic。
///
/// 注意：BEP 20 的原文仍写「4 位各一个十进制数字」，因此 `xfer-bt` 里
/// 用于**展示他人** peer 客户端版本的 `azureus_version()` 只认数字
/// （保持对第三方 id 的既有解读不变）。含义是：版本号真的出现两位数时，
/// 我们的 peer 在对方界面里会显示成无版本（纯装饰性差异，不影响连接）。
fn version_digit(v: u8) -> u8 {
    match v {
        0..=9 => b'0' + v,
        10..=35 => b'A' + (v - 10),
        _ => b'Z',
    }
}

/// [`version_digit`] 的逆运算（解析回版本号用）。
fn version_digit_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'A'..=b'Z' => Some(b - b'A' + 10),
        _ => None,
    }
}

impl std::fmt::Debug for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PeerId({})", String::from_utf8_lossy(&self.0))
    }
}

/// 解析 [`ENGINE_VERSION`] 为 (major, minor, micro)。
/// 各分量按 [`version_digit`] 编进 `-XR####-`（0-35 可完整往返）。
fn version_tuple() -> (u8, u8, u8) {
    let mut it = ENGINE_VERSION
        .split('.')
        .map(|s| s.parse::<u8>().expect("版本号非法"));
    let maj = it.next().expect("major");
    let min = it.next().expect("minor");
    let mic = it.next().expect("micro");
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

    /// `ENGINE_VERSION` 是手写常量，必须与 Cargo 工作区版本严格一致。
    ///
    /// 两者曾各自漂移：`engine.getVersion`、TUI 标题、peer-id 前缀
    /// （`-XR####-`）全部读 `ENGINE_VERSION`，而 tracker 的 HTTP UA
    /// （`xfer-bt` 里的 `env!("CARGO_PKG_VERSION")` 拼成 `XferRust/<ver>`）
    /// 读 Cargo 版本。一旦不一致，引擎会对外宣称两个不同版本号
    /// （peer-id 说 0.3.0、UA 说 0.3.1），且发布说明与实际产物对不上。
    #[test]
    fn engine_version_matches_cargo_version() {
        assert_eq!(
            ENGINE_VERSION,
            env!("CARGO_PKG_VERSION"),
            "xfer-types::ENGINE_VERSION 与工作区 Cargo.toml 的 version 不一致"
        );
        // 版本段必须落在单个字符位能无损承载的范围内（见 version_digit）
        let (maj, min, mic) = version_tuple();
        assert!(
            maj <= 35 && min <= 35 && mic <= 35,
            "版本分量超过 35 后 peer-id 前缀只能饱和编码（版本号丢失）"
        );
        // 且 ENGINE_VERSION 里不得夹带预发布后缀（-XR 前缀与 UA 都要裸版本）
        assert!(
            !ENGINE_VERSION.contains('-') && !ENGINE_VERSION.contains('+'),
            "ENGINE_VERSION 只允许 major.minor.patch"
        );
    }

    /// 两位数版本必须能编码（前缀恰好 8 字节、随机段完整保留），不 panic。
    ///
    /// 回归点：此前用 `format!("-XR{maj}{min}{mic}0-")` 拼字符串，0.10.0 会
    /// 拼出 9 字节前缀 → `out[p.len()..]` 只剩 11 字节而随机段 12 字节 →
    /// `copy_from_slice` panic（release 里 `debug_assert` 不生效）。
    #[test]
    fn azureus_prefix_encodes_two_digit_versions() {
        let random = [0x5Au8; 12];

        // 0.10.0 → 前缀第 5 位用字母 'A'（10），仍是 `-XR####-` 8 字节
        let pid = azureus_prefix_from(0, 10, 0, &random);
        assert_eq!(&pid[..8], b"-XR0A00-");
        assert_eq!(&pid[8..], &random, "随机段必须完整保留（12 字节）");
        assert_eq!(PeerId(pid).parse_azureus(), Some((0, 10, 0)));

        // 两位数的多个分量：12.34.5 → 'C' 'Y' '5'
        let pid = azureus_prefix_from(12, 34, 5, &random);
        assert_eq!(&pid[..8], b"-XRCY50-");
        assert_eq!(PeerId(pid).parse_azureus(), Some((12, 34, 5)));

        // 超出单字符位承载范围：饱和到 'Z'，绝不 panic
        let pid = azureus_prefix_from(99, 99, 99, &random);
        assert_eq!(&pid[..8], b"-XRZZZ0-");
        assert_eq!(&pid[8..], &random);

        // 个位版本与旧编码逐字节一致（不改变现有对外标识）
        let pid = azureus_prefix_from(0, 3, 1, &random);
        assert_eq!(&pid[..8], b"-XR0310-");
        assert_eq!(PeerId(pid).parse_azureus(), Some((0, 3, 1)));
    }
}
