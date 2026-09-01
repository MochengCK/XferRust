//! 文件存储：顺序写入与整文件哈希校验，以及 BT piece 级存储。
//!
//! M1 范围：HTTP(S) 单连接下载的目标文件落盘（支持从已有部分文件
//! 续写）、完成后的 checksum 校验（sha-1 / sha-256 / sha-512 / md5）。
//! M2 范围：BT piece 位图 / 多文件布局 / 片校验与乱序落盘。

mod piece;

use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use sha1::Sha1;
use sha2::{Digest, Sha256, Sha512};

pub use piece::{verify_piece, FileLayout, PieceLayout, PieceMap, PieceStore};

/// 已存在文件的字节数；文件不存在返回 0。
pub fn existing_len(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// 控制文件存放目录：环境变量 `XFER_CTRL_DIR` 优先，
/// 默认用户主目录下 `.xfer/ctrl`（跨平台：Windows 读
/// `USERPROFILE`；主目录不可得退当前目录）。
pub fn ctrl_dir() -> PathBuf {
    if let Some(d) = std::env::var_os("XFER_CTRL_DIR") {
        if !d.is_empty() {
            return PathBuf::from(d);
        }
    }
    xfer_types::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".xfer")
        .join("ctrl")
}

/// 控制文件路径：`<ctrl_dir>/<目标文件路径 SHA-256 前 24 hex>.xfer`。
/// 以目标路径字符串为键——同一目标续传命中，不同目标互不冲突。
/// 注意不能用 canonicalize：macOS 上 /tmp 是符号链接，目标文件创建
/// 前后 canonicalize 结果不同会破坏哈希稳定性（续传失效）。
/// 引擎侧保证传入绝对路径。
///
/// HTTP 分片控制文件与 BT 续传控制文件共用此键派生：
/// 任务管理器按同一路径即可清理两类控制文件。
pub fn ctrl_path(path: &Path) -> PathBuf {
    let mut h = Sha256::new();
    h.update(path.to_string_lossy().as_bytes());
    let dig = hex::encode(h.finalize());
    ctrl_dir().join(format!("{}.xfer", &dig[..24]))
}

/// 顺序写入的目标文件句柄。
///
/// 打开时若指定 `append` 则从文件末尾续写（断点续传），
/// 否则截断重建。写入由调用方串行驱动（单连接场景），
/// 每次写完即落盘语义由 [`FileSink::flush`] 显式控制。
///
/// 内部用 512KB `BufWriter` 包裹：整个传输期复用同一块缓冲，
/// 合并小块写入（网络块常为 8KB~64KB，突发时更小），
/// 显著减少 `write` 系统调用次数；仅在 `flush` 时刷到底层
/// 文件 + `sync_all`。
pub struct FileSink {
    writer: BufWriter<std::fs::File>,
    path: PathBuf,
    pos: u64,
}

/// 写缓冲容量：足够吸收常见网络块突发，又不至于拖慢落盘可见性。
const FILE_SINK_BUF: usize = 512 * 1024;

impl FileSink {
    /// 打开（或截断创建）目标文件；父目录不存在时自动创建。
    pub fn create(path: &Path) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .read(true)
            .open(path)?;
        Ok(Self {
            writer: BufWriter::with_capacity(FILE_SINK_BUF, file),
            path: path.to_path_buf(),
            pos: 0,
        })
    }

    /// 以续写模式打开：定位到 `offset`（必须等于现有文件长度）。
    pub fn append_at(path: &Path, offset: u64) -> io::Result<Self> {
        use std::io::Seek;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .read(true)
            .open(path)?;
        let mut writer = BufWriter::with_capacity(FILE_SINK_BUF, file);
        writer.seek(io::SeekFrom::Start(offset))?;
        Ok(Self {
            writer,
            path: path.to_path_buf(),
            pos: offset,
        })
    }

    /// 追加一段数据（顺序写，不移动游标）。
    pub fn write(&mut self, buf: &[u8]) -> io::Result<u64> {
        self.writer.write_all(buf)?;
        self.pos += buf.len() as u64;
        Ok(self.pos)
    }

    /// 当前写入位置（= 已完成字节数）。
    pub fn position(&self) -> u64 {
        self.pos
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 刷盘（数据 + 元数据，确保断电后长度可见）。
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        // 获取底层 File 引用做 sync_all（BufWriter 不提供 sync）
        self.writer.get_ref().sync_all()
    }
}

/// 校验算法（线上 checksum 选项的哈希类型）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashAlgo {
    Sha1,
    Sha256,
    Sha512,
    Md5,
}

impl HashAlgo {
    /// 解析线上协议的 hash 类型字符串（大小写不敏感）。
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "sha-1" | "sha1" => Some(Self::Sha1),
            "sha-256" | "sha256" => Some(Self::Sha256),
            "sha-512" | "sha512" => Some(Self::Sha512),
            "md5" => Some(Self::Md5),
            _ => None,
        }
    }
}

/// 流式计算文件哈希并比对期望值（hex，大小写不敏感）。
///
/// 读取错误必须向上传播：`while let Ok` 会把中途读错误当 EOF，
/// 对截断前缀算哈希——校验通过与否都不可信。
pub fn verify_file_hash(path: &Path, algo: HashAlgo, expected_hex: &str) -> Result<(), String> {
    let expected = hex::decode(expected_hex.trim())
        .map_err(|_| format!("期望哈希值不是合法 hex: {expected_hex}"))?;
    let mut file = std::fs::File::open(path).map_err(|e| format!("打开文件失败: {e}"))?;
    // 256KB 读块：一次分配整个校验过程复用，大文件读系统调用更少
    let mut buf = vec![0u8; 256 * 1024];
    // 逐块喂入哈希器；读错误返回 Err（不得静默截断）。
    fn feed<H: Digest>(
        h: &mut H,
        file: &mut std::fs::File,
        buf: &mut [u8],
    ) -> std::io::Result<u64> {
        use std::io::Read;
        let mut n_total = 0u64;
        loop {
            let n = file.read(buf)?;
            if n == 0 {
                return Ok(n_total);
            }
            n_total += n as u64;
            h.update(&buf[..n]);
        }
    }
    let (actual, total): (Vec<u8>, u64) = match algo {
        HashAlgo::Sha1 => {
            let mut h = Sha1::new();
            let t = feed(&mut h, &mut file, &mut buf).map_err(|e| format!("读取文件失败: {e}"))?;
            (h.finalize().to_vec(), t)
        }
        HashAlgo::Sha256 => {
            let mut h = Sha256::new();
            let t = feed(&mut h, &mut file, &mut buf).map_err(|e| format!("读取文件失败: {e}"))?;
            (h.finalize().to_vec(), t)
        }
        HashAlgo::Sha512 => {
            let mut h = Sha512::new();
            let t = feed(&mut h, &mut file, &mut buf).map_err(|e| format!("读取文件失败: {e}"))?;
            (h.finalize().to_vec(), t)
        }
        HashAlgo::Md5 => {
            use md5::{Digest as _, Md5};
            let mut h = Md5::new();
            let t = feed(&mut h, &mut file, &mut buf).map_err(|e| format!("读取文件失败: {e}"))?;
            (h.finalize().to_vec(), t)
        }
    };
    if total == 0 {
        return Err("文件为空或读取失败".into());
    }
    if actual.len() != expected.len() || !constant_time_eq(&actual, &expected) {
        return Err(format!(
            "哈希校验失败: 期望 {}, 实际 {}",
            hex::encode(&expected),
            hex::encode(&actual)
        ));
    }
    Ok(())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_and_resume() {
        let dir = std::env::temp_dir().join(format!("xfer-storage-test-{}", std::process::id()));
        let path = dir.join("a.bin");
        let _ = std::fs::remove_file(&path);

        let mut sink = FileSink::create(&path).unwrap();
        sink.write(b"hello ").unwrap();
        sink.flush().unwrap();
        drop(sink);

        assert_eq!(existing_len(&path), 6);

        let mut sink = FileSink::append_at(&path, 6).unwrap();
        assert_eq!(sink.position(), 6);
        sink.write(b"world").unwrap();
        sink.flush().unwrap();
        drop(sink);

        let content = std::fs::read(&path).unwrap();
        assert_eq!(content, b"hello world");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn hash_verify_all_algos() {
        let path = std::env::temp_dir().join(format!("xfer-hash-test-{}.bin", std::process::id()));
        std::fs::write(&path, b"abcdef").unwrap();

        // md5("abcdef")
        assert!(verify_file_hash(&path, HashAlgo::Md5, "e80b5017098950fc58aad83c8c14978e").is_ok());
        assert!(verify_file_hash(&path, HashAlgo::Md5, "E80B5017098950FC58AAD83C8C14978E").is_ok());
        assert!(
            verify_file_hash(&path, HashAlgo::Md5, "00000000000000000000000000000000").is_err()
        );
        // sha-1("abcdef")
        assert!(verify_file_hash(
            &path,
            HashAlgo::Sha1,
            "1f8ac10f23c5b5bc1167bda84b833e5c057a77d2"
        )
        .is_ok());
        // sha-256("abcdef")
        assert!(verify_file_hash(
            &path,
            HashAlgo::Sha256,
            "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721"
        )
        .is_ok());
        // sha-512("abcdef")
        assert!(
            verify_file_hash(
                &path,
                HashAlgo::Sha512,
                "e32ef19623e8ed9d267f657a81944b3d07adbb768518068e88435745564e8d4150a0a703be2a7d88b61e3d390c2bb97e2d4c311fdc69d6b1267f05f59aa920e7"
            )
            .is_ok()
        );
        assert!(verify_file_hash(&path, HashAlgo::Md5, "not-hex!").is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn algo_parse() {
        assert_eq!(HashAlgo::parse("sha-1"), Some(HashAlgo::Sha1));
        assert_eq!(HashAlgo::parse("SHA-256"), Some(HashAlgo::Sha256));
        assert_eq!(HashAlgo::parse("md5"), Some(HashAlgo::Md5));
        assert_eq!(HashAlgo::parse("crc32"), None);
    }
}
