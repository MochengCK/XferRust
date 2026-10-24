//! 断点续传控制文件：记录已完成片位图。
//!
//! 与 HTTP 分片控制文件分开命名（`<hash>.xfer.sftp`），两种协议的
//! 控制文件不会互相误读。控制文件只在数据文件已完成 `fsync` 之后写入，
//! 记录的是"拍快照时已经写完的片"，崩溃后最多重下最近一秒的片。

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::SftpError;

/// 片大小：1 MiB。断点续传精度与位图体积的折中（1 TiB → 128 KiB 位图）。
pub(crate) const PIECE_LEN: u64 = 1024 * 1024;
const MAGIC: &str = "xfer-sftp";
const VERSION: u32 = 1;

/// SFTP 控制文件路径（与 HTTP 的 `.xfer` 并存，互不干扰）。
pub fn ctrl_path(target: &Path) -> PathBuf {
    let base = xfer_storage::ctrl_path(target);
    let mut s = base.into_os_string();
    s.push(".sftp");
    PathBuf::from(s)
}

/// 已完成片位图。
#[derive(Debug, Clone)]
pub(crate) struct Bitmap {
    bits: Vec<u8>,
    pieces: usize,
    done: usize,
}

impl Bitmap {
    pub fn new(total: u64) -> Self {
        let pieces = total.div_ceil(PIECE_LEN) as usize;
        Self {
            bits: vec![0; pieces.div_ceil(8)],
            pieces,
            done: 0,
        }
    }

    pub fn from_hex(hex: &str, pieces: usize) -> Option<Self> {
        let bytes = hex::decode(hex).ok()?;
        if bytes.len() != pieces.div_ceil(8) {
            return None;
        }
        let mut b = Self {
            bits: bytes,
            pieces,
            done: 0,
        };
        // 末字节的填充位一律视为未完成
        if !pieces.is_multiple_of(8) {
            let mask = (1u8 << (pieces % 8)) - 1;
            if let Some(last) = b.bits.last_mut() {
                *last &= mask;
            }
        }
        b.done = (0..pieces).filter(|&i| b.is_done(i)).count();
        Some(b)
    }

    pub fn to_hex(&self) -> String {
        hex::encode(&self.bits)
    }

    pub fn pieces(&self) -> usize {
        self.pieces
    }

    pub fn is_done(&self, idx: usize) -> bool {
        idx < self.pieces && (self.bits[idx / 8] & (1 << (idx % 8))) != 0
    }

    /// 标记完成；重复标记返回 false。
    pub fn mark(&mut self, idx: usize) -> bool {
        if idx >= self.pieces {
            return false;
        }
        let byte = &mut self.bits[idx / 8];
        let mask = 1 << (idx % 8);
        if *byte & mask != 0 {
            return false;
        }
        *byte |= mask;
        self.done += 1;
        true
    }

    pub fn done_pieces(&self) -> usize {
        self.done
    }

    pub fn is_complete(&self) -> bool {
        self.done == self.pieces
    }

    /// 已完成的字节数（末片按实际长度计）。
    pub fn done_bytes(&self, total: u64) -> u64 {
        if self.pieces == 0 {
            return 0;
        }
        let tail_len = total - (self.pieces - 1) as u64 * PIECE_LEN;
        (0..self.pieces)
            .filter(|&i| self.is_done(i))
            .map(|i| {
                if i + 1 == self.pieces {
                    tail_len
                } else {
                    PIECE_LEN
                }
            })
            .sum()
    }
}

#[derive(Serialize, Deserialize)]
struct CtrlFile {
    magic: String,
    v: u32,
    total: u64,
    piece: u64,
    remote: String,
    done: String,
}

/// 读取可用的续传状态；总长/片长/远程路径任一不符都视为无效。
pub(crate) fn load(target: &Path, total: u64, remote: &str) -> Option<Bitmap> {
    let path = ctrl_path(target);
    let bytes = std::fs::read(&path).ok()?;
    let cf: CtrlFile = serde_json::from_slice(&bytes).ok()?;
    if cf.magic != MAGIC || cf.v != VERSION || cf.total != total || cf.piece != PIECE_LEN {
        return None;
    }
    if cf.remote != remote {
        return None;
    }
    let pieces = total.div_ceil(PIECE_LEN) as usize;
    Bitmap::from_hex(&cf.done, pieces)
}

/// 可继承的已下载字节数（无有效现场为 0）。
pub fn resume_bytes(target: &Path, total: u64, remote: &str) -> u64 {
    if total == 0 {
        return 0;
    }
    if std::fs::metadata(target).map(|m| m.len()).unwrap_or(0) != total {
        return 0;
    }
    load(target, total, remote)
        .map(|b| b.done_bytes(total))
        .unwrap_or(0)
}

/// 目标文件上是否存在可继续的 SFTP 下载现场。
pub fn can_resume(target: &Path, total: u64, remote: &str) -> bool {
    if total == 0 {
        return false;
    }
    let Ok(meta) = std::fs::metadata(target) else {
        return false;
    };
    // 数据文件按总长预分配；长度不符说明被外部改动/截断，宁可重下。
    if meta.len() != total {
        return false;
    }
    load(target, total, remote).is_some_and(|b| !b.is_complete())
}

/// 原子写入控制文件（tmp + rename），保证崩溃时不会留下半截 JSON。
pub(crate) fn save(
    target: &Path,
    total: u64,
    remote: &str,
    bitmap: &Bitmap,
) -> Result<(), SftpError> {
    let path = ctrl_path(target);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| SftpError::Io(e.to_string()))?;
    }
    let cf = CtrlFile {
        magic: MAGIC.into(),
        v: VERSION,
        total,
        piece: PIECE_LEN,
        remote: remote.to_string(),
        done: bitmap.to_hex(),
    };
    let bytes = serde_json::to_vec(&cf).map_err(|e| SftpError::Io(e.to_string()))?;
    let tmp = path.with_extension("sftp.tmp");
    std::fs::write(&tmp, &bytes).map_err(|e| SftpError::Io(e.to_string()))?;
    std::fs::rename(&tmp, &path).map_err(|e| SftpError::Io(e.to_string()))?;
    Ok(())
}

pub(crate) fn remove(target: &Path) {
    let _ = std::fs::remove_file(ctrl_path(target));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "xfer-sftp-ctrl-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn bitmap_roundtrip_and_tail() {
        let total = PIECE_LEN * 2 + 123;
        let mut b = Bitmap::new(total);
        assert_eq!(b.pieces(), 3);
        assert!(!b.is_done(0));
        assert!(b.mark(0));
        assert!(!b.mark(0));
        assert!(b.mark(2));
        assert_eq!(b.done_bytes(total), PIECE_LEN + 123);
        let hex = b.to_hex();
        let b2 = Bitmap::from_hex(&hex, 3).unwrap();
        assert!(b2.is_done(0));
        assert!(!b2.is_done(1));
        assert!(b2.is_done(2));
        assert_eq!(b2.done_bytes(total), PIECE_LEN + 123);
    }

    #[test]
    fn ctrl_roundtrip_requires_same_remote_and_size() {
        let dir = tmpdir("roundtrip");
        let target = dir.join("file.bin");
        std::fs::write(&target, vec![0u8; (PIECE_LEN * 2) as usize]).unwrap();
        let total = std::fs::metadata(&target).unwrap().len();
        let mut b = Bitmap::new(total);
        b.mark(0);
        save(&target, total, "/remote/file.bin", &b).unwrap();
        assert!(can_resume(&target, total, "/remote/file.bin"));
        assert!(!can_resume(&target, total, "/remote/other.bin"));
        assert!(!can_resume(&target, total + 1, "/remote/file.bin"));
        let loaded = load(&target, total, "/remote/file.bin").unwrap();
        assert!(loaded.is_done(0));
        assert!(!loaded.is_done(1));
        remove(&target);
        assert!(!can_resume(&target, total, "/remote/file.bin"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn rejects_wrong_bitmap_length() {
        assert!(Bitmap::from_hex("00", 9).is_none());
        assert!(Bitmap::from_hex("zz", 1).is_none());
    }
}
