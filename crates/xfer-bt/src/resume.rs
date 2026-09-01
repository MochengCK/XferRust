//! BT 续传控制文件：持久化已校验片的位图。
//!
//! 语义与 HTTP 分片控制文件一致（存于 `ctrl_dir()`，键为数据路径的
//! SHA-256 前 24 hex）：暂停/重启后按位图跳过已完成的片，避免从零下载。
//! 版本化 JSON（PLAN §6：新格式版本化）；写入走临时文件 + rename 原子替换。

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

const VERSION: u32 = 1;
const KIND: &str = "bt-resume";

/// 临时文件名序号：保证并发保存（新旧引擎并存场景）互不覆盖。
static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Serialize, Deserialize)]
struct ResumeDoc {
    v: u32,
    kind: String,
    /// info_hash（hex，40 字符）——防止不同种子同目录串用。
    info_hash: String,
    piece_count: u32,
    /// 已完成片位图（hex；BEP 3 位序：最高位 = 片 0）。
    bitfield: String,
    /// Unix 秒；仅诊断用。
    saved_at: u64,
}

/// 读取续传控制文件；版本/类型/info_hash/片数任一不符或损坏返回 `None`。
pub fn load(ctrl: &Path, info_hash: &[u8; 20], piece_count: u32) -> Option<Vec<u8>> {
    let bytes = std::fs::read(ctrl).ok()?;
    let doc: ResumeDoc = serde_json::from_slice(&bytes).ok()?;
    if doc.v != VERSION || doc.kind != KIND {
        return None;
    }
    if doc.info_hash != hex::encode(info_hash) || doc.piece_count != piece_count {
        return None;
    }
    let bf = hex::decode(doc.bitfield).ok()?;
    if bf.len() != (piece_count as usize).div_ceil(8) {
        return None;
    }
    Some(bf)
}

/// 原子写入续传控制文件（临时文件 + rename）。
pub fn save(ctrl: &Path, info_hash: &[u8; 20], piece_count: u32, bitfield: &[u8]) -> std::io::Result<()> {
    let doc = ResumeDoc {
        v: VERSION,
        kind: KIND.to_string(),
        info_hash: hex::encode(info_hash),
        piece_count,
        bitfield: hex::encode(bitfield),
        saved_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    };
    let json = serde_json::to_vec(&doc).map_err(|e| std::io::Error::other(e.to_string()))?;
    if let Some(parent) = ctrl.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // 临时文件与目标同目录（保证同文件系统，rename 原子）；
    // 名称含 pid + 序号，避免并发保存互相覆盖未 rename 的临时文件
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let base = ctrl
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("ctrl");
    let tmp = ctrl.with_file_name(format!("{base}.xfer.{}.{seq}.tmp", std::process::id()));
    std::fs::write(&tmp, &json)?;
    std::fs::rename(&tmp, ctrl)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_ctrl(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("bt-resume-test-{tag}-{}", std::process::id()))
    }

    #[test]
    fn save_load_roundtrip() {
        let ctrl = tmp_ctrl("roundtrip");
        let ih = [7u8; 20];
        let bf = vec![0b1010_0000, 0b0100_0000];
        save(&ctrl, &ih, 9, &bf).unwrap();
        assert_eq!(load(&ctrl, &ih, 9), Some(bf));
        let _ = std::fs::remove_file(&ctrl);
    }

    #[test]
    fn rejects_wrong_info_hash_or_count() {
        let ctrl = tmp_ctrl("mismatch");
        let ih = [3u8; 20];
        save(&ctrl, &ih, 8, &[0xFF]).unwrap();
        assert_eq!(load(&ctrl, &[4u8; 20], 8), None);
        assert_eq!(load(&ctrl, &ih, 9), None);
        let _ = std::fs::remove_file(&ctrl);
    }

    #[test]
    fn rejects_corrupt_or_missing() {
        let ctrl = tmp_ctrl("corrupt");
        std::fs::write(&ctrl, b"{ not json !!!").unwrap();
        assert_eq!(load(&ctrl, &[1u8; 20], 8), None);
        let _ = std::fs::remove_file(&ctrl);
        assert_eq!(load(&ctrl, &[1u8; 20], 8), None);
    }
}
