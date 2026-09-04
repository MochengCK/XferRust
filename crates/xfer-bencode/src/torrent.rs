//! .torrent 文件解析（BEP 3）。
//!
//! 关键正确性点：info_hash 必须对 **info 字典的原始编码字节** 计算 SHA-1
//! （不能对解析后的值重新编码——非规范编码的种子会哈希错位导致全网不识别）。

use sha1::{Digest, Sha1};

use crate::{Parser, Value};

/// 解析后的 .torrent 元信息。
#[derive(Debug, Clone)]
pub struct TorrentMeta {
    pub info: Info,
    /// info 字典原始字节的 SHA-1（20 字节）。
    pub info_hash: [u8; 20],
    pub announce: Option<String>,
    /// announce-list（BEP 12），tier 顺序保留。
    pub announce_list: Vec<Vec<String>>,
    pub comment: Option<String>,
    pub created_by: Option<String>,
    pub creation_date: Option<i64>,
    /// 原始 bencode 字节（会话持久化用：重启后直接重新 parse 恢复 bt_meta）。
    pub raw_bencode: Option<Vec<u8>>,
    /// info 字典的原始编码字节。
    ///
    /// ut_metadata（BEP 9）服务端用：对端请求元数据时必须返回 info 字典的
    /// **原始字节**（重新编码可能哈希错位），因此解析时保留原文。
    pub raw_info: Option<Vec<u8>>,
}

/// info 字典解析结果。
#[derive(Debug, Clone)]
pub struct Info {
    /// 顶层目录名（多文件）/ 单文件名。
    pub name: String,
    /// 每片字节数（最后一片可能不足）。
    pub piece_length: u64,
    /// 各片 SHA-1（每项 20 字节）。
    pub pieces: Vec<[u8; 20]>,
    /// 文件列表（单文件种子为 1 项）。
    pub files: Vec<FileEntry>,
    /// private 标记（禁止 DHT/PEX）。
    pub private: bool,
}

/// 单个文件条目。
#[derive(Debug, Clone)]
pub struct FileEntry {
    /// 相对路径各部分（多文件时 ≥1 段；单文件时仅 name）。
    pub path: Vec<String>,
    pub length: u64,
}

/// 解析 .torrent 字节。
pub fn parse_torrent(bytes: &[u8]) -> Result<TorrentMeta, String> {
    let mut p = Parser::new(bytes);
    let (top, ranges) = p
        .parse_root()
        .map_err(|e| format!("bencode 解析失败: {e}"))?;
    let dict = top
        .as_dict()
        .ok_or_else(|| "torrent 顶层必须是字典".to_string())?;

    let info_bytes_range = ranges
        .get(b"info".as_slice())
        .ok_or_else(|| "缺少 info 字典".to_string())?;
    let info_bytes = &bytes[info_bytes_range.0..info_bytes_range.1];
    let info_hash: [u8; 20] = {
        let mut h = Sha1::new();
        h.update(info_bytes);
        h.finalize().into()
    };

    let info = parse_info(
        dict.get(b"info".as_slice())
            .ok_or_else(|| "缺少 info 字典".to_string())?,
    )?;

    let announce = dict
        .get(b"announce".as_slice())
        .and_then(Value::as_str)
        .map(String::from);
    let announce_list = dict
        .get(b"announce-list".as_slice())
        .and_then(Value::as_list)
        .map(|tiers| {
            tiers
                .iter()
                .filter_map(|t| {
                    t.as_list().map(|urls| {
                        urls.iter()
                            .filter_map(Value::as_str)
                            .map(String::from)
                            .collect::<Vec<_>>()
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let comment = dict
        .get(b"comment".as_slice())
        .and_then(Value::as_str)
        .map(String::from);
    let created_by = dict
        .get(b"created by".as_slice())
        .and_then(Value::as_str)
        .map(String::from);
    let creation_date = dict
        .get(b"creation date".as_slice())
        .and_then(Value::as_int);

    Ok(TorrentMeta {
        info,
        info_hash,
        announce,
        announce_list,
        comment,
        created_by,
        creation_date,
        raw_bencode: Some(bytes.to_vec()),
        raw_info: Some(info_bytes.to_vec()),
    })
}

/// 从 info 字典的原始编码字节构造 [`TorrentMeta`]（ut_metadata / 磁力链接用）。
///
/// 磁力链接只有 info_hash，元数据经 BEP 9 ut_metadata 交换获得——对端返回的
/// 是 **info 字典**（非完整 .torrent），此处直接解析并对其原始字节计算 info_hash。
pub fn parse_info_bytes(bytes: &[u8]) -> Result<TorrentMeta, String> {
    let (top, consumed) = crate::decode_prefix(bytes).map_err(|e| format!("info 解析失败: {e}"))?;
    // 必须是完整字典（无尾随字节）
    if consumed != bytes.len() {
        return Err("info 字典后存在尾随数据".into());
    }
    let info_hash: [u8; 20] = {
        let mut h = Sha1::new();
        h.update(bytes);
        h.finalize().into()
    };
    let info = parse_info(&top)?;
    Ok(TorrentMeta {
        info,
        info_hash,
        announce: None,
        announce_list: Vec::new(),
        comment: None,
        created_by: None,
        creation_date: None,
        raw_bencode: None,
        raw_info: Some(bytes.to_vec()),
    })
}

/// 清洗种子内单个路径段 —— 防路径穿越写任意文件。
///
/// 恶意 .torrent / 磁力元数据可用 `name="../x"`、path 段 `"../.."` 把文件写到
/// 下载目录外（`PieceStore` 用 `root.join(name)` / `base.join(path…)` 拼路径）。
/// 采用 qBittorrent/aria2 的**替换**语义而非整体拒绝：个别段奇怪的合法种子
/// 仍可下载，危险字节被中和——
/// - `/` 与 `\`（路径分隔符，段内出现时中和为字面字符，无法再构造层级）；
/// - `:`（Windows 盘符 "C:" 与 NTFS 备用数据流）；
/// - 整段为空 / `.` / `..` / 含 NUL → 整段替换为 `_`（穿越只对整段成立）。
fn sanitize_segment(seg: &str) -> String {
    if seg.contains('\0') {
        return "_".to_string();
    }
    let cleaned: String = seg
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' => '_',
            _ => c,
        })
        .collect();
    if cleaned.is_empty() || cleaned == "." || cleaned == ".." {
        "_".to_string()
    } else {
        cleaned
    }
}

fn parse_info(v: &Value) -> Result<Info, String> {
    let dict = v.as_dict().ok_or_else(|| "info 必须是字典".to_string())?;
    let name = sanitize_segment(
        dict.get(b"name.utf-8".as_slice())
            .or_else(|| dict.get(b"name".as_slice()))
            .and_then(Value::as_str)
            .ok_or_else(|| "info 缺少 name".to_string())?,
    );
    let piece_length = dict
        .get(b"piece length".as_slice())
        .and_then(Value::as_int)
        .filter(|&n| n > 0)
        .ok_or_else(|| "info 缺少合法的 piece length".to_string())? as u64;
    let pieces_raw = dict
        .get(b"pieces".as_slice())
        .and_then(Value::as_bytes)
        .ok_or_else(|| "info 缺少 pieces".to_string())?;
    if pieces_raw.len() % 20 != 0 {
        return Err(format!("pieces 长度 {} 不是 20 的倍数", pieces_raw.len()));
    }
    let pieces: Vec<[u8; 20]> = pieces_raw
        .chunks_exact(20)
        .map(|c| {
            let mut a = [0u8; 20];
            a.copy_from_slice(c);
            a
        })
        .collect();
    let private = dict
        .get(b"private".as_slice())
        .and_then(Value::as_int)
        .map(|n| n != 0)
        .unwrap_or(false);

    // 单文件模式：length + name
    let files = if let Some(len) = dict.get(b"length".as_slice()).and_then(Value::as_int) {
        if len < 0 {
            return Err("文件长度不能为负".into());
        }
        vec![FileEntry {
            path: vec![name.clone()],
            length: len as u64,
        }]
    } else {
        let list = dict
            .get(b"files".as_slice())
            .and_then(Value::as_list)
            .ok_or_else(|| "info 既无 length 也无 files".to_string())?;
        let mut out = Vec::with_capacity(list.len());
        for f in list {
            let fd = f
                .as_dict()
                .ok_or_else(|| "files 条目必须是字典".to_string())?;
            let length =
                fd.get(b"length".as_slice())
                    .and_then(Value::as_int)
                    .filter(|&n| n >= 0)
                    .ok_or_else(|| "文件条目缺少合法 length".to_string())? as u64;
            let path_raw: Vec<u8> = fd
                .get(b"path.utf-8".as_slice())
                .or_else(|| fd.get(b"path".as_slice()))
                .and_then(Value::as_list)
                .map(|parts| {
                    parts
                        .iter()
                        .filter_map(Value::as_bytes)
                        .flat_map(|b| {
                            let mut v = b.to_vec();
                            v.push(0); // 分隔占位，下面按 0 分组
                            v
                        })
                        .collect()
                })
                .ok_or_else(|| "文件条目缺少 path".to_string())?;
            let parts: Vec<String> = path_raw
                .split(|&b| b == 0)
                .filter(|s| !s.is_empty())
                .map(|s| sanitize_segment(&String::from_utf8_lossy(s)))
                .collect();
            if parts.is_empty() {
                return Err("文件条目 path 为空".into());
            }
            out.push(FileEntry {
                path: parts,
                length,
            });
        }
        out
    };

    Ok(Info {
        name,
        piece_length,
        pieces,
        files,
        private,
    })
}

impl Info {
    /// 文件总长度。
    pub fn total_length(&self) -> u64 {
        self.files.iter().map(|f| f.length).sum()
    }

    /// 片数（按总长与片长推导；对 pieces 数组不一致的种子取较小者防御）。
    pub fn piece_count(&self) -> u32 {
        let total = self.total_length();
        let by_len = if self.piece_length == 0 {
            0
        } else {
            total.div_ceil(self.piece_length)
        };
        by_len.min(self.pieces.len() as u64) as u32
    }

    /// 第 index 片的实际长度（最后一片可能不足 piece_length）。
    pub fn piece_len(&self, index: u32) -> u64 {
        let total = self.total_length();
        let start = index as u64 * self.piece_length;
        if start >= total {
            return 0;
        }
        (total - start).min(self.piece_length)
    }

    /// 第 index 片在文件布局中的分片段落：
    /// (文件索引, 该文件内偏移, 长度)，按文件顺序排列（跨文件片被切分）。
    pub fn piece_segments(&self, index: u32) -> Vec<(usize, u64, u64)> {
        let piece_start = index as u64 * self.piece_length;
        let piece_end = (piece_start + self.piece_length).min(self.total_length());
        let mut segs = Vec::new();
        let mut cursor = 0u64; // 当前文件全局起始偏移
        for (fi, f) in self.files.iter().enumerate() {
            let f_start = cursor;
            let f_end = cursor + f.length;
            cursor = f_end;
            if piece_end <= f_start || piece_start >= f_end {
                continue;
            }
            let seg_start = piece_start.max(f_start) - f_start;
            let seg_end = piece_end.min(f_end) - f_start;
            segs.push((fi, seg_start, seg_end - seg_start));
        }
        segs
    }

    /// 全局字节偏移 → (文件索引, 文件内偏移)。
    pub fn locate(&self, global_offset: u64) -> (usize, u64) {
        let mut cursor = 0u64;
        for (fi, f) in self.files.iter().enumerate() {
            if global_offset < cursor + f.length {
                return (fi, global_offset - cursor);
            }
            cursor += f.length;
        }
        (self.files.len() - 1, 0)
    }

    /// 各文件在文件布局中的全局起始偏移。
    pub fn file_offsets(&self) -> Vec<u64> {
        let mut out = Vec::with_capacity(self.files.len());
        let mut cursor = 0u64;
        for f in &self.files {
            out.push(cursor);
            cursor += f.length;
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::{bytes, dict, encode, int, list};

    fn single_file_torrent_bytes() -> Vec<u8> {
        let info = dict(BTreeMap::from([
            (b"name".to_vec(), bytes("file.txt")),
            (b"piece length".to_vec(), int(16)),
            (b"length".to_vec(), int(40)),
            (
                b"pieces".to_vec(),
                bytes(vec![0xAA; 60]), // 3 片 × 20 字节
            ),
        ]));
        let top = dict(BTreeMap::from([
            (b"announce".to_vec(), bytes("http://t.example/announce")),
            (b"info".to_vec(), info),
        ]));
        encode(&top)
    }

    #[test]
    fn parse_single_file() {
        let meta = parse_torrent(&single_file_torrent_bytes()).unwrap();
        assert_eq!(meta.announce.as_deref(), Some("http://t.example/announce"));
        assert_eq!(meta.info.name, "file.txt");
        assert_eq!(meta.info.piece_length, 16);
        assert_eq!(meta.info.piece_count(), 3);
        assert_eq!(meta.info.total_length(), 40);
        assert_eq!(meta.info.files.len(), 1);
        assert_eq!(meta.info.files[0].path, vec!["file.txt".to_string()]);
        // 最后一片只有 8 字节
        assert_eq!(meta.info.piece_len(2), 8);
        // info_hash 与重新编码的 info 一致
        let re_enc = encode(&dict(BTreeMap::from([
            (b"name".to_vec(), bytes("file.txt")),
            (b"piece length".to_vec(), int(16)),
            (b"length".to_vec(), int(40)),
            (b"pieces".to_vec(), bytes(vec![0xAA; 60])),
        ])));
        let mut h = Sha1::new();
        h.update(&re_enc);
        let expect: [u8; 20] = h.finalize().into();
        assert_eq!(meta.info_hash, expect);
    }

    #[test]
    fn piece_segments_cross_file() {
        let info = Info {
            name: "d".into(),
            piece_length: 10,
            pieces: vec![[0u8; 20]; 4],
            files: vec![
                FileEntry {
                    path: vec!["a".into()],
                    length: 15,
                },
                FileEntry {
                    path: vec!["b".into()],
                    length: 10,
                },
                FileEntry {
                    path: vec!["c".into()],
                    length: 5,
                },
            ],
            private: false,
        };
        // 总长 30，片长 10 → 3 片
        assert_eq!(info.piece_count(), 3);
        // piece 0: [0,10) 全在文件 0
        assert_eq!(info.piece_segments(0), vec![(0, 0, 10)]);
        // piece 1: [10,20) 跨文件 0(尾 5) 和文件 1(头 5)
        assert_eq!(info.piece_segments(1), vec![(0, 10, 5), (1, 0, 5)]);
        // piece 2: [20,30) 跨文件 1(尾 5) 和文件 2(全 5)
        assert_eq!(info.piece_segments(2), vec![(1, 5, 5), (2, 0, 5)]);
        // 定位
        assert_eq!(info.locate(0), (0, 0));
        assert_eq!(info.locate(15), (1, 0));
        assert_eq!(info.locate(25), (2, 0));
        assert_eq!(info.locate(29), (2, 4));
    }

    #[test]
    fn parse_multiple_files() {
        let files = list(vec![
            dict(BTreeMap::from([
                (b"length".to_vec(), int(10)),
                (b"path".to_vec(), list(vec![bytes("dir"), bytes("f1.bin")])),
            ])),
            dict(BTreeMap::from([
                (b"length".to_vec(), int(20)),
                (b"path".to_vec(), list(vec![bytes("f2.bin")])),
            ])),
        ]);
        let info = dict(BTreeMap::from([
            (b"name".to_vec(), bytes("root")),
            (b"piece length".to_vec(), int(16)),
            (b"pieces".to_vec(), bytes(vec![0x11; 20])),
            (b"files".to_vec(), files),
        ]));
        let top = dict(BTreeMap::from([(b"info".to_vec(), info)]));
        let meta = parse_torrent(&encode(&top)).unwrap();
        assert_eq!(meta.info.files.len(), 2);
        assert_eq!(
            meta.info.files[0].path,
            vec!["dir".to_string(), "f1.bin".to_string()]
        );
        assert_eq!(meta.info.total_length(), 30);
    }

    #[test]
    fn rejects_invalid() {
        // 顶层不是字典
        assert!(parse_torrent(b"li1ee").is_err());
        // 缺少 info
        let top = dict(BTreeMap::from([(b"announce".to_vec(), bytes("x"))]));
        assert!(parse_torrent(&encode(&top)).is_err());
        // pieces 长度非法
        let info = dict(BTreeMap::from([
            (b"name".to_vec(), bytes("x")),
            (b"piece length".to_vec(), int(16)),
            (b"length".to_vec(), int(40)),
            (b"pieces".to_vec(), bytes(vec![0xAA; 21])),
        ]));
        let top = dict(BTreeMap::from([(b"info".to_vec(), info)]));
        assert!(parse_torrent(&encode(&top)).is_err());
    }

    // 路径穿越清洗 —— 恶意 name/path 段不得逃出下载目录
    #[test]
    fn sanitizes_path_traversal() {
        // name 带穿越段
        let info = dict(BTreeMap::from([
            (b"name".to_vec(), bytes("../evil")),
            (b"piece length".to_vec(), int(16)),
            (b"length".to_vec(), int(40)),
            (b"pieces".to_vec(), bytes(vec![0xAA; 20])),
        ]));
        let top = dict(BTreeMap::from([(b"info".to_vec(), info)]));
        let meta = parse_torrent(&encode(&top)).unwrap();
        // "/"" 被中和为字面字符："../evil" → ".._evil"（单段，不再含层级）
        assert_eq!(meta.info.name, ".._evil");

        // 整段就是 ".." / "." / 绝对路径
        assert_eq!(super::sanitize_segment(".."), "_");
        assert_eq!(super::sanitize_segment("."), "_");
        assert_eq!(super::sanitize_segment("/etc/passwd"), "_etc_passwd");
        // ':' 与 '\' 各替换一次
        assert_eq!(super::sanitize_segment("C:\\Windows\\evil"), "C__Windows_evil");
        // 反斜杠 / 冒号（Windows 盘符、备用数据流）
        assert_eq!(super::sanitize_segment("a\\..\\..\\b"), "a_.._.._b");
        assert_eq!(super::sanitize_segment("name:stream"), "name_stream");
        // NUL
        assert_eq!(super::sanitize_segment("a\0b"), "_");
        assert_eq!(super::sanitize_segment(""), "_");
        // 正常名不受影响
        assert_eq!(super::sanitize_segment("Ubuntu 24.04 LTS"), "Ubuntu 24.04 LTS");
        assert_eq!(super::sanitize_segment("a..b"), "a..b");

        // 多文件种子的 path 段同样清洗
        let files = list(vec![dict(BTreeMap::from([
            (b"length".to_vec(), int(10)),
            (
                b"path".to_vec(),
                list(vec![bytes(".."), bytes("sub"), bytes("f.bin")]),
            ),
        ]))]);
        let info = dict(BTreeMap::from([
            (b"name".to_vec(), bytes("root")),
            (b"piece length".to_vec(), int(16)),
            (b"pieces".to_vec(), bytes(vec![0x11; 20])),
            (b"files".to_vec(), files),
        ]));
        let top = dict(BTreeMap::from([(b"info".to_vec(), info)]));
        let meta = parse_torrent(&encode(&top)).unwrap();
        assert_eq!(
            meta.info.files[0].path,
            vec!["_".to_string(), "sub".to_string(), "f.bin".to_string()]
        );
    }
}
