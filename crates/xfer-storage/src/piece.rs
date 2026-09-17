//! BT piece 级存储：多文件布局、位图、片校验与乱序落盘。
//!
//! M2 范围：单种子下载所需的 piece 位图（BEP 3 bitfield 互转）、
//! piece SHA-1 校验、按文件布局随机写（跨文件片切分落盘）。

use std::collections::VecDeque;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use sha1::{Digest, Sha1};

/// piece 位图（BEP 3 bitfield 语义：每片 1 bit，字节内高位在前）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PieceMap {
    count: u32,
    bits: Vec<u8>,
}

impl PieceMap {
    pub fn new(count: u32) -> Self {
        Self {
            count,
            bits: vec![0u8; (count as usize).div_ceil(8)],
        }
    }

    pub fn count(&self) -> u32 {
        self.count
    }

    pub fn set(&mut self, i: u32) {
        if i < self.count {
            let byte = &mut self.bits[(i / 8) as usize];
            *byte |= 0x80 >> (i % 8);
        }
    }

    pub fn is_set(&self, i: u32) -> bool {
        if i >= self.count {
            return false;
        }
        self.bits[(i / 8) as usize] & (0x80 >> (i % 8)) != 0
    }

    /// 清除单片标记（写回缓存中的片落盘前，位图不得声称它已完成）。
    pub fn unset(&mut self, i: u32) {
        if i < self.count {
            let byte = &mut self.bits[(i / 8) as usize];
            *byte &= !(0x80 >> (i % 8));
        }
    }

    pub fn done_count(&self) -> u32 {
        self.bits
            .iter()
            .map(|&b| b.count_ones())
            .sum::<u32>()
            .min(self.count)
    }

    pub fn all_done(&self) -> bool {
        self.done_count() == self.count
    }

    /// 标记所有片为已拥有（BEP 6 HaveAll 用）。
    pub fn set_all(&mut self) {
        for b in &mut self.bits {
            *b = 0xFF;
        }
        // 修正末尾字节的超出位
        let trailing = (self.count as usize) % 8;
        if trailing != 0 {
            if let Some(last) = self.bits.last_mut() {
                *last &= 0xFF << (8 - trailing);
            }
        }
    }

    /// 清除所有片标记（BEP 6 HaveNone 用）。
    pub fn clear(&mut self) {
        for b in &mut self.bits {
            *b = 0;
        }
    }

    /// 未下载的片索引（升序）。
    pub fn missing(&self) -> Vec<u32> {
        (0..self.count).filter(|&i| !self.is_set(i)).collect()
    }

    /// 序列化为 wire bitfield（BEP 3：最高位对应片 0）。
    pub fn to_bitfield(&self) -> Vec<u8> {
        self.bits.clone()
    }

    /// 从对端 bitfield 合并设置（容忍末尾截断的短 bitfield）。
    pub fn set_from_bitfield(&mut self, bf: &[u8]) {
        for (i, &b) in bf.iter().enumerate() {
            if (i as u32) * 8 >= self.count {
                break;
            }
            for bit in 0..8 {
                let idx = (i as u32) * 8 + bit;
                if idx >= self.count {
                    break;
                }
                if b & (0x80 >> bit) != 0 {
                    self.set(idx);
                }
            }
        }
    }
}

/// 文件布局：BT 种子文件集合（含全局偏移）。
#[derive(Debug, Clone)]
pub struct PieceLayout {
    pub piece_length: u64,
    pub files: Vec<FileLayout>,
    /// BEP 3 多文件模式：落盘路径为 `<name>/<path 段拼接>`；
    /// false（单文件种子）落盘路径就是 `<name>`。
    ///
    /// 不能用 `files.len() == 1` 代替：只有 1 个文件的多文件种子同样是
    /// 1 项 `files`，但它的文件必须放在 `<name>/` 目录里面。判据只能来自
    /// 种子结构本身（[`xfer_bencode`] 的 `Info::multi_file`）。
    pub multi_file: bool,
}

#[derive(Debug, Clone)]
pub struct FileLayout {
    /// 相对路径各部分（单文件为 [name]）。
    pub path: Vec<String>,
    pub length: u64,
    /// 全局字节偏移（布局构建时计算）。
    pub offset: u64,
}

impl PieceLayout {
    /// 构建布局：`files` 为 (路径段, 长度) 列表。
    pub fn new(piece_length: u64, files: Vec<(Vec<String>, u64)>) -> Self {
        let mut offset = 0u64;
        let files: Vec<FileLayout> = files
            .into_iter()
            .map(|(path, length)| {
                let f = FileLayout {
                    path,
                    length,
                    offset,
                };
                offset += f.length;
                f
            })
            .collect();
        // 与历史启发式逐字节一致（`single = files.len()==1 &&
        // files[0].path.len()==1`）：只有 1 项**且**路径仅 1 段才当单文件，
        // 否则按多文件。1 项但路径嵌套的种子本来就走 `<name>/<path>`，
        // 不能被改成单文件。真正有歧义的只有「1 项且 1 段」这一种情形
        //（单文件种子 vs 只有 1 个文件的多文件种子），调用方需用
        // [`Self::with_multi_file`] 传种子结构位来消歧。
        let multi_file = !(files.len() == 1 && files[0].path.len() == 1);
        Self {
            piece_length,
            files,
            multi_file,
        }
    }

    /// 声明是否 BEP 3 多文件模式（返回自身，便于链式构造）。
    ///
    /// 调用方应传种子里解析出的结构位（`Info::multi_file`），
    /// 不要再用 `files.len()` 推断。
    pub fn with_multi_file(mut self, multi_file: bool) -> Self {
        self.multi_file = multi_file;
        self
    }

    /// 是否单文件种子布局（落盘路径就是 `<name>`）。
    ///
    /// 判据同时要求「结构位是非多文件」**且**「只有 1 个文件」：结构位与
    /// 文件数互相矛盾时（外部用 [`Self::with_multi_file`] 传错，或字段被
    /// 直接改写）按多文件处理——否则 N 个句柄会全部指向 `root/<name>`
    /// 互相覆盖，写出的文件内容静默错乱。
    pub fn is_single_file(&self) -> bool {
        !self.multi_file && self.files.len() == 1
    }

    pub fn total_length(&self) -> u64 {
        self.files.iter().map(|f| f.length).sum()
    }

    pub fn piece_count(&self) -> u32 {
        let total = self.total_length();
        if self.piece_length == 0 {
            0
        } else {
            total.div_ceil(self.piece_length) as u32
        }
    }

    pub fn piece_len(&self, index: u32) -> u64 {
        let total = self.total_length();
        let start = index as u64 * self.piece_length;
        if start >= total {
            return 0;
        }
        (total - start).min(self.piece_length)
    }

    /// 第 index 片在文件布局中的分片段落：(文件索引, 文件内偏移, 长度)。
    pub fn piece_segments(&self, index: u32) -> Vec<(usize, u64, u64)> {
        let piece_start = index as u64 * self.piece_length;
        let piece_end = (piece_start + self.piece_length).min(self.total_length());
        let mut segs = Vec::new();
        for (fi, f) in self.files.iter().enumerate() {
            let f_start = f.offset;
            let f_end = f.offset + f.length;
            if piece_end <= f_start || piece_start >= f_end {
                continue;
            }
            let seg_start = piece_start.max(f_start) - f_start;
            let seg_end = piece_end.min(f_end) - f_start;
            segs.push((fi, seg_start, seg_end - seg_start));
        }
        segs
    }

    /// 文件选择 → 需要下载的片位图。
    ///
    /// `selected = None` 表示全部文件（返回 None，调用方按全量语义处理）；
    /// `Some(索引)` 时返回仅覆盖所选文件的片位图。跨选/未选文件边界的片
    /// 无法拆分，按需下载（与其余客户端行为一致：边界片整体下载，
    /// 未选文件一侧的字节落盘但文件保持稀疏）。
    pub fn wanted_piece_mask(&self, selected: Option<&[usize]>) -> Option<PieceMap> {
        let sel = selected?;
        let set: std::collections::HashSet<usize> = sel.iter().copied().collect();
        let mut m = PieceMap::new(self.piece_count());
        for idx in 0..self.piece_count() {
            if self.piece_segments(idx).iter().any(|(fi, _, _)| set.contains(fi)) {
                m.set(idx);
            }
        }
        Some(m)
    }

    /// 所选文件的字节总量（None = 全部文件）。
    pub fn selected_length(&self, selected: Option<&[usize]>) -> u64 {
        match selected {
            None => self.total_length(),
            Some(sel) => {
                let set: std::collections::HashSet<usize> = sel.iter().copied().collect();
                self.files
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| set.contains(i))
                    .map(|(_, f)| f.length)
                    .sum()
            }
        }
    }

    /// 每文件已完成字节（与 [`files_done_bytes`] 同源，取值 ≤ 各文件长度）。
    pub fn files_done_bytes(&self, have: impl Fn(u32) -> bool) -> Vec<u64> {
        let bounds: Vec<(u64, u64)> = self.files.iter().map(|f| (f.offset, f.length)).collect();
        files_done_bytes(self.piece_length, self.total_length(), &bounds, have)
    }
}

/// 每文件「已完成字节」：逐片查位图，累加已完成片落在各文件内的段长。
///
/// `file_bounds` 为 (全局起始偏移, 长度) 且按偏移升序（与 [`PieceLayout`]
/// 同源）；`have(i)` 判定第 i 片是否已校验完成。返回与 `file_bounds`
/// 等长的向量，第 i 项 = 文件 i 内已被已完成片覆盖的字节数（≤ 文件长度）。
///
/// 为什么不用「文件长度 × 总进度 / 总长度」估算（曾如此，导致两个口径
/// 问题）：估算与片的实际归属无关，跨文件边界的片会同时被两侧文件计入，
/// 于是未选中的文件也会显示进度、任务级 completedLength 会超过
/// totalLength（进度 >100%）。逐片按段长归属统计则严格守恒：
/// 所有文件之和 = 已完成片覆盖的字节数，选中文件之和 ≤ selected_length。
pub fn files_done_bytes(
    piece_length: u64,
    total_length: u64,
    file_bounds: &[(u64, u64)],
    have: impl Fn(u32) -> bool,
) -> Vec<u64> {
    let mut out = vec![0u64; file_bounds.len()];
    if piece_length == 0 || total_length == 0 || file_bounds.is_empty() {
        return out;
    }
    let count = total_length.div_ceil(piece_length);
    // 片与文件都按偏移升序，用单调游标把复杂度控制在 O(片数 + 文件数)
    // （大种子下 O(片数 × 文件数) 的逐片重扫会拖慢 1Hz 状态上报）。
    let mut fi = 0usize;
    for i in 0..count {
        if !have(i as u32) {
            continue;
        }
        let p_start = i * piece_length;
        let p_end = (p_start + piece_length).min(total_length);
        while fi < file_bounds.len() && file_bounds[fi].0 + file_bounds[fi].1 <= p_start {
            fi += 1;
        }
        let mut j = fi;
        while j < file_bounds.len() && file_bounds[j].0 < p_end {
            let (f_start, f_len) = file_bounds[j];
            let seg_start = p_start.max(f_start);
            let seg_end = p_end.min(f_start + f_len);
            if seg_end > seg_start {
                out[j] += seg_end - seg_start;
            }
            j += 1;
        }
    }
    out
}

/// piece 存储：按需打开文件句柄，支持随机读写。
///
/// `wanted` 语义（[`Self::open`] 的 `wanted` 参数）：
/// - `None` = 全部文件：所有目录与文件立即创建（全量下载）；
/// - `Some(索引)` = 仅创建所选文件的目录与句柄，未选文件不落盘——
///   跨选/未选边界的片在写入时跳过未选文件一侧（校验仍针对完整片
///   数据，完整性不受影响）；
/// - `Some(空)` = 什么都不创建（磁力解析流程等待用户勾选的占位）。
pub struct PieceStore {
    root: PathBuf,
    layout: PieceLayout,
    files: Vec<Option<OpenFile>>,
    map: PieceMap,
    /// 片级写回缓存（FIFO）：数据已校验但尚未落盘的片。
    ///
    /// 磁盘缓存（`disk-cache` 引擎全局选项）的载体：随机写场景下把若干
    /// 片聚合在内存中，减少 seek/小写次数。`cache_limit == 0` 时恒为空
    /// （直写路径，与旧行为完全一致）。
    write_cache: VecDeque<CachedPiece>,
    /// 缓存中片数据的字节总数（= 各条目长度之和）。
    write_cache_bytes: u64,
    /// 缓存上限（字节；0 = 关闭缓存）。
    cache_limit: u64,
}

/// 写回缓存中的一个片。
struct CachedPiece {
    index: u32,
    data: Vec<u8>,
}

struct OpenFile {
    file: std::fs::File,
    path: PathBuf,
}

/// 旧版单文件布局迁移：把 `<root>/<name>` 这个**普通文件**搬进
/// `<root>/<name>/<file>`。
///
/// 背景：旧版用 `files.len() == 1 && path.len() == 1` 判断单文件，于是
/// 「只有 1 个文件的多文件种子」的数据被落成 `<root>/<name>` 这个普通文件。
/// 升级后这类种子要落在 `<root>/<name>/<file>`，若不处理，`create_dir_all`
/// 会因同名文件存在而失败——任务启动即报「打开 piece 存储失败」，已下载的
/// 数据也悬在旧路径上没人管。
///
/// 只在「目标位置已被普通文件占用」时动手：文件数 >1 时该位置不可能是本
/// 任务的数据（旧版多文件种子本来就走目录），保持报错交还给用户处理。
fn migrate_legacy_single_entry_layout(dir: &Path, layout: &PieceLayout) -> std::io::Result<()> {
    let Ok(md) = std::fs::symlink_metadata(dir) else {
        return Ok(()); // 不存在：正常新建目录
    };
    if md.is_dir() {
        return Ok(()); // 已是新布局
    }
    if md.file_type().is_symlink() || layout.files.len() != 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!(
                "{} 已存在且不是目录（不是旧版单文件布局，无法自动迁移）",
                dir.display()
            ),
        ));
    }
    let dst = dir.join(layout.files[0].path.iter().collect::<PathBuf>());
    let tmp = dir.with_extension("xfer-legacy");
    // 先挪到同层临时名腾出位置，建好目录后再放进 <dir>/<file>
    std::fs::rename(dir, &tmp)?;
    match std::fs::create_dir_all(dir).and_then(|_| {
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::rename(&tmp, &dst)
    }) {
        Ok(()) => {
            tracing::warn!(
                from = %dir.display(),
                to = %dst.display(),
                "检测到旧版落盘布局，已迁移为 <目录>/<文件>（保留已下载数据）"
            );
            Ok(())
        }
        Err(e) => {
            // 回滚：尽量把数据放回原位，别让用户找不到数据
            let _ = std::fs::rename(&tmp, dir);
            Err(e)
        }
    }
}

impl PieceStore {
    /// 打开（必要时创建）存储。
    ///
    /// - 单文件种子：`root/name`；
    /// - 多文件种子：`root/name/…`（按 path 分段建目录）。
    ///
    /// `wanted`：`None` 创建全部文件；`Some(索引)` 只创建所选文件的
    /// 目录与句柄（未选文件不落盘）；`Some(空)` 连种子根目录都不建
    /// （磁力等待勾选阶段）。已有文件不截断（支持续传保留已落盘数据）。
    pub fn open(
        root: &Path,
        name: &str,
        layout: PieceLayout,
        wanted: Option<&[usize]>,
    ) -> std::io::Result<Self> {
        let wanted_set: Option<std::collections::HashSet<usize>> =
            wanted.map(|w| w.iter().copied().collect());
        let create_all = wanted_set.is_none();
        let any_wanted = create_all
            || wanted_set
                .as_ref()
                .is_some_and(|s| !s.is_empty());
        // 单文件 / 多文件由布局的结构位决定（不能用 `files.len()==1` 猜：
        // 只有 1 个文件的多文件种子同样是 1 项，但必须落在 `<name>/` 里）。
        let single = layout.is_single_file();
        let base = if single {
            root.join(name)
        } else {
            let d = root.join(name);
            if any_wanted {
                // 旧版把这种种子写成 `<root>/<name>` 这个**普通文件**：直接
                // 建目录会因同名文件存在而失败（任务启动即报错）。先把旧文件
                // 搬进新目录，保住已下载的数据。
                migrate_legacy_single_entry_layout(&d, &layout)?;
                std::fs::create_dir_all(&d)?;
            }
            d
        };
        let mut files = Vec::with_capacity(layout.files.len());
        for (fi, f) in layout.files.iter().enumerate() {
            let path = if single {
                base.clone()
            } else {
                base.join(f.path.iter().collect::<PathBuf>())
            };
            let is_wanted = create_all
                || wanted_set
                    .as_ref()
                    .is_some_and(|s| s.contains(&fi));
            if !is_wanted {
                // 未选文件：不创建目录、不打开句柄（写入/读取按缺席处理）
                files.push(None);
                continue;
            }
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let file = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(&path)?;
            files.push(Some(OpenFile {
                file,
                path: path.clone(),
            }));
        }
        let count = layout.piece_count();
        Ok(Self {
            root: root.to_path_buf(),
            layout,
            files,
            map: PieceMap::new(count),
            write_cache: VecDeque::new(),
            write_cache_bytes: 0,
            cache_limit: 0,
        })
    }

    // ------------------------------------------------------------------
    // 磁盘缓存（写回缓冲）
    // ------------------------------------------------------------------

    /// 设置写回缓存上限（字节；0 = 关闭缓存、直写）。
    ///
    /// 缩小上限时把超出部分按 FIFO 逐出落盘（IO 失败向上传播，不静默
    /// 丢数据）；设为 0 时把缓存全部落盘后关闭缓存。
    pub fn set_write_cache_limit(&mut self, bytes: u64) -> std::io::Result<()> {
        self.cache_limit = bytes;
        self.evict_to_limit()
    }

    /// 缓存上限（字节）。
    pub fn write_cache_limit(&self) -> u64 {
        self.cache_limit
    }

    /// 缓存中尚未落盘的字节数。
    pub fn write_cache_bytes(&self) -> u64 {
        self.write_cache_bytes
    }

    /// 指定片当前是否驻留在缓存中（测试/调试用）。
    pub fn write_cache_contains(&self, index: u32) -> bool {
        self.write_cache.iter().any(|c| c.index == index)
    }

    /// 缓存中该片的数据（未命中返回 None）。
    fn cache_get(&self, index: u32) -> Option<&[u8]> {
        self.write_cache
            .iter()
            .find(|c| c.index == index)
            .map(|c| c.data.as_slice())
    }

    /// 把缓存中的片全部落盘并清空缓存（按入队顺序写出）。
    ///
    /// 某片落盘失败时该片数据放回队首并立即返回错误：调用方（可能是
    /// 忽略错误的 `flush_all`）不会因此丢掉尚未落盘的数据。
    pub fn flush_write_cache(&mut self) -> std::io::Result<()> {
        while let Some(oldest) = self.write_cache.pop_front() {
            self.write_cache_bytes -= oldest.data.len() as u64;
            if let Err(e) = self.write_piece_direct(oldest.index, &oldest.data) {
                // 写盘失败：数据放回队首（维持 FIFO 序），不静默丢数据
                self.write_cache_bytes += oldest.data.len() as u64;
                self.write_cache.push_front(oldest);
                return Err(e);
            }
        }
        Ok(())
    }

    /// 逐出最旧的片直到缓存不超过上限。
    ///
    /// 逐出即落盘：只有真正写入成功才从缓存移除，写失败时把该片放回
    /// 队首并返回错误（错误经 [`Self::write_piece`] 传播给调用方，数据
    /// 仍在缓存中不丢失）。
    fn evict_to_limit(&mut self) -> std::io::Result<()> {
        while self.write_cache_bytes > self.cache_limit {
            let Some(oldest) = self.write_cache.pop_front() else {
                break;
            };
            self.write_cache_bytes -= oldest.data.len() as u64;
            if let Err(e) = self.write_piece_direct(oldest.index, &oldest.data) {
                self.write_cache_bytes += oldest.data.len() as u64;
                self.write_cache.push_front(oldest);
                return Err(e);
            }
        }
        Ok(())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn layout(&self) -> &PieceLayout {
        &self.layout
    }

    pub fn piece_count(&self) -> u32 {
        self.map.count()
    }

    pub fn piece_len(&self, index: u32) -> u64 {
        self.layout.piece_len(index)
    }

    pub fn total_length(&self) -> u64 {
        self.layout.total_length()
    }

    pub fn map(&self) -> &PieceMap {
        &self.map
    }

    /// 强制标记一片为已完成（续传/种子场景）。
    pub fn mark_done(&mut self, index: u32) {
        self.map.set(index);
    }

    /// 强制标记全部片已完成（文件已完整时）。
    pub fn mark_all_done(&mut self) {
        for i in 0..self.map.count() {
            self.map.set(i);
        }
    }

    /// 以持久化的位图恢复已完成片集合（续传控制文件）。
    /// 位图长度与片数不符时忽略（防御损坏文件）。
    pub fn set_bitfield(&mut self, bf: &[u8]) {
        if bf.len() == self.map.count().div_ceil(8) as usize {
            self.map.set_from_bitfield(bf);
        }
    }

    pub fn have_piece(&self, i: u32) -> bool {
        self.map.is_set(i)
    }

    /// 已下载字节数（已校验片的长度之和）。
    pub fn done_bytes(&self) -> u64 {
        let mut n = 0u64;
        for i in 0..self.map.count() {
            if self.map.is_set(i) {
                n += self.layout.piece_len(i);
            }
        }
        n
    }

    /// 校验并通过则落盘（或进入写回缓存）并标记；校验失败返回
    /// Ok(false)（数据已丢弃）。
    pub fn accept_piece(
        &mut self,
        index: u32,
        data: &[u8],
        expected: &[u8; 20],
    ) -> std::io::Result<bool> {
        if !verify_piece(data, expected) {
            return Ok(false);
        }
        self.write_piece(index, data)?;
        self.map.set(index);
        Ok(true)
    }

    /// 写入一片（测试/seed 场景，无条件写入不校验）。
    ///
    /// 磁盘缓存开启（`set_write_cache_limit`）且片长不超过缓存上限时，
    /// 数据先留在写回缓存中（超限按 FIFO 逐出落盘），否则直写磁盘；
    /// 读路径（[`Self::read_piece`] / [`Self::read_block`]）对缓存命中的
    /// 片直接返回缓存内容，因此缓存期间读取语义与已落盘一致。
    ///
    /// 未选文件一侧的分段被跳过（句柄缺席即不落盘、不创建文件）；
    /// 片校验在调用方针对完整片数据完成，跳过不影响完整性判定。
    pub fn write_piece(&mut self, index: u32, data: &[u8]) -> std::io::Result<()> {
        if self.cache_limit == 0 || data.len() as u64 > self.cache_limit {
            return self.write_piece_direct(index, data);
        }
        // 同片重复写入（重下/双路竞态）时替换旧条目，避免重复计数
        if let Some(pos) = self.write_cache.iter().position(|c| c.index == index) {
            if let Some(old) = self.write_cache.remove(pos) {
                self.write_cache_bytes -= old.data.len() as u64;
            }
        }
        self.write_cache.push_back(CachedPiece {
            index,
            data: data.to_vec(),
        });
        self.write_cache_bytes += data.len() as u64;
        self.evict_to_limit()
    }

    /// 绕过缓存直接写磁盘（逐出 / flush / 缓存关闭时使用）。
    fn write_piece_direct(&mut self, index: u32, data: &[u8]) -> std::io::Result<()> {
        let segs = self.layout.piece_segments(index);
        let mut written = 0usize;
        for (fi, off, len) in segs {
            let seg = &data[written..written + len as usize];
            written += len as usize;
            if let Some(f) = self.files[fi].as_mut() {
                f.file.seek(SeekFrom::Start(off))?;
                f.file.write_all(seg)?;
            }
        }
        Ok(())
    }

    /// 读取一片（跨文件拼接）。
    ///
    /// 未选文件一侧的分段按缺席跳过：返回的数据短于片长（仅 seed
    /// 场景可能触及，调用方按短读容忍处理）。缓存命中时按同一规则
    /// 从缓存切片，不访问磁盘。
    pub fn read_piece(&mut self, index: u32) -> std::io::Result<Vec<u8>> {
        if let Some(cached) = self.cache_get(index) {
            let mut out = Vec::new();
            let mut written = 0usize;
            for (fi, _off, len) in self.layout.piece_segments(index) {
                let end = (written + len as usize).min(cached.len());
                let seg = &cached[written.min(cached.len())..end];
                written += len as usize;
                // 未选文件一侧按缺席跳过（与磁盘路径同语义）
                if self.files[fi].is_none() {
                    continue;
                }
                out.extend_from_slice(seg);
            }
            return Ok(out);
        }
        let mut out = Vec::new();
        for (fi, off, len) in self.layout.piece_segments(index) {
            let Some(f) = self.files[fi].as_mut() else {
                continue;
            };
            f.file.seek(SeekFrom::Start(off))?;
            // 直接读进出缓冲，避免每段一次临时缓冲拷贝
            let pos = out.len();
            out.resize(pos + len as usize, 0);
            f.file.read_exact(&mut out[pos..])?;
        }
        Ok(out)
    }

    /// 读取一片中指定偏移和长度的块（seed 模式上传用）。
    ///
    /// `begin` 是片内偏移，`length` 是请求长度。
    /// 自动处理跨文件边界。片驻留写回缓存时从缓存取（不碰磁盘）。
    pub fn read_block(&mut self, index: u32, begin: u32, length: u32) -> std::io::Result<Vec<u8>> {
        let piece_start = index as u64 * self.layout.piece_length;
        let block_start = piece_start + begin as u64;
        let block_end = block_start + length as u64;

        // 缓存命中：按与磁盘路径相同的分段规则从缓存切片
        if let Some(cached) = self.cache_get(index) {
            let mut out = Vec::with_capacity(length as usize);
            for (fi, f_layout) in self.layout.files.iter().enumerate() {
                let f_start = f_layout.offset;
                let f_end = f_layout.offset + f_layout.length;
                if block_end <= f_start || block_start >= f_end {
                    continue;
                }
                // 未选文件一侧按缺席跳过（返回短块，对端容忍截断）
                if self.files[fi].is_none() {
                    continue;
                }
                // 片内偏移 = 段起点的全局偏移 - 片起点
                let rel = (block_start.max(f_start) - piece_start) as usize;
                let seg_len = (block_end.min(f_end) - block_start.max(f_start)) as usize;
                if rel >= cached.len() {
                    continue;
                }
                let end = (rel + seg_len).min(cached.len());
                out.extend_from_slice(&cached[rel..end]);
            }
            return Ok(out);
        }

        let mut out = Vec::with_capacity(length as usize);
        for (fi, f_layout) in self.layout.files.iter().enumerate() {
            let f_start = f_layout.offset;
            let f_end = f_layout.offset + f_layout.length;
            if block_end <= f_start || block_start >= f_end {
                continue;
            }
            let seg_start = block_start.max(f_start) - f_start;
            let seg_end = block_end.min(f_end) - f_start;
            let seg_len = (seg_end - seg_start) as usize;
            // 未选文件一侧按缺席跳过（返回短块，对端容忍截断）
            let Some(file) = self.files[fi].as_mut() else {
                continue;
            };
            let file = &mut file.file;
            file.seek(SeekFrom::Start(seg_start))?;
            // 直接读进出缓冲（上传热路径：每供一块省一次 16KiB 拷贝）
            let pos = out.len();
            out.resize(pos + seg_len, 0);
            file.read_exact(&mut out[pos..])?;
        }
        // 如果请求超出了文件总长，out 可能短于 length（对端会容忍截断）
        Ok(out)
    }

    /// 对端 wire bitfield（本端已下载位）。
    pub fn bitfield(&self) -> Vec<u8> {
        self.map.to_bitfield()
    }

    /// **已落盘**片位图：写回缓存中尚未 flush 的片按「未完成」呈现。
    ///
    /// 续传控制文件必须用它而不是 [`Self::bitfield`]——`bitfield` 反映的是
    /// `mark_done` 的即时结果，而开启 `disk-cache` 后片的字节只在内存
    /// （FIFO 回写缓冲），此时若把位图写进续传文件，进程崩溃/断电后按位图
    /// 恢复就会重现「已完成的空洞」（[`Self::flush_all`] 的文档不变式）。
    /// 代价只是崩溃后重下缓存里那几个片，远小于静默产出损坏文件的代价。
    pub fn durable_bitfield(&self) -> Vec<u8> {
        if self.cache_limit == 0 || self.write_cache.is_empty() {
            return self.bitfield();
        }
        let mut map = self.map.clone();
        for c in &self.write_cache {
            map.unset(c.index);
        }
        map.to_bitfield()
    }

    /// 刷盘全部已打开文件（先把写回缓存落盘，再 flush + sync 各句柄）。
    ///
    /// 顺序是上层不变式的前提：续传位图落盘前必须保证位图对应的片数据
    /// 已在磁盘上，否则崩溃后按位图恢复会重现"已完成"的空洞。
    pub fn flush_all(&mut self) -> std::io::Result<()> {
        self.flush_write_cache()?;
        for f in self.files.iter_mut().flatten() {
            f.file.flush()?;
            f.file.sync_all()?;
        }
        Ok(())
    }

    /// 已打开文件的路径（调试/展示用；未创建的文件不在列）。
    pub fn file_paths(&self) -> Vec<PathBuf> {
        self.files
            .iter()
            .flatten()
            .map(|f| f.path.clone())
            .collect()
    }

    /// 已打开句柄的文件索引（升序；未创建的文件不在列）。
    pub fn opened_indices(&self) -> Vec<usize> {
        self.files
            .iter()
            .enumerate()
            .filter(|(_, f)| f.is_some())
            .map(|(i, _)| i)
            .collect()
    }
}

/// 校验一片数据是否与期望 SHA-1 一致。
pub fn verify_piece(data: &[u8], expected: &[u8; 20]) -> bool {
    let mut h = Sha1::new();
    h.update(data);
    let digest = h.finalize();
    digest.as_slice() == expected
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout3() -> PieceLayout {
        PieceLayout::new(
            10,
            vec![
                (vec!["a.bin".into()], 15),
                (vec!["b.bin".into()], 10),
                (vec!["c.bin".into()], 5),
            ],
        )
    }

    #[test]
    fn layout_segments_and_lengths() {
        let l = layout3();
        assert_eq!(l.total_length(), 30);
        assert_eq!(l.piece_count(), 3);
        assert_eq!(l.piece_len(0), 10);
        assert_eq!(l.piece_len(2), 10);
        assert_eq!(l.piece_segments(0), vec![(0, 0, 10)]);
        assert_eq!(l.piece_segments(1), vec![(0, 10, 5), (1, 0, 5)]);
        assert_eq!(l.piece_segments(2), vec![(1, 5, 5), (2, 0, 5)]);
    }

    #[test]
    fn wanted_mask_full_selection_is_none() {
        let l = layout3();
        // None = 全部文件：无位图，调用方按全量语义处理
        assert!(l.wanted_piece_mask(None).is_none());
        assert_eq!(l.selected_length(None), 30);
    }

    #[test]
    fn wanted_mask_single_file_includes_boundary_pieces() {
        let l = layout3();
        // 只选 b.bin（文件 1）：片 1 跨 a/b、片 2 跨 b/c，均需下载
        let m = l.wanted_piece_mask(Some(&[1])).unwrap();
        assert!(m.is_set(1) && m.is_set(2));
        assert!(!m.is_set(0));
        assert_eq!(l.selected_length(Some(&[1])), 10);
    }

    #[test]
    fn wanted_mask_multiple_files_and_invalid_index() {
        let l = layout3();
        // 选 a + c：片 0 全在 a、片 2 含 c；片 1 只触 a 尾(属于 a，边界片已含)
        let m = l.wanted_piece_mask(Some(&[0, 2])).unwrap();
        assert!(m.is_set(0) && m.is_set(1) && m.is_set(2));
        assert_eq!(l.selected_length(Some(&[0, 2])), 20);
        // 越界索引被忽略
        let m2 = l.wanted_piece_mask(Some(&[7])).unwrap();
        assert_eq!(m2.done_count(), 0);
        assert_eq!(l.selected_length(Some(&[7])), 0);
    }

    #[test]
    fn files_done_bytes_attributes_boundary_pieces_per_file() {
        // 布局：a.bin 15 / b.bin 10 / c.bin 5，片长 10（片 1 跨 a/b，片 2 跨 b/c）
        let l = layout3();
        let all = |_i: u32| true;

        // 全部完成：每文件恰好等于自身长度，总和 = 总长（口径守恒）
        let done = l.files_done_bytes(all);
        assert_eq!(done, vec![15, 10, 5]);
        assert_eq!(done.iter().sum::<u64>(), l.total_length());

        // 只有片 0（0..10 全在 a.bin）：b/c 不得因为"整体有进度"而被估算出字节
        let only0 = |i: u32| i == 0;
        assert_eq!(l.files_done_bytes(only0), vec![10, 0, 0]);

        // 只有片 1（跨 a 尾 5 + b 头 5）：两侧各计自己那 5 字节
        let only1 = |i: u32| i == 1;
        assert_eq!(l.files_done_bytes(only1), vec![5, 5, 0]);

        // 只有片 2（跨 b 尾 5 + c 全 5）
        let only2 = |i: u32| i == 2;
        assert_eq!(l.files_done_bytes(only2), vec![0, 5, 5]);

        // 未完成的片不计（含跨边界片未完成时两侧都是 0）
        let none = |_i: u32| false;
        assert_eq!(l.files_done_bytes(none), vec![0, 0, 0]);
    }

    #[test]
    fn files_done_bytes_never_exceeds_file_length() {
        // 单个 5 字节文件、片长 4：末片只剩 1 字节，全完成时不得按片长虚报
        let l = PieceLayout::new(4, vec![(vec!["only.bin".into()], 5)]);
        let done = l.files_done_bytes(|_| true);
        assert_eq!(done, vec![5]);
    }

    #[test]
    fn files_done_bytes_zero_length_file_stays_zero() {
        let l = PieceLayout::new(
            10,
            vec![
                (vec!["a.bin".into()], 10),
                (vec!["empty.bin".into()], 0),
                (vec!["b.bin".into()], 10),
            ],
        );
        let done = l.files_done_bytes(|_| true);
        assert_eq!(done, vec![10, 0, 10]);
    }

    #[test]
    fn piecemap_bitfield_roundtrip() {
        let mut m = PieceMap::new(10);
        m.set(0);
        m.set(7);
        m.set(9);
        let bf = m.to_bitfield();
        assert_eq!(bf.len(), 2);
        // bit0 → 0x80, bit7 → 0x01, bit9 → 第二个字节 bit1 → 0x40
        assert_eq!(bf[0], 0x81);
        assert_eq!(bf[1], 0x40);

        let mut m2 = PieceMap::new(10);
        m2.set_from_bitfield(&bf);
        assert_eq!(m, m2);
        assert_eq!(m2.done_count(), 3);
        assert!(!m2.all_done());
        assert_eq!(m2.missing(), vec![1, 2, 3, 4, 5, 6, 8]);
    }

    #[test]
    fn piecemap_short_bitfield_tolerated() {
        // 对端截断 bitfield（只发前 8 位所在字节）
        let mut m = PieceMap::new(13);
        m.set_from_bitfield(&[0xFF, 0xF8]); // 字节1 高 5 位 → 片 8..13
        assert_eq!(m.done_count(), 13);
        let mut m2 = PieceMap::new(13);
        m2.set_from_bitfield(&[0xFF]); // 只 8 片
        assert_eq!(m2.done_count(), 8);
        assert_eq!(m2.missing(), (8..13).collect::<Vec<u32>>());
    }

    #[test]
    fn store_write_read_across_files() {
        let dir = std::env::temp_dir().join(format!("xfer-piece-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let mut store = PieceStore::open(&dir, "dl", layout3(), None).unwrap();
        // 片 1 跨 a.bin(尾 5) 与 b.bin(头 5)
        let piece1: Vec<u8> = (0..10).map(|i| i as u8).collect();
        store.write_piece(1, &piece1).unwrap();
        // 片 2 跨 b.bin(尾 5) 与 c.bin(全 5)
        let piece2: Vec<u8> = (100..110).map(|i| i as u8).collect();
        store.write_piece(2, &piece2).unwrap();

        assert_eq!(store.read_piece(1).unwrap(), piece1);
        assert_eq!(store.read_piece(2).unwrap(), piece2);

        // 验证文件内容
        let a = std::fs::read(dir.join("dl").join("a.bin")).unwrap();
        assert_eq!(a.len(), 15);
        assert_eq!(&a[10..], &piece1[..5]);
        let b = std::fs::read(dir.join("dl").join("b.bin")).unwrap();
        assert_eq!(&b[..5], &piece1[5..]);
        assert_eq!(&b[5..], &piece2[..5]);
        let c = std::fs::read(dir.join("dl").join("c.bin")).unwrap();
        assert_eq!(&c[..], &piece2[5..]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn accept_piece_verifies_hash() {
        let dir = std::env::temp_dir().join(format!("xfer-piece2-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut store = PieceStore::open(&dir, "dl", layout3(), None).unwrap();

        let data: Vec<u8> = (0..10).map(|i| i as u8).collect();
        let good = {
            let mut h = Sha1::new();
            h.update(&data);
            let d: [u8; 20] = h.finalize().into();
            d
        };
        assert!(store.accept_piece(0, &data, &good).unwrap());
        assert!(store.have_piece(0));
        assert_eq!(store.done_bytes(), 10);

        // 坏哈希 → 拒绝且不落盘
        let bad = [0u8; 20];
        assert!(!store.accept_piece(1, &data, &bad).unwrap());
        assert!(!store.have_piece(1));
        assert_eq!(store.done_bytes(), 10);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_respects_wanted_files() {
        let dir = std::env::temp_dir().join(format!("xfer-piece-sel-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        // 空选择（等待勾选占位）：什么都不创建
        PieceStore::open(&dir, "dl", layout3(), Some(&[])).unwrap();
        assert!(!dir.join("dl").exists());

        // 只选文件 1（b.bin）：根目录与 b.bin 存在，a/c 不存在
        let mut store = PieceStore::open(&dir, "dl", layout3(), Some(&[1])).unwrap();
        assert!(dir.join("dl").join("b.bin").exists());
        assert!(!dir.join("dl").join("a.bin").exists());
        assert!(!dir.join("dl").join("c.bin").exists());
        assert_eq!(store.file_paths(), vec![dir.join("dl").join("b.bin")]);

        // 边界片 1 跨 a/b：写入只落 b 侧，a 仍不创建
        let piece1: Vec<u8> = (0..10).map(|i| i as u8).collect();
        store.write_piece(1, &piece1).unwrap();
        assert!(!dir.join("dl").join("a.bin").exists());
        let b = std::fs::read(dir.join("dl").join("b.bin")).unwrap();
        assert_eq!(&b[..5], &piece1[5..]);

        // None = 全量：三个文件都创建
        drop(store);
        let dir2 = std::env::temp_dir().join(format!("xfer-piece-sel2-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir2);
        PieceStore::open(&dir2, "dl", layout3(), None).unwrap();
        assert!(dir2.join("dl").join("a.bin").exists());
        assert!(dir2.join("dl").join("b.bin").exists());
        assert!(dir2.join("dl").join("c.bin").exists());
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    /// 只有 1 个文件的**多文件**种子必须落在 `<name>/` 目录里。
    ///
    /// 回归点：`open` 此前用 `files.len()==1 && path.len()==1` 猜单文件，
    /// 于是「1 项 files 列表」的多文件种子被当成单文件——文件写成
    /// `root/<name>`（占了目录名，文件自己的路径段被整个丢掉）。判据必须是
    /// 种子结构位（`PieceLayout::multi_file`），不能靠项数推断。
    #[test]
    fn single_entry_multi_file_writes_into_named_dir() {
        let dir = std::env::temp_dir().join(format!("xfer-piece-multi1-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let layout =
            PieceLayout::new(10, vec![(vec!["movie.mkv".into()], 20)]).with_multi_file(true);
        let store = PieceStore::open(&dir, "Best Movie", layout, None).unwrap();
        let expected = dir.join("Best Movie").join("movie.mkv");
        assert_eq!(store.file_paths(), vec![expected.clone()]);
        assert!(expected.exists(), "文件必须落在 <name>/<path> 里");
        assert!(
            dir.join("Best Movie").is_dir(),
            "<name> 必须仍是目录，不能被同名文件占掉"
        );

        // 对照：同样 1 项、1 段路径，但结构位是单文件种子 → 直接写 <name>
        let dir2 =
            std::env::temp_dir().join(format!("xfer-piece-single1-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir2);
        let layout = PieceLayout::new(10, vec![(vec!["movie.mkv".into()], 20)]);
        assert!(!layout.multi_file, "1 项 + 1 段路径的默认判定仍是单文件");
        let store = PieceStore::open(&dir2, "movie.mkv", layout, None).unwrap();
        assert_eq!(store.file_paths(), vec![dir2.join("movie.mkv")]);
        assert!(dir2.join("movie.mkv").is_file());

        // 嵌套路径的 1 项种子历史行为也是多文件（不能被这次改动改掉）
        let layout = PieceLayout::new(10, vec![(vec!["sub".into(), "a.bin".into()], 20)]);
        assert!(layout.multi_file, "路径嵌套的 1 项种子一直按多文件处理");

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    /// 旧版把 1 项多文件种子落成 `<root>/<name>` **普通文件**：升级后 open
    /// 必须把它搬进 `<root>/<name>/<file>`，而不是因同名文件无法建目录而
    /// 报错（否则任务启动即失败、已下载数据悬在旧路径上没人管）。
    #[test]
    fn legacy_single_file_layout_is_migrated_into_dir() {
        let dir = std::env::temp_dir().join(format!("xfer-piece-mig-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // 旧版布局：数据在 <root>/Best Movie（普通文件）
        let legacy = dir.join("Best Movie");
        std::fs::write(&legacy, b"OLD-DATA").unwrap();

        let layout =
            PieceLayout::new(10, vec![(vec!["movie.bin".into()], 20)]).with_multi_file(true);
        let store = PieceStore::open(&dir, "Best Movie", layout, None).unwrap();
        let new_path = dir.join("Best Movie").join("movie.bin");
        assert!(
            legacy.is_dir(),
            "旧位置应变成目录（文件已被搬进 <name>/<file>）"
        );
        assert!(
            std::fs::read(&new_path).unwrap() == b"OLD-DATA",
            "已下载数据必须随迁移保留"
        );
        assert_eq!(store.file_paths(), vec![new_path]);

        // 对照：文件数 >1 时 `<name>` 不可能是本任务的数据 → 明确报错，绝不误搬
        let dir2 = std::env::temp_dir().join(format!("xfer-piece-mig2-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir2);
        std::fs::create_dir_all(&dir2).unwrap();
        let foreign = dir2.join("Best Movie");
        std::fs::write(&foreign, b"NOT-OURS").unwrap();
        let layout = PieceLayout::new(
            10,
            vec![(vec!["a.bin".into()], 10), (vec!["b.bin".into()], 10)],
        )
        .with_multi_file(true);
        let err = match PieceStore::open(&dir2, "Best Movie", layout, None) {
            Err(e) => e,
            Ok(_) => panic!("外来文件占位时必须报错，不得静默搬走"),
        };
        assert!(foreign.is_file(), "外来文件必须原样保留");
        assert!(err.to_string().contains("无法自动迁移"), "错误信息: {err}");

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    /// 结构位与文件数矛盾（multi_file=false 却给 2 个文件）必须按多文件兜底：
    /// 否则 N 个句柄全部指向 `<name>`，写入内容互相覆盖且无任何报错。
    #[test]
    fn inconsistent_layout_falls_back_to_multi() {
        let layout = PieceLayout::new(
            10,
            vec![(vec!["a.bin".into()], 10), (vec!["b.bin".into()], 10)],
        )
        .with_multi_file(false);
        assert!(!layout.is_single_file(), "矛盾结构必须按多文件兜底");
    }

    #[test]
    fn verify_piece_matches() {
        let data = b"hello bt";
        let mut h = Sha1::new();
        h.update(data);
        let digest: [u8; 20] = h.finalize().into();
        assert!(verify_piece(data, &digest));
        assert!(!verify_piece(data, &[0u8; 20]));
    }

    // ------------------------------------------------------------------
    // 磁盘缓存（写回缓冲）
    // ------------------------------------------------------------------

    /// 10 字节片长的单文件布局：3 片（10/10/5）。
    fn cache_layout() -> PieceLayout {
        PieceLayout::new(10, vec![(vec!["one.bin".into()], 25)])
    }

    /// 返回 (临时目录, 存储, 数据文件路径)。单文件种子布局下数据文件
    /// 直接落在 `root/dl`（不是目录），因此路径统一由 store 自己给出。
    fn cache_store(tag: &str) -> (PathBuf, PieceStore, PathBuf) {
        let dir = std::env::temp_dir().join(format!("xfer-piece-cache-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = PieceStore::open(&dir, "dl", cache_layout(), None).unwrap();
        let data = store.file_paths()[0].clone();
        (dir, store, data)
    }

    #[test]
    fn write_cache_serves_reads_before_flush() {
        let (dir, mut store, data) = cache_store("read");
        // 上限足够容纳全部片：写入后仍在缓存、磁盘为空文件
        store.set_write_cache_limit(4096).unwrap();
        let p0: Vec<u8> = (0..10).collect();
        store.write_piece(0, &p0).unwrap();
        assert!(store.write_cache_contains(0));
        assert_eq!(store.write_cache_bytes(), 10);
        assert_eq!(std::fs::metadata(&data).unwrap().len(), 0);

        // 读路径命中缓存（read_piece / read_block 均不碰磁盘）
        assert_eq!(store.read_piece(0).unwrap(), p0);
        assert_eq!(store.read_block(0, 2, 4).unwrap(), &p0[2..6]);
        assert_eq!(store.read_block(0, 8, 4).unwrap(), &p0[8..10]);

        // flush 后落盘、缓存清空
        store.flush_write_cache().unwrap();
        assert_eq!(store.write_cache_bytes(), 0);
        assert!(!store.write_cache_contains(0));
        let f = std::fs::read(&data).unwrap();
        assert_eq!(&f[..10], &p0[..]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 持久化位图不得领先于磁盘：仍在写回缓存里的片必须按「未完成」呈现。
    ///
    /// 回归点：续传控制文件此前用 `bitfield()`（= mark_done 的即时结果），
    /// 开启 disk-cache 后片数据只在内存——崩溃后按这份位图恢复，会重现
    /// 「已完成的空洞」并静默产出损坏文件。
    #[test]
    fn durable_bitfield_excludes_unflushed_pieces() {
        let (dir, mut store, data) = cache_store("durable");
        // 无缓存时两者一致（直写路径：mark_done 即已落盘）
        store.write_piece(0, &vec![7u8; 10]).unwrap();
        store.mark_done(0);
        assert_eq!(store.durable_bitfield(), store.bitfield());
        store.flush_write_cache().unwrap();

        // 开启缓存：片 1 只进内存 → 只出现在 bitfield，不出现在 durable
        store.set_write_cache_limit(4096).unwrap();
        store.write_piece(1, &vec![9u8; 10]).unwrap();
        store.mark_done(1);
        let live = store.bitfield();
        let durable = store.durable_bitfield();
        assert_eq!(live.len(), durable.len());
        assert_eq!(live[0] & 0b1100_0000, 0b1100_0000, "bitfield 应含片 0/1");
        assert_eq!(durable[0] & 0b1000_0000, 0b1000_0000, "片 0 已落盘应保留");
        assert_eq!(durable[0] & 0b0100_0000, 0, "片 1 未落盘不得声称完成");
        // 落盘后重新对齐
        assert_eq!(std::fs::metadata(&data).unwrap().len(), 10, "片 1 尚未落盘");
        store.flush_write_cache().unwrap();
        assert_eq!(store.durable_bitfield(), store.bitfield());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_cache_evicts_oldest_when_full() {
        let (dir, mut store, data) = cache_store("evict");
        // 上限 15 字节：第 3 片入队后必须逐出最旧的片 0
        store.set_write_cache_limit(15).unwrap();
        let p0: Vec<u8> = vec![1; 10];
        let p1: Vec<u8> = vec![2; 10];
        store.write_piece(0, &p0).unwrap();
        store.write_piece(1, &p1).unwrap();
        // 上限 15：写入 p1 后总量 20 超限 → 逐出最旧的 p0，缓存回落到 10
        assert_eq!(store.write_cache_bytes(), 10);
        assert!(!store.write_cache_contains(0));
        assert!(store.write_cache_contains(1));
        // 被逐出的片数据已在磁盘上
        let f = std::fs::read(&data).unwrap();
        assert_eq!(&f[..10], &p0[..]);
        assert_eq!(store.read_piece(0).unwrap(), p0);
        assert_eq!(store.read_piece(1).unwrap(), p1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_cache_dedupes_same_piece() {
        let (dir, mut store, data) = cache_store("dedupe");
        store.set_write_cache_limit(4096).unwrap();
        let a: Vec<u8> = vec![7; 10];
        let b: Vec<u8> = vec![9; 10];
        store.write_piece(1, &a).unwrap();
        store.write_piece(1, &b).unwrap();
        // 同片替换而非叠加
        assert_eq!(store.write_cache_bytes(), 10);
        assert_eq!(store.read_piece(1).unwrap(), b);
        store.flush_all().unwrap();
        // flush_all 后磁盘上是最后一次写入的内容（片 1 = 文件 10..20）
        let f = std::fs::read(&data).unwrap();
        assert_eq!(&f[10..20], &b[..]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_cache_disabled_writes_through() {
        let (dir, mut store, data) = cache_store("off");
        // 默认关闭缓存 → 直写（与旧行为一致）
        assert_eq!(store.write_cache_limit(), 0);
        let p0: Vec<u8> = vec![3; 10];
        store.write_piece(0, &p0).unwrap();
        assert_eq!(store.write_cache_bytes(), 0);
        assert_eq!(std::fs::metadata(&data).unwrap().len(), 10);
        // 关闭缓存后读取同样正确
        assert_eq!(store.read_piece(0).unwrap(), p0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn oversized_piece_bypasses_cache() {
        let (dir, mut store, data) = cache_store("oversize");
        // 上限小于片长（10）：该片不进缓存，直接落盘
        store.set_write_cache_limit(4).unwrap();
        let p0: Vec<u8> = vec![5; 10];
        store.write_piece(0, &p0).unwrap();
        assert_eq!(store.write_cache_bytes(), 0);
        assert!(!store.write_cache_contains(0));
        assert_eq!(std::fs::metadata(&data).unwrap().len(), 10);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shrink_cache_limit_flushes_overflow() {
        let (dir, mut store, data) = cache_store("shrink");
        store.set_write_cache_limit(4096).unwrap();
        store.write_piece(0, &[1u8; 10]).unwrap();
        store.write_piece(1, &[2u8; 10]).unwrap();
        // 缩小上限 → 超出部分逐出落盘
        store.set_write_cache_limit(10).unwrap();
        assert_eq!(store.write_cache_bytes(), 10);
        assert!(!store.write_cache_contains(0));
        let f = std::fs::read(&data).unwrap();
        assert_eq!(&f[..10], &[1u8; 10][..]);
        // 设为 0 → 全部落盘并关闭缓存
        store.set_write_cache_limit(0).unwrap();
        assert_eq!(store.write_cache_bytes(), 0);
        let f = std::fs::read(&data).unwrap();
        assert_eq!(&f[10..20], &[2u8; 10][..]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cache_read_skips_unselected_files() {
        let dir = std::env::temp_dir().join(format!("xfer-piece-cachesel-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        // 只选 b.bin：跨片 1（a 尾 + b 头）写入时缓存整片，读回只含 b 侧
        let mut store = PieceStore::open(&dir, "dl", layout3(), Some(&[1])).unwrap();
        store.set_write_cache_limit(4096).unwrap();
        let piece1: Vec<u8> = (0..10).map(|i| i as u8).collect();
        store.write_piece(1, &piece1).unwrap();
        assert_eq!(store.read_piece(1).unwrap(), piece1[5..].to_vec());
        assert_eq!(store.read_block(1, 0, 10).unwrap(), piece1[5..].to_vec());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn parse_size_bytes_handles_units() {
        use crate::parse_size_bytes;
        assert_eq!(parse_size_bytes("1k"), Some(1024));
        assert_eq!(parse_size_bytes("8M"), Some(8 * 1024 * 1024));
        assert_eq!(parse_size_bytes("128m"), Some(128 * 1024 * 1024));
        assert_eq!(parse_size_bytes("1G"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_size_bytes("1048576"), Some(1048576));
        assert_eq!(parse_size_bytes(" 16 M "), Some(16 * 1024 * 1024));
        // 非法输入
        assert_eq!(parse_size_bytes(""), None);
        assert_eq!(parse_size_bytes("abc"), None);
        assert_eq!(parse_size_bytes("12x"), None);
        assert_eq!(parse_size_bytes("M"), None);
    }
}
