//! 产物时间轴归零：把"源侧时间轴"平移到从 0 起算（收尾时跑一次）。
//!
//! 直播（以及个别带偏移的 VOD）分片自带上游的时间原点 —— fMP4 的 `tfdt`
//! 从"开播以来"起算（实测某 Bilibili 直播是 2h43m），拼接出的产物时间轴
//! 因此不从 0 开始。这不是不能播，而是**时长显示全看播放器心情**：把
//! "末端时间戳"当总时长的播放器显示成"开播至今"，按跨度算的才显示实际长度，
//! 还有一批干脆给不出时长 —— 用户看到的"显示直播总时长而不是实际录制时长"
//! 就是这么来的。
//!
//! 做法是**整体平移**（把时间轴平移到从 0 起算，即通行的 "shift to zero" 口径）：
//! - 基准 = 各轨首条样本时间戳换算成秒后的最小值（最靠前的轨道对齐到 0）；
//! - 每轨平移量 = 基准秒数 × 该轨时基（取整），**轨道间的相对偏移保持不变**
//!   （即音视频同步关系不动）；
//! - 只动样本时间戳（fMP4：`tfdt` 与 `sidx.earliest_presentation_time`），
//!   不动任何时长字段（`mvhd` 保持 0 是 fragmented 的常态，播放器照常扫分段）。
//!
//! 全部**就地改写**（字段等宽，文件长度不变），只在产物**最终完成**时调用：
//! 暂停 / 续传的中间态不能改（恢复后还会继续追加，提前改会让前后两截时间轴
//! 对不上）；失败也不影响文件内容，调用方 warn 即可。
//!
//! 注意：本版只处理 fMP4（直播场景实测的那一类）；MPEG-TS 的 PES PTS/PCR
//! 平移是同类问题，属后续项，当前按"认不出就无操作"处理。

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::HttpError;

/// 单个 box 读取上限：`moof` / `sidx` / `moov` 都在 KB 量级，超过它多半是
/// 扫描错位（或者根本不是这类文件）——宁可不动，也不要改坏。
const MAX_BOX_READ: u64 = 8 << 20;

/// 产物收尾时把时间轴归零。认得出容器（fMP4）才动手，其余直接无操作。
pub(crate) fn normalize_product_timeline(path: &Path) -> Result<(), HttpError> {
    let len = std::fs::metadata(path)
        .map_err(|e| HttpError::Io(e.to_string()))?
        .len();
    if len < 16 {
        return Ok(());
    }
    let mut head = [0u8; 376];
    let n = {
        let mut f = File::open(path).map_err(|e| HttpError::Io(e.to_string()))?;
        f.read(&mut head).map_err(|e| HttpError::Io(e.to_string()))?
    };
    if n >= 8 && matches!(&head[4..8], b"ftyp" | b"styp" | b"moof" | b"moov" | b"sidx" | b"free") {
        return normalize_fmp4(path, len);
    }
    // MPEG-TS（两包连续的同步字节）：同类问题，见模块头注释 —— 暂不处理。
    Ok(())
}

/// 读一个 box 的头：`(偏移, 总长, 类型)`。越界 / 截断（尾部半截 box）返回 None
/// —— 调用方就此收工，前面已处理的 box 保持已处理。
fn next_box(f: &mut (impl Seek + Read), off: u64, len: u64) -> Option<(u64, u64, [u8; 4])> {
    if off.checked_add(8)? > len {
        return None;
    }
    f.seek(SeekFrom::Start(off)).ok()?;
    let mut hdr = [0u8; 8];
    f.read_exact(&mut hdr).ok()?;
    let size32 = u32::from_be_bytes(hdr[0..4].try_into().ok()?);
    let typ = [hdr[4], hdr[5], hdr[6], hdr[7]];
    let (size, hdr_len) = if size32 == 1 {
        let mut ext = [0u8; 8];
        f.read_exact(&mut ext).ok()?;
        (u64::from_be_bytes(ext), 16u64)
    } else if size32 == 0 {
        (len.checked_sub(off)?, 8u64)
    } else {
        (size32 as u64, 8u64)
    };
    if size < hdr_len || off.checked_add(size)? > len {
        return None;
    }
    Some((off, size, typ))
}

fn be32(b: &[u8]) -> u32 {
    u32::from_be_bytes(b[0..4].try_into().unwrap())
}

fn be64(b: &[u8]) -> u64 {
    u64::from_be_bytes(b[0..8].try_into().unwrap())
}

/// 把 box 整体读进内存（超过上限或读失败返回 None）。
fn read_box(r: &mut BufReader<File>, off: u64, size: u64) -> Option<Vec<u8>> {
    if size > MAX_BOX_READ {
        return None;
    }
    r.seek(SeekFrom::Start(off)).ok()?;
    let mut buf = vec![0u8; size as usize];
    r.read_exact(&mut buf).ok()?;
    Some(buf)
}

/// 把改好的 box 写回原位置。
fn write_box(r: &mut BufReader<File>, off: u64, buf: &[u8]) -> Result<(), HttpError> {
    let f = r.get_mut();
    f.seek(SeekFrom::Start(off))
        .map_err(|e| HttpError::Io(e.to_string()))?;
    f.write_all(buf).map_err(|e| HttpError::Io(e.to_string()))
}

/// 通用子 box 遍历：对 `buf[start..end]` 里的每个 box 调 `f(slice, 绝对偏移)`。
/// 遇到截断 / 异常直接停（防御式，不做报错）。
fn walk_children<F: FnMut(&[u8], usize)>(buf: &[u8], start: usize, end: usize, mut f: F) {
    let mut p = start;
    while p + 8 <= end {
        let size32 = be32(&buf[p..]) as usize;
        let (total, hdr) = if size32 == 1 {
            if p + 16 > end {
                break;
            }
            (be64(&buf[p + 8..]) as usize, 16usize)
        } else if size32 == 0 {
            (end - p, 8usize)
        } else {
            (size32, 8usize)
        };
        if total < hdr || p + total > end {
            break;
        }
        f(&buf[p..p + total], p);
        p += total;
    }
}

/// 在 `moof`（含 box 头）里找出全部 `traf→tfdt`：
/// `(track_id, tfdt 值字段在 buf 内的偏移, 是否 64 位, 值)`。
///
/// `tfhd` 按规范排在 `traf` 内各 box 之前，先见 `tfhd` 拿 track_id；
/// 没见过 `tfhd` 的 `tfdt` 跳过（畸形，不动它）。
fn tfdt_entries(moof: &[u8]) -> Vec<(u32, usize, bool, u64)> {
    let mut out = Vec::new();
    walk_children(moof, 8, moof.len(), |traf, traf_off| {
        if &traf[4..8] != b"traf" {
            return;
        }
        let mut tid: Option<u32> = None;
        walk_children(traf, 8, traf.len(), |child, child_off| match &child[4..8] {
            b"tfhd" => {
                // ver/flags(4) 之后紧跟 track_id（恒存在）
                if child.len() >= 16 {
                    tid = Some(be32(&child[12..]));
                }
            }
            b"tfdt" => {
                let Some(t) = tid else { return };
                if child.len() < 16 {
                    return;
                }
                let v1 = child[8] == 1;
                if v1 && child.len() < 20 {
                    return;
                }
                let val = if v1 {
                    be64(&child[12..])
                } else {
                    be32(&child[12..]) as u64
                };
                out.push((t, traf_off + child_off + 12, v1, val));
            }
            _ => {}
        });
    });
    out
}

/// 解析 `moov`：`track_id → 媒体时基`（tkhd + 该 trak 的 mdhd）。
fn parse_moov_tracks(moov: &[u8], out: &mut HashMap<u32, u32>) {
    walk_children(moov, 8, moov.len(), |trak, _| {
        if &trak[4..8] != b"trak" {
            return;
        }
        let mut tid: Option<u32> = None;
        let mut ts: Option<u32> = None;
        walk_children(trak, 8, trak.len(), |child, _| {
            match &child[4..8] {
                b"tkhd" => {
                    // ver/flags(4) + creation(4|8) + modification(4|8) → track_id(4)
                    let v1 = child.len() > 8 && child[8] == 1;
                    let off = if v1 { 12 + 16 } else { 12 + 8 };
                    if child.len() >= off + 4 {
                        tid = Some(be32(&child[off..]));
                    }
                }
                b"mdia" => {
                    walk_children(child, 8, child.len(), |mdia_child, _| {
                        if &mdia_child[4..8] == b"mdhd" {
                            let v1 = mdia_child.len() > 8 && mdia_child[8] == 1;
                            let off = if v1 { 12 + 16 } else { 12 + 8 };
                            if mdia_child.len() >= off + 4 {
                                ts = Some(be32(&mdia_child[off..]));
                            }
                        }
                    });
                }
                _ => {}
            }
        });
        if let (Some(t), Some(s)) = (tid, ts) {
            if s > 0 {
                out.insert(t, s);
            }
        }
    });
}

/// fMP4 归零：两遍扫描（第一遍定基准，第二遍就地改写）。
fn normalize_fmp4(path: &Path, len: u64) -> Result<(), HttpError> {
    // —— 第一遍：轨道时基 + 各轨首个 tfdt ——
    let mut track_ts: HashMap<u32, u32> = HashMap::new();
    let mut firsts: HashMap<u32, u64> = HashMap::new();
    let mut moofs = 0u64;
    {
        let f = File::open(path).map_err(|e| HttpError::Io(e.to_string()))?;
        let mut r = BufReader::new(f);
        let mut off = 0u64;
        while let Some((b_off, b_size, typ)) = next_box(&mut r, off, len) {
            match &typ {
                b"moov" => {
                    if let Some(buf) = read_box(&mut r, b_off, b_size) {
                        parse_moov_tracks(&buf, &mut track_ts);
                    }
                }
                b"moof" => {
                    moofs += 1;
                    if let Some(buf) = read_box(&mut r, b_off, b_size) {
                        for (tid, _foff, _v1, val) in tfdt_entries(&buf) {
                            firsts.entry(tid).or_insert(val);
                        }
                    }
                }
                _ => {}
            }
            off = b_off + b_size;
        }
    }
    if moofs == 0 || firsts.is_empty() {
        return Ok(());
    }

    // —— 基准：各轨"首条样本时间戳（秒）"的最小值；每轨平移量按各自时基换算 ——
    let mut start_secs: Option<f64> = None;
    for (tid, first) in &firsts {
        let Some(&ts) = track_ts.get(tid) else { continue };
        let s = *first as f64 / ts as f64;
        start_secs = Some(start_secs.map_or(s, |x: f64| x.min(s)));
    }
    let Some(start_secs) = start_secs else {
        return Ok(());
    };
    let base_by_track: HashMap<u32, u64> = firsts
        .keys()
        .map(|tid| {
            let k = track_ts
                .get(tid)
                .map(|&ts| (start_secs * ts as f64).round() as u64)
                .unwrap_or(0);
            (*tid, k)
        })
        .collect();
    if base_by_track.values().all(|&k| k == 0) {
        return Ok(());
    }

    // —— 第二遍：就地改写 tfdt / sidx.ept ——
    let fw = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| HttpError::Io(e.to_string()))?;
    let mut rw = BufReader::new(fw);
    let mut patched_moofs = 0u64;
    let mut patched_sidx = 0u64;
    let mut off = 0u64;
    while let Some((b_off, b_size, typ)) = next_box(&mut rw, off, len) {
        match &typ {
            b"moof" => {
                if let Some(mut buf) = read_box(&mut rw, b_off, b_size) {
                    let entries = tfdt_entries(&buf);
                    let mut changed = false;
                    for (tid, foff, v1, val) in entries {
                        let k = base_by_track.get(&tid).copied().unwrap_or(0);
                        let new = val.saturating_sub(k);
                        if new == val {
                            continue;
                        }
                        let bytes = new.to_be_bytes();
                        if v1 {
                            buf[foff..foff + 8].copy_from_slice(&bytes);
                        } else {
                            buf[foff..foff + 4].copy_from_slice(&bytes[4..]);
                        }
                        changed = true;
                    }
                    if changed {
                        write_box(&mut rw, b_off, &buf)?;
                        patched_moofs += 1;
                    }
                }
            }
            b"sidx" => {
                if let Some(mut buf) = read_box(&mut rw, b_off, b_size) {
                    if buf.len() >= 24 {
                        let v1 = buf[8] == 1;
                        if !v1 || buf.len() >= 28 {
                            let ref_id = be32(&buf[12..]);
                            let ept_off = 20usize;
                            let (old, k) = (
                                if v1 { be64(&buf[ept_off..]) } else { be32(&buf[ept_off..]) as u64 },
                                base_by_track.get(&ref_id).copied().unwrap_or(0),
                            );
                            let new = old.saturating_sub(k);
                            if new != old {
                                let bytes = new.to_be_bytes();
                                if v1 {
                                    buf[ept_off..ept_off + 8].copy_from_slice(&bytes);
                                } else {
                                    buf[ept_off..ept_off + 4].copy_from_slice(&bytes[4..]);
                                }
                                write_box(&mut rw, b_off, &buf)?;
                                patched_sidx += 1;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        off = b_off + b_size;
    }
    tracing::info!(
        moofs,
        patched_moofs,
        patched_sidx,
        start_secs,
        "产物时间轴已归零（样本时间戳平移到从 0 起算）"
    );
    Ok(())
}

/// 各轨首个 `tfdt`（测试与诊断用）。
#[cfg(test)]
pub(crate) fn first_tfdts(path: &Path) -> HashMap<u32, u64> {
    let len = std::fs::metadata(path).unwrap().len();
    let mut firsts = HashMap::new();
    let f = File::open(path).unwrap();
    let mut r = BufReader::new(f);
    let mut off = 0u64;
    while let Some((b_off, b_size, typ)) = next_box(&mut r, off, len) {
        if &typ == b"moof" {
            if let Some(buf) = read_box(&mut r, b_off, b_size) {
                for (tid, _foff, _v1, val) in tfdt_entries(&buf) {
                    firsts.entry(tid).or_insert(val);
                }
            }
        }
        off = b_off + b_size;
    }
    firsts
}

#[cfg(test)]
pub(crate) mod fixtures {
    //! 构造最小可用的 fMP4 字节（只含归零逻辑关心的 box 结构）。

    /// 通用 box。
    pub(crate) fn bx(typ: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let mut v = ((payload.len() + 8) as u32).to_be_bytes().to_vec();
        v.extend_from_slice(typ);
        v.extend_from_slice(payload);
        v
    }

    fn full(ver: u8, rest: &[u8]) -> Vec<u8> {
        let mut v = vec![ver, 0, 0, 0];
        v.extend_from_slice(rest);
        v
    }

    fn tkhd(tid: u32) -> Vec<u8> {
        let mut rest = vec![0u8; 8];
        rest.extend_from_slice(&tid.to_be_bytes());
        bx(b"tkhd", &full(0, &rest))
    }

    fn mdhd(ts: u32) -> Vec<u8> {
        let mut rest = vec![0u8; 8];
        rest.extend_from_slice(&ts.to_be_bytes());
        bx(b"mdhd", &full(0, &rest))
    }

    fn trak(tid: u32, ts: u32) -> Vec<u8> {
        let mdia = bx(b"mdia", &mdhd(ts));
        let mut body = tkhd(tid);
        body.extend_from_slice(&mdia);
        bx(b"trak", &body)
    }

    fn tfhd(tid: u32) -> Vec<u8> {
        // flags = default-base-is-moof(0x020000)，无其他可选字段
        let mut rest = tid.to_be_bytes().to_vec();
        let mut body = vec![0u8, 0x02, 0x00, 0x00];
        body.append(&mut rest);
        bx(b"tfhd", &body)
    }

    fn tfdt(tid: u32, val: u64, v1: bool) -> Vec<u8> {
        let mut body = tfhd(tid);
        if v1 {
            let mut full_body = vec![1u8, 0, 0, 0];
            full_body.extend_from_slice(&val.to_be_bytes());
            body.extend_from_slice(&bx(b"tfdt", &full_body));
        } else {
            let mut full_body = vec![0u8, 0, 0, 0];
            full_body.extend_from_slice(&(val as u32).to_be_bytes());
            body.extend_from_slice(&bx(b"tfdt", &full_body));
        }
        body
    }

    fn traf(tid: u32, val: u64, v1: bool) -> Vec<u8> {
        bx(b"traf", &tfdt(tid, val, v1))
    }

    /// `ftyp + moov`（轨道：1 = 音频 48kHz，2 = 视频 90kHz）。
    pub(crate) fn fmp4_init() -> Vec<u8> {
        let mut v = bx(b"ftyp", b"isom\x00\x00\x02\x00isomiso6mp41");
        let mut moov_body = Vec::new();
        moov_body.extend_from_slice(&trak(1, 48000));
        moov_body.extend_from_slice(&trak(2, 90000));
        v.extend_from_slice(&bx(b"moov", &moov_body));
        v
    }

    /// 一个 fMP4 分片：`moof`（视频 + 音频两个 traf）+ `mdat`。
    pub(crate) fn fmp4_segment(video_tfdt: u64, audio_tfdt: u64) -> Vec<u8> {
        let mut moof_body = traf(2, video_tfdt, false);
        moof_body.extend_from_slice(&traf(1, audio_tfdt, false));
        let mut v = bx(b"moof", &moof_body);
        v.extend_from_slice(&bx(b"mdat", &[0u8; 64]));
        v
    }

    /// 一个 `sidx`（引用 track `ref_id`，`ept` 为最早呈现时间）。
    pub(crate) fn sidx(ref_id: u32, ts: u32, ept: u64) -> Vec<u8> {
        let mut body = vec![0u8, 0, 0, 0];
        body.extend_from_slice(&ref_id.to_be_bytes());
        body.extend_from_slice(&ts.to_be_bytes());
        body.extend_from_slice(&(ept as u32).to_be_bytes());
        body.extend_from_slice(&0u32.to_be_bytes()); // first_offset
        bx(b"sidx", &body)
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;

    fn write_tmp(name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("xfer-timeline-{}-{}", std::process::id(), name));
        std::fs::write(&p, bytes).unwrap();
        p
    }

    fn read(p: &Path) -> Vec<u8> {
        std::fs::read(p).unwrap()
    }

    /// 带偏移的 fMP4：整体平移到 0，且**每轨按各自时基**（相对偏移保持不变）。
    #[test]
    fn mp4_offsets_are_shifted_to_zero() {
        // 视频首段 10s@90k；音频首段 10s@48k；每段各前进 1s
        let mut data = fmp4_init();
        for i in 0..3u64 {
            data.extend_from_slice(&fmp4_segment(900_000 + i * 90_000, 480_000 + i * 48_000));
        }
        let p = write_tmp("shift", &data);
        normalize_product_timeline(&p).unwrap();
        let firsts = first_tfdts(&p);
        assert_eq!(firsts.get(&2).copied(), Some(0), "视频轨首 tfdt 应归零");
        assert_eq!(firsts.get(&1).copied(), Some(0), "音频轨首 tfdt 应归零");
        // 第二段：视频 900_000、音频 48_000 —— 相对推进量原样保留
        let data2 = read(&p);
        let mut all = Vec::new();
        walk_children(&data2, 0, data2.len(), |b, _| {
            if &b[4..8] == b"moof" {
                all.extend_from_slice(&tfdt_entries(b));
            }
        });
        let second_video = all
            .iter()
            .filter(|e| e.0 == 2)
            .nth(1)
            .map(|e| e.3)
            .unwrap();
        let second_audio = all
            .iter()
            .filter(|e| e.0 == 1)
            .nth(1)
            .map(|e| e.3)
            .unwrap();
        assert_eq!((second_video, second_audio), (90_000, 48_000));
        let _ = std::fs::remove_file(&p);
    }

    /// 两轨首样本秒数不同（视频 10.0005s、音频 10s）：按"最靠前的轨道对齐 0"，
    /// 其余轨道保留 0.5ms（45 tick @90k）的相对偏移。
    #[test]
    fn mp4_keeps_relative_track_offset() {
        let mut data = fmp4_init();
        data.extend_from_slice(&fmp4_segment(900_045, 480_000));
        data.extend_from_slice(&fmp4_segment(990_045, 528_000));
        let p = write_tmp("rel", &data);
        normalize_product_timeline(&p).unwrap();
        let firsts = first_tfdts(&p);
        assert_eq!(firsts.get(&1).copied(), Some(0));
        assert_eq!(firsts.get(&2).copied(), Some(45));
        let _ = std::fs::remove_file(&p);
    }

    /// `sidx.earliest_presentation_time` 同步平移。
    #[test]
    fn mp4_sidx_ept_is_shifted() {
        let mut data = fmp4_init();
        data.extend_from_slice(&sidx(2, 90_000, 900_000));
        data.extend_from_slice(&fmp4_segment(900_000, 480_000));
        let p = write_tmp("sidx", &data);
        normalize_product_timeline(&p).unwrap();
        let data2 = read(&p);
        let mut ept = None;
        walk_children(&data2, 0, data2.len(), |b, _| {
            if &b[4..8] == b"sidx" {
                ept = Some(be32(&b[20..]));
            }
        });
        assert_eq!(ept, Some(0));
        let _ = std::fs::remove_file(&p);
    }

    /// 时间轴本来就归零：逐字节不动（幂等 / 不产生无谓写）。
    #[test]
    fn mp4_zero_base_is_untouched() {
        let mut data = fmp4_init();
        data.extend_from_slice(&fmp4_segment(0, 0));
        data.extend_from_slice(&fmp4_segment(90_000, 48_000));
        let p = write_tmp("zero", &data);
        let before = read(&p);
        normalize_product_timeline(&p).unwrap();
        assert_eq!(read(&p), before);
        let _ = std::fs::remove_file(&p);
    }

    /// 不是 fMP4（随机字节 / TS 形态）：原样放过。
    #[test]
    fn non_mp4_is_noop() {
        let data: Vec<u8> = (0..4096u32).map(|i| (i * 7 % 251) as u8).collect();
        let p = write_tmp("nonmp4", &data);
        normalize_product_timeline(&p).unwrap();
        assert_eq!(read(&p), data);

        // 两个连续 0x47 同步字节的 TS 形态：同样不动（TS 属后续项）
        let mut ts = vec![0x47u8; 188 * 3];
        ts[1] = 0x00;
        let p2 = write_tmp("ts", &ts);
        normalize_product_timeline(&p2).unwrap();
        assert_eq!(read(&p2), ts);
        let _ = std::fs::remove_file(&p);
        let _ = std::fs::remove_file(&p2);
    }

    /// 尾部半截 box（写盘途中被截断的产物）：前面的 box 照常归零，
    /// 尾部原样保留，不报错。
    #[test]
    fn mp4_truncated_tail_is_tolerated() {
        let mut data = fmp4_init();
        data.extend_from_slice(&fmp4_segment(900_000, 480_000));
        data.extend_from_slice(&[0, 0, 0, 0x10, b'm']); // 断在 box 头中间
        let p = write_tmp("trunc", &data);
        normalize_product_timeline(&p).unwrap();
        let firsts = first_tfdts(&p);
        assert_eq!(firsts.get(&2).copied(), Some(0));
        assert_eq!(firsts.get(&1).copied(), Some(0));
        let _ = std::fs::remove_file(&p);
    }
}
