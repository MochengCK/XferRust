//! MPEG-TS 的 SAMPLE-AES 解密。
//!
//! 规范依据：Apple *MPEG-2 Stream Encryption Format for HTTP Live Streaming*
//! （`developer.apple.com/…/HLS_Sample_Encryption`）。要点：
//!
//! - **H.264**：只加密 `nal_unit_type` 为 1（非 IDR 片）与 5（IDR 片）的 NAL；
//!   NAL 头 1 字节 + 其后 31 字节明文，之后按「16 字节加密 + 至多 144 字节明文」
//!   循环（10% 跳加密）；整条 NAL **长度 ≤ 48 字节时完全明文**。
//! - **AAC**：ADTS 头（7 或 9 字节）+ 其后 16 字节明文，之后是整数个 16 字节密文块，
//!   帧尾 0~15 字节明文（无跳加密）。
//! - CBC 只在**单个 protected block（一个 NAL / 一个 AAC 帧）内部**链式，
//!   **每块开始时 IV 复位**。
//! - 加密后必须**重新施加起始码防竞争**（SCEP），所以解密要先去掉、解完再补回。
//!
//! ## 为什么要重新封装整段 TS
//!
//! 最后那条是这套格式在"下载器"语境下的关键：去掉 SCEP 再补回，**字节数会变**
//!（密文是伪随机的，会多出或少掉若干 `0x03`）。TS 是 188 字节定长包，长度一变就
//! 不能就地改，必须把 PES 重新拆包。所以这里的流程是：
//!
//! 拆 TS → 按 PID 重组 PES → 逐 PES 解密 ES → 重新打包 TS（保留原 PAT/PMT 与
//! 首个包的 adaptation field，连续性计数器重排）。
//!
//! 不这么做的下载器只能"就地改"，产物会带着一堆 `0x000001` 假起始码，播放器
//! 一读就花屏 —— 那还不如直接报错。

use super::crypto::{add_emulation_prevention, remove_emulation_prevention, SampleDecryptor};

/// TS 包长（固定 188，含 4 字节包头）。
const TS_PACKET: usize = 188;
/// 包头之后的净荷空间。
const TS_PAYLOAD: usize = TS_PACKET - 4;
/// 同步字节。
const TS_SYNC: u8 = 0x47;

/// H.264 的 SAMPLE-AES：NAL 头 + 31 字节明文之后，每 16 字节密文后跳过的明文字节数。
const H264_SKIP: usize = 144;
/// NAL 长度不超过它时规范规定完全明文（32 字节头 + 至多 16 字节"保护块"）。
const H264_MIN_ENCRYPTED_NAL: usize = 48;
/// AAC 的明文头长度（ADTS 头之后还要留 16 字节）。
const AAC_CLEAR_AFTER_HEADER: usize = 16;

/// 流类型（PMT 的 `stream_type`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamType {
    /// 0x1B：H.264/AVC。
    H264,
    /// 0x0F：AAC（ADTS 封装）。
    AacAdts,
    /// 认得出但**解不了**的类型（HEVC / AC-3 / 私有流…）：要明确拒绝，
    /// 不然会写出一个"看起来下完了、播起来全是噪声"的文件。
    Unsupported(u8),
}

impl StreamType {
    fn of(raw: u8) -> Self {
        match raw {
            0x1B => Self::H264,
            0x0F => Self::AacAdts,
            other => Self::Unsupported(other),
        }
    }

    fn name(self) -> String {
        match self {
            Self::H264 => "H.264".into(),
            Self::AacAdts => "AAC".into(),
            Self::Unsupported(t) => format!("stream_type=0x{t:02X}"),
        }
    }
}

/// 一段重组好的 PES（拆包前）。
struct Pes {
    pid: u16,
    /// PES 头（`00 00 01 xx …` 直到 ES 数据之前）。
    header: Vec<u8>,
    /// ES 数据（已解密）。
    payload: Vec<u8>,
    /// 这个 PES 首个 TS 包的 adaptation field 原始字节（长度字节之后的部分）。
    ///
    /// 保留它 = 保留 PCR / 随机访问标志 / 无缝拼接标志：丢掉 PCR 后严格一些的
    /// 播放器会直接判定流不合法。
    first_af: Vec<u8>,
}

/// TS 解密的入口：`data` 为一个完整分片（整数个 188 字节包）。
pub(crate) fn decrypt_sample_aes_ts(data: &[u8], key: &[u8], iv: &[u8; 16]) -> Result<Vec<u8>, String> {
    if data.len() < TS_PACKET || data.len() % TS_PACKET != 0 {
        return Err(format!(
            "SAMPLE-AES(TS) 分片长度 {} 不是 188 的整数倍",
            data.len()
        ));
    }
    if data[0] != TS_SYNC {
        return Err("SAMPLE-AES(TS) 分片缺少 TS 同步字节".into());
    }

    let (pmt_pid, streams) = scan_tables(data)?;
    let mut pes_list = reassemble_pes(data, &streams)?;
    if pes_list.is_empty() {
        return Err("SAMPLE-AES(TS) 分片里没有可识别的 PES 包".into());
    }

    // 逐 PES 解密。同一把密钥贯穿整段，但 IV 与 CBC 状态都是"每个 protected block
    // 复位"，所以解密器可以复用（见 crypto::SampleDecryptor）。
    let mut dec = SampleDecryptor::new(key, iv)?;
    for pes in pes_list.iter_mut() {
        match StreamType::of(streams[&pes.pid]) {
            StreamType::H264 => decrypt_h264_payload(&mut pes.payload, &mut dec)?,
            StreamType::AacAdts => decrypt_aac_payload(&mut pes.payload, &mut dec)?,
            other => {
                return Err(format!(
                    "SAMPLE-AES 暂不支持该 TS 流里的 {}（只支持 H.264 与 AAC）",
                    other.name()
                ))
            }
        }
    }

    remux_ts(data, pmt_pid, &pes_list)
}

// ---------------------------------------------------------------------------
// TS 拆包
// ---------------------------------------------------------------------------

/// 扫 PAT / PMT，拿到 PMT PID 与 `elementary_PID → stream_type` 映射。
fn scan_tables(data: &[u8]) -> Result<(u16, std::collections::HashMap<u16, u8>), String> {
    let mut pmt_pid: Option<u16> = None;
    for pkt in data.chunks_exact(TS_PACKET) {
        let (pid, pusi, payload) = packet_payload(pkt);
        if pid == 0 && pusi {
            pmt_pid = parse_pat(payload);
            if pmt_pid.is_some() {
                break;
            }
        }
    }
    let pmt_pid = pmt_pid.ok_or("SAMPLE-AES(TS) 分片里找不到 PAT/PMT")?;

    let mut streams = std::collections::HashMap::new();
    for pkt in data.chunks_exact(TS_PACKET) {
        let (pid, pusi, payload) = packet_payload(pkt);
        if pid == pmt_pid && pusi {
            if parse_pmt(payload, &mut streams)? {
                return Ok((pmt_pid, streams));
            }
        }
    }
    Err("SAMPLE-AES(TS) 分片里的 PMT 不完整（跨包）".into())
}

/// 解出一个 TS 包的 `(PID, PUSI, 净荷)`；净荷为空表示这个包只有 adaptation field。
fn packet_payload(pkt: &[u8]) -> (u16, bool, &[u8]) {
    let pusi = pkt[1] & 0x40 != 0;
    let pid = (((pkt[1] & 0x1F) as u16) << 8) | pkt[2] as u16;
    let afc = (pkt[3] >> 4) & 0x03;
    let mut off = 4usize;
    if afc & 0x02 != 0 {
        // adaptation field：第一个字节是它的长度
        let len = pkt[4] as usize;
        off = 5 + len;
    }
    let has_payload = afc & 0x01 != 0;
    if !has_payload || off >= TS_PACKET {
        return (pid, pusi, &[]);
    }
    (pid, pusi, &pkt[off..])
}

/// 从 PAT 净荷里取第一个节目的 PMT PID。
fn parse_pat(payload: &[u8]) -> Option<u16> {
    // 净荷第一个字节是 pointer_field，指向 section 起点
    let ptr = *payload.first()? as usize;
    let sec = payload.get(1 + ptr..)?;
    if sec.len() < 12 || sec[0] != 0x00 {
        return None;
    }
    let sec_len = (((sec[1] & 0x0F) as usize) << 8) | sec[2] as usize;
    if sec_len < 9 || sec.len() < 3 + sec_len - 4 + 4 {
        // 长度字段不含自身 3 字节与末尾 CRC 4 字节
    }
    let body = sec.get(8..)?;
    let n = (sec_len.saturating_sub(5)) / 4;
    for i in 0..n {
        let p = body.get(i * 4..i * 4 + 4)?;
        let program = ((p[0] as u16) << 8) | p[1] as u16;
        let pid = (((p[2] & 0x1F) as u16) << 8) | p[3] as u16;
        // program_number = 0 是 network PID，不是 PMT
        if program != 0 {
            return Some(pid);
        }
    }
    None
}

/// 从 PMT 净荷里收集 `elementary_PID → stream_type`。返回是否解析成功（false = 跨包，暂不支持）。
fn parse_pmt(
    payload: &[u8],
    out: &mut std::collections::HashMap<u16, u8>,
) -> Result<bool, String> {
    let ptr = match payload.first() {
        Some(p) => *p as usize,
        None => return Ok(false),
    };
    let sec = match payload.get(1 + ptr..) {
        Some(s) => s,
        None => return Ok(false),
    };
    if sec.len() < 12 || sec[0] != 0x02 {
        return Ok(false);
    }
    let sec_len = (((sec[1] & 0x0F) as usize) << 8) | sec[2] as usize;
    // section 总长 = 3 + sec_len（含 CRC），拿不到完整 section 就是跨包
    if sec.len() < 3 + sec_len {
        return Ok(false);
    }
    let program_info_len = (((sec[10] & 0x0F) as usize) << 8) | sec[11] as usize;
    let mut i = 12 + program_info_len;
    // 末尾 4 字节是 CRC32
    let end = 3 + sec_len - 4;
    while i + 5 <= end {
        let stream_type = sec[i];
        let pid = (((sec[i + 1] & 0x1F) as u16) << 8) | sec[i + 2] as u16;
        let es_info_len = (((sec[i + 3] & 0x0F) as usize) << 8) | sec[i + 4] as usize;
        out.insert(pid, stream_type);
        i += 5 + es_info_len;
    }
    if out.is_empty() {
        return Err("SAMPLE-AES(TS) 的 PMT 里没有基本流".into());
    }
    Ok(true)
}

/// 按 PID 重组 PES 包（保持原始出现顺序）。
fn reassemble_pes(
    data: &[u8],
    streams: &std::collections::HashMap<u16, u8>,
) -> Result<Vec<Pes>, String> {
    let mut out: Vec<Pes> = Vec::new();
    // 当前正在累积的 PES：索引进 out
    let mut cur: Option<usize> = None;

    for pkt in data.chunks_exact(TS_PACKET) {
        let pusi = pkt[1] & 0x40 != 0;
        let pid = (((pkt[1] & 0x1F) as u16) << 8) | pkt[2] as u16;
        if !streams.contains_key(&pid) {
            continue;
        }
        let afc = (pkt[3] >> 4) & 0x03;
        // adaptation field 原始字节（长度字节之后），只取每个 PES 的首包
        let af_data: &[u8] = if afc & 0x02 != 0 && pkt[4] > 0 {
            let len = pkt[4] as usize;
            &pkt[5..(5 + len).min(TS_PACKET)]
        } else {
            &[]
        };
        let off = if afc & 0x02 != 0 {
            5 + pkt[4] as usize
        } else {
            4
        };
        let payload: &[u8] = if afc & 0x01 != 0 && off < TS_PACKET {
            &pkt[off..]
        } else {
            &[]
        };

        if pusi {
            // 新 PES 开始
            let start = pes_payload_start(payload).ok_or_else(|| {
                format!("SAMPLE-AES(TS) 的 PID {pid} 上 PES 头不完整（跨包）")
            })?;
            out.push(Pes {
                pid,
                header: payload[..start].to_vec(),
                payload: payload[start..].to_vec(),
                first_af: af_data.to_vec(),
            });
            cur = Some(out.len() - 1);
            continue;
        }
        // 续包：接到当前 PES 上
        if let Some(i) = cur {
            if out[i].pid == pid {
                out[i].payload.extend_from_slice(payload);
            }
        }
    }
    Ok(out)
}

/// PES 头长度（到 ES 数据为止）。
fn pes_payload_start(pes: &[u8]) -> Option<usize> {
    if pes.len() < 6 || pes[0] != 0 || pes[1] != 0 || pes[2] != 1 {
        return None;
    }
    let stream_id = pes[3];
    // 这几种 stream_id 没有可选头，ES 数据直接从第 6 字节开始
    if matches!(stream_id, 0xBC | 0xBE | 0xBF | 0xF0 | 0xF1 | 0xF2 | 0xF8 | 0xFF) {
        return Some(6);
    }
    if pes.len() < 9 {
        return None;
    }
    let header_data_len = pes[8] as usize;
    let start = 9 + header_data_len;
    if start > pes.len() {
        return None;
    }
    Some(start)
}

// ---------------------------------------------------------------------------
// 逐 PES 解密
// ---------------------------------------------------------------------------

/// 解一段 H.264 ES（一个 PES 的净荷，Annex B 字节流）。
///
/// NAL 的边界靠起始码切；**最后一个 NAL 到净荷末尾为止**（HLS 的 TS 分片按
/// 访问单元切，一个 PES 装一个 AU，所以最后一个 NAL 一定是完整的）。
fn decrypt_h264_payload(payload: &mut Vec<u8>, dec: &mut SampleDecryptor) -> Result<(), String> {
    let starts = start_codes(payload);
    if starts.is_empty() {
        // 没有起始码：多半是上一包 NAL 的续包（编码器没按 AU 对齐 PES）。
        // 这种没法定位加密区，只能原样留着并记一笔 —— 报错会让整条任务失败，
        // 而这里通常只影响极少数包。
        tracing::debug!("SAMPLE-AES(TS)：H.264 净荷里没有起始码，跳过该 PES");
        return Ok(());
    }
    let mut out = Vec::with_capacity(payload.len() + 16);
    for (i, &(code_off, code_len)) in starts.iter().enumerate() {
        let nal_start = code_off + code_len;
        let nal_end = starts.get(i + 1).map(|&(o, _)| o).unwrap_or(payload.len());
        out.extend_from_slice(&payload[code_off..nal_start]);
        if nal_start >= nal_end {
            continue;
        }
        let nal = &payload[nal_start..nal_end];
        out.extend_from_slice(&decrypt_one_nal(nal, dec));
    }
    *payload = out;
    Ok(())
}

/// 解一条 NAL（返回它的新字节，长度可能与输入不同）。
fn decrypt_one_nal(nal: &[u8], dec: &mut SampleDecryptor) -> Vec<u8> {
    let nal_type = nal[0] & 0x1F;
    // 只加密类型 1（非 IDR 片）与 5（IDR 片）；长度 ≤ 48 的整条明文
    if (nal_type != 1 && nal_type != 5) || nal.len() <= H264_MIN_ENCRYPTED_NAL {
        return nal.to_vec();
    }
    // 加密是按"去掉防竞争字节之后"的 NAL 定位的
    let mut clean = remove_emulation_prevention(nal);
    if clean.len() <= H264_MIN_ENCRYPTED_NAL {
        return nal.to_vec();
    }
    // 头 32 字节明文，之后按 16 密文 + 144 明文循环
    dec.decrypt_block(&mut clean[32..], H264_SKIP, false);
    // 加密后必须重新施加防竞争（这也是长度会变、必须重新封装 TS 的原因）
    add_emulation_prevention(&clean)
}

/// 解一段 AAC（ADTS）ES：逐帧处理，跳过 ADTS 头与其后 16 字节明文。
fn decrypt_aac_payload(payload: &mut Vec<u8>, dec: &mut SampleDecryptor) -> Result<(), String> {
    let mut pos = 0usize;
    let mut frames = 0usize;
    while pos + 7 <= payload.len() {
        // ADTS 同步字：12 个 1
        if payload[pos] != 0xFF || (payload[pos + 1] & 0xF0) != 0xF0 {
            pos += 1;
            continue;
        }
        // protection_absent = 1 → 无 CRC，头 7 字节；否则 9 字节
        let header_len = if payload[pos + 1] & 0x01 != 0 { 7 } else { 9 };
        let frame_len = (((payload[pos + 3] as usize) & 0x03) << 11)
            | ((payload[pos + 4] as usize) << 3)
            | ((payload[pos + 5] as usize) >> 5);
        if frame_len == 0 || pos + frame_len > payload.len() {
            break;
        }
        let enc_start = pos + header_len + AAC_CLEAR_AFTER_HEADER;
        if frame_len > header_len + AAC_CLEAR_AFTER_HEADER && enc_start < pos + frame_len {
            // 加密区长度是 16 的整数倍，帧尾余下的 0~15 字节明文
            let avail = frame_len - header_len - AAC_CLEAR_AFTER_HEADER;
            let enc_len = avail / 16 * 16;
            if enc_len > 0 {
                // AAC：**每个整块都要解**（与 H.264 的 `> 16` 不同）
                dec.decrypt_block(&mut payload[enc_start..enc_start + enc_len], 0, true);
            }
        }
        frames += 1;
        pos += frame_len;
    }
    if frames == 0 {
        return Err("SAMPLE-AES(TS)：AAC 净荷里找不到 ADTS 帧".into());
    }
    Ok(())
}

/// 扫出所有起始码（`00 00 01` 或 `00 00 00 01`），返回 `(偏移, 码长)`。
fn start_codes(buf: &[u8]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 3 <= buf.len() {
        if buf[i] == 0 && buf[i + 1] == 0 {
            if buf[i + 2] == 1 {
                // 前面若还有一个 0，起始码算 4 字节（归到同一个 NAL 头之前）
                if i > 0 && buf[i - 1] == 0 {
                    out.push((i - 1, 4));
                } else {
                    out.push((i, 3));
                }
                i += 3;
                continue;
            }
            // 00 00 00 01：由上面 i-1 那一步处理
            if i + 4 <= buf.len() && buf[i + 2] == 0 && buf[i + 3] == 1 {
                out.push((i, 4));
                i += 4;
                continue;
            }
        }
        i += 1;
    }
    out
}

// ---------------------------------------------------------------------------
// 重新封装 TS
// ---------------------------------------------------------------------------

/// 把（已解密的）PES 重新打成 TS：保留原 PAT / PMT 包，其余按顺序重发。
fn remux_ts(data: &[u8], pmt_pid: u16, pes_list: &[Pes]) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(data.len() + data.len() / 8 + TS_PACKET * 4);
    // 1) 原样搬 PAT 与 PMT（表结构没变，重打一遍没必要）
    for pkt in data.chunks_exact(TS_PACKET) {
        let pid = (((pkt[1] & 0x1F) as u16) << 8) | pkt[2] as u16;
        if pid == 0 || pid == pmt_pid {
            out.extend_from_slice(pkt);
        }
    }
    // 2) 逐 PES 重新打包
    let mut cc: std::collections::HashMap<u16, u8> = std::collections::HashMap::new();
    for pes in pes_list {
        let counter = cc.entry(pes.pid).or_insert(0);
        write_pes(&mut out, pes, counter)?;
    }
    Ok(out)
}

/// 把一个 PES 写成若干个 188 字节的 TS 包。
fn write_pes(out: &mut Vec<u8>, pes: &Pes, cc: &mut u8) -> Result<(), String> {
    // PES 头里的包长度字段要跟着新的净荷长度改（视频常写 0 = 不定长，保持 0）
    let mut header = pes.header.clone();
    if header.len() >= 6 {
        let declared = ((header[4] as usize) << 8) | header[5] as usize;
        if declared != 0 {
            let new_len = header.len() - 6 + pes.payload.len();
            if new_len > 0xFFFF {
                // 超长：改成 0（不定长），解码器按 TS 层边界收
                header[4] = 0;
                header[5] = 0;
            } else {
                header[4] = (new_len >> 8) as u8;
                header[5] = (new_len & 0xFF) as u8;
            }
        }
    }
    let mut body = header;
    body.extend_from_slice(&pes.payload);

    let mut offset = 0usize;
    let mut first = true;
    while offset < body.len() || first {
        let remaining = body.len() - offset;
        // adaptation field 的总字节数（含长度字节）：首包保留原来的 AF，其余只做填充
        let mut af_total = if first && !pes.first_af.is_empty() {
            (1 + pes.first_af.len()).max(TS_PAYLOAD.saturating_sub(remaining))
        } else if remaining < TS_PAYLOAD {
            TS_PAYLOAD - remaining
        } else {
            0
        };
        // AFC=11 时 adaptation_field_length 允许 0..=182，但要有 flags 字节就得 ≥2
        if af_total == 1 {
            af_total = 2;
        }
        if af_total > TS_PAYLOAD {
            af_total = TS_PAYLOAD;
        }
        let payload_space = TS_PAYLOAD - af_total;
        let take = remaining.min(payload_space);

        let pkt_start = out.len();
        out.push(TS_SYNC);
        out.push((if first { 0x40 } else { 0x00 }) | ((pes.pid >> 8) as u8 & 0x1F));
        out.push((pes.pid & 0xFF) as u8);
        let afc: u8 = if af_total == 0 {
            0x01
        } else if take == 0 {
            0x02
        } else {
            0x03
        };
        out.push((afc << 4) | (*cc & 0x0F));
        *cc = (*cc + 1) & 0x0F;

        if af_total > 0 {
            // 长度字节
            out.push((af_total - 1) as u8);
            if af_total >= 2 {
                // flags 字节：首包沿用原来的（可能是 PCR / 随机访问标志），其余为 0
                let (flags, extra) = if first && !pes.first_af.is_empty() {
                    (pes.first_af[0], &pes.first_af[1..])
                } else {
                    (0u8, &[][..])
                };
                out.push(flags);
                out.extend_from_slice(extra);
                let used = 2 + extra.len();
                for _ in used..af_total {
                    out.push(0xFF);
                }
            }
        }
        out.extend_from_slice(&body[offset..offset + take]);
        offset += take;
        first = false;
        debug_assert_eq!(out.len() - pkt_start, TS_PACKET);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 造一个最小的 TS：PAT + PMT + 一条指定 stream_type 的 PES。
    ///
    /// PES **直接用生产代码的 [`write_pes`] 打包** —— 早先这里自己拼包，把填充的
    /// `0xFF` 写进了净荷里（而不是 adaptation field），于是重组出来的 ES 尾部多出
    /// 一长串 `0xFF`，测试和实现对不上。用同一个打包器就没有这种"两套实现"的问题。
    fn build_ts(stream_type: u8, es: &[u8], pid: u16) -> Vec<u8> {
        let mut out = Vec::new();
        // ---- PAT（PID 0）----
        let mut pat = vec![0x00u8]; // pointer_field
        let section: Vec<u8> = {
            let mut s = vec![0x00, 0xB0, 0x0D]; // table_id, section_length=13
            s.extend_from_slice(&[0x00, 0x01]); // transport_stream_id
            s.push(0xC1); // version/current
            s.extend_from_slice(&[0x00, 0x00]); // section_number, last_section_number
            s.extend_from_slice(&[0x00, 0x01, 0xE0, 0x64]); // program 1 → PMT PID 0x64
            s.extend_from_slice(&[0, 0, 0, 0]); // CRC32（本测试不校验）
            s
        };
        pat.extend_from_slice(&section);
        out.extend_from_slice(&make_packet(0, true, &pat, &[]));
        // ---- PMT（PID 0x64）----
        let mut pmt = vec![0x00u8];
        let sec: Vec<u8> = {
            let mut s = vec![0x02, 0xB0, 0x12];
            s.extend_from_slice(&[0x00, 0x01]);
            s.push(0xC1);
            s.extend_from_slice(&[0x00, 0x00]);
            s.extend_from_slice(&[0xE0, 0x00]); // PCR_PID（这里用 0，不校验）
            s.extend_from_slice(&[0xF0, 0x00]); // program_info_length = 0
            s.push(stream_type);
            s.extend_from_slice(&[0xE0 | ((pid >> 8) as u8 & 0x1F), (pid & 0xFF) as u8]);
            s.extend_from_slice(&[0xF0, 0x00]); // ES_info_length = 0
            s.extend_from_slice(&[0, 0, 0, 0]);
            s
        };
        pmt.extend_from_slice(&sec);
        out.extend_from_slice(&make_packet(0x64, true, &pmt, &[]));
        // ---- PES（用生产代码的打包器）----
        let mut header = vec![0x00u8, 0x00, 0x01, 0xE0, 0x00, 0x00, 0x80, 0x00, 0x00];
        let declared = header.len() - 6 + es.len();
        header[4] = (declared >> 8) as u8;
        header[5] = (declared & 0xFF) as u8;
        let pes = Pes {
            pid,
            header,
            payload: es.to_vec(),
            first_af: Vec::new(),
        };
        let mut cc = 0u8;
        write_pes(&mut out, &pes, &mut cc).unwrap();
        out
    }

    /// 造一个只用于 PAT / PMT 的定长包（净荷短，尾部 `0xFF` 填充不会影响
    /// 表解析 —— 那两个解析器都按 `section_length` 收口）。
    fn make_packet(pid: u16, pusi: bool, payload: &[u8], af: &[u8]) -> Vec<u8> {
        let mut pkt = vec![0xFFu8; TS_PACKET];
        pkt[0] = TS_SYNC;
        pkt[1] = (if pusi { 0x40 } else { 0 }) | ((pid >> 8) as u8 & 0x1F);
        pkt[2] = (pid & 0xFF) as u8;
        let afc = if af.is_empty() { 0x01 } else { 0x03 };
        pkt[3] = afc << 4;
        let mut off = 4;
        if !af.is_empty() {
            pkt[4] = af.len() as u8;
            pkt[5..5 + af.len()].copy_from_slice(af);
            off = 5 + af.len();
        }
        let take = payload.len().min(TS_PACKET - off);
        pkt[off..off + take].copy_from_slice(&payload[..take]);
        pkt
    }

    /// 按规范加密一条 NAL（与 ts.rs 的 decrypt_one_nal 互为逆运算）。
    fn encrypt_nal(nal: &[u8], key: &[u8; 16], iv: &[u8; 16]) -> Vec<u8> {
        use aes::cipher::{BlockEncryptMut, KeyInit};
        let nal_type = nal[0] & 0x1F;
        if (nal_type != 1 && nal_type != 5) || nal.len() <= H264_MIN_ENCRYPTED_NAL {
            return nal.to_vec();
        }
        let mut clean = remove_emulation_prevention(nal);
        if clean.len() <= H264_MIN_ENCRYPTED_NAL {
            return nal.to_vec();
        }
        let mut cipher = aes::Aes128::new(key.into());
        let mut prev = *iv;
        let mut pos = 32usize;
        while pos < clean.len() {
            if clean.len() - pos > 16 {
                let mut block = [0u8; 16];
                block.copy_from_slice(&clean[pos..pos + 16]);
                for i in 0..16 {
                    block[i] ^= prev[i];
                }
                cipher.encrypt_block_mut((&mut block).into());
                clean[pos..pos + 16].copy_from_slice(&block);
                prev = block;
            }
            pos += 16;
            pos += H264_SKIP.min(clean.len().saturating_sub(pos));
        }
        add_emulation_prevention(&clean)
    }

    #[test]
    fn h264_nal_roundtrip_through_ts() {
        use crate::playlist::crypto::{add_emulation_prevention, remove_emulation_prevention};
        let key = [0x11u8; 16];
        let iv = [0x22u8; 16];
        // 一条足够长的 IDR 片 NAL（type 5），里面故意塞满 `00 00 0X` 模式
        let mut clean = vec![0x65u8];
        for i in 0..900u32 {
            clean.push((i % 256) as u8);
            if i % 97 == 0 {
                clean.extend_from_slice(&[0x00, 0x00, 0x01]);
            }
        }
        // **服务器上的明文 NAL 是"已转义"的形态**：加防竞争后的字节流才是原始数据。
        // （第一次写错就是把未转义的 clean 当成了明文，于是解出来的
        //  "再转义一次"结果和它对不上 —— 实现是对的，测试样例不是。）
        let nal = add_emulation_prevention(&clean);
        let plain_es = {
            let mut v = vec![0x00u8, 0x00, 0x00, 0x01];
            v.extend_from_slice(&nal);
            v
        };
        let enc_nal = encrypt_nal(&nal, &key, &iv);
        assert_ne!(enc_nal, nal, "加密必须改变内容");
        // 解掉防竞争之后，密文同样必须与明文不同（否则等于没加密）
        assert_ne!(
            remove_emulation_prevention(&enc_nal),
            remove_emulation_prevention(&nal)
        );
        let mut enc_es = vec![0x00u8, 0x00, 0x00, 0x01];
        enc_es.extend_from_slice(&enc_nal);

        let ts = build_ts(0x1B, &enc_es, 0x100);
        let out = decrypt_sample_aes_ts(&ts, &key, &iv).expect("解密应成功");

        // 重新拆出 ES，与原始明文逐字节比对
        let (_, streams) = scan_tables(&out).unwrap();
        let pes = reassemble_pes(&out, &streams).unwrap();
        assert_eq!(pes.len(), 1);
        assert_eq!(pes[0].payload, plain_es);
        // 输出仍是合法的 188 字节定长包
        assert_eq!(out.len() % TS_PACKET, 0);
    }

    #[test]
    fn aac_frame_roundtrip() {
        use aes::cipher::{BlockEncryptMut, KeyInit};
        let key = [0x33u8; 16];
        let iv = [0x44u8; 16];
        // 一个 7 字节 ADTS 头 + 200 字节帧体
        let header_len = 7usize;
        let body_len = 200usize;
        let frame_len = header_len + body_len;
        let mut frame = vec![0u8; frame_len];
        frame[0] = 0xFF;
        frame[1] = 0xF1; // 同步字 + protection_absent = 1
        frame[2] = 0x50;
        frame[3] = 0x80 | (((frame_len >> 11) & 0x03) as u8);
        frame[4] = ((frame_len >> 3) & 0xFF) as u8;
        frame[5] = (((frame_len & 0x07) as u8) << 5) | 0x1F;
        frame[6] = 0xFC;
        for (i, b) in frame[header_len..].iter_mut().enumerate() {
            *b = (i % 256) as u8;
        }
        let plain_frame = frame.clone();

        // 加密：头 + 16 字节明文之后，整数个 16 字节块
        let mut cipher = aes::Aes128::new(&key.into());
        let mut prev = iv;
        let mut pos = header_len + AAC_CLEAR_AFTER_HEADER;
        let enc_len = (frame_len - header_len - AAC_CLEAR_AFTER_HEADER) / 16 * 16;
        let mut done = 0usize;
        while done < enc_len {
            let mut block = [0u8; 16];
            block.copy_from_slice(&frame[pos..pos + 16]);
            for i in 0..16 {
                block[i] ^= prev[i];
            }
            cipher.encrypt_block_mut((&mut block).into());
            frame[pos..pos + 16].copy_from_slice(&block);
            prev = block;
            pos += 16;
            done += 16;
        }

        let ts = build_ts(0x0F, &frame, 0x101);
        let out = decrypt_sample_aes_ts(&ts, &key, &iv).expect("AAC 解密应成功");
        let (_, streams) = scan_tables(&out).unwrap();
        let pes = reassemble_pes(&out, &streams).unwrap();
        assert_eq!(pes[0].payload, plain_frame);
    }

    #[test]
    fn unsupported_stream_type_is_rejected() {
        // HEVC（0x24）无法解，必须明确报错而不是写出噪声
        let ts = build_ts(0x24, &[0u8; 64], 0x102);
        let err = decrypt_sample_aes_ts(&ts, &[0u8; 16], &[0u8; 16]).unwrap_err();
        assert!(err.contains("0x24"), "错误里要点明流类型: {err}");
    }

    #[test]
    fn non_ts_input_is_rejected() {
        assert!(decrypt_sample_aes_ts(&[0u8; 100], &[0u8; 16], &[0u8; 16]).is_err());
    }

    #[test]
    fn start_code_scan_finds_both_lengths() {
        let buf = [
            0x00, 0x00, 0x00, 0x01, 0x65, 0x00, 0x00, 0x01, 0x41, 0xFF,
        ];
        let s = start_codes(&buf);
        assert_eq!(s, vec![(0, 4), (5, 3)]);
    }

    #[test]
    fn remux_preserves_pat_and_pmt_and_is_188_aligned() {
        let ts = build_ts(0x1B, &[0xAAu8; 500], 0x100);
        let (pmt_pid, streams) = scan_tables(&ts).unwrap();
        let pes = reassemble_pes(&ts, &streams).unwrap();
        let out = remux_ts(&ts, pmt_pid, &pes).unwrap();
        assert_eq!(out.len() % TS_PACKET, 0);
        // PAT / PMT 原样保留在最前
        assert_eq!(&out[..TS_PACKET], &ts[..TS_PACKET]);
        assert_eq!(&out[TS_PACKET..TS_PACKET * 2], &ts[TS_PACKET..TS_PACKET * 2]);
        // 且重新解析后净荷一致
        let (_, s2) = scan_tables(&out).unwrap();
        let p2 = reassemble_pes(&out, &s2).unwrap();
        assert_eq!(p2[0].payload, pes[0].payload);
    }
}
