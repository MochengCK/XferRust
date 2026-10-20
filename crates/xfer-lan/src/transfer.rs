//! 协议第 2 节：TCP 传输（一个文件一条连接，发送端串行）。
//!
//! 接收端：明文 offer 行 → 回 accept（带自己的临时公钥）→ 收加密帧 → 落盘
//! （`.part` → rename）→ 回加密 done 回执。
//! 发送端：connect → 明文 offer → 收 accept → 加密发帧 → 结束帧 → 解 done。
//!
//! 任何一步失败都删掉半截文件，任务转 error——绝不让"看起来下完了"的残缺文件留下。

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use tokio::fs::File;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use crate::crypto::{self, SessionCipher};
use crate::protocol::{Accept, Done, HandshakeReply, Offer, Reject, PROTO, VERSION};
use crate::tasks::{Direction, LanTask, TaskStatus};
use crate::{now_ms, random_hex, LanService};

/// 握手行等待上限。
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
/// 单帧等待上限（防止对端掉线后连接永久挂住）。
const FRAME_TIMEOUT: Duration = Duration::from_secs(60);
/// 连接建立上限。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// 握手行字节上限（协议报文是单行小 JSON）。
const HANDSHAKE_MAX: usize = 8 * 1024;

// ---------------------------------------------------------------------------
// 接收
// ---------------------------------------------------------------------------

pub(crate) async fn accept_loop(svc: Arc<LanService>, listener: TcpListener) {
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, addr)) => {
                        let svc = svc.clone();
                        tokio::spawn(async move {
                            if let Err(e) = receive_one(&svc, stream, addr).await {
                                tracing::debug!(peer = %addr, error = %e, "接收连接结束");
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "接收连接失败");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            _ = svc.stop_notify.notified() => break,
        }
    }
}

async fn receive_one(
    svc: &Arc<LanService>,
    stream: TcpStream,
    addr: SocketAddr,
) -> Result<(), String> {
    let _ = stream.set_nodelay(true);
    let (read_half, mut writer) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // ---- 明文握手：offer ----
    let line = timeout(
        HANDSHAKE_TIMEOUT,
        read_json_line(&mut reader, HANDSHAKE_MAX),
    )
    .await
    .map_err(|_| "等待 offer 超时".to_string())?
    .map_err(|e| format!("读取 offer 失败：{e}"))?;
    let offer: Offer = match serde_json::from_str(&line) {
        Ok(offer) => offer,
        Err(e) => {
            reject(&mut writer, "报文字段非法").await;
            return Err(format!("offer 解析失败：{e}"));
        }
    };
    if offer.proto != PROTO || offer.v != VERSION || offer.kind != "offer" {
        // 兼容性第 4 节：协议标记/版本不符直接丢弃（不回复）
        return Err("不是 lan-transfer v2 offer".to_string());
    }
    if !offer.is_valid() {
        reject(&mut writer, "size 非法").await;
        return Err(format!("size 非法：{}", offer.size));
    }
    let peer_key = match decode_public_key(&offer.pk) {
        Some(key) => key,
        None => {
            reject(&mut writer, "公钥非法").await;
            return Err("对端公钥非法".to_string());
        }
    };

    let name = sanitize_name(&offer.name);
    // ---- 落盘准备：先建 .part，建不出来就拒绝（不要先 accept 再失败）----
    let (part_path, reserved_final) = reserve_pair(svc.recv_dir(), &name);
    let mut file = match File::create(&part_path).await {
        Ok(file) => file,
        Err(e) => {
            reject(&mut writer, "接收端无法创建文件").await;
            return Err(format!("创建接收文件 {} 失败：{e}", part_path.display()));
        }
    };

    // ---- accept：带自己的临时公钥 ----
    let own_key = crypto::new_secret_key();
    let own_pk = base64::engine::general_purpose::STANDARD.encode(crypto::encode_public(&own_key));
    if let Err(e) = write_line(&mut writer, &Accept::new(own_pk)).await {
        let _ = tokio::fs::remove_file(&part_path).await;
        return Err(format!("发送 accept 失败：{e}"));
    }
    let crypto::SessionKeys { cipher, sas } = crypto::establish(&own_key, &peer_key);

    // ---- 建 in 任务（接受后立刻建；收到第一个字节转 active）----
    let task_id = unique_task_id(svc, &offer.id);
    let peer_name = if offer.sender_name.trim().is_empty() {
        addr.ip().to_string()
    } else {
        offer.sender_name.trim().chars().take(64).collect()
    };
    svc.add_task(LanTask {
        id: task_id.clone(),
        direction: Direction::In,
        name: name.clone(),
        size: offer.size,
        transferred: 0,
        status: TaskStatus::Waiting,
        peer_name,
        peer_address: addr.ip().to_string(),
        error: String::new(),
        sas,
        created_at: now_ms(),
        finished_at: None,
        path: None,
        peer_id: if offer.sender_id.trim().is_empty() {
            None
        } else {
            Some(offer.sender_id.trim().to_string())
        },
        source_path: None,
    });

    // ---- 收加密帧 ----
    let received = receive_frames(svc, &task_id, offer.size, &cipher, &mut reader, &mut file).await;
    match received {
        Ok(total) => {
            if let Err(e) = file.flush().await {
                drop(file);
                return abort_incoming(
                    svc,
                    &mut writer,
                    &cipher,
                    &task_id,
                    &part_path,
                    format!("写入文件失败：{e}"),
                )
                .await;
            }
            drop(file); // Windows：文件句柄不关无法 rename
                        // 预留名被并发任务抢走时再让一次（重名一律追加序号，绝不覆盖）
            let final_path = if reserved_final.exists() {
                unique_final(svc.recv_dir(), &name)
            } else {
                reserved_final
            };
            match tokio::fs::rename(&part_path, &final_path).await {
                Ok(()) => {
                    svc.complete_task(&task_id, Some(final_path.to_string_lossy().to_string()));
                    let payload = serde_json::to_vec(&Done::success()).unwrap_or_default();
                    let _ = crypto::write_frame(&mut writer, &cipher, &payload).await;
                    let _ = writer.flush().await;
                    tracing::info!(
                        peer = %addr,
                        path = %final_path.display(),
                        bytes = total,
                        "局域网接收完成"
                    );
                    Ok(())
                }
                Err(e) => {
                    abort_incoming(
                        svc,
                        &mut writer,
                        &cipher,
                        &task_id,
                        &part_path,
                        format!("落盘失败：{e}"),
                    )
                    .await
                }
            }
        }
        Err(e) => {
            drop(file);
            abort_incoming(svc, &mut writer, &cipher, &task_id, &part_path, e).await
        }
    }
}

/// 接收失败收尾：删半截文件、任务转 error、尽力回一条加密失败回执。
async fn abort_incoming<W>(
    svc: &LanService,
    writer: &mut W,
    cipher: &SessionCipher,
    task_id: &str,
    part_path: &Path,
    error: String,
) -> Result<(), String>
where
    W: AsyncWrite + Unpin,
{
    let _ = tokio::fs::remove_file(part_path).await;
    svc.fail_task(task_id, &error);
    let payload = serde_json::to_vec(&Done::failure(error.clone())).unwrap_or_default();
    let _ = crypto::write_frame(writer, cipher, &payload).await;
    let _ = writer.flush().await;
    Err(error)
}

async fn receive_frames(
    svc: &Arc<LanService>,
    task_id: &str,
    size: i64,
    cipher: &SessionCipher,
    reader: &mut BufReader<OwnedReadHalf>,
    file: &mut File,
) -> Result<i64, String> {
    let mut total: i64 = 0;
    loop {
        if !svc.task_alive(task_id) {
            return Err("cancelled".to_string());
        }
        let frame = timeout(FRAME_TIMEOUT, crypto::read_frame(reader))
            .await
            .map_err(|_| "接收超时".to_string())?
            .map_err(|e| format!("读取加密帧失败：{e}"))?;
        let Some(payload) = frame else {
            break; // L = 0：结束帧
        };
        let plain = cipher
            .open(&payload)
            .map_err(|e| format!("解密失败：{e}"))?;
        if plain.is_empty() {
            continue;
        }
        total += plain.len() as i64;
        if size >= 0 && total > size {
            return Err(format!("接收数据超出声明大小（{} > {size}）", total));
        }
        file.write_all(&plain)
            .await
            .map_err(|e| format!("写入文件失败：{e}"))?;
        // 收到第一个字节起 active
        svc.progress(task_id, total, true);
    }
    if size >= 0 && total != size {
        return Err(format!("接收不完整（{total}/{size} 字节）"));
    }
    Ok(total)
}

// ---------------------------------------------------------------------------
// 发送
// ---------------------------------------------------------------------------

pub(crate) async fn sender_loop(svc: Arc<LanService>) {
    loop {
        if svc.is_stopped() {
            break;
        }
        let Some(task) = svc.next_waiting_out() else {
            // 队列空：等新任务 / 停止；2s 兜底重扫（对端可能刚上线）
            tokio::select! {
                _ = svc.wake.notified() => continue,
                _ = svc.stop_notify.notified() => break,
                _ = tokio::time::sleep(Duration::from_secs(2)) => continue,
            }
        };
        match send_one(&svc, &task).await {
            Ok(()) => svc.complete_task(&task.id, None),
            Err(e) => svc.fail_task(&task.id, e),
        }
    }
}

async fn send_one(svc: &Arc<LanService>, task: &LanTask) -> Result<(), String> {
    let peer_id = task
        .peer_id
        .as_deref()
        .ok_or_else(|| "任务缺少对方设备 id".to_string())?;
    let target = svc
        .peer_target(peer_id)
        .ok_or_else(|| format!("设备 {} 已离线", task.peer_name))?;
    let source = task
        .source_path
        .as_deref()
        .ok_or_else(|| "任务缺少源文件路径".to_string())?;

    let mut file = File::open(source)
        .await
        .map_err(|e| format!("打开源文件失败：{e}"))?;
    let meta = file
        .metadata()
        .await
        .map_err(|e| format!("读取源文件信息失败：{e}"))?;
    if !meta.is_file() {
        return Err("源文件已不是普通文件".to_string());
    }
    let size = meta.len() as i64;
    svc.update_size(&task.id, size);

    let mut stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(target))
        .await
        .map_err(|_| format!("连接 {} 超时", task.peer_name))?
        .map_err(|e| format!("连接 {} 失败：{e}", task.peer_name))?;
    let _ = stream.set_nodelay(true);

    // ---- 明文 offer ----
    let own_key = crypto::new_secret_key();
    let own_pk = base64::engine::general_purpose::STANDARD.encode(crypto::encode_public(&own_key));
    let offer = serde_json::json!({
        "proto": crate::protocol::PROTO,
        "v": VERSION,
        "type": "offer",
        "id": task.id,
        "name": task.name,
        "size": size,
        "senderId": svc.device_id(),
        "senderName": svc.device_name(),
        "pk": own_pk,
    });
    write_line(&mut stream, &offer)
        .await
        .map_err(|e| format!("发送 offer 失败：{e}"))?;

    let (read_half, mut writer) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let line = timeout(
        HANDSHAKE_TIMEOUT,
        read_json_line(&mut reader, HANDSHAKE_MAX),
    )
    .await
    .map_err(|_| "等待应答超时".to_string())?
    .map_err(|e| format!("读取应答失败：{e}"))?;
    let reply: HandshakeReply =
        serde_json::from_str(&line).map_err(|e| format!("应答不是合法 JSON：{e}"))?;
    let peer_key = match reply.kind.as_str() {
        "accept" => {
            let pk = reply.pk.ok_or_else(|| "accept 缺少公钥".to_string())?;
            decode_public_key(&pk).ok_or_else(|| "对端公钥非法".to_string())?
        }
        "reject" => {
            return Err(reply
                .reason
                .filter(|r| !r.trim().is_empty())
                .unwrap_or_else(|| "对方拒绝接收".to_string()))
        }
        other => return Err(format!("未知握手应答：{other}")),
    };
    let crypto::SessionKeys { cipher, sas } = crypto::establish(&own_key, &peer_key);
    // 发送端在收到 accept 后才能算出 SAS，随任务展示给用户比对
    svc.update_sas(&task.id, sas);

    // ---- 加密发帧 ----
    let mut buffer = vec![0u8; crypto::MAX_PLAINTEXT];
    let mut sent: i64 = 0;
    loop {
        if !svc.task_alive(&task.id) {
            return Err("cancelled".to_string());
        }
        let n = file
            .read(&mut buffer)
            .await
            .map_err(|e| format!("读取源文件失败：{e}"))?;
        if n == 0 {
            break;
        }
        crypto::write_frame(&mut writer, &cipher, &buffer[..n])
            .await
            .map_err(|e| format!("发送数据失败：{e}"))?;
        sent += n as i64;
        svc.progress(&task.id, sent, false);
    }
    crypto::write_eof(&mut writer)
        .await
        .map_err(|e| format!("发送结束帧失败：{e}"))?;
    writer
        .flush()
        .await
        .map_err(|e| format!("刷新连接失败：{e}"))?;

    // ---- 解加密 done 回执 ----
    let frame = timeout(FRAME_TIMEOUT, crypto::read_frame(&mut reader))
        .await
        .map_err(|_| "等待接收回执超时".to_string())?
        .map_err(|e| format!("读取回执失败：{e}"))?;
    let payload = frame.ok_or_else(|| "对方未发完成回执".to_string())?;
    let plain = cipher
        .open(&payload)
        .map_err(|e| format!("回执解密失败：{e}"))?;
    let done: Done = serde_json::from_slice(&plain).map_err(|e| format!("回执格式非法：{e}"))?;
    if !done.ok {
        return Err(done
            .error
            .filter(|e| !e.trim().is_empty())
            .unwrap_or_else(|| "对方接收失败".to_string()));
    }
    tracing::info!(peer = %target, file = %task.name, bytes = sent, "局域网发送完成");
    Ok(())
}

// ---------------------------------------------------------------------------
// 公共小工具
// ---------------------------------------------------------------------------

fn decode_public_key(pk: &str) -> Option<p256::PublicKey> {
    let bytes = base64::engine::general_purpose::STANDARD.decode(pk).ok()?;
    crypto::parse_public(&bytes)
}

async fn write_line<W, T>(writer: &mut W, message: &T) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: serde::Serialize,
{
    let mut bytes = serde_json::to_vec(message).map_err(std::io::Error::other)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await
}

async fn reject<W>(writer: &mut W, reason: &str)
where
    W: AsyncWrite + Unpin,
{
    let _ = write_line(writer, &Reject::new(reason)).await;
}

/// 读一行（不含 `\n`），超长直接失败——避免对端用超长握手行吃内存。
async fn read_json_line<R>(reader: &mut R, limit: usize) -> std::io::Result<String>
where
    R: AsyncBufRead + Unpin,
{
    let mut out = Vec::new();
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "对端在握手阶段断开",
            ));
        }
        if let Some(pos) = chunk.iter().position(|&b| b == b'\n') {
            out.extend_from_slice(&chunk[..pos]);
            reader.consume(pos + 1);
            break;
        }
        let len = chunk.len();
        out.extend_from_slice(chunk);
        reader.consume(len);
        if out.len() > limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "握手报文过长",
            ));
        }
    }
    if out.len() > limit {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "握手报文过长",
        ));
    }
    String::from_utf8(out)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "握手报文不是 UTF-8"))
}

/// 文件名清洗（协议 2.3）：去路径分隔符/控制字符、禁止 `..`、空名用 `unnamed`。
pub(crate) fn sanitize_name(raw: &str) -> String {
    // 纯 "." / ".." 不是合法文件名（目录穿越的最小形态），直接当空名
    let trimmed_raw = raw.trim();
    if trimmed_raw.is_empty() || trimmed_raw == "." || trimmed_raw == ".." {
        return "unnamed".to_string();
    }
    let mut cleaned = String::with_capacity(raw.chars().count().min(256));
    for ch in raw.chars() {
        if ch == '/' || ch == '\\' || ch.is_control() {
            continue;
        }
        // Windows 的不允许字符（同一个引擎也跑 Windows，替换掉比落盘失败好）
        cleaned.push(match ch {
            ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            other => other,
        });
    }
    let cleaned = cleaned.replace("..", "_");
    let cleaned = cleaned.trim().trim_matches('.');
    let cleaned: String = cleaned.chars().take(180).collect();
    let cleaned = cleaned.trim_end_matches(|c: char| c == ' ' || c == '.');
    if cleaned.is_empty() {
        "unnamed".to_string()
    } else {
        cleaned.to_string()
    }
}

fn split_ext(name: &str) -> (&str, &str) {
    match name.rfind('.') {
        Some(index) if index > 0 && index + 1 < name.len() => (&name[..index], &name[index..]),
        _ => (name, ""),
    }
}

/// 预留 `<名字>.part` 与最终路径：两者都不冲突才算占位（重名追加 ` (1)`）。
fn reserve_pair(dir: &Path, name: &str) -> (PathBuf, PathBuf) {
    let (stem, ext) = split_ext(name);
    for n in 0u32..10_000 {
        let base = if n == 0 {
            format!("{stem}{ext}")
        } else {
            format!("{stem} ({n}){ext}")
        };
        let final_path = dir.join(&base);
        let part_path = dir.join(format!("{base}.part"));
        if !final_path.exists() && !part_path.exists() {
            return (part_path, final_path);
        }
    }
    let base = format!("{}-{name}", random_hex(4));
    let final_path = dir.join(&base);
    let part_path = dir.join(format!("{base}.part"));
    (part_path, final_path)
}

/// 最终路径（rename 前再确认一次，避免并发同名互相覆盖）。
fn unique_final(dir: &Path, name: &str) -> PathBuf {
    let (stem, ext) = split_ext(name);
    for n in 0u32..10_000 {
        let base = if n == 0 {
            format!("{stem}{ext}")
        } else {
            format!("{stem} ({n}){ext}")
        };
        let candidate = dir.join(&base);
        if !candidate.exists() {
            return candidate;
        }
    }
    dir.join(format!("{}-{name}", random_hex(4)))
}

fn unique_task_id(svc: &LanService, requested: &str) -> String {
    let requested = requested.trim();
    if requested.is_empty() || svc.task_exists(requested) {
        random_hex(16)
    } else {
        requested.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_strips_paths_and_controls() {
        assert_eq!(sanitize_name("../../etc/passwd"), "__etcpasswd");
        assert_eq!(sanitize_name("a\\b/c"), "abc");
        assert_eq!(sanitize_name("bad\u{0}name\n"), "badname");
        assert_eq!(sanitize_name("   "), "unnamed");
        assert_eq!(sanitize_name(".."), "unnamed");
        assert_eq!(sanitize_name("normal.bin"), "normal.bin");
        assert_eq!(sanitize_name("a:b*c?.txt"), "a_b_c_.txt");
    }

    #[test]
    fn duplicate_names_get_suffix() {
        let dir = std::env::temp_dir().join(format!(
            "xfer-lan-paths-{}-{}",
            std::process::id(),
            random_hex(4)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let (part0, final0) = reserve_pair(&dir, "movie.mp4");
        assert_eq!(final0.file_name().unwrap(), "movie.mp4");
        assert_eq!(part0.file_name().unwrap(), "movie.mp4.part");
        std::fs::write(&part0, b"x").unwrap();
        let (part1, final1) = reserve_pair(&dir, "movie.mp4");
        assert_eq!(final1.file_name().unwrap(), "movie (1).mp4");
        assert_eq!(part1.file_name().unwrap(), "movie (1).mp4.part");
        std::fs::write(&final1, b"y").unwrap();
        let (_, final2) = reserve_pair(&dir, "movie.mp4");
        assert_eq!(final2.file_name().unwrap(), "movie (2).mp4");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn unique_final_skips_existing() {
        let dir = std::env::temp_dir().join(format!(
            "xfer-lan-final-{}-{}",
            std::process::id(),
            random_hex(4)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.txt"), b"1").unwrap();
        assert_eq!(
            unique_final(&dir, "a.txt").file_name().unwrap(),
            "a (1).txt"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
