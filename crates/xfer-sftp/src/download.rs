//! 多通道分片下载：1 MiB 片、预分配目标文件、按偏移写盘、位图续传。

use std::io::SeekFrom;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use xfer_types::rate::RateLimiter;

use crate::client::SftpClient;
use crate::ctrl::{self, Bitmap, PIECE_LEN};
use crate::{SftpError, SftpOptions, SftpStats};

/// 每次从网络读一小块再消费限速令牌：1 MiB 整片等待在低限速下会形成
/// 很长的停顿，64 KiB 粒度让限速更平滑、取消也更快。
const READ_CHUNK: usize = 64 * 1024;

/// 下载完成结果。
#[derive(Debug, Clone, Copy)]
pub struct SftpDone {
    pub total: u64,
    /// 从上次控制文件继承的字节数（新任务为 0）。
    pub resumed: u64,
}

/// 多通道下载到 `target`（调用方已完成路径占位与命名冲突处理）。
#[allow(clippy::too_many_arguments)]
pub async fn download(
    client: &SftpClient,
    target: &Path,
    remote: &str,
    total: u64,
    opts: &SftpOptions,
    limiter: Arc<RateLimiter>,
    cancel: &CancellationToken,
    stats: Arc<SftpStats>,
) -> Result<SftpDone, SftpError> {
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent).map_err(|e| SftpError::Io(e.to_string()))?;
    }
    // truncate(false)：文件要在原处续写，长度由下面的 set_len 显式管理
    let existing = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(target)
        .map_err(|e| SftpError::Io(e.to_string()))?;

    // 只有控制文件与远端路径/大小完全匹配时才继承已下片；否则把文件
    // 清空重下。预分配后文件长度恒为 total，位图是唯一进度依据。
    let loaded = ctrl::load(target, total, remote).filter(|_| {
        existing
            .metadata()
            .map(|m| m.len() == total)
            .unwrap_or(false)
    });
    let bitmap = match loaded {
        Some(b) => b,
        None => {
            existing
                .set_len(0)
                .map_err(|e| SftpError::Io(e.to_string()))?;
            existing
                .set_len(total)
                .map_err(|e| SftpError::Io(e.to_string()))?;
            Bitmap::new(total)
        }
    };
    let resumed = bitmap.done_bytes(total);
    stats.completed.store(resumed, Ordering::Relaxed);

    if total == 0 || bitmap.is_complete() {
        finish_local(&existing, target).await?;
        stats.completed.store(total, Ordering::Relaxed);
        return Ok(SftpDone { total, resumed });
    }

    let ctx = Arc::new(CtrlCtx {
        file: Arc::new(existing),
        target: target.to_path_buf(),
        total,
        remote: remote.to_string(),
        bitmap: Mutex::new(bitmap),
    });

    // 开通道：服务器可能限制并发子系统，能开几条用几条。
    let pending = {
        let b = ctx.bitmap.lock().unwrap();
        b.pieces() - b.done_pieces()
    };
    let want = opts.connections.clamp(1, 8).min(pending).max(1);
    let mut sessions = Vec::new();
    let mut files = Vec::new();
    for n in 0..want {
        if cancel.is_cancelled() {
            return Err(SftpError::Cancelled);
        }
        let opened = tokio::select! {
            _ = cancel.cancelled() => return Err(SftpError::Cancelled),
            r = async {
                let s = client.open_session(opts).await?;
                let f = s.open(remote).await.map_err(SftpError::from)?;
                Ok::<_, SftpError>((s, f))
            } => r,
        };
        match opened {
            Ok((s, f)) => {
                sessions.push(s);
                files.push(f);
            }
            Err(e) if files.is_empty() => return Err(e),
            Err(e) => {
                tracing::warn!(
                    opened = files.len(),
                    wanted = want,
                    error = %e,
                    "SFTP 服务器拒绝更多并发通道，按已开通道继续"
                );
                break;
            }
        }
        if n + 1 < want {
            // 留一个极短的窗口给服务器接受上一条通道，避免瞬间打满。
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    stats
        .connections
        .store(files.len() as u64, Ordering::Relaxed);

    let local = cancel.child_token();
    let next = Arc::new(AtomicUsize::new(0));
    let mut workers = JoinSet::new();
    for file in files {
        workers.spawn(worker(
            file,
            ctx.clone(),
            next.clone(),
            stats.clone(),
            limiter.clone(),
            local.clone(),
            opts.timeout_secs,
        ));
    }

    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut fatal: Option<SftpError> = None;
    loop {
        if workers.is_empty() {
            break;
        }
        tokio::select! {
            _ = cancel.cancelled() => {
                local.cancel();
                workers.abort_all();
                while workers.join_next().await.is_some() {}
                let _ = ctx.save().await;
                stats.connections.store(0, Ordering::Relaxed);
                return Err(SftpError::Cancelled);
            }
            _ = tick.tick() => {
                if let Err(e) = ctx.save().await {
                    tracing::warn!(error = %e, "SFTP 续传控制文件保存失败");
                }
            }
            joined = workers.join_next() => {
                match joined {
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(e))) => {
                        if fatal.is_none() { fatal = Some(e); }
                        local.cancel();
                    }
                    Some(Err(e)) => {
                        if fatal.is_none() {
                            fatal = Some(SftpError::Protocol(format!("SFTP 工作线程异常: {e}")));
                        }
                        local.cancel();
                    }
                    None => break,
                }
            }
        }
    }
    stats.connections.store(0, Ordering::Relaxed);

    if let Some(e) = fatal {
        let _ = ctx.save().await;
        return Err(e);
    }

    ctx.finish().await?;
    stats.completed.store(total, Ordering::Relaxed);
    tracing::info!(
        target = %target.display(),
        total,
        resumed,
        channels = sessions.len(),
        "SFTP 下载完成"
    );
    Ok(SftpDone { total, resumed })
}

struct CtrlCtx {
    file: Arc<std::fs::File>,
    target: std::path::PathBuf,
    total: u64,
    remote: String,
    bitmap: Mutex<Bitmap>,
}

impl CtrlCtx {
    /// 先拍位图快照，再把数据 fsync 落盘，最后写控制文件——控制文件里的
    /// 片在写它之前已经同步完成，崩溃恢复不会把空洞当已下载。
    async fn save(&self) -> Result<(), SftpError> {
        let bitmap = self.bitmap.lock().unwrap().clone();
        let file = self.file.clone();
        let target = self.target.clone();
        let remote = self.remote.clone();
        let total = self.total;
        tokio::task::spawn_blocking(move || {
            file.sync_data().map_err(|e| SftpError::Io(e.to_string()))?;
            ctrl::save(&target, total, &remote, &bitmap)
        })
        .await
        .map_err(|e| SftpError::Io(format!("控制文件保存任务异常: {e}")))?
    }

    async fn finish(&self) -> Result<(), SftpError> {
        let file = self.file.clone();
        let target = self.target.clone();
        tokio::task::spawn_blocking(move || {
            file.sync_all().map_err(|e| SftpError::Io(e.to_string()))?;
            ctrl::remove(&target);
            Ok::<(), SftpError>(())
        })
        .await
        .map_err(|e| SftpError::Io(format!("收尾任务异常: {e}")))?
    }
}

async fn worker(
    mut file: russh_sftp::client::fs::File,
    ctx: Arc<CtrlCtx>,
    next: Arc<AtomicUsize>,
    stats: Arc<SftpStats>,
    limiter: Arc<RateLimiter>,
    local: CancellationToken,
    timeout_secs: u64,
) -> Result<(), SftpError> {
    let pieces = ctx.bitmap.lock().unwrap().pieces();
    // 读取单片的硬上限：服务器卡住时靠它把任务转成可重试的超时，
    // 而不是无限等待（大文件整体没有总超时）。
    let piece_timeout = Duration::from_secs(timeout_secs.clamp(10, 300) * 4);
    loop {
        if local.is_cancelled() {
            return Err(SftpError::Cancelled);
        }
        let idx = next.fetch_add(1, Ordering::Relaxed);
        if idx >= pieces {
            break;
        }
        if ctx.bitmap.lock().unwrap().is_done(idx) {
            continue;
        }
        let offset = idx as u64 * PIECE_LEN;
        let len = (ctx.total - offset).min(PIECE_LEN) as usize;
        let mut buf = vec![0u8; len];
        let read = async {
            file.seek(SeekFrom::Start(offset))
                .await
                .map_err(map_read_io)?;
            let mut filled = 0usize;
            while filled < len {
                if local.is_cancelled() {
                    return Err(SftpError::Cancelled);
                }
                let chunk = (len - filled).min(READ_CHUNK);
                if limiter.rate() > 0 {
                    tokio::select! {
                        _ = local.cancelled() => return Err(SftpError::Cancelled),
                        _ = limiter.acquire(chunk) => {}
                    }
                }
                let n = tokio::select! {
                    _ = local.cancelled() => return Err(SftpError::Cancelled),
                    r = file.read(&mut buf[filled..filled + chunk]) => r.map_err(map_read_io)?,
                };
                if n == 0 {
                    return Err(SftpError::ShortRead);
                }
                filled += n;
            }
            Ok::<(), SftpError>(())
        };
        tokio::select! {
            _ = local.cancelled() => return Err(SftpError::Cancelled),
            r = tokio::time::timeout(piece_timeout, read) => {
                match r {
                    Err(_) => return Err(SftpError::Timeout),
                    Ok(r) => r?,
                }
            }
        }

        let f = ctx.file.clone();
        tokio::task::spawn_blocking(move || write_all_at(&f, offset, &buf))
            .await
            .map_err(|e| SftpError::Io(format!("写盘任务异常: {e}")))??;

        let newly_done = ctx.bitmap.lock().unwrap().mark(idx);
        if newly_done {
            stats.completed.fetch_add(len as u64, Ordering::Relaxed);
            stats.received.fetch_add(len as u64, Ordering::Relaxed);
        }
    }
    Ok(())
}

async fn finish_local(file: &std::fs::File, target: &Path) -> Result<(), SftpError> {
    let f = file.try_clone().map_err(|e| SftpError::Io(e.to_string()))?;
    let target = target.to_path_buf();
    tokio::task::spawn_blocking(move || {
        f.sync_all().map_err(|e| SftpError::Io(e.to_string()))?;
        ctrl::remove(&target);
        Ok::<(), SftpError>(())
    })
    .await
    .map_err(|e| SftpError::Io(format!("收尾任务异常: {e}")))?
}

fn map_read_io(e: std::io::Error) -> SftpError {
    if e.kind() == std::io::ErrorKind::TimedOut {
        SftpError::Timeout
    } else {
        SftpError::Connect(e.to_string())
    }
}

#[cfg(unix)]
fn write_all_at(file: &std::fs::File, offset: u64, data: &[u8]) -> Result<(), SftpError> {
    use std::os::unix::fs::FileExt;
    let mut pos = 0usize;
    while pos < data.len() {
        let n = file
            .write_at(&data[pos..], offset + pos as u64)
            .map_err(|e| SftpError::Io(e.to_string()))?;
        if n == 0 {
            return Err(SftpError::Io("写入 0 字节".into()));
        }
        pos += n;
    }
    Ok(())
}

#[cfg(windows)]
fn write_all_at(file: &std::fs::File, offset: u64, data: &[u8]) -> Result<(), SftpError> {
    use std::os::windows::fs::FileExt;
    let mut pos = 0usize;
    while pos < data.len() {
        let n = file
            .seek_write(&data[pos..], offset + pos as u64)
            .map_err(|e| SftpError::Io(e.to_string()))?;
        if n == 0 {
            return Err(SftpError::Io("写入 0 字节".into()));
        }
        pos += n;
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn write_all_at(file: &std::fs::File, offset: u64, data: &[u8]) -> Result<(), SftpError> {
    use std::io::{Seek, SeekFrom, Write};
    let mut f = file.try_clone().map_err(|e| SftpError::Io(e.to_string()))?;
    f.seek(SeekFrom::Start(offset))
        .map_err(|e| SftpError::Io(e.to_string()))?;
    f.write_all(data).map_err(|e| SftpError::Io(e.to_string()))
}
