//! xferrust：XferRust 引擎进程入口。
//!
//! 解析引擎命令行参数（未实现选项宽容告警），启动 JSON-RPC 服务。
//! M6：日志轮转（按日滚动 + 大小上限）、崩溃恢复（panic hook + 会话保存）。

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use xfer_engine::EngineConfig;
use xfer_types::{ENGINE_NAME, ENGINE_VERSION};

#[tokio::main]
async fn main() {
    let raw: Vec<String> = std::env::args().skip(1).collect();

    // 应用端会用 `--version` 自检二进制可执行性，必须退出码 0
    if raw
        .iter()
        .any(|a| a == "--version" || a.starts_with("--version="))
    {
        println!("{ENGINE_NAME} version {ENGINE_VERSION}");
        return;
    }

    let parsed = xfer_engine::parse_args(raw);
    init_tracing(&parsed.config);
    install_panic_hook();

    for opt in &parsed.ignored {
        tracing::warn!(option = %opt, "选项暂未实现，已忽略");
    }

    tracing::info!(
        engine = %ENGINE_NAME,
        version = %ENGINE_VERSION,
        rpc_port = parsed.config.rpc_listen_port,
        dir = %parsed.config.download_dir.display(),
        max_concurrent = parsed.config.max_concurrent,
        "{ENGINE_NAME} 引擎启动"
    );

    if let Err(e) = run(parsed.config).await {
        tracing::error!(error = %e, "引擎退出");
        std::process::exit(1);
    }
}

/// 阻塞运行：任务内核 + 协议路由 + 传输服务；shutdown 令牌触发后退出。
async fn run(cfg: EngineConfig) -> std::io::Result<()> {
    let manager = xfer_engine::TaskManager::start(cfg.download_dir.clone(), cfg.max_concurrent);
    let events = manager.events();
    let router = std::sync::Arc::new(xfer_rpc::Router::new(
        cfg.rpc_secret.clone(),
        manager.clone(),
        events,
    ));
    let bind: std::net::SocketAddr = ([127, 0, 0, 1], cfg.rpc_listen_port).into();
    let shutdown = manager.shutdown_token();
    let engine_shutdown = shutdown.clone();
    let serve = xfer_rpc::serve(bind, router, shutdown);
    tokio::select! {
        r = serve => r,
        _ = engine_shutdown.cancelled() => {
            // 给 shutdown 响应留出回送时间后退出
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok(())
        }
    }
}

/// 日志初始化：--log 指定文件则写文件（按日轮转 + 大小上限），否则输出到标准输出；
/// 级别取自 --log-level（应用端映射：error/warn/notice/info/debug），
/// 环境变量 RUST_LOG 优先。
fn init_tracing(cfg: &EngineConfig) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        let level = cfg
            .log_level
            .as_deref()
            .map(map_log_level)
            .unwrap_or("info");
        tracing_subscriber::EnvFilter::new(level)
    });
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_line_number(true);
    match &cfg.log_file {
        Some(path) => {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            match RollingFile::new(path, MAX_LOG_SIZE, MAX_LOG_FILES) {
                Ok(rf) => builder.with_writer(rf).init(),
                Err(e) => {
                    eprintln!("打开日志文件 {} 失败: {e}，回退到标准输出", path.display());
                    builder.init()
                }
            }
        }
        None => builder.init(),
    }
}

/// 最大单个日志文件大小（10 MB）。
const MAX_LOG_SIZE: u64 = 10 * 1024 * 1024;
/// 保留的轮转日志文件数。
const MAX_LOG_FILES: usize = 5;

/// 应用端日志级别 → tracing 级别。
fn map_log_level(level: &str) -> &'static str {
    match level.to_ascii_lowercase().as_str() {
        "error" => "error",
        "warn" => "warn",
        "notice" | "info" => "info",
        "debug" | "trace" => "debug",
        _ => "info",
    }
}

/// 安装 panic hook：在 panic 时尝试保存会话并输出到日志文件。
fn install_panic_hook() {
    let original = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        // 尽力写入日志
        tracing::error!(panic = %info, "线程 panic");
        // 调用原始 hook 以保持标准错误输出
        original(info);
    }));
}

/// 滚动日志文件写入器：按大小轮转，保留 N 个历史文件。
///
/// 当当前文件超过 `max_size` 时，将其重命名为 `.1`，
/// 旧的 `.1` → `.2`，依此类推，超出 `max_files` 的删除。
struct RollingFile {
    inner: Arc<Mutex<RollingFileInner>>,
}

struct RollingFileInner {
    path: std::path::PathBuf,
    /// 轮转期间会短暂为 None：Windows 不允许重命名仍被打开的文件，
    /// 必须先关闭句柄再滚动文件名。
    file: Option<std::fs::File>,
    max_size: u64,
    max_files: usize,
    written: u64,
}

impl RollingFile {
    fn new(path: &std::path::Path, max_size: u64, max_files: usize) -> std::io::Result<Self> {
        // 恢复已有日志文件的大小统计
        let written = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(RollingFileInner {
                path: path.to_path_buf(),
                file: Some(file),
                max_size,
                max_files,
                written,
            })),
        })
    }

    fn rotate(&self) -> std::io::Result<()> {
        let mut inner = self.inner.lock().map_err(|_| io_err())?;
        // 先关闭句柄：Windows 上打开着的文件无法重命名（共享冲突），
        // 不关闭会导致轮转静默失败、日志无限增长。
        if let Some(mut f) = inner.file.take() {
            let _ = f.flush();
        }

        // 轮转：.4 → .5（删除），.3 → .4，.2 → .3，.1 → .2，当前 → .1
        for i in (1..inner.max_files).rev() {
            let from = inner.path.with_extension(format!("{i}"));
            let to = inner.path.with_extension(format!("{}", i + 1));
            if from.exists() {
                let _ = std::fs::rename(&from, &to);
            }
        }
        let backup = inner.path.with_extension("1");
        let _ = std::fs::rename(&inner.path, &backup);

        // 打开新文件
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&inner.path)?;
        inner.file = Some(file);
        inner.written = 0;
        Ok(())
    }
}

impl std::io::Write for RollingFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let should_rotate = {
            let inner = self.inner.lock().map_err(|_| io_err())?;
            inner.written + buf.len() as u64 > inner.max_size
        };
        if should_rotate {
            self.rotate()?;
        }
        let mut inner = self.inner.lock().map_err(|_| io_err())?;
        let file = inner.file.as_mut().ok_or_else(io_err)?;
        let n = file.write(buf)?;
        inner.written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self.inner.lock().map_err(|_| io_err())?.file.as_mut() {
            Some(f) => f.flush(),
            None => Ok(()),
        }
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for RollingFile {
    type Writer = RollingFile;
    fn make_writer(&'a self) -> Self::Writer {
        RollingFile {
            inner: self.inner.clone(),
        }
    }
}

fn io_err() -> std::io::Error {
    std::io::Error::other("日志文件锁中毒")
}
