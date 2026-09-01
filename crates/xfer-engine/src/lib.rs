//! 引擎配置与任务内核。
//!
//! M1：解析进程参数（`--key=value` 形式，未实现的已知选项宽容接受并
//! 告警）、任务管理器（状态机/并发调度/断点续传/事件广播）。
//! 本 crate 不依赖任何 RPC/传输层——协议适配由上层组装。

mod manager;
mod task;

use std::path::PathBuf;

pub use manager::{
    default_session_path, EngineEvent, TaskManager, TrackerSubscription, DEFAULT_MIN_SPLIT_SIZE,
    DEFAULT_SPLIT_CONNECTIONS,
};
pub use task::{status_json_native, Status};

/// 引擎运行配置。
#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub rpc_listen_port: u16,
    pub rpc_secret: Option<String>,
    pub download_dir: PathBuf,
    pub max_concurrent: usize,
    pub log_file: Option<PathBuf>,
    pub log_level: Option<String>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            rpc_listen_port: 6800,
            rpc_secret: None,
            download_dir: PathBuf::from("."),
            max_concurrent: 5,
            log_file: None,
            log_level: None,
        }
    }
}

/// 参数解析结果：配置 + 被忽略的选项（用于启动日志）。
#[derive(Debug)]
pub struct ParsedArgs {
    pub config: EngineConfig,
    pub ignored: Vec<String>,
}

/// 解析引擎命令行（`--key=value` 形式）。
///
/// 不用 clap：应用端（transformConfig）固定产出自带值的 `--k=v` 形式，
/// 且会传入大量尚未实现的引擎选项——手动解析可以精确做到
/// "已知选项生效、已知但未实现的告警、完全未知的告警"，
/// 避免 clap 的严格校验把启动打挂。
pub fn parse_args<I: IntoIterator<Item = String>>(args: I) -> ParsedArgs {
    let mut cfg = EngineConfig::default();
    let mut ignored = Vec::new();
    for arg in args {
        let Some(kv) = arg.strip_prefix("--") else {
            ignored.push(arg);
            continue;
        };
        let (key, value) = match kv.split_once('=') {
            Some((k, v)) => (k, v),
            None => (kv, ""),
        };
        match key {
            "rpc-listen-port" => {
                if let Ok(p) = value.parse() {
                    cfg.rpc_listen_port = p;
                }
            }
            "rpc-secret" => {
                if !value.is_empty() {
                    cfg.rpc_secret = Some(value.to_string());
                }
            }
            "dir" => cfg.download_dir = PathBuf::from(value),
            "max-concurrent-downloads" => {
                if let Ok(n) = value.parse::<usize>() {
                    cfg.max_concurrent = n.max(1);
                }
            }
            "log" => {
                if !value.is_empty() {
                    cfg.log_file = Some(PathBuf::from(value));
                }
            }
            "log-level" => {
                if !value.is_empty() {
                    cfg.log_level = Some(value.to_string());
                }
            }
            _ => ignored.push(format!("--{key}")),
        }
    }
    ParsedArgs {
        config: cfg,
        ignored,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_engine_style_args() {
        let p = parse_args([
            "--rpc-listen-port=21301".to_string(),
            "--rpc-secret=abc".to_string(),
            "--dir=/tmp/dl".to_string(),
            "--max-concurrent-downloads=10".to_string(),
            "--log=/tmp/x.log".to_string(),
            "--log-level=warn".to_string(),
            "--listen-port=21301".to_string(), // 已知但未实现
            "--bt-max-peers=128".to_string(),  // 已知但未实现
            "--enable-dht=true".to_string(),   // 已知但未实现
        ]);
        assert_eq!(p.config.rpc_listen_port, 21301);
        assert_eq!(p.config.rpc_secret.as_deref(), Some("abc"));
        assert_eq!(p.config.download_dir, PathBuf::from("/tmp/dl"));
        assert_eq!(p.config.max_concurrent, 10);
        assert_eq!(p.config.log_file, Some(PathBuf::from("/tmp/x.log")));
        assert_eq!(p.config.log_level.as_deref(), Some("warn"));
        assert_eq!(p.ignored.len(), 3);
    }

    #[test]
    fn defaults_when_empty() {
        let p = parse_args([]);
        assert_eq!(p.config.rpc_listen_port, 6800);
        assert!(p.config.rpc_secret.is_none());
        assert_eq!(p.config.max_concurrent, 5);
        assert!(p.ignored.is_empty());
    }
}
