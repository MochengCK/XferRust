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
    /// MCP（Model Context Protocol）服务监听端口；None = 不启用。
    ///
    /// 与 JSON-RPC 分开端口：MCP 面向本机 AI Agent（只监听回环地址），
    /// 鉴权复用 `rpc_secret`，无需另配一套凭据。
    pub mcp_listen_port: Option<u16>,
    pub download_dir: PathBuf,
    pub max_concurrent: usize,
    pub log_file: Option<PathBuf>,
    pub log_level: Option<String>,
    /// 会话文件路径（Some 时开启持久化：启动恢复 + 状态转移自动落盘）。
    pub session: Option<PathBuf>,
    /// 引擎数据目录（设备身份、局域网任务表等本地状态）。
    ///
    /// None = 由启动方推导：会话文件所在目录，否则 `~/.xfer`。
    pub data_dir: Option<PathBuf>,
    /// 命令行携带的运行时全局选项（--key=value），启动时注入
    /// global_options，与 RPC engine.changeOptions 同一存储。
    pub initial_options: Vec<(String, String)>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            rpc_listen_port: 6800,
            rpc_secret: None,
            mcp_listen_port: None,
            download_dir: PathBuf::from("."),
            max_concurrent: 5,
            log_file: None,
            log_level: None,
            session: None,
            data_dir: None,
            initial_options: Vec::new(),
        }
    }
}

/// 参数解析结果：配置 + 被忽略的参数（仅裸位置参数，用于启动日志）。
#[derive(Debug)]
pub struct ParsedArgs {
    pub config: EngineConfig,
    pub ignored: Vec<String>,
}

/// 解析引擎命令行（`--key=value` 形式）。
///
/// 不用 clap：应用端（transformConfig）固定产出自带值的 `--k=v` 形式，
/// 且会传入大量尚未实现的引擎选项——手动解析可以精确做到
/// "已知选项生效、未知选项作为外部默认参数宽容接受"，
/// 避免 clap 的严格校验把启动打挂。
///
/// 外部默认参数语义：任何 `--key=value` 都会被接受并存入 global_options
/// （与 `engine.changeOptions` 同一存储，`engine.getOptions` 可读回）——
/// 引擎已实现的键启动即生效，暂未实现的键仅作为默认值存储、不影响运行，
/// 由调用方决定语义。仅裸位置参数（非 `--` 开头）视为无效参数告警忽略。
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
            // MCP 服务：0 或空值表示关闭（应用侧用同一个键控制开关）。
            "mcp-listen-port" => {
                cfg.mcp_listen_port = value.parse::<u16>().ok().filter(|p| *p > 0);
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
            // 会话持久化：启动时若文件存在则恢复任务与设置，运行中自动落盘。
            // `input-file` 为兼容桌面端/aria2 风格启动参数的别名（同一路径
            // 兼作恢复与保存；save-session 显式给出时优先）。
            "save-session" => {
                if !value.is_empty() {
                    cfg.session = Some(PathBuf::from(value));
                }
            }
            "input-file" => {
                if !value.is_empty() {
                    cfg.session.get_or_insert_with(|| PathBuf::from(value));
                }
            }
            // 引擎数据目录：设备身份、局域网任务表等本地状态。
            // 不传时由启动方按会话文件目录 / `~/.xfer` 推导。
            "data-dir" => {
                if !value.is_empty() {
                    cfg.data_dir = Some(PathBuf::from(value));
                }
            }
            // 应用侧开关 → 引擎运行时选项映射：
            // UPnP/NAT-PMP 端口映射、uTP 传输开关
            "enable-upnp" | "enable-nat-pmp" => {
                let on = value != "false" && value != "0";
                cfg.initial_options
                    .push(("bt-port-mapping".into(), on.to_string()));
            }
            "enable-utp" => {
                let on = value != "false" && value != "0";
                cfg.initial_options
                    .push(("bt-protocol".into(), if on { "tcp+utp".into() } else { "tcp".into() }));
            }
            // 其余 `--key=value` 一律作为外部传入的默认参数注入 global_options：
            // 已实现键启动即生效；未实现键仅存储（getOptions 可读回），
            // 不告警、不忽略——外部调用方可自由透传完整配置。
            k => {
                cfg.initial_options.push((k.to_string(), value.to_string()));
            }
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
            "--listen-port=21301".to_string(), // 未实现键 → 作为默认参数宽容接受
            "--bt-max-peers=128".to_string(),  // 运行时选项 → 注入 global_options
            "--enable-dht=true".to_string(),   // 未实现键 → 作为默认参数宽容接受
        ]);
        assert_eq!(p.config.rpc_listen_port, 21301);
        assert_eq!(p.config.rpc_secret.as_deref(), Some("abc"));
        assert_eq!(p.config.download_dir, PathBuf::from("/tmp/dl"));
        assert_eq!(p.config.max_concurrent, 10);
        assert_eq!(p.config.log_file, Some(PathBuf::from("/tmp/x.log")));
        assert_eq!(p.config.log_level.as_deref(), Some("warn"));
        // 未知/未实现选项不再告警忽略，全部作为外部默认参数接受
        assert!(p.ignored.is_empty());
        assert!(p.config.initial_options.contains(&("bt-max-peers".to_string(), "128".to_string())));
        assert!(p.config.initial_options.contains(&("listen-port".to_string(), "21301".to_string())));
        assert!(p.config.initial_options.contains(&("enable-dht".to_string(), "true".to_string())));
    }

    #[test]
    fn positional_args_are_ignored() {
        let p = parse_args(["stray".to_string(), "--bt-seed-mode=true".to_string()]);
        assert_eq!(p.ignored, vec!["stray"]);
        assert!(p.config.initial_options.contains(&("bt-seed-mode".to_string(), "true".to_string())));
    }

    #[test]
    fn parses_data_dir_option() {
        let p = parse_args(["--data-dir=/tmp/xfer-data".to_string()]);
        assert_eq!(p.config.data_dir, Some(PathBuf::from("/tmp/xfer-data")));
        // 已知选项不再作为外部默认参数重复存储
        assert!(p.config.initial_options.is_empty());
        assert_eq!(parse_args([]).config.data_dir, None);
    }

    #[test]
    fn maps_app_toggles() {
        let p = parse_args([
            "--enable-upnp=true".to_string(),
            "--enable-utp=false".to_string(),
            "--enable-nat-pmp=true".to_string(),
        ]);
        assert!(p.ignored.is_empty());
        assert!(p.config.initial_options.contains(&("bt-port-mapping".to_string(), "true".to_string())));
        assert!(p.config.initial_options.contains(&("bt-protocol".to_string(), "tcp".to_string())));
    }

    #[test]
    fn parses_mcp_listen_port() {
        let p = parse_args(["--mcp-listen-port=16801".to_string()]);
        assert_eq!(p.config.mcp_listen_port, Some(16801));
        // 0 与非法值都表示关闭，且不再作为外部默认参数存储
        assert_eq!(parse_args(["--mcp-listen-port=0".to_string()]).config.mcp_listen_port, None);
        assert_eq!(parse_args(["--mcp-listen-port=x".to_string()]).config.mcp_listen_port, None);
        assert_eq!(parse_args([]).config.mcp_listen_port, None);
        assert!(p.config.initial_options.is_empty());
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
