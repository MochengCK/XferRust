//! 宿主（桌面端）配置：让独立运行的引擎沿用应用里那套设置。
//!
//! 桌面端把引擎选项交给 electron-store 存在 `<userData>/system.json`（键名就是引擎
//! 选项名）。`xferrust mcp` 在应用没运行时会在进程内自起一个引擎——如果按引擎
//! 默认值跑，下载目录、代理、分片数全和界面上显示的不一样：最典型的是下载目录
//! 退化成进程当前目录（被 AI 客户端拉起时就是它的工作区），文件莫名其妙落在
//! 项目根目录里。
//!
//! 这里按平台惯例找到那份配置，取出下载目录与全局选项。端口/凭据/会话/日志这类
//! "进程身份"不照搬：独立进程与应用的引擎可能同时活着，抢同一个端口或文件只会
//! 让两边都出问题。

use std::path::{Path, PathBuf};

use serde_json::Value;

/// 应用目录名（electron 的 `app.getPath('userData')` 末级）。
const APP_DIR_NAME: &str = "Lerxu";

/// 不照搬到独立进程的键：进程身份（端口/凭据/会话/日志）与监听类端口。
/// `dir` 与 `max-concurrent-downloads` 单独走结构体字段，也不在这里重复。
const BLOCKED_KEYS: [&str; 11] = [
    "rpc-listen-port",
    "rpc-secret",
    "mcp-enabled",
    "mcp-listen-port",
    "save-session",
    "input-file",
    "log",
    "log-level",
    "dht-file-path",
    "dht-file-path6",
    "dht-listen-port",
];

/// 从 `system.json` 读到的、独立进程该沿用的设置。
#[derive(Debug, Clone, PartialEq)]
pub struct HostConfig {
    /// 应用数据目录（`<userData>`）：MCP 会话、互传配置都放在这里。
    pub data_dir: PathBuf,
    /// 下载目录（界面上「默认下载目录」）。
    pub download_dir: Option<PathBuf>,
    /// 最大同时下载数。
    pub max_concurrent: Option<usize>,
    /// 其余全局引擎选项（代理、分片、BT 开关……），已排除 [`BLOCKED_KEYS`]。
    pub options: Vec<(String, String)>,
}

/// 解析应用数据目录：[`explicit`]（`--host-config`）优先，否则按平台惯例。
///
/// 只在目录真实存在时返回：没有装/没配过应用时，调用方应回落到引擎自己的默认值，
/// 而不是凭空造一个目录出来。
pub fn app_dir(explicit: Option<&str>) -> Option<PathBuf> {
    if let Some(dir) = explicit.map(str::trim).filter(|d| !d.is_empty()) {
        let path = PathBuf::from(dir);
        return path.is_dir().then_some(path);
    }
    let path = platform_app_dir()?;
    path.is_dir().then_some(path)
}

/// 平台惯例的 `<userData>`：macOS `~/Library/Application Support/Lerxu`、
/// Windows `%APPDATA%\Lerxu`、Linux `~/.config/Lerxu`。
fn platform_app_dir() -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        let home = xfer_types::home_dir()?;
        Some(
            home.join("Library")
                .join("Application Support")
                .join(APP_DIR_NAME),
        )
    }
    #[cfg(target_os = "windows")]
    {
        std::env::var_os("APPDATA").map(|dir| PathBuf::from(dir).join(APP_DIR_NAME))
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let home = xfer_types::home_dir()?;
        Some(home.join(".config").join(APP_DIR_NAME))
    }
}

/// 读取并解析 `<data_dir>/system.json`；文件缺失或损坏时返回 `None`
/// （独立进程必须能在任何情况下照常启动）。
pub fn load(data_dir: &Path) -> Option<HostConfig> {
    let text = std::fs::read_to_string(data_dir.join("system.json")).ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    Some(parse(data_dir, &value))
}

/// 把 `system.json` 的 JSON 解析成 [`HostConfig`]（纯函数，便于单测）。
pub fn parse(data_dir: &Path, value: &Value) -> HostConfig {
    let obj = value.as_object();
    let download_dir = obj
        .and_then(|o| o.get("dir"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(expand_home);
    let max_concurrent = obj
        .and_then(|o| o.get("max-concurrent-downloads"))
        .and_then(|v| match v {
            Value::Number(n) => n.as_u64(),
            Value::String(s) => s.parse().ok(),
            _ => None,
        })
        .map(|n| (n as usize).max(1));

    let mut options = Vec::new();
    if let Some(obj) = obj {
        for (key, raw) in obj {
            if key == "dir" || key == "max-concurrent-downloads" || BLOCKED_KEYS.contains(&key.as_str())
            {
                continue;
            }
            // 只认标量：对象/数组不是引擎选项，传过去只会污染 getOptions
            let text = match raw {
                Value::String(s) => s.clone(),
                Value::Bool(b) => b.to_string(),
                Value::Number(n) => n.to_string(),
                _ => continue,
            };
            options.push((key.clone(), text));
        }
    }
    options.sort_by(|a, b| a.0.cmp(&b.0));

    HostConfig {
        data_dir: data_dir.to_path_buf(),
        download_dir,
        max_concurrent,
        options,
    }
}

/// `~` / `~/xxx` 展开；其余原样（相对路径留给引擎按工作目录解析）。
fn expand_home(path: &str) -> PathBuf {
    if path == "~" {
        return xfer_types::home_dir().unwrap_or_else(|| PathBuf::from(path));
    }
    if let Some(rest) = path.strip_prefix("~/").or_else(|| path.strip_prefix("~\\")) {
        if let Some(home) = xfer_types::home_dir() {
            return home.join(rest);
        }
    }
    PathBuf::from(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample() -> Value {
        json!({
            "dir": "/Users/demo/Downloads",
            "max-concurrent-downloads": 10,
            "split": 32,
            "all-proxy": "http://127.0.0.1:7897",
            "continue": true,
            "max-overall-download-limit": "0",
            "rpc-listen-port": 18514,
            "rpc-secret": "secret",
            "mcp-listen-port": 16801,
            "log": "/tmp/engine.log",
            "listen-port": 51413,
            "dht-listen-port": 26701,
            "save-session": "/tmp/session.json",
            "user-agent": "Lerxu/1.0"
        })
    }

    #[test]
    fn parses_dir_concurrency_and_options() {
        let cfg = parse(Path::new("/tmp/host"), &sample());
        assert_eq!(cfg.data_dir, PathBuf::from("/tmp/host"));
        assert_eq!(cfg.download_dir, Some(PathBuf::from("/Users/demo/Downloads")));
        assert_eq!(cfg.max_concurrent, Some(10));

        let map: std::collections::HashMap<_, _> = cfg.options.iter().cloned().collect();
        // 普通选项照搬，布尔/数字都转成字符串
        assert_eq!(map.get("split").map(String::as_str), Some("32"));
        assert_eq!(
            map.get("all-proxy").map(String::as_str),
            Some("http://127.0.0.1:7897")
        );
        assert_eq!(map.get("continue").map(String::as_str), Some("true"));
        assert_eq!(map.get("listen-port").map(String::as_str), Some("51413"));
        assert_eq!(map.get("user-agent").map(String::as_str), Some("Lerxu/1.0"));
    }

    #[test]
    fn skips_process_identity_keys() {
        let cfg = parse(Path::new("/tmp/host"), &sample());
        let keys: Vec<&str> = cfg.options.iter().map(|(k, _)| k.as_str()).collect();
        for blocked in [
            "dir",
            "max-concurrent-downloads",
            "rpc-listen-port",
            "rpc-secret",
            "mcp-listen-port",
            "log",
            "save-session",
            "dht-listen-port",
        ] {
            assert!(!keys.contains(&blocked), "{blocked} 不该传给独立进程");
        }
    }

    #[test]
    fn expands_home_and_ignores_empty_dir() {
        let cfg = parse(Path::new("/tmp/host"), &json!({"dir": "~/Movies"}));
        let home = xfer_types::home_dir().expect("测试环境应有 home");
        assert_eq!(cfg.download_dir, Some(home.join("Movies")));

        let empty = parse(Path::new("/tmp/host"), &json!({"dir": "   "}));
        assert_eq!(empty.download_dir, None);
    }

    #[test]
    fn tolerates_odd_shapes() {
        // 非对象、缺字段、类型不对都不能 panic
        let cfg = parse(Path::new("/tmp/host"), &json!([1, 2, 3]));
        assert_eq!(cfg.download_dir, None);
        assert!(cfg.options.is_empty());
        let cfg = parse(Path::new("/tmp/host"), &json!({"dir": 42, "split": {"a": 1}}));
        assert_eq!(cfg.download_dir, None);
        assert!(cfg.options.is_empty());
    }
}
