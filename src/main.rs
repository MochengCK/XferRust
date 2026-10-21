//! xferrust：XferRust 引擎进程入口。
//!
//! 解析引擎命令行参数（未实现选项宽容告警），启动 JSON-RPC 服务。
//! M6：日志轮转（按日滚动 + 大小上限）、崩溃恢复（panic hook + 会话保存）。

use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
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

    // `xferrust mcp`：stdio ↔ HTTP 转发，供只支持 stdio 的 MCP 客户端调用
    // 已在运行的本机引擎（不新起引擎、不碰任务）。
    if raw.first().map(String::as_str) == Some("mcp") {
        std::process::exit(mcp_stdio(&raw[1..]).await);
    }

    let parsed = xfer_engine::parse_args(raw);
    init_tracing(&parsed.config);
    install_panic_hook();

    for opt in &parsed.ignored {
        tracing::warn!(option = %opt, "无效参数，已忽略");
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
    // 传入 --save-session（或兼容别名 --input-file）时开启会话持久化：
    // 启动恢复上次任务（waiting/active 断点续传、终态进历史），状态转移自动落盘
    let manager = match &cfg.session {
        Some(path) => xfer_engine::TaskManager::start_with_session(
            Some(cfg.download_dir.clone()),
            Some(cfg.max_concurrent),
            path.clone(),
        ),
        None => xfer_engine::TaskManager::start(cfg.download_dir.clone(), cfg.max_concurrent),
    };
    // 命令行运行时选项（split / bt-* / enable-upnp 映射等）注入全局配置。
    // 放在 start_with_session 之后：应用端每次启动都会携带最新配置，
    // CLI 传值应覆盖会话恢复的旧值；Android 等无 CLI 场景不受影响。
    manager.set_initial_options(&cfg.initial_options);
    let events = manager.events();

    // 局域网传输：按下载目录（接收落盘）+ 数据目录（设备身份/任务表）拉起。
    // 启动失败（端口/权限等）只告警，不影响引擎本身可用。
    let data_dir = lan_data_dir(&cfg);
    let lan = match xfer_lan::LanController::start(cfg.download_dir.clone(), data_dir.clone()).await {
        Ok(lan) => {
            let status = lan.status();
            tracing::info!(
                enabled = status.enabled,
                running = status.running,
                device_id = %status.device_id,
                device_name = %status.device_name,
                recv_dir = %status.recv_dir,
                data_dir = %data_dir.display(),
                "局域网互传控制器就绪"
            );
            Some(lan)
        }
        Err(e) => {
            tracing::warn!(error = %e, "局域网互传控制器启动失败，本次引擎不提供互传设置");
            None
        }
    };

    let router = std::sync::Arc::new(match lan.clone() {
        Some(lan) => xfer_rpc::Router::with_lan(
            cfg.rpc_secret.clone(),
            manager.clone(),
            events,
            lan,
        ),
        None => xfer_rpc::Router::new(cfg.rpc_secret.clone(), manager.clone(), events),
    });
    let bind: std::net::SocketAddr = ([127, 0, 0, 1], cfg.rpc_listen_port).into();
    let shutdown = manager.shutdown_token();
    let engine_shutdown = shutdown.clone();

    // MCP：面向本机 AI Agent 的接口，单独端口；未配置时不监听。
    // 鉴权直接复用 rpc-secret（MCP 服务器在进程内调 Router，同样会带 token）。
    let mcp_task = match cfg.mcp_listen_port {
        Some(port) => {
            let server = Arc::new(xfer_mcp::McpServer::new(
                router.clone(),
                cfg.rpc_secret.clone(),
            ));
            let bind: std::net::SocketAddr = ([127, 0, 0, 1], port).into();
            let token = shutdown.clone();
            Some(tokio::spawn(async move {
                if let Err(e) = xfer_mcp::serve(bind, server, token).await {
                    tracing::error!(error = %e, "MCP 服务退出");
                }
            }))
        }
        None => None,
    };

    let serve = xfer_rpc::serve(bind, router, shutdown);
    let result = tokio::select! {
        r = serve => r,
        _ = engine_shutdown.cancelled() => {
            // 给 shutdown 响应留出回送时间后退出
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok(())
        }
    };
    if let Some(task) = mcp_task {
        task.abort();
    }
    if let Some(lan) = lan {
        lan.stop().await;
    }
    result
}

/// `xferrust mcp`：以 stdio 提供 MCP 服务，供只支持 stdio 的客户端
/// （Claude Desktop、Cursor 等）直接拉起。
///
/// 两种工作方式，对客户端完全透明：
/// 1. 本机已有引擎在提供 MCP（应用正在运行）→ **转发**给它，任务与界面共享；
/// 2. 没有 → 在本进程里**直接起一个引擎**（独立会话与下载目录），
///    因此不需要打开应用也能用，装上应用即可被 AI 调用。
///
/// stdout 只允许出现响应帧（MCP 约定），诊断信息一律走 stderr。
async fn mcp_stdio(args: &[String]) -> i32 {
    let mut port: u16 = 16801;
    let mut token: Option<String> = None;
    let mut dir: Option<PathBuf> = None;
    let mut session: Option<PathBuf> = None;
    let mut host_dir: Option<String> = None;
    for arg in args {
        let Some(kv) = arg.strip_prefix("--") else {
            continue;
        };
        let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
        match k {
            "mcp-listen-port" | "port" => {
                if let Ok(p) = v.parse() {
                    port = p;
                }
            }
            "mcp-token" | "rpc-secret" | "token" => {
                if !v.is_empty() {
                    token = Some(v.to_string());
                }
            }
            "dir" => {
                if !v.is_empty() {
                    dir = Some(PathBuf::from(v));
                }
            }
            "save-session" | "session" => {
                if !v.is_empty() {
                    session = Some(PathBuf::from(v));
                }
            }
            // 应用数据目录（`app.getPath('userData')`）：设置页生成的配置会显式带上，
            // 便携版/自定义安装位置也能找到那份 system.json；不传则按平台惯例找。
            "host-config" | "data-dir" | "host" => {
                if !v.is_empty() {
                    host_dir = Some(v.to_string());
                }
            }
            _ => {}
        }
    }

    // 探测本机是否已有 MCP 端点（应用里的引擎）
    if let Some(client) = probe_client() {
        let health = format!("http://127.0.0.1:{port}/health");
        let alive = client
            .get(&health)
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false);
        if alive {
            eprintln!("[xferrust mcp] 已连接到运行中的引擎（端口 {port}）");
            return relay_stdio(client, format!("http://127.0.0.1:{port}/mcp"), token).await;
        }
    }

    let host = xfer_engine::host_config::app_dir(host_dir.as_deref())
        .and_then(|data_dir| xfer_engine::host_config::load(&data_dir));
    embedded_stdio(dir, session, host, port, token).await
}

/// 短超时的 HTTP 客户端：探测本机端点用，别让客户端等太久。
fn probe_client() -> Option<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_millis(800))
        .build()
        .ok()
}

/// 转发模式：逐行把 stdio 上的请求 POST 给本机 MCP 端点。
async fn relay_stdio(
    client: reqwest::Client,
    url: String,
    token: Option<String>,
) -> i32 {
    use tokio::io::AsyncBufReadExt;

    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    loop {
        let line = match lines.next_line().await {
            Ok(Some(l)) => l,
            Ok(None) => return 0,
            Err(e) => {
                eprintln!("stdin 读取失败: {e}");
                return 1;
            }
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // 出错时也要回一帧错误，否则客户端会一直等这个 id
        let id = serde_json::from_str::<serde_json::Value>(line)
            .ok()
            .and_then(|v| v.get("id").cloned())
            .unwrap_or(serde_json::Value::Null);

        match forward_line(&client, &url, token.as_deref(), line).await {
            Ok(Some(text)) => {
                println!("{text}");
                let _ = std::io::stdout().flush();
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!("{e}");
                reply_error(&id, &e);
            }
        }
    }
}

/// POST 一帧到 MCP 端点：`Ok(Some(text))` 有响应体、`Ok(None)` 是通知（无响应），
/// `Err` 为传输/HTTP 失败（调用方决定回错误帧还是回落本地引擎）。
async fn forward_line(
    client: &reqwest::Client,
    url: &str,
    token: Option<&str>,
    line: &str,
) -> Result<Option<String>, String> {
    let mut req = client
        .post(url)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .body(line.to_string());
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("MCP 请求失败: {e}"))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if text.is_empty() {
        return if status.is_success() {
            Ok(None)
        } else {
            Err(format!("MCP HTTP {status}"))
        };
    }
    Ok(Some(text))
}

/// 独立模式：本进程内直接起引擎并处理 stdio，不依赖应用是否在运行。
///
/// 会话与下载目录跟着**应用的配置**走（`<userData>/system.json`）：AI 下到哪、
/// 用什么代理，必须和界面上显示的一致；会话放在 `<userData>/mcp/session.json`，
/// 应用下次启动时会把它里面的任务并进自己的会话（见
/// `xfer_engine::TaskManager::start_with_session`），所以 AI 下的东西能在应用里看到。
///
/// 应用**中途启动**时本进程立刻让位：暂停全部任务、保存会话、停掉互传服务，
/// 之后把请求转发给应用的 MCP（应用开了的话）。两个引擎同时跑同一批任务会
/// 各写各的文件（断点续传按同一偏移写，直接毁数据），不能让它们并存。
async fn embedded_stdio(
    dir: Option<PathBuf>,
    session: Option<PathBuf>,
    host: Option<xfer_engine::host_config::HostConfig>,
    port: u16,
    token: Option<String>,
) -> i32 {
    use tokio::io::AsyncBufReadExt;

    let download_dir = dir
        .or_else(|| host.as_ref().and_then(|h| h.download_dir.clone()))
        .unwrap_or_else(default_download_dir);
    if let Err(e) = std::fs::create_dir_all(&download_dir) {
        eprintln!("下载目录不可用 {}: {e}", download_dir.display());
        return 1;
    }
    let session_path = session
        .or_else(|| {
            host.as_ref()
                .map(|h| h.data_dir.join("mcp").join("session.json"))
        })
        .unwrap_or_else(default_mcp_session_path);
    if let Some(parent) = session_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let conc = host.as_ref().and_then(|h| h.max_concurrent);
    let manager = xfer_engine::TaskManager::start_with_session(
        Some(download_dir.clone()),
        conc,
        session_path.clone(),
    );
    // 应用的全局选项（代理 / 分片 / BT 开关……）照搬；回环地址额外加进 no-proxy：
    // Agent 常见的"下载本机/内网地址"场景不该被系统代理拦下。
    let mut options = host.as_ref().map(|h| h.options.clone()).unwrap_or_default();
    push_loopback_no_proxy(&mut options);
    manager.set_initial_options(&options);

    // 互传：只读配置、按需启动（发现端口 41234 只能有一个监听者，独立进程先绑走
    // 会让之后启动的应用看不见任何设备）。
    let lan_dir = host
        .as_ref()
        .map(|h| h.data_dir.clone())
        .or_else(|| xfer_types::home_dir().map(|home| home.join(".xfer")));
    let lan = match lan_dir {
        Some(data_dir) => match xfer_lan::LanController::start_deferred(download_dir.clone(), data_dir)
            .await
        {
            Ok(lan) => Some(lan),
            Err(e) => {
                eprintln!("[xferrust mcp] 互传配置读取失败：{e}");
                None
            }
        },
        None => None,
    };

    let router = std::sync::Arc::new(match &lan {
        Some(lan) => xfer_rpc::Router::with_lan(None, manager.clone(), manager.events(), lan.clone()),
        None => xfer_rpc::Router::new(None, manager.clone(), manager.events()),
    });
    let server = xfer_mcp::McpServer::new(router, None);
    eprintln!(
        "[xferrust mcp] 独立模式：下载目录 {}，会话 {}{}",
        download_dir.display(),
        session_path.display(),
        if host.is_some() { "（沿用应用配置）" } else { "" }
    );

    // 让位状态：`relay` 有值 = 应用已接手，请求直接转过去；`handed_over` 只表示
    // "应用已在跑"，用来拦住那些会被应用看不见的新下载。
    let relay: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let handed_over = Arc::new(AtomicBool::new(false));
    if let Some(host) = host.clone() {
        // 没写端口时按应用默认值探测：刚装好的 system.json 可能只有被改过的键。
        let rpc_port = host.rpc_port.unwrap_or(16800);
        {
            let manager = manager.clone();
            let lan = lan.clone();
            let relay = relay.clone();
            let handed_over = handed_over.clone();
            let mcp_port = host.mcp_port.unwrap_or(port);
            let mcp_enabled = host.mcp_enabled;
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(400)).await;
                    if !tcp_alive(rpc_port).await {
                        continue;
                    }
                    // 应用起来了：先停手，再谈接手
                    let paused = manager.pause_all();
                    let _ = manager.save_session();
                    if let Some(lan) = &lan {
                        // 让出 UDP 41234，应用那边的互传才能发现设备
                        lan.stop().await;
                    }
                    handed_over.store(true, Ordering::SeqCst);
                    let note = if mcp_enabled && mcp_health_ok(mcp_port).await {
                        let url = format!("http://127.0.0.1:{mcp_port}/mcp");
                        *relay.lock().unwrap() = Some(url.clone());
                        format!("，后续请求转给应用（{url}）")
                    } else {
                        "，应用未启用 MCP：后续请求仍由本地引擎处理（新下载会被拒绝）".to_string()
                    };
                    eprintln!("[xferrust mcp] 检测到应用已启动：已暂停 {paused} 个任务并保存会话{note}");
                    break;
                }
            });
        }
    }

    let forward_client = reqwest::Client::new();
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    loop {
        let line = match lines.next_line().await {
            Ok(Some(l)) => l,
            Ok(None) => break,
            Err(e) => {
                eprintln!("stdin 读取失败: {e}");
                let _ = manager.save_session();
                return 1;
            }
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // 应用已接手：整帧转发，本地引擎不再碰任务
        let relay_url = { relay.lock().unwrap().clone() };
        if let Some(url) = relay_url {
            match forward_line(&forward_client, &url, token.as_deref(), line).await {
                Ok(Some(text)) => {
                    println!("{text}");
                    let _ = std::io::stdout().flush();
                    continue;
                }
                Ok(None) => continue,
                Err(e) => {
                    eprintln!("[xferrust mcp] 转发失败，改回本地引擎：{e}");
                    *relay.lock().unwrap() = None;
                }
            }
        }
        // 应用在跑但没开 MCP：不能再偷偷起一份下载 —— 那样的任务应用看不见，
        // 两边还会各写一份文件。明确告诉调用方去开 MCP。
        if handed_over.load(Ordering::SeqCst) && relay.lock().unwrap().is_none() {
            if let Some(frame) = reject_task_mutation(line) {
                println!("{frame}");
                let _ = std::io::stdout().flush();
                continue;
            }
        }
        // 真正用到互传服务（发现/任务/发送）时才拉起：平时不占发现端口，也不广播
        // 设备名；只读状态或改配置不需要先起服务（后者自己会按需启动）。
        if needs_lan_service(line) {
            if let Some(lan) = &lan {
                if let Err(e) = lan.ensure_started().await {
                    eprintln!("[xferrust mcp] {e}");
                }
            }
        }
        if let Some(resp) = server.handle(line) {
            println!("{}", serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into()));
            let _ = std::io::stdout().flush();
        }
    }
    let _ = manager.save_session();
    0
}

/// 应用引擎是否已经在监听（RPC 端口连得上就算起来了）。
async fn tcp_alive(port: u16) -> bool {
    matches!(
        tokio::time::timeout(
            Duration::from_millis(250),
            tokio::net::TcpStream::connect(("127.0.0.1", port))
        )
        .await,
        Ok(Ok(_))
    )
}

/// 应用的 MCP 端点是否可用（`/health` 回 2xx）。
async fn mcp_health_ok(port: u16) -> bool {
    let Some(client) = probe_client() else {
        return false;
    };
    client
        .get(format!("http://127.0.0.1:{port}/health"))
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

/// 应用接手但不支持转发时，拦下那些"会让本地引擎重新动任务"的调用。
///
/// 只拦**新建**与**继续**：这两个动作会让本地引擎和已经把任务并过去的应用
/// 同时写同一批文件。暂停/删除/查询都是安全的（本地这边本来就没在跑）。
fn reject_task_mutation(line: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(line).ok()?;
    if value.get("method").and_then(|m| m.as_str()) != Some("tools/call") {
        return None;
    }
    let name = value
        .get("params")
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())?;
    if !matches!(
        name,
        "add_url" | "add_magnet" | "add_torrent" | "resume_task"
    ) {
        return None;
    }
    let id = value.get("id")?.clone();
    Some(
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "content": [{
                    "type": "text",
                    "text": "Lerxu 正在运行，但没有启用 MCP 服务：为避免两个引擎同时处理同一个任务，新建/继续下载已被拦截。请在 设置 → 进阶设置 → MCP 服务 打开开关并重启应用后再试。"
                }],
                "isError": true
            }
        })
        .to_string(),
    )
}

/// 这一帧请求是不是"需要互传服务已在跑"的工具（`lan_status` / `lan_set_config`
/// 不需要：前者只读配置，后者自己会按需启动）。
fn needs_lan_service(line: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(line)
        .ok()
        .and_then(|v| {
            v.get("params")
                .and_then(|p| p.get("name"))
                .and_then(|n| n.as_str())
                .map(|name| {
                    matches!(
                        name,
                        "lan_peers" | "lan_tasks" | "lan_send" | "lan_cancel" | "lan_remove"
                    )
                })
        })
        .unwrap_or(false)
}

/// 把 `localhost` / `127.0.0.1` / `::1` 补进 no-proxy（已有条目保持原样）。
fn push_loopback_no_proxy(options: &mut Vec<(String, String)>) {
    const LOOPBACK: [&str; 3] = ["localhost", "127.0.0.1", "::1"];
    let existing = options
        .iter()
        .find(|(k, _)| k == "no-proxy")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    let mut parts: Vec<String> = existing
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    for host in LOOPBACK {
        if !parts.iter().any(|p| p.eq_ignore_ascii_case(host)) {
            parts.push(host.to_string());
        }
    }
    options.retain(|(k, _)| k != "no-proxy");
    options.push(("no-proxy".to_string(), parts.join(",")));
}

/// 独立模式的默认会话文件：`~/.xfer/mcp/session.json`（与应用会话分开）。
fn default_mcp_session_path() -> PathBuf {
    match xfer_types::home_dir() {
        Some(home) => home.join(".xfer").join("mcp").join("session.json"),
        None => PathBuf::from(".xfer-mcp-session.json"),
    }
}

/// 独立模式的默认下载目录：`~/Downloads`。
fn default_download_dir() -> PathBuf {
    match xfer_types::home_dir() {
        Some(home) => home.join("Downloads"),
        None => PathBuf::from("."),
    }
}

fn reply_error(id: &serde_json::Value, message: &str) {
    let frame = serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": -32603, "message": message },
    });
    println!("{frame}");
    let _ = std::io::stdout().flush();
}

/// 局域网本地状态目录：显式 `--data-dir` 优先；否则跟随会话文件目录
/// （应用端把会话放在 `<userData>/.xfer/`），再退 `~/.xfer`。
fn lan_data_dir(cfg: &EngineConfig) -> std::path::PathBuf {
    if let Some(dir) = &cfg.data_dir {
        return dir.clone();
    }
    if let Some(session) = &cfg.session {
        if let Some(parent) = session.parent() {
            if !parent.as_os_str().is_empty() {
                return parent.to_path_buf();
            }
        }
    }
    match xfer_types::home_dir() {
        Some(home) => home.join(".xfer"),
        None => cfg.download_dir.join(".xfer"),
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
