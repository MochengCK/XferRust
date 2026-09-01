//! 任务管理器：状态机、并发调度、断点续传、事件广播。
//!
//! 所有对外方法为同步短临界区（供 RPC 直接调用）；
//! 下载由独立的 tokio 任务驱动，`fill_slots` 按并发槽拉起。

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{broadcast, Notify};
use tokio_util::sync::CancellationToken;
use xfer_bencode::{parse_magnet, parse_torrent};
use xfer_bt::{TorrentConfig, TorrentEngine};
use xfer_types::{Gid, PeerId};

// M6: catch_unwind for async futures
use futures_util::future::FutureExt;

use crate::task::{
    filter_keys, snapshot, status_json, status_json_native, Intent, Status, Task, TaskFailure,
};
use xfer_storage::{existing_len, verify_file_hash, FileSink, HashAlgo};

/// 事件负载：(事件名, gid)，RPC 层拼装线上通知帧。
pub type EngineEvent = (String, String);

/// 内存中保留的停止结果条数上限。
const MAX_RESULTS: usize = 1000;

/// HTTP 分片下载默认参数（前端未配置时的引擎默认）。
/// 连接数取 split 与 max-connection-per-server 的较小者。
/// 导出给 TUI 设置页用于直接显示默认值。
pub const DEFAULT_SPLIT_CONNECTIONS: usize = 16;
pub const DEFAULT_MIN_SPLIT_SIZE: u64 = 4 * 1024 * 1024;

/// BT 预分配连接数默认与上限。
///
/// 与 HTTP 的 `split` **完全独立**：BT 连接的对象是对等节点（peer），
/// 不是同一服务器上的 Range 分片，两者的最优并发度没有可比性。
const DEFAULT_BT_MAX_PEERS: usize = 50;
const MAX_BT_MAX_PEERS: usize = 200;

/// 生成 16 字符 hex ID（用于订阅源标识）。
fn generate_id() -> String {
    let mut buf = [0u8; 8];
    getrandom::fill(&mut buf).expect("系统随机源不可用");
    hex::encode(buf)
}

struct Inner {
    tasks: HashMap<Gid, Arc<Task>>,
    /// 等待队列（队首优先）。
    queue: VecDeque<Gid>,
    active: HashSet<Gid>,
    /// 停止结果（旧→新；查询时倒序输出）。
    stopped_order: Vec<Gid>,
    stopped_total: u64,
    max_concurrent: usize,
    /// 活任务（waiting/active/paused）占用的目标文件路径。
    claims: HashSet<PathBuf>,
    global_options: HashMap<String, String>,
    download_dir: PathBuf,
    /// 会话文件路径（Some 时开启持久化）。
    session: Option<PathBuf>,
    /// 全局 BT tracker 列表（设置页配置，所有 BT 任务自动注入）。
    global_trackers: Vec<String>,
    /// Tracker 订阅源（远程 URL，定期获取更新 tracker 列表）。
    tracker_subscriptions: Vec<TrackerSubscription>,
    /// 是否启用 Tracker 订阅自动更新（默认 true）。
    auto_update_trackers: bool,
}

/// Tracker 订阅源：远程 URL 返回纯文本（每行一个 tracker URL）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TrackerSubscription {
    /// 唯一标识（UUID 短格式）。
    pub id: String,
    /// 订阅名称（用户自定义）。
    pub name: String,
    /// 远程 URL（HTTP/HTTPS），返回纯文本 tracker 列表。
    pub url: String,
    /// 是否启用自动更新。
    pub enabled: bool,
    /// 上次成功更新时间（Unix 秒；0 = 从未）。
    pub last_updated: u64,
    /// 上次更新获取的 tracker 数量。
    pub last_count: usize,
    /// 上次更新错误信息（空 = 成功）。
    pub last_error: String,
}

/// 引擎核心：任务生命周期管理。
pub struct TaskManager {
    inner: Mutex<Inner>,
    /// 活动 BT 引擎注册表（gid → 引擎）：全局限速变更时下发
    /// [`TorrentEngine::set_rate_limits`]，任务结束时移除。
    bt_engines: Mutex<HashMap<Gid, Arc<TorrentEngine>>>,
    /// 调度器唤醒信号。
    kick: Notify,
    events: broadcast::Sender<EngineEvent>,
    client: reqwest::Client,
    /// RPC shutdown 触发的整体退出令牌。
    shutdown_token: CancellationToken,
}

impl TaskManager {
    /// 构造任务管理器（不启动调度器，无 runtime 亲和性要求）。
    pub fn new(download_dir: PathBuf, max_concurrent: usize) -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(256);
        let download_dir = normalize_dir(&download_dir.to_string_lossy());
        Arc::new(Self {
            inner: Mutex::new(Inner {
                tasks: HashMap::new(),
                queue: VecDeque::new(),
                active: HashSet::new(),
                stopped_order: Vec::new(),
                stopped_total: 0,
                max_concurrent: max_concurrent.max(1),
                claims: HashSet::new(),
                global_options: HashMap::new(),
                download_dir,
                session: None,
                global_trackers: Vec::new(),
                tracker_subscriptions: Vec::new(),
                auto_update_trackers: true,
            }),
            bt_engines: Mutex::new(HashMap::new()),
            kick: Notify::new(),
            events: tx,
            client: xfer_http::build_client(),
            shutdown_token: CancellationToken::new(),
        })
    }

    /// 启动调度器循环（须在 tokio runtime 内调用，幂等性由调用方保证）。
    /// M6：同时启动定期会话保存（30s 间隔）。
    pub fn spawn_scheduler(self: &Arc<Self>) {
        let sched = self.clone();
        tokio::spawn(async move { sched.scheduler_loop().await });
        // M6：定期会话保存（防崩溃丢数据）
        let saver = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(30));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                saver.save_session_now();
            }
        });
        // Tracker 订阅源每日自动更新（24h 间隔）
        let sub_refresher = self.clone();
        tokio::spawn(async move {
            // 启动后延迟 10s 执行首次刷新（避免与启动争抢资源）
            tokio::time::sleep(Duration::from_secs(10)).await;
            loop {
                if !sub_refresher.should_auto_refresh() {
                    // 未启用或无订阅源，等 5min 再检查
                    tokio::time::sleep(Duration::from_secs(300)).await;
                    continue;
                }
                // 刷新所有已启用的订阅源
                let _ = sub_refresher.refresh_all_subscriptions().await;
                // 每小时检查一次（24h 内至少刷新一次）
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        });
    }

    /// 构造并启动调度器（须在 tokio runtime 内调用）。
    pub fn start(download_dir: PathBuf, max_concurrent: usize) -> Arc<Self> {
        let mgr = Self::new(download_dir, max_concurrent);
        mgr.spawn_scheduler();
        mgr
    }

    /// 构造并启动带会话持久化的调度器：加载历史任务与设置，
    /// 状态转移时自动写回 `session` 文件。
    ///
    /// - `dir`/`conc` 为 None 时采用会话文件中的设置（再退默认值）；
    ///   显式传入时覆盖会话设置。
    /// - 重启恢复：waiting/active 任务重新入队（磁盘已有部分自动续传），
    ///   paused 恢复暂停态，终态任务进历史列表。
    pub fn start_with_session(
        dir: Option<PathBuf>,
        conc: Option<usize>,
        session: PathBuf,
    ) -> Arc<Self> {
        let v = read_session_file(&session);
        let saved_dir = v
            .as_ref()
            .and_then(|v| v["settings"]["dir"].as_str())
            .map(PathBuf::from);
        let saved_conc = v
            .as_ref()
            .and_then(|v| v["settings"]["max-concurrent-downloads"].as_u64())
            .map(|n| n as usize);
        let cli_dir = dir.clone();
        let cli_conc = conc;
        let dir = dir.or(saved_dir).unwrap_or_else(|| PathBuf::from("."));
        let conc = conc.or(saved_conc).unwrap_or(3).max(1);
        let mgr = Self::new(dir, conc);
        {
            let mut inner = mgr.inner.lock().unwrap();
            inner.session = Some(session);
            // 恢复全局选项（split / max-connection-per-server 等）
            if let Some(opts) = v
                .as_ref()
                .and_then(|v| v["settings"]["options"].as_object())
            {
                for (k, val) in opts {
                    if let Some(s) = val.as_str() {
                        inner.global_options.insert(k.clone(), s.to_string());
                    }
                }
            }
            // options 中的并发/目录是用户在界面里保存的最新设置；顶层数字
            // 字段可能被上次启动的命令行参数（-j/-d，临时覆盖）改写。
            // 命令行未显式指定时以 options map 为准恢复。
            if cli_conc.is_none() {
                if let Some(n) = inner
                    .global_options
                    .get("max-concurrent-downloads")
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    inner.max_concurrent = n.max(1);
                }
            }
            if cli_dir.is_none() {
                if let Some(d) = inner.global_options.get("dir") {
                    if !d.is_empty() {
                        inner.download_dir = normalize_dir(d);
                    }
                }
            }
        }
        // 恢复全局 BT tracker 列表
        if let Some(trackers) = v
            .as_ref()
            .and_then(|v| v["settings"]["btTrackers"].as_array())
        {
            let gt: Vec<String> = trackers
                .iter()
                .filter_map(|t| t.as_str().map(String::from))
                .collect();
            if !gt.is_empty() {
                mgr.inner.lock().unwrap().global_trackers = gt;
            }
        }
        // 恢复 Tracker 订阅源
        if let Some(subs) = v
            .as_ref()
            .and_then(|v| v["settings"]["trackerSubscriptions"].as_array())
        {
            let parsed: Vec<TrackerSubscription> = subs
                .iter()
                .filter_map(|s| serde_json::from_value(s.clone()).ok())
                .collect();
            if !parsed.is_empty() {
                mgr.inner.lock().unwrap().tracker_subscriptions = parsed;
            }
        }
        // 恢复自动更新开关
        if let Some(auto) = v
            .as_ref()
            .and_then(|v| v["settings"]["autoUpdateTrackers"].as_bool())
        {
            mgr.inner.lock().unwrap().auto_update_trackers = auto;
        }
        if let Some(v) = v {
            mgr.restore_tasks(&v);
        }
        mgr.spawn_scheduler();
        mgr
    }

    /// 从会话 JSON 恢复任务（调用方保证已持有 session 配置）。
    fn restore_tasks(&self, v: &Value) {
        let mut inner = self.inner.lock().unwrap();
        let Some(tasks) = v["tasks"].as_array() else {
            return;
        };
        let mut restored = 0usize;
        for t in tasks {
            let Some(gid_s) = t["gid"].as_str() else {
                continue;
            };
            let uris: Vec<String> = t["uris"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|u| u.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            let gid = Gid::from(gid_s);
            let dir = t["dir"]
                .as_str()
                .map(normalize_dir)
                .unwrap_or_else(|| inner.download_dir.clone());
            let out = t["out"].as_str().map(String::from);
            let checksum = t["checksum"].as_str().and_then(parse_checksum_option);
            let task_opts: HashMap<String, String> = t["options"]
                .as_object()
                .map(|o| {
                    o.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default();

            // 尝试恢复 BT torrent 元数据（.torrent 文件任务）
            let bt_meta = t["btTorrentB64"].as_str().and_then(|b64| {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD
                    .decode(b64.trim())
                    .ok()
                    .and_then(|bytes| parse_torrent(&bytes).ok())
                    .map(Arc::new)
            });

            let task = Arc::new(Task::new(gid.clone(), uris, dir, out, checksum, task_opts));
            // 恢复 BT 相关字段
            {
                let bt_trackers: Vec<String> = t["btTrackers"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|u| u.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                if !bt_trackers.is_empty() {
                    *task.bt_trackers.lock().unwrap() = bt_trackers;
                }
                // 恢复 info_hash（hex 编码）
                if let Some(hex) = t["btInfoHash"].as_str() {
                    let mut ih = [0u8; 20];
                    if hex.len() == 40 {
                        for i in 0..20 {
                            ih[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap_or(0);
                        }
                        *task.bt_info_hash.lock().unwrap() = Some(ih);
                    }
                }
            }
            // 恢复 bt_meta（.torrent 文件任务的完整元数据）
            if let Some(meta) = bt_meta {
                *task.bt_meta.lock().unwrap() = Some(meta);
            }
            {
                let mut sh = task.shared.lock().unwrap();
                sh.completed = t["completedLength"].as_u64().unwrap_or(0);
                sh.total_len = t["totalLength"].as_u64();
                sh.file_len = sh.total_len.unwrap_or(0);
                // 恢复累计活跃时长（暂停任务重启后已用时不丢）
                sh.active_ms = t["elapsedMs"].as_u64().unwrap_or(0);
                sh.active_since = None;
                sh.path = t["path"].as_str().map(PathBuf::from);
                sh.filename = t["path"]
                    .as_str()
                    .map(|p| p.rsplit(['/', '\\']).next().unwrap_or("").to_string());
                sh.error_code = t["errorCode"].as_i64().unwrap_or(0);
                sh.error_message = t["errorMessage"].as_str().unwrap_or("").to_string();
                match t["status"].as_str().unwrap_or("waiting") {
                    "paused" => sh.status = Status::Paused,
                    "complete" => sh.status = Status::Complete,
                    "error" => sh.status = Status::Error,
                    "removed" => sh.status = Status::Removed,
                    _ => {} // waiting / active → Waiting（active 重启后重新下载）
                }
            }
            let status = task.status();
            match status {
                Status::Waiting => {
                    // 磁盘已有部分 → 回填进度显示（下载启动时 probe 再校准）
                    // 先 clone path 再释放锁，避免在锁内重复 lock 导致死锁
                    let path_opt = task.shared.lock().unwrap().path.clone();
                    if let Some(p) = &path_opt {
                        let el = existing_len(p);
                        if el > 0 {
                            task.shared.lock().unwrap().completed = el;
                            task.completed_atomic.store(el, Ordering::Relaxed);
                        }
                    }
                    inner.queue.push_back(gid.clone());
                }
                Status::Paused => {}
                _ => inner.stopped_order.push(gid.clone()),
            }
            inner.tasks.insert(gid, task);
            restored += 1;
        }
        inner.stopped_total = inner
            .stopped_total
            .max(v["stoppedTotal"].as_u64().unwrap_or(0))
            .max(inner.stopped_order.len() as u64);
        while inner.stopped_order.len() > MAX_RESULTS {
            let old = inner.stopped_order.remove(0);
            inner.tasks.remove(&old);
        }
        tracing::info!(count = restored, "会话任务恢复完成");
        drop(inner);
        self.kick();
    }

    /// 序列化当前会话（设置 + 全量任务快照）。
    fn session_json(&self) -> Value {
        let inner = self.inner.lock().unwrap();
        let mut all: Vec<Arc<Task>> = inner.tasks.values().cloned().collect();
        all.sort_by_key(|t| t.created_at);
        let tasks: Vec<Value> = all
            .iter()
            .map(|t| {
                let s = snapshot(t);
                // active 序列化为 waiting：重启后重新入队（断点续传）
                let status = match t.status() {
                    Status::Active => "waiting",
                    st => st.as_str(),
                };
                let mut opts = serde_json::Map::new();
                for (k, v) in t.options.lock().unwrap().iter() {
                    opts.insert(k.clone(), Value::String(v.clone()));
                }
                json!({
                    "gid": s.gid,
                    "uris": t.uris,
                    "dir": s.dir,
                    "out": t.out,
                    "checksum": t.checksum.as_ref().map(|(a, e)| format!("{a:?}={e}").to_lowercase()),
                    "options": Value::Object(opts),
                    "status": status,
                    "completedLength": s.completed,
                    "totalLength": s.total_len.unwrap_or(0),
                    "elapsedMs": s.elapsed_ms,
                    "path": s.path,
                    "errorCode": s.error_code,
                    "errorMessage": s.error_message,
                    "btTrackers": t.bt_trackers.lock().unwrap().clone(),
                    "btInfoHash": t.bt_info_hash.lock().unwrap().map(|h| h.iter().map(|b| format!("{b:02x}")).collect::<String>()),
                    "btTorrentB64": t.bt_meta.lock().unwrap().as_ref().and_then(|m| {
                        // 保存 .torrent 原始文件的 base64（重启后重新解析恢复 bt_meta）
                        m.raw_bencode.as_ref().map(|bytes| {
                            use base64::Engine;
                            base64::engine::general_purpose::STANDARD.encode(bytes)
                        })
                    }),
                })
            })
            .collect();
        let mut global_opts = serde_json::Map::new();
        for (k, v) in &inner.global_options {
            global_opts.insert(k.clone(), Value::String(v.clone()));
        }
        json!({
            "version": 1,
            "settings": {
                "max-concurrent-downloads": inner.max_concurrent,
                "dir": inner.download_dir.to_string_lossy(),
                "options": Value::Object(global_opts),
                "btTrackers": inner.global_trackers.clone(),
                "trackerSubscriptions": inner.tracker_subscriptions.clone(),
                "autoUpdateTrackers": inner.auto_update_trackers,
            },
            "stoppedTotal": inner.stopped_total,
            "tasks": tasks,
        })
    }

    /// 立即保存会话（未开启持久化时为空操作）。
    fn save_session_now(&self) {
        let path = self.inner.lock().unwrap().session.clone();
        let Some(path) = path else { return };
        let v = self.session_json();
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let tmp = path.with_extension("json.tmp");
        match serde_json::to_string_pretty(&v)
            .map_err(|e| e.to_string())
            .and_then(|text| {
                std::fs::write(&tmp, text).map_err(|e| e.to_string())?;
                std::fs::rename(&tmp, &path).map_err(|e| e.to_string())
            }) {
            Ok(()) => tracing::debug!(path = %path.display(), "会话已保存"),
            Err(e) => tracing::warn!(path = %path.display(), error = %e, "会话保存失败"),
        }
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

    fn emit(&self, event: &str, gid: &Gid) {
        tracing::debug!(event, gid = %gid, "任务事件");
        let _ = self.events.send((event.to_string(), gid.0.clone()));
    }

    fn kick(&self) {
        self.kick.notify_one();
    }

    // ------------------------------------------------------------------
    // 调度
    // ------------------------------------------------------------------

    async fn scheduler_loop(self: Arc<Self>) {
        loop {
            self.kick.notified().await;
            self.fill_slots();
        }
    }

    /// 按空闲并发槽把等待队列头部任务拉起。
    fn fill_slots(self: &Arc<Self>) {
        let mut started: Vec<Arc<Task>> = Vec::new();
        {
            let mut inner = self.inner.lock().unwrap();
            while inner.active.len() < inner.max_concurrent {
                let Some(gid) = inner.queue.front().cloned() else {
                    break;
                };
                let Some(task) = inner.tasks.get(&gid).cloned() else {
                    inner.queue.pop_front();
                    continue;
                };
                if task.status() != Status::Waiting {
                    // 状态已被 pause/remove 转移，跳过残留队列项
                    inner.queue.pop_front();
                    continue;
                }
                inner.queue.pop_front();
                inner.active.insert(gid);
                task.set_status(Status::Active);
                started.push(task);
            }
        }
        for task in started {
            tracing::info!(gid = %task.gid, "任务开始");
            self.emit("start", &task.gid);
            let mgr = self.clone();
            tokio::spawn(async move { mgr.run_task(task).await });
        }
    }

    // ------------------------------------------------------------------
    // 下载工作者
    // ------------------------------------------------------------------

    async fn run_task(self: Arc<Self>, task: Arc<Task>) {
        let cancel = task.cancel.read().unwrap().clone();
        let ticker = tokio::spawn(speed_ticker(task.clone(), self.events.clone()));
        // M6：panic 隔离 — 任务级 panic 不应崩垮整个引擎
        let failure = if task.bt_meta.lock().unwrap().is_some()
            || task.bt_info_hash.lock().unwrap().is_some()
        {
            match std::panic::AssertUnwindSafe(drive_bt_download(&self, &task, &cancel))
                .catch_unwind()
                .await
            {
                Ok(Ok(r)) => Ok(r),
                Ok(Err(e)) => Err(e),
                Err(panic) => Err(TaskFailure::Bt(format!(
                    "任务 panic: {}",
                    panic_downcast(panic)
                ))),
            }
        } else {
            match std::panic::AssertUnwindSafe(drive_download(&self, &task, &cancel))
                .catch_unwind()
                .await
            {
                Ok(Ok(r)) => Ok(r),
                Ok(Err(e)) => Err(e),
                Err(panic) => Err(TaskFailure::Bt(format!(
                    "任务 panic: {}",
                    panic_downcast(panic)
                ))),
            }
        };
        ticker.abort();

        let (event, terminal) = {
            let mut sh = task.shared.lock().unwrap();
            sh.download_speed = 0;
            sh.upload_speed = 0;
            sh.connections = 0;
            // 清零原子计数器：避免终态后 snapshot/stat_raw 仍读到旧值
            task.speed_atomic.store(0, Ordering::Relaxed);
            task.upload_speed_atomic.store(0, Ordering::Relaxed);
            task.connections_atomic.store(0, Ordering::Relaxed);
            // 离开 active：冻结活跃计时（paused 冻结，终态定格）
            if let Some(since) = sh.active_since.take() {
                sh.active_ms += since.elapsed().as_millis() as u64;
            }
            match failure {
                Ok(()) => {
                    sh.status = Status::Complete;
                    sh.error_code = 0;
                    ("complete", true)
                }
                Err(f) if f.is_cancelled() => {
                    let intent = *task.intent.lock().unwrap();
                    match intent {
                        Intent::Pause => {
                            sh.status = Status::Paused;
                            ("pause", false)
                        }
                        _ => {
                            sh.status = Status::Removed;
                            ("stop", true)
                        }
                    }
                }
                Err(f) => {
                    sh.status = Status::Error;
                    sh.error_code = f.error_code();
                    sh.error_message = f.to_string();
                    tracing::warn!(gid = %task.gid, code = sh.error_code, "任务失败: {f}");
                    ("error", true)
                }
            }
        };
        self.emit(event, &task.gid);

        // active 移除路径：worker 已完全退出，安全执行删除收尾
        if task.status() == Status::Removed {
            if task.delete_files.load(Ordering::SeqCst) {
                delete_task_files(&task);
            } else {
                delete_task_ctrl(&task);
            }
        }

        {
            let mut inner = self.inner.lock().unwrap();
            inner.active.remove(&task.gid);
            if task.status() == Status::Removed {
                // 移除任务：彻底从 tasks 和 stopped_order 中删除，不留历史
                inner.tasks.remove(&task.gid);
                inner.stopped_order.retain(|g| g != &task.gid);
                if let Some(p) = task.shared.lock().unwrap().path.clone() {
                    inner.claims.remove(&p);
                }
            } else if terminal {
                inner.stopped_order.push(task.gid.clone());
                inner.stopped_total += 1;
                if let Some(p) = task.shared.lock().unwrap().path.clone() {
                    inner.claims.remove(&p);
                }
                // 上限淘汰最旧的停止结果
                while inner.stopped_order.len() > MAX_RESULTS {
                    let old = inner.stopped_order.remove(0);
                    inner.tasks.remove(&old);
                }
            }
        }
        self.kick();
        if terminal {
            self.save_session_now();
        }
    }

    // ------------------------------------------------------------------
    // 任务生命周期
    // ------------------------------------------------------------------

    pub fn add_uri(
        &self,
        uris: Vec<String>,
        options: &Value,
        position: Option<i64>,
    ) -> Result<Gid, String> {
        if uris.is_empty() {
            return Err("uris 为空".into());
        }
        // 磁力链接：整批按首个 magnet 转磁力任务
        if let Some(m) = uris.iter().find(|u| u.trim().starts_with("magnet:")) {
            return self.add_magnet(m, options, position);
        }
        let opts = options.as_object().cloned().unwrap_or_default();
        let dir = match opts.get("dir").and_then(Value::as_str) {
            Some(d) => normalize_dir(d),
            None => self.inner.lock().unwrap().download_dir.clone(),
        };
        let out = opts
            .get("out")
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        let checksum = opts
            .get("checksum")
            .and_then(Value::as_str)
            .and_then(parse_checksum_option);
        // 任务级选项（split / min-split-size 等，下载时与全局合并）
        let task_opts = collect_task_options(&opts);

        let gid = Gid::generate();
        let task = Arc::new(Task::new(gid.clone(), uris, dir, out, checksum, task_opts));
        {
            let mut inner = self.inner.lock().unwrap();
            inner.tasks.insert(gid.clone(), task);
            match position {
                Some(pos) if pos >= 0 => {
                    let pos = (pos as usize).min(inner.queue.len());
                    inner.queue.insert(pos, gid.clone());
                }
                _ => inner.queue.push_back(gid.clone()),
            }
        }
        tracing::info!(gid = %gid, "新增任务");
        self.kick();
        self.save_session_now();
        Ok(gid)
    }

    /// 新增 BT 任务：`torrent_b64` 为 .torrent 文件的 base64。
    pub fn add_torrent(
        &self,
        torrent_b64: &str,
        options: &Value,
        position: Option<i64>,
    ) -> Result<Gid, String> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(torrent_b64.trim())
            .map_err(|e| format!("torrent base64 解码失败: {e}"))?;
        let meta = parse_torrent(&bytes).map_err(|e| format!("torrent 解析失败: {e}"))?;
        let opts = options.as_object().cloned().unwrap_or_default();
        let dir = match opts.get("dir").and_then(Value::as_str) {
            Some(d) => normalize_dir(d),
            None => self.inner.lock().unwrap().download_dir.clone(),
        };
        let gid = Gid::generate();
        let task = Arc::new(Task::new_torrent(
            gid.clone(),
            dir,
            Arc::new(meta),
            collect_task_options(&opts),
        ));
        {
            let mut inner = self.inner.lock().unwrap();
            inner.tasks.insert(gid.clone(), task);
            match position {
                Some(pos) if pos >= 0 => {
                    let pos = (pos as usize).min(inner.queue.len());
                    inner.queue.insert(pos, gid.clone());
                }
                _ => inner.queue.push_back(gid.clone()),
            }
        }
        tracing::info!(gid = %gid, "新增 BT 任务");
        self.kick();
        self.save_session_now();
        Ok(gid)
    }

    /// 新增磁力链接任务（BEP 9）：只有 info_hash，元数据经 ut_metadata 从 peer 获取。
    pub fn add_magnet(
        &self,
        magnet: &str,
        options: &Value,
        position: Option<i64>,
    ) -> Result<Gid, String> {
        let m = parse_magnet(magnet.trim()).map_err(|e| format!("磁力链接解析失败: {e}"))?;
        let opts = options.as_object().cloned().unwrap_or_default();
        let dir = match opts.get("dir").and_then(Value::as_str) {
            Some(d) => normalize_dir(d),
            None => self.inner.lock().unwrap().download_dir.clone(),
        };
        let gid = Gid::generate();
        let task = Arc::new(Task::new_magnet(
            gid.clone(),
            dir,
            m.info_hash,
            m.trackers,
            m.display_name,
            collect_task_options(&opts),
        ));
        {
            let mut inner = self.inner.lock().unwrap();
            inner.tasks.insert(gid.clone(), task);
            match position {
                Some(pos) if pos >= 0 => {
                    let pos = (pos as usize).min(inner.queue.len());
                    inner.queue.insert(pos, gid.clone());
                }
                _ => inner.queue.push_back(gid.clone()),
            }
        }
        tracing::info!(gid = %gid, "新增磁力任务");
        self.kick();
        self.save_session_now();
        Ok(gid)
    }

    pub fn pause(&self, gid: &Gid) -> Result<(), String> {
        let task = self.task_of(gid)?;
        match task.status() {
            Status::Waiting => {
                self.inner.lock().unwrap().queue.retain(|g| g != gid);
                task.set_status(Status::Paused);
                self.emit("pause", gid);
                self.kick();
                self.save_session_now();
                Ok(())
            }
            Status::Active => {
                // 先设意图再取消：工作者据此转入 Paused
                *task.intent.lock().unwrap() = Intent::Pause;
                task.cancel.read().unwrap().cancel();
                Ok(())
            }
            Status::Paused => Err("任务已暂停".into()),
            _ => Err("任务已结束，无法暂停".into()),
        }
    }

    pub fn unpause(&self, gid: &Gid) -> Result<(), String> {
        let task = self.task_of(gid)?;
        if task.status() != Status::Paused {
            return Err("任务未处于暂停状态".into());
        }
        // 令牌不可复用：替换为新令牌并重置意图，再入队等待调度
        *task.cancel.write().unwrap() = CancellationToken::new();
        *task.intent.lock().unwrap() = Intent::None;
        task.delete_files.store(false, Ordering::SeqCst);
        self.inner.lock().unwrap().queue.push_back(gid.clone());
        task.set_status(Status::Waiting);
        self.kick();
        self.save_session_now();
        Ok(())
    }

    pub fn remove(&self, gid: &Gid) -> Result<(), String> {
        self.remove_with_files(gid, false)
    }

    /// 移除任务；`delete_files` 为 true 时连带删除已下载文件与控制文件。
    /// 各状态均可移除：waiting/paused/终态同步完成，active 异步
    /// （worker 退出后由 run_task 收尾执行删除）。
    pub fn remove_with_files(&self, gid: &Gid, delete_files: bool) -> Result<(), String> {
        let task = self.task_of(gid)?;
        task.delete_files.store(delete_files, Ordering::SeqCst);
        match task.status() {
            Status::Waiting => {
                self.inner.lock().unwrap().queue.retain(|g| g != gid);
                self.finish_removed(&task, gid);
                Ok(())
            }
            Status::Active => {
                *task.intent.lock().unwrap() = Intent::Remove;
                task.cancel.read().unwrap().cancel();
                Ok(())
            }
            Status::Paused => {
                self.finish_removed(&task, gid);
                Ok(())
            }
            _ => {
                self.finish_removed(&task, gid);
                Ok(())
            }
        }
    }

    fn finish_removed(&self, task: &Arc<Task>, gid: &Gid) {
        task.set_status(Status::Removed);
        {
            let mut inner = self.inner.lock().unwrap();
            // 彻底删除：从 tasks 和 stopped_order 中移除，不留历史记录
            inner.tasks.remove(gid);
            inner.stopped_order.retain(|g| g != gid);
            if let Some(p) = task.shared.lock().unwrap().path.clone() {
                inner.claims.remove(&p);
            }
        }
        // 移除时同步清理：勾选删文件 → 删数据文件；控制文件一律删除
        if task.delete_files.load(Ordering::SeqCst) {
            delete_task_files(task);
        } else {
            delete_task_ctrl(task);
        }
        self.emit("stop", gid);
        self.kick();
        self.save_session_now();
    }

    fn task_of(&self, gid: &Gid) -> Result<Arc<Task>, String> {
        self.inner
            .lock()
            .unwrap()
            .tasks
            .get(gid)
            .cloned()
            .ok_or_else(|| format!("GID {gid} 不存在"))
    }

    // ------------------------------------------------------------------
    // 查询
    // ------------------------------------------------------------------

    pub fn tell_status(&self, gid: &Gid, keys: Option<&[String]>) -> Result<Value, String> {
        let task = self.task_of(gid)?;
        Ok(filter_keys(status_json(&task), keys))
    }

    /// 原生协议版状态查询（数值字段为真实 JSON 数值）。
    pub fn tell_status_native(&self, gid: &Gid, keys: Option<&[String]>) -> Result<Value, String> {
        let task = self.task_of(gid)?;
        Ok(filter_keys(status_json_native(&task), keys))
    }

    /// 轻量进度快照：(status, completedLength, totalLength, downloadSpeed)。
    ///
    /// `task.progress` 推送（1Hz）专用，只读共享状态、不做完整状态序列化，
    /// 避免多文件 BT 任务在每秒每连接上重复序列化整个 files 数组。
    pub fn progress_snapshot(&self, gid: &Gid) -> Option<(String, u64, u64, u64)> {
        let task = self.task_of(gid).ok()?;
        // 无锁实时值先行（*_live 回退会锁 shared，不能持锁调用）
        let completed = task.completed_live();
        let speed = task.speed_live();
        let sh = task.shared.lock().unwrap();
        Some((
            sh.status.as_str().to_string(),
            completed,
            sh.total_len.unwrap_or(0),
            speed,
        ))
    }

    pub fn tell_active(&self, keys: Option<&[String]>) -> Value {
        let inner = self.inner.lock().unwrap();
        let mut actives: Vec<Arc<Task>> = inner
            .active
            .iter()
            .filter_map(|g| inner.tasks.get(g).cloned())
            .collect();
        drop(inner);
        // 按创建顺序稳定输出
        actives.sort_by_key(|t| t.created_at);
        Value::Array(
            actives
                .iter()
                .map(|t| filter_keys(status_json(t), keys))
                .collect(),
        )
    }

    pub fn tell_waiting(&self, offset: i64, num: i64, keys: Option<&[String]>) -> Value {
        let inner = self.inner.lock().unwrap();
        let ordered: Vec<Arc<Task>> = inner
            .queue
            .iter()
            .filter_map(|g| inner.tasks.get(g).cloned())
            .collect();
        drop(inner);
        slice_tasks(ordered, offset, num, keys)
    }

    pub fn tell_stopped(&self, offset: i64, num: i64, keys: Option<&[String]>) -> Value {
        let inner = self.inner.lock().unwrap();
        // 新→旧输出
        let ordered: Vec<Arc<Task>> = inner
            .stopped_order
            .iter()
            .rev()
            .filter_map(|g| inner.tasks.get(g).cloned())
            .collect();
        drop(inner);
        slice_tasks(ordered, offset, num, keys)
    }

    /// 原生协议版列表查询（scope: active|waiting|stopped|all）。
    pub fn list_native(
        &self,
        scope: &str,
        offset: i64,
        num: i64,
        keys: Option<&[String]>,
    ) -> Value {
        let inner = self.inner.lock().unwrap();
        let mut ordered: Vec<Arc<Task>> = Vec::new();
        if scope == "active" || scope == "all" {
            let mut actives: Vec<Arc<Task>> = inner
                .active
                .iter()
                .filter_map(|g| inner.tasks.get(g).cloned())
                .collect();
            actives.sort_by_key(|t| t.created_at);
            ordered.extend(actives);
        }
        if scope == "waiting" || scope == "all" {
            ordered.extend(
                inner
                    .queue
                    .iter()
                    .filter_map(|g| inner.tasks.get(g).cloned()),
            );
        }
        if scope == "stopped" || scope == "all" {
            // 暂停中的任务既不进 active/queue，也不进 stopped_order，
            // 需单独从 tasks 中捞出纳入列表。
            let mut paused: Vec<Arc<Task>> = inner
                .tasks
                .values()
                .filter(|t| t.status() == Status::Paused)
                .cloned()
                .collect();
            paused.sort_by_key(|t| t.created_at);
            ordered.extend(paused);
            ordered.extend(
                inner
                    .stopped_order
                    .iter()
                    .rev()
                    .filter_map(|g| inner.tasks.get(g).cloned()),
            );
        }
        drop(inner);
        let start = offset.max(0) as usize;
        if start >= ordered.len() {
            return Value::Array(vec![]);
        }
        let end = if num < 0 {
            ordered.len()
        } else {
            (start + num as usize).min(ordered.len())
        };
        Value::Array(
            ordered[start..end]
                .iter()
                .map(|t| filter_keys(status_json_native(t), keys))
                .collect(),
        )
    }

    pub fn global_stat(&self) -> Value {
        let (speed, upload, active, waiting, stopped, stopped_total) = self.stat_raw();
        serde_json::json!({
            "downloadSpeed": speed.to_string(),
            "uploadSpeed": upload.to_string(),
            "numActive": active.to_string(),
            "numWaiting": waiting.to_string(),
            "numStopped": stopped.to_string(),
            "numStoppedTotal": stopped_total.to_string(),
        })
    }

    /// 原生协议版全局统计（数值类型）。
    pub fn global_stat_native(&self) -> Value {
        let (speed, upload, active, waiting, stopped, stopped_total) = self.stat_raw();
        serde_json::json!({
            "downloadSpeed": speed,
            "uploadSpeed": upload,
            "numActive": active,
            "numWaiting": waiting,
            "numStopped": stopped,
            "numStoppedTotal": stopped_total,
        })
    }

    fn stat_raw(&self) -> (u64, u64, usize, usize, usize, u64) {
        let inner = self.inner.lock().unwrap();
        let mut speed = 0u64;
        let mut upload = 0u64;
        for gid in &inner.active {
            if let Some(t) = inner.tasks.get(gid) {
                // 无锁读速度：避免统计轮询与下载热路径争抢任务锁
                speed += t.speed_live();
                upload += t.upload_speed_live();
            }
        }
        (
            speed,
            upload,
            inner.active.len(),
            inner.queue.len(),
            inner.stopped_order.len(),
            inner.stopped_total,
        )
    }

    // ------------------------------------------------------------------
    // 目标路径解析（含 claims 冲突检查）
    // ------------------------------------------------------------------

    /// 计算 HTTP 分片下载参数：任务选项 > 全局选项 > 默认值。
    /// connections = min(split, max-connection-per-server)，上限 128。
    fn split_options(&self, task: &Task) -> xfer_http::SplitOptions {
        let g = self.inner.lock().unwrap().global_options.clone();
        let t = task.options.lock().unwrap().clone();
        let get = |k: &str| t.get(k).or_else(|| g.get(k));
        let num = |k: &str, d: usize| {
            get(k)
                .and_then(|v| v.trim().parse::<usize>().ok())
                .filter(|n| *n > 0)
                .unwrap_or(d)
        };
        let split = num("split", DEFAULT_SPLIT_CONNECTIONS);
        let max_conn = num("max-connection-per-server", DEFAULT_SPLIT_CONNECTIONS);
        let connections = split.min(max_conn).clamp(1, 128);
        let min_split = get("min-split-size")
            .and_then(|v| parse_size_bytes(v))
            .unwrap_or(DEFAULT_MIN_SPLIT_SIZE)
            .clamp(64 * 1024, 1 << 30);
        // 自适应调度：默认启用。
        // 用户设置的 split 值即为「预分配连接数」——以此并发度起步；
        // 慢/停滞连接退役减员、吞吐回升时重新扩充，不超过此上限。
        // 评估周期收紧到 1s：分段/减员决策对限速服务器越早生效越好。
        let adaptive_enabled = get("adaptive")
            .map(|v| v != "false" && v != "0")
            .unwrap_or(true);
        let adaptive = if adaptive_enabled {
            Some(xfer_http::AdaptiveConfig {
                initial_connections: connections,
                max_connections: connections, // 上限 = 预分配连接数
                min_connections: 1,
                eval_interval: std::time::Duration::from_secs(1),
                ..Default::default()
            })
        } else {
            None
        };
        xfer_http::SplitOptions {
            connections,
            min_split_size: min_split,
            adaptive,
        }
    }

    /// 计算 BT 下载参数：任务选项 > 全局选项 > 默认值。
    ///
    /// 返回 `(预分配连接数, 是否启用智能调度)`。BT 连接数独立于 HTTP 的 split。
    fn bt_options(&self, task: &Task) -> (usize, bool) {
        let g = self.inner.lock().unwrap().global_options.clone();
        let t = task.options.lock().unwrap().clone();
        let get = |k: &str| t.get(k).or_else(|| g.get(k));

        let max_peers = get("bt-max-peers")
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_BT_MAX_PEERS)
            .clamp(1, MAX_BT_MAX_PEERS);
        // 智能调度默认启用：按吞吐边际收益动态增减连接
        let adaptive = get("bt-adaptive")
            .map(|v| v != "false" && v != "0")
            .unwrap_or(true);
        (max_peers, adaptive)
    }

    /// BT 加密模式与传输协议（`bt-encryption` / `bt-protocol`，任务级覆盖全局）。
    /// 非法取值退回默认（优先加密 / TCP+uTP），与白名单校验形成双保险。
    fn bt_modes(&self, task: &Task) -> (xfer_bt::EncryptionMode, xfer_bt::BtProtocol) {
        let g = self.inner.lock().unwrap().global_options.clone();
        let t = task.options.lock().unwrap().clone();
        let get = |k: &str| t.get(k).or_else(|| g.get(k));
        let encryption = get("bt-encryption")
            .and_then(|v| xfer_bt::EncryptionMode::parse(v))
            .unwrap_or_default();
        let protocol = get("bt-protocol")
            .and_then(|v| xfer_bt::BtProtocol::parse(v))
            .unwrap_or_default();
        (encryption, protocol)
    }

    fn resolve_path(&self, task: &Arc<Task>, probe: &xfer_http::Probe) -> PathBuf {
        // 暂停恢复：沿用已解析路径
        if let Some(p) = task.shared.lock().unwrap().path.clone() {
            return p;
        }
        let base_name = task
            .out
            .clone()
            .or_else(|| probe.filename.clone())
            .unwrap_or_else(|| "download".to_string());

        let mut n = 0usize;
        loop {
            let name = if n == 0 {
                base_name.clone()
            } else {
                rename_with_counter(&base_name, n)
            };
            let candidate = task.dir.join(&name);
            let existing = existing_len(&candidate);
            let can_resume = probe.accepts_ranges
                && existing > 0
                && probe.total_len.is_none_or(|t| existing < t);
            let disk_conflict = !can_resume && existing > 0;
            let claimed = {
                let mut inner = self.inner.lock().unwrap();
                let claimed = inner.claims.contains(&candidate);
                if !claimed && !disk_conflict {
                    inner.claims.insert(candidate.clone());
                }
                claimed
            };
            if !claimed && !disk_conflict {
                let mut sh = task.shared.lock().unwrap();
                sh.path = Some(candidate.clone());
                sh.filename = Some(name);
                return candidate;
            }
            n += 1;
        }
    }

    // ------------------------------------------------------------------
    // 全局选项
    // ------------------------------------------------------------------

    pub fn change_global_option(&self, options: &Value) -> Result<(), String> {
        let Some(opts) = options.as_object() else {
            return Err("options 必须是对象".into());
        };
        let mut inner = self.inner.lock().unwrap();
        let mut rate_changed = false;
        let mut bt_modes_changed = false;
        for (k, v) in opts {
            let v = match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            if k == "max-concurrent-downloads" {
                if let Ok(n) = v.parse::<usize>() {
                    inner.max_concurrent = n.max(1);
                }
            } else if k == "dir" {
                // 运行时改目录：仅影响后续新增任务，已入队任务保持原目录。
                if !v.is_empty() {
                    inner.download_dir = normalize_dir(&v);
                }
            } else if matches!(
                k.as_str(),
                "max-overall-download-limit" | "max-overall-upload-limit"
            ) {
                // 全局限速（bytes/s，0 = 不限制）：校验后存储，并立即
                // 下发到活动 BT 引擎（aria2 changeGlobalOption 语义）
                if v.trim().parse::<u64>().is_err() {
                    return Err(format!("限速值必须是非负整数（字节/秒）: {k}={v}"));
                }
                rate_changed = true;
            } else if k == "bt-encryption" {
                // 加密模式：adaptive（优先加密）/ force（强制）/ plain（仅明文）
                if xfer_bt::EncryptionMode::parse(&v).is_none() {
                    return Err(format!("bt-encryption 取值无效: {v}（可选 adaptive/force/plain）"));
                }
                bt_modes_changed = true;
            } else if k == "bt-protocol" {
                // 传输协议：tcp+utp / tcp / utp（uTP 就绪前切 utp 会被引擎拒绝）
                if xfer_bt::BtProtocol::parse(&v).is_none() {
                    return Err(format!("bt-protocol 取值无效: {v}（可选 tcp+utp/tcp/utp）"));
                }
                bt_modes_changed = true;
            } else if matches!(
                k.as_str(),
                "split"
                    | "max-connection-per-server"
                    | "min-split-size"
                    | "bt-max-peers"
                    | "bt-adaptive"
            ) {
                // HTTP 分片参数 / BT 连接参数：存储后在下载时生效
            } else {
                tracing::debug!(option = %k, "全局选项暂未支持，已忽略");
            }
            inner.global_options.insert(k.clone(), v);
        }
        drop(inner);
        if rate_changed {
            self.apply_rate_limits();
        }
        if bt_modes_changed {
            self.apply_bt_modes()?;
        }
        self.kick();
        self.save_session_now();
        Ok(())
    }

    /// 读取全局限速配置（下载/上传，bytes/s，0 = 不限制；缺失 = 0）。
    fn rate_limits(&self) -> (u64, u64) {
        let inner = self.inner.lock().unwrap();
        let dl = inner
            .global_options
            .get("max-overall-download-limit")
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
        let ul = inner
            .global_options
            .get("max-overall-upload-limit")
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
        (dl, ul)
    }

    /// 将当前全局限速下发到所有活动 BT 引擎（运行时立即生效）。
    fn apply_rate_limits(&self) {
        let (dl, ul) = self.rate_limits();
        for engine in self.bt_engines.lock().unwrap().values() {
            engine.set_rate_limits(dl, ul);
        }
    }

    /// 把当前全局 `bt-encryption` / `bt-protocol` 热下发到所有活动 BT 引擎。
    /// 引擎拒绝时（如 uTP 未就绪却切 utp）返回错误，选项本身已保存。
    fn apply_bt_modes(&self) -> Result<(), String> {
        let (encryption, protocol) = {
            let g = self.inner.lock().unwrap().global_options.clone();
            (
                g.get("bt-encryption")
                    .and_then(|v| xfer_bt::EncryptionMode::parse(v)),
                g.get("bt-protocol").and_then(|v| xfer_bt::BtProtocol::parse(v)),
            )
        };
        if encryption.is_none() && protocol.is_none() {
            return Ok(());
        }
        for engine in self.bt_engines.lock().unwrap().values() {
            engine.set_bt_modes(encryption, protocol)?;
        }
        Ok(())
    }

    pub fn get_global_option(&self) -> Value {
        let inner = self.inner.lock().unwrap();
        let mut m = serde_json::Map::new();
        for (k, v) in &inner.global_options {
            m.insert(k.clone(), Value::String(v.clone()));
        }
        m.insert(
            "max-concurrent-downloads".into(),
            Value::String(inner.max_concurrent.to_string()),
        );
        m.insert(
            "dir".into(),
            Value::String(inner.download_dir.to_string_lossy().into()),
        );
        // 预分配连接数（split）：TUI 设置页显示与编辑
        let split_val = inner
            .global_options
            .get("split")
            .cloned()
            .unwrap_or_else(|| DEFAULT_SPLIT_CONNECTIONS.to_string());
        m.insert("split".into(), Value::String(split_val));
        // BT 预分配连接数（bt-max-peers）：与 HTTP split 独立，TUI 设置页显示与编辑
        let bt_peers_val = inner
            .global_options
            .get("bt-max-peers")
            .cloned()
            .unwrap_or_else(|| DEFAULT_BT_MAX_PEERS.to_string());
        m.insert("bt-max-peers".into(), Value::String(bt_peers_val));
        // BT 智能调度开关
        let bt_adaptive = inner
            .global_options
            .get("bt-adaptive")
            .map(|v| v != "false" && v != "0")
            .unwrap_or(true);
        m.insert("bt-adaptive".into(), Value::String(bt_adaptive.to_string()));
        // 全局 BT tracker 服务器列表
        m.insert(
            "bt-trackers".into(),
            Value::Array(
                inner
                    .global_trackers
                    .iter()
                    .map(|t| Value::String(t.clone()))
                    .collect(),
            ),
        );
        // Tracker 订阅源
        m.insert(
            "tracker-subscriptions".into(),
            serde_json::to_value(&inner.tracker_subscriptions).unwrap_or(Value::Array(vec![])),
        );
        m.insert(
            "auto-update-trackers".into(),
            Value::Bool(inner.auto_update_trackers),
        );
        if let Some(p) = &inner.session {
            m.insert(
                "session-path".into(),
                Value::String(p.to_string_lossy().into()),
            );
        }
        Value::Object(m)
    }
    // ------------------------------------------------------------------
    // 任务附加信息查询与维护
    // ------------------------------------------------------------------

    pub fn get_files(&self, gid: &Gid) -> Result<Value, String> {
        let task = self.task_of(gid)?;
        Ok(status_json(&task)
            .get("files")
            .cloned()
            .unwrap_or_else(|| Value::Array(vec![])))
    }

    pub fn get_uris(&self, gid: &Gid) -> Result<Value, String> {
        let task = self.task_of(gid)?;
        Ok(status_json(&task)["files"][0]
            .get("uris")
            .cloned()
            .unwrap_or_else(|| Value::Array(vec![])))
    }

    /// BT 任务当前连接的 peer 列表（原生 task.getPeers）。
    pub fn get_peers(&self, gid: &Gid) -> Result<Value, String> {
        let task = self.task_of(gid)?;
        if task.bt_meta.lock().unwrap().is_none() && task.bt_info_hash.lock().unwrap().is_none() {
            return Err("非 BT 任务，无 peer 信息".into());
        }
        let peers = task.bt_peers.lock().unwrap();
        let arr: Vec<Value> = peers
            .iter()
            .map(|p| {
                json!({
                    "addr": p.addr,
                    "peerId": p.peer_id,
                    "client": p.client,
                    "source": p.source.as_str(),
                    "choked": p.choked,
                    "interested": p.interested,
                    "seed": p.seed,
                    "downloaded": p.downloaded,
                    "connected": p.connected,
                    "encrypted": p.encrypted,
                    "uploaded": p.uploaded,
                    "protocol": p.protocol,
                    "connectedSecs": p.connected_secs,
                    "progress": p.progress,
                })
            })
            .collect();
        Ok(Value::Array(arr))
    }

    /// 向 BT 任务动态添加 tracker URL。
    ///
    /// - 等待/暂停/活动状态均可添加，新 tracker 立即参与下一轮 announce。
    /// - 重复 URL 自动去重；非 BT 任务返回错误。
    /// - 活动任务的引擎实例会实时感知（通过 `task.bt_trackers` 共享引用）。
    pub fn add_trackers(&self, gid: &Gid, trackers: Vec<String>) -> Result<(), String> {
        let task = self.task_of(gid)?;
        if task.bt_meta.lock().unwrap().is_none() && task.bt_info_hash.lock().unwrap().is_none() {
            return Err("非 BT 任务，无法添加 tracker".into());
        }
        if task.status().is_terminal() {
            return Err("任务已结束，无法添加 tracker".into());
        }
        let added: Vec<String> = {
            let mut current = task.bt_trackers.lock().unwrap();
            let mut added = Vec::new();
            for url in trackers {
                let url = url.trim().to_string();
                if url.is_empty() {
                    continue;
                }
                if current.iter().any(|t| t == &url) {
                    continue;
                }
                current.push(url.clone());
                added.push(url);
            }
            added
        };
        if !added.is_empty() {
            tracing::info!(gid = %gid, count = added.len(), "已添加 tracker");
            self.save_session_now();
        }
        Ok(())
    }

    /// 查询 BT 任务的 tracker 列表。
    pub fn get_trackers(&self, gid: &Gid) -> Result<Value, String> {
        let task = self.task_of(gid)?;
        if task.bt_meta.lock().unwrap().is_none() && task.bt_info_hash.lock().unwrap().is_none() {
            return Err("非 BT 任务，无 tracker 信息".into());
        }
        // 合并：torrent 自带 tracker + 运行时添加的 + 全局 tracker（去重）
        let mut seen = std::collections::HashSet::new();
        let mut urls: Vec<String> = Vec::new();
        // 1. torrent 文件自带的 tracker
        if let Some(meta) = &*task.bt_meta.lock().unwrap() {
            if let Some(a) = &meta.announce {
                if seen.insert(a.clone()) {
                    urls.push(a.clone());
                }
            }
            for tier in &meta.announce_list {
                for url in tier {
                    if seen.insert(url.clone()) {
                        urls.push(url.clone());
                    }
                }
            }
        }
        // 2. 运行时添加的 tracker（含磁力链接 tr 参数）
        for url in task.bt_trackers.lock().unwrap().iter() {
            if seen.insert(url.clone()) {
                urls.push(url.clone());
            }
        }
        // 3. 全局 tracker
        for url in self.inner.lock().unwrap().global_trackers.iter() {
            if seen.insert(url.clone()) {
                urls.push(url.clone());
            }
        }
        let arr: Vec<Value> = urls.iter().map(|t| json!({ "url": t })).collect();
        Ok(Value::Array(arr))
    }

    // ------------------------------------------------------------------
    // 全局 Tracker 服务器管理（设置页配置，所有 BT 任务自动注入）
    // ------------------------------------------------------------------

    /// 查询全局 tracker 服务器列表。
    pub fn get_global_trackers(&self) -> Vec<String> {
        self.inner.lock().unwrap().global_trackers.clone()
    }

    /// 向全局列表添加 tracker（去重），并同步注入到所有活动 BT 任务。
    pub fn add_global_tracker(&self, url: &str) -> Result<(), String> {
        let url = url.trim().to_string();
        if url.is_empty() {
            return Err("tracker URL 不能为空".into());
        }
        let added = {
            let mut inner = self.inner.lock().unwrap();
            if inner.global_trackers.iter().any(|t| t == &url) {
                return Err(format!("tracker 已存在: {url}"));
            }
            inner.global_trackers.push(url.clone());
            true
        };
        if added {
            // 同步注入到所有非终态 BT 任务
            let tasks: Vec<Arc<Task>> = {
                let inner = self.inner.lock().unwrap();
                inner
                    .tasks
                    .values()
                    .filter(|t| {
                        !t.status().is_terminal()
                            && (t.bt_meta.lock().unwrap().is_some()
                                || t.bt_info_hash.lock().unwrap().is_some())
                    })
                    .cloned()
                    .collect()
            };
            for task in tasks {
                let mut bt = task.bt_trackers.lock().unwrap();
                if !bt.iter().any(|t| t == &url) {
                    bt.push(url.clone());
                }
            }
            tracing::info!(url, "已添加全局 tracker");
            self.save_session_now();
        }
        Ok(())
    }

    /// 从全局列表移除 tracker，并从所有活动 BT 任务中同步移除。
    pub fn remove_global_tracker(&self, url: &str) -> Result<(), String> {
        let url = url.trim().to_string();
        let removed = {
            let mut inner = self.inner.lock().unwrap();
            let before = inner.global_trackers.len();
            inner.global_trackers.retain(|t| t != &url);
            before != inner.global_trackers.len()
        };
        if removed {
            // 从所有非终态 BT 任务中同步移除
            let tasks: Vec<Arc<Task>> = {
                let inner = self.inner.lock().unwrap();
                inner
                    .tasks
                    .values()
                    .filter(|t| {
                        !t.status().is_terminal()
                            && (t.bt_meta.lock().unwrap().is_some()
                                || t.bt_info_hash.lock().unwrap().is_some())
                    })
                    .cloned()
                    .collect()
            };
            for task in tasks {
                let mut bt = task.bt_trackers.lock().unwrap();
                bt.retain(|t| t != &url);
            }
            tracing::info!(url, "已移除全局 tracker");
            self.save_session_now();
        } else {
            return Err(format!("tracker 不存在: {url}"));
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Tracker 订阅源管理（远程 URL 自动更新 tracker 列表）
    // ------------------------------------------------------------------

    /// 添加 Tracker 订阅源。
    pub fn add_subscription(
        &self,
        name: &str,
        url: &str,
        enabled: bool,
    ) -> Result<TrackerSubscription, String> {
        let url = url.trim().to_string();
        if url.is_empty() {
            return Err("订阅 URL 不能为空".into());
        }
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err("订阅 URL 必须以 http:// 或 https:// 开头".into());
        }
        let sub = TrackerSubscription {
            id: generate_id(),
            name: if name.trim().is_empty() {
                url.clone()
            } else {
                name.trim().to_string()
            },
            url,
            enabled,
            last_updated: 0,
            last_count: 0,
            last_error: String::new(),
        };
        {
            let mut inner = self.inner.lock().unwrap();
            if inner.tracker_subscriptions.iter().any(|s| s.url == sub.url) {
                return Err(format!("订阅源已存在: {}", sub.url));
            }
            inner.tracker_subscriptions.push(sub.clone());
        }
        tracing::info!(name = %sub.name, url = %sub.url, "已添加 Tracker 订阅源");
        self.save_session_now();
        Ok(sub)
    }

    /// 移除 Tracker 订阅源。
    pub fn remove_subscription(&self, id: &str) -> Result<(), String> {
        let removed = {
            let mut inner = self.inner.lock().unwrap();
            let before = inner.tracker_subscriptions.len();
            inner.tracker_subscriptions.retain(|s| s.id != id);
            before != inner.tracker_subscriptions.len()
        };
        if removed {
            tracing::info!(id, "已移除 Tracker 订阅源");
            self.save_session_now();
            Ok(())
        } else {
            Err(format!("订阅源不存在: {id}"))
        }
    }

    /// 查询所有 Tracker 订阅源。
    pub fn get_subscriptions(&self) -> Vec<TrackerSubscription> {
        self.inner.lock().unwrap().tracker_subscriptions.clone()
    }

    /// 切换订阅源的启用状态。
    pub fn toggle_subscription(&self, id: &str) -> Result<(), String> {
        {
            let mut inner = self.inner.lock().unwrap();
            let sub = inner
                .tracker_subscriptions
                .iter_mut()
                .find(|s| s.id == id)
                .ok_or_else(|| format!("订阅源不存在: {id}"))?;
            sub.enabled = !sub.enabled;
        }
        self.save_session_now();
        Ok(())
    }

    /// 是否启用自动更新。
    pub fn get_auto_update_trackers(&self) -> bool {
        self.inner.lock().unwrap().auto_update_trackers
    }

    /// 设置自动更新开关。
    pub fn set_auto_update_trackers(&self, enabled: bool) {
        self.inner.lock().unwrap().auto_update_trackers = enabled;
        self.save_session_now();
    }

    /// 是否应该执行自动刷新（启用且有订阅源）。
    fn should_auto_refresh(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.auto_update_trackers
            && !inner.tracker_subscriptions.is_empty()
            && inner.tracker_subscriptions.iter().any(|s| s.enabled)
    }

    /// 从远程 URL 获取 tracker 列表（纯文本，每行一个 URL）。
    async fn fetch_subscription_trackers(
        client: &reqwest::Client,
        url: &str,
    ) -> Result<Vec<String>, String> {
        let resp = client
            .get(url)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(|e| format!("请求失败: {e}"))?;
        let text = resp
            .text()
            .await
            .map_err(|e| format!("读取响应失败: {e}"))?;
        let trackers: Vec<String> = text
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| {
                !l.is_empty()
                    && !l.starts_with('#')
                    && (l.starts_with("udp://")
                        || l.starts_with("http://")
                        || l.starts_with("https://")
                        || l.starts_with("wss://"))
            })
            .collect();
        Ok(trackers)
    }

    /// 刷新单个订阅源：远程获取 tracker 列表并合并到全局列表。
    pub async fn refresh_subscription(&self, id: &str) -> Result<usize, String> {
        // 取出订阅信息（避免持锁跨 await）
        let sub_url = {
            let inner = self.inner.lock().unwrap();
            inner
                .tracker_subscriptions
                .iter()
                .find(|s| s.id == id)
                .map(|s| s.url.clone())
                .ok_or_else(|| format!("订阅源不存在: {id}"))?
        };

        let trackers = Self::fetch_subscription_trackers(&self.client, &sub_url).await?;

        // 合并到全局 tracker 列表（去重）
        let added = {
            let mut inner = self.inner.lock().unwrap();
            let mut count = 0;
            for url in &trackers {
                if !inner.global_trackers.contains(url) {
                    inner.global_trackers.push(url.clone());
                    count += 1;
                }
            }
            count
        };
        // 同步注入到活动 BT 任务
        {
            let inner = self.inner.lock().unwrap();
            let tasks: Vec<Arc<Task>> = inner
                .tasks
                .values()
                .filter(|t| {
                    !t.status().is_terminal()
                        && (t.bt_meta.lock().unwrap().is_some()
                            || t.bt_info_hash.lock().unwrap().is_some())
                })
                .cloned()
                .collect();
            drop(inner);
            for url in &trackers {
                for task in &tasks {
                    let mut bt = task.bt_trackers.lock().unwrap();
                    if !bt.contains(url) {
                        bt.push(url.clone());
                    }
                }
            }
        }
        // 更新订阅状态
        {
            let mut inner = self.inner.lock().unwrap();
            if let Some(sub) = inner.tracker_subscriptions.iter_mut().find(|s| s.id == id) {
                sub.last_updated = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                sub.last_count = trackers.len();
                sub.last_error = String::new();
            }
        }
        tracing::info!(id, total = trackers.len(), added, "订阅源刷新完成");
        self.save_session_now();
        Ok(trackers.len())
    }

    /// 刷新所有已启用的订阅源。
    pub async fn refresh_all_subscriptions(&self) -> Result<usize, String> {
        let subs: Vec<(String, String)> = {
            let inner = self.inner.lock().unwrap();
            inner
                .tracker_subscriptions
                .iter()
                .filter(|s| s.enabled)
                .map(|s| (s.id.clone(), s.url.clone()))
                .collect()
        };
        if subs.is_empty() {
            return Ok(0);
        }
        let mut total = 0;
        for (id, url) in &subs {
            match Self::fetch_subscription_trackers(&self.client, url).await {
                Ok(trackers) => {
                    // 合并到全局
                    {
                        let mut inner = self.inner.lock().unwrap();
                        for t in &trackers {
                            if !inner.global_trackers.contains(t) {
                                inner.global_trackers.push(t.clone());
                            }
                        }
                        if let Some(sub) =
                            inner.tracker_subscriptions.iter_mut().find(|s| s.id == *id)
                        {
                            sub.last_updated = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            sub.last_count = trackers.len();
                            sub.last_error = String::new();
                        }
                    }
                    total += trackers.len();
                    tracing::info!(id, count = trackers.len(), "订阅源刷新成功");
                }
                Err(e) => {
                    let mut inner = self.inner.lock().unwrap();
                    if let Some(sub) = inner.tracker_subscriptions.iter_mut().find(|s| s.id == *id)
                    {
                        sub.last_error = e.clone();
                    }
                    tracing::warn!(id, url, error = %e, "订阅源刷新失败");
                }
            }
        }
        self.save_session_now();
        Ok(total)
    }

    /// 是否为 BT 任务（compat 层据此区分 onBtDownloadComplete / onDownloadComplete）。
    pub fn is_bt_task(&self, gid: &Gid) -> bool {
        self.task_of(gid)
            .map(|t| {
                t.bt_meta.lock().unwrap().is_some() || t.bt_info_hash.lock().unwrap().is_some()
            })
            .unwrap_or(false)
    }

    pub fn get_option(&self, gid: &Gid) -> Result<Value, String> {
        let task = self.task_of(gid)?;
        let mut m = serde_json::Map::new();
        // 全局选项打底，任务级覆盖
        {
            let inner = self.inner.lock().unwrap();
            for (k, v) in &inner.global_options {
                m.insert(k.clone(), Value::String(v.clone()));
            }
        }
        for (k, v) in task.options.lock().unwrap().iter() {
            m.insert(k.clone(), Value::String(v.clone()));
        }
        m.insert(
            "dir".into(),
            Value::String(task.dir.to_string_lossy().into()),
        );
        if let Some(out) = &task.out {
            m.insert("out".into(), Value::String(out.clone()));
        }
        if let Some((algo, expect)) = &task.checksum {
            m.insert(
                "checksum".into(),
                Value::String(format!("{algo:?}={expect}").to_lowercase()),
            );
        }
        Ok(Value::Object(m))
    }

    pub fn change_option(&self, gid: &Gid, options: &Value) -> Result<(), String> {
        let task = self.task_of(gid)?;
        let Some(opts) = options.as_object() else {
            return Err("options 必须是对象".into());
        };
        if task.status().is_terminal() {
            return Err("任务已结束，无法修改选项".into());
        }
        {
            let mut t = task.options.lock().unwrap();
            for (k, v) in opts {
                if k == "dir" || k == "out" || k == "checksum" {
                    tracing::debug!(gid = %gid, option = %k, "该选项暂不支持热修改");
                    continue;
                }
                let v = match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                t.insert(k.clone(), v);
            }
        }
        self.save_session_now();
        Ok(())
    }

    pub fn purge_download_result(&self) -> Result<(), String> {
        // 先收集终态任务引用，释放锁后清理控制文件（不能持锁做文件 IO）
        let terminal: Vec<Arc<Task>> = {
            let inner = self.inner.lock().unwrap();
            inner
                .tasks
                .values()
                .filter(|t| t.status().is_terminal())
                .cloned()
                .collect()
        };
        let mut inner = self.inner.lock().unwrap();
        inner.tasks.retain(|_, t| !t.status().is_terminal());
        inner.stopped_order.clear();
        drop(inner);
        for t in terminal {
            delete_task_ctrl(&t);
        }
        self.save_session_now();
        Ok(())
    }

    pub fn remove_download_result(&self, gid: &Gid) -> Result<(), String> {
        let task = self.task_of(gid)?;
        if !task.status().is_terminal() {
            return Err("任务未结束，无法移除下载结果".into());
        }
        let mut inner = self.inner.lock().unwrap();
        inner.tasks.remove(gid);
        inner.stopped_order.retain(|g| g != gid);
        drop(inner);
        delete_task_ctrl(&task);
        self.save_session_now();
        Ok(())
    }

    pub fn save_session(&self) -> Result<(), String> {
        // RPC saveSession / 客户端退出时显式保存。
        self.save_session_now();
        Ok(())
    }

    /// 事件广播通道（协议层据此订阅推送）。
    pub fn events(&self) -> broadcast::Sender<EngineEvent> {
        self.events.clone()
    }

    /// 整体退出（RPC shutdown 触发）。
    pub fn shutdown(&self) {
        tracing::info!("shutdown 触发，引擎退出");
        self.shutdown_token.cancel();
    }
}

fn slice_tasks(ordered: Vec<Arc<Task>>, offset: i64, num: i64, keys: Option<&[String]>) -> Value {
    let start = offset.max(0) as usize;
    if start >= ordered.len() {
        return Value::Array(vec![]);
    }
    let items: Vec<Value> = if num < 0 {
        ordered[start..]
            .iter()
            .map(|t| filter_keys(status_json(t), keys))
            .collect()
    } else {
        let end = (start + num as usize).min(ordered.len());
        ordered[start..end]
            .iter()
            .map(|t| filter_keys(status_json(t), keys))
            .collect()
    };
    Value::Array(items)
}

fn parse_checksum_option(v: &str) -> Option<(HashAlgo, String)> {
    let (algo, digest) = v.split_once('=')?;
    let algo = HashAlgo::parse(algo)?;
    Some((algo, digest.trim().to_ascii_lowercase()))
}

/// 默认会话文件路径：用户主目录下 `.xfer/session.json`
/// （跨平台；主目录不可得退当前目录）。
pub fn default_session_path() -> PathBuf {
    let home = home_dir();
    home.join(".xfer").join("session.json")
}

/// 用户主目录（跨平台：Windows 读 `USERPROFILE`；皆缺失退当前目录）。
fn home_dir() -> PathBuf {
    xfer_types::home_dir().unwrap_or_else(|| PathBuf::from("."))
}

/// 规范化下载目录：
/// - 展开前导 `~`（`~` → 主目录，`~/x` / `~\x` → 主目录 `x`）；
/// - 相对路径按当前工作目录解析为绝对路径。
///
/// 应用端（开发者模式）常通过 `--dir=~/Downloads` 之类的形式直接传参，
/// 未经 shell 展开 `~`；此处统一处理，避免在程序目录下创建 `~` 或同名目录。
pub fn normalize_dir(dir: &str) -> PathBuf {
    let expanded = if dir == "~" {
        home_dir()
    } else if let Some(rest) = dir.strip_prefix("~/").or_else(|| dir.strip_prefix("~\\")) {
        home_dir().join(rest)
    } else {
        PathBuf::from(dir)
    };
    if expanded.is_absolute() {
        expanded
    } else if let Ok(cwd) = std::env::current_dir() {
        cwd.join(expanded)
    } else {
        expanded
    }
}

/// 读取会话文件（不存在/解析失败返回 None，宽容处理）。
fn read_session_file(path: &std::path::Path) -> Option<Value> {
    let text = std::fs::read_to_string(path).ok()?;
    match serde_json::from_str(&text) {
        Ok(v) => Some(v),
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "会话文件解析失败，忽略");
            None
        }
    }
}

/// 每秒广播进度事件（订阅方推送用，免轮询）；速度按 3s 窗口采样，
/// 避免片级批量落盘导致 1s 窗口在 0 与尖峰间抖动。
///
/// 全程无锁：进度读 [`Task::completed_live`]（原子优先），
/// 速度写 `speed_atomic`（查询侧经 [`Task::speed_live`] 读取），
/// 不与下载落盘热路径争抢任务锁。
async fn speed_ticker(task: Arc<Task>, events: broadcast::Sender<EngineEvent>) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut prev = task.completed_live();
    let mut prev_uploaded = task.uploaded_live();
    let mut sample_count = 1u64;
    loop {
        interval.tick().await;
        sample_count += 1;
        if sample_count >= 3 {
            let cur = task.completed_live();
            task.speed_atomic
                .store(cur.saturating_sub(prev) / sample_count, Ordering::Relaxed);
            let cur_uploaded = task.uploaded_live();
            task.upload_speed_atomic.store(
                cur_uploaded.saturating_sub(prev_uploaded) / sample_count,
                Ordering::Relaxed,
            );
            prev = cur;
            prev_uploaded = cur_uploaded;
            sample_count = 0;
        }
        let _ = events.send(("progress".to_string(), task.gid.0.clone()));
    }
}

// ----------------------------------------------------------------------
// 下载驱动
// ----------------------------------------------------------------------

/// BT 下载驱动：创建引擎、1Hz 进度上报、等待完成/取消。
async fn drive_bt_download(
    _mgr: &TaskManager,
    task: &Arc<Task>,
    cancel: &CancellationToken,
) -> Result<(), TaskFailure> {
    let meta = task.bt_meta.lock().unwrap().clone();
    let magnet_ih = *task.bt_info_hash.lock().unwrap();
    let mut announce_urls = Vec::new();
    let mut udp_announce_urls = Vec::new();
    if let Some(meta) = &meta {
        if let Some(a) = &meta.announce {
            if a.starts_with("udp://") {
                udp_announce_urls.push(a.clone());
            } else {
                announce_urls.push(a.clone());
            }
        }
        for tier in &meta.announce_list {
            for url in tier {
                if url.starts_with("udp://") {
                    udp_announce_urls.push(url.clone());
                } else {
                    announce_urls.push(url.clone());
                }
            }
        }
    } else {
        // 磁力链接：tracker 来自链接的 tr 参数 + 运行时 add_trackers 添加的
        for url in task.bt_trackers.lock().unwrap().iter() {
            if url.starts_with("udp://") {
                udp_announce_urls.push(url.clone());
            } else {
                announce_urls.push(url.clone());
            }
        }
    }

    // 合并全局 tracker 服务器（设置页配置，所有 BT 任务自动注入）
    for url in _mgr.get_global_trackers() {
        if url.starts_with("udp://") {
            if !udp_announce_urls.contains(&url) {
                udp_announce_urls.push(url);
            }
        } else if !announce_urls.contains(&url) {
            announce_urls.push(url);
        }
    }

    let mut rand12 = [0u8; 12];
    getrandom::fill(&mut rand12).map_err(|e| TaskFailure::Bt(format!("随机源失败: {e}")))?;
    // DHT 始终启用（除非 private 种子），作为 tracker 的补充 peer 发现渠道。
    // 即使有 tracker，DHT 也能发现更多 peer，且在 tracker 失效时是唯一来源。
    let enable_dht = meta.as_ref().map(|m| !m.info.private).unwrap_or(true);
    let (max_peers, adaptive) = _mgr.bt_options(task);
    let (dl_limit, ul_limit) = _mgr.rate_limits();
    let (encryption, bt_protocol) = _mgr.bt_modes(task);
    let cfg = TorrentConfig {
        dir: task.dir.clone(),
        peer_id: PeerId::azureus_prefix(&rand12),
        listen_port: 0,
        max_peers,
        adaptive,
        numwant: 50,
        announce_urls,
        udp_announce_urls,
        pipeline: 0, // 自适应 16→256
        enable_dht,
        dht_port: 0,
        encryption,
        bt_protocol,
        download_limit: dl_limit,
        upload_limit: ul_limit,
        seed_mode: false,
        seed_duration: 0,
    };
    let engine = match (&meta, magnet_ih) {
        (Some(m), _) => TorrentEngine::new((**m).clone(), cfg).map_err(TaskFailure::Bt)?,
        (None, Some(ih)) => TorrentEngine::new_magnet(ih, cfg).map_err(TaskFailure::Bt)?,
        _ => return Err(TaskFailure::Bt("BT 任务缺少元信息或磁力 info_hash".into())),
    };
    // 注册活动引擎：全局限速变更时据此下发（任务结束在下方移除）
    _mgr.bt_engines
        .lock()
        .unwrap()
        .insert(task.gid.clone(), engine.clone());
    // 记录数据路径：删除任务时据此清理控制文件与数据文件（磁力链接在元信息到手后补齐）
    if let Some(m) = &meta {
        let mut sh = task.shared.lock().unwrap();
        if sh.path.is_none() {
            sh.path = Some(task.dir.join(&m.info.name));
        }
    }

    // 1Hz 进度/peer 上报
    let prog_task = tokio::spawn({
        let engine = engine.clone();
        let task = task.clone();
        async move {
            let mut iv = tokio::time::interval(Duration::from_secs(1));
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                iv.tick().await;
                let p = engine.progress();
                let peers = engine.peers_info();
                let conn_count = peers.iter().filter(|p| p.connected).count();
                {
                    let mut sh = task.shared.lock().unwrap();
                    sh.completed = p.done;
                    sh.total_len = Some(p.total);
                    sh.file_len = p.total;
                    sh.connections = conn_count;
                }
                // 无锁同步：speed_ticker / snapshot / stat_raw 优先读原子值
                task.completed_atomic.store(p.done, Ordering::Relaxed);
                task.connections_atomic.store(conn_count as u64, Ordering::Relaxed);
                task.uploaded_atomic.store(engine.uploaded(), Ordering::Relaxed);
                *task.bt_peers.lock().unwrap() = peers;
                if p.done >= p.total && p.total > 0 {
                    // 完成后也同步一次再退出
                    break;
                }
            }
        }
    });

    let r = engine.clone().run(cancel.clone()).await;
    prog_task.abort();
    // 引擎已停止：移出限速下发表（无论成功/失败/取消）
    _mgr.bt_engines.lock().unwrap().remove(&task.gid);
    // 取消时同步一次进度与 peer 快照（避免极快完成的任务未被 1Hz ticker 采样到）
    let p = engine.progress();
    {
        let mut sh = task.shared.lock().unwrap();
        sh.completed = p.done;
    }
    task.completed_atomic.store(p.done, Ordering::Relaxed);
    task.uploaded_atomic.store(engine.uploaded(), Ordering::Relaxed);
    *task.bt_peers.lock().unwrap() = engine.peers_info();
    // 磁力链接补齐数据路径：引擎运行中拿到元信息后才能确定落盘路径
    {
        let mut sh = task.shared.lock().unwrap();
        if sh.path.is_none() {
            if let Some(m) = engine.meta() {
                sh.path = Some(task.dir.join(&m.info.name));
            }
        }
    }
    match r {
        Ok(()) => Ok(()),
        Err(e) if e.contains("已取消") => Err(TaskFailure::Cancelled),
        Err(e) => Err(TaskFailure::Bt(e)),
    }
}

/// 同一 URI 瞬态失败的重试上限（含首次共 3 次尝试）。
const URI_TRANSIENT_ATTEMPTS: u32 = 3;

/// 是否为值得同 URI 重试的瞬态失败：连接类/超时/中途断流/5xx
/// 都可立即重试（分片走控制文件续传、单连接走 Range 续传，
/// 重试成本只有剩余部分）；4xx、本地 IO、取消不重试。
fn is_transient_failure(f: &TaskFailure) -> bool {
    use xfer_http::HttpError;
    match f {
        TaskFailure::Http(
            HttpError::Timeout
            | HttpError::Connect(_)
            | HttpError::Protocol(_)
            | HttpError::ShortRead,
        ) => true,
        TaskFailure::Http(HttpError::Http(code)) => *code >= 500,
        _ => false,
    }
}

/// 依次尝试 URI 列表（镜像故障转移），全部失败返回最后一个错误。
/// 瞬态失败先在同 URI 上重试（断点续传式），避免单次网络抖动
/// 直接打失败整个任务。
async fn drive_download(
    mgr: &TaskManager,
    task: &Arc<Task>,
    cancel: &CancellationToken,
) -> Result<(), TaskFailure> {
    let mut last_err: Option<TaskFailure> = None;
    for (idx, uri) in task.uris.iter().enumerate() {
        let mut attempt = 1u32;
        loop {
            match try_uri(mgr, &mgr.client, task, uri, idx, cancel).await {
                Ok(()) => return Ok(()),
                Err(f) if f.is_cancelled() => return Err(f),
                Err(f) => {
                    if is_transient_failure(&f) && attempt < URI_TRANSIENT_ATTEMPTS {
                        attempt += 1;
                        tracing::warn!(
                            gid = %task.gid, uri = idx, attempt,
                            error = %f, "瞬态失败，同 URI 断点续传重试"
                        );
                        let backoff = match attempt {
                            2 => Duration::from_secs(1),
                            _ => Duration::from_secs(3),
                        };
                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {}
                            _ = cancel.cancelled() => return Err(f),
                        }
                        continue;
                    }
                    tracing::warn!(gid = %task.gid, uri = idx, error = %f, "URI 下载失败，切换下一个");
                    last_err = Some(f);
                    break;
                }
            }
        }
    }
    Err(
        last_err.unwrap_or(TaskFailure::Http(xfer_http::HttpError::Protocol(
            "无可用下载地址".into(),
        ))),
    )
}

async fn try_uri(
    mgr: &TaskManager,
    client: &reqwest::Client,
    task: &Arc<Task>,
    uri: &str,
    uri_idx: usize,
    cancel: &CancellationToken,
) -> Result<(), TaskFailure> {
    let probe = xfer_http::probe(client, uri, cancel).await?;
    task.mark_uri_used(uri_idx);

    // 总长度回填
    {
        let mut sh = task.shared.lock().unwrap();
        if let Some(t) = probe.total_len {
            sh.total_len = Some(t);
            sh.file_len = t;
        }
    }

    let path = mgr.resolve_path(task, &probe);

    // 分片请求被服务器忽略过（NotSplittable）时，不再信任其对 Range
    // 的任何承诺——回退单连接时强制从头下载。
    let mut force_fresh = false;

    // —— 多连接分片路径（支持 Range 且已知总长）——
    if let Some(total) = probe.total_len.filter(|t| *t > 0 && probe.accepts_ranges) {
        let ctrl = xfer_http::ctrl_path(&path);
        let existing = existing_len(&path);
        if !ctrl.exists() && existing == total {
            // 无控制文件且长度已齐（旧任务恢复/外部补齐）：直接进入完成校验
            task.shared.lock().unwrap().completed = total;
            task.completed_atomic.store(total, Ordering::Relaxed);
            return finish_http_task(task, &path).await;
        }
        let opts = mgr.split_options(task);
        let stats = xfer_http::SplitStats::new(existing);
        let sampler = spawn_split_sampler(task, &stats);
        let r = xfer_http::download_split(client, uri, &path, total, &opts, cancel, stats.clone())
            .await;
        sampler.abort();
        match r {
            Ok(_) => {
                {
                    let mut sh = task.shared.lock().unwrap();
                    sh.completed = total;
                    sh.connections = 0;
                }
                task.completed_atomic.store(total, Ordering::Relaxed);
                task.connections_atomic.store(0, Ordering::Relaxed);
                return finish_http_task(task, &path).await;
            }
            // 服务器不支持分段请求：文件已被截断为连续前缀、控制文件已删除。
            // 该服务器对 Range 的行为已被证明不可信（probe 通过但区间请求
            // 被忽略），回退单连接时强制从头下载，避免再次落入同样的陷阱。
            Err(xfer_http::HttpError::NotSplittable(msg)) => {
                force_fresh = true;
                tracing::info!(gid = %task.gid, reason = %msg, "服务器不支持分段，回退单连接");
            }
            Err(e) => {
                let mut sh = task.shared.lock().unwrap();
                let completed = stats.completed.load(Ordering::Relaxed);
                sh.completed = completed;
                sh.connections = 0;
                task.completed_atomic.store(completed, Ordering::Relaxed);
                task.connections_atomic.store(0, Ordering::Relaxed);
                return Err(TaskFailure::Http(e));
            }
        }
    }

    // —— 单连接路径（不支持 Range / 未知总长 / 分段回退）——
    // 已存在部分文件：决定续传/重建
    let existing = existing_len(&path);
    let total = probe.total_len;
    let can_resume =
        !force_fresh && probe.accepts_ranges && existing > 0 && total.is_none_or(|t| existing < t);
    let (start, mode) = if can_resume {
        (existing, SinkMode::Resume(existing))
    } else {
        (0, SinkMode::Fresh)
    };

    let mut sink = ResumeSink::new(task.clone(), path, mode);
    let done = xfer_http::download(client, uri, start, cancel, &mut sink).await?;

    // 总长度以传输响应为准（重定向后可能不同）
    {
        let mut sh = task.shared.lock().unwrap();
        if let Some(t) = done.total_len {
            sh.total_len = Some(t);
            sh.file_len = t;
        }
    }

    // 终局进度回填：传输期间进度走无锁原子计数（高频路径免锁），
    // 这里一次性写回共享状态，保持非原子读侧一致。
    let completed = task.completed_live();
    task.shared.lock().unwrap().completed = completed;

    // 传输完整性：EOF 但未达总长视为失败
    {
        let sh = task.shared.lock().unwrap();
        if let Some(t) = sh.total_len {
            if completed < t {
                return Err(TaskFailure::Http(xfer_http::HttpError::Protocol(format!(
                    "传输不完整: {}/{}",
                    completed, t
                ))));
            }
        }
    }

    finish_http_task(task, sink.path.as_path()).await
}

/// HTTP 任务完成收尾：校验和（如配置）。
///
/// 哈希校验走阻塞线程池：大文件整读可能耗时数秒，同步执行会卡住
/// tokio 工作线程（§7.18⑤：async 上下文禁止同步磁盘 IO）。
async fn finish_http_task(task: &Arc<Task>, path: &Path) -> Result<(), TaskFailure> {
    if let Some((algo, expect)) = task.checksum.clone() {
        let p = path.to_path_buf();
        tokio::task::spawn_blocking(move || verify_file_hash(&p, algo, &expect))
            .await
            .map_err(|e| TaskFailure::Checksum(format!("校验任务异常: {e}")))?
            .map_err(TaskFailure::Checksum)?;
    }
    Ok(())
}

/// 解析带单位的大小字符串："4M"/"512k"/"1G"/"1048576"。
fn parse_size_bytes(v: &str) -> Option<u64> {
    let v = v.trim();
    let (num, mult) = match v.as_bytes().last()? {
        b'k' | b'K' => (&v[..v.len() - 1], 1024u64),
        b'm' | b'M' => (&v[..v.len() - 1], 1024 * 1024),
        b'g' | b'G' => (&v[..v.len() - 1], 1024 * 1024 * 1024),
        _ => (v, 1),
    };
    num.trim()
        .parse::<u64>()
        .ok()
        .map(|n| n.saturating_mul(mult))
}

/// 从 addUri/addTorrent 的 options 中提取任务级选项（排除 dir/out/checksum
/// 这类已单独处理的字段）。
fn collect_task_options(opts: &serde_json::Map<String, Value>) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for (k, v) in opts {
        if k == "dir" || k == "out" || k == "checksum" {
            continue;
        }
        let s = match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        m.insert(k.clone(), s);
    }
    m
}

/// 分片下载进度采样：高频轮询无锁统计到任务原子计数器。
///
/// 不锁 `task.shared`：查询/速度路径经 `completed_live` /
/// `connections_live` 直接读原子值；终局时 `try_uri` 会把最终值
/// 回填共享状态（暂停/失败/完成各路径均已覆盖）。
fn spawn_split_sampler(
    task: &Arc<Task>,
    stats: &Arc<xfer_http::SplitStats>,
) -> tokio::task::JoinHandle<()> {
    let task = task.clone();
    let stats = stats.clone();
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(Duration::from_millis(200));
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            iv.tick().await;
            task.completed_atomic
                .store(stats.completed.load(Ordering::Relaxed), Ordering::Relaxed);
            task.connections_atomic.store(
                stats.connections.load(Ordering::Relaxed) as u64,
                Ordering::Relaxed,
            );
        }
    })
}

enum SinkMode {
    Fresh,
    Resume(u64),
}

/// 落盘 + 进度记账：每块写入后以无锁原子计数更新进度
/// （查询侧经 `completed_live` 读取），高频路径零锁竞争。
struct ResumeSink {
    task: Arc<Task>,
    path: PathBuf,
    mode: SinkMode,
    sink: Option<FileSink>,
}

impl ResumeSink {
    fn new(task: Arc<Task>, path: PathBuf, mode: SinkMode) -> Self {
        Self {
            task,
            path,
            mode,
            sink: None,
        }
    }
}

impl xfer_http::TransferSink for ResumeSink {
    fn begin(&mut self, restarted: bool) -> std::io::Result<u64> {
        let sink = if restarted {
            FileSink::create(&self.path)?
        } else {
            match self.mode {
                SinkMode::Fresh => FileSink::create(&self.path)?,
                SinkMode::Resume(off) => FileSink::append_at(&self.path, off)?,
            }
        };
        let base = sink.position();
        self.sink = Some(sink);
        {
            let mut sh = self.task.shared.lock().unwrap();
            sh.completed = base;
            sh.connections = 1; // 单连接模式
        }
        // 原子基线：write_chunk 高频路径与查询路径全程无锁
        self.task.completed_atomic.store(base, Ordering::Relaxed);
        self.task.connections_atomic.store(1, Ordering::Relaxed);
        Ok(base)
    }

    fn write_chunk(&mut self, data: &[u8]) -> std::io::Result<()> {
        let sink = self.sink.as_mut().expect("begin 未调用");
        sink.write(data)?;
        // 无锁进度：每块一次原子 store（原先每块一次 Mutex 获取，
        // 高吞吐下与查询/速度路径激烈竞争）。
        self.task
            .completed_atomic
            .store(sink.position(), Ordering::Relaxed);
        Ok(())
    }

    fn finish(&mut self) -> std::io::Result<u64> {
        let sink = self.sink.as_mut().expect("begin 未调用");
        sink.flush()?;
        Ok(sink.position())
    }
}

/// `file.zip` → `file.1.zip`；无扩展名 `file` → `file.1`。
/// 删除任务的已下载数据文件（含控制文件）。
/// HTTP：目标文件；BT：种子根目录（多文件）或目标文件（单文件）。
fn delete_task_files(task: &Task) {
    delete_task_ctrl(task);
    let path = task.shared.lock().unwrap().path.clone();
    if let Some(p) = path {
        if p.is_dir() {
            let _ = std::fs::remove_dir_all(&p);
            tracing::info!(dir = %p.display(), "已删除任务数据目录");
        } else if p.is_file() {
            let _ = std::fs::remove_file(&p);
            tracing::info!(file = %p.display(), "已删除任务数据文件");
        }
    }
}

/// 删除任务的控制文件（存于引擎数据目录，用户不可见）。
fn delete_task_ctrl(task: &Task) {
    let path = task.shared.lock().unwrap().path.clone();
    if let Some(p) = path {
        let ctrl = xfer_http::ctrl_path(&p);
        if ctrl.exists() {
            let _ = std::fs::remove_file(&ctrl);
        }
    }
}

fn rename_with_counter(name: &str, n: usize) -> String {
    match name.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() => format!("{stem}.{n}.{ext}"),
        _ => format!("{name}.{n}"),
    }
}

/// 将 panic payload 转为可读字符串。
fn panic_downcast(p: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "<unknown panic>".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_dir_keeps_absolute() {
        assert_eq!(normalize_dir("/tmp/a/b"), PathBuf::from("/tmp/a/b"));
    }

    #[test]
    fn normalize_dir_expands_tilde() {
        assert_eq!(normalize_dir("~"), home_dir());
        assert_eq!(normalize_dir("~/Downloads"), home_dir().join("Downloads"));
    }

    #[test]
    fn normalize_dir_resolves_relative_to_cwd() {
        let expected = std::env::current_dir().unwrap().join("rel/dir");
        assert_eq!(normalize_dir("rel/dir"), expected);
    }
}
