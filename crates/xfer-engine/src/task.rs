//! 任务实体：共享状态、控制信号与状态序列化。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Instant, SystemTime};

use serde_json::{json, Map, Value};
use tokio_util::sync::CancellationToken;
use xfer_bencode::TorrentMeta;
use xfer_bt::PeerInfo;
use xfer_storage::HashAlgo;
use xfer_types::Gid;

/// 任务状态（协议字段值）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Waiting,
    Active,
    Paused,
    Complete,
    Error,
    Removed,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Waiting => "waiting",
            Status::Active => "active",
            Status::Paused => "paused",
            Status::Complete => "complete",
            Status::Error => "error",
            Status::Removed => "removed",
        }
    }

    /// 是否为终态（进入停止结果列表）。
    pub fn is_terminal(self) -> bool {
        matches!(self, Status::Complete | Status::Error | Status::Removed)
    }
}

/// 单个 URI 的使用状态（files[].uris[].status）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UriState {
    Used,
    Waiting,
}

impl UriState {
    pub fn as_str(self) -> &'static str {
        match self {
            UriState::Used => "used",
            UriState::Waiting => "waiting",
        }
    }
}

/// 活动下载的取消意图（取消令牌触发后由工作者据此转移状态）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Intent {
    None,
    Pause,
    Remove,
}

/// 引擎侧任务失败分类（映射任务错误码）。
#[derive(Debug, thiserror::Error)]
pub enum TaskFailure {
    #[error(transparent)]
    Http(#[from] xfer_http::HttpError),
    #[error("{0}")]
    Checksum(String),
    /// BT 下载失败（tracker/连接/存储等）。
    #[error("{0}")]
    Bt(String),
    /// 用户取消（pause/remove 触发，与 HTTP 取消语义一致）。
    #[error("已取消")]
    Cancelled,
}

impl TaskFailure {
    /// 任务错误码（0 无错；2 超时；3 资源不存在；5 网络问题；9 校验不符；1 其他）。
    pub fn error_code(&self) -> i64 {
        match self {
            TaskFailure::Http(e) => e.error_code(),
            TaskFailure::Checksum(_) => 9,
            TaskFailure::Bt(_) => 1,
            TaskFailure::Cancelled => 0,
        }
    }

    pub fn is_cancelled(&self) -> bool {
        matches!(
            self,
            TaskFailure::Http(xfer_http::HttpError::Cancelled) | TaskFailure::Cancelled
        )
    }
}

/// 任务的易变状态（进度/状态/错误信息），供查询读、工作者写。
#[derive(Debug)]
pub struct TaskShared {
    pub status: Status,
    pub total_len: Option<u64>,
    pub completed: u64,
    pub download_speed: u64,
    /// 上传速度（字节/秒，仅 BT 任务非零）。
    pub upload_speed: u64,
    /// 累计上传字节数（仅 BT 任务非零）。
    pub uploaded: u64,
    /// 当前活跃连接数（HTTP 分片连接 / BT peer 数）。
    pub connections: usize,
    pub file_len: u64,
    pub path: Option<PathBuf>,
    pub filename: Option<String>,
    pub error_code: i64,
    pub error_message: String,
    /// 累计活跃毫秒（仅 active 状态计数）。
    pub active_ms: u64,
    /// 当前活跃期起点（active 时 Some）。
    pub active_since: Option<Instant>,
}

/// 一个下载任务。
pub struct Task {
    pub gid: Gid,
    pub uris: Vec<String>,
    pub dir: PathBuf,
    /// out 选项指定的文件名（可选）。
    pub out: Option<String>,
    /// checksum 选项（算法=期望值）。
    pub checksum: Option<(HashAlgo, String)>,
    /// 任务级选项（addUri/changeOption 传入，如 split/min-split-size），
    /// 优先于全局选项。
    pub options: Mutex<HashMap<String, String>>,
    /// BT 元信息（addTorrent 任务时为 Some；磁力链接获取元数据后也设为 Some）。
    pub bt_meta: Mutex<Option<Arc<TorrentMeta>>>,
    /// BT 磁力链接 info_hash（add_magnet 任务时 Some，元数据获取前无 bt_meta）。
    pub bt_info_hash: Mutex<Option<[u8; 20]>>,
    /// BT 磁力链接附带 tracker（无 .torrent 时的 announce 来源）。
    /// 运行时可动态添加（add_trackers），用 Mutex 保护并发安全。
    pub bt_trackers: Mutex<Vec<String>>,
    /// BT 任务当前连接 peer 列表（getPeers 查询用）。
    pub bt_peers: Mutex<Vec<PeerInfo>>,
    /// 磁力任务等待文件选择：元数据就绪后自动暂停，等用户在 TUI
    /// 勾选要下载的文件（`bt-file-selection` 任务选项置位）。
    pub awaiting_selection: AtomicBool,
    /// 用户选择的文件索引（None = 全部文件；下载时透传给 BT 引擎）。
    pub selected_files: Mutex<Option<Vec<usize>>>,
    pub created_at: SystemTime,
    pub shared: Mutex<TaskShared>,
    pub uri_states: Mutex<Vec<UriState>>,
    /// 活动下载的取消令牌（暂停恢复时整体替换，令牌不可复用）。
    pub cancel: RwLock<CancellationToken>,
    /// 取消意图（先设置意图再 cancel，保证工作者读到正确意图）。
    pub intent: Mutex<Intent>,
    /// 移除任务时是否连带删除已下载文件（worker 退出后统一执行）。
    pub delete_files: AtomicBool,
    /// 无锁进度计数器：高频写入路径（ResumeSink / split sampler）
    /// 直接 store，speed_ticker / 查询路径 load，避免每次写入都
    /// lock Mutex<TaskShared>。
    pub completed_atomic: AtomicU64,
    /// 无锁连接数计数器：split worker / BT 上报直接 store，
    /// 查询路径 load，避免锁竞争。
    pub connections_atomic: AtomicU64,
    /// 无锁下载速度计数器：speed_ticker store，stat_raw load，
    /// 避免全局统计遍历加锁。
    pub speed_atomic: AtomicU64,
    /// 无锁上传字节计数器：BT 上报任务 store（engine.uploaded()）。
    pub uploaded_atomic: AtomicU64,
    /// 无锁上传速度计数器：speed_ticker 按上传字节差值 store。
    pub upload_speed_atomic: AtomicU64,
}

impl Task {
    pub fn new(
        gid: Gid,
        uris: Vec<String>,
        dir: PathBuf,
        out: Option<String>,
        checksum: Option<(HashAlgo, String)>,
        options: HashMap<String, String>,
    ) -> Self {
        Self {
            uri_states: Mutex::new(vec![UriState::Waiting; uris.len()]),
            gid,
            uris,
            dir,
            out,
            checksum,
            options: Mutex::new(options),
            bt_meta: Mutex::new(None),
            bt_info_hash: Mutex::new(None),
            bt_trackers: Mutex::new(Vec::new()),
            bt_peers: Mutex::new(Vec::new()),
            awaiting_selection: AtomicBool::new(false),
            selected_files: Mutex::new(None),
            created_at: SystemTime::now(),
            shared: Mutex::new(TaskShared {
                status: Status::Waiting,
                total_len: None,
                completed: 0,
                download_speed: 0,
                upload_speed: 0,
                uploaded: 0,
                connections: 0,
                file_len: 0,
                path: None,
                filename: None,
                error_code: 0,
                error_message: String::new(),
                active_ms: 0,
                active_since: None,
            }),
            cancel: RwLock::new(CancellationToken::new()),
            intent: Mutex::new(Intent::None),
            delete_files: AtomicBool::new(false),
            completed_atomic: AtomicU64::new(0),
            connections_atomic: AtomicU64::new(0),
            speed_atomic: AtomicU64::new(0),
            uploaded_atomic: AtomicU64::new(0),
            upload_speed_atomic: AtomicU64::new(0),
        }
    }

    /// 构造 BT 任务（总长/片信息来自元信息）。
    pub fn new_torrent(
        gid: Gid,
        dir: PathBuf,
        meta: Arc<TorrentMeta>,
        options: HashMap<String, String>,
    ) -> Self {
        let total = meta.info.total_length();
        let name = meta.info.name.clone();
        Self {
            uri_states: Mutex::new(Vec::new()),
            gid,
            uris: Vec::new(),
            dir,
            out: None,
            checksum: None,
            options: Mutex::new(options),
            bt_meta: Mutex::new(Some(meta)),
            bt_info_hash: Mutex::new(None),
            bt_trackers: Mutex::new(Vec::new()),
            bt_peers: Mutex::new(Vec::new()),
            awaiting_selection: AtomicBool::new(false),
            selected_files: Mutex::new(None),
            created_at: SystemTime::now(),
            shared: Mutex::new(TaskShared {
                status: Status::Waiting,
                total_len: Some(total),
                completed: 0,
                download_speed: 0,
                upload_speed: 0,
                uploaded: 0,
                connections: 0,
                file_len: total,
                path: None,
                filename: Some(name),
                error_code: 0,
                error_message: String::new(),
                active_ms: 0,
                active_since: None,
            }),
            cancel: RwLock::new(CancellationToken::new()),
            intent: Mutex::new(Intent::None),
            delete_files: AtomicBool::new(false),
            completed_atomic: AtomicU64::new(0),
            connections_atomic: AtomicU64::new(0),
            speed_atomic: AtomicU64::new(0),
            uploaded_atomic: AtomicU64::new(0),
            upload_speed_atomic: AtomicU64::new(0),
        }
    }

    /// 构造磁力链接任务（只有 info_hash，元数据经 ut_metadata 获取）。
    pub fn new_magnet(
        gid: Gid,
        dir: PathBuf,
        info_hash: [u8; 20],
        trackers: Vec<String>,
        display_name: Option<String>,
        options: HashMap<String, String>,
    ) -> Self {
        Self {
            uri_states: Mutex::new(Vec::new()),
            gid,
            uris: Vec::new(),
            dir,
            out: None,
            checksum: None,
            options: Mutex::new(options),
            bt_meta: Mutex::new(None),
            bt_info_hash: Mutex::new(Some(info_hash)),
            bt_trackers: Mutex::new(trackers),
            bt_peers: Mutex::new(Vec::new()),
            awaiting_selection: AtomicBool::new(false),
            selected_files: Mutex::new(None),
            created_at: SystemTime::now(),
            shared: Mutex::new(TaskShared {
                status: Status::Waiting,
                total_len: None,
                completed: 0,
                download_speed: 0,
                upload_speed: 0,
                uploaded: 0,
                connections: 0,
                file_len: 0,
                path: None,
                filename: display_name,
                error_code: 0,
                error_message: String::new(),
                active_ms: 0,
                active_since: None,
            }),
            cancel: RwLock::new(CancellationToken::new()),
            intent: Mutex::new(Intent::None),
            delete_files: AtomicBool::new(false),
            completed_atomic: AtomicU64::new(0),
            connections_atomic: AtomicU64::new(0),
            speed_atomic: AtomicU64::new(0),
            uploaded_atomic: AtomicU64::new(0),
            upload_speed_atomic: AtomicU64::new(0),
        }
    }

    pub fn status(&self) -> Status {
        self.shared.lock().unwrap().status
    }

    pub fn set_status(&self, s: Status) {
        let mut sh = self.shared.lock().unwrap();
        let prev = sh.status;
        sh.status = s;
        // 活跃计时：进入 active 开始计，离开 active 累计并暂停
        let now = Instant::now();
        match (prev, s) {
            (_, Status::Active) if prev != Status::Active => {
                sh.active_since = Some(now);
            }
            (Status::Active, other) if other != Status::Active => {
                if let Some(since) = sh.active_since.take() {
                    sh.active_ms += since.elapsed().as_millis() as u64;
                }
            }
            _ => {}
        }
    }

    /// 当前已用时（毫秒）：累计活跃时间 + 当前活跃期。
    pub fn elapsed_ms(&self) -> u64 {
        let sh = self.shared.lock().unwrap();
        sh.active_ms
            + sh.active_since
                .map(|s| s.elapsed().as_millis() as u64)
                .unwrap_or(0)
    }

    pub fn mark_uri_used(&self, idx: usize) {
        if let Some(st) = self.uri_states.lock().unwrap().get_mut(idx) {
            *st = UriState::Used;
        }
    }

    /// 实时已完成字节：高频写路径（下载落盘）直接更新原子值，
    /// 查询侧无锁读取；原子值为 0（任务未开始/会话恢复）回退共享状态。
    pub fn completed_live(&self) -> u64 {
        let v = self.completed_atomic.load(Ordering::Relaxed);
        if v > 0 {
            v
        } else {
            self.shared.lock().unwrap().completed
        }
    }

    /// 实时下载速度（字节/秒）：无锁原子优先。
    pub fn speed_live(&self) -> u64 {
        let v = self.speed_atomic.load(Ordering::Relaxed);
        if v > 0 {
            v
        } else {
            self.shared.lock().unwrap().download_speed
        }
    }

    /// 实时活跃连接数：无锁原子优先。
    pub fn connections_live(&self) -> usize {
        let v = self.connections_atomic.load(Ordering::Relaxed);
        if v > 0 {
            v as usize
        } else {
            self.shared.lock().unwrap().connections
        }
    }

    /// 实时上传速度（字节/秒）：无锁原子优先。
    pub fn upload_speed_live(&self) -> u64 {
        let v = self.upload_speed_atomic.load(Ordering::Relaxed);
        if v > 0 {
            v
        } else {
            self.shared.lock().unwrap().upload_speed
        }
    }

    /// 实时累计上传字节：无锁原子优先。
    pub fn uploaded_live(&self) -> u64 {
        let v = self.uploaded_atomic.load(Ordering::Relaxed);
        if v > 0 {
            v
        } else {
            self.shared.lock().unwrap().uploaded
        }
    }
}

/// 状态快照（一次性提取，供两种序列化使用，避免反复加锁）。
pub struct TaskSnapshot {
    pub gid: String,
    pub status: Status,
    pub total_len: Option<u64>,
    pub completed: u64,
    pub download_speed: u64,
    /// 上传速度（字节/秒，仅 BT 任务非零）。
    pub upload_speed: u64,
    /// 累计上传字节数（仅 BT 任务非零）。
    pub uploaded: u64,
    pub connections: usize,
    pub file_len: u64,
    pub path: String,
    pub dir: String,
    /// 任务显示名（HTTP 为文件名；磁力为 magnet dn / 种子 name）。
    pub filename: Option<String>,
    pub error_code: i64,
    pub error_message: String,
    pub uris: Vec<(String, UriState)>,
    /// 已用时间（毫秒，仅 active 状态累计；暂停/等待冻结）。
    pub elapsed_ms: u64,
}

pub fn snapshot(task: &Task) -> TaskSnapshot {
    // 无锁实时值先行：*_live 回退路径会锁 shared，
    // 必须在持有 shared 锁之前求值，否则自我死锁。
    let completed = task.completed_live();
    let download_speed = task.speed_live();
    let upload_speed = task.upload_speed_live();
    let uploaded = task.uploaded_live();
    let connections = task.connections_live();
    let sh = task.shared.lock().unwrap();
    let states = task.uri_states.lock().unwrap();
    TaskSnapshot {
        gid: task.gid.0.clone(),
        status: sh.status,
        total_len: sh.total_len,
        completed,
        download_speed,
        upload_speed,
        uploaded,
        connections,
        file_len: sh.file_len,
        path: sh
            .path
            .as_ref()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default(),
        dir: task.dir.to_string_lossy().to_string(),
        filename: sh.filename.clone(),
        error_code: sh.error_code,
        error_message: sh.error_message.clone(),
        elapsed_ms: sh.active_ms
            + sh.active_since
                .map(|s| s.elapsed().as_millis() as u64)
                .unwrap_or(0),
        uris: task
            .uris
            .iter()
            .enumerate()
            .map(|(i, u)| {
                (
                    u.clone(),
                    states.get(i).copied().unwrap_or(UriState::Waiting),
                )
            })
            .collect(),
    }
}

/// 按协议字段过滤；None 表示不过滤。
pub fn filter_keys(v: Value, keys: Option<&[String]>) -> Value {
    match keys {
        None => v,
        Some(ks) => {
            let mut m = v.as_object().cloned().unwrap_or_default();
            m.retain(|k, _| ks.iter().any(|want| want == k));
            Value::Object(m)
        }
    }
}

/// 任务状态 → 前端兼容协议 JSON（数值以字符串承载）。
pub fn status_json(task: &Task) -> Value {
    let s = snapshot(task);
    let sel = task.selected_files.lock().unwrap().clone();
    let is_selected = |i: usize| match &sel {
        None => true,
        Some(v) => v.contains(&i),
    };
    let files = if let Some(meta) = &*task.bt_meta.lock().unwrap() {
        let total_done = s.completed;
        meta.info
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| {
                // M2 不细分文件进度：completedLength 估算按字节占比
                let frac = if meta.info.total_length() == 0 {
                    0
                } else {
                    f.length as u128 * total_done as u128 / meta.info.total_length() as u128
                };
                json!({
                    "index": (i + 1).to_string(),
                    "path": format!("{}/{}", meta.info.name, f.path.join("/")),
                    "length": f.length.to_string(),
                    "completedLength": frac.to_string(),
                    "selected": is_selected(i).to_string(),
                    "uris": [],
                })
            })
            .collect()
    } else if task.bt_info_hash.lock().unwrap().is_some() {
        // 磁力任务：元数据获取中，尚无文件布局
        Vec::new()
    } else {
        let uris: Vec<Value> = s
            .uris
            .iter()
            .map(|(u, st)| json!({ "uri": u, "status": st.as_str() }))
            .collect();
        vec![json!({
            "index": "1",
            "path": s.path,
            "length": s.file_len.to_string(),
            "completedLength": s.completed.to_string(),
            "selected": "true",
            "uris": uris,
        })]
    };
    let (num_pieces, piece_length) = match &*task.bt_meta.lock().unwrap() {
        Some(m) => (m.info.piece_count(), m.info.piece_length),
        None => (0, 0),
    };
    let num_seeders = task
        .bt_peers
        .lock()
        .unwrap()
        .iter()
        .filter(|p| p.seed)
        .count();
    let seeder = s.completed > 0 && s.total_len.is_some_and(|t| s.completed >= t && t > 0);
    let mut m = Map::new();
    m.insert("gid".into(), json!(s.gid));
    m.insert("status".into(), json!(s.status.as_str()));
    m.insert(
        "totalLength".into(),
        json!(s.total_len.unwrap_or(0).to_string()),
    );
    m.insert("completedLength".into(), json!(s.completed.to_string()));
    m.insert("uploadLength".into(), json!(s.uploaded.to_string()));
    m.insert("downloadSpeed".into(), json!(s.download_speed.to_string()));
    m.insert("uploadSpeed".into(), json!(s.upload_speed.to_string()));
    m.insert("bitfield".into(), json!(""));
    m.insert("connections".into(), json!(s.connections.to_string()));
    m.insert("errorCode".into(), json!(s.error_code.to_string()));
    m.insert("errorMessage".into(), json!(s.error_message));
    m.insert("elapsedMs".into(), json!(s.elapsed_ms.to_string()));
    m.insert("belongsTo".into(), json!("0"));
    m.insert("dir".into(), json!(s.dir));
    m.insert("files".into(), Value::Array(files));
    m.insert("numSeeders".into(), json!(num_seeders.to_string()));
    m.insert("seeder".into(), json!(seeder.to_string()));
    m.insert("numPieces".into(), json!(num_pieces.to_string()));
    m.insert("pieceLength".into(), json!(piece_length.to_string()));
    Value::Object(m)
}

/// 任务状态 → 原生协议 JSON（数值字段为真实 JSON 数值）。
pub fn status_json_native(task: &Task) -> Value {
    let s = snapshot(task);
    let files = if let Some(meta) = &*task.bt_meta.lock().unwrap() {
        let total_done = s.completed;
        meta.info
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let frac = if meta.info.total_length() == 0 {
                    0
                } else {
                    (f.length as u128 * total_done as u128 / meta.info.total_length() as u128)
                        as u64
                };
                json!({
                    "index": i + 1,
                    "path": format!("{}/{}", meta.info.name, f.path.join("/")),
                    "length": f.length,
                    "completedLength": frac,
                    "selected": true,
                    "uris": [],
                })
            })
            .collect()
    } else if task.bt_info_hash.lock().unwrap().is_some() {
        // 磁力任务：元数据获取中，尚无文件布局
        Vec::new()
    } else {
        let uris: Vec<Value> = s
            .uris
            .iter()
            .map(|(u, st)| json!({ "uri": u, "status": st.as_str() }))
            .collect();
        vec![json!({
            "index": 1,
            "path": s.path,
            "length": s.file_len,
            "completedLength": s.completed,
            "selected": true,
            "uris": uris,
        })]
    };
    let (num_pieces, piece_length) = match &*task.bt_meta.lock().unwrap() {
        Some(m) => (m.info.piece_count(), m.info.piece_length),
        None => (0, 0),
    };
    let num_seeders = task
        .bt_peers
        .lock()
        .unwrap()
        .iter()
        .filter(|p| p.seed)
        .count();
    let seeder = s.completed > 0 && s.total_len.is_some_and(|t| s.completed >= t && t > 0);
    json!({
        "gid": s.gid,
        "status": s.status.as_str(),
        "totalLength": s.total_len.unwrap_or(0),
        "completedLength": s.completed,
        "filename": s.filename,
        "uploadLength": s.uploaded,
        "downloadSpeed": s.download_speed,
        "uploadSpeed": s.upload_speed,
        "bitfield": "",
        "connections": s.connections,
        "errorCode": s.error_code,
        "errorMessage": s.error_message,
        "elapsedMs": s.elapsed_ms,
        "dir": s.dir,
        "files": files,
        "numSeeders": num_seeders,
        "seeder": seeder,
        "numPieces": num_pieces,
        "pieceLength": piece_length,
        "awaitingSelection": task.awaiting_selection.load(Ordering::Relaxed),
    })
}
