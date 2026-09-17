//! 任务实体：共享状态、控制信号与状态序列化。

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Instant, SystemTime};

use serde_json::{json, Map, Value};
use tokio_util::sync::CancellationToken;
use xfer_bencode::{Info, TorrentMeta};
use xfer_bt::PeerInfo;
use xfer_storage::{files_done_bytes, HashAlgo};
use xfer_types::Gid;
use crate::manager::parse_size_bytes;

/// 任务状态（协议字段值）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Waiting,
    Active,
    /// BT 下载完成后的做种状态（活跃，继续上传；可暂停/停止）。
    Seeding,
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
            Status::Seeding => "seeding",
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
    /// 停止做种：seed 模式运行中用户手动结束 → 任务转完成。
    StopSeeding,
    /// 磁力单文件自动续下：元数据就绪后发现单文件布局，无需用户
    /// 选择——工作者重启后转回 Waiting 重新入队，以全量选择续下。
    Restart,
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
    /// 任务 URI 列表（可经 task.changeUri 运行时更新，Mutex 保护）。
    pub uris: Mutex<Vec<String>>,
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
    /// BT 任务本端已完成片位图（wire 语义字节流，驱动侧 1Hz 同步；
    /// status 查询时序列化为 aria2 兼容 hex 字符串）。任务暂停后保留
    /// 最后已知状态供 UI 展示；非 BT 任务恒为空。
    pub bt_bitfield: Mutex<Vec<u8>>,
    /// HTTP 任务实时分片位图（下载启动时挂接，写线程按落盘区间增量
    /// 维护；BT 任务不使用——分片信息来自 bt_meta）。暂停后保留最后
    /// 已知状态供 UI 展示；未知总长或不支持 Range 的任务为 None。
    pub http_pieces: RwLock<Option<Arc<xfer_http::PieceTrack>>>,
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
    /// 无锁**接收**字节计数器：BT 上报任务 store（engine.received_total()），
    /// 字节级实时（含未落盘块）。speed_ticker 的速度窗口用它采样——
    /// BT 按片落盘，低速率下某秒 0 片完成会让完成字节差值归零
    /// （peer 明明在传输，任务速度却显示 0）。
    pub received_atomic: AtomicU64,
    /// 无锁上传速度计数器：speed_ticker 按上传字节差值 store。
    pub upload_speed_atomic: AtomicU64,
    /// 单任务下载限速（bytes/s，0 = 跟随全局）：`max-download-limit`
    /// 任务选项的原子镜像。构造/恢复/changeOption 时由
    /// [`Task::sync_task_limits`] 从 options 重建；实际生效值由
    /// Manager 按单任务优先覆盖（未设置跟随全局）合成后下发。
    pub task_dl_limit: AtomicU64,
    /// 单任务上传限速（bytes/s，0 = 跟随全局）：`max-upload-limit`
    /// 任务选项的原子镜像（仅 BT 任务的传输路径消费）。
    pub task_ul_limit: AtomicU64,
    /// 任务级 HTTP 下载限速器：所有连接（单连接 + split 多连接）共享，
    /// rate = 单任务优先覆盖（未设置跟随全局）。驱动启动与限速变更时由
    /// Manager 同步。
    pub http_limiter: OnceLock<Arc<xfer_http::RateLimiter>>,
    /// 完成/错误时刻（Unix 毫秒；0 = 未知）。终态转移时设置，
    /// 会话持久化保存，重启恢复后客户端仍可显示完成时间。
    pub finished_at: AtomicU64,
    /// 平均速度累计——活动下载阶段（Status::Active，不含做种）每秒
    /// 记一次：avg_active_ms += 1000、avg_bytes += 完成字节增量。
    /// averageSpeed = avg_bytes * 1000 / avg_active_ms，随会话持久化，
    /// 重启续传后均值不漂移。
    pub avg_active_ms: AtomicU64,
    pub avg_bytes: AtomicU64,
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
        let (dl0, ul0) = limits_from_options(&options);
        Self {
            uri_states: Mutex::new(vec![UriState::Waiting; uris.len()]),
            gid,
            uris: Mutex::new(uris),
            dir,
            out,
            checksum,
            options: Mutex::new(options),
            bt_meta: Mutex::new(None),
            bt_info_hash: Mutex::new(None),
            bt_trackers: Mutex::new(Vec::new()),
            bt_peers: Mutex::new(Vec::new()),
            bt_bitfield: Mutex::new(Vec::new()),
            http_pieces: RwLock::new(None),
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
            received_atomic: AtomicU64::new(0),
            upload_speed_atomic: AtomicU64::new(0),
            task_dl_limit: AtomicU64::new(dl0),
            task_ul_limit: AtomicU64::new(ul0),
            http_limiter: OnceLock::new(),
            finished_at: AtomicU64::new(0),
            avg_active_ms: AtomicU64::new(0),
            avg_bytes: AtomicU64::new(0),
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
        let (dl0, ul0) = limits_from_options(&options);
        Self {
            uri_states: Mutex::new(Vec::new()),
            gid,
            uris: Mutex::new(Vec::new()),
            dir,
            out: None,
            checksum: None,
            options: Mutex::new(options),
            bt_meta: Mutex::new(Some(meta)),
            bt_info_hash: Mutex::new(None),
            bt_trackers: Mutex::new(Vec::new()),
            bt_peers: Mutex::new(Vec::new()),
            bt_bitfield: Mutex::new(Vec::new()),
            http_pieces: RwLock::new(None),
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
            received_atomic: AtomicU64::new(0),
            upload_speed_atomic: AtomicU64::new(0),
            task_dl_limit: AtomicU64::new(dl0),
            task_ul_limit: AtomicU64::new(ul0),
            http_limiter: OnceLock::new(),
            finished_at: AtomicU64::new(0),
            avg_active_ms: AtomicU64::new(0),
            avg_bytes: AtomicU64::new(0),
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
        let (dl0, ul0) = limits_from_options(&options);
        Self {
            uri_states: Mutex::new(Vec::new()),
            gid,
            uris: Mutex::new(Vec::new()),
            dir,
            out: None,
            checksum: None,
            options: Mutex::new(options),
            bt_meta: Mutex::new(None),
            bt_info_hash: Mutex::new(Some(info_hash)),
            bt_trackers: Mutex::new(trackers),
            bt_peers: Mutex::new(Vec::new()),
            bt_bitfield: Mutex::new(Vec::new()),
            http_pieces: RwLock::new(None),
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
            received_atomic: AtomicU64::new(0),
            upload_speed_atomic: AtomicU64::new(0),
            task_dl_limit: AtomicU64::new(dl0),
            task_ul_limit: AtomicU64::new(ul0),
            http_limiter: OnceLock::new(),
            finished_at: AtomicU64::new(0),
            avg_active_ms: AtomicU64::new(0),
            avg_bytes: AtomicU64::new(0),
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
    /// 从任务 options 重建单任务限速原子镜像（构造/恢复/changeOption
    /// 后调用）。取值非法时按 0（跟随全局）处理。
    pub fn sync_task_limits(&self) {
        let opts = self.options.lock().unwrap();
        let dl = opts
            .get("max-download-limit")
            .and_then(|v| parse_size_bytes(v))
            .unwrap_or(0);
        let ul = opts
            .get("max-upload-limit")
            .and_then(|v| parse_size_bytes(v))
            .unwrap_or(0);
        self.task_dl_limit.store(dl, Ordering::Relaxed);
        self.task_ul_limit.store(ul, Ordering::Relaxed);
    }

    /// 任务级 HTTP 限速器（惰性创建，rate 由 Manager 同步维护）。
    pub fn http_task_limiter(&self) -> Arc<xfer_http::RateLimiter> {
        self.http_limiter
            .get_or_init(|| xfer_http::RateLimiter::new(0))
            .clone()
    }

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

    /// 实时累计**接收**字节（本轮运行，字节级）：仅 BT 驱动上报。
    /// 无回退到 shared.completed——语义是「本轮收到的字节数」，
    /// 与完成字节（含 resume 历史）无关。
    pub fn received_live(&self) -> u64 {
        self.received_atomic.load(Ordering::Relaxed)
    }
}

/// 从任务选项解析单任务限速初值（bytes/s，0 = 跟随全局）。
fn limits_from_options(options: &HashMap<String, String>) -> (u64, u64) {
    let dl = options
        .get("max-download-limit")
        .and_then(|v| parse_size_bytes(v))
        .unwrap_or(0);
    let ul = options
        .get("max-upload-limit")
        .and_then(|v| parse_size_bytes(v))
        .unwrap_or(0);
    (dl, ul)
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
    /// 完成/错误时刻（Unix 毫秒；0 = 未知）。
    pub finished_at: u64,
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
        finished_at: task.finished_at.load(Ordering::Relaxed),
        uris: task
            .uris
            .lock()
            .unwrap()
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

/// wire 位图字节流 → aria2 兼容 hex 字符串（每片 1 bit，字节内高位在前）。
/// 空位图（非 BT 任务 / 元数据未就绪）返回空串。
fn bitfield_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

/// wire 位图第 i 位（BEP 3：字节内高位在前）。
fn bitfield_bit(bytes: &[u8], i: u32) -> bool {
    match bytes.get((i / 8) as usize) {
        Some(b) => b & (0x80 >> (i % 8)) != 0,
        None => false,
    }
}

/// 每文件已完成字节（BT 任务，对应文件表「进度」列的 completedLength）。
///
/// 逐片按位图累加「已完成片落在该文件内的段长」：跨文件边界的片必须按
/// 段长归属拆分到两侧文件；未选中的文件恒为 0 —— 边界片在未选一侧既不
/// 下发也不落盘（[`xfer_storage::PieceStore`] 写入时跳过缺席句柄）。
///
/// 曾用「文件长度 × 总进度 / 全部文件总长」估算，导致两个可见错误：
/// 未勾选下载的文件也显示进度（如 80% / 11.4 MB），以及各文件之和与
/// 任务进度对不上。逐片归属后文件之和 = 已完成片覆盖字节，口径自洽。
///
/// `complete` 仅在位图缺失（旧会话）时用于兜底：无片数据时不得再退回
/// 比例估算，只能按任务是否完整决定「整文件完成」或 0。
fn bt_files_done_bytes(
    info: &Info,
    bitfield: &[u8],
    sel: Option<&[usize]>,
    complete: bool,
) -> Vec<u64> {
    // 位图缺失（旧会话无 btBitfield 字段 / 引擎未运行）：若任务已完整，
    // 按「选中的文件整文件已完成」兜底；否则只能报 0（无片数据无法细分，
    // 不能凭空按比例估算——那正是本次修复前的错误行为）
    if bitfield.is_empty() {
        if !complete {
            return vec![0; info.files.len()];
        }
        return info
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let selected = sel.is_none_or(|s| s.contains(&i));
                if selected {
                    f.length
                } else {
                    0
                }
            })
            .collect();
    }
    let mut bounds: Vec<(u64, u64)> = Vec::with_capacity(info.files.len());
    let mut offset = 0u64;
    for f in &info.files {
        bounds.push((offset, f.length));
        offset += f.length;
    }
    let mut done = files_done_bytes(info.piece_length, info.total_length(), &bounds, |i| {
        bitfield_bit(bitfield, i)
    });
    if let Some(sel) = sel {
        let set: HashSet<usize> = sel.iter().copied().collect();
        for (i, v) in done.iter_mut().enumerate() {
            if !set.contains(&i) {
                *v = 0;
            }
        }
    }
    done
}

/// 需下载片位图（wire 位图字节；全选/无选择返回空）。
///
/// 勾选部分文件时，跨选/未选边界的片仍需下载（片不可拆分），这类片在
/// 位图中为「需要」；只属于未选文件的片为「不需要」，永远不会置位。
/// 界面据此把「未选择，无需下载」的片与「未下载」区分开——否则任务
/// 已 100% 完成时，末尾仍会残留几格灰色分片，看起来像没下完。
fn wanted_bitfield(info: &Info, sel: Option<&[usize]>) -> Vec<u8> {
    let Some(sel) = sel else {
        return Vec::new();
    };
    let count = info.piece_count();
    if count == 0 || info.piece_length == 0 {
        return Vec::new();
    }
    let set: HashSet<usize> = sel.iter().copied().collect();
    let mut needed = vec![false; count as usize];
    let mut offset = 0u64;
    for (fi, f) in info.files.iter().enumerate() {
        if f.length > 0 && set.contains(&fi) {
            // 文件覆盖的片区间（首片/末片可能与其他文件共享）
            let first = offset / info.piece_length;
            let last = (offset + f.length - 1) / info.piece_length;
            for i in first..=last.min(count as u64 - 1) {
                needed[i as usize] = true;
            }
        }
        offset += f.length;
    }
    let mut bf = vec![0u8; (count as usize).div_ceil(8)];
    for (i, want) in needed.iter().enumerate() {
        if *want {
            bf[i / 8] |= 0x80 >> (i % 8);
        }
    }
    bf
}

/// 计算 BT 任务的 info_hash 十六进制表示（None = 非 BT 任务）。
/// .torrent 任务 bt_info_hash 不落盘，回退取 bt_meta 解析时计算的哈希。
fn info_hash_hex(task: &Task) -> Option<String> {    if let Some(h) = &*task.bt_info_hash.lock().unwrap() {
        return Some(h.iter().map(|b| format!("{b:02x}")).collect());
    }
    task.bt_meta
        .lock()
        .unwrap()
        .as_ref()
        .map(|m| m.info_hash.iter().map(|b| format!("{b:02x}")).collect())
}

/// 合成 aria2 风格 `bittorrent` 对象（前端 BT 识别与任务命名依赖）：
/// - 非 BT 任务：`Null`（前端判 BT 依据 `task.bittorrent` 真值）；
/// - 元数据就绪（.torrent / 磁力已取回元信息）：`{"info": {"name", "hash"}}`；
/// - 磁力元数据获取中：`{}`（前端据此显示"获取元数据中"）。
fn bittorrent_json(task: &Task, hash: Option<&str>) -> Value {
    let Some(hash) = hash else {
        return Value::Null;
    };
    match task.bt_meta.lock().unwrap().as_ref().map(|m| m.info.name.clone()) {
        Some(name) => json!({
            "info": {
                "name": name,
                "hash": hash,
            },
        }),
        // 磁力任务：元数据获取中，尚无 info 字段
        None => json!({}),
    }
}

/// 平均速度（字节/秒）：活动下载阶段累计字节 / 活动时长。
/// 两者均随会话持久化（重启续传后均值不漂移），做种期不累计不稀释。
/// 用 bytes*1000/ms 而非 bytes/(ms/1000)：后者在 ms<2000 时除数
/// 截断会把均值放大（ms=1999 时误差近 2 倍），开头几秒数值虚高。
fn average_speed_of(task: &Task) -> u64 {
    let ms = task.avg_active_ms.load(Ordering::Relaxed);
    if ms < 1000 {
        return 0;
    }
    task.avg_bytes
        .load(Ordering::Relaxed)
        .saturating_mul(1000)
        / ms
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
        // 每文件已完成字节由 piece 位图推导（未选文件恒 0），
        // 不再按「文件占比 × 总进度」估算
        let bitfield = task.bt_bitfield.lock().unwrap().clone();
        let complete = s.total_len.is_some_and(|t| t > 0 && s.completed >= t);
        let per_file = bt_files_done_bytes(&meta.info, &bitfield, sel.as_deref(), complete);
        meta.info
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| {
                json!({
                    "index": (i + 1).to_string(),
                    "path": meta.info.file_rel_path(f),
                    "length": f.length.to_string(),
                    "completedLength": per_file.get(i).copied().unwrap_or(0).to_string(),
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
            "selected": is_selected(0).to_string(),
            "uris": uris,
        })]
    };
    // 分片信息：BT 来自元信息；HTTP 来自分片跟踪（写线程按落盘区间
    // 增量维护），未知总长或不支持 Range 时为空。
    // partial_bitfield：HTTP 分片部分下载位图（BT 任务为空）。
    // wanted_bitfield：BT 需下载片位图（全选/无选择为空）——界面据此把
    // 「未选择，无需下载」的片与「未下载」区分开。
    let (num_pieces, piece_length, status_bitfield, partial_bitfield, wanted_bitfield) = {
        let meta = task.bt_meta.lock().unwrap();
        if let Some(m) = &*meta {
            (
                m.info.piece_count() as u64,
                m.info.piece_length,
                task.bt_bitfield.lock().unwrap().clone(),
                Vec::new(),
                wanted_bitfield(&m.info, sel.as_deref()),
            )
        } else {
            match task.http_pieces.read().unwrap().as_ref() {
                Some(p) => (
                    p.num_pieces() as u64,
                    p.piece_len(),
                    p.bitfield(),
                    p.partial_bitfield(),
                    Vec::new(),
                ),
                None => (0, 0, Vec::new(), Vec::new(), Vec::new()),
            }
        }
    };
    let num_seeders = task
        .bt_peers
        .lock()
        .unwrap()
        .iter()
        .filter(|p| p.seed)
        .count();
    let seeder = task.bt_meta.lock().unwrap().is_some()
        && s.completed > 0
        && s.total_len.is_some_and(|t| s.completed >= t && t > 0);
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
    // 平均速度（应用端进度窗口/任务详情直取引擎，1Hz 刷新）
    m.insert(
        "averageSpeed".into(),
        json!(average_speed_of(task).to_string()),
    );
    m.insert("bitfield".into(), json!(bitfield_hex(&status_bitfield)));
    m.insert("partialBitfield".into(), json!(bitfield_hex(&partial_bitfield)));
    // 需下载片位图（未选择文件覆盖的片为 0；全选时为空串）
    m.insert(
        "wantedBitfield".into(),
        json!(bitfield_hex(&wanted_bitfield)),
    );
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
    // 做种分享率（uploaded/total，0 时为 0）
    let seed_ratio = if s.total_len.unwrap_or(0) > 0 {
        s.uploaded as f64 / s.total_len.unwrap() as f64
    } else {
        0.0
    };
    m.insert("seedRatio".into(), json!(format!("{:.3}", seed_ratio)));
    // BT 标识：bittorrent 对象（前端任务命名 / BT 识别依赖）与 infoHash
    let info_hash = info_hash_hex(task);
    if info_hash.is_some() {
        m.insert(
            "bittorrent".into(),
            bittorrent_json(task, info_hash.as_deref()),
        );
        m.insert("infoHash".into(), json!(info_hash));
    }
    Value::Object(m)
}

/// 任务状态 → 原生协议 JSON（数值字段为真实 JSON 数值）。
pub fn status_json_native(task: &Task) -> Value {
    let s = snapshot(task);
    // 文件选择状态（aria2 兼容编码同源）：None = 全选
    let sel = task.selected_files.lock().unwrap().clone();
    let is_selected = |i: usize| match &sel {
        None => true,
        Some(v) => v.contains(&i),
    };
    let files = if let Some(meta) = &*task.bt_meta.lock().unwrap() {
        // 每文件已完成字节由 piece 位图推导（未选文件恒 0），
        // 不再按「文件占比 × 总进度」估算
        let bitfield = task.bt_bitfield.lock().unwrap().clone();
        let complete = s.total_len.is_some_and(|t| t > 0 && s.completed >= t);
        let per_file = bt_files_done_bytes(&meta.info, &bitfield, sel.as_deref(), complete);
        meta.info
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| {
                json!({
                    "index": i + 1,
                    "path": meta.info.file_rel_path(f),
                    "length": f.length,
                    "completedLength": per_file.get(i).copied().unwrap_or(0),
                    "selected": is_selected(i),
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
            "selected": is_selected(0),
            "uris": uris,
        })]
    };
    // 分片信息：BT 来自元信息；HTTP 来自分片跟踪（写线程按落盘区间
    // 增量维护），未知总长或不支持 Range 时为空。
    // partial_bitfield：HTTP 分片部分下载位图（BT 任务为空）。
    // wanted_bitfield：BT 需下载片位图（全选/无选择为空）——界面据此把
    // 「未选择，无需下载」的片与「未下载」区分开。
    let (num_pieces, piece_length, status_bitfield, partial_bitfield, wanted_bitfield) = {
        let meta = task.bt_meta.lock().unwrap();
        if let Some(m) = &*meta {
            (
                m.info.piece_count() as u64,
                m.info.piece_length,
                task.bt_bitfield.lock().unwrap().clone(),
                Vec::new(),
                wanted_bitfield(&m.info, sel.as_deref()),
            )
        } else {
            match task.http_pieces.read().unwrap().as_ref() {
                Some(p) => (
                    p.num_pieces() as u64,
                    p.piece_len(),
                    p.bitfield(),
                    p.partial_bitfield(),
                    Vec::new(),
                ),
                None => (0, 0, Vec::new(), Vec::new(), Vec::new()),
            }
        }
    };
    let num_seeders = task
        .bt_peers
        .lock()
        .unwrap()
        .iter()
        .filter(|p| p.seed)
        .count();
    let seeder = task.bt_meta.lock().unwrap().is_some()
        && s.completed > 0
        && s.total_len.is_some_and(|t| s.completed >= t && t > 0);
    let hash = info_hash_hex(task);
    json!({
        "gid": s.gid,
        "status": s.status.as_str(),
        "totalLength": s.total_len.unwrap_or(0),
        "completedLength": s.completed,
        "filename": s.filename,
        "uploadLength": s.uploaded,
        "downloadSpeed": s.download_speed,
        "uploadSpeed": s.upload_speed,
        // 平均速度（应用端进度窗口/任务详情直取引擎，1Hz 刷新）
        "averageSpeed": average_speed_of(task),
        "bitfield": bitfield_hex(&status_bitfield),
        "partialBitfield": bitfield_hex(&partial_bitfield),
        // 需下载片位图（未选择文件覆盖的片为 0；全选时为空串）
        "wantedBitfield": bitfield_hex(&wanted_bitfield),
        "connections": s.connections,
        "errorCode": s.error_code,
        "errorMessage": s.error_message,
        "elapsedMs": s.elapsed_ms,
        "finishedAt": s.finished_at,
        "dir": s.dir,
        "files": files,
        "numSeeders": num_seeders,
        "seeder": seeder,
        "numPieces": num_pieces,
        "pieceLength": piece_length,
        "awaitingSelection": task.awaiting_selection.load(Ordering::Relaxed),
        // 做种分享率（uploaded/total，0 时为 0）
        "seedRatio": if s.total_len.unwrap_or(0) > 0 {
            s.uploaded as f64 / s.total_len.unwrap() as f64
        } else {
            0.0
        },
        // BT 标识：bittorrent 对象（前端任务命名 / BT 识别依赖）与 infoHash
        "infoHash": hash,
        "bittorrent": bittorrent_json(task, hash.as_deref()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use xfer_bencode::FileEntry;

    /// 构造测试用 info：片长 10，文件长度按参数（片哈希用占位，仅几何参与计算）。
    fn info_of(lengths: &[u64]) -> Info {
        let files: Vec<FileEntry> = lengths
            .iter()
            .enumerate()
            .map(|(i, len)| FileEntry {
                path: vec![format!("f{i}.bin")],
                length: *len,
            })
            .collect();
        let total: u64 = lengths.iter().sum();
        let count = total.div_ceil(10) as usize;
        Info {
            name: "t".into(),
            piece_length: 10,
            pieces: vec![[0u8; 20]; count],
            files,
            // 助手构造的是 `files` 列表形态（BEP 3 多文件模式），
            // 结构位如实置 true：哪怕只有 1 项也不能靠 files.len() 推断
            multi_file: true,
            private: false,
        }
    }

    fn bits_from(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn files_done_bytes_attributes_per_piece_segment() {
        // 文件 15 / 10 / 5，片长 10：片 1 跨 0-1，片 2 跨 1-2
        let info = info_of(&[15, 10, 5]);
        // 位图 0b111xxxxx → 三片全完成
        let bf = bits_from("e0");
        assert_eq!(
            bt_files_done_bytes(&info, &bf, None, false),
            vec![15, 10, 5]
        );
        // 只完成片 0（0x80）：未选文件的字节不得因「整体有进度」被估算出来
        let bf0 = bits_from("80");
        assert_eq!(bt_files_done_bytes(&info, &bf0, None, false), vec![10, 0, 0]);
        // 只完成片 1（0x40，跨文件 0/1）：两侧各计自己那 5 字节
        let bf1 = bits_from("40");
        assert_eq!(bt_files_done_bytes(&info, &bf1, None, false), vec![5, 5, 0]);
    }

    #[test]
    fn files_done_bytes_zeroes_unselected_files() {
        let info = info_of(&[15, 10, 5]);
        let all_done = bits_from("e0");
        // 只勾选文件 1：文件 0/2 恒为 0，文件 1 为自身长度
        let per_file = bt_files_done_bytes(&info, &all_done, Some(&[1]), false);
        assert_eq!(per_file, vec![0, 10, 0]);
        assert_eq!(per_file.iter().sum::<u64>(), 10);
    }

    #[test]
    fn files_done_bytes_without_bitfield_falls_back_to_complete_flag() {
        let info = info_of(&[15, 10, 5]);
        // 位图缺失（旧会话）+ 任务已完成 → 选中的文件按整文件完成，未选为 0；
        // 绝不能退回「按比例估算」
        assert_eq!(
            bt_files_done_bytes(&info, &[], Some(&[1]), true),
            vec![0, 10, 0]
        );
        // 位图缺失且任务未完成 → 全部 0（无片数据无法细分）
        assert_eq!(
            bt_files_done_bytes(&info, &[], Some(&[1]), false),
            vec![0, 0, 0]
        );
    }

    #[test]
    fn wanted_bitfield_marks_pieces_of_selected_files() {
        let info = info_of(&[15, 10, 5]);
        // 全选 / 无选择：空串（界面不区分「未选择」）
        assert!(wanted_bitfield(&info, None).is_empty());
        // 只勾选文件 1（片 1 全在其中、片 2 跨文件 1/2）→ 0b011xxxxx
        assert_eq!(bitfield_hex(&wanted_bitfield(&info, Some(&[1]))), "60");
        // 只勾选文件 0（片 0 全在其中、片 1 跨文件 0/1）→ 0b110xxxxx
        assert_eq!(bitfield_hex(&wanted_bitfield(&info, Some(&[0]))), "c0");
        // 勾选全部文件 = 全 1 位图（与 bitfield 全满一致）
        assert_eq!(bitfield_hex(&wanted_bitfield(&info, Some(&[0, 1, 2]))), "e0");
    }

    #[test]
    fn wanted_bitfield_empty_for_zero_length_file() {
        // 长度为 0 的文件不覆盖任何片：只勾选它时无片需要下载
        let info = info_of(&[10, 0, 10]);
        assert_eq!(bitfield_hex(&wanted_bitfield(&info, Some(&[1]))), "00");
    }
}
