//! 端手互通 · 局域网文件传输（引擎端）。
//!
//! 实现 `.workbuddy/lan-transfer-protocol.md`（协议 **v2**，通用版）：
//!
//! - UDP 发现（固定 41234，discover / hello，8s 过期）
//! - TCP 接收（临时端口，端口写进 discover）
//! - ECDH P-256 → HKDF-SHA256（salt = SHA-256(排序后的两把临时公钥)）→ AES-256-GCM 加密分帧
//! - 短认证串 SAS（6 位数字，RPC 任务字段 `sas`）供两端用户比对防中间人
//! - 任务表（最近 50 条，持久化到 `<data_dir>/lan-tasks.json`）
//!
//! 协议对实现开放（`proto:"lan-transfer"`，不校验客户端身份），
//! **线上的字段名、端口、salt/info、分帧格式必须与对端逐字节一致**；
//! 改协议先改文档，再改这里。

mod config;
mod controller;
mod crypto;
mod device;
mod discovery;
mod protocol;
mod tasks;
mod transfer;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

pub use config::{effective_recv_dir, LanUserConfig};
pub use controller::{LanConfigPatch, LanController, LanStatus};
pub use tasks::{Direction, LanTask, TaskStatus};

/// 发现端口（协议第 1 节，固定，不协商）。
pub const DISCOVERY_PORT: u16 = 41234;
/// 对端报文超过该时长没有更新 → 从设备列表移除。
pub const PEER_TTL: Duration = Duration::from_millis(8_000);
/// 发现广播间隔。
pub const DISCOVERY_INTERVAL: Duration = Duration::from_millis(2_000);
/// 接收目录名：`<下载目录>/LerxuTransfer/`。
pub const RECV_DIR_NAME: &str = "LerxuTransfer";

/// 启动配置：引擎把下载目录与数据目录传进来；设备名与接收目录可选覆盖。
#[derive(Debug, Clone)]
pub struct LanConfig {
    /// 下载目录（默认接收目录落在其下的 `LerxuTransfer/`）。
    pub download_dir: PathBuf,
    /// 引擎数据目录（设备身份、任务表、用户配置）。
    pub data_dir: PathBuf,
    /// 设备名覆盖；None 表示沿用设备身份文件里的名字。
    pub device_name: Option<String>,
    /// 接收目录覆盖；None 表示默认 `<下载目录>/LerxuTransfer/`。
    pub recv_dir: Option<PathBuf>,
}

impl LanConfig {
    pub fn new(download_dir: impl Into<PathBuf>, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            download_dir: download_dir.into(),
            data_dir: data_dir.into(),
            device_name: None,
            recv_dir: None,
        }
    }
}

/// 发现到的对端设备（RPC `lan.peers` 的输出元素）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LanPeer {
    pub id: String,
    pub name: String,
    pub platform: String,
    pub address: String,
    pub port: u16,
}

/// 启动/运行期错误（对外只暴露可读信息，内部不区分细类）。
#[derive(Debug)]
pub enum LanError {
    Io(std::io::Error),
    Msg(String),
}

impl std::fmt::Display for LanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LanError::Io(e) => write!(f, "{e}"),
            LanError::Msg(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for LanError {}

impl From<std::io::Error> for LanError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

pub(crate) struct PeerEntry {
    pub id: String,
    pub name: String,
    pub platform: String,
    pub address: String,
    pub port: u16,
    pub last_seen: Instant,
}

pub(crate) struct State {
    /// 最新在前。
    pub tasks: Vec<LanTask>,
    pub peers: HashMap<String, PeerEntry>,
    /// 已被取消（传输循环每写一块就检查一次）。
    pub cancelled: HashSet<String>,
    /// 任务表有变更、等待节流落盘。
    pub dirty: bool,
}

/// 局域网传输服务。`start` 后由后台任务驱动，`stop` 优雅收尾。
pub struct LanService {
    /// 接收目录；运行期可改（已开始的传输仍落在开始时的那份目录）。
    recv_dir: Mutex<PathBuf>,
    data_dir: PathBuf,
    device_id: String,
    /// 设备名；运行期可改，发现广播每次读最新值。
    device_name: Mutex<String>,
    platform: String,
    /// TCP 接收端口（discover.port）。
    port: u16,
    state: Mutex<State>,
    /// 发送队列有新任务。
    wake: Notify,
    /// 停止信号。
    stop_notify: Notify,
    stopped: AtomicBool,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl LanService {
    /// 启动发现 + 接收 + 发送队列 + 任务表落盘。
    ///
    /// UDP 41234 被占用时按协议"只发不收"降级（发现仍可用），不阻断启动。
    pub async fn start(config: LanConfig) -> Result<Arc<Self>, LanError> {
        let recv_dir = config
            .recv_dir
            .unwrap_or_else(|| config.download_dir.join(RECV_DIR_NAME));
        tokio::fs::create_dir_all(&recv_dir).await?;
        std::fs::create_dir_all(&config.data_dir)?;
        let device = device::Device::load_or_create(&config.data_dir, config.device_name.as_deref())?;
        let restored_tasks = tasks::load(&config.data_dir);

        let listener = tokio::net::TcpListener::bind(("0.0.0.0", 0u16)).await?;
        let port = listener.local_addr()?.port();

        let service = Arc::new(Self {
            recv_dir: Mutex::new(recv_dir),
            data_dir: config.data_dir,
            device_id: device.id,
            device_name: Mutex::new(device.name),
            platform: device.platform,
            port,
            state: Mutex::new(State {
                tasks: restored_tasks,
                peers: HashMap::new(),
                cancelled: HashSet::new(),
                // 启动迁移（waiting/active → error）也要落盘
                dirty: true,
            }),
            wake: Notify::new(),
            stop_notify: Notify::new(),
            stopped: AtomicBool::new(false),
            handles: Mutex::new(Vec::new()),
        });

        let mut handles = Vec::with_capacity(4);
        handles.push(tokio::spawn(discovery::run(
            service.clone(),
            discovery::bind_socket().await,
        )));
        handles.push(tokio::spawn(transfer::accept_loop(
            service.clone(),
            listener,
        )));
        handles.push(tokio::spawn(transfer::sender_loop(service.clone())));
        handles.push(tokio::spawn(service.clone().persist_loop()));
        *service.handles.lock().expect("handles 锁中毒") = handles;

        tracing::info!(
            device_id = %service.device_id,
            device_name = %service.device_name(),
            platform = %service.platform,
            tcp_port = port,
            recv_dir = %service.recv_dir().display(),
            data_dir = %service.data_dir.display(),
            "局域网传输已启动"
        );
        Ok(service)
    }

    /// 本机设备 id（持久化，跨重启稳定）。
    pub fn device_id(&self) -> &str {
        &self.device_id
    }

    /// 本机设备名。
    pub fn device_name(&self) -> String {
        self.device_name
            .lock()
            .expect("设备名锁中毒")
            .clone()
    }

    /// 平台标识（macos / windows / linux / android）。
    pub fn platform(&self) -> &str {
        &self.platform
    }

    /// TCP 接收端口（写进 discover 的那个）。
    pub fn port(&self) -> u16 {
        self.port
    }

    /// 当前接收目录（运行期可改）。
    pub fn recv_dir(&self) -> PathBuf {
        self.recv_dir.lock().expect("接收目录锁中毒").clone()
    }

    /// 修改设备名：先落盘设备身份，再更新发现广播使用的最新值。
    pub fn set_device_name(&self, raw: &str) -> Result<(), LanError> {
        let device = device::Device::load_or_create(&self.data_dir, Some(raw))?;
        *self.device_name.lock().expect("设备名锁中毒") = device.name;
        Ok(())
    }

    /// 修改接收目录：只影响之后的接收任务，已开始的传输保持原目录。
    pub fn set_recv_dir(&self, dir: &Path) -> Result<(), LanError> {
        if !dir.is_absolute() {
            return Err(LanError::Msg("接收目录必须是绝对路径".to_string()));
        }
        std::fs::create_dir_all(dir)?;
        *self.recv_dir.lock().expect("接收目录锁中毒") = dir.to_path_buf();
        Ok(())
    }

    /// 当前局域网内在线设备（最近活跃在前）。
    pub fn peers(&self) -> Vec<LanPeer> {
        let state = self.lock_state();
        let mut list: Vec<(Instant, LanPeer)> = state
            .peers
            .values()
            .map(|peer| {
                (
                    peer.last_seen,
                    LanPeer {
                        id: peer.id.clone(),
                        name: peer.name.clone(),
                        platform: peer.platform.clone(),
                        address: peer.address.clone(),
                        port: peer.port,
                    },
                )
            })
            .collect();
        drop(state);
        list.sort_by(|a, b| b.0.cmp(&a.0));
        list.into_iter().map(|(_, peer)| peer).collect()
    }

    /// 任务列表（最新在前，最多 50 条）。
    pub fn tasks(&self) -> Vec<LanTask> {
        self.lock_state().tasks.clone()
    }

    /// 把若干本地文件排进发送队列（串行发送，每个文件一条 out 任务）。
    ///
    /// 只做校验 + 入队，不阻塞 RPC 线程；返回本次新建的任务对象。
    pub fn send(&self, peer_id: &str, paths: &[String]) -> Result<Vec<LanTask>, String> {
        if paths.is_empty() {
            return Err("paths 不能为空".to_string());
        }
        let (peer_name, peer_address) = {
            let state = self.lock_state();
            let peer = state
                .peers
                .get(peer_id)
                .ok_or_else(|| format!("设备 {peer_id} 不在局域网在线列表中"))?;
            (peer.name.clone(), peer.address.clone())
        };

        // 先整体校验，避免"前两个文件已入队、第三个才报错"的半截行为
        let mut pending = Vec::with_capacity(paths.len());
        for raw in paths {
            let path = PathBuf::from(raw);
            let meta = std::fs::metadata(&path).map_err(|e| format!("读取 {raw} 失败：{e}"))?;
            if !meta.is_file() {
                return Err(format!("{raw} 不是普通文件"));
            }
            let name = path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| "unnamed".to_string());
            pending.push((path, name, meta.len() as i64));
        }

        let created_at = now_ms();
        let mut state = self.lock_state();
        let mut created = Vec::with_capacity(pending.len());
        for (path, name, size) in pending {
            let task = LanTask {
                id: random_hex(16),
                direction: Direction::Out,
                name,
                size,
                transferred: 0,
                status: TaskStatus::Waiting,
                peer_name: peer_name.clone(),
                peer_address: peer_address.clone(),
                error: String::new(),
                // 收到 accept、建立会话后才算出 SAS
                sas: String::new(),
                created_at,
                finished_at: None,
                path: None,
                peer_id: Some(peer_id.to_string()),
                source_path: Some(path.to_string_lossy().to_string()),
            };
            state.tasks.insert(0, task.clone());
            created.push(task);
        }
        state.tasks = tasks::trim(std::mem::take(&mut state.tasks));
        state.dirty = true;
        drop(state);
        self.wake.notify_one();
        Ok(created)
    }

    /// 从任务表移除一条（传输中的会先取消）。
    pub fn remove(&self, id: &str) -> Result<(), String> {
        let mut state = self.lock_state();
        let Some(index) = state.tasks.iter().position(|t| t.id == id) else {
            return Err(format!("任务 {id} 不存在"));
        };
        if !state.tasks[index].is_finished() {
            state.cancelled.insert(id.to_string());
        }
        state.tasks.remove(index);
        state.dirty = true;
        drop(state);
        self.wake.notify_one();
        Ok(())
    }

    /// 取消一条任务（排队中 → 直接 error「cancelled」；传输中 → 由传输循环收尾）。
    pub fn cancel(&self, id: &str) -> Result<(), String> {
        let mut state = self.lock_state();
        let Some(task) = state.tasks.iter_mut().find(|t| t.id == id) else {
            return Err(format!("任务 {id} 不存在"));
        };
        match task.status {
            TaskStatus::Done | TaskStatus::Error => Ok(()),
            TaskStatus::Waiting => {
                task.status = TaskStatus::Error;
                task.error = "cancelled".to_string();
                task.finished_at = Some(now_ms());
                state.cancelled.insert(id.to_string());
                state.dirty = true;
                drop(state);
                self.wake.notify_one();
                Ok(())
            }
            TaskStatus::Active => {
                // 传输循环每块检查一次 cancelled 标志；已建好的任务由它写入 error
                state.cancelled.insert(id.to_string());
                Ok(())
            }
        }
    }

    /// 优雅停止：停广播/停止接收/结束队列，落盘后退出。
    pub async fn stop(&self) {
        if self.stopped.swap(true, Ordering::SeqCst) {
            return;
        }
        self.stop_notify.notify_waiters();
        self.wake.notify_waiters();
        self.persist_if_dirty();
        let handles: Vec<JoinHandle<()>> = {
            let mut guard = self.handles.lock().expect("handles 锁中毒");
            std::mem::take(&mut *guard)
        };
        for handle in handles {
            handle.abort();
        }
        tracing::info!("局域网传输已停止");
    }

    // ------------------------------------------------------------------
    // 内部状态维护（transfer / discovery 使用）
    // ------------------------------------------------------------------

    pub(crate) fn lock_state(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("局域网状态锁中毒")
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    pub(crate) fn upsert_peer(&self, msg: &protocol::DiscoverMsg, ip: std::net::IpAddr) {
        let name: String = if msg.name.trim().is_empty() {
            msg.id.clone()
        } else {
            msg.name.trim().chars().take(64).collect()
        };
        let mut state = self.lock_state();
        state.peers.insert(
            msg.id.clone(),
            PeerEntry {
                id: msg.id.clone(),
                name,
                platform: msg.platform.clone(),
                address: ip.to_string(),
                port: msg.port,
                last_seen: Instant::now(),
            },
        );
    }

    /// 移除超过 TTL 没动静的设备。
    pub(crate) fn prune_peers(&self, ttl: Duration) {
        let mut state = self.lock_state();
        state.peers.retain(|_, peer| peer.last_seen.elapsed() < ttl);
    }

    pub(crate) fn peer_target(&self, peer_id: &str) -> Option<SocketAddr> {
        let state = self.lock_state();
        let peer = state.peers.get(peer_id)?;
        let ip = peer.address.parse().ok()?;
        Some(SocketAddr::new(ip, peer.port))
    }

    pub(crate) fn task_exists(&self, id: &str) -> bool {
        self.lock_state().tasks.iter().any(|t| t.id == id)
    }

    pub(crate) fn add_task(&self, task: LanTask) {
        let mut state = self.lock_state();
        state.tasks.insert(0, task);
        state.tasks = tasks::trim(std::mem::take(&mut state.tasks));
        state.dirty = true;
    }

    /// 取最早排队的 out 任务并标记 active（队列串行）。
    pub(crate) fn next_waiting_out(&self) -> Option<LanTask> {
        let mut state = self.lock_state();
        let cancelled = &state.cancelled;
        let index = state.tasks.iter().rposition(|t| {
            t.direction == Direction::Out
                && t.status == TaskStatus::Waiting
                && !cancelled.contains(&t.id)
        })?;
        let task = &mut state.tasks[index];
        task.status = TaskStatus::Active;
        let snapshot = task.clone();
        state.dirty = true;
        Some(snapshot)
    }

    /// 任务是否仍有效（未删除、未取消）。
    pub(crate) fn task_alive(&self, id: &str) -> bool {
        let state = self.lock_state();
        state.tasks.iter().any(|t| t.id == id) && !state.cancelled.contains(id)
    }

    /// 进度更新；`activate` 用于接收端"收到第一个字节起 active"。
    pub(crate) fn progress(&self, id: &str, transferred: i64, activate: bool) {
        let mut state = self.lock_state();
        let Some(task) = state.tasks.iter_mut().find(|t| t.id == id) else {
            return;
        };
        task.transferred = transferred;
        if task.size >= 0 {
            task.transferred = task.transferred.min(task.size);
        }
        if activate && task.status == TaskStatus::Waiting {
            task.status = TaskStatus::Active;
            state.dirty = true;
        }
    }

    pub(crate) fn update_size(&self, id: &str, size: i64) {
        let mut state = self.lock_state();
        if let Some(task) = state.tasks.iter_mut().find(|t| t.id == id) {
            task.size = size;
            state.dirty = true;
        }
    }

    /// 写入短认证串（发送端在收到 accept、算出会话密钥后调用）。
    pub(crate) fn update_sas(&self, id: &str, sas: String) {
        let mut state = self.lock_state();
        if let Some(task) = state.tasks.iter_mut().find(|t| t.id == id) {
            task.sas = sas;
            state.dirty = true;
        }
    }

    pub(crate) fn complete_task(&self, id: &str, path: Option<String>) {
        let mut state = self.lock_state();
        let Some(task) = state.tasks.iter_mut().find(|t| t.id == id) else {
            return;
        };
        task.status = TaskStatus::Done;
        task.error.clear();
        if task.size >= 0 {
            task.transferred = task.size;
        }
        task.finished_at = Some(now_ms());
        if path.is_some() {
            task.path = path;
        }
        state.dirty = true;
    }

    pub(crate) fn fail_task(&self, id: &str, error: impl Into<String>) {
        let mut state = self.lock_state();
        let Some(task) = state.tasks.iter_mut().find(|t| t.id == id) else {
            return;
        };
        if task.status == TaskStatus::Done {
            // 已完整的任务不因为收尾动作（如回执写失败）被改回失败
            return;
        }
        task.status = TaskStatus::Error;
        task.error = clamp_error(error.into());
        task.finished_at = Some(now_ms());
        state.dirty = true;
    }

    fn persist_if_dirty(&self) {
        let snapshot = {
            let mut state = self.lock_state();
            if !state.dirty {
                return;
            }
            state.dirty = false;
            state.tasks.clone()
        };
        if let Err(e) = tasks::save(&self.data_dir, &snapshot) {
            tracing::warn!(error = %e, path = %self.data_dir.display(), "写入局域网任务表失败");
            self.lock_state().dirty = true;
        }
    }

    /// 任务表节流落盘（500ms 检查一次）；退出前再冲一次。
    async fn persist_loop(self: Arc<Self>) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(500)) => self.persist_if_dirty(),
                _ = self.stop_notify.notified() => {
                    self.persist_if_dirty();
                    break;
                }
            }
        }
    }
}

/// 毫秒时间戳（任务对象的 createdAt / finishedAt）。
pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 随机 id（十六进制字符串），设备 id / transferId 共用。
pub(crate) fn random_hex(bytes: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut buf = vec![0u8; bytes];
    if getrandom::fill(&mut buf).is_err() {
        // 随机源不可用：退化成时间戳，至少保证本地唯一
        return format!("{:032x}", now_ms());
    }
    let mut out = String::with_capacity(bytes * 2);
    for byte in buf {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn clamp_error(mut error: String) -> String {
    if error.chars().count() > 200 {
        error = error.chars().take(200).collect();
    }
    error
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::DiscoverMsg;

    #[test]
    fn random_ids_are_hex_and_unique() {
        let a = random_hex(16);
        let b = random_hex(16);
        assert_eq!(a.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }

    #[test]
    fn now_ms_is_plausible() {
        // 2020-01-01 之后（毫秒），且不是秒级时间戳
        assert!(now_ms() > 1_577_836_800_000);
    }

    /// 端到端：两个服务实例（同机）走完 offer → accept → 加密分帧 → done，
    /// 校验接收文件字节一致、任务状态与 transferred 正确。
    /// 发现（UDP）不参与：这里手工注入对端，避免测试依赖 41234 是否空闲。
    #[tokio::test]
    async fn encrypted_end_to_end_transfer() {
        let root = std::env::temp_dir().join(format!(
            "xfer-lan-e2e-{}-{}",
            std::process::id(),
            random_hex(4)
        ));
        let dir_send = root.join("send");
        let dir_recv = root.join("recv");
        std::fs::create_dir_all(&dir_send).unwrap();
        std::fs::create_dir_all(&dir_recv).unwrap();

        let sender = LanService::start(LanConfig::new(&dir_send, root.join("data-send")))
            .await
            .unwrap();
        let receiver = LanService::start(LanConfig::new(&dir_recv, root.join("data-recv")))
            .await
            .unwrap();

        // 跳过 UDP 发现，直接把对端塞进设备表（生产环境由 discover/hello 完成）
        receiver.upsert_peer(
            &DiscoverMsg::new(
                "hello",
                sender.device_id(),
                &sender.device_name(),
                sender.platform(),
                sender.port(),
            ),
            "127.0.0.1".parse().unwrap(),
        );
        sender.upsert_peer(
            &DiscoverMsg::new(
                "hello",
                receiver.device_id(),
                &receiver.device_name(),
                receiver.platform(),
                receiver.port(),
            ),
            "127.0.0.1".parse().unwrap(),
        );

        // 700 KB：跨多个 256 KiB 分片
        let payload: Vec<u8> = (0..700 * 1024).map(|i| (i % 251) as u8).collect();
        let source = dir_send.join("payload.bin");
        std::fs::write(&source, &payload).unwrap();

        let created = sender
            .send(
                receiver.device_id(),
                &[source.to_string_lossy().to_string()],
            )
            .unwrap();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].direction, Direction::Out);
        assert_eq!(created[0].size, payload.len() as i64);

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let out = sender
                .tasks()
                .into_iter()
                .find(|t| t.direction == Direction::Out);
            let incoming = receiver
                .tasks()
                .into_iter()
                .find(|t| t.direction == Direction::In);
            let done = matches!(&out, Some(t) if t.status == TaskStatus::Done)
                && matches!(&incoming, Some(t) if t.status == TaskStatus::Done);
            if done {
                break;
            }
            if let Some(t) = out.as_ref() {
                assert_ne!(t.status, TaskStatus::Error, "发送失败：{}", t.error);
            }
            if let Some(t) = incoming.as_ref() {
                assert_ne!(t.status, TaskStatus::Error, "接收失败：{}", t.error);
            }
            assert!(
                Instant::now() < deadline,
                "传输超时：{out:?} / {incoming:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let out = sender
            .tasks()
            .into_iter()
            .find(|t| t.direction == Direction::Out)
            .unwrap();
        assert_eq!(out.transferred, payload.len() as i64);
        assert!(out.finished_at.is_some());

        let incoming = receiver
            .tasks()
            .into_iter()
            .find(|t| t.direction == Direction::In)
            .unwrap();
        assert_eq!(incoming.transferred, payload.len() as i64);
        assert_eq!(incoming.peer_address, "127.0.0.1");
        // SAS：两端必须一致、6 位数字（用户口头/肉眼比对的凭证）
        assert_eq!(out.sas, incoming.sas);
        assert_eq!(out.sas.len(), 6, "SAS 应为 6 位：{}", out.sas);
        assert!(out.sas.chars().all(|c| c.is_ascii_digit()));
        let saved = incoming.path.expect("接收任务应记录落盘路径");
        let saved_path = std::path::PathBuf::from(&saved);
        assert_eq!(saved_path.parent().unwrap(), receiver.recv_dir().as_path());
        assert_eq!(std::fs::read(&saved_path).unwrap(), payload);
        assert!(!saved_path.with_extension("bin.part").exists());
        // 没有残留的 .part
        let leftovers: Vec<_> = std::fs::read_dir(receiver.recv_dir())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().ends_with(".part"))
            .collect();
        assert!(leftovers.is_empty(), "残留临时文件：{leftovers:?}");

        sender.stop().await;
        receiver.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }

    /// 发送一个不存在的设备必须立刻报错（不会静默入队）。
    #[tokio::test]
    async fn send_to_unknown_peer_fails() {
        let root = std::env::temp_dir().join(format!(
            "xfer-lan-peer-{}-{}",
            std::process::id(),
            random_hex(4)
        ));
        let service = LanService::start(LanConfig::new(&root, root.join("data")))
            .await
            .unwrap();
        let file = root.join("a.txt");
        std::fs::write(&file, b"hello").unwrap();
        let err = service
            .send("no-such-device", &[file.to_string_lossy().to_string()])
            .unwrap_err();
        assert!(err.contains("不在局域网在线列表"), "错误信息：{err}");
        assert!(service.tasks().is_empty());
        service.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }
}
