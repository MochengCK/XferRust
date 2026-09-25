//! 多连接分片下载（自研架构）。
//!
//! 与旧引擎（aria2 系）的固定分片 + 慢速连接剔除不同，本实现采用
//! **单写线程 + 异步网络工作协程** 的 actor 结构：
//!
//! - **写线程**（`spawn_blocking`）独占全部段表状态与磁盘 IO：
//!   调度（发段）、对冲切分（窃取）、进度记账、控制文件落盘。
//!   所有协调经由 FIFO 通道 + oneshot 回执，天然无锁、无数据竞争。
//! - **工作协程**是纯粹的网络泵：领取段区间，流式 Range 请求，
//!   把 `(offset, bytes)` 投递给写线程（pwrite 位置无关写）。
//! - **工作窃取对冲**：空闲协程把"剩余最多的活跃段"对半切开接管
//!   后半段——按实际吞吐自适应负载均衡，无需速度启发式，也不像
//!   旧引擎那样中止慢连接（避免连接重建成本）。
//! - **控制文件**存于引擎数据目录（`$HOME/.xfer/ctrl/`，可用环境变量
//!   `XFER_CTRL_DIR` 覆盖）：JSON 段级水位线（只记录已
//!   `fsync` 的字节），原子写入（tmp + rename），成功后删除。
//!   以目标文件绝对路径哈希命名，对用户不可见，不污染下载目录。
//!   数据文件的 `fsync` 与控制文件落盘由**独立同步线程**执行：
//!   写线程只拍水位快照并提交任务，绝不阻塞在磁盘同步上——
//!   高吞吐下脏页巨大时，同步开销不再反压网络与写入管线。
//!
//! 崩溃安全：水位快照先于 `fsync` 拍取，控制文件只记录快照值，
//! 保证 `written ≤ 实际落盘`；反向（落盘多于记录）只是重复下载，
//! 幂等无害。

use std::collections::VecDeque;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio_util::sync::CancellationToken;

use crate::adaptive::{AdaptiveConfig, AdaptiveScheduler, ConnPerf, ScheduleAction};
use crate::rate::RateLimiter;
use crate::HttpError;

/// 段失败重试预算（超出即任务失败，交给镜像切换兜底）。
const MAX_FAILURES: u32 = 4;
/// 单协程连续短读上限：尾段断流通常一两次恢复，不占失败预算；
/// 连续超阈值才折抵一次失败，保留对"恒坏服务器"的最终失败能力。
const MAX_SHORT_READS: u32 = 8;
/// 空闲协程被写线程要求停靠的时间。
const PARK_MS: u64 = 250;

/// 尾声模式（end-game）阈值：总剩余量低于此值时进入尾声。
/// 常规切分下限是 min_split_size（默认 4MiB）——快完成时剩余段
/// 全部小于 2×min_split_size，对冲窃取失效，空闲协程全部 Park，
/// 最后一段退化为单连接串行收尾：对按连接限速的服务器，总速率
/// 从 N×单连接速率塌缩到 1×，表现为"速度骤降归零"。
const ENDGAME_THRESHOLD: u64 = 64 * 1024 * 1024;
/// 尾声切分下限：细段并行收尾（往返开销换并行度，仅在尾声值得）。
/// 取 64KiB：尾段能切多细就切多细——每一份都能交给一条独立连接，
/// 单条连接卡住不再拖住整条尾巴。
const ENDGAME_MIN_SPLIT: u64 = 64 * 1024;

/// 连接静默上限（读空闲）：响应体连续该时长无任何字节，即按"这条连接
/// 已经不行了"处理——断开并从水位重连。
///
/// **不再按"剩余量是否进入尾声"分层取值**。分层是历史包袱：大文件下到
/// 99% 时全局剩余往往还有几十 MB（1% of 4GB = 40MB），"剩余 ≤ 8MiB 才
/// 收紧"的判据根本不成立，僵死连接照样等满 10s —— 用户看到的正是
/// "99% 卡十几秒、暂停再继续才恢复"。
///
/// 静默重连续传的代价只有一次往返（已收字节全部保留、不占失败预算，
/// 见 [`MAX_SHORT_READS`]），因此阈值该取"明显异常"的量级（3s），
/// 而不是"确定死亡"的量级。
const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(3);
/// 首字节等待上限：请求发出（含建连/TLS/服务端思考）到响应体首块的
/// 允许时长。与读空闲分开取值：请求刚发出时尚未证明连接有问题，
/// 给足建连与首包时间；一旦开始出数据，间隙判据收紧到 3s。
const FIRST_BYTE_TIMEOUT: Duration = Duration::from_secs(15);

/// 段租约：持有者连续该时长无字节落盘即视为搁浅，写线程把剩余区间
/// **转交**给新连接（见 [`Writer::handoff`]）。
///
/// 与读空闲超时的分工：读空闲让持有者自己重连（大多数情况够用）；
/// 租约由写线程换人，覆盖持有者来不及或无法自救的场合（连接半死但
/// 读没超时、需要一条全新连接绕过服务器对旧连接的限速、协程扑在
/// 别的等待上）。刻意略大于读空闲超时，避免两条自救路径抢跑。
const SEG_LEASE: Duration = Duration::from_secs(4);
/// 转交阈值（整段 / 最小）：剩余 ≤ [`HANDOFF_WHOLE_MAX`] 时整个剩余区间
/// 交给新连接；否则只转交后半、持有者保留前半再给一次机会。
/// 小于 [`HANDOFF_MIN`] 的尾巴不值得另开连接（读空闲重连即可）。
const HANDOFF_WHOLE_MAX: u64 = 4 * 1024 * 1024;
const HANDOFF_MIN: u64 = 32 * 1024;

/// 停滞看门狗阈值：全局无字节落盘达到该时长即强制回收搁浅段。
/// 租约（[`SEG_LEASE`]）已覆盖"持有者搁浅"，本看门狗只兜底
/// "段既无人持有、也不在队列"的理论缺口（出队/归还竞态引入的 bug），
/// 因此取值远大于租约、只作最后防线。
const STALL_TIMEOUT: Duration = Duration::from_secs(15);
/// 控制文件的最小落盘间隔（节流 fsync）。
const CTRL_SAVE_INTERVAL: Duration = Duration::from_secs(1);
/// 请求批水位（通道容量）：过高徒增内存，过低限制吞吐。
/// 256 槽位提供数 MB 写后置缓冲，平滑磁盘写入抖动，
/// 避免瞬时写慢立即反压网络协程。
const CHANNEL_CAP: usize = 256;
/// 自适应指标上报窗口：字节数或时间先到者触发。
/// 字节阈值取 2MB：极快连接上避免把上报通道打满数千条小消息
/// （评估周期 1s，500ms 时间上限已保证慢连接每窗口至少 2 次上报）。
const PERF_REPORT_BYTES: u64 = 2 * 1024 * 1024;
const PERF_REPORT_INTERVAL: Duration = Duration::from_millis(500);

// ---------------------------------------------------------------------------
// 公共接口
// ---------------------------------------------------------------------------

/// 分片下载选项（由引擎层合并 `split` / `max-connection-per-server` 后给出）。
#[derive(Debug, Clone)]
pub struct SplitOptions {
    /// 目标并发连接数（已与 split / max-connection-per-server 取 min）。
    /// 自适应模式启用时作为初始连接数。
    pub connections: usize,
    /// 最小分段大小（字节），既是对冲切分的下限，也约束初始段数——
    /// 过小会让 Range 请求往返开销占比过高。
    pub min_split_size: u64,
    /// 自适应调度配置（None = 固定连接数传统模式）。
    pub adaptive: Option<AdaptiveConfig>,
    /// 全局限速器（None = 不限速）：所有连接共享，读循环消费令牌。
    pub limiter: Option<Arc<RateLimiter>>,
    /// 逐任务自定义请求头（`Referer` / `Cookie` / `User-Agent` 等）。
    ///
    /// 必须用 [`crate::parse_header_lines`] 产出（已滤掉非法项与
    /// `Range` / `Host` 等会破坏分段语义的头）；每个分片请求都会带上，
    /// 否则受保护地址只对缺头的请求回 403，整条任务必然失败。
    pub headers: crate::RequestHeaders,
}

/// 引擎轮询的进度句柄（无锁原子）。
pub struct SplitStats {
    /// 已落盘字节（含续传基线）。
    pub completed: AtomicU64,
    /// 当前活跃连接数。
    pub connections: AtomicUsize,
    /// 实时分片位图（下载启动时挂接一次，写线程增量维护）。
    pieces: OnceLock<Arc<PieceTrack>>,
    /// 活跃连接表（"连接详情"数据源：逐连接的主机/已收字节/实时速率）。
    conns: Mutex<Vec<ConnSlot>>,
    /// 连接号分配（每次请求一个，只增不减）。
    next_conn_id: AtomicU64,
}

/// 连接表条目：一条**在飞的请求**的实时状态。
struct ConnSlot {
    id: u64,
    host: String,
    /// 本次请求已接收字节。
    downloaded: u64,
    /// 速率结算窗口：窗口内累计字节与窗口起点。
    window_bytes: u64,
    window_start: Instant,
    /// 平滑后的实时速率（字节/秒）。
    speed: u64,
    /// 请求是否仍在飞。
    active: bool,
    /// 最近一次数据/状态更新时刻（过期即从表中剔除）。
    updated: Instant,
}

/// 连接详情快照（供 RPC/界面逐连接展示）。
#[derive(Debug, Clone)]
pub struct ConnSnapshot {
    /// 连接号（每次请求递增）。
    pub id: u64,
    /// 目标主机。
    pub host: String,
    /// 本次请求已接收字节。
    pub downloaded: u64,
    /// 实时速率（字节/秒）。
    pub speed: u64,
    /// 请求是否仍在飞。
    pub active: bool,
}

/// 速率结算窗口（时间/字节先到者）：界面轮询间隔通常 0.3~1s，
/// 结算粒度取 100ms 或攒够 256KiB ——分片区间可能几十毫秒就下完，
/// 只按时间结算会让这些"短促快连接"永远算不出速率（显示 0）。
const CONN_SPEED_SETTLE: Duration = Duration::from_millis(100);
const CONN_SPEED_SETTLE_BYTES: u64 = 256 * 1024;
/// 速率陈旧时限：在飞连接超过该时长没有新字节，速率按 0 报
/// （对端静默 = 没有下载能力，界面应显示为不活跃）。
const CONN_SPEED_STALE: Duration = Duration::from_millis(700);
/// 连接表批量结算阈值（字节 / 时间先到者）：避免每块一次加锁。
const CONN_FLUSH_BYTES: u64 = 64 * 1024;
const CONN_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
/// 连接表过期时限：请求结束后再过该时长仍未更新即剔除
/// （留一点"残影"，界面轮询间隔通常 0.3~1s，不至于闪烁消失）。
const CONN_IDLE_TTL: Duration = Duration::from_secs(2);
/// 在飞请求的过期时限：请求在飞但长时间无字节（僵死连接的现场），
/// 超过该时长也不再展示（此时租约/读空闲已在换人）。
const CONN_ACTIVE_TTL: Duration = Duration::from_secs(6);

impl SplitStats {
    pub fn new(baseline: u64) -> Arc<Self> {
        Arc::new(Self {
            completed: AtomicU64::new(baseline),
            connections: AtomicUsize::new(0),
            pieces: OnceLock::new(),
            conns: Mutex::new(Vec::new()),
            next_conn_id: AtomicU64::new(0),
        })
    }

    /// 登记一条新连接（每次请求一次），返回连接号。
    pub fn conn_open(&self, host: &str) -> u64 {
        let id = self.next_conn_id.fetch_add(1, Ordering::Relaxed) + 1;
        let now = Instant::now();
        let mut t = self.conns.lock().unwrap();
        prune_conns(&mut t, now);
        t.push(ConnSlot {
            id,
            host: host.to_string(),
            downloaded: 0,
            window_bytes: 0,
            window_start: now,
            speed: 0,
            active: true,
            updated: now,
        });
        id
    }

    /// 记账：连接 `id` 本次请求新收到 `n` 字节（顺带结算窗口速率）。
    pub fn conn_bytes(&self, id: u64, n: u64) {
        let now = Instant::now();
        let mut t = self.conns.lock().unwrap();
        let Some(slot) = t.iter_mut().find(|s| s.id == id) else {
            return;
        };
        slot.downloaded += n;
        slot.window_bytes += n;
        slot.updated = now;
        let dt = now.duration_since(slot.window_start);
        if dt >= CONN_SPEED_SETTLE || slot.window_bytes >= CONN_SPEED_SETTLE_BYTES {
            let inst = slot.window_bytes as f64 / dt.as_secs_f64().max(0.001);
            // 与上一窗口各半：界面轮询周期与结算周期不同步，平滑后不跳变
            slot.speed = if slot.speed == 0 {
                inst as u64
            } else {
                ((slot.speed as f64 + inst) / 2.0) as u64
            };
            slot.window_bytes = 0;
            slot.window_start = now;
        }
    }

    /// 标记连接 `id` 的请求结束（保留条目至过期，速率归零）。
    pub fn conn_close(&self, id: u64) {
        let now = Instant::now();
        let mut t = self.conns.lock().unwrap();
        if let Some(slot) = t.iter_mut().find(|s| s.id == id) {
            slot.active = false;
            slot.speed = 0;
            slot.updated = now;
        }
    }

    /// 一致快照（过期条目已剔除；在飞但久无字节者速率按 0 报）。
    pub fn conn_snapshot(&self) -> Vec<ConnSnapshot> {
        let now = Instant::now();
        let mut t = self.conns.lock().unwrap();
        prune_conns(&mut t, now);
        t.iter()
            .map(|s| {
                let live = s.active && now.duration_since(s.updated) < CONN_SPEED_STALE;
                ConnSnapshot {
                    id: s.id,
                    host: s.host.clone(),
                    downloaded: s.downloaded,
                    speed: if live { s.speed } else { 0 },
                    active: s.active,
                }
            })
            .collect()
    }

    /// 挂接分片位图（下载启动时一次；重复调用忽略后续）。
    pub fn attach_pieces(&self, track: Arc<PieceTrack>) {
        let _ = self.pieces.set(track);
    }

    /// 已挂接的分片位图。
    pub fn piece_track(&self) -> Option<Arc<PieceTrack>> {
        self.pieces.get().cloned()
    }

    /// 当前位图快照（未挂接返回 None）。
    pub fn piece_snapshot(&self) -> Option<PieceSnapshot> {
        self.pieces.get().map(|t| t.snapshot())
    }
}

/// 剔除过期的连接表条目（已结束超过 [`CONN_IDLE_TTL`]、或在飞但
/// 超过 [`CONN_ACTIVE_TTL`] 无字节的僵死连接）。
fn prune_conns(t: &mut Vec<ConnSlot>, now: Instant) {
    t.retain(|s| {
        let ttl = if s.active {
            CONN_ACTIVE_TTL
        } else {
            CONN_IDLE_TTL
        };
        now.duration_since(s.updated) < ttl
    });
}

/// 分片位图快照：查询侧读取的一致视图。
#[derive(Debug, Clone)]
pub struct PieceSnapshot {
    /// 片长（字节；末片为总长余数）。
    pub piece_len: u64,
    /// 片数。
    pub num_pieces: u32,
    /// wire 语义位图（MSB-first，片 0 = 首字节最高位），与 BT 位图编码一致。
    pub bitfield: Vec<u8>,
}

/// HTTP 下载的实时分片位图。
///
/// 片长 = 进度显示粒度（调用方决定，通常细于 min-split-size，见
/// [`split_download`]），重启恢复后按控制文件水位重放
/// （[`Self::add_range`]），位图与真实落盘天然自洽。写侧单线程
/// 串行调用（分片写线程 / 单连接顺序写），各区间互不重叠，增量计数
/// 无双重计入；查询侧无锁快照。
pub struct PieceTrack {
    total: u64,
    piece_len: u64,
    /// 每片已落盘字节。
    done: Vec<AtomicU64>,
    /// wire 语义位图（MSB-first）。
    bf: Mutex<Vec<u8>>,
}

impl PieceTrack {
    pub fn new(total: u64, piece_len: u64) -> Arc<Self> {
        let piece_len = piece_len.max(1);
        let n = total.div_ceil(piece_len);
        Arc::new(Self {
            total,
            piece_len,
            done: (0..n).map(|_| AtomicU64::new(0)).collect(),
            bf: Mutex::new(vec![0u8; n.div_ceil(8) as usize]),
        })
    }

    /// 片长（字节）。
    pub fn piece_len(&self) -> u64 {
        self.piece_len
    }

    /// 片数。
    pub fn num_pieces(&self) -> u32 {
        self.done.len() as u32
    }

    /// 记录 [offset, offset+len) 已落盘。各写入区间互不重叠。
    pub fn add_range(&self, offset: u64, len: u64) {
        if len == 0 || self.done.is_empty() {
            return;
        }
        let end = offset.saturating_add(len).min(self.total);
        let mut offset = offset.min(end);
        let mut i = (offset / self.piece_len) as usize;
        while offset < end {
            let ps = i as u64 * self.piece_len;
            let pe = ps + self.len_at(i);
            let lo = offset.max(ps);
            let hi = end.min(pe);
            if hi > lo {
                let d = &self.done[i];
                d.fetch_add(hi - lo, Ordering::Relaxed);
                if d.load(Ordering::Relaxed) >= self.len_at(i) {
                    self.bf.lock().unwrap()[i / 8] |= 0x80 >> (i % 8);
                }
            }
            offset = hi;
            i += 1;
        }
    }

    /// 片长（末片为余数）。
    fn len_at(&self, i: usize) -> u64 {
        let start = i as u64 * self.piece_len;
        (start + self.piece_len).min(self.total) - start
    }

    /// 一致性位图快照。
    pub fn snapshot(&self) -> PieceSnapshot {
        PieceSnapshot {
            piece_len: self.piece_len,
            num_pieces: self.num_pieces(),
            bitfield: self.bitfield(),
        }
    }

    /// wire 语义位图副本（每片 1 bit：已落盘满 = 1）。
    pub fn bitfield(&self) -> Vec<u8> {
        self.bf.lock().unwrap().clone()
    }

    /// 部分下载位图：每片 1 bit：已落盘 > 0 但未满 = 1。
    /// 用于 UI 三态展示：未开始(灰)、下载中(黄)、已完成(绿)。
    pub fn partial_bitfield(&self) -> Vec<u8> {
        let n = self.done.len();
        let nbytes = n.div_ceil(8);
        let mut bf = vec![0u8; nbytes];
        for i in 0..n {
            let d = self.done[i].load(Ordering::Relaxed);
            // done[i] > 0 且 done[i] < len_at(i) → 部分下载
            if d > 0 && d < self.len_at(i) {
                bf[i / 8] |= 0x80 >> (i % 8);
            }
        }
        bf
    }
}

/// 分片下载成功结果。
#[derive(Debug, Clone, Copy)]
pub struct SplitDone {
    pub total_len: u64,
}

/// 控制文件目录与路径派生：与 BT 续传控制文件共用（见 `xfer_storage`）。
pub use xfer_storage::ctrl_path;

/// 旧版控制文件路径：`<目标文件>.xfer`（曾与下载文件同目录）。
fn legacy_ctrl_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".xfer");
    PathBuf::from(s)
}

/// 一次性迁移旧版控制文件到新位置（不存在则忽略）。
fn migrate_legacy_ctrl(path: &Path, ctrl: &Path) {
    let legacy = legacy_ctrl_path(path);
    if legacy.is_file() && !ctrl.exists() {
        if let Some(parent) = ctrl.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if std::fs::rename(&legacy, ctrl).is_err() {
            // 跨设备 rename 失败：回退复制
            if let Ok(bytes) = std::fs::read(&legacy) {
                if std::fs::write(ctrl, bytes).is_ok() {
                    let _ = std::fs::remove_file(&legacy);
                }
            }
        }
    }
}

/// 多连接分片下载入口。
///
/// 前置条件：`total > 0` 且服务器支持 Range（由调用方 probe 判断）。
/// 服务器实际不配合分段请求时返回 [`HttpError::NotSplittable`]——
/// 此时本地文件已被截断为连续前缀、控制文件已删除，
/// 调用方可安全回退单连接续传。
pub async fn download_split(
    client: &reqwest::Client,
    url: &str,
    path: &Path,
    total: u64,
    opts: &SplitOptions,
    cancel: &CancellationToken,
    stats: Arc<SplitStats>,
) -> Result<SplitDone, HttpError> {
    assert!(total > 0, "分片下载要求已知总长度");
    let ctrl = ctrl_path(path);
    migrate_legacy_ctrl(path, &ctrl);
    let done_notify = Arc::new(Notify::new());
    let fatal_slot: Arc<Mutex<Option<HttpError>>> = Arc::new(Mutex::new(None));
    let (tx, rx) = mpsc::channel::<ToWriter>(CHANNEL_CAP);
    // "停工"令牌：任务收尾时中止所有工作协程。
    let stop = CancellationToken::new();
    // 停工守卫：future 未走到正常收尾就被丢弃时（运行时关停中止任务、
    // 上层提前 drop），保证 stop 一定被取消。否则自适应评估线程
    // （原生线程，持有通道发送端）会滞留循环，写线程的通道接收
    // 永远等不到全部发送端关闭，进程退出将永久挂起
    // （tokio 阻塞池关停需等待写线程退出）。
    struct StopGuard(CancellationToken);
    impl Drop for StopGuard {
        fn drop(&mut self) {
            self.0.cancel();
        }
    }
    let _stop_guard = StopGuard(stop.clone());
    // 调度器期望的协程数（自适应增减的落点）：
    // - 初始 = 启动协程数；
    // - 调度器 Spawn 决策抬高 → 主循环补拉协程；
    // - 段转交/退休后由写线程按需重算（见 Writer::ensure_spare）——
    //   池子只减不增会让尾段只剩一条连接可用，这正是"暂停再继续才恢复"
    //   的现场：恢复动作重建了整池连接。
    let desired_workers = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    // 连接详情用主机名（同一 URI 的所有分片连接同主机）。
    let host: Arc<str> = Arc::from(
        reqwest::Url::parse(url)
            .ok()
            .and_then(|u| u.host_str().map(|h| h.to_string()))
            .unwrap_or_default()
            .as_str(),
    );

    // 自适应调度器初始化（若启用）
    let adaptive_tx = if opts.adaptive.is_some() {
        let aconfig = opts.adaptive.clone().unwrap_or_default();
        let (mut scheduler, perf_tx) = AdaptiveScheduler::new(aconfig);
        // 在单独的阻塞线程中运行自适应调度评估
        let eval_tx = tx.clone();
        let eval_stop = stop.clone();
        std::thread::spawn(move || {
            while !eval_stop.is_cancelled() {
                std::thread::sleep(scheduler.eval_interval());
                if eval_stop.is_cancelled() {
                    break;
                }
                let actions = scheduler.evaluate();
                // 将决策发送给 writer_loop 处理
                for action in actions {
                    let _ = eval_tx.blocking_send(ToWriter::AdaptiveAction { action });
                }
            }
        });
        Some(perf_tx)
    } else {
        None
    };

    // 分片位图：引擎预挂接（同步到任务状态查询）；独立调用（测试等）
    // 未挂接时就地创建。片长 = 进度显示粒度，与分段粒度（min-split-size）
    // 解耦：取 min-split-size 与 max(total/2048, 64KB) 的较小者——
    // 几 MB 的小文件也能及时点亮分片（否则一片 = 整个 min-split 段，
    // 下载几 MB 位图仍全零）；大文件按 min-split-size 保持既有粒度。
    // Writer::bootstrap 经 stats 取位图并按控制文件水位预填。
    if stats.piece_track().is_none() {
        let display_piece_len = opts
            .min_split_size
            .min((total / 2048).max(64 * 1024))
            .max(1);
        stats.attach_pieces(PieceTrack::new(total, display_piece_len));
    }

    // 写线程启动回执：携带应启动的工作协程数。
    let (ready_tx, ready_rx) = oneshot::channel::<io::Result<usize>>();
    let writer_task = {
        let wpath = path.to_path_buf();
        let wurl = url.to_string();
        let wopts = opts.clone();
        let wnotify = done_notify.clone();
        let wfatal = fatal_slot.clone();
        let wstats = stats.clone();
        let wdesired = desired_workers.clone();
        tokio::task::spawn_blocking(move || {
            let mut w = match Writer::bootstrap(
                &wpath, &ctrl, &wurl, total, &wopts, &wstats, &wnotify, &wfatal, &wdesired,
            ) {
                Ok(w) => w,
                Err(e) => {
                    let _ = ready_tx.send(Err(e));
                    return;
                }
            };
            // 立即持久化初始段表
            if let Err(e) = w.save_ctrl(true) {
                let _ = ready_tx.send(Err(e));
                return;
            }
            if w.finished() {
                wnotify.notify_one();
            }
            let n = w.spawn_count();
            let _ = ready_tx.send(Ok(n));
            writer_loop(w, rx);
        })
    };
    let nworkers = ready_rx
        .await
        .map_err(|_| HttpError::Io("写线程启动失败".into()))?
        .map_err(|e| HttpError::Io(e.to_string()))?;
    desired_workers.store(nworkers, Ordering::Release);

    // 逐任务自定义请求头：所有分片协程共享一份（Arc），每个段请求都带上。
    let headers: Arc<crate::RequestHeaders> = Arc::new(opts.headers.clone());

    // 拉起一个工作协程（初始并发与自适应补拉共用；panic 隔离：
    // 转 Fatal，不让任务静默消失造成死等）。
    let url_arc: Arc<str> = Arc::from(url);
    let spawn_worker = |tx: &mpsc::Sender<ToWriter>| -> tokio::task::JoinHandle<()> {
        let ctx = WorkerCtx {
            client: client.clone(),
            url: Arc::clone(&url_arc),
            host: Arc::clone(&host),
            tx: tx.clone(),
            stats: stats.clone(),
            stop: stop.clone(),
            perf_tx: adaptive_tx.clone(),
            limiter: opts.limiter.clone(),
            headers: Arc::clone(&headers),
        };
        tokio::spawn(run_worker_guarded(ctx))
    };

    let mut handles: Vec<tokio::task::JoinHandle<()>> =
        (0..nworkers).map(|_| spawn_worker(&tx)).collect();

    let mut tick = tokio::time::interval(Duration::from_millis(500));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let cancelled = loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break true,
            _ = done_notify.notified() => break false,
            _ = tick.tick() => {
                // 周期性保存控制文件（防崩溃丢进度；写线程内 1s 节流）；
                // 顺带在写线程里做租约回收与停滞兜底。
                let _ = tx.send(ToWriter::SaveCtrl).await;
                // 收割已退出的协程（Done/退休/panic 转移后自然结束）
                handles.retain(|h| !h.is_finished());
                // 按写线程/调度器期望补拉协程（增长与"转交换人"的落点）
                let want = desired_workers.load(Ordering::Acquire);
                while handles.len() < want {
                    handles.push(spawn_worker(&tx));
                    // 通知写线程存活数 +1（Spawn 决策据此计算增长目标）
                    let _ = tx.send(ToWriter::WorkerJoined).await;
                }
            }
        }
    };

    // ---- 收尾：停工 → join → 终局刷盘 ----
    stop.cancel();
    for h in handles {
        let _ = h.await;
    }
    let fatal = fatal_slot.lock().unwrap().clone();
    let mode = if fatal.is_none() && !cancelled {
        FinishMode::Success
    } else if matches!(fatal, Some(HttpError::NotSplittable(_))) {
        FinishMode::Discard
    } else {
        FinishMode::Preserve
    };
    let (ftx, frx) = oneshot::channel();
    let send_ok = tx.send(ToWriter::Finish { mode, reply: ftx }).await.is_ok();
    let fin = if send_ok {
        frx.await
            .unwrap_or_else(|_| Err(io::Error::other("写线程已退出")))
    } else {
        Err(io::Error::other("写线程通道已关闭"))
    };
    // 等待写线程完全退出（数据文件句柄与同步线程随之释放）：
    // Windows 上句柄未关闭时删除/移动文件会撞共享冲突，
    // 引擎侧的删文件/迁移等收尾必须在句柄释放后进行。
    let _ = writer_task.await;

    if cancelled {
        return Err(HttpError::Cancelled);
    }
    if let Some(e) = fatal {
        return Err(e);
    }
    fin.map_err(|e| HttpError::Io(e.to_string()))?;
    Ok(SplitDone { total_len: total })
}

// ---------------------------------------------------------------------------
// 协议：工作协程 → 写线程
// ---------------------------------------------------------------------------

enum ToWriter {
    /// 写入一段数据（`offset` 为绝对文件偏移）。
    Write {
        seg: usize,
        offset: u64,
        data: Bytes,
    },
    /// 交还当前段并领取新任务；`failures` 为该协程累计失败次数，
    /// 写线程据此决定让其退休（还有其他协程存活时）。
    Next {
        release: Option<(usize, bool)>,
        failures: u32,
        reply: oneshot::Sender<Assignment>,
    },
    /// 致命错误：任务终止。
    Fatal { err: HttpError },
    /// 保存控制文件（周期/段完成时）。
    SaveCtrl,
    /// 终局收尾。
    Finish {
        mode: FinishMode,
        reply: oneshot::Sender<io::Result<()>>,
    },
    /// 自适应调度决策（由调度线程发送）。
    AdaptiveAction { action: ScheduleAction },
    /// 主循环补拉了一个工作协程（写线程维护存活计数）。
    WorkerJoined,
}

/// 写线程对 Next 的答复。
enum Assignment {
    /// 领到一段：从 `from` 下载到 `end`（`end` 可能被对冲收缩，协程自截）。
    Work {
        seg: usize,
        from: u64,
        end: Arc<AtomicU64>,
    },
    /// 无段可领，停靠片刻再来。
    Park { ms: u64 },
    /// 退休：还有其他协程存活时的减员（失败过多 / 无长期价值）。
    Retire,
    /// 全部完成（或任务已终止）。
    Done,
}

enum FinishMode {
    /// 成功：sync + 截齐总长 + 删除控制文件。
    Success,
    /// 保留：sync + 保存控制文件（暂停/取消/普通失败）。
    Preserve,
    /// 丢弃：sync + 截断为连续前缀 + 删除控制文件（服务器不支持分段，
    /// 调用方将回退单连接续传）。
    Discard,
}

// ---------------------------------------------------------------------------
// 写线程（调度 + 磁盘 IO 单点）
// ---------------------------------------------------------------------------

/// 分片区间。多个协程可先后（甚至同时）持有同一段：水位守卫
/// （[`Writer::on_write`]）只接受恰接水位的那条流，因此并发持有
/// 不会双计、不会留空洞；`holders` 只用于归还判定（无人持有且
/// 未完成的段必须回到队列，否则 `todo` 永不归零）。
struct Seg {
    start: u64,
    /// 声明区间终点（对冲/转交收缩）。
    end: u64,
    /// pwrite 高水位（相对 start）。
    written: u64,
    done: bool,
    queued: bool,
    /// 正在读取本段的协程数。
    holders: usize,
    /// 最近一次有字节落盘的时刻（段租约的续租点）。
    last_progress: Instant,
    /// 与工作协程共享的端点（收缩广播）。
    end_shared: Arc<AtomicU64>,
}

#[derive(Serialize, Deserialize, Clone)]
struct CtrlSeg {
    s: u64,
    w: u64,
    e: u64,
}

#[derive(Serialize, Deserialize)]
struct CtrlFile {
    v: u32,
    total: u64,
    #[serde(default)]
    base: u64,
    #[serde(default)]
    url: String,
    segs: Vec<CtrlSeg>,
}

// ---------------------------------------------------------------------------
// 控制文件同步线程（数据文件 fsync + 控制文件原子写入脱离写线程）
// ---------------------------------------------------------------------------

/// 同步线程消息。
enum SyncMsg {
    /// 周期同步任务：携带序列化好的控制文件内容（水位快照，
    /// 提交前拍取 → fsync 完成后快照中的字节必然已落盘）。
    Job(Vec<u8>),
    /// 屏障：此前所有 Job 处理完后答复（强制保存/终局前排空在途任务）。
    Barrier(std::sync::mpsc::SyncSender<()>),
}

/// 控制文件同步器：写线程与同步线程的唯一通信入口。
///
/// 通道容量 1：至多一个任务在途 + 一个排队。提交满即跳过本轮
/// （下一心跳重试）——磁盘慢时自然降频，水位记录滞后只意味着
/// 崩溃后多重下最近 1~2s 的字节，幂等无害。
struct CtrlSyncer {
    tx: std::sync::mpsc::SyncSender<SyncMsg>,
}

impl CtrlSyncer {
    fn start(file: Arc<std::fs::File>, ctrl: PathBuf) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel::<SyncMsg>(1);
        std::thread::Builder::new()
            .name("xfer-ctrl-sync".into())
            .spawn(move || {
                // Writer 丢弃发送端（任务结束）→ recv 出错 → 线程退出。
                while let Ok(msg) = rx.recv() {
                    match msg {
                        SyncMsg::Job(bytes) => {
                            if let Err(e) = file.sync_all() {
                                tracing::warn!(error = %e, "数据文件同步失败，本轮控制文件不更新");
                                continue;
                            }
                            if let Err(e) = write_ctrl_atomic(&ctrl, &bytes) {
                                tracing::warn!(error = %e, "控制文件保存失败");
                            }
                        }
                        SyncMsg::Barrier(reply) => {
                            let _ = reply.send(());
                        }
                    }
                }
            })
            .expect("启动控制文件同步线程失败");
        Self { tx }
    }

    /// 提交一次周期同步任务；上一轮仍在途（通道满）返回 false。
    fn submit(&self, bytes: Vec<u8>) -> bool {
        self.tx.try_send(SyncMsg::Job(bytes)).is_ok()
    }

    /// 等待已提交的全部任务完成（强制保存与终局收尾前调用）。
    /// 屏障消息本身用阻塞 send：最坏等前一 Job 被消费，必然送达。
    fn barrier(&self) {
        let (btx, brx) = std::sync::mpsc::sync_channel::<()>(1);
        if self.tx.send(SyncMsg::Barrier(btx)).is_ok() {
            let _ = brx.recv();
        }
    }
}

/// 控制文件原子写入：tmp + sync + rename（崩溃时要么旧版要么新版）。
fn write_ctrl_atomic(ctrl: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = ctrl.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let tmp = ctrl.with_extension("xfer.tmp");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, ctrl)?;
    Ok(())
}

struct Writer {
    /// 数据文件句柄：Arc 共享给同步线程（fsync 用；pwrite 本身只需 &File）。
    file: Arc<std::fs::File>,
    path: PathBuf,
    ctrl: PathBuf,
    url: String,
    total: u64,
    /// 连续前缀基线（继承旧单连接引擎的已有文件）。
    base: u64,
    segs: Vec<Seg>,
    queue: VecDeque<usize>,
    /// 剩余字节数：== 0 即完成。
    todo: u64,
    /// 存活协程数（退休时递减）。
    alive: usize,
    fatal: Option<HttpError>,
    fatal_slot: Arc<Mutex<Option<HttpError>>>,
    finished: bool,
    stats: Arc<SplitStats>,
    done_notify: Arc<Notify>,
    last_save: Instant,
    opts: SplitOptions,
    /// 自适应调度模式是否启用。
    adaptive_enabled: bool,
    /// 调度器期望的协程数（与主循环共享：Spawn/转交抬高、退休递减）。
    desired_workers: Arc<std::sync::atomic::AtomicUsize>,
    /// 最近一次有字节落盘的时间（停滞兜底看门狗用）。
    last_progress: Instant,
    /// 可复用的控制文件序列化缓冲：避免每次 save_ctrl 都分配新 Vec。
    /// 下载过程中频繁保存控制文件时减少内存分配 / GC 压力。
    ctrl_buf: Vec<u8>,
    /// 控制文件同步器：周期性保存时把「数据文件 fsync + 控制文件
    /// 原子写入」整体委托给独立线程，写线程不因磁盘同步停摆。
    syncer: CtrlSyncer,
    /// 实时分片位图（来自 stats，写线程按落盘区间增量维护）。
    pieces: Option<Arc<PieceTrack>>,
}

impl Writer {
    #[allow(clippy::too_many_arguments)]
    fn bootstrap(
        path: &Path,
        ctrl: &Path,
        url: &str,
        total: u64,
        opts: &SplitOptions,
        stats: &Arc<SplitStats>,
        done_notify: &Arc<Notify>,
        fatal_slot: &Arc<Mutex<Option<HttpError>>>,
        desired_workers: &Arc<std::sync::atomic::AtomicUsize>,
    ) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(path)?;
        let file_len = file.metadata()?.len();
        let file = Arc::new(file);

        // 控制文件可用性：版本/总长匹配，且有进度的段水位不超过实际文件
        // 长度（目标文件被外部截断/替换时自动作废，从头下载）。
        // 注意 w==0 的段不校验：其起点天然可能超出当前文件长度
        // （pwrite 只扩展到最高写入位置，尚未开写的远端段起点在洞之外）。
        let loaded = load_ctrl(ctrl)
            .filter(|c| c.total == total)
            .filter(|c| c.segs.iter().all(|s| s.w == 0 || s.s + s.w <= file_len));
        // 段表严格平铺 [base, total) 校验：无重叠、无间隙。
        // 校验不过的控制文件必须**等价于没有控制文件**（走下面的 None
        // 分支：把已有文件当连续前缀、按需重新切段）——绝不能保留它的
        // 空段表继续走下去：空段表让下面 todo 算出 0，于是一个字节都
        // 没下载也判 `finished`（假成功，且目标文件不会被截齐到 total）。
        let loaded = loaded.and_then(|c| {
            // 零长度段（租约转交可能留下的空段）先剔除：它们不含任何
            // 字节，留着会让下面的严格平铺校验整体失败（详见
            // serialize_ctrl 的说明）。老版本写出的控制文件不含空段，
            // 这里只是对新旧格式都不挑剔。
            let mut sorted: Vec<CtrlSeg> = c.segs.into_iter().filter(|s| s.e > s.s).collect();
            sorted.sort_by_key(|s| s.s);
            let mut cursor = c.base.min(total);
            let tiled = !sorted.is_empty() && {
                let mut ok = true;
                for s in &sorted {
                    // 严格平铺 [base, total)：不允许重叠也不允许间隙。
                    // 间隙意味着一段字节脱离段表——todo 永不归零，
                    // 恢复时若把文件长度误当连续前缀，间隙中的空洞
                    // 会被静默跳过（假完成、文件损坏）。
                    if s.s != cursor || s.e <= s.s {
                        ok = false;
                        break;
                    }
                    cursor = s.e;
                }
                ok && cursor == total
            };
            if !tiled {
                tracing::warn!(path = %ctrl.display(), "控制文件段表不连续，忽略");
                return None;
            }
            Some((c.base.min(total), sorted))
        });
        let mut segs: Vec<Seg> = Vec::new();
        // 两条分支都必然赋值（控制文件可用 / 不可用）
        let base: u64;

        if let Some((ctrl_base, sorted)) = loaded {
            base = ctrl_base;
            for s in &sorted {
                // 已完成段必须保留在段表（done=true，不入队）：
                // 1) 序列化时随之保存——若在此丢弃，后续任何一次
                //    save_ctrl 都会写出有间隙的段表，下次恢复校验
                //    失败 → 文件长度被当作连续前缀 → 中间段空洞被
                //    静默跳过（假完成、文件损坏）；
                // 2) 恢复时据此跳过重下。
                let len = s.e - s.s;
                // 防御性 clamp：损坏的 w 超界会导致 todo 下溢
                let written = s.w.min(len);
                let done = written >= len;
                segs.push(Seg {
                    start: s.s,
                    end: s.e,
                    written,
                    done,
                    queued: !done,
                    holders: 0,
                    last_progress: Instant::now(),
                    end_shared: Arc::new(AtomicU64::new(s.e)),
                });
            }
        } else {
            // 无控制文件（或控制文件不可用）：把已有文件视为连续前缀
            //（兼容旧引擎进度）。
            base = file_len.min(total);
            let remain = total - base;
            if remain > 0 {
                let workers = opts.connections.max(1);
                let by_min = remain.div_ceil(opts.min_split_size).max(1) as usize;
                // 初始段数 ∈ [1, 2×workers]：上限防小文件碎片化，
                // 下限保住并行度（配合对冲切分动态均衡）。
                let nseg = by_min.clamp(1, workers * 2);
                let mut prev = base;
                for i in 1..=nseg {
                    let end = base + remain * (i as u64) / (nseg as u64);
                    segs.push(Seg {
                        start: prev,
                        end,
                        written: 0,
                        done: false,
                        queued: true,
                        holders: 0,
                        last_progress: Instant::now(),
                        end_shared: Arc::new(AtomicU64::new(end)),
                    });
                    prev = end;
                }
            }
            if file_len > total {
                // 目标文件比资源长（外部残留）：截齐，避免完成后长度错误
                file.set_len(total)?;
            }
        }

        let todo: u64 = segs.iter().map(|s| s.end - s.start - s.written).sum();
        let done = todo == 0;
        // 分片位图：按控制文件水位预填已完成区间（连续前缀基线 + 各段
        // 已写部分），与后续增量写入衔接——恢复后位图与真实落盘一致。
        let pieces = stats.piece_track();
        if let Some(p) = &pieces {
            if base > 0 {
                p.add_range(0, base);
            }
            for s in &segs {
                if s.written > 0 {
                    p.add_range(s.start, s.written);
                }
            }
        }
        stats.completed.store(
            base + (total - base).saturating_sub(todo),
            Ordering::Relaxed,
        );
        let mut w = Self {
            file: file.clone(),
            path: path.to_path_buf(),
            ctrl: ctrl.to_path_buf(),
            url: url.to_string(),
            total,
            base,
            segs,
            queue: Default::default(),
            todo,
            alive: 0,
            fatal: None,
            fatal_slot: fatal_slot.clone(),
            finished: done,
            stats: stats.clone(),
            done_notify: done_notify.clone(),
            last_save: Instant::now() - CTRL_SAVE_INTERVAL,
            opts: opts.clone(),
            adaptive_enabled: opts.adaptive.is_some(),
            desired_workers: desired_workers.clone(),
            last_progress: Instant::now(),
            ctrl_buf: Vec::new(),
            syncer: CtrlSyncer::start(file, ctrl.to_path_buf()),
            pieces,
        };
        w.rebuild_queue();
        Ok(w)
    }

    fn rebuild_queue(&mut self) {
        self.queue.clear();
        for (i, s) in self.segs.iter().enumerate() {
            if s.queued && !s.done {
                self.queue.push_back(i);
            }
        }
    }

    /// 应启动的协程数：并行度上限与初始段数取小。
    fn spawn_count(&self) -> usize {
        self.opts.connections.max(1).min(self.segs.len().max(1))
    }

    fn finished(&self) -> bool {
        self.finished
    }

    fn set_fatal(&mut self, err: HttpError) {
        if self.fatal.is_none() {
            tracing::warn!(error = %err, "分片下载致命错误");
            self.fatal = Some(err.clone());
            *self.fatal_slot.lock().unwrap() = Some(err);
            self.done_notify.notify_one();
        }
    }

    fn on_write(&mut self, sid: usize, offset: u64, data: &[u8]) {
        if self.fatal.is_some() {
            return;
        }
        let seg = match self.segs.get_mut(sid) {
            Some(s) => s,
            None => return,
        };
        // 越界（对冲/转交收缩后协程多读的部分）或回退写（重试/转交竞态）
        // → 丢弃。只接受恰好衔接水位的写：一段可能被两个持有者并发读取
        // （租约转交后新旧连接并存），接受越过水位的写会在 [水位, offset)
        // 留下永久空洞而 todo 照常归零（假完成、文件损坏）。落后的流只会
        // 被丢弃或追平，不会产生空洞。
        if offset < seg.start || offset >= seg.end || offset - seg.start != seg.written {
            return;
        }
        let n = ((seg.end - offset) as usize).min(data.len());
        if n == 0 {
            return;
        }
        if let Err(e) = write_at(&self.file, offset, &data[..n]) {
            let msg = e.to_string();
            self.set_fatal(HttpError::Io(msg));
            return;
        }
        seg.written = offset + n as u64 - seg.start;
        // 段续租：有字节落盘就不再算搁浅
        seg.last_progress = Instant::now();
        self.stats.completed.fetch_add(n as u64, Ordering::Relaxed);
        if let Some(p) = &self.pieces {
            p.add_range(offset, n as u64);
        }
        self.todo -= n as u64;
        self.last_progress = Instant::now();
        if seg.written == seg.end - seg.start && !seg.done {
            seg.done = true;
            // 段完成不触发 save_ctrl：由 1s 心跳统一节流保存。
            // 高频触发 fsync 是性能瓶颈（多连接并行完成段时尤其严重）。
        }
        if self.todo == 0 && !self.finished {
            self.finished = true;
            self.done_notify.notify_one();
        }
    }

    /// 当前生效的切分下限：尾声模式下放宽到 ENDGAME_MIN_SPLIT，
    /// 让空闲协程能细分最后一段并行收尾（避免单连接串行尾）。
    fn effective_min_split(&self) -> u64 {
        if self.todo <= ENDGAME_THRESHOLD {
            self.opts.min_split_size.min(ENDGAME_MIN_SPLIT)
        } else {
            self.opts.min_split_size
        }
    }

    /// 把 `[cut, seg.end)` 从段 `sid` 上切出，作为新段入队，返回新段号。
    ///
    /// `cut` 必须落在 `[水位, seg.end)` 内。原段端点收缩到 `cut` 并广播，
    /// 持有者（若有）在下一个块边界自行截断。**只切出、不复制**：
    /// `todo` 与已落盘字节都不变，段的剩余量在新旧段之间平移。
    fn split_off(&mut self, sid: usize, cut: u64) -> usize {
        let (old_end, start, written) = {
            let s = &self.segs[sid];
            (s.end, s.start, s.written)
        };
        debug_assert!(cut >= start + written && cut < old_end);
        self.segs[sid].end = cut;
        self.segs[sid].end_shared.store(cut, Ordering::Release);
        let nsid = self.segs.len();
        self.segs.push(Seg {
            start: cut,
            end: old_end,
            written: 0,
            done: false,
            queued: true,
            holders: 0,
            last_progress: Instant::now(),
            end_shared: Arc::new(AtomicU64::new(old_end)),
        });
        self.queue.push_back(nsid);
        nsid
    }

    /// 从剩余最多的活跃段对半切出一段新工作（工作窃取对冲）。
    /// 仅当剩余 ≥ 2×生效下限，保证切出的段不小于生效下限。
    fn try_steal(&mut self) -> Option<usize> {
        let floor = self.effective_min_split().saturating_mul(2);
        let mut best: Option<(u64, usize)> = None;
        for (i, s) in self.segs.iter().enumerate() {
            if s.done || s.queued {
                continue;
            }
            let remaining = s.end - s.start - s.written;
            if remaining >= floor && best.is_none_or(|(r, _)| remaining > r) {
                best = Some((remaining, i));
            }
        }
        let (_, sid) = best?;
        let (pos, old_end) = {
            let s = &self.segs[sid];
            (s.start + s.written, s.end)
        };
        let mid = pos + (old_end - pos) / 2;
        let nsid = self.split_off(sid, mid);
        tracing::debug!(
            seg = sid,
            from = mid,
            to = old_end,
            "对冲切分：空闲协程接管慢段后半区间"
        );
        Some(nsid)
    }

    /// 转交搁浅段的剩余区间：把 `[cut, end)` 交给新连接重新取（全新请求，
    /// 等价于用户手动"暂停再继续"，但不中断任务、已下字节全部保留）。
    ///
    /// `whole` 为真时整段转交（`cut` = 当前水位）；否则只转交后半，
    /// 持有者保留前半再给一次机会。原段被截断到 `cut`：持有者在下一个
    /// 块边界自行收尾（若它只是慢而不是死，前半仍归它继续收）。
    ///
    /// `force` 忽略 [`HANDOFF_MIN`]——兜底看门狗路径上无论多小的尾巴
    /// 都要换人（那里已确认全局长时间无进度，不值得再等持有者自救）。
    ///
    /// 与旧"读空闲超时"的区别：那条路要等**持有者**发现静默（10s 且
    /// 判据失效）并重连；这里由写线程在租约到期时直接换人，恢复时间
    /// 与持有者状态无关。
    fn handoff(&mut self, sid: usize, whole: bool, force: bool) -> bool {
        let (pos, old_end, holders) = {
            let Some(s) = self.segs.get(sid) else {
                return false;
            };
            (s.start + s.written, s.end, s.holders)
        };
        if pos >= old_end {
            return false;
        }
        let remaining = old_end - pos;
        if remaining < HANDOFF_MIN && !force {
            return false; // 太小的尾巴交给持有者自己的读空闲重连
        }
        let cut = if whole || remaining <= HANDOFF_WHOLE_MAX {
            pos
        } else {
            pos + remaining / 2
        };
        self.split_off(sid, cut);
        if cut == pos {
            // 剩余整段已切走：原段就此了结（迟到写入被 on_write 丢弃）
            self.segs[sid].done = true;
        }
        self.segs[sid].last_progress = Instant::now();
        tracing::debug!(
            seg = sid,
            cut,
            end = old_end,
            holders,
            whole = cut == pos,
            "租约转交：剩余区间交给新连接"
        );
        true
    }

    /// 保证有协程能接手被转交的区间：把期望协程数抬到「存活 + 1」，
    /// 上限为配置的连接数。
    ///
    /// 池子只减不增是本轮之前的另一处根因：协程因偶发失败退休后
    /// `desired_workers` 只降不升，尾段只剩一两条连接可用——用户看到的
    /// "连接数掉到 1、卡尾、暂停再继续才恢复"（恢复会重建整池连接）。
    fn ensure_spare(&mut self) {
        let cap = self.opts.connections.max(1);
        let want = (self.alive + 1).min(cap);
        if want > self.desired_workers.load(Ordering::Acquire) {
            self.desired_workers.store(want, Ordering::Release);
            tracing::debug!(alive = self.alive, want, "转交后扩充协程：保证有人接手");
        }
    }

    /// 租约回收：持有中但连续 [`SEG_LEASE`] 无字节落盘的段，剩余区间
    /// 直接转交给新连接（转交即让"此刻正在等活"的协程立刻接手）。
    fn reclaim_stalled(&mut self) {
        if self.finished || self.fatal.is_some() || self.todo == 0 {
            return;
        }
        let mut reclaimed = 0usize;
        for i in 0..self.segs.len() {
            let stalled = {
                let s = &self.segs[i];
                !s.done
                    && !s.queued
                    && s.holders > 0
                    && s.end - s.start > s.written
                    && s.last_progress.elapsed() >= SEG_LEASE
            };
            if stalled && self.handoff(i, false, false) {
                reclaimed += 1;
            }
        }
        if reclaimed > 0 {
            tracing::warn!(reclaimed, todo = self.todo, "段租约到期：搁浅区间已转交新连接");
            self.ensure_spare();
        }
    }

    fn next_work(&mut self, release: Option<(usize, bool)>, failures: u32) -> Assignment {
        if let Some((sid, _failed)) = release {
            if let Some(seg) = self.segs.get_mut(sid) {
                // 归还：段可能在归还前被转交/收缩（竞态），仍按当时状态判定。
                //
                // 关键不变式：未完成段要么在队列、要么有持有者。若丢了这个
                // 不变式，段会停在"既不在队、也无人续传"的搁浅态：todo 永不
                // 归零，任务卡在尾声零速（暂停/恢复恰能救活，因为恢复时
                // bootstrap 会把未完成段重新入队）。
                seg.holders = seg.holders.saturating_sub(1);
                let incomplete = !seg.done && seg.end - seg.start > seg.written;
                if incomplete && !seg.queued && seg.holders == 0 {
                    seg.queued = true;
                    self.queue.push_back(sid);
                }
            }
        }
        if self.fatal.is_some() || self.finished {
            // 终局：压低期望协程数，防主循环在收尾窗口补拉新协程
            self.desired_workers.store(0, Ordering::Release);
            return Assignment::Done;
        }
        // 租约回收先于派发：让"此刻正在等活"的协程立即接手搁浅区间
        self.reclaim_stalled();
        // 出队时跳过已完成段：在途写入竞态可能把队列中的段写完
        // （旧持有者的迟到写入落在 [written, end) 内会被接受），
        // 派发空区间会发出非法 Range 请求。
        while let Some(sid) = self.queue.pop_front() {
            if let Some(seg) = self.segs.get_mut(sid) {
                if seg.done {
                    continue;
                }
                seg.queued = false;
                seg.holders += 1;
                seg.last_progress = Instant::now();
                let from = seg.start + seg.written;
                return Assignment::Work {
                    seg: sid,
                    from,
                    end: seg.end_shared.clone(),
                };
            }
        }
        if let Some(sid) = self.try_steal() {
            let seg = &mut self.segs[sid];
            seg.queued = false;
            seg.holders += 1;
            seg.last_progress = Instant::now();
            return Assignment::Work {
                seg: sid,
                from: seg.start,
                end: seg.end_shared.clone(),
            };
        }
        // 失败较多的协程退休（至少保留一名存活；池子随后由
        // ensure_spare / 调度器 Spawn 在需要时补回）
        if failures >= 2 && self.alive > 1 {
            self.retire_one();
            return Assignment::Retire;
        }
        Assignment::Park { ms: PARK_MS }
    }

    /// 协程退休：存活数 -1 并同步压低期望数（主循环不会把
    /// 刻意的减员补回来；调度器后续 Spawn 可重新抬高）。
    fn retire_one(&mut self) {
        self.alive = self.alive.saturating_sub(1);
        self.desired_workers.store(self.alive, Ordering::Release);
    }

    /// 停滞兜底看门狗：全局长时间无任何字节落盘时，把"不在队列且无人
    /// 正常流转"的未完成段剩余区间整体转交。
    ///
    /// 第一道防线是段租约（[`SEG_LEASE`]，4s、按段判定），本看门狗只兜底
    /// 理论缺口——"段既不在队列、也没有持有者"（出队/归还竞态引入的 bug）
    /// 与"协程扑在非预期等待上"。因此阈值取 [`STALL_TIMEOUT`]（15s），
    /// 远大于租约，只在所有常规路径都失效时才出手。
    fn watchdog_requeue_stalled(&mut self) {
        if self.finished || self.fatal.is_some() || self.todo == 0 {
            return;
        }
        if self.last_progress.elapsed() < STALL_TIMEOUT {
            return;
        }
        let mut requeued = 0usize;
        for i in 0..self.segs.len() {
            let stalled = {
                let s = &self.segs[i];
                !s.done && !s.queued && s.end - s.start > s.written
            };
            if stalled && self.handoff(i, true, true) {
                requeued += 1;
            }
        }
        if requeued > 0 {
            tracing::warn!(requeued, todo = self.todo, "停滞看门狗：搁浅区间已整体转交");
            self.ensure_spare();
        }
        // 重置计时：给转交后的区间一个完整窗口，避免每个心跳重复转交刷屏
        self.last_progress = Instant::now();
    }

    /// 序列化控制文件内容（水位快照）到复用缓冲，返回可移交的字节。
    ///
    /// 注意：已完成段也要保存（w == e-s）——恢复时据此跳过重下，
    /// 且保证段表平铺 [base, total) 的校验可以通过。**零长度段除外**：
    /// 租约转交会把"尚未写入任何字节就整段交出"的原段截断成 [x, x)，
    /// 这类空段不含任何字节，写进控制文件反而会让恢复时的
    /// "严格平铺、无重叠无间隙"校验整体失败——校验一失败，控制文件
    /// 被当作不存在，文件长度被误当作连续前缀（中间空洞被静默跳过，
    /// 假完成、文件损坏）。
    fn serialize_ctrl(&mut self) -> io::Result<Vec<u8>> {
        let cf = CtrlFile {
            v: 1,
            total: self.total,
            base: self.base,
            url: self.url.clone(),
            segs: self
                .segs
                .iter()
                .filter(|s| s.end > s.start)
                .map(|s| CtrlSeg {
                    s: s.start,
                    w: s.written,
                    e: s.end,
                })
                .collect(),
        };
        // 复用预分配的序列化缓冲，避免每次保存都分配
        self.ctrl_buf.clear();
        serde_json::to_writer(&mut self.ctrl_buf, &cf)?;
        Ok(self.ctrl_buf.clone())
    }

    /// 保存控制文件（`forced` 绕过节流）。
    ///
    /// **周期保存（非强制）**：写线程只拍水位快照并提交给同步线程，
    /// 数据文件 `fsync` 与控制文件原子写入全部在同步线程完成——
    /// 写线程（唯一调度 + 写入点）绝不阻塞在磁盘同步上。
    /// 快照先于 `fsync` 拍取：快照记录的字节在 `fsync` 完成时必然
    /// 已落盘，崩溃安全不变式 `written ≤ 实际落盘` 保持不变。
    /// 提交失败（上一轮仍在途）不刷 `last_save`，下一心跳立即重试。
    ///
    /// **强制保存（启动/终局）**：先屏障排空在途任务，再同步
    /// `sync_all` + 原子写入，保证终局状态持久且不与异步任务
    /// 竞争同一临时文件。
    fn save_ctrl(&mut self, forced: bool) -> io::Result<()> {
        let now = Instant::now();
        if !forced {
            if now.duration_since(self.last_save) < CTRL_SAVE_INTERVAL {
                return Ok(());
            }
            let bytes = self.serialize_ctrl()?;
            if self.syncer.submit(bytes) {
                self.last_save = now;
            }
            return Ok(());
        }
        self.syncer.barrier();
        self.file.sync_all()?;
        let bytes = self.serialize_ctrl()?;
        write_ctrl_atomic(&self.ctrl, &bytes)?;
        self.last_save = now;
        tracing::debug!(path = %self.ctrl.display(), todo = self.todo, "控制文件已保存（强制）");
        Ok(())
    }

    /// 计算已完整写好的连续前缀（用于 NotSplittable 回退前的截断）。
    fn contiguous_prefix(&self) -> u64 {
        let mut idx: Vec<&Seg> = self.segs.iter().collect();
        idx.sort_by_key(|s| s.start);
        let mut cur = self.base;
        for seg in idx {
            if seg.start > cur {
                break;
            }
            let reach = seg.start + seg.written;
            if reach > cur {
                cur = reach;
            }
            if reach < seg.end {
                break;
            }
        }
        cur
    }

    fn finish(&mut self, mode: FinishMode) -> io::Result<()> {
        // 排空在途异步同步任务：防止终局后迟到的周期保存
        // 重建已删除的控制文件（Success）或覆盖截断后的状态。
        self.syncer.barrier();
        self.file.sync_all()?;
        match mode {
            FinishMode::Success => {
                self.file.set_len(self.total)?;
                let _ = std::fs::remove_file(&self.ctrl);
                tracing::info!(path = %self.path.display(), total = self.total, "分片下载完成");
            }
            FinishMode::Preserve => {
                self.save_ctrl(true)?;
            }
            FinishMode::Discard => {
                let prefix = self.contiguous_prefix();
                self.file.set_len(prefix)?;
                let _ = std::fs::remove_file(&self.ctrl);
                tracing::info!(prefix, "服务器不支持分段请求，回退单连接模式");
            }
        }
        Ok(())
    }

    /// 应用自适应调度决策。
    ///
    /// - `Shrink`：定向收缩调度器指认的慢段（段已完成/已切分时
    ///   回退收缩剩余最多者），尾部区间立即重建为新段入队
    ///   （语义同对冲切分 [`Writer::try_steal`]，字节不丢失）。
    /// - `Retire`：调度器判定该连接连续多个评估窗口无进展（≈3s）
    ///   → **转交**其剩余区间给新连接（语义同租约回收
    ///   [`Writer::reclaim_stalled`]）。不再只是"减员建议"：降池速不
    ///   解决问题，换一条新连接才会真正推进（用户的手动"暂停再继续"
    ///   就是这个动作）。
    /// - `Spawn`：抬高期望协程数（主循环 500ms 内补拉），上限
    ///   `max_connections` 且不超过当前段数的 2 倍——超出可并行
    ///   工作量的协程只能空转 Park；增长步长翻倍，吞吐仍在上升时
    ///   快速爬坡。
    /// - `Grow`：倾向性日志（快连接的尾部区间已通过对冲切分即时入队）。
    fn apply_adaptive_action(&mut self, action: ScheduleAction) {
        if !self.adaptive_enabled {
            return;
        }
        match action {
            ScheduleAction::Maintain => {}
            ScheduleAction::Grow { extra_bytes } => {
                tracing::trace!(extra_bytes, "自适应：快连接（尾部区间已即时入队）");
            }
            ScheduleAction::Shrink {
                conn_id,
                reclaim_bytes,
            } => {
                self.shrink_seg(conn_id, reclaim_bytes);
            }
            ScheduleAction::Retire { conn_id } => {
                if self.handoff(conn_id, false, false) {
                    tracing::debug!(conn_id, "自适应：停滞连接区间已转交新连接");
                    self.ensure_spare();
                }
            }
            ScheduleAction::Spawn => {
                let cap = self
                    .opts
                    .adaptive
                    .as_ref()
                    .map(|a| a.max_connections)
                    .unwrap_or(self.opts.connections)
                    .max(1)
                    // 工作量上限：协程远多于段数时无段可领只能 Park。
                    // 2× 留出对冲切分的动态扩段空间。
                    .min(self.segs.len().saturating_mul(2).max(1));
                // 目标 = min(存活×2, 上限)，至少 +1：爬坡快且不越上限
                let target = cap
                    .min(self.alive.saturating_mul(2).max(self.alive + 1))
                    .max(self.alive);
                if target > self.alive {
                    self.desired_workers.store(target, Ordering::Release);
                    tracing::debug!(from = self.alive, to = target, "自适应：扩充并发");
                }
            }
        }
    }

    /// 收缩指定慢段：把尾部 `reclaim` 字节切出为新段并入队。
    /// `preferred` 为调度器指认的段（决策依据）；其已失效
    /// （完成/剩余不足）时回退收缩剩余最多的活跃段。
    ///
    /// 与对冲切分共享同一语义：活跃协程通过 `end_shared` 观察到收缩后
    /// 自行截断，越界写入由 `on_write` 丢弃。尾部必须立即切出入队——
    /// 否则收缩的字节会永久脱离段表，`todo` 无法归零，任务将挂起。
    fn shrink_seg(&mut self, preferred: usize, reclaim_bytes: u64) {
        let floor = self.effective_min_split();
        // 目标选择：指认段有效则定向收缩，否则回退「收缩剩余最多者」。
        // 调度决策滞后一个评估周期，指认段可能已完成/已被对冲切分。
        let eligible = |s: &Seg| !s.done && s.end - s.start - s.written >= floor.saturating_mul(2);
        let sid = if self.segs.get(preferred).is_some_and(&eligible) {
            preferred
        } else if let Some(i) = self.find_largest_active_seg().filter(|&i| eligible(&self.segs[i]))
        {
            i
        } else {
            return;
        };
        let (start, written, old_end) = {
            let s = &self.segs[sid];
            (s.start, s.written, s.end)
        };
        let remaining = old_end - (start + written);
        if remaining < floor.saturating_mul(2) {
            return; // 两侧都不小于 min_split_size 才切
        }
        // 不做"reclaim < floor 就放弃"的提前返回：慢段的原始分配区间（
        // assigned_range）可能很小（尾部段尤甚），按比例算出的 reclaim
        // 会低于下限而被整天跳过——慢尾段因此一直留在慢连接上。这里
        // 由 clamp 抬到下限，保证每次评估至少把 floor 字节交给别的连接。
        let reclaim = reclaim_bytes.clamp(floor, remaining - floor);
        let new_end = old_end - reclaim;
        self.split_off(sid, new_end);
        tracing::debug!(
            seg = sid,
            from = new_end,
            to = old_end,
            targeted = sid == preferred,
            "自适应：收缩慢段，尾部切出入队"
        );
    }

    /// 找到剩余字节数最多的活跃（未完成、已入队或正在下载）段。
    fn find_largest_active_seg(&self) -> Option<usize> {
        let mut best: Option<(u64, usize)> = None;
        for (i, s) in self.segs.iter().enumerate() {
            if s.done {
                continue;
            }
            let remaining = s.end - s.start - s.written;
            if remaining > 0 && best.is_none_or(|(r, _)| remaining > r) {
                best = Some((remaining, i));
            }
        }
        best.map(|(_, i)| i)
    }
}

fn load_ctrl(ctrl: &Path) -> Option<CtrlFile> {
    let bytes = std::fs::read(ctrl).ok()?;
    match serde_json::from_slice::<CtrlFile>(&bytes) {
        Ok(c) if c.v == 1 => Some(c),
        _ => {
            tracing::warn!(path = %ctrl.display(), "控制文件损坏，忽略");
            None
        }
    }
}

fn writer_loop(mut w: Writer, mut rx: mpsc::Receiver<ToWriter>) {
    w.alive = w.spawn_count();
    while let Some(cmd) = rx.blocking_recv() {
        match cmd {
            ToWriter::Write { seg, offset, data } => w.on_write(seg, offset, &data),
            ToWriter::Next {
                release,
                failures,
                reply,
            } => {
                let a = w.next_work(release, failures);
                let _ = reply.send(a);
            }
            ToWriter::Fatal { err } => w.set_fatal(err),
            ToWriter::SaveCtrl => {
                // 心跳里的两道收尾防线：租约回收（搁浅段换人）在先，
                // 停滞兜底在后（兜底只处理"既不在队、也无人持有"的缺口）
                w.reclaim_stalled();
                w.watchdog_requeue_stalled();
                if let Err(e) = w.save_ctrl(false) {
                    tracing::warn!(error = %e, "控制文件保存失败");
                }
            }
            ToWriter::Finish { mode, reply } => {
                let _ = reply.send(w.finish(mode));
                return;
            }
            ToWriter::AdaptiveAction { action } => {
                w.apply_adaptive_action(action);
            }
            ToWriter::WorkerJoined => {
                w.alive += 1;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 工作协程（网络泵）
// ---------------------------------------------------------------------------

struct WorkerCtx {
    client: reqwest::Client,
    url: Arc<str>,
    tx: mpsc::Sender<ToWriter>,
    stats: Arc<SplitStats>,
    stop: CancellationToken,
    /// 自适应性能上报通道（None = 自适应未启用）。
    perf_tx: Option<mpsc::UnboundedSender<ConnPerf>>,
    /// 全局限速器（None = 不限速）。
    limiter: Option<Arc<RateLimiter>>,
    /// 逐任务自定义请求头（见 [`SplitOptions::headers`]）。Arc 共享：
    /// 每个工作协程各持一份引用，不重复拷贝整个头列表。
    headers: Arc<crate::RequestHeaders>,
    /// 连接详情用主机名。
    host: Arc<str>,
}

/// panic 隔离包装：协程 panic → 转致命错误，避免主流程死等。
async fn run_worker_guarded(ctx: WorkerCtx) {
    let tx = ctx.tx.clone();
    if let Err(p) = std::panic::AssertUnwindSafe(run_worker(ctx))
        .catch_unwind()
        .await
    {
        let msg = p
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| p.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "未知 panic".into());
        let _ = tx
            .send(ToWriter::Fatal {
                err: HttpError::Protocol(format!("分片协程 panic: {msg}")),
            })
            .await;
    }
}

async fn run_worker(ctx: WorkerCtx) {
    let mut failures = 0u32;
    // 短读独立预算：服务器尾部断流通常一两次就恢复，连续多次
    // （阈值内）只重试、不占 failures；超阈值才减值到 failures 一次，
    // 保留对"恒坏服务器"最终失败的能力，避免无限重试刷流量。
    let mut short_reads = 0u32;
    let mut release: Option<(usize, bool)> = None;
    loop {
        if ctx.stop.is_cancelled() {
            return;
        }
        let assign = match ask_next(&ctx, release.take(), failures).await {
            Some(a) => a,
            None => return, // 写线程已死
        };
        let Assignment::Work { seg, from, end } = assign else {
            match assign {
                Assignment::Done | Assignment::Retire => return,
                Assignment::Park { ms } => {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(ms)) => {}
                        _ = ctx.stop.cancelled() => return,
                    }
                    continue;
                }
                Assignment::Work { .. } => unreachable!(),
            }
        };
        match run_segment(&ctx, seg, from, &end).await {
            Ok(()) => {
                failures = 0;
                short_reads = 0;
                release = Some((seg, false));
            }
            Err(e) => {
                if matches!(e, HttpError::NotSplittable(_)) {
                    // 服务器实际不支持分段请求：整体回退单连接
                    let _ = ctx.tx.send(ToWriter::Fatal { err: e }).await;
                    return;
                }
                let is_short_read = matches!(e, HttpError::ShortRead);
                if is_short_read {
                    short_reads += 1;
                    if short_reads >= MAX_SHORT_READS {
                        // 短读风暴超出容忍：折算为一次正常失败，
                        // 由 failures 预算在更上层收敛整个任务。
                        failures += 1;
                        short_reads = 0;
                    }
                } else {
                    failures += 1;
                }
                if failures >= MAX_FAILURES {
                    let _ = ctx.tx.send(ToWriter::Fatal { err: e }).await;
                    return;
                }
                release = Some((seg, true));
                let ms = if is_short_read {
                    short_read_backoff_ms(short_reads)
                } else {
                    backoff_ms(failures)
                };
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(ms)) => {}
                    _ = ctx.stop.cancelled() => return,
                }
            }
        }
    }
}

fn backoff_ms(failures: u32) -> u64 {
    match failures {
        1 => 500,
        2 => 1000,
        _ => 2000,
    }
}

/// 短读重试退避（独立于普通失败的退避曲线）。
///
/// 短读（响应体短于请求区间）是可再生瞬态：重试从水位续传必然推进，
/// 退避过久只会拖长尾声收尾（线上"99% 停顿十几秒"的构成之一：
/// 0.5→2s 的短读退避在重试风暴下累积）。上限压到 1s——
/// 真正恒坏的服务器由 8 次短读折算失败的预算收敛，不受影响。
fn short_read_backoff_ms(n: u32) -> u64 {
    match n {
        0 | 1 => 250,
        2 => 500,
        _ => 1000,
    }
}

async fn ask_next(
    ctx: &WorkerCtx,
    release: Option<(usize, bool)>,
    failures: u32,
) -> Option<Assignment> {
    let (tx_reply, rx_reply) = oneshot::channel();
    ctx.tx
        .send(ToWriter::Next {
            release,
            failures,
            reply: tx_reply,
        })
        .await
        .ok()?;
    rx_reply.await.ok()
}

/// 连接表批量结算器（见 [`run_segment`]）：热路径只累加本地计数，
/// 攒够 [`CONN_FLUSH_BYTES`] 或超过 [`CONN_FLUSH_INTERVAL`] 才进表一次。
struct ConnMeter<'a> {
    stats: &'a SplitStats,
    id: u64,
    pending: u64,
    since: Instant,
}

impl ConnMeter<'_> {
    fn add(&mut self, n: u64) {
        self.pending += n;
        if self.pending >= CONN_FLUSH_BYTES || self.since.elapsed() >= CONN_FLUSH_INTERVAL {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.pending > 0 {
            self.stats.conn_bytes(self.id, self.pending);
            self.pending = 0;
        }
        self.since = Instant::now();
    }
}

impl Drop for ConnMeter<'_> {
    fn drop(&mut self) {
        self.flush();
    }
}

/// 下载一个段区间：`from..end`（`end` 可能被对冲/转交收缩，协程自截）。
///
/// 自适应模式启用时，采样 RTT / 吞吐 / 连接建立时间并上报给调度器；
/// 同时把本条请求登记进连接表（界面"连接详情"逐连接展示的数据源）。
async fn run_segment(
    ctx: &WorkerCtx,
    seg: usize,
    from: u64,
    end_shared: &Arc<AtomicU64>,
) -> Result<(), HttpError> {
    /// 连接存活守卫：退出时统一标记请求结束并递减活跃计数
    /// （所有 return 路径都覆盖，含取消/错误/panic 展开）。
    struct ConnLease<'a> {
        stats: &'a SplitStats,
        id: u64,
    }
    impl Drop for ConnLease<'_> {
        fn drop(&mut self) {
            self.stats.connections.fetch_sub(1, Ordering::Relaxed);
            self.stats.conn_close(self.id);
        }
    }

    let end = end_shared.load(Ordering::Acquire);
    // 空区间防御：在途写入竞态可能在派发前把该段写满
    // （next_work 出队已跳过 done 段，此为二重保险）——
    // 不发 bytes=X-(X-1) 这类非法 Range，直接按完成处理。
    if end <= from {
        tracing::trace!(seg, "派发时段已写满，跳过");
        return Ok(());
    }

    let conn_id = ctx.stats.conn_open(&ctx.host);
    ctx.stats.connections.fetch_add(1, Ordering::Relaxed);
    let _lease = ConnLease {
        stats: &ctx.stats,
        id: conn_id,
    };
    // 连接详情批量结算：热路径上只累加协程本地计数，攒够阈值才进表
    // （每块一次加锁在高吞吐下是白送的争用）。Drop 时结算余量，
    // 任何 return 路径都不会少记字节。声明在 `_lease` 之后 ⇒ 先析构，
    // 保证最后一笔进表后连接才被标记结束。
    let mut meter = ConnMeter {
        stats: &ctx.stats,
        id: conn_id,
        pending: 0,
        since: Instant::now(),
    };

    // 性能采样开始
    let t_request_start = Instant::now();

    // 首字节上限：请求发出（建连/TLS/服务端首包）超过 [`FIRST_BYTE_TIMEOUT`]
    // 即按短读重连续传；不再依赖 reqwest 的 read_timeout（30s，且会消耗
    // 普通失败预算）。
    let resp = tokio::time::timeout(
        FIRST_BYTE_TIMEOUT,
        crate::apply_headers(ctx.client.get(&*ctx.url), &ctx.headers)
            .header("Range", format!("bytes={}-{}", from, end - 1))
            .send(),
    )
    .await
    .map_err(|_| HttpError::ShortRead)?
    .map_err(|e| HttpError::from_reqwest(&e))?;

    // RTT = 请求发出到首字节到达
    let rtt = t_request_start.elapsed();
    // 注：reqwest 不直接暴露连接建立时间/TLS 握手耗时，
    // 未来可通过自定义连接器或 middleware 采样。当前仅用 RTT。
    let connect_time = None;
    let tls_time = None;

    let status = resp.status();
    if !status.is_success() {
        return Err(HttpError::Http(status.as_u16()));
    }
    // 非 206 的成功响应：
    // - from == 0 时响应体即完整文件前缀，可直接按段消费（部分 CDN
    //   对整段起点返回 200）；
    // - 否则视为服务器不支持分段。
    if status.as_u16() != 206 && from != 0 {
        return Err(HttpError::NotSplittable(format!(
            "Range 请求被忽略 (HTTP {status})"
        )));
    }

    let mut stream = resp.bytes_stream();
    let mut sent: u64 = 0;
    let mut truncated = false; // 命中对冲/转交收缩端点，提前结束
    let t_first_byte = Instant::now();
    let mut bytes_this_seg: u64 = 0;
    // 周期上报窗口：给调度器连续的实时指标（而非段末一次性突发）
    let mut bytes_since_report: u64 = 0;
    let mut last_report = t_first_byte;
    // 静默上限（绝对时刻）：自上一块起算，**不再按"是否尾声"分层**。
    // 分层判据在大文件 99% 时形同虚设（全局剩余仍有几十 MB），僵死连接
    // 照样等满 10s —— 用户看到的"99% 卡十几秒"就是这么来的。
    //
    // 首块单独放宽到 [`FIRST_BYTE_TIMEOUT`]（服务端"先给响应头、再慢慢
    // 生成首块"是正常行为；"接受请求后一直不吭声"的僵死则由段租约在
    // 4s 内换人，不靠这条超时兜着）。
    let mut gap_deadline = tokio::time::Instant::now() + FIRST_BYTE_TIMEOUT;
    loop {
        let chunk = tokio::select! {
            biased;
            _ = ctx.stop.cancelled() => return Ok(()), // 收尾由主流程统一处理
            c = stream.next() => match c {
                Some(Ok(c)) => c,
                Some(Err(e)) => return Err(HttpError::from_reqwest(&e)),
                None => break,
            },
            // 连接静默（对端无数据也无 FIN）→ 按短读处理：断开后从水位
            // 重连（全新连接），已收字节不丢、不占普通失败预算。
            _ = tokio::time::sleep_until(gap_deadline) => {
                tracing::debug!(seg, sent, "读空闲超时，重连续传");
                return Err(HttpError::ShortRead);
            }
        };
        if chunk.is_empty() {
            continue;
        }
        gap_deadline = tokio::time::Instant::now() + READ_IDLE_TIMEOUT;
        let end = end_shared.load(Ordering::Acquire);
        let pos = from + sent;
        if pos >= end {
            truncated = true;
            break;
        }
        let cap = ((end - pos) as usize).min(chunk.len());
        // 整块直传：满长块不做 slice（省去一次引用计数操作），
        // 仅命中收缩端点时才截取前缀（slice 共享底层内存，无拷贝）。
        let full_chunk = cap == chunk.len();
        let take = if full_chunk {
            chunk
        } else {
            chunk.slice(..cap)
        };
        // 全局限速：入队写线程前消费令牌，不足时等待（TCP 背压收敛）
        if let Some(l) = &ctx.limiter {
            l.acquire(cap).await;
        }
        ctx.tx
            .send(ToWriter::Write {
                seg,
                offset: pos,
                data: take,
            })
            .await
            .map_err(|_| HttpError::Io("写线程已退出".into()))?;
        sent += cap as u64;
        bytes_this_seg += cap as u64;
        bytes_since_report += cap as u64;
        // 连接详情记账：本连接已接收字节与速率（界面逐连接展示）
        meter.add(cap as u64);
        if let Some(perf_tx) = &ctx.perf_tx {
            let now = Instant::now();
            if bytes_since_report >= PERF_REPORT_BYTES
                || now.duration_since(last_report) >= PERF_REPORT_INTERVAL
            {
                let win = last_report.elapsed().as_secs_f64().max(0.001);
                let _ = perf_tx.send(ConnPerf {
                    conn_id: seg,
                    rtt: Some(rtt),
                    // 窗口瞬时吞吐；EWMA 平滑由调度器维护
                    throughput_ewma: bytes_since_report as f64 / win,
                    connect_time,
                    tls_time,
                    bytes_downloaded: bytes_this_seg,
                    last_report: now,
                    assigned_range: end - from,
                    stall_count: 0,
                });
                bytes_since_report = 0;
                last_report = now;
            }
        }
        if !full_chunk {
            truncated = true;
            break;
        }
    }

    // 段终上报：同步最终累计值（EWMA 字段由调度器保留，不覆盖）
    if let Some(perf_tx) = &ctx.perf_tx {
        let _ = perf_tx.send(ConnPerf {
            conn_id: seg,
            rtt: Some(rtt),
            throughput_ewma: 0.0,
            connect_time,
            tls_time,
            bytes_downloaded: bytes_this_seg,
            last_report: Instant::now(),
            assigned_range: end_shared.load(Ordering::Acquire) - from,
            stall_count: 0,
        });
    }

    if truncated {
        return Ok(());
    }
    // 服务器 EOF 早于请求区间 → 短读：可再生瞬态，按佣金独立
    // 重试（不计入正常失败预算——尾段断流风暴不应误杀整个任务，
    // 线上"99% 无速度、须暂停/恢复"根因）。
    let end = end_shared.load(Ordering::Acquire);
    if from + sent < end {
        return Err(HttpError::ShortRead);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 平台相关的位置无关写（pwrite / SeekWrite）
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn write_at(file: &std::fs::File, offset: u64, buf: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}

#[cfg(windows)]
fn write_at(file: &std::fs::File, offset: u64, buf: &[u8]) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut off = offset;
    let mut s = buf;
    while !s.is_empty() {
        let n = file.seek_write(s, off)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "seek_write 返回 0",
            ));
        }
        off += n as u64;
        s = &s[n..];
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{header, HeaderValue, StatusCode};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    /// 生成确定性测试数据（位置敏感，任何错位写都会被校验出来）。
    fn sample(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    /// 在途请求计数流：Drop 时递减，用于测真实并发峰值。
    /// （内部 Box::pin 以抹平 !Unpin 的组合子流。）
    struct GuardedStream<S> {
        inner: std::pin::Pin<Box<S>>,
        inflight: Arc<AtomicUsize>,
    }
    impl<S: futures_util::Stream> futures_util::Stream for GuardedStream<S> {
        type Item = S::Item;
        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            self.inner.as_mut().poll_next(cx)
        }
    }
    impl<S> Drop for GuardedStream<S> {
        fn drop(&mut self) {
            self.inflight.fetch_sub(1, Ordering::SeqCst);
        }
    }

    struct TestServer {
        addr: SocketAddr,
        peak_concurrency: Arc<AtomicUsize>,
        bytes_served: Arc<AtomicU64>,
    }

    impl TestServer {
        fn url(&self) -> String {
            format!("http://{}/file.bin", self.addr)
        }
    }

    /// 起一个支持 Range 的 axum 服务：按 8KB 块流式响应，`chunk_delay`
    /// 为每块的发送间隔（0 = 不延迟），`slow_from` 之后区域的块延迟
    /// 乘以 `slow_mult`（制造慢段以触发对冲窃取）。
    /// `truncate_first` = Some(n) 时每个区间的前 n 次请求返回截断响应
    /// （验证短读重试）。
    async fn start_server(
        data: Arc<Vec<u8>>,
        chunk_delay: Duration,
        slow_from: Option<u64>,
        slow_mult: u32,
        lie_200: bool,
    ) -> TestServer {
        start_server_ex(data, chunk_delay, slow_from, slow_mult, lie_200, None).await
    }

    /// `tail_truncation`：Some((tail_begin, prob)) 时，凡请求区间
    /// 结束点落在 `tail_begin` 之后的响应，以 `prob` 概率只发送
    /// 60%~99% 的应发字节后干净 EOF（响应体短于 206 声明的区间，
    /// 模拟真实 CDN/服务器在文件尾声的随机断流）。可复现
    /// "下到 99% 无速度" 的尾声卡死回归。
    async fn start_server_ex(
        data: Arc<Vec<u8>>,
        chunk_delay: Duration,
        slow_from: Option<u64>,
        slow_mult: u32,
        lie_200: bool,
        tail_truncation: Option<(u64, f64)>,
    ) -> TestServer {
        let peak = Arc::new(AtomicUsize::new(0));
        let inflight = Arc::new(AtomicUsize::new(0));
        let served = Arc::new(AtomicU64::new(0));
        let served_ret = served.clone();
        let peak_ret = peak.clone();
        let app = axum::Router::new().route(
            "/file.bin",
            axum::routing::get(move |headers: axum::http::HeaderMap| {
                let data = data.clone();
                let peak = peak.clone();
                let inflight = inflight.clone();
                let served = served.clone();
                let delay = chunk_delay;
                let slow_from = slow_from;
                let slow_mult = slow_mult;
                let lie_200 = lie_200;
                let tail_truncation = tail_truncation;
                async move {
                    let range = headers
                        .get(header::RANGE)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or_default()
                        .to_string();
                    let total = data.len();
                    let (from, to) =
                        match range.strip_prefix("bytes=").and_then(|r| r.split_once('-')) {
                            Some((f, t)) => (
                                f.parse::<usize>().unwrap_or(0),
                                t.parse::<usize>().unwrap_or(total),
                            ),
                            None => (0, total),
                        };
                    let from = from.min(total);
                    let to = (to + 1).min(total).max(from); // 转 [from, to)
                                                            // 谎报 200：对 from>0 的 Range 请求返回 200 + 完整文件
                                                            // （符合 HTTP 语义：200 必须携带完整资源）。
                    let lied = lie_200 && from > 0;
                    let (from, to) = if lied { (0, total) } else { (from, to) };
                    let this_delay = if slow_from.is_some_and(|s| from >= s as usize) {
                        delay.mul_f32(slow_mult as f32)
                    } else {
                        delay
                    };
                    // 尾声断流注入：请求区间结束点落在 tail_begin 之后、
                    // 且抽中概率时，提前 60%~99% 截断响应体（干净 EOF，
                    // 模拟真实服务器在文件尾声的随机断流）。
                    let mut truncate_at = to;
                    if let Some((tail_begin, prob)) = tail_truncation {
                        let counter = (from + to).wrapping_mul(2654435761) as u64
                            ^ served.load(Ordering::Relaxed).wrapping_mul(0x9E3779B97F4A7C15);
                        let mut x = counter | 1;
                        x ^= x >> 12;
                        x ^= x << 25;
                        x ^= x >> 27;
                        let r01 = (x.wrapping_mul(2685821657736338717) >> 33) as f64 / (1u64 << 31) as f64;
                        if to as u64 > tail_begin && r01 < prob {
                            let keep = 0.60 + r01 * 0.39; // 60%~99%
                            truncate_at = from + (((to - from) as f64 * keep) as usize).max(1);
                        }
                    }
                    let mut chunks: Vec<Result<Bytes, std::io::Error>> = Vec::new();
                    let mut off = from;
                    while off < truncate_at {
                        let end = (off + 8192).min(truncate_at);
                        chunks.push(Ok(Bytes::copy_from_slice(&data[off..end])));
                        off = end;
                    }
                    served.fetch_add((to - from) as u64, Ordering::Relaxed);
                    // 请求级在途计数：响应体 Drop 时递减（真实并发峰值）
                    // inflight 随请求生命周期增减；peak 只单调记录历史最大值。
                    let n = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(n, Ordering::SeqCst);
                    let stream = futures_util::stream::iter(chunks).then(move |c| {
                        let delay = this_delay;
                        async move {
                            if !delay.is_zero() {
                                tokio::time::sleep(delay).await;
                            }
                            c
                        }
                    });
                    let guarded = GuardedStream {
                        inner: Box::pin(stream),
                        inflight: inflight.clone(),
                    };
                    let body = axum::body::Body::from_stream(guarded);
                    let mut resp = axum::response::Response::new(body);
                    if lied {
                        // 谎报 200：测 NotSplittable 回退（body 已是完整文件）
                    } else if from > 0 || to < total {
                        *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                        resp.headers_mut().insert(
                            header::CONTENT_RANGE,
                            HeaderValue::from_str(&format!(
                                "bytes {}-{}/{}",
                                from,
                                to.saturating_sub(1),
                                total
                            ))
                            .unwrap(),
                        );
                    }
                    resp
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        TestServer {
            addr,
            peak_concurrency: peak_ret,
            bytes_served: served_ret,
        }
    }

    fn opts(connections: usize, min_split: u64) -> SplitOptions {
        SplitOptions {
            headers: crate::RequestHeaders::new(),
            connections,
            min_split_size: min_split,
            adaptive: None, // 测试默认不启用自适应
            limiter: None,
        }
    }

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("xfer-split-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        // 控制文件目录隔离：同进程所有测试共用一个（路径哈希互不冲突），
        // 且不污染真实 ~/.xfer/ctrl。**必须走全进程一次的初始化**——
        // 每个用例各自 set_var 成不同目录时，并行执行的用例会互相改写，
        // 详见 `crate::testutil`。
        crate::testutil::init_ctrl_dir();
        d
    }

    fn assert_file(path: &Path, expect: &[u8]) {
        let got = std::fs::read(path).unwrap();
        assert_eq!(got.len(), expect.len(), "文件长度不一致");
        assert_eq!(got, expect, "文件内容错位");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multi_connection_correctness_and_concurrency() {
        let data = Arc::new(sample(1024 * 1024));
        // 3ms/块制造足够流式时长，保证多个请求同时在途
        let srv = start_server(data.clone(), Duration::from_millis(3), None, 1, false).await;
        let dir = tmpdir("basic");
        let path = dir.join("out.bin");
        let stats = SplitStats::new(0);
        let cancel = CancellationToken::new();

        let done = download_split(
            &crate::build_client(),
            &srv.url(),
            &path,
            data.len() as u64,
            &opts(4, 64 * 1024),
            &cancel,
            stats,
        )
        .await
        .unwrap();
        assert_eq!(done.total_len, data.len() as u64);
        assert_file(&path, &data);
        // 多连接真的并行了
        assert!(
            srv.peak_concurrency.load(Ordering::SeqCst) >= 2,
            "未观察到并发请求: peak={}",
            srv.peak_concurrency.load(Ordering::SeqCst)
        );
        // 控制文件已清理
        assert!(!ctrl_path(&path).exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn slow_segment_steal_balances() {
        // 后 3/4 区域每块延迟 ×8：制造慢段，空闲协程应对冲窃取
        let data = Arc::new(sample(1024 * 1024));
        let len = data.len() as u64;
        let srv = start_server(
            data.clone(),
            Duration::from_millis(1),
            Some(len * 3 / 4),
            8,
            false,
        )
        .await;
        let dir = tmpdir("steal");
        let path = dir.join("out.bin");
        let cancel = CancellationToken::new();

        download_split(
            &crate::build_client(),
            &srv.url(),
            &path,
            len,
            &opts(4, 16 * 1024),
            &cancel,
            SplitStats::new(0),
        )
        .await
        .unwrap();
        assert_file(&path, &data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tail_truncation_storm_completes() {
        // 尾声断流回归：服务器在文件最后 1/8 的区间响应中大概率
        // 提前断流（干净 EOF 短读），迫使尾声阶段不断出现
        // 短读重试/释放回队——若调度在某条路径上把未完成段搁浅
        // （todo 不归零、协程全部 Park），下载会永久停在 99% 零速，
        // 只能靠暂停/恢复重建状态（线上反馈的真实缺陷，回归守卫）。
        for round in 0..10 {
            let tail_begin = (10 * 1024 * 1024) as u64 + round as u64;
            let len = (tail_begin + 2 * 1024 * 1024 + round * 7919) as usize;
            let data = Arc::new(sample(len));
            let srv = start_server_ex(
                data.clone(),
                Duration::ZERO,
                None,
                1,
                false,
                Some((tail_begin, 0.5)),
            )
            .await;
            let dir = tmpdir(&format!("tail-storm-{round}"));
            let path = dir.join("out.bin");
            let cancel = CancellationToken::new();
            let dl = tokio::spawn({
                let path = path.clone();
                async move {
                    download_split(
                        &crate::build_client(),
                        &srv.url(),
                        &path,
                        len as u64,
                        &opts(6, 512 * 1024),
                        &cancel,
                        SplitStats::new(0),
                    )
                    .await
                }
            });
            // 卡死保护：90s 未完成即判定失败（正常应远快于此）
            let r = tokio::time::timeout(Duration::from_secs(90), dl)
                .await
                .expect("尾声下载卡死（超过 90s）")
                .expect("下载任务 panic");
            r.expect("分片下载失败");
            assert_file(&path, &data);
        }
    }

    /// 回归（根因级）：僵死连接由**写线程换人**，不再等持有者自救。
    ///
    /// 场景：服务器对首个落在某区间的请求先发一小块后**永久静默**
    /// （连接半死，无数据也无 FIN），之后的请求一切正常。旧实现把恢复
    /// 完全押在"持有者读空闲超时后自己重连"上，且阈值按"剩余量是否
    /// 进入尾声"分层——大文件 99% 时剩余仍有几十 MB，判据失效 ⇒ 干等
    /// 10s（用户看到的"99% 卡十几秒、暂停再继续才恢复"）。新实现：
    /// 读空闲统一 3s + 段租约 4s 把剩余区间转交新连接。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dead_connection_seg_handed_off_fast() {
        let len = 16 * 1024 * 1024;
        let data = Arc::new(sample(len));
        let expect = data.clone();
        // 首个落在前 1MiB 的请求注入永久静默；后续同区间请求正常服务。
        // 恢复速度用**服务端时刻差**度量（僵死开始 → 同区间重连请求到达）：
        // 只含"客户端发现静默并换人"的耗时，不受同进程并行用例抢 CPU 影响。
        let poisoned = Arc::new(AtomicBool::new(false));
        let early_requests = Arc::new(AtomicUsize::new(0));
        let early_requests_srv = early_requests.clone();
        let stall_at: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
        let reconnect_at: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
        let stall_at_srv = stall_at.clone();
        let reconnect_at_srv = reconnect_at.clone();
        let app = axum::Router::new().route(
            "/file.bin",
            axum::routing::get(move |headers: axum::http::HeaderMap| {
                let data = data.clone();
                let poisoned = poisoned.clone();
                let early_requests = early_requests_srv.clone();
                let stall_at = stall_at_srv.clone();
                let reconnect_at = reconnect_at_srv.clone();
                async move {
                    let range = headers
                        .get(header::RANGE)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or_default()
                        .to_string();
                    let total = data.len();
                    let (from, to) =
                        match range.strip_prefix("bytes=").and_then(|r| r.split_once('-')) {
                            Some((f, t)) => (
                                f.parse::<usize>().unwrap_or(0),
                                t.parse::<usize>().unwrap_or(total),
                            ),
                            None => (0, total),
                        };
                    let from = from.min(total);
                    let to = (to + 1).min(total).max(from);
                    // 首个区间起点 < 1MiB 的请求：发 8KB 后永久挂起
                    let stall = from < 1024 * 1024 && !poisoned.swap(true, Ordering::SeqCst);
                    if from < 1024 * 1024 {
                        early_requests.fetch_add(1, Ordering::SeqCst);
                        if stall {
                            *stall_at.lock().unwrap() = Some(Instant::now());
                        } else if stall_at.lock().unwrap().is_some()
                            && reconnect_at.lock().unwrap().is_none()
                        {
                            *reconnect_at.lock().unwrap() = Some(Instant::now());
                        }
                    }
                    let mut head: Vec<Result<Bytes, std::io::Error>> = Vec::new();
                    let mut rest: Vec<Result<Bytes, std::io::Error>> = Vec::new();
                    let mut off = from;
                    while off < to {
                        let end = (off + 8192).min(to);
                        let c = Ok(Bytes::copy_from_slice(&data[off..end]));
                        if stall && off == from {
                            head.push(c);
                        } else {
                            rest.push(c);
                        }
                        off = end;
                    }
                    let stream = futures_util::stream::iter(head)
                        .chain(futures_util::stream::once(async move {
                            if stall {
                                // 永久静默：连接不关闭、不再发一个字节
                                std::future::pending::<()>().await;
                            }
                            Ok(Bytes::new())
                        }))
                        .chain(futures_util::stream::iter(rest));
                    let mut resp =
                        axum::response::Response::new(axum::body::Body::from_stream(stream));
                    *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                    resp.headers_mut().insert(
                        header::CONTENT_RANGE,
                        HeaderValue::from_str(&format!("bytes {}-{}/{}", from, to - 1, total))
                            .unwrap(),
                    );
                    resp
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });

        let dir = tmpdir("dead-conn-handoff");
        let path = dir.join("out.bin");
        let cancel = CancellationToken::new();
        let t0 = Instant::now();
        download_split(
            &crate::build_client(),
            &format!("http://{addr}/file.bin"),
            &path,
            len as u64,
            &opts(4, 64 * 1024),
            &cancel,
            SplitStats::new(0),
        )
        .await
        .expect("僵死连接不该拖死整条任务");
        let elapsed = t0.elapsed();
        assert_file(&path, &expect);
        assert!(
            early_requests.load(Ordering::SeqCst) >= 2,
            "前 1MiB 区间只被请求了一次：僵死段未被重新取（没有换人也没有重连）"
        );
        let stalled = stall_at.lock().unwrap().expect("僵死注入未生效");
        let recovered = reconnect_at
            .lock()
            .unwrap()
            .expect("僵死区间始终没有新的请求到达（任务靠别的路径侥幸完成？）");
        let recovery = recovered.duration_since(stalled);
        // 旧行为：分层读空闲在此规模（剩余 > 8MiB）下判据失效 ⇒ 干等 10s
        // 才重连；新行为：读空闲 3s / 租约 4s → 6s 内必有新请求落到该区间。
        assert!(
            recovery < Duration::from_secs(6),
            "僵死连接恢复耗时 {recovery:?}（总耗时 {elapsed:?}），疑似仍在等旧的分层读空闲/长超时"
        );
    }

    /// 连接详情的数据源：分片下载期间连接表必须反映**多条在飞连接**，
    /// 且结束后不再有在飞条目（界面"连接数恒为 1"的直接回归守卫）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn conn_snapshot_reports_live_connections() {
        let data = Arc::new(sample(1024 * 1024));
        let srv = start_server(data.clone(), Duration::from_millis(5), None, 1, false).await;
        let dir = tmpdir("conn-table");
        let path = dir.join("out.bin");
        let stats = SplitStats::new(0);
        let cancel = CancellationToken::new();
        let dl = {
            let stats = stats.clone();
            let url = srv.url();
            let path = path.clone();
            let cancel = cancel.clone();
            let total = data.len() as u64;
            tokio::spawn(async move {
                download_split(
                    &crate::build_client(),
                    &url,
                    &path,
                    total,
                    &opts(4, 64 * 1024),
                    &cancel,
                    stats,
                )
                .await
            })
        };
        let mut max_active = 0usize;
        let mut host_seen = String::new();
        while !dl.is_finished() {
            let snap = stats.conn_snapshot();
            max_active = max_active.max(snap.iter().filter(|c| c.active).count());
            if let Some(c) = snap.iter().find(|c| !c.host.is_empty()) {
                host_seen = c.host.clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        dl.await.unwrap().expect("分片下载失败");
        assert_file(&path, &data);
        assert!(
            max_active >= 2,
            "连接详情只看到 {max_active} 条在飞连接（并发 4 路却报不出多连接）"
        );
        assert_eq!(host_seen, "127.0.0.1", "连接条目应带上目标主机");
        assert!(
            stats.conn_snapshot().iter().all(|c| !c.active),
            "下载结束后不应残留在飞连接"
        );
    }

    /// 回归：读空闲超时。服务器对首个区间请求先发一小块后静默停摆
    /// （无数据也无 FIN，模拟连接僵死）。旧行为只能等 reqwest 30s
    /// read_timeout（归类 Timeout，消耗普通失败预算）；现在读空闲
    /// 统一 3s 即按短读从水位重连续传——总耗时应明显小于 30s。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_idle_stall_recovers_fast() {
        let len = 4 * 1024 * 1024;
        let data = Arc::new(sample(len));
        let expect = data.clone();
        let stall_flag = Arc::new(AtomicBool::new(false));
        let app = axum::Router::new().route(
            "/file.bin",
            axum::routing::get(move |headers: axum::http::HeaderMap| {
                let data = data.clone();
                let stall_flag = stall_flag.clone();
                async move {
                    let range = headers
                        .get(header::RANGE)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or_default()
                        .to_string();
                    let total = data.len();
                    let (from, to) =
                        match range.strip_prefix("bytes=").and_then(|r| r.split_once('-')) {
                            Some((f, t)) => (
                                f.parse::<usize>().unwrap_or(0),
                                t.parse::<usize>().unwrap_or(total),
                            ),
                            None => (0, total),
                        };
                    let from = from.min(total);
                    let to = (to + 1).min(total).max(from);
                    // 仅首个请求注入 12s 静默停摆（> 读空闲 3s）
                    let stall = !stall_flag.swap(true, Ordering::SeqCst);
                    let mut head: Vec<Result<Bytes, std::io::Error>> = Vec::new();
                    let mut rest: Vec<Result<Bytes, std::io::Error>> = Vec::new();
                    let mut off = from;
                    while off < to {
                        let end = (off + 8192).min(to);
                        let c = Ok(Bytes::copy_from_slice(&data[off..end]));
                        if stall && off == from {
                            head.push(c);
                        } else {
                            rest.push(c);
                        }
                        off = end;
                    }
                    let stalled = stall;
                    let stream = futures_util::stream::iter(head)
                        .chain(futures_util::stream::once(async move {
                            if stalled {
                                tokio::time::sleep(Duration::from_secs(12)).await;
                            }
                            Ok(Bytes::new())
                        }))
                        .chain(futures_util::stream::iter(rest));
                    let body = axum::body::Body::from_stream(stream);
                    let mut resp = axum::response::Response::new(body);
                    if from > 0 || to < total {
                        *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                        resp.headers_mut().insert(
                            header::CONTENT_RANGE,
                            HeaderValue::from_str(&format!(
                                "bytes {}-{}/{}",
                                from,
                                to.saturating_sub(1),
                                total
                            ))
                            .unwrap(),
                        );
                    }
                    resp
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        let url = format!("http://{addr}/file.bin");
        let dir = tmpdir("read-idle-stall");
        let path = dir.join("out.bin");
        let cancel = CancellationToken::new();
        let t0 = Instant::now();
        download_split(
            &crate::build_client(),
            &url,
            &path,
            len as u64,
            &opts(4, 256 * 1024),
            &cancel,
            SplitStats::new(0),
        )
        .await
        .expect("分片下载失败");
        let elapsed = t0.elapsed();
        assert_file(&path, &expect);
        // 旧行为：僵死连接要等 30s read_timeout 才恢复；现在读空闲 3s
        // 即重连续传。25s 上限证明走的是快路径（留足 CI 抖动余量）。
        assert!(
            elapsed < Duration::from_secs(25),
            "读空闲恢复耗时 {elapsed:?}，疑似仍卡在 30s read_timeout"
        );
    }

    /// 回归：静默恢复必须发生在**读空闲阈值**（[`READ_IDLE_TIMEOUT`]，3s）
    /// 附近——不因"剩余量多少"而分层放宽。
    ///
    /// 守护的正是线上「下载到 99% 卡十几秒」：最后一个分片的连接僵死，
    /// 其余协程全在 Park，任务停在 99% 零速。旧实现按"剩余是否 ≤ 8MiB"
    /// 分层取值，大文件 99% 时剩余仍有几十 MB ⇒ 判据失效、干等 10s。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn endgame_idle_stall_recovers_faster() {
        let len = 4 * 1024 * 1024;
        let data = Arc::new(sample(len));
        let expect = data.clone();
        let stall_flag = Arc::new(AtomicBool::new(false));
        // 服务端观测点：僵死开始时刻、以及**僵死 1s 之后**到达的首个请求
        // （= 客户端发现静默后从水位重连的那一次；1s 用于排除起始批次的
        // 并发请求）。
        //
        // 断言这两个时刻的间隔，而不是整个下载的耗时：总耗时里还叠着
        // 同进程并行用例争抢 CPU 的时间，与"静默阈值取多少"无关，却是
        // 把总耗时断言顶破的元凶。间隔只含"客户端发现连接静默所花的时间"。
        let stall_at: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
        let reconnect_at: Arc<Mutex<Option<Instant>>> = Arc::new(Mutex::new(None));
        let stall_at_h = stall_at.clone();
        let reconnect_at_h = reconnect_at.clone();
        let app = axum::Router::new().route(
            "/file.bin",
            axum::routing::get(move |headers: axum::http::HeaderMap| {
                let data = data.clone();
                let stall_flag = stall_flag.clone();
                let stall_at = stall_at_h.clone();
                let reconnect_at = reconnect_at_h.clone();
                async move {
                    let range = headers
                        .get(header::RANGE)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or_default()
                        .to_string();
                    let total = data.len();
                    let (from, to) =
                        match range.strip_prefix("bytes=").and_then(|r| r.split_once('-')) {
                            Some((f, t)) => (
                                f.parse::<usize>().unwrap_or(0),
                                t.parse::<usize>().unwrap_or(total),
                            ),
                            None => (0, total),
                        };
                    let from = from.min(total);
                    let to = (to + 1).min(total).max(from);
                    // 仅首个请求注入 12s 静默停摆（远大于尾声的 3s 读空闲）
                    let stall = !stall_flag.swap(true, Ordering::SeqCst);
                    let arrived = Instant::now();
                    if stall {
                        *stall_at.lock().unwrap() = Some(arrived);
                    } else {
                        // 起始那一批并发请求几乎同时到达，只有 1s 之后才来的
                        // 才可能是重连（读空闲 3s + 重连开销）
                        let stalled = *stall_at.lock().unwrap();
                        if let Some(t) = stalled {
                            if arrived.duration_since(t) > Duration::from_secs(1) {
                                let mut r = reconnect_at.lock().unwrap();
                                if r.is_none() {
                                    *r = Some(arrived);
                                }
                            }
                        }
                    }
                    let mut head: Vec<Result<Bytes, std::io::Error>> = Vec::new();
                    let mut rest: Vec<Result<Bytes, std::io::Error>> = Vec::new();
                    let mut off = from;
                    while off < to {
                        let end = (off + 8192).min(to);
                        let c = Ok(Bytes::copy_from_slice(&data[off..end]));
                        if stall && off == from {
                            head.push(c);
                        } else {
                            rest.push(c);
                        }
                        off = end;
                    }
                    let stalled = stall;
                    let stream = futures_util::stream::iter(head)
                        .chain(futures_util::stream::once(async move {
                            if stalled {
                                tokio::time::sleep(Duration::from_secs(12)).await;
                            }
                            Ok(Bytes::new())
                        }))
                        .chain(futures_util::stream::iter(rest));
                    let body = axum::body::Body::from_stream(stream);
                    let mut resp = axum::response::Response::new(body);
                    if from > 0 || to < total {
                        *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                        resp.headers_mut().insert(
                            header::CONTENT_RANGE,
                            HeaderValue::from_str(&format!(
                                "bytes {}-{}/{}",
                                from,
                                to.saturating_sub(1),
                                total
                            ))
                            .unwrap(),
                        );
                    }
                    resp
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        let url = format!("http://{addr}/file.bin");
        let dir = tmpdir("endgame-idle-stall");
        let path = dir.join("out.bin");
        let cancel = CancellationToken::new();
        let t0 = Instant::now();
        download_split(
            &crate::build_client(),
            &url,
            &path,
            len as u64,
            &opts(4, 256 * 1024),
            &cancel,
            SplitStats::new(0),
        )
        .await
        .expect("分片下载失败");
        let elapsed = t0.elapsed();
        assert_file(&path, &expect);
        let stalled = stall_at.lock().unwrap().expect("未观测到注入的僵死请求");
        let reconnected = reconnect_at.lock().unwrap().expect("未观测到读空闲后的重连请求");
        let gap = reconnected.duration_since(stalled);
        // 4MiB 总量全程落在尾声门槛（8MiB）内，应走 3s 读空闲。
        // 上限取 6s：既证明没等满常规 10s，又给 CI 抖动留足余量；
        // 而"僵死 → 重连"的间隔不含并行用例的 CPU 争抢，不会像
        // 总耗时那样在机器繁忙时被顶破。
        assert!(
            gap < Duration::from_secs(6),
            "僵死到重连间隔 {gap:?}，疑似仍按常规 10s 读空闲等待"
        );
        // 总耗时只作粗粒度兜底（防"整条任务挂死"），阈值给得足够松
        assert!(
            elapsed < Duration::from_secs(30),
            "下载总耗时 {elapsed:?}，疑似卡在 30s read_timeout"
        );
    }

    /// 回归：分片下载短读风暴 + 暂停/恢复循环下仍能正确完成。
    /// 直接穿引擎层会引入过多噪声，这里只验证 HTTP 层自身在
    /// 尾声断流 + 中途取消恢复的叠加下不丢字节、不假完成。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tail_truncation_with_cancel_resume() {
        let len = 16 * 1024 * 1024 + 12345usize;
        let tail_begin = (len - 2 * 1024 * 1024) as u64;
        let data = Arc::new(sample(len));
        // 2ms/8KB 块的确定性延迟：本地回环太快时，「观察到 >3/4」与
        // cancel 落地之间的窗口（10ms 轮询 + 派发）足以让剩余 25% 在
        // 无延迟服务器上直接下完 → 取消落在完成后返回 Ok 而非
        // Err(Cancelled)（CI 曾因此假失败）。2ms/块下剩余 25% 至少
        // 需要 ~170ms，取消必然先于完成。
        let srv = start_server_ex(
            data.clone(),
            Duration::from_millis(2),
            None,
            1,
            false,
            Some((tail_begin, 0.35)),
        )
        .await;
        let dir = tmpdir("tail-cancel-resume");
        let path = dir.join("out.bin");
        let cancel = CancellationToken::new();
        let stats = SplitStats::new(0);
        let client = crate::build_client();
        let url = srv.url();
        let total = len as u64;

        // 第一轮读到尾声（完成部分转移），中途取消
        let dl = tokio::spawn({
            let cancel = cancel.clone();
            let path = path.clone();
            let stats = stats.clone();
            async move {
                download_split(&client, &url, &path, total, &opts(6, 256 * 1024), &cancel, stats)
                    .await
            }
        });
        let mut done_any = false;
        // 10ms 轮询压缩「观察→取消」的未观察窗口；30s 上限兜底：
        // 并行跑测试套件时机器负载高，短上限会偶发误报
        for _ in 0..3000 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if stats.completed.load(Ordering::Relaxed) > len as u64 * 3 / 4 {
                done_any = true;
                break;
            }
        }
        assert!(done_any, "第一轮未推进到 3/4 以上");
        cancel.cancel();
        assert!(matches!(dl.await.unwrap(), Err(HttpError::Cancelled)));
        assert!(ctrl_path(&path).exists(), "取消后应保留控制文件");

        // 第二轮：断点续传完成（尾声断流仍按概率注入）
        download_split(
            &crate::build_client(),
            &srv.url(),
            &path,
            total,
            &opts(6, 256 * 1024),
            &CancellationToken::new(),
            SplitStats::new(0),
        )
        .await
        .unwrap();
        assert_file(&path, &data);
        assert!(!ctrl_path(&path).exists(), "完成后应删除控制文件");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancel_preserves_ctrl_and_resume() {
        let data = Arc::new(sample(1024 * 1024));
        let srv = start_server(data.clone(), Duration::from_millis(5), None, 1, false).await;
        let dir = tmpdir("resume");
        let path = dir.join("out.bin");

        // 第一轮：并发跑下载，中途取消
        let cancel = CancellationToken::new();
        let stats = SplitStats::new(0);
        let client = crate::build_client();
        let url = srv.url();
        let total = data.len() as u64;
        let dl = tokio::spawn({
            let stats = stats.clone();
            let cancel = cancel.clone();
            let path = path.clone();
            async move {
                download_split(
                    &client,
                    &url,
                    &path,
                    total,
                    &opts(4, 64 * 1024),
                    &cancel,
                    stats,
                )
                .await
            }
        });
        // 等下载真正跑起来（有进度）
        let mut got_progress = false;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if stats.completed.load(Ordering::Relaxed) > 32 * 1024 {
                got_progress = true;
                break;
            }
        }
        assert!(got_progress, "取消前未观察到下载进度");
        cancel.cancel();
        let r = dl.await.unwrap();
        assert!(matches!(r, Err(HttpError::Cancelled)));
        let partial = stats.completed.load(Ordering::Relaxed);
        assert!(partial > 0, "取消前应有部分进度");
        assert!(partial < data.len() as u64);
        assert!(ctrl_path(&path).exists(), "取消后应保留控制文件");
        let first_served = srv.bytes_served.load(Ordering::Relaxed);

        // 第二轮：断点续传
        let cancel2 = CancellationToken::new();
        download_split(
            &crate::build_client(),
            &srv.url(),
            &path,
            data.len() as u64,
            &opts(4, 64 * 1024),
            &cancel2,
            SplitStats::new(0),
        )
        .await
        .unwrap();
        assert_file(&path, &data);
        assert!(!ctrl_path(&path).exists(), "完成后应删除控制文件");
        // 续传真的少下了字节（本轮服务量 < 全量）
        let second_served = srv.bytes_served.load(Ordering::Relaxed) - first_served;
        assert!(
            second_served < data.len() as u64,
            "续传应跳过已完成部分: served={second_served}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn not_splittable_falls_back_with_clean_state() {
        // probe（bytes=0-0）返回 206，但实际区间请求谎报 200
        let data = Arc::new(sample(512 * 1024));
        let srv = start_server(data.clone(), Duration::ZERO, None, 1, true).await;
        let dir = tmpdir("fallback");
        let path = dir.join("out.bin");
        let cancel = CancellationToken::new();

        let r = download_split(
            &crate::build_client(),
            &srv.url(),
            &path,
            data.len() as u64,
            &opts(4, 64 * 1024),
            &cancel,
            SplitStats::new(0),
        )
        .await;
        match &r {
            Err(HttpError::NotSplittable(_)) => {}
            other => panic!("期望 NotSplittable，得到 {other:?}"),
        }
        // 文件被截断为连续前缀（无散写洞），控制文件已删除
        assert!(!ctrl_path(&path).exists());
        let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let expect_prefix = len as usize; // 回退后应从该前缀续传
        assert!(expect_prefix <= data.len());

        // 单连接回退：引擎已 force_fresh（服务器 Range 不可信），
        // 但若续传起点>0 服务器也应返回 200+完整文件，验证 restarted 语义。
        let mut sink = VecSink {
            buf: std::fs::read(&path).unwrap_or_default(),
        };
        let done = crate::download(
            &crate::build_client(),
            &srv.url(),
            sink.buf.len() as u64,
            &cancel,
            &mut sink,
            None,
        )
        .await
        .unwrap();
        assert_eq!(sink.buf, data[..]);
        // 服务器返回 200 + 完整文件 → restarted 全量传输
        assert_eq!(done.transferred, data.len() as u64);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn corrupted_ctrl_ignored() {
        let data = Arc::new(sample(256 * 1024));
        let srv = start_server(data.clone(), Duration::ZERO, None, 1, false).await;
        let dir = tmpdir("ctrlbad");
        let path = dir.join("out.bin");
        // 预置损坏控制文件
        std::fs::write(ctrl_path(&path), b"{ not json !!!").unwrap();
        download_split(
            &crate::build_client(),
            &srv.url(),
            &path,
            data.len() as u64,
            &opts(4, 64 * 1024),
            &CancellationToken::new(),
            SplitStats::new(0),
        )
        .await
        .unwrap();
        assert_file(&path, &data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tiny_file_single_segment() {
        let data = Arc::new(sample(100 * 1024));
        let srv = start_server(data.clone(), Duration::ZERO, None, 1, false).await;
        let dir = tmpdir("tiny");
        let path = dir.join("out.bin");
        download_split(
            &crate::build_client(),
            &srv.url(),
            &path,
            data.len() as u64,
            &opts(16, 1024 * 1024), // min_split 1MB > 文件 → 单段
            &CancellationToken::new(),
            SplitStats::new(0),
        )
        .await
        .unwrap();
        assert_file(&path, &data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn resume_from_legacy_prefix_file() {
        // 模拟旧单连接引擎遗留的连续前缀文件（无控制文件）
        let data = Arc::new(sample(512 * 1024));
        let srv = start_server(data.clone(), Duration::ZERO, None, 1, false).await;
        let dir = tmpdir("legacy");
        let path = dir.join("out.bin");
        std::fs::write(&path, &data[..300 * 1024]).unwrap();

        download_split(
            &crate::build_client(),
            &srv.url(),
            &path,
            data.len() as u64,
            &opts(4, 64 * 1024),
            &CancellationToken::new(),
            SplitStats::new(0),
        )
        .await
        .unwrap();
        assert_file(&path, &data);
        // 前缀之外的部分才重新下载
        let served = srv.bytes_served.load(Ordering::Relaxed);
        assert!(served <= data.len() as u64 - 300 * 1024 + 8192);
    }

    /// 测试用内存 sink（复用单连接 download 的回退验证）。
    struct VecSink {
        buf: Vec<u8>,
    }
    impl crate::TransferSink for VecSink {
        fn begin(&mut self, restarted: bool) -> std::io::Result<u64> {
            if restarted {
                self.buf.clear();
            }
            Ok(self.buf.len() as u64)
        }
        fn write_chunk(&mut self, data: &[u8]) -> std::io::Result<()> {
            self.buf.extend_from_slice(data);
            Ok(())
        }
        fn finish(&mut self) -> std::io::Result<u64> {
            Ok(self.buf.len() as u64)
        }
    }

    #[test]
    fn ctrl_path_is_hashed_in_ctrl_dir() {
        // 控制文件不再与下载文件同目录：位于隔离目录内，以路径哈希命名
        crate::testutil::init_ctrl_dir();
        let p = ctrl_path(Path::new("/tmp/a/b.zip"));
        let dir = xfer_storage::ctrl_dir();
        assert!(p.starts_with(&dir), "控制文件应位于数据目录: {p:?}");
        assert!(
            p.extension().is_some_and(|e| e == "xfer"),
            "控制文件应以 .xfer 结尾: {p:?}"
        );
        // 同一路径稳定命中，不同路径不冲突
        assert_eq!(p, ctrl_path(Path::new("/tmp/a/b.zip")));
        assert_ne!(p, ctrl_path(Path::new("/tmp/a/b.zip2")));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn legacy_ctrl_file_migrates_to_data_dir() {
        // 旧版控制文件（与下载文件同目录）应在启动时迁移到数据目录并正常续传
        let data = Arc::new(sample(256 * 1024));
        let srv = start_server(data.clone(), Duration::ZERO, None, 1, false).await;
        let dir = tmpdir("migrate");
        let path = dir.join("out.bin");
        // 伪造旧版控制文件：一段已写 100KB
        let legacy = dir.join("out.bin.xfer");
        std::fs::write(
            &legacy,
            serde_json::to_vec(&CtrlFile {
                v: 1,
                total: data.len() as u64,
                base: 0,
                url: srv.url(),
                segs: vec![CtrlSeg {
                    s: 0,
                    w: 100 * 1024,
                    e: data.len() as u64,
                }],
            })
            .unwrap(),
        )
        .unwrap();
        // 目标文件前 100KB 已落盘
        std::fs::write(&path, &data[..100 * 1024]).unwrap();

        download_split(
            &crate::build_client(),
            &srv.url(),
            &path,
            data.len() as u64,
            &opts(4, 64 * 1024),
            &CancellationToken::new(),
            SplitStats::new(0),
        )
        .await
        .unwrap();
        assert_file(&path, &data);
        // 旧位置文件已被迁移走
        assert!(!legacy.exists(), "旧控制文件应被迁移");
        assert!(!ctrl_path(&path).exists(), "完成后控制文件应清理");
        // 续传真的少下了字节
        let served = srv.bytes_served.load(Ordering::Relaxed);
        assert!(
            served <= data.len() as u64 - 100 * 1024 + 8192,
            "应从旧控制文件进度续传: served={served}"
        );
    }

    /// 自适应调度：启用后应正确完成下载，且自适应调度线程不影响正确性。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn adaptive_download_correctness() {
        let data = Arc::new(sample(2 * 1024 * 1024));
        let srv = start_server(data.clone(), Duration::from_millis(2), None, 1, false).await;
        let dir = tmpdir("adaptive");
        let path = dir.join("out.bin");
        let stats = SplitStats::new(0);
        let cancel = CancellationToken::new();

        let opts = SplitOptions {
            headers: crate::RequestHeaders::new(),
            connections: 4,
            min_split_size: 64 * 1024,
            adaptive: Some(AdaptiveConfig {
                enabled: true,
                initial_connections: 4,
                max_connections: 16,
                min_connections: 1,
                eval_interval: Duration::from_millis(500),
                ..Default::default()
            }),
            limiter: None,
        };

        let done = download_split(
            &crate::build_client(),
            &srv.url(),
            &path,
            data.len() as u64,
            &opts,
            &cancel,
            stats,
        )
        .await
        .unwrap();
        assert_eq!(done.total_len, data.len() as u64);
        assert_file(&path, &data);
        assert!(!ctrl_path(&path).exists(), "完成后控制文件应清理");
    }

    /// 自适应调度：慢段场景下自适应调度应完成下载（窃取 + 收缩协同）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn adaptive_slow_segment_completes() {
        let data = Arc::new(sample(1024 * 1024));
        let len = data.len() as u64;
        // 后 3/4 区域每块延迟 ×8：制造慢段
        let srv = start_server(
            data.clone(),
            Duration::from_millis(1),
            Some(len * 3 / 4),
            8,
            false,
        )
        .await;
        let dir = tmpdir("adaptive-slow");
        let path = dir.join("out.bin");
        let cancel = CancellationToken::new();

        let opts = SplitOptions {
            headers: crate::RequestHeaders::new(),
            connections: 4,
            min_split_size: 16 * 1024,
            adaptive: Some(AdaptiveConfig {
                enabled: true,
                initial_connections: 4,
                max_connections: 8,
                eval_interval: Duration::from_millis(300),
                ..Default::default()
            }),
            limiter: None,
        };

        download_split(
            &crate::build_client(),
            &srv.url(),
            &path,
            len,
            &opts,
            &cancel,
            SplitStats::new(0),
        )
        .await
        .unwrap();
        assert_file(&path, &data);
    }

    /// 自适应调度：禁用时应回退到固定连接数模式，行为不变。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn adaptive_disabled_behaves_like_fixed() {
        let data = Arc::new(sample(512 * 1024));
        let srv = start_server(data.clone(), Duration::from_millis(2), None, 1, false).await;
        let dir = tmpdir("adaptive-off");
        let path = dir.join("out.bin");
        let cancel = CancellationToken::new();

        let opts = SplitOptions {
            headers: crate::RequestHeaders::new(),
            connections: 4,
            min_split_size: 64 * 1024,
            adaptive: None, // 显式禁用
            limiter: None,
        };

        download_split(
            &crate::build_client(),
            &srv.url(),
            &path,
            data.len() as u64,
            &opts,
            &cancel,
            SplitStats::new(0),
        )
        .await
        .unwrap();
        assert_file(&path, &data);
    }

    /// 尾声切分下限：总剩余量 ≤ 阈值时放宽对冲窃取的切分下限，
    /// 修复收尾阶段空闲协程无法窃取、最后一段单连接串行收尾
    /// （总速率塌缩、速度骤降归零）。
    #[test]
    fn endgame_steal_relaxes_floor() {
        let dir = tmpdir("endgame");
        let notify = Arc::new(Notify::new());
        let fatal: Arc<Mutex<Option<HttpError>>> = Arc::new(Mutex::new(None));
        let desired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stats = Arc::new(SplitStats::new(0));
        let opts = SplitOptions {
            headers: crate::RequestHeaders::new(),
            connections: 4,
            min_split_size: 20 * 1024 * 1024,
            adaptive: None,
            limiter: None,
        };

        // 常规模式（todo > 阈值）：段剩余 25MB < 2×20MB 下限 → 不可窃取
        let path = dir.join("big.bin");
        let mut w = Writer::bootstrap(
            &path,
            &ctrl_path(&path),
            "http://x/f.bin",
            200 * 1024 * 1024,
            &opts,
            &stats,
            &notify,
            &fatal,
            &desired,
        )
        .unwrap();
        // 模拟协程持有段（next_work 派发时会清除 queued 标记）
        w.segs[0].queued = false;
        assert!(w.todo > ENDGAME_THRESHOLD, "应处于常规模式");
        assert!(
            w.try_steal().is_none(),
            "常规模式应维持 min_split_size 下限"
        );

        // 尾声模式（todo ≤ 阈值）：同样的大段可被连续细切
        let path2 = dir.join("tail.bin");
        let mut w2 = Writer::bootstrap(
            &path2,
            &ctrl_path(&path2),
            "http://x/t.bin",
            60 * 1024 * 1024,
            &opts,
            &stats,
            &notify,
            &fatal,
            &desired,
        )
        .unwrap();
        w2.segs[0].queued = false;
        assert!(w2.todo <= ENDGAME_THRESHOLD, "应处于尾声模式");
        // 60MB / 3 段 = 20MB；对半切 → 新段 [10MB, 20MB)
        let sid = w2.try_steal().expect("尾声模式应允许细切活跃段");
        assert_eq!(w2.segs[sid].start, 10 * 1024 * 1024);
        assert_eq!(w2.segs[sid].end, 20 * 1024 * 1024);
        // 可连续细切（每次对半，剩余 ≥ 2×256KiB 即可再切）
        let mut steals = 1usize;
        while w2.try_steal().is_some() {
            steals += 1;
        }
        assert!(steals >= 5, "尾声模式应能连续细切: {steals}");
        // 所有段都不小于尾声下限（细切不会产生碎片段）
        for s in &w2.segs {
            assert!(s.end - s.start >= ENDGAME_MIN_SPLIT);
        }
    }

    /// 搁浅回收回归：协程以"成功"释放未完成的段（收缩竞态路径），
    /// 段必须自动回队——否则 todo 永不归零，任务卡尾声零速，
    /// 必须暂停/恢复才能救活（旧引擎遗留缺陷）。
    #[test]
    fn released_incomplete_seg_requeues() {
        let dir = tmpdir("strand");
        let notify = Arc::new(Notify::new());
        let fatal: Arc<Mutex<Option<HttpError>>> = Arc::new(Mutex::new(None));
        let desired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stats = Arc::new(SplitStats::new(0));
        let opts = SplitOptions {
            headers: crate::RequestHeaders::new(),
            connections: 2,
            min_split_size: 1024,
            adaptive: None,
            limiter: None,
        };
        let path = dir.join("s.bin");
        let mut w = Writer::bootstrap(
            &path,
            &ctrl_path(&path),
            "http://x/s.bin",
            8192,
            &opts,
            &stats,
            &notify,
            &fatal,
            &desired,
        )
        .unwrap();

        // 模拟：协程领取段 0（出队），部分写入后被收缩截断，
        // 以 Ok 释放（release failed=false）。
        // 注意：8192 字节 / connections=2 → 初始 4 段 × 2048 字节
        // （段数上限 2×workers），写 1024 = 半段，确保段处于未完成态。
        let a1 = w.next_work(None, 0);
        let Assignment::Work { seg: sid, from, .. } = a1 else {
            panic!("应能领取段");
        };
        assert_eq!(from, 0);
        // 写入一半
        w.on_write(sid, 0, &[7u8; 1024]);
        assert_eq!(w.segs[sid].written, 1024);
        assert!(!w.segs[sid].queued);
        // Ok 释放（未完成！旧行为：不回队 → 搁浅）
        let a2 = w.next_work(Some((sid, false)), 0);
        // 队列里还有其他段，先派别的；被释放段应已回到队尾
        match a2 {
            Assignment::Work { .. } => {}
            Assignment::Park { .. } => panic!("队列不应为空"),
            _ => panic!("应有工作"),
        }
        assert!(w.segs[sid].queued, "未完成段必须回队");
        // 把队列消费干净，最终应能再次领到该段且从断点续传
        let mut reassigned = None;
        loop {
            match w.next_work(None, 0) {
                Assignment::Work { seg: s, from, .. } => {
                    if s == sid {
                        reassigned = Some(from);
                        break;
                    }
                }
                Assignment::Park { .. } => break,
                _ => panic!(),
            }
        }
        assert_eq!(reassigned, Some(1024), "搁浅段应从持久水位续传");
    }

    /// 水位连续性回归：越过水位的写入必须丢弃（看门狗回收后旧持有者
    /// "复活"与新持有者双流并行的竞态）——接受跳位写会在 [水位, offset)
    /// 留下永久空洞而 todo 照常归零（假完成、文件损坏）。恰好衔接水位
    /// 的写入正常接受。
    #[test]
    fn writes_must_be_contiguous_at_watermark() {
        let dir = tmpdir("contig");
        let notify = Arc::new(Notify::new());
        let fatal: Arc<Mutex<Option<HttpError>>> = Arc::new(Mutex::new(None));
        let desired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stats = Arc::new(SplitStats::new(0));
        let opts = SplitOptions {
            headers: crate::RequestHeaders::new(),
            connections: 1,
            min_split_size: 1024,
            adaptive: None,
            limiter: None,
        };
        let path = dir.join("g.bin");
        let mut w = Writer::bootstrap(
            &path,
            &ctrl_path(&path),
            "http://x/g.bin",
            8192,
            &opts,
            &stats,
            &notify,
            &fatal,
            &desired,
        )
        .unwrap();
        let Assignment::Work { seg: sid, from, .. } = w.next_work(None, 0) else {
            panic!("应能领取段");
        };
        assert_eq!(from, 0);
        let todo0 = w.todo;

        // 正常推进：0..1024
        w.on_write(sid, 0, &[1u8; 1024]);
        assert_eq!(w.segs[sid].written, 1024);
        assert_eq!(w.todo, todo0 - 1024);

        // 跳位写（2048 起，越过水位）：必须整体丢弃，不留空洞
        let completed = w.stats.completed.load(Ordering::Relaxed);
        w.on_write(sid, 2048, &[2u8; 1024]);
        assert_eq!(w.segs[sid].written, 1024, "跳位写不得推进水位");
        assert_eq!(w.todo, todo0 - 1024, "跳位写不得扣减 todo");
        assert_eq!(
            w.stats.completed.load(Ordering::Relaxed),
            completed,
            "跳位写不得计入进度"
        );

        // 恰好衔接水位的写：正常接受
        w.on_write(sid, 1024, &[3u8; 1024]);
        assert_eq!(w.segs[sid].written, 2048);
        assert_eq!(w.todo, todo0 - 2048);
    }

    /// 停滞看门狗：段不在队、无人认领（模拟协程挂死持有）时，
    /// 超时后把剩余区间**转交**（切出新段入队），不丢字节。
    #[test]
    fn watchdog_requeues_stranded_segs() {
        let dir = tmpdir("watchdog");
        let notify = Arc::new(Notify::new());
        let fatal: Arc<Mutex<Option<HttpError>>> = Arc::new(Mutex::new(None));
        let desired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stats = Arc::new(SplitStats::new(0));
        let opts = SplitOptions {
            headers: crate::RequestHeaders::new(),
            connections: 2,
            min_split_size: 1024,
            adaptive: None,
            limiter: None,
        };
        let path = dir.join("w.bin");
        let mut w = Writer::bootstrap(
            &path,
            &ctrl_path(&path),
            "http://x/w.bin",
            8192,
            &opts,
            &stats,
            &notify,
            &fatal,
            &desired,
        )
        .unwrap();

        // 协程领取段 0 后"挂死"：段不在队、written 部分推进
        let Assignment::Work { seg: sid, .. } = w.next_work(None, 0) else {
            panic!();
        };
        w.segs[sid].holders = 1;
        w.on_write(sid, 0, &[9u8; 1024]);
        assert!(!w.segs[sid].queued);
        let todo_before = w.todo;
        let end_before = w.segs[sid].end;
        let nsegs_before = w.segs.len();

        // 未到超时：看门狗不动
        w.watchdog_requeue_stalled();
        assert!(!w.segs[sid].queued, "超时前不应回收");

        // 快进停滞时间：libstd 无法 mock Instant，直接把进度时间拨回
        // 过去模拟长时间无进度
        w.last_progress = Instant::now() - STALL_TIMEOUT - Duration::from_secs(1);
        w.watchdog_requeue_stalled();
        // 转交语义：剩余区间切为新段入队（原段被截断到水位），
        // todo 不变、字节不丢
        assert_eq!(w.todo, todo_before, "转交不得改变剩余量");
        assert!(w.segs.len() > nsegs_before, "应切出新段");
        let nsid = w.segs.len() - 1;
        assert!(w.segs[nsid].queued, "新段应在队列里");
        assert_eq!(w.segs[nsid].start, 1024, "新段起点 = 原段水位");
        assert_eq!(w.segs[nsid].end, end_before, "新段覆盖原剩余区间");
        assert!(w.segs[sid].done, "原段截断到水位后即了结");
    }

    /// 回归：续传时已完成段必须保留在段表——否则保存后的控制文件
    /// 出现段间隙，下次恢复校验失败，文件长度被当作连续前缀，中间段
    /// 空洞被静默跳过（假完成、文件损坏）。
    ///
    /// 场景：并行乱序完成是常态（对冲窃取尤甚）——段 0/2 已完成、
    /// 段 1 只写了一半（此时文件长度已到 total，含中间空洞）。
    #[test]
    fn ctrl_roundtrip_preserves_completed_segs() {
        let dir = tmpdir("ctrl-done");
        let notify = Arc::new(Notify::new());
        let fatal: Arc<Mutex<Option<HttpError>>> = Arc::new(Mutex::new(None));
        let desired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stats = Arc::new(SplitStats::new(0));
        let opts = SplitOptions {
            headers: crate::RequestHeaders::new(),
            connections: 2,
            min_split_size: 1024,
            adaptive: None,
            limiter: None,
        };
        let path = dir.join("c.bin");
        let ctrl = ctrl_path(&path);
        let mb = 1024u64;
        // 第一轮保存的控制文件：3 段各 1MB，段 0/2 完成、段 1 过半
        std::fs::write(
            &ctrl,
            serde_json::to_vec(&CtrlFile {
                v: 1,
                total: 3 * mb,
                base: 0,
                url: "http://x/c.bin".into(),
                segs: vec![
                    CtrlSeg {
                        s: 0,
                        w: mb,
                        e: mb,
                    },
                    CtrlSeg {
                        s: mb,
                        w: mb / 2,
                        e: 2 * mb,
                    },
                    CtrlSeg {
                        s: 2 * mb,
                        w: mb,
                        e: 3 * mb,
                    },
                ],
            })
            .unwrap(),
        )
        .unwrap();
        // 数据文件已覆盖全部段（段 1 中间有未写入的空洞）
        std::fs::write(&path, vec![0u8; 3 * mb as usize]).unwrap();

        let w = Writer::bootstrap(
            &path,
            &ctrl,
            "http://x/c.bin",
            3 * mb,
            &opts,
            &stats,
            &notify,
            &fatal,
            &desired,
        )
        .unwrap();
        // 已完成段保留在段表（修复核心）；只剩段 1 的半程
        assert_eq!(w.segs.len(), 3, "已完成段不得从段表丢弃");
        assert_eq!(w.todo, mb / 2);
        assert!(w.segs[0].done && w.segs[2].done && !w.segs[1].done);
        assert!(w.segs.iter().any(|s| s.queued), "未完成段应入队");

        // 往返：save → 重新加载，段表必须仍然通过严格平铺校验
        let mut w = w;
        w.save_ctrl(true).unwrap();
        let w2 = Writer::bootstrap(
            &path,
            &ctrl,
            "http://x/c.bin",
            3 * mb,
            &opts,
            &stats,
            &notify,
            &fatal,
            &desired,
        )
        .unwrap();
        assert_eq!(w2.segs.len(), 3, "控制文件往返后必须保留已完成段");
        assert_eq!(w2.todo, mb / 2, "只有未完成段的剩余量待下");
        assert!(w2.segs.iter().any(|s| s.queued));
    }

    /// 段表不连续（间隙/重叠/空表）的控制文件必须等价于「无控制文件」：
    /// 重新按剩余量切段，绝不沿用空段表。
    ///
    /// 回归点：此前非平铺分支既不回落也不建段，段表为空 → todo 算出 0
    /// → `finished = true`——**一个字节都没下载就报完成**（且目标文件
    /// 连截齐到 total 都跳过），用户拿到的是残缺文件却显示成功。
    #[test]
    fn broken_ctrl_falls_back_to_fresh_split() {
        let dir = tmpdir("ctrl-broken");
        let notify = Arc::new(Notify::new());
        let fatal: Arc<Mutex<Option<HttpError>>> = Arc::new(Mutex::new(None));
        let desired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stats = Arc::new(SplitStats::new(0));
        let opts = SplitOptions {
            headers: crate::RequestHeaders::new(),
            connections: 2,
            min_split_size: 1024 * 1024,
            adaptive: None,
            limiter: None,
        };
        let path = dir.join("broken.bin");
        let ctrl = ctrl_path(&path);
        let mb = 1024u64 * 1024;
        let total = 3 * mb;
        // 段表有间隙（段 0 之后直接跳到 2MB）：总长匹配、水位也不越界，
        // 但脱离段表的 1MB 永远无人下载。
        std::fs::write(
            &ctrl,
            serde_json::to_vec(&CtrlFile {
                v: 1,
                total,
                base: 0,
                url: "http://x/broken.bin".into(),
                segs: vec![
                    CtrlSeg {
                        s: 0,
                        w: mb,
                        e: mb,
                    },
                    CtrlSeg {
                        s: 2 * mb,
                        w: 0,
                        e: 3 * mb,
                    },
                ],
            })
            .unwrap(),
        )
        .unwrap();
        // 数据文件已有 1MB（连续前缀）
        {
            let f = std::fs::File::create(&path).unwrap();
            f.set_len(mb).unwrap();
        }

        let mut w = Writer::bootstrap(
            &path,
            &ctrl,
            "http://x/broken.bin",
            total,
            &opts,
            &stats,
            &notify,
            &fatal,
            &desired,
        )
        .unwrap();
        assert!(!w.finished(), "段表不可用不得判完成");
        assert!(!w.segs.is_empty(), "必须重新切段，不能留空段表");
        // 已有 1MB 视作连续前缀，剩余 2MB 全部待下（含被跳过的那 1MB）
        assert_eq!(w.todo, 2 * mb);
        assert_eq!(w.base, mb);
        let covered: u64 = w.segs.iter().map(|s| s.end - s.start).sum();
        assert_eq!(covered, 2 * mb, "重切段必须无缝覆盖 [base, total)");

        // 重写控制文件后往返一次：仍是可用段表，进度不回退
        w.save_ctrl(true).unwrap();
        let w2 = Writer::bootstrap(
            &path,
            &ctrl,
            "http://x/broken.bin",
            total,
            &opts,
            &stats,
            &notify,
            &fatal,
            &desired,
        )
        .unwrap();
        assert!(!w2.finished(), "重写后的控制文件必须可用");
        assert_eq!(w2.todo, 2 * mb);
    }

    /// 定向收缩：优先收缩调度器指认的慢段（尾部重建入队）；
    /// 指认段已失效（完成/剩余不足）时回退收缩剩余最多的段。
    #[test]
    fn shrink_targets_identified_seg_with_fallback() {
        let dir = tmpdir("shrink");
        let notify = Arc::new(Notify::new());
        let fatal: Arc<Mutex<Option<HttpError>>> = Arc::new(Mutex::new(None));
        let desired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stats = Arc::new(SplitStats::new(0));
        let opts = SplitOptions {
            headers: crate::RequestHeaders::new(),
            connections: 2,
            min_split_size: 1024 * 1024,
            adaptive: None,
            limiter: None,
        };
        // 8MB / min_split 1MB → by_min=8，上限 2×workers=4 → 4 段 × 2MB
        let path = dir.join("t.bin");
        let mut w = Writer::bootstrap(
            &path,
            &ctrl_path(&path),
            "http://x/t.bin",
            8 * 1024 * 1024,
            &opts,
            &stats,
            &notify,
            &fatal,
            &desired,
        )
        .unwrap();
        assert_eq!(w.segs.len(), 4);
        let mb = 1024 * 1024u64;
        // 段 0 已完成（无效目标），段 1 下载中（指认的慢段）
        w.segs[0].done = true;
        w.segs[0].written = 2 * mb;
        w.segs[1].queued = false;

        // 指认段有效 → 定向收缩：尾部 1MB 重建为新段入队
        w.shrink_seg(1, mb);
        assert_eq!(w.segs[1].start, 2 * mb);
        assert_eq!(w.segs[1].end, 3 * mb, "应定向收缩指认段");
        assert_eq!(w.segs.len(), 5);
        let tail = &w.segs[4];
        assert_eq!((tail.start, tail.end), (3 * mb, 4 * mb));
        assert!(tail.queued, "收缩尾部必须立即入队");
        assert!(w.queue.contains(&4));

        // 指认段无效（已完成）→ 回退收缩剩余最多者（段 2 剩 2MB）
        w.shrink_seg(0, mb);
        assert_eq!(w.segs[2].end, 5 * mb, "应回退收缩剩余最多的段");
    }

    /// 自适应的"停滞连接"决策必须是**换人**（转交区间），不是减员。
    ///
    /// 旧行为：调度器每评估周期给一条停滞连接发一条 Retire 减员建议，
    /// 空闲协程在 `next_work` 里把建议消化成"自己退休"——一条僵死连接
    /// 每秒制造一条建议，空闲协程逐个退出，池子在一个下载内就塌到
    /// `min_connections`（=1）。现场表现正是用户报的"连接数一直只有 1、
    /// 尾巴卡住、暂停再继续才恢复"（恢复会重建整池连接）。
    #[test]
    fn adaptive_retire_hands_off_instead_of_draining_pool() {
        let dir = tmpdir("retire-handoff");
        let notify = Arc::new(Notify::new());
        let fatal: Arc<Mutex<Option<HttpError>>> = Arc::new(Mutex::new(None));
        let desired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stats = Arc::new(SplitStats::new(0));
        let opts = SplitOptions {
            headers: crate::RequestHeaders::new(),
            connections: 4,
            min_split_size: 1024 * 1024,
            adaptive: Some(AdaptiveConfig {
                enabled: true,
                initial_connections: 4,
                max_connections: 4,
                min_connections: 1,
                ..Default::default()
            }),
            limiter: None,
        };
        let path = dir.join("t.bin");
        let mut w = Writer::bootstrap(
            &path,
            &ctrl_path(&path),
            "http://x/t.bin",
            8 * 1024 * 1024,
            &opts,
            &stats,
            &notify,
            &fatal,
            &desired,
        )
        .unwrap();
        w.alive = 4;
        desired.store(4, Ordering::Release);

        // 协程 0 领取段 0 后停滞（只落了 1MiB）
        let Assignment::Work { seg: sid, .. } = w.next_work(None, 0) else {
            panic!("应能领取段");
        };
        w.segs[sid].holders = 1;
        w.on_write(sid, 0, &vec![1u8; 512 * 1024]);
        let nsegs_before = w.segs.len();
        let end_before = w.segs[sid].end;
        assert!(!w.segs[sid].done, "段应处于未完成（停滞）状态");

        // 调度器判定该连接停滞 → 转交
        w.apply_adaptive_action(ScheduleAction::Retire { conn_id: sid });

        assert_eq!(w.alive, 4, "不得因停滞连接而减员（池子只减不增是旧缺陷）");
        assert_eq!(
            desired.load(Ordering::Acquire),
            4,
            "期望协程数不得被压低（否则主循环再也不会补拉）"
        );
        assert!(w.segs.len() > nsegs_before, "停滞区间应切出为新工作");
        let nsid = w.segs.len() - 1;
        assert!(w.segs[nsid].queued && w.queue.contains(&nsid), "新工作必须立即可领");
        assert_eq!(w.segs[nsid].start, 512 * 1024, "新工作从停滞段水位开始");
        assert_eq!(w.segs[nsid].end, end_before);
        assert!(w.segs[sid].done, "原段已在转交点了结");
    }

    /// 自适应调度：吞吐上升时应真实扩充并发（评估 → Spawn →
    /// 期望协程数抬高 → 主循环补拉），服务端可观察到并发峰值增长。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn adaptive_spawn_grows_concurrency() {
        // 8MB / 3ms每8KB块：单连接约 2MB/s，下载需数秒——
        // 给评估-扩充机制留出足够的观察窗口
        let data = Arc::new(sample(8 * 1024 * 1024));
        let srv = start_server(data.clone(), Duration::from_millis(3), None, 1, false).await;
        let dir = tmpdir("adaptive-spawn");
        let path = dir.join("out.bin");
        let cancel = CancellationToken::new();

        let opts = SplitOptions {
            headers: crate::RequestHeaders::new(),
            // 初始只拉 1 个协程，扩充完全交给自适应调度
            connections: 1,
            min_split_size: 64 * 1024,
            adaptive: Some(AdaptiveConfig {
                enabled: true,
                initial_connections: 1,
                max_connections: 4,
                eval_interval: Duration::from_millis(300),
                ..Default::default()
            }),
            limiter: None,
        };

        download_split(
            &crate::build_client(),
            &srv.url(),
            &path,
            data.len() as u64,
            &opts,
            &cancel,
            SplitStats::new(0),
        )
        .await
        .unwrap();
        assert_file(&path, &data);
        // 并发应从 1 增长到 ≥3（1 → 2 → 4 的翻倍爬坡）
        let peak = srv.peak_concurrency.load(Ordering::SeqCst);
        assert!(
            peak >= 3,
            "自适应扩充未生效: peak={peak}（期望从 1 爬坡到 ≥3）"
        );
    }

    /// 分片下载的每个段请求都必须带上逐任务自定义请求头
    /// （Referer / Cookie / User-Agent），否则受保护地址只对缺头的
    /// 请求回 403，任务必然失败；同时调用方下发的 Range 不得覆盖
    /// 引擎自己算的分段区间。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn split_requests_carry_custom_headers() {
        let data = Arc::new(sample(256 * 1024));
        type Seen = Arc<std::sync::Mutex<Vec<(String, String, String, String)>>>;
        let seen: Seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let app = axum::Router::new().route(
            "/file.bin",
            axum::routing::get({
                let data = data.clone();
                let seen = seen.clone();
                move |headers: axum::http::HeaderMap| {
                    let data = data.clone();
                    let seen = seen.clone();
                    async move {
                        let get = |n: &str| {
                            headers
                                .get(n)
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or_default()
                                .to_string()
                        };
                        let range = get("range");
                        seen.lock().unwrap().push((
                            get("referer"),
                            get("cookie"),
                            get("user-agent"),
                            range.clone(),
                        ));
                        let total = data.len();
                        let (from, to) = match range
                            .strip_prefix("bytes=")
                            .and_then(|r| r.split_once('-'))
                        {
                            Some((f, t)) => (
                                f.trim().parse::<usize>().unwrap_or(0),
                                t.trim()
                                    .parse::<usize>()
                                    .unwrap_or(total - 1)
                                    .min(total - 1),
                            ),
                            None => (0, total - 1),
                        };
                        let body = data[from..=to].to_vec();
                        let mut resp =
                            axum::response::Response::new(axum::body::Body::from(body));
                        *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                        resp.headers_mut().insert(
                            header::CONTENT_RANGE,
                            HeaderValue::from_str(&format!(
                                "bytes {}-{}/{}",
                                from, to, total
                            ))
                            .unwrap(),
                        );
                        resp
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });

        let url = format!("http://{addr}/file.bin");
        let dir = tmpdir("custom-headers");
        let path = dir.join("out.bin");
        let opts = SplitOptions {
            headers: crate::parse_header_lines([
                "Referer: https://example.com/page",
                "Cookie: sid=1",
                "User-Agent: LerxuTest/1.0",
                "Range: bytes=0-1", // 必须被丢弃，不得覆盖分段区间
            ]),
            connections: 3,
            min_split_size: 32 * 1024,
            adaptive: None,
            limiter: None,
        };
        let done = download_split(
            &crate::build_client(),
            &url,
            &path,
            data.len() as u64,
            &opts,
            &CancellationToken::new(),
            SplitStats::new(0),
        )
        .await
        .unwrap();
        assert_eq!(done.total_len, data.len() as u64);
        assert_file(&path, &data);

        let seen = seen.lock().unwrap();
        assert!(seen.len() >= 2, "应发出多个段请求: {}", seen.len());
        for (referer, cookie, ua, range) in seen.iter() {
            assert_eq!(referer, "https://example.com/page");
            assert_eq!(cookie, "sid=1");
            assert_eq!(ua, "LerxuTest/1.0");
            assert!(range.starts_with("bytes="), "段请求必须带区间: {range}");
            assert_ne!(range, "bytes=0-1", "调用方下发的 Range 覆盖了分段区间");
        }
    }
}
