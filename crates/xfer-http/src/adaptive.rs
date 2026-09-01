//! Adaptive Download Scheduling（自适应下载调度）。
//!
//! 传统下载器使用固定连接数，XferRust 实时测量每条连接的：
//! - **RTT**（请求往返延迟）
//! - **瞬时吞吐**（bytes/s，EWMA 平滑）
//! - **连接建立时间**（TCP handshake + TLS）
//! - **TLS 开销**（握手耗时占比）
//!
//! 据此动态调整：
//! - 慢连接 → 收缩其 Range，尾部区间立即重建入队供快连接领取
//! - 停滞连接 → 退役（worker 退出，释放服务器/本端资源）
//! - 吞吐仍在上升 → 扩充并发（翻倍爬坡，受 max_connections 约束）
//! - 自动寻找当前网络/服务器条件下的最优并发度
//!
//! 架构：`AdaptiveScheduler` 由独立评估线程持有（`download_split`
//! 启动），worker 协程通过通道上报窗口指标，评估线程周期性
//! 计算决策并发给写线程执行（Range 重分配）与主循环（协程增减）。

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

/// 单条连接的性能快照（由工作协程采样上报）。
#[derive(Debug, Clone)]
pub struct ConnPerf {
    /// 连接 ID（与段 ID 对应）。
    pub conn_id: usize,
    /// 首字节 RTT（请求发出到首字节到达）。
    pub rtt: Option<Duration>,
    /// 瞬时吞吐（bytes/s），基于最近活跃窗口的 EWMA。
    pub throughput_ewma: f64,
    /// 连接建立耗时（TCP handshake，含 TLS 如果适用）。
    pub connect_time: Option<Duration>,
    /// TLS 握手耗时（若有）。
    pub tls_time: Option<Duration>,
    /// 该连接已下载的字节数。
    pub bytes_downloaded: u64,
    /// 最近一次上报时间。
    pub last_report: Instant,
    /// 最近一次该连接被分配的字节区间大小。
    pub assigned_range: u64,
    /// 连续无进度上报次数（用于判断停滞）。
    pub stall_count: u32,
}

impl Default for ConnPerf {
    fn default() -> Self {
        Self {
            conn_id: 0,
            rtt: None,
            throughput_ewma: 0.0,
            connect_time: None,
            tls_time: None,
            bytes_downloaded: 0,
            last_report: Instant::now(),
            assigned_range: 0,
            stall_count: 0,
        }
    }
}

/// 自适应调度器的配置参数。
#[derive(Debug, Clone)]
pub struct AdaptiveConfig {
    /// 是否启用自适应调度（false 时回退固定连接数模式）。
    pub enabled: bool,
    /// 初始连接数（启动时的并发度）。
    pub initial_connections: usize,
    /// 最大连接数上限。
    pub max_connections: usize,
    /// 最小连接数下限。
    pub min_connections: usize,
    /// 调度评估周期（多久做一次决策）。
    pub eval_interval: Duration,
    /// 吞吐 EWMA 衰减系数（0~1，越大越敏感）。
    pub ewma_alpha: f64,
    /// 慢连接判定阈值：吞吐低于整体均值的此比例 → 减载。
    pub slow_threshold_ratio: f64,
    /// 快连接判定阈值：吞吐高于整体均值的此比例 → 加载。
    pub fast_threshold_ratio: f64,
    /// 停滞判定：连续 N 次评估无进度增长 → 标记停滞。
    pub stall_eval_limit: u32,
    /// 单次 Range 重分配的最大比例（避免剧烈波动）。
    pub rebalance_step_ratio: f64,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_connections: 4,
            max_connections: 64,
            min_connections: 1,
            eval_interval: Duration::from_millis(1500),
            ewma_alpha: 0.3,
            slow_threshold_ratio: 0.5,
            fast_threshold_ratio: 1.5,
            stall_eval_limit: 3,
            rebalance_step_ratio: 0.25,
        }
    }
}

/// 调度决策：对特定连接的操作指令。
#[derive(Debug, Clone)]
pub enum ScheduleAction {
    /// 继续维持当前分配。
    Maintain,
    /// 给该连接追加 Range（快连接）。
    Grow { extra_bytes: u64 },
    /// 收缩该连接的 Range（慢连接），将剩余转让给其他连接。
    /// `conn_id` 为决策依据的段 ID；写线程发现其已失效
    /// （完成/已切分）时回退「收缩剩余最多者」。
    Shrink { conn_id: usize, reclaim_bytes: u64 },
    /// 退役该连接（停滞或持续低效）。
    Retire,
    /// 新增一个连接。
    Spawn,
}

/// 自适应调度器（由独立评估线程持有）。
pub struct AdaptiveScheduler {
    config: AdaptiveConfig,
    /// 各连接的性能快照。
    perf: HashMap<usize, ConnPerf>,
    /// 上次评估时间。
    last_eval: Instant,
    /// 上次评估时的整体吞吐（用于趋势判断）。
    last_total_throughput: f64,
    /// 历史最优并发度（最高整体吞吐时对应的连接数）。
    best_conn_count: usize,
    /// 历史最高整体吞吐。
    best_throughput: f64,
    /// 评估周期计数。
    eval_count: u64,
    /// 上一次评估时各连接的已下载字节数（用于计算增量）。
    last_bytes: HashMap<usize, u64>,
    /// 各连接参与过的评估次数（冷启动保护：<2 次采样的连接
    /// 不参与慢速/停滞判定——首次评估的增量结构性为 0）。
    eval_seen: HashMap<usize, u32>,
    /// 接收协程性能上报的通道。
    report_rx: mpsc::UnboundedReceiver<ConnPerf>,
}

impl AdaptiveScheduler {
    pub fn new(config: AdaptiveConfig) -> (Self, mpsc::UnboundedSender<ConnPerf>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let now = Instant::now();
        let initial = config.initial_connections;
        let scheduler = Self {
            config,
            perf: HashMap::new(),
            last_eval: now,
            last_total_throughput: 0.0,
            best_conn_count: initial,
            best_throughput: 0.0,
            eval_count: 0,
            last_bytes: HashMap::new(),
            eval_seen: HashMap::new(),
            report_rx: rx,
        };
        (scheduler, tx)
    }

    /// 接收所有待处理的上报（非阻塞）。合并进快照表时保留调度器侧
    /// 维护的状态：EWMA（worker 只提供窗口瞬时值，平滑在 evaluate 做）
    /// 与停滞计数 `stall_count`（worker 上报恒为 0，累计在评估侧，
    /// 若被上报覆盖则永远无法达到退役阈值）。
    pub fn drain_reports(&mut self) {
        while let Ok(report) = self.report_rx.try_recv() {
            let entry = self.perf.entry(report.conn_id).or_default();
            let ewma = entry.throughput_ewma;
            let stall = entry.stall_count;
            *entry = report;
            if ewma > 0.0 {
                entry.throughput_ewma = ewma;
            }
            entry.stall_count = stall;
        }
    }

    /// 执行一次调度评估，返回对各连接的决策 + 是否需要新增连接。
    ///
    /// 调用时机：评估线程按 `eval_interval` 周期调用。
    ///
    /// **冷启动保护**：新连接首次参与评估时，增量基线回退为当前
    /// 累计值 → 增量结构性为 0。若据此判定慢速/停滞，刚启动的快
    /// 连接会被误收缩（且 EWMA 从 0 衰减需要多个窗口才追上真实
    /// 速度，期间持续被误判）。因此：样本数 <2 的连接不参与慢速/
    /// 停滞判定，首个有效样本直接作为 EWMA 初值（无历史可平滑）。
    pub fn evaluate(&mut self) -> Vec<ScheduleAction> {
        self.drain_reports();
        // 清理已结束段（不再上报）的陈旧快照，防止长任务快照表无限增长
        let keep = self.config.eval_interval * 4;
        self.perf.retain(|_, p| p.last_report.elapsed() < keep);
        self.last_bytes.retain(|k, _| self.perf.contains_key(k));
        self.eval_seen.retain(|k, _| self.perf.contains_key(k));

        self.eval_count += 1;
        let now = Instant::now();
        // 用实际评估间隔计算吞吐（评估被延迟时不高估瞬时速率）
        let elapsed = now.duration_since(self.last_eval).as_secs_f64().max(0.1);
        self.last_eval = now;

        // 计算各连接增量吞吐与整体均值（EWMA 写回快照持久化）
        let mut total_throughput = 0.0f64;
        // (conn_id, ewma, delta_bytes, 累计采样次数)
        let mut conn_throughputs: Vec<(usize, f64, u64, u32)> = Vec::new();

        for (&conn_id, perf) in self.perf.iter_mut() {
            let prev_bytes = self
                .last_bytes
                .get(&conn_id)
                .copied()
                .unwrap_or(perf.bytes_downloaded);
            let delta = perf.bytes_downloaded.saturating_sub(prev_bytes);
            let inst_tp = delta as f64 / elapsed;
            let seen = {
                let slot = self.eval_seen.entry(conn_id).or_insert(0);
                *slot += 1;
                *slot
            };
            // 首个有效样本无历史可平滑，直接取窗口值——
            // 避免从 0 缓慢衰减导致新连接被误判为慢连接。
            let ewma = if perf.throughput_ewma <= 0.0 {
                inst_tp
            } else {
                self.config.ewma_alpha * inst_tp
                    + (1.0 - self.config.ewma_alpha) * perf.throughput_ewma
            };
            perf.throughput_ewma = ewma;
            conn_throughputs.push((conn_id, ewma, delta, seen));
            total_throughput += ewma;
        }

        // 更新 last_bytes
        for (conn_id, _, _, _) in &conn_throughputs {
            if let Some(perf) = self.perf.get(conn_id) {
                self.last_bytes.insert(*conn_id, perf.bytes_downloaded);
            }
        }

        let n_active = conn_throughputs.len().max(1);
        let avg_throughput = total_throughput / n_active as f64;

        // 更新历史最优
        if total_throughput > self.best_throughput {
            self.best_throughput = total_throughput;
            self.best_conn_count = n_active;
        }

        // 整体吞吐趋势：如果增加连接后吞吐没有增长甚至下降，可能已经过载
        let throughput_trend = total_throughput - self.last_total_throughput;
        self.last_total_throughput = total_throughput;

        let mut actions = Vec::new();

        // 先收集所有需要的数据（避免借用冲突）
        // (conn_id, ewma, delta, seen, stall_count, assigned_range)
        let perf_snapshots: Vec<(usize, f64, u64, u32, u32, u64)> = conn_throughputs
            .iter()
            .filter_map(|(conn_id, throughput, delta, seen)| {
                let perf = self.perf.get(conn_id)?;
                // 冷启动窗口（<2 采样）不计停滞：首次评估增量为结构性 0
                let stall_count = if *seen < 2 {
                    0
                } else if *delta == 0 {
                    perf.stall_count + 1
                } else {
                    0
                };
                Some((
                    *conn_id,
                    *throughput,
                    *delta,
                    *seen,
                    stall_count,
                    perf.assigned_range,
                ))
            })
            .collect();

        // 更新 stall_count
        for &(conn_id, _, _, _, stall_count, _) in &perf_snapshots {
            if let Some(p) = self.perf.get_mut(&conn_id) {
                p.stall_count = stall_count;
            }
        }

        for &(conn_id, throughput, _delta, seen, stall_count, assigned_range) in &perf_snapshots
        {
            if stall_count >= self.config.stall_eval_limit && n_active > 1 {
                // 停滞连接 → 退役
                actions.push(ScheduleAction::Retire);
                tracing::debug!(conn_id, stall_count, "自适应调度：退役停滞连接");
                continue;
            }

            // 冷启动保护：采样不足 2 个窗口前不分类
            if seen < 2 {
                actions.push(ScheduleAction::Maintain);
                continue;
            }

            // 慢连接判定
            if throughput < avg_throughput * self.config.slow_threshold_ratio {
                // 慢连接：收缩部分 Range（携带段 ID 定向收缩）
                let reclaim = (assigned_range as f64 * self.config.rebalance_step_ratio) as u64;
                actions.push(ScheduleAction::Shrink {
                    conn_id,
                    reclaim_bytes: reclaim,
                });
                tracing::debug!(
                    conn_id,
                    throughput = throughput as u64,
                    avg = avg_throughput as u64,
                    "自适应调度：收缩慢连接 Range"
                );
            } else if throughput > avg_throughput * self.config.fast_threshold_ratio {
                // 快连接：在 reclaim 池中分配
                let extra = (assigned_range as f64 * self.config.rebalance_step_ratio) as u64;
                actions.push(ScheduleAction::Grow { extra_bytes: extra });
                tracing::debug!(
                    conn_id,
                    throughput = throughput as u64,
                    avg = avg_throughput as u64,
                    "自适应调度：增长快连接 Range"
                );
            } else {
                actions.push(ScheduleAction::Maintain);
            }
        }

        // 连接数决策：如果整体吞吐在增长且未达上限 → 新增连接
        // 如果吞吐在下降且连接数 > 最优 → 不新增（让自然退役减员）
        if n_active < self.config.max_connections
            && throughput_trend > 0.0
            && total_throughput > 0.0
        {
            // 只有在当前连接数仍低于历史最优、且吞吐在增长时才扩张
            if n_active <= self.best_conn_count || self.best_conn_count == 0 {
                actions.push(ScheduleAction::Spawn);
                tracing::debug!(
                    active = n_active,
                    best = self.best_conn_count,
                    "自适应调度：尝试新增连接"
                );
            }
        }

        tracing::debug!(
            eval = self.eval_count,
            active = n_active,
            total_throughput = total_throughput as u64,
            best = self.best_conn_count,
            best_throughput = self.best_throughput as u64,
            "自适应调度评估完成"
        );

        actions
    }

    /// 调度评估周期。
    pub fn eval_interval(&self) -> Duration {
        self.config.eval_interval
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast_config() -> AdaptiveConfig {
        AdaptiveConfig {
            eval_interval: Duration::from_millis(100),
            ..Default::default()
        }
    }

    fn report(conn_id: usize, bytes: u64, assigned_range: u64) -> ConnPerf {
        ConnPerf {
            conn_id,
            bytes_downloaded: bytes,
            assigned_range,
            last_report: Instant::now(),
            ..Default::default()
        }
    }

    fn shrink_targets(actions: &[ScheduleAction]) -> Vec<usize> {
        actions
            .iter()
            .filter_map(|a| match a {
                ScheduleAction::Shrink { conn_id, .. } => Some(*conn_id),
                _ => None,
            })
            .collect()
    }

    /// 冷启动保护：新连接首次评估增量结构性为 0，
    /// 不得被误判慢速/停滞而收缩。
    #[test]
    fn cold_start_not_misjudged_slow() {
        let (mut s, tx) = AdaptiveScheduler::new(fast_config());
        // 已建立连接 A（快）
        tx.send(report(0, 1_000_000, 10_000_000)).unwrap();
        s.evaluate(); // A 预热窗口
        std::thread::sleep(Duration::from_millis(110));
        // A 第二窗口 + 新连接 B 首次上报（速度与 A 相当）
        tx.send(report(0, 2_000_000, 10_000_000)).unwrap();
        tx.send(report(1, 500_000, 10_000_000)).unwrap();
        let actions = s.evaluate();
        assert!(
            !shrink_targets(&actions).contains(&1),
            "新连接在预热窗口被误判收缩: {actions:?}"
        );
        // 第三窗口：B 已有完整采样，等速连接不应被收缩
        std::thread::sleep(Duration::from_millis(110));
        tx.send(report(0, 3_000_000, 10_000_000)).unwrap();
        tx.send(report(1, 1_500_000, 10_000_000)).unwrap();
        let actions = s.evaluate();
        assert!(
            shrink_targets(&actions).is_empty(),
            "等速连接不应被收缩: {actions:?}"
        );
    }

    /// 慢连接判定：Shrink 携带真实慢连接的段 ID（定向收缩）。
    #[test]
    fn slow_connection_shrink_carries_conn_id() {
        let (mut s, tx) = AdaptiveScheduler::new(fast_config());
        tx.send(report(0, 2_000_000, 10_000_000)).unwrap(); // A 快
        tx.send(report(1, 200_000, 10_000_000)).unwrap(); // B 慢
        s.evaluate(); // 双方预热
        std::thread::sleep(Duration::from_millis(110));
        tx.send(report(0, 4_000_000, 10_000_000)).unwrap(); // A +2MB
        tx.send(report(1, 250_000, 10_000_000)).unwrap(); // B +50KB
        let actions = s.evaluate();
        assert_eq!(
            shrink_targets(&actions),
            vec![1],
            "应只收缩慢连接: {actions:?}"
        );
    }

    /// 停滞判定同样受冷启动保护：新连接首窗口不计停滞，
    /// 连续无进度达到阈值才退役。
    #[test]
    fn stall_detection_respects_warmup() {
        let mut cfg = fast_config();
        cfg.stall_eval_limit = 2;
        let (mut s, tx) = AdaptiveScheduler::new(cfg);
        tx.send(report(0, 1_000_000, 10_000_000)).unwrap(); // A 正常
        tx.send(report(1, 500_000, 10_000_000)).unwrap(); // B 将停滞
        let actions = s.evaluate(); // 预热：不计停滞
        assert!(
            !actions.iter().any(|a| matches!(a, ScheduleAction::Retire)),
            "预热窗口不应退役: {actions:?}"
        );
        let mut a_bytes = 1_000_000u64;
        let mut saw_retire = false;
        for _ in 0..2 {
            std::thread::sleep(Duration::from_millis(110));
            a_bytes += 1_000_000;
            tx.send(report(0, a_bytes, 10_000_000)).unwrap(); // A 前进
            tx.send(report(1, 500_000, 10_000_000)).unwrap(); // B 无进度
            let actions = s.evaluate(); // 停滞计数逐窗口累积
            saw_retire |= actions.iter().any(|a| matches!(a, ScheduleAction::Retire));
        }
        assert!(saw_retire, "连续停滞达到阈值应退役");
    }
}
