//! BT 自适应 peer 调度（智能调度）。
//!
//! 与 HTTP 的 [`xfer_http::AdaptiveScheduler`] 同一思路，但调度对象是**对等连接**
//! 而非 Range 分片：以「预分配连接数」（`bt-max-peers`）为上限，按吞吐的
//! **边际收益**动态调整目标连接数——
//!
//! - 增加连接还能换来吞吐增长 → 继续扩张；
//! - 吞吐停滞 → 停止扩张（避免无收益的连接开销），并淘汰慢 peer 换血；
//! - 任何时刻不超过用户设置的预分配上限。
//!
//! 本模块是**纯逻辑**（不触碰网络/存储），便于独立单元测试。

use std::net::SocketAddr;
use std::time::Duration;

/// 调度配置。
#[derive(Debug, Clone)]
pub struct PeerSchedulerConfig {
    /// 预分配连接数：调度上限（来自 `bt-max-peers`）。
    pub max_peers: usize,
    /// 保底连接数。
    pub min_peers: usize,
    /// 单次扩张步长。
    pub expand_step: usize,
    /// 判定「有增益」的吞吐相对增长比例。
    pub gain_ratio: f64,
    /// 连续停滞多少轮后触发换血。
    pub stagnant_rounds: u32,
    /// 慢 peer 判定：低于中位速率的该比例。
    pub slow_ratio: f64,
    /// 慢 peer 速率绝对下限（bytes/s）：整体很慢时不至于把连接砍光。
    pub slow_floor: u64,
    /// 新连接宽限期：此时间内不参与淘汰。
    pub grace_period: Duration,
    /// 单轮淘汰上限比例（避免一次砍掉太多连接）。
    pub evict_ratio: f64,
}

impl Default for PeerSchedulerConfig {
    fn default() -> Self {
        Self {
            max_peers: 50,
            min_peers: 2,
            expand_step: 4,
            gain_ratio: 0.10,
            stagnant_rounds: 2,
            slow_ratio: 0.25,
            slow_floor: 1024,
            grace_period: Duration::from_secs(15),
            evict_ratio: 0.2,
        }
    }
}

/// 单个 peer 的一轮采样快照。
#[derive(Debug, Clone, Copy)]
pub struct PeerSample {
    pub addr: SocketAddr,
    /// 近期速率（bytes/s）。
    pub speed: u64,
    /// 已连接时长。
    pub connected_for: Duration,
}

/// 调度决策。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleAction {
    /// 维持现状。
    Hold,
    /// 扩张：再建立 n 条连接。
    Expand(usize),
    /// 换血：断开这些慢 peer，腾出槽位给候选 peer。
    Replace(Vec<SocketAddr>),
}

/// BT 自适应 peer 调度器。
#[derive(Debug)]
pub struct PeerScheduler {
    cfg: PeerSchedulerConfig,
    /// 当前目标连接数（<= cfg.max_peers）。
    target: usize,
    /// 上一轮总吞吐（bytes/s）。
    last_throughput: u64,
    /// 连续停滞轮次。
    stagnant: u32,
    /// 是否已评估过（首轮直接拉满目标，快速起速）。
    started: bool,
}

impl PeerScheduler {
    pub fn new(mut cfg: PeerSchedulerConfig) -> Self {
        // min > max 属配置错误：钳制保底，保证目标永不超预分配上限
        cfg.min_peers = cfg.min_peers.min(cfg.max_peers);
        let target = cfg.max_peers;
        Self {
            cfg,
            target,
            last_throughput: 0,
            stagnant: 0,
            started: false,
        }
    }

    /// 当前目标连接数。
    pub fn target(&self) -> usize {
        self.target
    }

    /// 每轮评估：依据吞吐边际收益调整目标，并给出本轮动作。
    ///
    /// `peers` 为当前在线 peer 采样，`pending` 为待连接候选数。
    pub fn evaluate(&mut self, peers: &[PeerSample], pending: usize) -> ScheduleAction {
        let active = peers.len();
        let throughput: u64 = peers.iter().map(|p| p.speed).sum();

        if !self.started {
            // 冷启动：先按预分配上限起速，后续按收益收敛
            self.started = true;
            self.target = self.cfg.max_peers;
            self.last_throughput = throughput;
            return self.expand(active, pending);
        }

        // 吞吐边际收益：相对上一轮的增长率
        let base = self.last_throughput.max(1);
        let gain = throughput as f64 / base as f64 - 1.0;
        self.last_throughput = throughput;

        if gain > self.cfg.gain_ratio {
            // 加连接确实带来了吞吐增长 → 继续扩张（不超过上限）
            self.stagnant = 0;
            self.target = (self.target + self.cfg.expand_step).min(self.cfg.max_peers);
        } else if gain >= 0.0 {
            // 持平：不再抬高目标，仅按现有目标补位
            self.stagnant += 1;
        } else {
            // 吞吐下滑：乘性缩减目标（AIMD 的 MD 半），下限保底；
            // 目标不降的话会一直满上限拨号，换血→宽限→再停滞无限震荡
            self.stagnant = self.stagnant.saturating_add(2);
            self.target = ((self.target * 3) / 4).max(self.cfg.min_peers);
        }

        // 未达目标且有候选 → 继续补连接
        if let Some(action) = self.try_expand(active, pending) {
            return action;
        }

        // 已达目标但吞吐停滞 → 淘汰慢 peer 换血
        if self.stagnant >= self.cfg.stagnant_rounds && pending > 0 {
            if let Some(action) = self.try_replace(peers) {
                self.stagnant = 0;
                return action;
            }
        }

        ScheduleAction::Hold
    }

    fn try_expand(&self, active: usize, pending: usize) -> Option<ScheduleAction> {
        if active >= self.target || pending == 0 {
            return None;
        }
        Some(ScheduleAction::Expand((self.target - active).min(pending)))
    }

    fn expand(&self, active: usize, pending: usize) -> ScheduleAction {
        self.try_expand(active, pending)
            .unwrap_or(ScheduleAction::Hold)
    }

    /// 挑出本轮要淘汰的慢 peer：速率低于「中位速率 × slow_ratio」且已过宽限期。
    fn try_replace(&self, peers: &[PeerSample]) -> Option<ScheduleAction> {
        if peers.len() <= self.cfg.min_peers {
            return None;
        }
        let mut speeds: Vec<u64> = peers.iter().map(|p| p.speed).collect();
        speeds.sort_unstable();
        let median = speeds[speeds.len() / 2];
        let threshold = ((median as f64) * self.cfg.slow_ratio) as u64;
        let threshold = threshold.max(self.cfg.slow_floor);

        // 过了宽限期且速率低于阈值的，按速率升序取前 N 个
        let mut slow: Vec<&PeerSample> = peers
            .iter()
            .filter(|p| p.connected_for >= self.cfg.grace_period && p.speed < threshold)
            .collect();
        if slow.is_empty() {
            return None;
        }
        slow.sort_by_key(|p| p.speed);
        let max_evict = ((peers.len() as f64) * self.cfg.evict_ratio).ceil() as usize;
        let max_evict = max_evict.max(1).min(peers.len() - self.cfg.min_peers);
        let victims: Vec<SocketAddr> = slow.iter().take(max_evict).map(|p| p.addr).collect();
        if victims.is_empty() {
            None
        } else {
            Some(ScheduleAction::Replace(victims))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn sample(port: u16, speed: u64, secs: u64) -> PeerSample {
        PeerSample {
            addr: addr(port),
            speed,
            connected_for: Duration::from_secs(secs),
        }
    }

    fn cfg(max_peers: usize) -> PeerSchedulerConfig {
        PeerSchedulerConfig {
            max_peers,
            ..Default::default()
        }
    }

    #[test]
    fn cold_start_expands_up_to_target() {
        let mut s = PeerScheduler::new(cfg(20));
        let peers: Vec<PeerSample> = vec![];
        // 首轮：目标拉满 20，候选 5 个 → 扩张 5
        assert_eq!(s.evaluate(&peers, 5), ScheduleAction::Expand(5));
        assert_eq!(s.target(), 20);
    }

    #[test]
    fn never_exceeds_max_peers() {
        let mut s = PeerScheduler::new(cfg(8));
        let peers: Vec<PeerSample> = (0..8).map(|i| sample(1000 + i, 10_000, 30)).collect();
        // 已达上限，即使吞吐大涨也只维持（或换血），不会超过 8
        let action = s.evaluate(&peers, 50);
        assert!(!matches!(action, ScheduleAction::Expand(_)));
        assert!(s.target() <= 8);
        // 吞吐大幅增长后目标仍钳制在上限
        let peers2: Vec<PeerSample> = (0..8).map(|i| sample(1000 + i, 200_000, 30)).collect();
        let _ = s.evaluate(&peers2, 50);
        assert!(s.target() <= 8);
    }

    #[test]
    fn expands_while_throughput_grows() {
        let mut s = PeerScheduler::new(cfg(50));
        // 首轮：低吞吐、只有 2 个连接
        let peers: Vec<PeerSample> = vec![sample(1, 5_000, 30), sample(2, 5_000, 30)];
        let _ = s.evaluate(&peers, 20);
        // 第二轮：吞吐明显增长 → 扩张目标上调，且本轮继续补连接
        let peers2: Vec<PeerSample> = (0..6).map(|i| sample(10 + i, 20_000, 30)).collect();
        let action = s.evaluate(&peers2, 20);
        assert!(matches!(action, ScheduleAction::Expand(_)));
    }

    #[test]
    fn stops_expanding_and_replaces_when_stagnant() {
        let mut s = PeerScheduler::new(cfg(6));
        // 稳定在 6 个连接、吞吐几乎不变（停滞），其中有慢 peer
        let mut peers: Vec<PeerSample> = (0..5).map(|i| sample(20 + i, 100_000, 60)).collect();
        peers.push(sample(25, 100, 60)); // 明显慢的节点
        let mut replaced = false;
        for _ in 0..5 {
            if let ScheduleAction::Replace(v) = s.evaluate(&peers, 10) {
                assert!(
                    v.contains(&addr(25)),
                    "应淘汰慢节点 127.0.0.1:25，实际 {v:?}"
                );
                replaced = true;
                break;
            }
        }
        assert!(replaced, "停滞若干轮后应触发换血");
    }

    #[test]
    fn respects_grace_period_for_new_peers() {
        let mut s = PeerScheduler::new(cfg(6));
        // 全部是新连接（未过 15s 宽限期），即使很慢也不应立刻淘汰
        let peers: Vec<PeerSample> = (0..6).map(|i| sample(30 + i, 0, 2)).collect();
        for _ in 0..5 {
            let action = s.evaluate(&peers, 10);
            assert!(
                !matches!(action, ScheduleAction::Replace(_)),
                "宽限期内不应淘汰：{action:?}"
            );
        }
    }

    #[test]
    fn keeps_min_peers() {
        let mut s = PeerScheduler::new(PeerSchedulerConfig {
            max_peers: 4,
            min_peers: 3,
            ..Default::default()
        });
        let peers: Vec<PeerSample> =
            vec![sample(40, 10, 60), sample(41, 20, 60), sample(42, 30, 60)];
        for _ in 0..5 {
            let action = s.evaluate(&peers, 10);
            assert!(
                !matches!(action, ScheduleAction::Replace(_)),
                "不应把连接砍到低于 min_peers：{action:?}"
            );
        }
    }

    #[test]
    fn target_shrinks_on_throughput_drop() {
        let mut s = PeerScheduler::new(cfg(20));
        let peers: Vec<PeerSample> = (0..10).map(|i| sample(100 + i, 100_000, 60)).collect();
        let _ = s.evaluate(&peers, 0);
        assert_eq!(s.target(), 20, "冷启动目标应为上限");
        // 吞吐骤降 >10% → 乘性缩减（20 × 3/4 = 15）
        let poor: Vec<PeerSample> = (0..10).map(|i| sample(100 + i, 1_000, 60)).collect();
        let _ = s.evaluate(&poor, 0);
        assert_eq!(s.target(), 15, "吞吐下滑应乘性缩减目标");
        // 连续下滑（每轮吞吐减半）也不得低于 min_peers（默认 2）
        let mut speed = 500u64;
        for _ in 0..20 {
            let declining: Vec<PeerSample> =
                (0..10).map(|i| sample(100 + i, speed, 60)).collect();
            let _ = s.evaluate(&declining, 0);
            speed /= 2;
        }
        assert_eq!(s.target(), 2, "目标应停在保底连接数");
    }

    #[test]
    fn clamps_min_peers_above_max() {
        let s = PeerScheduler::new(PeerSchedulerConfig {
            max_peers: 5,
            min_peers: 50,
            ..Default::default()
        });
        assert!(s.target() <= 5, "min > max 时目标仍不得超过上限");
    }
}
