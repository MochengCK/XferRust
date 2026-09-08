//! 全局下载限速器（bytes/s，0 = 不限）：跨任务共享的异步令牌桶。
//!
//! aria2 `max-overall-download-limit` 语义 = 所有下载任务合计限速，
//! 因此由引擎管理器持有一个实例，注入到每条 HTTP 下载连接；
//! 连接在读到数据块后、入队写线程前消费令牌，令牌不足时异步等待，
//! TCP 背压自然收敛（socket 停止读取 → 发送端减速）。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// 桶容量下限：低限速值下仍允许小突发，避免 64KB 以下的块被长时间卡住。
const MIN_BURST: f64 = 64.0 * 1024.0;
/// 单次等待上限：限速值运行时被调大/清零时最多 50ms 内重新评估。
const MAX_SLEEP: Duration = Duration::from_millis(50);

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

/// 共享令牌桶限速器。克隆语义经 `Arc<RateLimiter>` 共享。
#[derive(Debug)]
pub struct RateLimiter {
    rate: AtomicU64,
    state: Mutex<Bucket>,
}

impl RateLimiter {
    pub fn new(rate: u64) -> Arc<Self> {
        Arc::new(Self {
            rate: AtomicU64::new(rate),
            state: Mutex::new(Bucket {
                tokens: MIN_BURST,
                last: Instant::now(),
            }),
        })
    }

    /// 运行时调整限速值（bytes/s，0 = 不限）。热路径无锁读。
    pub fn set_rate(&self, rate: u64) {
        self.rate.store(rate, Ordering::Relaxed);
    }

    pub fn rate(&self) -> u64 {
        self.rate.load(Ordering::Relaxed)
    }

    /// 消费 `n` 个令牌；不足时等待回补后重试。`rate = 0`（不限速）立即返回。
    pub async fn acquire(&self, n: usize) {
        if n == 0 {
            return;
        }
        loop {
            let wait = {
                let mut b = self.state.lock().unwrap();
                let rate = self.rate.load(Ordering::Relaxed) as f64;
                if rate <= 0.0 {
                    return;
                }
                let now = Instant::now();
                let elapsed = now.duration_since(b.last).as_secs_f64();
                b.last = now;
                // 回补令牌；桶容量 = max(rate, MIN_BURST)，限速调整后自然收敛
                b.tokens = (b.tokens + elapsed * rate).min(rate.max(MIN_BURST));
                if b.tokens >= n as f64 {
                    b.tokens -= n as f64;
                    return;
                }
                let deficit = n as f64 - b.tokens;
                Duration::from_secs_f64((deficit / rate).clamp(0.001, 1.0))
            };
            tokio::time::sleep(wait.min(MAX_SLEEP)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unlimited_returns_immediately() {
        let limiter = RateLimiter::new(0);
        let start = Instant::now();
        limiter.acquire(1024 * 1024).await;
        assert!(start.elapsed().as_millis() < 50);
    }

    #[tokio::test]
    async fn limited_paces_throughput() {
        // 1 MB/s：连续取 2 MB 至少耗时 ~0.5s（扣除突发余量）
        let limiter = RateLimiter::new(1024 * 1024);
        let start = Instant::now();
        for _ in 0..8 {
            limiter.acquire(256 * 1024).await;
        }
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(400), "elapsed={elapsed:?}");
        assert!(elapsed < Duration::from_secs(3), "elapsed={elapsed:?}");
    }

    #[tokio::test]
    async fn rate_change_takes_effect() {
        let limiter = RateLimiter::new(0);
        limiter.set_rate(0);
        limiter.acquire(1).await;
        limiter.set_rate(512 * 1024);
        // 512KB/s 下取 512KB（超出 64KB 突发余量）必须等待，不会立即返回；
        // 预期 ~0.9s（(512-64)KB / 512KB/s），断言上限防回归为不限速
        let start = Instant::now();
        limiter.acquire(512 * 1024).await;
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(100), "elapsed={elapsed:?}");
        assert!(elapsed < Duration::from_secs(5), "elapsed={elapsed:?}");
    }
}
