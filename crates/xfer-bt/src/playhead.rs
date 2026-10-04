//! 播放头优先选片：**边下边播**的排片档位（纯位置计算，单测钉住）。
//!
//! 普通 BT 客户端的默认策略是 rarest-first（最稀有的片先下）——它优化的是
//! "尽快让整个种子在 swarm 里存活"，完全不关心本地有没有人在看。边下边播要的是
//! **播放头附近立刻有连续数据**：容器头、索引、紧接着的几十秒码流。
//! 两者冲突时以前者为准，表现就是"刚打开播放器就转圈、播两秒停一秒"。
//!
//! 档位与窗口大小**直接沿用 MediaEngine 的 `me-bt` picker**（那边有完整论证，
//! 见 `crates/me-bt/src/picker.rs`）：窗口内按片号升序而不是稀有度 —— 稀有度排序
//! 会把窗口里的片打散，"连续前缀"迟迟不成立，而连续前缀正是容器解析能不能
//! 进行下去的前提。
//!
//! | 档位 | 区间（相对播放头 `P`） | 档内排序 |
//! |---|---|---|
//! | [`Tier::Stream`] | `[P, P + 32 MiB)` | 片号升序 |
//! | [`Tier::Next`] | `[P + 32 MiB, P + 128 MiB)` | 片号升序 |
//! | [`Tier::Soon`] | `[P - 8 MiB, P)` | 片号升序 |
//! | [`Tier::Backfill`] | 其余 | **rarest-first**（保持旧行为） |
//!
//! 本模块**不碰网络、不看 have 位图**：它只回答"这些候选片按什么顺序下"。
//! 谁去下、下多久是 `engine.rs` 的事。

/// 播放头往前的"必须马上有"区间。
///
/// 32 MiB 的依据：seek 之后容器解析器要重读一段索引（MP4 的 moov/碎片、
/// TS 的 PMT），加上播放器自身的解码缓冲，量级在几十 MB；再大就变成
/// "为了保险而浪费带宽"。
pub const STREAM_WINDOW_BYTES: u64 = 32 * 1024 * 1024;

/// 再往前一档（提前铺路，抗一次带宽抖动）。
pub const NEXT_WINDOW_BYTES: u64 = 128 * 1024 * 1024;

/// 播放头往回的一档，供 seek 回退。
///
/// 比往前小得多是**故意**的：回退是用户主动跳回去，他看得到一次可接受的
/// 缓冲；把回退缓冲做大等于常年浪费带宽。
pub const BACK_WINDOW_BYTES: u64 = 8 * 1024 * 1024;

/// 片的优先级档位（数值即排序优先级，越小越急）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Stream = 0,
    Next = 1,
    Soon = 2,
    Backfill = 3,
}

/// 一片落在哪一档。`piece_start`/`piece_end` 是该片的字节区间（半开），
/// `playhead` 是播放头字节偏移。**纯位置函数**：不看 have、不看在飞。
pub fn tier_of(piece_start: u64, piece_end: u64, playhead: u64) -> Tier {
    // 顺序有讲究：Stream → Next → Soon。一片同时落在两个窗口里时取更急的那档
    // （seek 回退后，回退缓冲与前进窗口可能重叠）。
    if overlaps(piece_start, piece_end, playhead, playhead.saturating_add(STREAM_WINDOW_BYTES)) {
        return Tier::Stream;
    }
    let stream_end = playhead.saturating_add(STREAM_WINDOW_BYTES);
    let next_end = playhead.saturating_add(NEXT_WINDOW_BYTES);
    if overlaps(piece_start, piece_end, stream_end, next_end) {
        return Tier::Next;
    }
    let back_start = playhead.saturating_sub(BACK_WINDOW_BYTES);
    if overlaps(piece_start, piece_end, back_start, playhead) {
        return Tier::Soon;
    }
    Tier::Backfill
}

/// 两个半开区间是否相交（空区间不相交）。
fn overlaps(a_start: u64, a_end: u64, b_start: u64, b_end: u64) -> bool {
    a_start < b_end && b_start < a_end
}

/// 把候选片排成"该按什么顺序下"。
///
/// `candidates` 是 `(片号, 稀有度)`：稀有度 = 当前连接中拥有该片的人数
/// （越小越稀有）。`piece_range` 给出片号对应的字节区间。
///
/// - 播放头未知（`None`，即没人告诉过引擎）→ 与旧行为**逐位一致**的
///   rarest-first（先稀有度、再片号）；
/// - 有播放头 → 先按档位，档内见模块注释（窗口内片号升序、其余仍 rarest-first）。
pub fn order(
    candidates: &mut [(u32, u32)],
    playhead: Option<u64>,
    piece_range: impl Fn(u32) -> (u64, u64),
) {
    let Some(ph) = playhead else {
        // 没有播放头时**逐位复刻**改造前的 rarest-first（先稀有度、再片号）
        candidates.sort_unstable_by_key(|(idx, rarity)| (*rarity, *idx));
        return;
    };
    candidates.sort_unstable_by_key(|(idx, rarity)| {
        let (start, end) = piece_range(*idx);
        match tier_of(start, end, ph) {
            Tier::Backfill => (Tier::Backfill as u8, *rarity, *idx),
            tier => (tier as u8, *idx, *rarity),
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    /// 1 MiB 一片的布局：片 i → [i MiB, (i+1) MiB)。
    fn range_1mib(idx: u32) -> (u64, u64) {
        (idx as u64 * MIB, (idx as u64 + 1) * MIB)
    }

    fn ordered(cands: &[(u32, u32)], playhead: Option<u64>) -> Vec<u32> {
        let mut v = cands.to_vec();
        order(&mut v, playhead, range_1mib);
        v.into_iter().map(|(idx, _)| idx).collect()
    }

    #[test]
    fn no_playhead_keeps_rarest_first() {
        // 稀有度优先、同稀有度按片号 —— 与改造前的 sort_unstable() 行为一致
        let cands = [(5u32, 1u32), (1, 3), (9, 1), (2, 3)];
        assert_eq!(ordered(&cands, None), vec![5, 9, 1, 2]);
    }

    #[test]
    fn stream_window_is_index_ascending_not_rarest_first() {
        // 播放头在 0：片 0..4 都在 Stream 窗口里。稀有度故意反过来，
        // 顺序仍必须是 0,1,2,3（连续前缀），稀有度只在窗口外说话。
        let cands = [(3u32, 1u32), (0, 99), (2, 1), (1, 97), (40, 1)];
        assert_eq!(ordered(&cands, Some(0)), vec![0, 1, 2, 3, 40]);
    }

    #[test]
    fn windows_are_ordered_stream_next_soon_backfill() {
        // 播放头在 64 MiB、1 MiB 片：
        //   Stream = [64, 96) → 片 64..96
        //   Next   = [96, 192) → 片 96..192
        //   Soon   = [56, 64)  → 片 56..64
        //   Backfill = 其余
        let ph = 64 * MIB;
        let cands = [
            (100u32, 1u32),  // Next
            (10, 1),         // Backfill
            (60, 9),         // Soon
            (80, 9),         // Stream
            (200, 1),        // Backfill
            (56, 9),         // Soon
            (64, 500),       // Stream（稀有度极高也要排在 Next/Soon 前面）
        ];
        assert_eq!(ordered(&cands, Some(ph)), vec![64, 80, 100, 56, 60, 10, 200]);
    }

    #[test]
    fn piece_straddling_a_window_boundary_takes_the_urgent_tier() {
        // 4 MiB 片：片 8 = [32, 36) MiB。播放头在 33 MiB 时它跨过 P，
        // 但整片与 Stream 窗口 [33, 65) 相交 → 必须是 Stream（不能算 Soon/Backfill）。
        let ph = 33 * MIB;
        let r = |idx: u32| (idx as u64 * 4 * MIB, (idx as u64 + 1) * 4 * MIB);
        assert_eq!(tier_of(r(8).0, r(8).1, ph), Tier::Stream);
        // 片 7 = [28, 32) MiB：整片在 P 之前，且与回退窗口 [25, 33) 相交 → Soon
        assert_eq!(tier_of(r(7).0, r(7).1, ph), Tier::Soon);
        // 片 40 = [160, 164) MiB：与 Next 窗口 [65, 161) 相交 → Next
        assert_eq!(tier_of(r(40).0, r(40).1, ph), Tier::Next);
        // 片 41 = [164, 168) MiB：窗口外 → Backfill
        assert_eq!(tier_of(r(41).0, r(41).1, ph), Tier::Backfill);
    }

    #[test]
    fn playhead_at_file_end_leaves_everything_backfill() {
        // 播放头在 1 GiB（远超所有候选片）：谁都不在窗口里 → 纯 rarest-first
        let cands = [(3u32, 5u32), (1, 2), (2, 7)];
        assert_eq!(ordered(&cands, Some(1024 * MIB)), vec![1, 3, 2]);
    }

    #[test]
    fn zero_length_piece_never_matches_a_window() {
        // 空区间（防御）：不该被任何窗口"擦边"命中
        assert_eq!(tier_of(0, 0, 0), Tier::Backfill);
        assert_eq!(tier_of(1024, 1024, 1024), Tier::Backfill);
    }
}
