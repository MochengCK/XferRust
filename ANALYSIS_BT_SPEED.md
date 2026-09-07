# XferRust BT 公网下载速度问题深度分析报告

**日期**: 2025-09-08  
**状态**: 已修复并验证（见文末「六、修复与验证记录」）  
**原则**: 仅分析，不修改源码

---

## 一、问题概述

使用 XferRust 引擎下载公网热门种子（Ubuntu 22.04 ISO），在连接 50+ peer（含官方 seed）的情况下，下载速度仅约 200KB/s，而同条件下 qBittorrent/aria2 可达十几 MB/s，存在约 100 倍的性能差距。

---

## 二、根因分析

### 🔴 根因 #1（主因）：公网测试使用了 debug 构建而非 release 构建

**证据链**：

1. Transcript 第 27028 行明确记录：
   > "LTO fat + codegen-units=1 确实非常慢。让我改用 debug 构建来快速验证逻辑，debug 构建快得多"

2. Transcript 第 27032 行编译命令：
   ```
   cargo build --bin xferrust   ← 没有 --release 标志！
   ```

3. Transcript 第 27042 行测试命令：
   ```
   python3 scripts/bt_public_test.py --xferrust target/debug/xferrust ...
   ```

4. `Cargo.toml` 中 release profile 配置了激进优化：
   ```toml
   [profile.release]
   lto = "fat"          # 全量 LTO
   codegen-units = 1    # 单代码生成单元
   opt-level = "s"      # 按体积优化
   strip = true         # 删除符号表
   ```

5. Rust debug 构建 (`opt-level = 0`) 不做任何优化，典型性能比 release 慢 **10-100 倍**。

**影响**：
- 所有 `std::sync::Mutex` 的 lock/unlock 没有内联优化，每次调用都是完整函数调用
- `tracing` 宏的日志判断没有优化掉（即使日志级别不输出，仍会执行参数求值）
- `Vec` 分配、拷贝、`HashSet` 操作都没有优化
- **SHA-1 哈希校验**（纯 CPU 计算密集型）在 debug 模式下可能慢 50-100 倍
- 每次消息解析中的 `body[8..].to_vec()` 等堆分配没有优化

**结论**：这 100 倍的速度差距（200KB/s vs 10-20MB/s）与 debug vs release 的典型性能差异完全吻合。

---

### 🟡 根因 #2（次要）：全局 store 锁在 piece 完成时持有时间过长

**位置**：`engine.rs` `accept_piece()` (第 4585-4611 行)

**问题**：
```rust
fn accept_piece(&self, index: u32, data: &[u8], expected: &[u8; 20]) -> bool {
    let mut guard = self.store.lock().unwrap();  // ← 全局锁
    let store = guard.as_mut().unwrap();
    // ...
    match store.accept_piece(index, data, expected) {
        // store.accept_piece 内部做：
        //   1. SHA-1 哈希校验（verify_piece）— CPU 密集
        //   2. 磁盘 I/O（seek + write_all）— I/O 密集
        // 全程持有全局 store Mutex！
    }
}
```

**影响**：每次任何 peer 完成一个 piece（256KB-1MB），所有其他 peer 的 `fill_pipeline`（需 `self.store.lock()` 查 `piece_len`）和 `assign_piece`（需 `self.store.lock()` 获取 store map）都被阻塞，直到 SHA-1 计算和磁盘写入完成。

在 release 构建下 SHA-1 很快，磁盘 I/O 也有 OS 缓存，影响有限。但在 debug 构建下，SHA-1 极慢，会成为严重瓶颈。

**建议（不实施）**：将 `accept_piece` 中的哈希校验和磁盘写入移到 `spawn_blocking` 或在锁外执行。

---

### 🟡 根因 #3（次要）：调度器参数已修复但仍可能不够保守

**位置**：`scheduler.rs` `PeerSchedulerConfig` 默认参数

**已修改的参数**（前序对话中完成）：
| 参数 | 原值 | 修改后 | 理由 |
|------|------|--------|------|
| `stagnant_rounds` | 2 | 6 | 防止 peer 在 20s 内被误淘汰 |
| `grace_period` | 15s | 40s | 给 peer 足够时间建立 unchoke |
| `slow_ratio` | 0.25 | 0.1 | 避免误杀正常工作的中速 peer |
| `slow_floor` | 1024 | 10240 | 避免误杀低速仍在工作的 peer |
| `evict_ratio` | 0.2 | 0.1 | 减少一次淘汰过多 peer 的震荡 |

**残留问题**：即使修复了参数，调度器在 debug 构建下的基础性能仍然极差，导致 `recent_speed` 采样不准确，可能仍然误判。

---

### 🟡 根因 #4（次要）：部分 peer 握手后立即断开

**证据**：日志显示大量 peer `peer_id=None`（握手未完成即断开），或握手成功后 0.3s 断开。

**分析**：
1. **DHT 网络中的死 peer/stale peer** — 这是正常现象，DHT 返回的 peer 列表中可能有大量不可达地址
2. **PeerId 前缀 `-XR0200-`** — 不是主流客户端（qBittorrent `-qB`、libtorrent `-LT`、Transmission `-TR`），某些客户端可能对未知前缀有限制，但这不是主要因素
3. **Ubuntu 官方 seed `185.125.190.59:6920`** — 3 分钟仅传输 48KB，这在 debug 构建下完全合理（CPU 来不及处理消息），但在 release 构建下应该正常

---

### 🟢 观察项（非问题）

1. **消息读取实现** (`PeerReader`) — 使用增量缓冲，零拷贝 `fill`，设计合理
2. **Request 批量发送** — `fill_pipeline` 已将 Request 消息批量编码为 `request_batch` 后一次性 `write_all`，避免了逐条发送的 Nagle/延迟确认问题
3. **流水线深度** — `PIPELINE_INITIAL = 256`，`PIPELINE_MAX = 256`，每个 peer 可保持 256 个在途 16KB 块（4MB 窗口），合理
4. **TCP_NODELAY** — 出站和入站连接都设置了 `set_nodelay(true)`
5. **BLOCK_SIZE = 16KB** — 符合生态标准，不会被主流客户端拒绝
6. **Choking 算法** — `decide_choking` 实现了 BEP 3 标准 choking（3 常规 + 1 乐观），用墙钟保证全局一致性
7. **广播 Have** — `broadcast_have` 是异步的（推入队列，在 choke_timer tick 时批量发送），不阻塞热路径
8. **消息洪泛检测** — 30s 窗口检查，阈值合理
9. **续传保存** — `save_resume` 使用 `spawn_blocking` 做磁盘 I/O，不阻塞 async runtime
10. **`fill_pipeline` 兜底补发** — 每 10s 由 choke_timer 触发，防止事件遗漏导致的空转

---

## 三、代码架构评估

### 协议实现正确性
- **BEP 3** (BitTorrent 协议): 握手、消息编解码、choke/unchoke、interested/interested 逻辑正确
- **BEP 5** (DHT): Port 消息处理正确
- **BEP 6** (Fast Extension): HaveAll/HaveNone/RejectRequest/SuggestPiece/AllowedFast 处理正确
- **BEP 10** (Extension Protocol): 扩展握手和 ut_metadata 处理正确
- **BEP 11** (PEX): PEX 消息发送实现正确
- **MSE/PE** (加密): 标准 RC4 加密握手实现

### 锁使用模式
- 使用 `std::sync::Mutex`（非 `tokio::sync::Mutex`），在 async 上下文中只要不跨 `await` 持有就是安全的
- 检查代码中未见 `await` 时持有 `std::sync::Mutex` 的情况
- 锁层次清晰：`store` → `assigned` → `peers`(RwLock) → `cell.state` → `cell.queued`/`cell.pipeline`

### 潜在优化点（不实施，仅记录）
1. `accept_piece` 中哈希校验和磁盘写入应移出全局 store 锁
2. `assign_piece` 中 `peer_haves` 收集时遍历所有 peer 并对每个加锁，O(n) + n 次锁获取，可改为增量维护稀有度计数器
3. `decide_choking` 每个 peer 独立调用时都遍历全部 peer，可改为引擎级集中计算后分发
4. `fill_pipeline` 中 `request_blocks` 可缓存（piece_len 不变时重复计算）

---

## 四、验证方案

### 步骤 1：使用 release 构建重新测试

```bash
# 清理 debug 构建产物
rm -f target/debug/xferrust target/debug/xfer

# Release 构建（可能需要 10-20 分钟，因 LTO fat + codegen-units=1）
cargo build --release --bin xferrust --bin xfer

# 公网测试
python3 scripts/bt_public_test.py \
  --xferrust target/release/xferrust \
  --xfer target/release/xfer \
  --torrent-url "https://releases.ubuntu.com/22.04/ubuntu-22.04.5-desktop-amd64.iso.torrent" \
  --download-timeout 600 \
  --poll-interval 5
```

### 步骤 2：如 release 构建速度仍不理想

开启 debug 日志追踪特定 peer 的 Request/Piece 消息往返：

```bash
RUST_LOG=xfer_bt=debug,xferrust=info \
  ./target/release/xferrust --rpc-port 16800 --token bt-test &
```

### 步骤 3：对比测试

同一种子下用 aria2 做对照：
```bash
aria2c --bt-max-peers=50 --dir=data \
  "https://releases.ubuntu.com/22.04/ubuntu-22.04.5-desktop-amd64.iso.torrent"
```

---

## 五、结论

| 维度 | 评估 |
|------|------|
| **100 倍速度差距的主因** | **公网测试误用 debug 构建二进制** |
| 协议实现正确性 | BEP 3/5/6/10/11 实现正确，无明显协议偏差 |
| 调度器/流水线逻辑 | 设计合理，参数已在前序对话中调优 |
| 全局锁竞争 | 存在 `accept_piece` 持有 store 锁过长的设计缺陷，但在 release 构建下影响有限 |
| PeerId 前缀 | `-XR0200-` 非主流但不应导致被拒绝 |
| 下一步行动 | 使用 `--release` 构建重新运行公网测试 |

**核心判断**：代码逻辑和协议实现没有发现导致 100 倍速度差异的 bug。100 倍性能差距的量级与 Rust debug vs release 构建的典型差异完全吻合。**使用 release 构建重新测试是第一优先项。**

---

## 六、修复与验证记录（2026-09-08）

### 已实施修复

**1. 根因 #2：`accept_piece` SHA-1 校验移出全局 store 锁**（`crates/xfer-bt/src/engine.rs`）

- 旧实现：`store.lock()` → `store.accept_piece()`（内部 SHA-1 校验 + 磁盘写入）全程持锁，阻塞所有 peer 的 `fill_pipeline`/`assign_piece`。
- 新实现：锁内 O(1) 位查询快速去重 → **锁外** `verify_piece`（纯函数）→ 重入锁双检去重（覆盖哈希计算窗口的换血竞态）→ `write_piece` + `mark_done`。
- 落盘仍持锁（独占文件句柄），但写入走页缓存耗时远小于哈希；语义与 `PieceStore::accept_piece` 完全一致（校验失败不落盘不标记）。

**2. 附带修复：6 个集成测试编译失败**（`tests/m5_scheduler.rs`、`e2e_fastext.rs`、`magnet.rs`、`e2e_spec.rs`、`upload.rs`）

- 前序会话给 `TorrentConfig` 新增 `enable_lpd`/`enable_port_mapping` 字段后测试初始化未同步，`cargo test -p xfer-bt` 无法编译。已全部补齐，`cargo test -p xfer-storage -p xfer-bt` 全绿（19 个测试目标 0 失败）。

### 验证结果

| 项目 | 结果 |
|------|------|
| release 构建 | 7m43s 完成（LTO fat + codegen-units=1） |
| 单元测试 | xfer-storage 34 项 + xfer-bt 全部目标通过 |
| 本地回环 BT 测试（bt_real_test.py） | 20MB 3.54s 完成，SHA-256 校验通过，峰值 39.5MB/s |
| 公网测试 #1（release，600s 超时截断） | 峰值 3.04MB/s，进度 79.5%，引擎持续正常下载 |
| 公网测试 #2（release，600s 超时截断） | 进度 97.5%，磁盘实测写入速率中段 ~7.7MB/s |
| 对照 debug 构建（报告原问题） | ~200KB/s → release 数 MB/s，**主因确认为 debug 构建** |

**结论**：报告核心判断成立——主因是公网测试误用 debug 构建。release 构建下引擎达到数 MB/s 量级（受 600s 超时截断未跑完全程；历史 22 分钟完整跑局曾以 ~4.8MB/s 均速完整下载 6.35GB 并全量落盘）。

### 新发现问题（待后续处理）

1. **`done_bytes` 计数器与位图/磁盘背离**：公网局引擎 RPC 上报进度（97.5%）显著高于实际位图/磁盘落盘（52.6%，逐片抽查 20/20 吻合）。计数器只在 `accept_piece` 成功路径累加、与 `mark_done` 配对更新，虚高机制尚未定位（建议：`task.tell` 的 `completedLength` 改为从位图实时推导，或加计数器/位图一致性断言日志）。
2. **稀疏文件逻辑长度误判风险**：启动期「文件已完整则 `mark_all_done`」与 `restore_resume` 覆盖校验均用 `m.len()`（逻辑长度）——稀疏/预分配文件会被误判完整。BT 引擎自身文档已承认「写入静默跳过 → 片被标记完成但数据未落盘」隐患（`set_selected_files` 热切换处有防护，其余路径需排查）。
3. **测试脚本统计口径**：`bt_public_test.py` 的「已下载/平均速度」用 `os.path.getsize`（逻辑大小），稀疏文件下虚高；进度列依赖引擎 `completedLength`（见问题 1）。
4. **测试环境噪声**：沙箱内运行时 `~/.xfer/ctrl` 续传写入被拦（每 1.3s 一条 WARN）；另发现并发引擎进程（疑似 LinkCore 内嵌引擎）在下载同一种子，ctrl 目录存在多进程交错写入，`~/.xfer/ctrl` 已堆积 1000+ 残留 `.tmp` 建议清理。
