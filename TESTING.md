# XferRust 测试规范

统一的测试分层、入口与编写约定。本地与 CI 共用同一套流程（`scripts/test.sh`），
保证「本地验证过的 = CI 验证的」。

## 一、三层测试体系

| 层级 | 命令 | 覆盖内容 | 耗时基线 | 使用场景 |
|------|------|----------|----------|----------|
| **unit（快速层）** | `cargo t` 或 `scripts/test.sh unit` | `cargo test --workspace --lib --bins`：全部单元测试 + TUI/bin 测试 | **~2.5 分钟** | 日常开发，每次改动后随手跑 |
| **full（全量层）** | `cargo tfull` 或 `scripts/test.sh full` | `cargo test --workspace`：单元层 + `crates/*/tests/` 集成测试（BT e2e、resume、magnet、调度器、RPC 等） | 首次编译 ~13 分钟，缓存后 **~4 分钟** | 提交前、合并前 |
| **blackbox（黑盒层）** | `scripts/test.sh blackbox` | 构建 release 产物后跑 `scripts/ci_test.py`：版本冒烟、引擎 RPC 探活、HTTP/HTTPS 下载 SHA 校验、磁力全流程 | ~10 分钟（含 release 编译） | 发版前、CI 门禁 |

三条铁律：

1. **push 前至少跑满 `full`**；只跑过 `unit` 的改动不要推送 main。
2. **CI 与本地同源**：CI 的 cargo 测试 job 与黑盒 job 调用的就是 `scripts/test.sh`，
   不要在 CI 里另写一套命令。
3. **失败即停止**：任何一层失败先修复再继续，不允许带失败推送（`concurrency` 会
   取消旧 run，但不会阻止坏代码进 main）。

## 二、快速参考

```bash
scripts/test.sh                    # = unit，日常默认
scripts/test.sh full               # 提交前必跑
scripts/test.sh blackbox           # 发版前必跑（走 CI 同款黑盒脚本）
scripts/test.sh all                # 三层按序全跑
scripts/test.sh full -- --nocapture  # 透传参数给 cargo test
XFER_TEST_HTTPS_URL=<url> scripts/test.sh blackbox  # 自定义 HTTPS 测试源

cargo t            # 等价 unit（.cargo/config.toml 别名）
cargo tfull        # 等价 full
```

单测过滤：`cargo t -p xfer-bt`、`cargo t rate_change`（按名字过滤）、
`cargo tfull -- --test-threads=1`（串行排查顺序依赖问题）。

## 三、CI 集成（.github/workflows/ci.yml）

```
build ──┬──> test（黑盒：3 平台跑 ci_test.py）──┐
unit-test（cargo test 全量层，ubuntu）─────────┼──> release（全绿才出草稿）
build-android ────────────────────────────────┘
```

- `unit-test` job：ubuntu 上跑 `scripts/test.sh full`（`timeout-minutes: 20`）。
  此前 CI 只跑黑盒，`cargo test` 从未在 CI 执行——曾出现单元测试静默等待
  33 分钟无人发现（见第五节），自 2026-09 起补齐。
- `test` job：3 平台黑盒功能测试（下载 ci_test.py 的报告与日志）。
- `release` job 的 `needs` 必须包含以上全部测试 job。

## 四、编写测试的规范

### 分层归属

- **unit 层**（`src` 内 `#[cfg(test)]` / `src/bin` 测试）：纯逻辑、秒级、确定性。
  每个新函数/协议分支都应有归属。
- **full 层**（`crates/*/tests/*.rs`）：跨 crate 集成、多组件协作（BT 引擎
  端到端、断点续传、RPC 往返）。允许多秒级耗时，但不允许外网依赖。
- **blackbox 层**（`scripts/ci_test.py`）：只针对「编译产物能否正确工作」，
  不测源码内部逻辑。新增面向用户的功能（如新的 RPC 能力）在此补端到端用例。

### 硬性约定

1. **不碰真实外网**：测试内只允许 loopback（`127.0.0.1`、本地 axum/静态服务器）
   和 `example.*` 假域名（用于验证「不可达」行为）。真实 tracker/公网文件只出现在
   blackbox 层且经参数注入（如 `--https-url`）。
2. **时间相关测试必须秒级完成**：任何单个测试 ≤ 5 秒；依赖真实睡眠的用例要写明
   预期等待时长并设上界断言（防止限速/退避逻辑回归成永久等待）。
   能用 `tokio::time::pause` 虚拟时间就不要真实 sleep。
3. **确定性**：不依赖执行顺序、不共享全局状态；并发测试用 `--test-threads`
   无关的写法。
4. **文件隔离**：临时数据一律写 `std::env::temp_dir()` 下的独立子目录或
   `tempfile`，禁止写引擎默认配置目录（`~/.xfer/`）——单元测试进程会真实落盘
   ctrl 文件，既污染本机又会在受限环境报错。
5. **会话文件隔离**：起引擎进程的测试（含 blackbox）必须显式传
   `--save-session <tmp>`，避免读到真实会话。
6. **新增全局/任务选项**：至少覆盖「非法值被拒绝」「合法值生效」「会话恢复后仍生效」
   三类用例（参考 `max-download-limit` 的做法）。

## 五、已知坑与历史教训

- **`cargo test` 不重建主二进制**：`target/debug/xferrust` / `target/release/*`
  只在 `cargo build` 时更新。RPC 探测、黑盒测试前先 `cargo build --bins`
  （blackbox 层已在脚本内处理）。
- **真实 sleep 的代价**：`rate_change_takes_effect` 曾在 1KB/s 限速下
  `acquire(2MB)` 需要等待约 33 分钟——本地表现为「测试卡死」，CI 每轮空耗半小时
  却因当时 CI 不跑 cargo test 而无人发现。修复后同断言 0.9 秒完成。
  新增时间相关用例时把这条写进直觉：**等待时长 = 缺口/速率，先算再写**。
- **`~/.xfer/ctrl` 污染**：未隔离会话目录的引擎测试会向真实配置目录写 ctrl
  临时文件；在沙箱/受限环境会大量 WARN（file-write-unlink）。本机可定期清理
  `~/.xfer/ctrl/*.tmp`，但正确做法是测试里显式指定会话/数据目录。
- **xfer-geo 真实数据库测试**：`load_real_v4_db` 依赖仓库内
  `data/ip2region_v4.xdb`（11MB，已入库）。删库或换库前先改掉该测试。
- **Windows 黑盒编码**：cp1252 控制台无法输出中文，ci_test.py 已强制 UTF-8
  （`PYTHONUTF8=1`）；新增黑盒脚本沿用该做法。
- **BSD grep**（macOS）不支持 BRE `\|` 交替，写检索命令用 `grep -E "a|b"`。

## 六、故障排查

| 现象 | 处置 |
|------|------|
| 测试「卡住」不动 | `sample <pid>` 看栈（macOS）确认卡点；先算等待时长是否合理 |
| 本地过、CI 挂（或反之） | 确认两侧跑的是同一层（`scripts/test.sh` 同一 tier），并对比编译产物新旧 |
| ctrl 文件 WARN 刷屏 | 见第五节会话隔离条目 |
| geo 测试偶发失败 | data 文件是否在位；读失败被 `.ok()` 吞掉时看日志级别 |
