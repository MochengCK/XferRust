#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# XferRust 测试统一入口 —— 本地与 CI 共用同一套分层流程。
#
# 用法：
#   scripts/test.sh              # 等价于 unit（日常开发默认）
#   scripts/test.sh unit         # 快速层：单元测试（lib + bins）
#   scripts/test.sh full         # 全量层：单元 + 集成（BT/引擎 e2e 等）
#   scripts/test.sh blackbox     # 黑盒层：release 产物 + ci_test.py 功能测试
#   scripts/test.sh all          # unit → full → blackbox 依次执行
#   scripts/test.sh full -- --nocapture   # 额外参数透传给 cargo test / ci_test.py
#
# 层级说明见 TESTING.md。任一层失败立即退出（exit != 0）。
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/.."

# 非交互环境（CI / 沙箱 / 被调度的脚本）PATH 可能缺 rustup 安装位，兜底补上
if ! command -v cargo >/dev/null 2>&1; then
    export PATH="$HOME/.cargo/bin:$PATH"
fi

TIER="${1:-unit}"
[ $# -gt 0 ] && shift

banner() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

run_unit() {
    banner "单元测试（lib + bins，不含集成 e2e）"
    cargo test --workspace --lib --bins "$@"
}

run_full() {
    banner "全量测试（单元 + crates/*/tests 集成 e2e）"
    cargo test --workspace "$@"
}

run_blackbox() {
    banner "黑盒功能测试（release 产物 + scripts/ci_test.py）"
    # 黑盒针对最终产物：先确保 release 二进制是当前代码的（cargo test 不会重建主二进制）
    cargo build --release --bin xfer --bin xferrust
    # HTTPS 用例默认取与 CI 相同的公开稳定文件；本地可经 XFER_TEST_HTTPS_URL 覆盖
    HTTPS_URL="${XFER_TEST_HTTPS_URL:-https://raw.githubusercontent.com/torvalds/linux/master/README}"
    PYTHONUTF8=1 python3 scripts/ci_test.py \
        --xfer target/release/xfer \
        --xferrust target/release/xferrust \
        --out target/test-out \
        --token local \
        --https-url "$HTTPS_URL" "$@"
}

case "$TIER" in
    unit)     run_unit "$@" ;;
    full)     run_full "$@" ;;
    blackbox) run_blackbox "$@" ;;
    all)
        run_unit
        run_full
        run_blackbox
        ;;
    *)
        sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'
        exit 1
        ;;
esac
