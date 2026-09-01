#!/usr/bin/env bash
# 交叉 C 编译包装器（zig → x86_64 Windows/GNU）：
# 过滤 cc-rs 追加的 --target=<rust triple>（zig 不认识 Rust 目标三元组，
# 目标由包装器固定为 x86_64-windows-gnu）。
#
# 用法：
#   CC_x86_64_pc_windows_gnu=<repo>/scripts/zig-cc-windows.bash \
#   AR_x86_64_pc_windows_gnu=<repo>/scripts/zig-ar.bash \
#   cargo check --workspace --all-targets --target x86_64-pc-windows-gnu
filtered=()
for a in "$@"; do
    [[ "$a" == --target=* ]] && continue
    filtered+=("$a")
done
exec zig cc -target x86_64-windows-gnu "${filtered[@]}"
