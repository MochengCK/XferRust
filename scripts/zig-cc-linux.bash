#!/usr/bin/env bash
# 交叉 C 编译包装器（zig → x86_64 Linux）：
# 过滤 cc-rs 追加的 --target=<rust triple>（zig 不认识 Rust 目标三元组，
# 目标由包装器固定为 x86_64-linux-gnu）。
#
# 用法：
#   CC_x86_64_unknown_linux_gnu=<repo>/scripts/zig-cc-linux.bash \
#   AR_x86_64_unknown_linux_gnu=<repo>/scripts/zig-ar.bash \
#   cargo check --workspace --all-targets --target x86_64-unknown-linux-gnu
filtered=()
for a in "$@"; do
    [[ "$a" == --target=* ]] && continue
    filtered+=("$a")
done
exec zig cc -target x86_64-linux-gnu "${filtered[@]}"
