#!/usr/bin/env bash
# 交叉归档包装器（zig ar）：供交叉编译时 cc-rs 使用。
exec zig ar "$@"
