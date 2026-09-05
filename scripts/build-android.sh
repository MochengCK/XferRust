#!/usr/bin/env bash
# Android NDK 交叉编译脚本：仅构建引擎内核（xferrust），不包含 TUI。
#
# 前置条件：
#   1. 安装 Android NDK（推荐 r26+），设置 ANDROID_NDK_HOME 环境变量
#   2. 安装 Rust Android 目标：rustup target add aarch64-linux-android
#
# 用法：
#   scripts/build-android.sh [ndk_api_level]
#
# 产出：dist/android-arm64-v8a/xferrust
#
# 构建命令等价于：
#   CC_aarch64_linux_android=$NDK/toolchains/llvm/prebuilt/<host-tag>/bin/aarch64-linux-android24-clang \
#   CXX_aarch64_linux_android=$NDK/toolchains/llvm/prebuilt/<host-tag>/bin/aarch64-linux-android24-clang++ \
#   AR_aarch64_linux_android=$NDK/toolchains/llvm/prebuilt/<host-tag>/bin/llvm-ar \
#   CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER=$NDK/toolchains/llvm/prebuilt/<host-tag>/bin/aarch64-linux-android24-clang \
#   cargo build --release --no-default-features --bin xferrust --target aarch64-linux-android
set -euo pipefail

API_LEVEL="${1:-24}"  # Android 7.0+ (API 24)，覆盖绝大多数活跃设备
TARGET="aarch64-linux-android"
ARCH_DIR="arm64-v8a"

# ─── NDK 定位 ──────────────────────────────────────────────────────────────
NDK="${ANDROID_NDK_HOME:-${NDK_HOME:-}}"
if [ -z "$NDK" ]; then
    echo "错误：未设置 ANDROID_NDK_HOME 环境变量" >&2
    echo "请安装 Android NDK r26+ 并设置：" >&2
    echo "  export ANDROID_NDK_HOME=/path/to/android-ndk-r26" >&2
    exit 1
fi
# NDK r23+ 目录结构：toolchains/llvm/prebuilt/<host-tag>/bin
# 自动检测 host tag（macOS → darwin-x86_64，Linux → linux-x86_64）
HOST_TAG=""
for tag in "darwin-x86_64" "linux-x86_64" "darwin-arm64"; do
    if [ -d "$NDK/toolchains/llvm/prebuilt/$tag" ]; then
        HOST_TAG="$tag"
        break
    fi
done
if [ -z "$HOST_TAG" ]; then
    echo "错误：$NDK 不是有效的 NDK 路径（找不到 toolchains/llvm/prebuilt/<host-tag>）" >&2
    echo "请确认 ANDROID_NDK_HOME 指向 NDK 根目录" >&2
    exit 1
fi

LLVM_BIN="$NDK/toolchains/llvm/prebuilt/$HOST_TAG/bin"
CLANG_PREFIX="${TARGET}${API_LEVEL}"
CC="${LLVM_BIN}/${CLANG_PREFIX}-clang"
CXX="${LLVM_BIN}/${CLANG_PREFIX}-clang++"
AR="${LLVM_BIN}/llvm-ar"
LINKER="${CC}"  # 用 clang 做 linker（NDK 推荐）

echo "━━━ Android NDK 交叉编译 ━━━"
echo "NDK:       $NDK"
echo "Host Tag:  $HOST_TAG"
echo "Target:    $TARGET"
echo "API Level: $API_LEVEL"
echo "CC:        $CC"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ─── 构建 ────────────────────────────────────────────────────────────────────
cd "$(dirname "$0")/.."

export CC_aarch64_linux_android="$CC"
export CXX_aarch64_linux_android="$CXX"
export AR_aarch64_linux_android="$AR"
export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="$LINKER"

# --no-default-features 禁用 TUI；--bin xferrust 仅构建引擎内核
cargo build --release \
    --no-default-features \
    --bin xferrust \
    --target "$TARGET"

# ─── 产出 ────────────────────────────────────────────────────────────────────
DIST_DIR="dist/android-${ARCH_DIR}"
mkdir -p "$DIST_DIR"
cp "target/${TARGET}/release/xferrust" "$DIST_DIR/xferrust"
cp LICENSE README.md "$DIST_DIR/"

echo ""
echo "✓ 构建成功"
echo "  产物: $DIST_DIR/xferrust"
file "$DIST_DIR/xferrust"
