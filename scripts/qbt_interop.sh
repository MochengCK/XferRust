#!/bin/bash
# 真实客户端加密互通验收：本引擎 ↔ qBittorrent（libtorrent，强制加密）。
# qBittorrent 设 Session\Encryption=1（仅允许加密），任何成功传输必为加密。
# 方向 A：qBittorrent 拨入本引擎做种端（验证我方 MSE 响应方）；
# 方向 B：本引擎拨入 qBittorrent（验证我方 MSE 发起方）。
# 独立 tracker 进程贯穿两个方向，避免中途杀 seed 造成发现断档。
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK=/tmp/xfer_qbt_interop
TRACKER_PORT=18219
TORRENT="$WORK/qbt.torrent"
SOURCE="$WORK/source_data.bin"
QBT=/Applications/qbittorrent.app/Contents/MacOS/qbittorrent
QLOG="$WORK/qbt_profile/qBittorrent/data/logs/qbittorrent.log"

rm -rf "$WORK"
mkdir -p "$WORK/qbt_profile/qBittorrent/config" "$WORK/qbt_download"

cat > "$WORK/qbt_profile/qBittorrent/config/qBittorrent.ini" <<EOF
[AutoRun]
enabled=false

[BitTorrent]
Session\DHTEnabled=false
Session\Encryption=1
Session\GlobalUPSpeedLimit=524288
Session\LSDEnabled=false
Session\PeXEnabled=false
Session\QueueingSystemEnabled=false
Session\SavePath=$WORK/qbt_download/
Session\TempPathEnabled=false

[LegalNotice]
Accepted=true

[Preferences]
Downloads\SavePath=$WORK/qbt_download/
EOF

TRACKER_PID=""
SEED_PID=""
QBT_PID=""
cleanup() {
    [ -n "$SEED_PID" ] && kill "$SEED_PID" 2>/dev/null
    [ -n "$TRACKER_PID" ] && kill "$TRACKER_PID" 2>/dev/null
    [ -n "$QBT_PID" ] && kill "$QBT_PID" 2>/dev/null
    sleep 1
    [ -n "$QBT_PID" ] && kill -9 "$QBT_PID" 2>/dev/null
    true
}
trap cleanup EXIT

echo "== 构建互通验收台 =="
(cd "$ROOT" && cargo build -q -p xfer-bt --example qbt_interop) || exit 1
BIN="$ROOT/target/debug/examples/qbt_interop"

echo "== 启动独立 tracker（贯穿两个方向）=="
"$BIN" tracker --tracker-port "$TRACKER_PORT" > "$WORK/tracker.log" 2>&1 &
TRACKER_PID=$!
for _ in $(seq 1 20); do grep -q "^TRACKER_READY" "$WORK/tracker.log" && break; sleep 0.25; done
grep "^TRACKER_READY" "$WORK/tracker.log" || { echo "TRACKER_NOT_READY"; cat "$WORK/tracker.log"; exit 1; }

echo "== 方向 A：qBittorrent → 本引擎（强制加密拨入）=="
"$BIN" seed --tracker-port "$TRACKER_PORT" --no-tracker --upload-limit 524288 \
    --torrent "$TORRENT" --source "$SOURCE" \
    > "$WORK/seed.log" 2>&1 &
SEED_PID=$!
for _ in $(seq 1 40); do grep -q "^READY" "$WORK/seed.log" && break; sleep 0.25; done
grep "^READY" "$WORK/seed.log" || { echo "SEED_NOT_READY"; cat "$WORK/seed.log"; exit 1; }

# qBittorrent 直接以 torrent 文件为 CLI 参数（不经 WebUI）
"$QBT" --profile="$WORK/qbt_profile" --no-splash "$TORRENT" \
    > "$WORK/qbt_stdout.log" 2>&1 &
QBT_PID=$!

DIR_A_OK=0
for i in $(seq 1 90); do
    SIZE=$(stat -f%z "$WORK/qbt_download/data.bin" 2>/dev/null || echo 0)
    [ "$i" -eq 1 ] || [ $((i % 10)) -eq 0 ] && echo "已下载 $SIZE/1048576"
    [ "$SIZE" = "1048576" ] && { DIR_A_OK=1; break; }
    sleep 1
done
[ "$DIR_A_OK" = 1 ] || { echo "方向 A 超时"; tail -20 "$WORK/seed.log" "$QLOG"; exit 1; }
sleep 2   # 给锁存观察一个采样窗口
cmp "$WORK/qbt_download/data.bin" "$SOURCE" || { echo "方向 A 内容不一致"; exit 1; }
grep -q "加密支持：强制" "$QLOG" || { echo "qBittorrent 未处于强制加密模式"; cat "$QLOG"; exit 1; }
tail -3 "$WORK/seed.log"
# 累计量断言：uploaded 单调递增到全量；锁存标记证明曾出现加密的 qBittorrent 对端
grep -q "uploaded=1048576" "$WORK/seed.log" || \
    { echo "方向 A 本引擎未上传满全量"; tail -5 "$WORK/seed.log"; exit 1; }
grep "STATUS" "$WORK/seed.log" | grep -q "ever_encrypted=true ever_saw_qbt=true" || \
    { echo "方向 A 本引擎侧未观察到加密 qBittorrent 对端"; grep STATUS "$WORK/seed.log" | tail -5; exit 1; }
echo "方向 A 通过：qBittorrent 强制加密拨入，1MiB 传输内容一致，本引擎确认加密对端"

echo "== 方向 B：本引擎 → qBittorrent（强制加密拨出）=="
# 仅停 seed 的供块；独立 tracker 继续运行，qBittorrent 已成种子并持续 announce
kill "$SEED_PID" 2>/dev/null; SEED_PID=""; sleep 2
"$BIN" leech --no-tracker --torrent "$TORRENT" --dir "$WORK/leech" --download-limit 524288 \
    > "$WORK/leech.log" 2>&1
LE=$?
tail -4 "$WORK/leech.log"
[ $LE -eq 0 ] || { echo "方向 B 失败"; exit 1; }
grep -q "LEECH_OK encrypted_observed=true" "$WORK/leech.log" || \
    { echo "方向 B 未观察到加密连接"; exit 1; }
echo "方向 B 通过：本引擎加密拨出至 qBittorrent，1MiB 下载内容一致"

echo "== PASS：与真实 qBittorrent（libtorrent，强制加密）双向互通验证通过 =="
