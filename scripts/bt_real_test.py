#!/usr/bin/env python3
"""真实环境 BT 下载测试：验证 Rust 引擎的 BT 下载速度与正确性。

流程：
1. 生成 20MB 伪随机测试数据
2. 用 ci_bt.py 生成 .torrent 文件
3. 启动 ci_bt.py 的 seed（本地 HTTP tracker + BT seeder）
4. 启动 xferrust 引擎守护进程（release 二进制）
5. 通过 WS RPC 添加 .torrent 下载任务
6. 持续轮询 task.tell，记录下载进度与速度
7. 验证下载文件 SHA-256 一致性
8. 输出速度报告

用法：
  python3 scripts/bt_real_test.py [--xferrust <path>] [--xfer <path>] [--size <bytes>]
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.parse

# Windows 控制台编码兼容
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def run(cmd, timeout=60, cwd=None):
    return subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8",
        errors="replace", timeout=timeout, cwd=cwd,
    )


def wait_until(fn, timeout, interval=0.5, desc=""):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if fn():
                return True
        except Exception:
            pass
        time.sleep(interval)
    raise AssertionError(f"超时等待 {desc}")


def format_speed(bps: float) -> str:
    if bps < 1024:
        return f"{bps:.0f} B/s"
    elif bps < 1024 * 1024:
        return f"{bps / 1024:.1f} KB/s"
    else:
        return f"{bps / (1024 * 1024):.2f} MB/s"


def format_size(b: int) -> str:
    if b < 1024:
        return f"{b} B"
    elif b < 1024 * 1024:
        return f"{b / 1024:.1f} KB"
    else:
        return f"{b / (1024 * 1024):.2f} MB"


def main() -> int:
    ap = argparse.ArgumentParser(description="XferRust 真实环境 BT 下载测试")
    ap.add_argument("--xferrust", default=None, help="xferrust 引擎二进制路径")
    ap.add_argument("--xfer", default=None, help="xfer CLI 二进制路径")
    ap.add_argument("--size", type=int, default=20 * 1024 * 1024, help="测试数据大小（字节）")
    ap.add_argument("--rpc-port", type=int, default=0, help="RPC 端口（默认随机）")
    ap.add_argument("--token", default="bt-test", help="RPC secret")
    ap.add_argument("--keep-daemon", action="store_true", help="测试后不杀守护进程")
    ap.add_argument("--download-timeout", type=int, default=120, help="下载超时（秒）")
    args = ap.parse_args()

    # 定位二进制
    xferrust = args.xferrust or os.path.join(
        os.path.dirname(SCRIPT_DIR), "target", "release", "xferrust"
    )
    xfer = args.xfer or os.path.join(
        os.path.dirname(SCRIPT_DIR), "target", "release", "xfer"
    )
    if not os.path.exists(xferrust):
        # 尝试 debug 构建
        xferrust_alt = os.path.join(
            os.path.dirname(SCRIPT_DIR), "target", "debug", "xferrust"
        )
        if os.path.exists(xferrust_alt):
            xferrust = xferrust_alt
        else:
            print(f"错误：找不到 xferrust 二进制 ({xferrust})")
            return 1

    print(f"引擎二进制: {xferrust}")
    print(f"CLI 二进制:  {xfer} ({'存在' if os.path.exists(xfer) else '不存在，将直接用 WS RPC'})")

    # 工作目录
    work = os.path.join(os.environ.get("TMPDIR", "/tmp"), f"bt-real-test-{os.getpid()}")
    os.makedirs(work, exist_ok=True)
    print(f"工作目录: {work}")

    rpc_port = args.rpc_port or free_port()
    token = args.token

    # ---- 1. 生成测试数据 ----
    print(f"\n[1/7] 生成 {format_size(args.size)} 测试数据...")
    src_file = os.path.join(work, "src.bin")
    chunk_size = 65536
    chunk = bytes((i * 31 + 7) % 256 for i in range(chunk_size))
    with open(src_file, "wb") as f:
        remaining = args.size
        while remaining > 0:
            n = min(remaining, chunk_size)
            f.write(chunk[:n])
            remaining -= n
    src_sha256 = sha256_file(src_file)
    print(f"  源文件 SHA-256: {src_sha256[:16]}...")
    print(f"  源文件大小: {format_size(args.size)}")

    # ---- 2. 生成 .torrent ----
    print("\n[2/7] 生成 .torrent 文件...")
    bt_script = os.path.join(SCRIPT_DIR, "ci_bt.py")
    tracker_port = free_port()
    peer_port = free_port()
    announce = f"http://127.0.0.1:{tracker_port}/announce"
    torrent_file = os.path.join(work, "test.torrent")

    r = run([sys.executable, bt_script, "make-torrent", src_file, announce, torrent_file], timeout=60)
    if r.returncode != 0:
        print(f"  失败: {r.stderr}")
        return 1
    info_hash = r.stdout.strip()
    print(f"  info_hash: {info_hash}")
    print(f"  announce:  {announce}")

    # ---- 3. 启动 seeder ----
    print("\n[3/7] 启动本地 BT seeder + tracker...")
    seed = subprocess.Popen(
        [sys.executable, bt_script, "seed", src_file, torrent_file,
         "--tracker-port", str(tracker_port),
         "--peer-port", str(peer_port)],
        stdout=subprocess.PIPE,
        stderr=open(os.path.join(work, "seed.err"), "wb"),
    )
    time.sleep(1.5)  # 等 tracker/seeder 就绪
    if seed.poll() is not None:
        print(f"  Seeder 启动失败！")
        with open(os.path.join(work, "seed.err"), "r", errors="replace") as f:
            print(f"  错误: {f.read()}")
        return 1
    print(f"  Tracker 端口: {tracker_port}")
    print(f"  Seeder 端口: {peer_port}")

    # ---- 4. 启动 xferrust 引擎守护进程 ----
    print("\n[4/7] 启动 xferrust 引擎守护进程...")
    log_file = os.path.join(work, "daemon.log")
    logf = open(log_file, "wb", buffering=0)
    daemon = subprocess.Popen(
        [xferrust,
         f"--rpc-listen-port={rpc_port}",
         f"--rpc-secret={token}",
         f"--dir={work}",
         f"--max-concurrent-downloads=5"],
        stdout=logf,
        stderr=subprocess.STDOUT,
    )

    ws_url = f"ws://127.0.0.1:{rpc_port}/jsonrpc"

    # 等待守护进程就绪
    try:
        wait_until(
            lambda: run([xfer, "stat", "--token", token, "--connect", ws_url], timeout=10).returncode == 0
            if os.path.exists(xfer)
            else _http_rpc(rpc_port, token, "engine.stat", {}).get("result") is not None,
            timeout=30, desc="守护进程 RPC 就绪"
        )
    except AssertionError:
        # 用 HTTP POST 兜底探活
        wait_until(
            lambda: _http_rpc(rpc_port, token, "engine.stat", {}).get("result") is not None,
            timeout=15, desc="守护进程 HTTP RPC 就绪"
        )
    print(f"  RPC 端口: {rpc_port}")
    print(f"  WS URL: {ws_url}")

    # ---- 5. 添加 BT 下载任务 ----
    print("\n[5/7] 添加 .torrent 下载任务...")
    with open(torrent_file, "rb") as f:
        torrent_b64 = base64.b64encode(f.read()).decode()

    dl_dir = os.path.join(work, "download")
    os.makedirs(dl_dir, exist_ok=True)

    resp = _http_rpc(rpc_port, token, "task.add", {
        "torrent": torrent_b64,
        "dir": dl_dir,
    })
    if "error" in resp:
        print(f"  添加任务失败: {resp['error']}")
        _cleanup(daemon, seed, logf, not args.keep_daemon)
        return 1
    gid = resp.get("result", {}).get("gid", "")
    print(f"  GID: {gid}")

    # ---- 6. 监控下载进度与速度 ----
    print(f"\n[6/7] 监控下载进度（超时 {args.download_timeout}s）...")
    print(f"  {'时间':>6}  {'进度':>6}  {'已下载':>10}  {'总大小':>10}  {'速度':>12}  {'peer数':>6}")
    print(f"  {'-'*6}  {'-'*6}  {'-'*10}  {'-'*10}  {'-'*12}  {'-'*6}")

    start_time = time.time()
    last_bytes = 0
    last_time = start_time
    speed_samples = []
    max_speed = 0
    peer_count = 0
    completed = False

    while time.time() - start_time < args.download_timeout:
        time.sleep(0.5)
        resp = _http_rpc(rpc_port, token, "task.tell", {"gid": gid})
        if "error" in resp:
            continue

        info = resp.get("result", {})
        status = info.get("status", "")
        total = info.get("totalLength", 0)
        completed_bytes = info.get("completedLength", 0)
        speed = info.get("downloadSpeed", 0)
        peers = info.get("peers", [])
        peer_count = len(peers) if isinstance(peers, list) else 0

        elapsed = time.time() - start_time
        dt = time.time() - last_time
        if dt > 0 and completed_bytes > last_bytes:
            inst_speed = (completed_bytes - last_bytes) / dt
        else:
            inst_speed = speed

        if inst_speed > max_speed:
            max_speed = inst_speed
        if inst_speed > 0:
            speed_samples.append(inst_speed)

        pct = (completed_bytes / total * 100) if total > 0 else 0
        print(f"  {elapsed:6.1f}s  {pct:5.1f}%  {format_size(completed_bytes):>10}  {format_size(total):>10}  {format_speed(inst_speed):>12}  {peer_count:>6}")

        last_bytes = completed_bytes
        last_time = time.time()

        if status in ("complete", "completed"):
            completed = True
            break
        if status in ("error", "removed"):
            print(f"\n  任务异常状态: {status}")
            break

    elapsed_total = time.time() - start_time

    # ---- 7. 验证下载文件 ----
    print(f"\n[7/7] 验证下载文件...")
    downloaded_file = os.path.join(dl_dir, "src.bin")
    if not os.path.exists(downloaded_file):
        # 尝试其他文件名
        try:
            files = os.listdir(dl_dir)
            print(f"  下载目录文件: {files}")
            if files:
                downloaded_file = os.path.join(dl_dir, files[0])
        except OSError:
            pass

    if os.path.exists(downloaded_file):
        dl_size = os.path.getsize(downloaded_file)
        dl_sha256 = sha256_file(downloaded_file)
        print(f"  下载文件: {os.path.basename(downloaded_file)}")
        print(f"  下载大小: {format_size(dl_size)}")
        print(f"  下载 SHA-256: {dl_sha256[:16]}...")
        print(f"  源 SHA-256:   {src_sha256[:16]}...")
        if dl_sha256 == src_sha256:
            print("  SHA-256 校验: 通过")
            integrity_ok = True
        else:
            print("  SHA-256 校验: 失败")
            integrity_ok = False
    else:
        print("  下载文件不存在！")
        dl_size = 0
        integrity_ok = False

    # ---- 速度报告 ----
    print("\n" + "=" * 60)
    print("BT 下载测试报告")
    print("=" * 60)
    print(f"  文件大小:     {format_size(args.size)}")
    print(f"  下载大小:     {format_size(dl_size)}")
    print(f"  耗时:         {elapsed_total:.2f}s")
    if elapsed_total > 0 and dl_size > 0:
        avg_speed = dl_size / elapsed_total
        print(f"  平均速度:     {format_speed(avg_speed)}")
    if speed_samples:
        avg_sample = sum(speed_samples) / len(speed_samples)
        print(f"  采样均速:     {format_speed(avg_sample)}")
    print(f"  峰值速度:     {format_speed(max_speed)}")
    print(f"  最大 peer 数: {peer_count}")
    print(f"  完整性校验:   {'通过' if integrity_ok else '失败'}")
    print(f"  下载状态:     {'完成' if completed else '未完成'}")
    print("=" * 60)

    # ---- 清理 ----
    _cleanup(daemon, seed, logf, not args.keep_daemon)

    if completed and integrity_ok:
        print("\n✅ BT 下载测试通过：下载完整且速度正常")
        return 0
    elif completed and not integrity_ok:
        print("\n⚠️  下载完成但完整性校验失败")
        return 1
    else:
        print(f"\n❌ BT 下载未完成（{elapsed_total:.1f}s 内未完成）")
        # 打印守护进程日志尾部
        with open(log_file, "r", errors="replace") as f:
            lines = f.read().splitlines()
            if lines:
                print("\n--- 守护进程日志尾部 ---")
                for line in lines[-30:]:
                    print(f"  {line}")
        return 1


def _http_rpc(port: int, token: str, method: str, params: dict) -> dict:
    """通过 HTTP POST 发送 JSON-RPC 请求。"""
    import urllib.request
    payload = {
        "jsonrpc": "2.0",
        "id": "1",
        "method": method,
        "params": {**params, "token": token},
    }
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/jsonrpc",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


def _cleanup(daemon, seed, logf, kill_daemon=True):
    if kill_daemon:
        daemon.terminate()
        try:
            daemon.wait(timeout=15)
        except subprocess.TimeoutExpired:
            daemon.kill()
    seed.kill()
    try:
        seed.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass
    logf.close()


if __name__ == "__main__":
    raise SystemExit(main())
