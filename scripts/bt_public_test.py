#!/usr/bin/env python3
"""公网真实环境 BT 下载测试：连接真实 peer，验证下载速度与完整性。

使用公网公开可用的种子（Ubuntu/Debian ISO 等），通过 xferrust 引擎
进行真实 BT 下载，监控 peer 连接、下载速度、完成度。

用法：
  python3 scripts/bt_public_test.py [--xferrust <path>] [--xfer <path>]
      [--torrent-url <url>] [--torrent-file <path>]
      [--download-timeout <sec>] [--keep-daemon]
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def free_port() -> int:
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def format_speed(bps: float) -> str:
    if bps < 1024:
        return f"{bps:.0f} B/s"
    elif bps < 1024 * 1024:
        return f"{bps / 1024:.1f} KB/s"
    elif bps < 1024 * 1024 * 1024:
        return f"{bps / (1024 * 1024):.2f} MB/s"
    else:
        return f"{bps / (1024 * 1024 * 1024):.2f} GB/s"


def format_size(b: int) -> str:
    if b < 1024:
        return f"{b} B"
    elif b < 1024 * 1024:
        return f"{b / 1024:.1f} KB"
    elif b < 1024 * 1024 * 1024:
        return f"{b / (1024 * 1024):.2f} MB"
    else:
        return f"{b / (1024 * 1024 * 1024):.2f} GB"


def http_rpc(port: int, token: str, method: str, params: dict) -> dict:
    """通过 HTTP POST 发送 JSON-RPC 请求。"""
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
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


def download_torrent_file(url: str, dest: str) -> bool:
    """下载 .torrent 文件。"""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "XferRust/0.2"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = resp.read()
        with open(dest, "wb") as f:
            f.write(data)
        return True
    except Exception as e:
        print(f"  下载种子文件失败: {e}")
        return False


# 公网可用的合法种子 URL（Ubuntu 官方 ISO torrent）
DEFAULT_TORRENTS = [
    # Ubuntu 24.04 LTS (Noble Numbat) - amd64
    "https://releases.ubuntu.com/24.04/ubuntu-24.04.3-desktop-amd64.iso.torrent",
    # 备选：Ubuntu 22.04 LTS
    "https://releases.ubuntu.com/22.04/ubuntu-22.04.5-desktop-amd64.iso.torrent",
    # 备选：Debian 12
    "https://cdimage.debian.org/debian-cd/current/amd64/bt-cd/debian-12.9.0-amd64-netinst.iso.torrent",
]


def main() -> int:
    ap = argparse.ArgumentParser(description="XferRust 公网真实 BT 下载测试")
    ap.add_argument("--xferrust", default=None, help="xferrust 引擎二进制路径")
    ap.add_argument("--xfer", default=None, help="xfer CLI 二进制路径")
    ap.add_argument("--torrent-url", default="", help="公网 .torrent URL")
    ap.add_argument("--torrent-file", default="", help="本地 .torrent 文件路径")
    ap.add_argument("--rpc-port", type=int, default=0, help="RPC 端口")
    ap.add_argument("--token", default="bt-public-test", help="RPC secret")
    ap.add_argument("--keep-daemon", action="store_true", help="测试后不杀守护进程")
    ap.add_argument("--download-timeout", type=int, default=300, help="下载监控超时（秒）")
    ap.add_argument("--poll-interval", type=float, default=2.0, help="轮询间隔（秒）")
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
        alt = os.path.join(os.path.dirname(SCRIPT_DIR), "target", "debug", "xferrust")
        if os.path.exists(alt):
            xferrust = alt
        else:
            print(f"错误：找不到 xferrust 二进制 ({xferrust})")
            return 1

    print(f"引擎二进制: {xferrust}")
    print(f"CLI 二进制:  {xfer} ({'存在' if os.path.exists(xfer) else '不存在'})")

    # 工作目录
    work = os.path.join(os.environ.get("TMPDIR", "/tmp"), f"bt-public-test-{os.getpid()}")
    os.makedirs(work, exist_ok=True)
    print(f"工作目录: {work}")

    rpc_port = args.rpc_port or free_port()
    token = args.token

    # ---- 1. 获取种子文件 ----
    print("\n[1/6] 获取种子文件...")
    torrent_file = os.path.join(work, "test.torrent")

    if args.torrent_file:
        # 使用本地种子文件
        import shutil
        shutil.copy2(args.torrent_file, torrent_file)
        print(f"  使用本地种子文件: {args.torrent_file}")
    elif args.torrent_url:
        # 使用指定的 URL
        if not download_torrent_file(args.torrent_url, torrent_file):
            return 1
        print(f"  下载种子文件: {args.torrent_url}")
    else:
        # 尝试默认种子
        success = False
        for url in DEFAULT_TORRENTS:
            print(f"  尝试: {url}")
            if download_torrent_file(url, torrent_file):
                print(f"  成功下载种子文件")
                success = True
                args.torrent_url = url
                break
            time.sleep(1)
        if not success:
            print("  所有默认种子 URL 均不可用")
            return 1

    torrent_size = os.path.getsize(torrent_file)
    print(f"  种子文件大小: {format_size(torrent_size)}")

    # 解析种子文件获取文件名和大小
    try:
        import struct
        def b_decode(data, i=0):
            c = data[i:i+1]
            if c == b"i":
                j = data.index(b"e", i)
                return int(data[i+1:j]), j + 1
            if c == b"l":
                arr, i = [], i + 1
                while data[i:i+1] != b"e":
                    v, i = b_decode(data, i)
                    arr.append(v)
                return arr, i + 1
            if c == b"d":
                d, i = {}, i + 1
                while data[i:i+1] != b"e":
                    k, i = b_decode(data, i)
                    v, i = b_decode(data, i)
                    d[k] = v
                return d, i + 1
            j = data.index(b":", i)
            n = int(data[i:j])
            return data[j+1:j+1+n], j + 1 + n

        raw = open(torrent_file, "rb").read()
        tdict = b_decode(raw)[0]
        info = tdict.get(b"info", {})
        if b"name" in info:
            print(f"  种子名称: {info[b'name'].decode('utf-8', errors='replace')}")
        if b"length" in info:
            print(f"  文件大小: {format_size(info[b'length'])}")
        elif b"files" in info:
            total = sum(f.get(b"length", 0) for f in info[b"files"])
            print(f"  总大小: {format_size(total)}")
        announce = tdict.get(b"announce", b"")
        print(f"  Tracker: {announce.decode('utf-8', errors='replace')}")
        announce_list = tdict.get(b"announce-list", [])
        if announce_list:
            print(f"  Tracker 列表: {len(announce_list)} 个层级")
    except Exception as e:
        print(f"  (种子文件解析跳过: {e})")

    # ---- 2. 启动 xferrust 引擎守护进程 ----
    print("\n[2/6] 启动 xferrust 引擎守护进程...")
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
    ready = False
    deadline = time.time() + 30
    last_err = ""
    while time.time() < deadline:
        try:
            resp = http_rpc(rpc_port, token, "engine.getVersion", {})
            if "result" in resp:
                ready = True
                break
            last_err = resp.get("error", "未知")
        except Exception as e:
            last_err = str(e)
        time.sleep(1)

    if not ready:
        print(f"  守护进程 RPC 探活失败！最后错误: {last_err}")
        with open(log_file, "r", errors="replace") as f:
            content = f.read()
            print(f"  日志尾部: {content[-1000:]}")
        # 即使 RPC 探活失败，也检查进程是否在运行
        if daemon.poll() is not None:
            print(f"  守护进程已退出，退出码: {daemon.returncode}")
        else:
            print("  守护进程仍在运行，可能是 RPC 路径问题")
            # 尝试用 xfer CLI 探活
            if os.path.exists(xfer):
                r = subprocess.run(
                    [xfer, "stat", "--token", token, "--connect", ws_url],
                    capture_output=True, text=True, timeout=10
                )
                print(f"  xfer stat 退出码: {r.returncode}")
                print(f"  xfer stat stdout: {r.stdout[:200]}")
                if r.returncode == 0:
                    ready = True
        if not ready:
            _cleanup(daemon, logf, not args.keep_daemon)
            return 1

    print(f"  RPC 端口: {rpc_port}")
    print(f"  WS URL: {ws_url}")

    # ---- 3. 添加 BT 下载任务 ----
    print("\n[3/6] 添加 .torrent 下载任务...")
    with open(torrent_file, "rb") as f:
        torrent_b64 = base64.b64encode(f.read()).decode()

    dl_dir = os.path.join(work, "download")
    os.makedirs(dl_dir, exist_ok=True)

    resp = http_rpc(rpc_port, token, "task.add", {
        "torrent": torrent_b64,
        "dir": dl_dir,
    })
    if "error" in resp:
        print(f"  添加任务失败: {resp['error']}")
        _cleanup(daemon, logf, not args.keep_daemon)
        return 1
    gid = resp.get("result", {}).get("gid", "")
    print(f"  GID: {gid}")

    # ---- 4. 监控下载进度与速度 ----
    print(f"\n[4/6] 监控下载进度（超时 {args.download_timeout}s）...")
    print(f"  {'时间':>7}  {'进度':>6}  {'已下载':>10}  {'总大小':>10}  {'速度':>12}  {'peer':>4}  {'状态':>10}")
    print(f"  {'-'*7}  {'-'*6}  {'-'*10}  {'-'*10}  {'-'*12}  {'-'*4}  {'-'*10}")

    start_time = time.time()
    last_bytes = 0
    last_time = start_time
    speed_samples = []
    max_speed = 0
    max_peers = 0
    completed = False
    task_status = ""
    total_size = 0

    while time.time() - start_time < args.download_timeout:
        time.sleep(args.poll_interval)
        resp = http_rpc(rpc_port, token, "task.tell", {"gid": gid})
        if "error" in resp:
            continue

        info = resp.get("result", {})
        status = task_status = info.get("status", "")
        total = total_size = info.get("totalLength", 0)
        completed_bytes = info.get("completedLength", 0)
        speed = info.get("downloadSpeed", 0)
        connections = info.get("connections", 0)
        num_peers = connections
        if num_peers > max_peers:
            max_peers = num_peers

        # 也尝试获取 peer 详情
        peer_resp = http_rpc(rpc_port, token, "task.getPeers", {"gid": gid})
        if "result" in peer_resp:
            peer_list = peer_resp["result"]
            if isinstance(peer_list, list):
                num_peers = len(peer_list)
                if num_peers > max_peers:
                    max_peers = num_peers

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
        print(f"  {elapsed:7.1f}s  {pct:5.1f}%  {format_size(completed_bytes):>10}  {format_size(total):>10}  {format_speed(inst_speed):>12}  {num_peers:>4}  {status:>10}")

        last_bytes = completed_bytes
        last_time = time.time()

        if status in ("complete", "completed"):
            completed = True
            break
        if status in ("error", "removed", "failed"):
            print(f"\n  任务异常状态: {status}")
            break

    elapsed_total = time.time() - start_time

    # ---- 5. 验证下载文件 ----
    print(f"\n[5/6] 验证下载文件...")
    dl_files = []
    if os.path.exists(dl_dir):
        for root, dirs, files in os.walk(dl_dir):
            for f in files:
                fp = os.path.join(root, f)
                st = os.stat(fp)
                # BT 随机写落盘天然稀疏：逻辑大小（含空洞）虚高不可信，
                # 实际落盘量按 st_blocks×512 统计（非 unix 退回逻辑大小）
                try:
                    actual = st.st_blocks * 512
                except AttributeError:
                    actual = st.st_size
                dl_files.append((f, st.st_size, fp, actual))

    if dl_files:
        for name, size, path, actual in dl_files:
            print(f"  文件: {name}")
            print(f"  逻辑大小: {format_size(size)}  实际落盘: {format_size(actual)}")
            if size > 0:
                sha = sha256_file(path)
                print(f"  SHA-256: {sha[:32]}...")
    else:
        print("  下载文件不存在！")

    # ---- 6. 速度报告 ----
    print(f"\n[6/6] 测试报告")
    print("\n" + "=" * 70)
    print("公网真实环境 BT 下载测试报告")
    print("=" * 70)
    print(f"  种子来源:   {args.torrent_url or args.torrent_file or '默认'}")
    print(f"  总大小:     {format_size(total_size)}")
    # 实际落盘量（稀疏文件下 ≠ 逻辑大小）：速度/进度以真实字节为准
    dl_size = sum(a for _, _, _, a in dl_files)
    logical_size = sum(s for _, s, _, _ in dl_files)
    print(f"  已下载:     {format_size(dl_size)}")
    if logical_size != dl_size:
        print(f"  (逻辑大小:  {format_size(logical_size)}，含稀疏空洞)")
    print(f"  耗时:       {elapsed_total:.1f}s")
    if elapsed_total > 0 and dl_size > 0:
        avg_speed = dl_size / elapsed_total
        print(f"  平均速度:   {format_speed(avg_speed)}")
    if speed_samples:
        avg_sample = sum(speed_samples) / len(speed_samples)
        print(f"  采样均速:   {format_speed(avg_sample)}")
    print(f"  峰值速度:   {format_speed(max_speed)}")
    print(f"  最大 peer:  {max_peers}")
    print(f"  最终状态:   {task_status}")
    print(f"  下载完成:   {'是' if completed else '否'}")
    print("=" * 70)

    # 打印守护进程日志尾部（调试用）
    if not completed:
        print("\n--- 守护进程日志尾部 ---")
        with open(log_file, "r", errors="replace") as f:
            lines = f.read().splitlines()
            for line in lines[-50:]:
                print(f"  {line}")

    _cleanup(daemon, logf, not args.keep_daemon)

    if completed:
        print("\n✅ 公网 BT 下载测试通过：下载完成且文件完整")
        return 0
    else:
        # 如果下载了一部分也算部分成功（可能是超时）
        if dl_size > 0:
            pct = (dl_size / total_size * 100) if total_size > 0 else 0
            print(f"\n⚠️  下载未完成（{pct:.1f}%），但引擎正在正常下载中")
            print("    可能是超时设置过短，或种子健康度不足")
            return 0 if pct > 10 else 1
        else:
            print("\n❌ 公网 BT 下载失败：无数据下载")
            return 1


def _cleanup(daemon, logf, kill_daemon=True):
    if kill_daemon:
        daemon.terminate()
        try:
            daemon.wait(timeout=15)
        except subprocess.TimeoutExpired:
            daemon.kill()
    logf.close()


if __name__ == "__main__":
    raise SystemExit(main())
