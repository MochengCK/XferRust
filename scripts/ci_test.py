#!/usr/bin/env python3
"""XferRust CI 黑盒功能测试：验证编译产物在各平台可运行 + 下载能力。

覆盖（全部针对已构建的二进制，非源码单测）：
1. 版本冒烟        —— xfer / xferrust --version 退出码 0 且含版本号
2. 引擎可运行      —— 启动 xferrust 守护进程，经 WS RPC 探活（engine runs）
3. HTTP 下载       —— 本地静态服务器 + 下载后 SHA-256 逐字节校验
4. HTTPS 下载      —— 外部 TLS 源（需为公开稳定文件，经 --https-url 传入），与 urllib 拉取结果校验
5. 磁力下载        —— 本地 tracker + seeder（scripts/ci_bt.py），ut_metadata + piece 全流程

用法：
  python3 scripts/ci_test.py --xfer <path> --xferrust <path> --out <dir> \
      --token <secret> --https-url <https-url>

任一用例失败即整体失败（退出码 1），供 CI 严格门禁。
"""
from __future__ import annotations

import argparse
import functools
import hashlib
import http.server
import os
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

# Windows 控制台默认用 cp1252 编码，无法输出中文（测试名/引擎中文日志），
# 强制 UTF-8 并把无法编码的字符替换为占位符，保证永不因编码崩溃。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass


def bin_path(base: str) -> str:
    """Windows 下自动补 .exe 后缀。"""
    if os.name == "nt" and not base.lower().endswith(".exe"):
        exe = base + ".exe"
        if os.path.exists(exe):
            return exe
    return base


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def run(cmd, timeout=60, cwd=None):
    # Windows 默认按 cp1252 解码子进程输出，引擎中文日志会导致 UnicodeDecodeError；
    # 统一按 UTF-8 解码，非法字节替换为占位符。
    return subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8",
        errors="replace", timeout=timeout, cwd=cwd,
    )


def wait_until(fn, timeout, interval=1.0, desc=""):
    """轮询直到 fn() 返回真值；超时抛 AssertionError。"""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = fn()
        except Exception as e:  # noqa: BLE001
            last = e
        if last is True:
            return
        if isinstance(last, Exception):
            time.sleep(interval)
            continue
        time.sleep(interval)
    if isinstance(last, Exception):
        raise AssertionError(f"超时等待 {desc}（最近错误: {last}）")
    raise AssertionError(f"超时等待 {desc}")


# ---------------------------------------------------------------------------
# 用例基座
# ---------------------------------------------------------------------------
class Tester:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.xfer = bin_path(args.xfer)
        self.xferrust = bin_path(args.xferrust)
        self.token = args.token
        self.port = args.rpc_port
        self.url = f"ws://127.0.0.1:{self.port}/jsonrpc"
        self.results = []  # (name, ok, detail)

    # -- RPC 封装 --
    def rpc(self, sub: str, extra=None, timeout=60):
        cmd = [self.xfer, sub]
        if extra:
            cmd += extra
        cmd += ["--token", self.token, "--connect", self.url]
        return run(cmd, timeout=timeout)

    def add(self, target, out_dir, out=None, timeout=60):
        extra = ["-d", out_dir]
        if out:
            extra += ["-o", out]
        r = self.rpc("add", [target] + extra, timeout=timeout)
        if r.returncode != 0:
            raise AssertionError(f"task.add 失败: {r.stderr.strip() or r.stdout.strip()}")
        return r.stdout.strip()  # gid

    # -- 结果 --
    def ok(self, name, detail=""):
        self.results.append((name, True, detail))
        print(f"[PASS] {name}" + (f" — {detail}" if detail else ""))

    def fail(self, name, detail):
        self.results.append((name, False, detail))
        print(f"[FAIL] {name} — {detail}")

    def run_case(self, name, fn):
        try:
            detail = fn()
            self.ok(name, detail or "")
        except Exception as e:  # noqa: BLE001
            self.fail(name, str(e))

    def summary(self) -> int:
        failed = [r for r in self.results if not r[1]]
        print("\n===== 测试汇总 =====")
        for name, ok, _ in self.results:
            print(f"  {'PASS' if ok else 'FAIL'}  {name}")
        if failed:
            print(f"\n{len(failed)} 项失败 → CI 门禁不通过")
            return 1
        print("\n全部通过")
        return 0


# ---------------------------------------------------------------------------
# 用例
# ---------------------------------------------------------------------------
def case_version(t: Tester):
    """版本冒烟：两个二进制 --version 均退出 0 且输出含版本号。"""
    for exe, label in ((t.xferrust, "xferrust"), (t.xfer, "xfer")):
        r = run([exe, "--version"], timeout=30)
        assert r.returncode == 0, f"{label} --version 退出码 {r.returncode}: {r.stderr}"
        assert re.search(r"\d+\.\d+\.\d+", r.stdout), f"{label} --version 未输出版本号: {r.stdout!r}"
    return "xfer / xferrust 均可执行且输出版本号"


def case_engine_rpc(t: Tester):
    """引擎可运行：RPC 探活 + 全局统计。"""
    r = t.rpc("stat")
    assert r.returncode == 0, f"xfer stat 失败: {r.stderr.strip() or r.stdout.strip()}"
    assert "速度" in r.stdout or "speed" in r.stdout.lower() or "B/s" in r.stdout, \
        f"stat 输出异常: {r.stdout!r}"
    return "守护进程 RPC 探活成功"


def case_http(t: Tester, work: str):
    """HTTP 下载：本地静态服务器 + SHA-256 校验。"""
    size = 4 * 1024 * 1024
    src = os.path.join(work, "http-src.bin")
    # 确定性伪随机内容，方便复现
    with open(src, "wb") as f:
        chunk = bytes((i * 31 + 7) % 256 for i in range(65536))
        for _ in range(size // len(chunk)):
            f.write(chunk)
        f.write(chunk[: size % len(chunk)])
    http_port = free_port()
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler, directory=work
    )
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", http_port), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        out_dir = os.path.join(work, "http-out")
        os.makedirs(out_dir, exist_ok=True)
        t.add(f"http://127.0.0.1:{http_port}/http-src.bin", out_dir, out="http-dst.bin")

        dst = os.path.join(out_dir, "http-dst.bin")

        wait_until(lambda: os.path.exists(dst) and os.path.getsize(dst) == size,
                   timeout=180, desc="HTTP 下载完成")
        assert sha256_file(src) == sha256_file(dst), "HTTP 下载文件与源不一致"
    finally:
        srv.shutdown()
    return "HTTP 下载成功且 SHA-256 一致"


def case_https(t: Tester, work: str):
    """HTTPS 下载：外部 TLS 源，与 urllib 拉取结果 SHA-256 一致。"""
    url = t.args.https_url
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            expected = resp.read()
    except urllib.error.URLError as e:
        raise AssertionError(f"无法访问 HTTPS 源 {url}: {e}") from e
    out_dir = os.path.join(work, "https-out")
    os.makedirs(out_dir, exist_ok=True)
    t.add(url, out_dir, out="https-dst.bin")

    dst = os.path.join(out_dir, "https-dst.bin")

    def done():
        return os.path.exists(dst) and os.path.getsize(dst) == len(expected)

    wait_until(done, timeout=240, desc="HTTPS 下载完成")
    with open(dst, "rb") as f:
        assert hashlib.sha256(f.read()).hexdigest() == hashlib.sha256(expected).hexdigest(), \
            "HTTPS 下载文件与源不一致"
    return f"HTTPS({url.split('/')[2]}) 下载成功且 SHA-256 一致"


def case_magnet(t: Tester, work: str, script: str):
    """磁力下载：本地 tracker + seeder，经 ut_metadata 获取元数据后完成下载。"""
    size = 2 * 1024 * 1024
    src = os.path.join(work, "magnet-src.bin")
    with open(src, "wb") as f:
        chunk = bytes((i * 17 + 3) % 256 for i in range(65536))
        for _ in range(size // len(chunk)):
            f.write(chunk)
        f.write(chunk[: size % len(chunk)])

    tracker_port = free_port()
    peer_port = free_port()
    torrent = os.path.join(work, "magnet.torrent")
    announce = f"http://127.0.0.1:{tracker_port}/announce"

    r = run([sys.executable, script, "make-torrent", src, announce, torrent], timeout=60)
    assert r.returncode == 0, f"make-torrent 失败: {r.stderr}"
    info_hash = r.stdout.strip()

    seed = subprocess.Popen(
        [sys.executable, script, "seed", src, torrent,
         "--tracker-port", str(tracker_port), "--peer-port", str(peer_port)],
        stdout=subprocess.DEVNULL,
        stderr=open(os.path.join(work, "ci_bt_seed.err"), "wb"),
    )
    try:
        time.sleep(1.0)  # 等 tracker/seeder 就绪
        magnet = (f"magnet:?xt=urn:btih:{info_hash}"
                  f"&dn=magnet-src.bin&tr={urllib.parse.quote(announce, safe='')}")
        out_dir = os.path.join(work, "magnet-out")
        os.makedirs(out_dir, exist_ok=True)
        t.add(magnet, out_dir)
        dst = os.path.join(out_dir, "magnet-src.bin")

        # 等待磁力下载完成（文件就位且大小一致）
        def done():
            return os.path.exists(dst) and os.path.getsize(dst) == size

        wait_until(done, timeout=300, desc="磁力下载完成")
        assert sha256_file(src) == sha256_file(dst), "磁力下载文件与源不一致"
    finally:
        # SIGTERM 在 accept() 阻塞期间被 Python 延迟处理，必须用 SIGKILL 兜底
        seed.kill()
        try:
            seed.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
    return "磁力（ut_metadata）下载成功且 SHA-256 一致"


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="XferRust CI 黑盒功能测试")
    ap.add_argument("--xfer", required=True, help="xfer(TUI) 二进制路径")
    ap.add_argument("--xferrust", required=True, help="xferrust(引擎内核) 二进制路径")
    ap.add_argument("--out", required=True, help="工作目录（生成测试数据/日志）")
    ap.add_argument("--token", default="ci", help="RPC secret")
    ap.add_argument("--rpc-port", type=int, default=0, help="守护进程 RPC 端口（默认随机）")
    ap.add_argument("--https-url", required=True, help="HTTPS 下载测试源 URL")
    ap.add_argument("--keep-daemon", action="store_true", help="测试结束后不杀守护进程（调试用）")
    args = ap.parse_args()

    work = args.out
    if not args.rpc_port:
        args.rpc_port = free_port()
    os.makedirs(work, exist_ok=True)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    bt_script = os.path.join(script_dir, "ci_bt.py")

    t = Tester(args)
    log = os.path.join(work, "daemon.log")
    logf = open(log, "wb", buffering=0)
    daemon = subprocess.Popen(
        [t.xferrust, f"--rpc-listen-port={args.rpc_port}",
         f"--rpc-secret={t.token}", f"--dir={work}"],
        stdout=logf, stderr=subprocess.STDOUT,
    )
    try:
        wait_until(
            lambda: t.rpc("stat").returncode == 0,
            timeout=30, desc="守护进程 RPC 就绪",
        )
        t.run_case("版本冒烟", lambda: case_version(t))
        t.run_case("引擎可运行(RPC)", lambda: case_engine_rpc(t))
        t.run_case("HTTP 下载", lambda: case_http(t, work))
        t.run_case("HTTPS 下载", lambda: case_https(t, work))
        t.run_case("磁力下载", lambda: case_magnet(t, work, bt_script))
    finally:
        if not args.keep_daemon:
            daemon.terminate()
            try:
                daemon.wait(timeout=15)
            except subprocess.TimeoutExpired:
                daemon.kill()
        logf.close()
        if daemon.poll() is not None:
            tail = open(log, "rb").read().decode(errors="replace")
            if "Traceback" in tail or "panic" in tail.lower():
                print("--- daemon.log 尾部（含异常）---")
                print("\n".join(tail.splitlines()[-30:]))
    return t.summary()


if __name__ == "__main__":
    raise SystemExit(main())
