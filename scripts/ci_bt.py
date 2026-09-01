#!/usr/bin/env python3
"""XferRust CI 本地 BitTorrent 测试基建：生成 .torrent、本地 HTTP tracker、最小 BT seeder。

用于黑盒测试编译产物（xfer / xferrust）的磁力下载能力，全部在本机完成，不依赖公网 peer：

- make-torrent <file> <announce-url> <out.torrent>
    创建单文件种子（SHA-1 逐 piece 校验），把 info 字典的原始字节 + info_hash 打印/保存，
    供后续构造磁力链接与提供 ut_metadata（BEP 9）。
- seed <file> <torrent> --tracker-port T --peer-port P
    同进程启动本地 HTTP tracker 与 BT seeder（扩展握手 + ut_metadata + piece 全流程），
    阻塞运行直到被终止。

设计对齐 crates/xfer-bt/tests/magnet.rs 的 mock seed：纯明文 BT 协议；
引擎默认 adaptive 加密下，MSE 协商失败会自动明文重连，因此无需改动引擎配置。
"""
from __future__ import annotations

import argparse
import hashlib
import os
import socket
import struct
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

PIECE_LEN = 64 * 1024       # 64 KiB
META_PIECE = 16 * 1024      # BEP 9 元数据分片


# ---------------------------------------------------------------------------
# bencode
# ---------------------------------------------------------------------------
def b_encode(v):
    if isinstance(v, int):
        return b"i%de" % v
    if isinstance(v, bytes):
        return b"%d:%s" % (len(v), v)
    if isinstance(v, list):
        return b"l" + b"".join(b_encode(x) for x in v) + b"e"
    if isinstance(v, dict):
        out = bytearray(b"d")
        for k in sorted(v.keys()):
            if isinstance(k, str):
                k = k.encode()
            out += b_encode(k) + b_encode(v[k])
        out += b"e"
        return bytes(out)
    raise TypeError("unsupported bencode type: %r" % type(v))


def _b_decode(data: bytes, i: int):
    c = data[i:i + 1]
    if c == b"i":
        j = data.index(b"e", i)
        return int(data[i + 1:j]), j + 1
    if c == b"l":
        arr, i = [], i + 1
        while data[i:i + 1] != b"e":
            v, i = _b_decode(data, i)
            arr.append(v)
        return arr, i + 1
    if c == b"d":
        d, i = {}, i + 1
        while data[i:i + 1] != b"e":
            k, i = _b_decode(data, i)
            v, i = _b_decode(data, i)
            d[k] = v
        return d, i + 1
    j = data.index(b":", i)
    n = int(data[i:j])
    return data[j + 1:j + 1 + n], j + 1 + n


def b_decode(data: bytes):
    v, i = _b_decode(data, 0)
    assert i == len(data), "trailing bytes in bencode"
    return v


def raw_info_bytes(torrent: bytes) -> bytes:
    """从 .torrent 提取 info 字典的原始字节。

    磁力元数据（BEP 9）必须逐字节一致，因为 info_hash 就是对这段原始字节做 SHA-1。
    """
    assert torrent[:1] == b"d", "torrent 顶层必须是字典"
    i = 1
    while torrent[i:i + 1] != b"e":
        key, i = _b_decode(torrent, i)
        val_start = i
        _, i = _b_decode(torrent, i)
        if key == b"info":
            return torrent[val_start:i]
    raise ValueError("torrent 缺少 info 键")


# ---------------------------------------------------------------------------
# make-torrent
# ---------------------------------------------------------------------------
def cmd_make_torrent(args: argparse.Namespace) -> int:
    data = open(args.file, "rb").read()
    name = os.path.basename(args.file)
    pieces = b"".join(
        hashlib.sha1(data[i:i + PIECE_LEN]).digest()
        for i in range(0, len(data), PIECE_LEN)
    )
    info = {
        b"length": len(data),
        b"name": name.encode(),
        b"piece length": PIECE_LEN,
        b"pieces": pieces,
    }
    info_bytes = b_encode(info)
    info_hash = hashlib.sha1(info_bytes).digest()
    torrent = b_encode({b"announce": args.announce_url.encode(), b"info": info})
    with open(args.out_torrent, "wb") as f:
        f.write(torrent)
    print(info_hash.hex())  # 供 ci_test.py 构造磁力链接
    return 0


# ---------------------------------------------------------------------------
# tracker + seeder
# ---------------------------------------------------------------------------
def _len4(n: int) -> bytes:
    """4 字节大端长度前缀（BEP 3：BT 消息 = 4 字节长度前缀 + 载荷）。"""
    return struct.pack(">I", n)


def start_tracker(port: int, peer_addr: tuple):
    state = {"peer": peer_addr}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            ip, p = state["peer"]
            compact = socket.inet_aton(ip) + struct.pack(">H", p)
            resp = b_encode(
                {b"interval": 60, b"complete": 1, b"incomplete": 0, b"peers": compact}
            )
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(resp)))
            self.end_headers()
            self.wfile.write(resp)

        def log_message(self, *a):  # 静默
            pass

    srv = HTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def cmd_seed(args: argparse.Namespace) -> int:
    data = open(args.file, "rb").read()
    torrent = open(args.torrent, "rb").read()
    info = b_decode(torrent)[b"info"]
    piece_len = info[b"piece length"]
    n_pieces = (len(data) + piece_len - 1) // piece_len
    info_bytes = raw_info_bytes(torrent)
    info_hash = hashlib.sha1(info_bytes).digest()
    peer_id = b"-XF0001-" + b"0" * 12  # 20 字节 azureus 风格

    start_tracker(args.tracker_port, ("127.0.0.1", args.peer_port))

    # 全 1 bitfield（我们拥有全部 piece）
    bitfield = bytearray((n_pieces + 7) // 8)
    for i in range(n_pieces):
        bitfield[i // 8] |= 0x80 >> (i % 8)
    bitfield_msg = _len4(1 + len(bitfield)) + b"\x05" + bytes(bitfield)
    unchoke_msg = _len4(1) + b"\x01"

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", args.peer_port))
    listener.listen(32)

    def _handle_peer(conn, data, piece_len, n_pieces, bitfield_msg, unchoke_msg,
                     info_hash, info_bytes, peer_id):
        conn.settimeout(90)
        first = conn.recv(1)
        if first != b"\x13":
            # 非明文握手（引擎 adaptive 加密会先试 MSE）：忽略，引擎随后明文重连
            return
        rest = b""
        while len(rest) < 67:
            chunk = conn.recv(67 - len(rest))
            if not chunk:
                return
            rest += chunk
        # 布局：19 pstr + 8 reserved + 20 info_hash + 20 peer_id
        if rest[27:47] != info_hash:
            return
        # BT 握手：扩展协议位（reserved byte5 = 0x10）
        reserved = b"\x00\x00\x00\x00\x00\x10\x00\x00"
        conn.sendall(b"\x13BitTorrent protocol" + reserved + info_hash + peer_id)
        conn.sendall(bitfield_msg)
        conn.sendall(unchoke_msg)
        # 扩展握手：声明 ut_metadata=1
        ext_hs = b_encode({b"m": {b"ut_metadata": 1}, b"v": b"xfer-ci-seed"})
        conn.sendall(_len4(2 + len(ext_hs)) + b"\x14\x00" + ext_hs)

        peer_meta_id = 1  # 对端广告的 ut_metadata 扩展 id（BEP 10 回包必须用对端 id）
        while True:
            # BT 消息 = 4 字节长度前缀 + 载荷（首字节为消息 id）
            hdr = b""
            while len(hdr) < 4:
                chunk = conn.recv(4 - len(hdr))
                if not chunk:
                    return
                hdr += chunk
            (mlen,) = struct.unpack(">I", hdr)
            if mlen == 0:
                continue  # keep-alive
            msg = b""
            while len(msg) < mlen:
                chunk = conn.recv(mlen - len(msg))
                if not chunk:
                    return
                msg += chunk
            mid = msg[0]
            if mid == 20:  # extended
                ext_id = msg[1]
                payload = msg[2:]
                if ext_id == 0:
                    d = b_decode(payload)
                    m = d.get(b"m", {})
                    peer_meta_id = m.get(b"ut_metadata", 1)
                else:
                    d = b_decode(payload)
                    if d.get(b"msg_type") == 0:  # metadata request
                        piece = d.get(b"piece", 0)
                        start = piece * META_PIECE
                        seg = info_bytes[start:start + META_PIECE]
                        body = b_encode(
                            {b"msg_type": 1, b"piece": piece,
                             b"total_size": len(info_bytes)}
                        ) + seg
                        conn.sendall(
                            _len4(2 + len(body)) + b"\x14" + bytes([peer_meta_id]) + body
                        )
            elif mid == 6:  # request(index, begin, length)
                idx, begin, length = struct.unpack(">III", msg[1:13])
                off = idx * piece_len + begin
                block = data[off:off + length]
                conn.sendall(
                    _len4(9 + len(block)) + b"\x07"
                    + struct.pack(">II", idx, begin) + block
                )
            # 其余消息（choke/unchoke/interested/have/cancel/port…）忽略

    def handle(conn: socket.socket):
        try:
            try:
                _handle_peer(conn, data, piece_len, n_pieces, bitfield_msg,
                             unchoke_msg, info_hash, info_bytes, peer_id)
            except Exception:  # 线程内异常需显式打印，否则静默吞掉
                import traceback
                traceback.print_exc()
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def accept_loop():
        while True:
            try:
                conn, _ = listener.accept()
            except OSError:
                return
            threading.Thread(target=handle, args=(conn,), daemon=True).start()

    accept_loop()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="XferRust CI 本地 BT 测试基建")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("make-torrent", help="创建单文件种子，输出 info_hash")
    p.add_argument("file")
    p.add_argument("announce_url")
    p.add_argument("out_torrent")
    p.set_defaults(fn=cmd_make_torrent)

    p = sub.add_parser("seed", help="启动本地 tracker + seeder（阻塞）")
    p.add_argument("file")
    p.add_argument("torrent")
    p.add_argument("--tracker-port", type=int, required=True)
    p.add_argument("--peer-port", type=int, required=True)
    p.set_defaults(fn=cmd_seed)

    args = ap.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
