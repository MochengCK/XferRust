#!/usr/bin/env python3
"""真实客户端 seeder：libtorrent（qBittorrent/Deluge 的底层引擎）。

用于 XferRust 的真实客户端互操作验收测试（PLAN §8 金标准）。与仿真种子
不同，这里跑的是完整真实实现：真实的 choking 算法、真实的请求校验
（超过 16KiB 的 request 被拒绝）、真实的 BEP10 / Fast Extension 行为。

用法:
    python3 interop_seeder.py --data <文件> --tracker <url> \
        --port-file <路径> --torrent-file <路径>

行为: 以 <文件> 创建真实种子（256KB piece），把 .torrent 写入 <种子文件>，
启动 libtorrent session 做种，把监听端口写入 <端口文件>，之后常驻直到被杀死。

依赖: pip3 install libtorrent
"""

import argparse
import os
import time


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="要做种的数据文件")
    ap.add_argument("--tracker", required=True, help="tracker announce URL")
    ap.add_argument("--port-file", required=True, help="监听端口输出文件")
    ap.add_argument("--torrent-file", required=True, help=".torrent 输出文件")
    args = ap.parse_args()

    import libtorrent as lt

    data_path = os.path.abspath(args.data)
    data_dir = os.path.dirname(data_path)
    size = os.path.getsize(data_path)

    # 1) 构造真实 .torrent（标准结构，256KB piece）
    fs = lt.file_storage()
    fs.add_file(os.path.basename(data_path), size)
    ct = lt.create_torrent(fs, piece_size=256 * 1024)
    lt.set_piece_hashes(ct, data_dir)
    ct.set_tracker(args.tracker)
    torrent_bytes = lt.bencode(ct.generate())
    with open(args.torrent_file, "wb") as f:
        f.write(torrent_bytes)
    ti = lt.torrent_info(torrent_bytes)

    # 2) 启动真实 session：DHT/LSD/UPnP 全关（本地环回环境），
    #    其余全部保留真实实现行为。
    ses = lt.session(
        {
            "listen_interfaces": "127.0.0.1:0",
            "enable_dht": False,
            "enable_lsd": False,
            "enable_natpmp": False,
            "enable_upnp": False,
        }
    )

    atp = lt.add_torrent_params()
    atp.ti = ti
    atp.save_path = data_dir
    atp.trackers = [args.tracker]
    handle = ses.add_torrent(atp)
    handle.set_flags(lt.torrent_flags.seed_mode)

    # 3) 等待监听端口就绪后写出（Rust 侧测试通过 tracker 分发该地址）
    deadline = time.time() + 30
    port = 0
    while time.time() < deadline:
        port = ses.listen_port()
        if port:
            break
        time.sleep(0.1)
    if not port:
        raise SystemExit("libtorrent 监听端口未就绪")
    with open(args.port_file, "w") as f:
        f.write(str(port))

    print(f"seeder ready: 127.0.0.1:{port}", flush=True)
    # 4) 常驻做种，直到被杀
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
