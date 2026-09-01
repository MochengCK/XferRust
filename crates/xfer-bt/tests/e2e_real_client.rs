//! 真实客户端互操作验收测试（PLAN §8 金标准）。
//!
//! 用真实 libtorrent（qBittorrent/Deluge 的底层引擎）实例做种，XferRust
//! 作为下载方端到端下载。与仿真种子的本质区别：真实客户端会以真实实现
//! 校验并拒绝不合规的 request（>16KiB 块）、运行真实 choking 算法、
//! 完整 BEP10/Fast Extension 协商——任何协议偏差都会在这里暴露，
//! 而自洽仿真测试发现不了（§7 教训 10/11）。
//!
//! 依赖：`python3` + `libtorrent`（`pip3 install libtorrent`）。
//! 依赖缺失时测试打印指引并跳过（不失败），避免无依赖环境被阻塞；
//! 有依赖时必须完整下载成功。

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use xfer_bencode::parse_torrent;
use xfer_bt::{TorrentConfig, TorrentEngine};
use xfer_types::PeerId;

async fn tracker_announce(
    Query(_q): Query<HashMap<String, String>>,
    State(state): State<Arc<RwLock<Vec<SocketAddr>>>>,
) -> Response {
    let addrs = state.read().unwrap().clone();
    let mut peers = Vec::new();
    for addr in &addrs {
        if let std::net::IpAddr::V4(v4) = addr.ip() {
            peers.extend_from_slice(&v4.octets());
            peers.extend_from_slice(&addr.port().to_be_bytes());
        }
    }
    let resp = xfer_bencode::dict(BTreeMap::from([
        (b"interval".to_vec(), xfer_bencode::int(60)),
        (b"complete".to_vec(), xfer_bencode::int(addrs.len() as i64)),
        (b"peers".to_vec(), xfer_bencode::bytes(peers)),
    ]));
    ([(header::CONTENT_TYPE, "text/plain")], xfer_bencode::encode(&resp)).into_response()
}

async fn start_tracker() -> (SocketAddr, Arc<RwLock<Vec<SocketAddr>>>) {
    let state: Arc<RwLock<Vec<SocketAddr>>> = Arc::new(RwLock::new(Vec::new()));
    let app = Router::new()
        .route("/announce", get(tracker_announce))
        .with_state(state.clone());
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(l, app).await;
    });
    (addr, state)
}

fn probe_libtorrent() -> bool {
    std::process::Command::new("python3")
        .args(["-c", "import libtorrent"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// 金标准：从真实 libtorrent seeder 完整下载。
///
/// 修复前（64KB 请求）此测试超时失败——libtorrent 拒绝超过 16KiB 的
/// request，引擎有节点无速度；修复后（16KiB 标准块）应完整下载成功。
#[tokio::test]
async fn download_from_real_libtorrent_seeder() {
    if !probe_libtorrent() {
        eprintln!(
            "跳过：未安装 python3 libtorrent 绑定（pip3 install libtorrent）。\
             安装后本测试将以真实客户端做金标准验收。"
        );
        return;
    }

    let base = std::env::temp_dir().join(format!("e2e-realclient-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let dl_dir = base.join("dl");
    std::fs::create_dir_all(&dl_dir).unwrap();

    // 3MB 数据（256KB piece → 12 片，多块流水线可充分展开）
    let data: Vec<u8> = (0..(3 * 1024 * 1024u64)).map(|i| (i.wrapping_mul(7) % 251) as u8).collect();
    let data_file = base.join("seed.bin");
    std::fs::write(&data_file, &data).unwrap();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let port_file = base.join("seeder.port");
    let torrent_file = base.join("real.torrent");

    // 启动真实 libtorrent seeder
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tools")
        .join("interop_seeder.py");
    let mut child = tokio::process::Command::new("python3")
        .arg(&script)
        .arg("--data")
        .arg(&data_file)
        .arg("--tracker")
        .arg(&tracker_url)
        .arg("--port-file")
        .arg(&port_file)
        .arg("--torrent-file")
        .arg(&torrent_file)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("启动 interop_seeder.py 失败");

    // 等待 seeder 就绪（端口文件 + 真实 .torrent 文件出现）
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let seeder_port: u16 = loop {
        if let Ok(content) = std::fs::read_to_string(&port_file) {
            if let Ok(p) = content.trim().parse::<u16>() {
                if torrent_file.exists() {
                    break p;
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            let _ = child.kill().await;
            panic!("libtorrent seeder 30s 内未就绪");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    *seed_ref.write().unwrap() = vec![SocketAddr::from(([127, 0, 0, 1], seeder_port))];

    // 使用 seeder 生成的真实 .torrent 驱动引擎（元信息零偏差）
    let tb = std::fs::read(&torrent_file).unwrap();
    let meta = parse_torrent(&tb).expect("解析真实 .torrent 失败");

    let cfg = TorrentConfig {
        dir: dl_dir.clone(),
        peer_id: PeerId::azureus_prefix(&[5u8; 12]),
        listen_port: 0,
        max_peers: 8,
        adaptive: false,
        numwant: 50,
        announce_urls: vec![tracker_url],
        pipeline: 0,
        udp_announce_urls: Vec::new(),
        enable_dht: false,
        dht_port: 0,
        encryption: xfer_bt::EncryptionMode::PlaintextOnly,
        bt_protocol: xfer_bt::BtProtocol::TcpOnly,
        download_limit: 0,
        upload_limit: 0,
        seed_mode: false,
        seed_duration: 0,
    };
    let engine = TorrentEngine::new(meta, cfg).unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(90),
        engine.clone().run(CancellationToken::new()),
    )
    .await;

    let _ = child.kill().await;

    let inner = match result {
        Err(_) => panic!("从真实 libtorrent seeder 下载超时（有节点无速度）"),
        Ok(r) => r,
    };
    inner.expect("从真实 libtorrent seeder 下载应成功");

    let out = std::fs::read(dl_dir.join("seed.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&base);
}
