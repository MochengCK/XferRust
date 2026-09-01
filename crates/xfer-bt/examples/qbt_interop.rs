//! 真实客户端互通验收台：本引擎 ↔ qBittorrent（libtorrent）。
//!
//! 两种模式（由外部脚本驱动，人工亦可单跑）：
//! - `seed`：本引擎做种（MSE 开启）+ 内置双向 tracker，打印
//!   `READY torrent=... source=...` 后持续打印对端状态（含 `client`
//!   与 `encrypted` 标记），用于验证 qBittorrent 主动拨入的加密协商；
//! - `leech`：本引擎从 tracker 学到 qBittorrent 地址后主动拨入，
//!   下载完成后校验内容并输出 `LEECH_OK encrypted_observed=...`。
//!
//! 用法：
//!   cargo run -p xfer-bt --example qbt_interop -- seed \
//!       --tracker-port 18219 --torrent /tmp/qbt.torrent --source /tmp/qbt_src.bin
//!   cargo run -p xfer-bt --example qbt_interop -- leech \
//!       --torrent /tmp/qbt.torrent --dir /tmp/qbt_leech

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use sha1::{Digest, Sha1};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use xfer_bencode::{bytes, dict, encode, int, parse_torrent, TorrentMeta};
use xfer_bt::{TorrentConfig, TorrentEngine};
use xfer_types::PeerId;

const PIECE_LEN: usize = 64 * 1024;
const DATA_LEN: usize = 1024 * 1024;

fn data_pattern() -> Vec<u8> {
    (0..DATA_LEN).map(|i| ((i * 7) % 251) as u8).collect()
}

fn sha1_of(b: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(b);
    h.finalize().into()
}

fn make_torrent_bytes(data: &[u8], tracker_url: &str) -> Vec<u8> {
    let pieces: Vec<u8> = data.chunks(PIECE_LEN).flat_map(sha1_of).collect();
    let info = dict(BTreeMap::from([
        (b"name".to_vec(), bytes("data.bin")),
        (b"piece length".to_vec(), int(PIECE_LEN as i64)),
        (b"length".to_vec(), int(data.len() as i64)),
        (b"pieces".to_vec(), bytes(pieces)),
    ]));
    let top = dict(BTreeMap::from([
        (b"announce".to_vec(), bytes(tracker_url)),
        (b"info".to_vec(), info),
    ]));
    encode(&top)
}

/// 双向 mini tracker：记录每个 announce 者，向请求者返回其他所有对端。
async fn start_tracker(port: u16) -> Arc<RwLock<Vec<SocketAddr>>> {
    let state: Arc<RwLock<Vec<SocketAddr>>> = Arc::new(RwLock::new(Vec::new()));

    async fn announce(
        Query(q): Query<HashMap<String, String>>,
        ConnectInfo(src): ConnectInfo<SocketAddr>,
        State(state): State<Arc<RwLock<Vec<SocketAddr>>>>,
    ) -> Response {
        let port = q
            .get("port")
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(src.port());
        let me = SocketAddr::from((std::net::Ipv4Addr::new(127, 0, 0, 1), port));
        let others: Vec<SocketAddr> = {
            let mut st = state.write().unwrap();
            if !st.contains(&me) {
                st.push(me);
            }
            st.iter().copied().filter(|a| *a != me).collect()
        };
        let mut peers = Vec::with_capacity(others.len() * 6);
        for a in &others {
            peers.extend_from_slice(&[127, 0, 0, 1]);
            peers.extend_from_slice(&a.port().to_be_bytes());
        }
        let resp = dict(BTreeMap::from([
            (b"interval".to_vec(), int(3)),
            (b"complete".to_vec(), int(1)),
            (b"peers".to_vec(), bytes(peers)),
        ]));
        ([(header::CONTENT_TYPE, "text/plain")], encode(&resp)).into_response()
    }

    async fn stats(State(state): State<Arc<RwLock<Vec<SocketAddr>>>>) -> String {
        format!("{{\"peers\":{}}}", state.read().unwrap().len())
    }

    let app = Router::new()
        .route("/announce", get(announce))
        .route("/stats", get(stats))
        .with_state(state.clone());
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port)))
        .await
        .unwrap_or_else(|e| panic!("tracker 端口 {port} 绑定失败: {e}"));
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await;
    });
    state
}

fn make_config(
    meta: &TorrentMeta,
    dir: &std::path::Path,
    seed_mode: bool,
    upload_limit: u64,
) -> TorrentConfig {
    TorrentConfig {
        dir: dir.to_path_buf(),
        peer_id: PeerId::azureus_prefix(&[0xAB; 12]),
        listen_port: 0,
        max_peers: 8,
        adaptive: false,
        numwant: 50,
        announce_urls: meta
            .announce
            .iter()
            .cloned()
            .chain(meta.announce_list.iter().flat_map(|t| t.iter().cloned()))
            .collect(),
        udp_announce_urls: Vec::new(),
        pipeline: 0,
        enable_dht: false,
        dht_port: 0,
        encryption: xfer_bt::EncryptionMode::PreferEncryption,
        bt_protocol: xfer_bt::BtProtocol::default(),
        download_limit: 0,
        upload_limit,
        seed_mode,
        seed_duration: 0,
    }
}

fn arg_value(args: &[String], key: &str) -> Option<PathBuf> {
    args.iter()
        .position(|a| a == key)
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
}

async fn run_tracker(args: Vec<String>) {
    let tracker_port: u16 = arg_value(&args, "--tracker-port")
        .and_then(|p| p.to_str().unwrap().parse().ok())
        .unwrap_or(18219);
    start_tracker(tracker_port).await;
    println!("TRACKER_READY port={tracker_port}");
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

async fn run_seed(args: Vec<String>) {
    let tracker_port: u16 = arg_value(&args, "--tracker-port")
        .and_then(|p| p.to_str().unwrap().parse().ok())
        .unwrap_or(18219);
    let torrent_out = arg_value(&args, "--torrent").expect("seed 需要 --torrent");
    let source_out = arg_value(&args, "--source").expect("seed 需要 --source");

    if !args.iter().any(|a| a == "--no-tracker") {
        start_tracker(tracker_port).await;
    }
    let data = data_pattern();
    std::fs::write(&source_out, &data).unwrap();
    let tracker_url = format!("http://127.0.0.1:{tracker_port}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    std::fs::write(&torrent_out, &tb).unwrap();

    let seed_dir = std::env::temp_dir().join(format!("xfer-qbt-seed-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&seed_dir);
    std::fs::create_dir_all(&seed_dir).unwrap();
    std::fs::write(seed_dir.join("data.bin"), &data).unwrap();

    let meta = parse_torrent(&tb).unwrap();
    let upload_limit: u64 = arg_value(&args, "--upload-limit")
        .and_then(|p| p.to_str().unwrap().parse().ok())
        .unwrap_or(0);
    let cfg = make_config(&meta, &seed_dir, true, upload_limit);
    let engine = TorrentEngine::new(meta, cfg).unwrap();
    let cancel = CancellationToken::new();
    let e2 = engine.clone();
    let c2 = cancel.clone();
    tokio::spawn(async move {
        let _ = e2.run(c2).await;
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    let listen = loop {
        let port = engine.listen_port();
        if port != 0 {
            break port;
        }
        assert!(Instant::now() < deadline, "seed 监听端口未在 5s 内就绪");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    println!(
        "READY tracker_port={tracker_port} listen={listen} torrent={} source={}",
        torrent_out.display(),
        source_out.display()
    );
    let mut ever_encrypted = false;
    let mut ever_saw_qbt = false;
    let mut tick = 0u32;
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let peers = engine.peers_info();
        if peers.iter().any(|p| p.connected && p.encrypted) {
            ever_encrypted = true;
        }
        if peers.iter().any(|p| p.connected && p.client.contains("qBittorrent")) {
            ever_saw_qbt = true;
        }
        tick += 1;
        if tick % 10 == 0 {
            let connected = peers.iter().filter(|p| p.connected).count();
            let encrypted = peers.iter().filter(|p| p.connected && p.encrypted).count();
            let clients: Vec<String> = peers.iter().map(|p| p.client.clone()).collect();
            println!(
                "STATUS peers={connected} encrypted={encrypted} uploaded={} \
                 ever_encrypted={ever_encrypted} ever_saw_qbt={ever_saw_qbt} clients={clients:?}",
                engine.uploaded()
            );
        }
    }
}

async fn run_leech(args: Vec<String>) {
    let torrent_in = arg_value(&args, "--torrent").expect("leech 需要 --torrent");
    let out_dir = arg_value(&args, "--dir").expect("leech 需要 --dir");
    let _ = std::fs::remove_dir_all(&out_dir);
    std::fs::create_dir_all(&out_dir).unwrap();

    let tb = std::fs::read(&torrent_in).unwrap();
    let meta = parse_torrent(&tb).unwrap();

    // 从种子的 announce URL 解析 tracker 端口并本地起一个，保证双方可互相发现
    // （方向 B：seed 进程及其 tracker 已被停掉，leech 必须自带 tracker）。
    let tracker_port = meta
        .announce
        .as_ref()
        .and_then(|u| {
            u.rsplit_once(':')
                .and_then(|(_, rest)| rest.split('/').next()?.parse::<u16>().ok())
        })
        .expect("torrent 缺少可解析的 announce 端口");
    if !args.iter().any(|a| a == "--no-tracker") {
        start_tracker(tracker_port).await;
    }

    let download_limit: u64 = arg_value(&args, "--download-limit")
        .and_then(|p| p.to_str().unwrap().parse().ok())
        .unwrap_or(0);
    let mut cfg = make_config(&meta, &out_dir, false, 0);
    cfg.download_limit = download_limit;
    let engine = TorrentEngine::new(meta, cfg).unwrap();
    let e2 = engine.clone();
    let run = tokio::spawn(async move { e2.run(CancellationToken::new()).await });

    let mut saw_encrypted = false;
    let deadline = Instant::now() + Duration::from_secs(120);
    let done = loop {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let peers = engine.peers_info();
        if peers.iter().any(|p| p.connected && p.encrypted) {
            saw_encrypted = true;
        }
        let clients: Vec<String> = peers
            .iter()
            .filter(|p| p.connected)
            .map(|p| format!("{}(enc={})", p.client, p.encrypted))
            .collect();
        println!(
            "PROGRESS uploaded={} downloaded={} peers={clients:?} encrypted_seen={saw_encrypted}",
            engine.uploaded(),
            peers.iter().map(|p| p.downloaded).sum::<u64>()
        );
        if run.is_finished() {
            break run.await.unwrap().is_ok();
        }
        assert!(Instant::now() < deadline, "leech 超时");
    };

    let content = std::fs::read(out_dir.join("data.bin")).unwrap_or_default();
    let ok = done && content == data_pattern();
    println!(
        "{} encrypted_observed={saw_encrypted}",
        if ok { "LEECH_OK" } else { "LEECH_FAIL" }
    );
    if !ok {
        std::process::exit(1);
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("tracker") => run_tracker(args).await,
        Some("seed") => run_seed(args).await,
        Some("leech") => run_leech(args).await,
        _ => {
            eprintln!("用法: qbt_interop tracker|seed|leech --torrent <path> ...");
            std::process::exit(2);
        }
    }
}
