//! 高延迟链路吞吐回归测试（带宽时延积）。
//!
//! 旧实现每连接同一时刻只有一片（256KiB）在途：吞吐 = 在途字节 / RTT，
//! 200ms RTT 下被限死在 ~1.3MB/s，与真实用户「别人十几 MB、自己 1MB」
//! 的症状一致。修复后每连接按流水线窗口多片并行（在途 = pipeline × 16KiB）。
//!
//! 本测试在 seed 与引擎之间插入双向 100ms/块 的延迟代理：
//! - 旧模型必须 16 片串行（每片 ≥1 个请求-响应往返 ≈ 200ms）→ ≥3.2s；
//! - 新模型一个窗口内并行请求所有块 → ~1s 内完成。
//! 断言总耗时落在两者之间，回归到旧模型即失败。

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use xfer_bencode::{bytes, dict, encode, int, parse_torrent};
use xfer_bt::message::{encode_handshake, Message, PeerReader};
use xfer_bt::{TorrentConfig, TorrentEngine};
use xfer_types::{InfoHash, PeerId};

/// 256KiB/片 × 16 片 = 4MiB（与常见真实种子的片尺寸一致）。
const PIECE_LEN: usize = 256 * 1024;
const DATA_LEN: usize = 16 * PIECE_LEN;
/// 代理对每个非空数据块单方向施加的固定延迟。
const LINK_LATENCY: Duration = Duration::from_millis(100);

fn sha1_of(b: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(b);
    h.finalize().into()
}

/// 参考 seed：握手 → Bitfield + Unchoke → 按请求回块（无任何限速）。
async fn serve_seed(listener: TcpListener, data: Arc<Vec<u8>>, info_hash: InfoHash) {
    let peer_id = PeerId::azureus_prefix(&[0xAA; 12]);
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let data = data.clone();
        tokio::spawn(async move {
            let _ = handle_seed(stream, &data, info_hash, peer_id).await;
        });
    }
}

async fn handle_seed(
    mut stream: TcpStream,
    data: &[u8],
    info_hash: InfoHash,
    peer_id: PeerId,
) -> std::io::Result<()> {
    let mut reader = PeerReader::new();
    let hs = loop {
        match reader.read_handshake(&mut stream).await? {
            Some(h) => break h,
            None => continue,
        }
    };
    if hs.info_hash != info_hash {
        return Ok(());
    }
    stream
        .write_all(&encode_handshake(&info_hash, &peer_id))
        .await?;

    let n_pieces = data.len().div_ceil(PIECE_LEN);
    let mut bf = vec![0u8; n_pieces.div_ceil(8)];
    for i in 0..n_pieces {
        bf[i / 8] |= 0x80 >> (i % 8);
    }
    stream.write_all(&Message::Bitfield(bf).encode()).await?;
    stream.write_all(&Message::Unchoke.encode()).await?;

    loop {
        match reader.read_message(&mut stream).await? {
            None => break,
            Some(Message::Request {
                index,
                begin,
                length,
            }) => {
                let off = index as usize * PIECE_LEN + begin as usize;
                let end = (off + length as usize).min(data.len());
                if off >= data.len() {
                    continue;
                }
                stream
                    .write_all(
                        &Message::Piece {
                            index,
                            begin,
                            block: data[off..end].to_vec(),
                        }
                        .encode(),
                    )
                    .await?;
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// 延迟代理：双向各按「每块固定延迟」转发，只加延迟不限带宽。
async fn serve_latency_proxy(listener: TcpListener, target: SocketAddr) {
    loop {
        let Ok((conn, _)) = listener.accept().await else {
            return;
        };
        tokio::spawn(async move {
            let Ok(up) = TcpStream::connect(target).await else {
                return;
            };
            relay(conn, up).await;
        });
    }
}

async fn relay_half<R, W>(mut r: R, mut w: W)
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    // 大缓冲：一次突发尽量合并为一块 → 延迟按往返计而非按字节计
    let mut buf = vec![0u8; 8 * 1024 * 1024];
    loop {
        let Ok(n) = r.read(&mut buf).await else {
            break;
        };
        if n == 0 {
            break;
        }
        tokio::time::sleep(LINK_LATENCY).await;
        if w.write_all(&buf[..n]).await.is_err() {
            break;
        }
    }
    let _ = w.shutdown().await;
}

async fn relay(a: TcpStream, b: TcpStream) {
    let (ar, aw) = a.into_split();
    let (br, bw) = b.into_split();
    tokio::join!(relay_half(ar, bw), relay_half(br, aw));
}

/// 简易 HTTP tracker：返回 compact peers。
async fn tracker_announce(
    Query(_q): Query<std::collections::HashMap<String, String>>,
    State(seed): State<Arc<RwLock<Option<SocketAddr>>>>,
) -> Response {
    let Some(addr) = *seed.read().unwrap() else {
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "no seed",
        )
            .into_response();
    };
    let mut peers = Vec::with_capacity(6);
    if let std::net::IpAddr::V4(v4) = addr.ip() {
        peers.extend_from_slice(&v4.octets());
    } else {
        peers.extend_from_slice(&[127, 0, 0, 1]);
    }
    peers.extend_from_slice(&addr.port().to_be_bytes());
    let resp = dict(BTreeMap::from([
        (b"interval".to_vec(), int(60)),
        (b"complete".to_vec(), int(1)),
        (b"peers".to_vec(), bytes(peers)),
    ]));
    ([(header::CONTENT_TYPE, "text/plain")], encode(&resp)).into_response()
}

#[tokio::test]
async fn latency_link_throughput_requires_multipiece_window() {
    let data: Vec<u8> = (0..DATA_LEN).map(|i| (i % 251) as u8).collect();

    // 1. tracker（先启动，种子文件需要其 URL）
    let state: Arc<RwLock<Option<SocketAddr>>> = Arc::new(RwLock::new(None));
    let app = Router::new()
        .route("/announce", get(tracker_announce))
        .with_state(state.clone());
    let tl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let taddr = tl.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(tl, app).await;
    });

    // 2. 种子文件（真实 piece 哈希）→ info_hash
    let tracker_url = format!("http://{taddr}/announce");
    let pieces: Vec<u8> = data.chunks(PIECE_LEN).flat_map(sha1_of).collect();
    let info = dict(BTreeMap::from([
        (b"name".to_vec(), bytes("data.bin")),
        (b"piece length".to_vec(), int(PIECE_LEN as i64)),
        (b"length".to_vec(), int(data.len() as i64)),
        (b"pieces".to_vec(), bytes(pieces)),
    ]));
    let top = dict(BTreeMap::from([
        (b"announce".to_vec(), bytes(tracker_url.clone())),
        (b"info".to_vec(), info),
    ]));
    let tb = encode(&top);
    let meta = parse_torrent(&tb).unwrap();
    let info_hash = InfoHash::from_bytes(&meta.info_hash);

    // 3. seed + 延迟代理（引擎只经代理访问 seed）
    let sl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seed_addr = sl.local_addr().unwrap();
    tokio::spawn(serve_seed(sl, Arc::new(data.clone()), info_hash));

    let pl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = pl.local_addr().unwrap();
    tokio::spawn(serve_latency_proxy(pl, seed_addr));

    *state.write().unwrap() = Some(proxy_addr);

    // 4. 引擎下载并计时
    let dir = std::env::temp_dir().join(format!("e2e-latency-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = TorrentConfig {
        dir: dir.clone(),
        peer_id: PeerId::azureus_prefix(&[0xBB; 12]),
        listen_port: 0,
        max_peers: 8,
        adaptive: false,
        numwant: 50,
        announce_urls: vec![tracker_url],
        udp_announce_urls: Vec::new(),
        pipeline: 0,
        enable_dht: false,
        dht_port: 0,
        encryption: xfer_bt::EncryptionMode::PlaintextOnly,
        // 测的是多片在途窗口的吞吐，seed 为纯 TCP：
        // 默认 TcpAndUtp 会先等 5s uTP 拨号超时再回退 TCP，污染计时。
        bt_protocol: xfer_bt::BtProtocol::TcpOnly,
        download_limit: 0,
        upload_limit: 0,
        seed_mode: false,
        seed_duration: 0,
        seed_ratio: 0.0,
        selected_files: None,
    };
    let engine = TorrentEngine::new(meta, cfg).unwrap();
    let start = Instant::now();
    let r = tokio::time::timeout(Duration::from_secs(30), engine.clone().run(CancellationToken::new()))
        .await
        .expect("下载超时（30s）");
    r.expect("下载失败");
    let elapsed = start.elapsed();

    // 文件逐字节一致
    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data);
    let _ = std::fs::remove_dir_all(&dir);

    // 延迟代理必须真实生效：至少一个「请求→数据」往返（双向各 ≥100ms）
    assert!(
        elapsed > Duration::from_millis(300),
        "完成过快（{elapsed:?}）：延迟代理可能未生效，测试失去意义"
    );
    // 旧单片模型需 16 片串行 × ≥200ms 往返 ≈ ≥3.2s；
    // 多片窗口模型 ~1s 完成。阈值取 2.5s 隔离两者。
    assert!(
        elapsed < Duration::from_millis(2500),
        "4MiB @ 100ms 链路耗时 {elapsed:?}：在途窗口不足，吞吐被带宽时延积限死（回归到单片模型）"
    );
}
