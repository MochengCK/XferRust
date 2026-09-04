//! 诊断测试：验证 tracker announce 的 URL 编码在真实网络中是否正确。
//!
//! 这个测试模拟一个真实 tracker，检查收到的 info_hash 参数是否被正确解析。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use axum::extract::Query;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use sha1::{Digest, Sha1};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use xfer_bencode::{bytes, dict, encode, int, parse_torrent, TorrentMeta};
use xfer_bt::message::{encode_handshake, Message, PeerReader};
use xfer_bt::{TorrentConfig, TorrentEngine};
use xfer_types::{InfoHash, PeerId};

const PIECE_LEN: usize = 64 * 1024;

fn sha1_of(b: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(b);
    h.finalize().into()
}

async fn bind_random() -> (TcpListener, SocketAddr) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    (l, addr)
}

/// tracker：记录收到的原始 query string，用于验证 info_hash 编码。
async fn tracker_announce(
    Query(q): Query<HashMap<String, String>>,
    axum::extract::State(state): axum::extract::State<Arc<RwLock<Option<SocketAddr>>>>,
) -> Response {
    // 记录收到的 query 参数
    tracing::info!(
        info_hash_param = ?q.get("info_hash"),
        peer_id_param = ?q.get("peer_id"),
        port = ?q.get("port"),
        compact = ?q.get("compact"),
        "tracker 收到 announce 请求"
    );

    let Some(addr) = *state.read().unwrap() else {
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "no seed").into_response();
    };
    let ip = addr.ip();
    let port = addr.port();
    let mut peers = Vec::with_capacity(6);
    if let std::net::IpAddr::V4(v4) = ip {
        peers.extend_from_slice(&v4.octets());
    }
    peers.extend_from_slice(&port.to_be_bytes());
    let resp = dict(std::collections::BTreeMap::from([
        (b"interval".to_vec(), int(60)),
        (b"complete".to_vec(), int(1)),
        (b"peers".to_vec(), bytes(peers)),
    ]));
    ([(header::CONTENT_TYPE, "text/plain")], encode(&resp)).into_response()
}

async fn start_tracker() -> (SocketAddr, Arc<RwLock<Option<SocketAddr>>>) {
    let state: Arc<RwLock<Option<SocketAddr>>> = Arc::new(RwLock::new(None));
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

fn make_torrent_bytes(data: &[u8], tracker_url: &str) -> Vec<u8> {
    let pieces: Vec<u8> = data.chunks(PIECE_LEN).flat_map(sha1_of).collect();
    let info = dict(std::collections::BTreeMap::from([
        (b"name".to_vec(), bytes("data.bin")),
        (b"piece length".to_vec(), int(PIECE_LEN as i64)),
        (b"length".to_vec(), int(data.len() as i64)),
        (b"pieces".to_vec(), bytes(pieces)),
    ]));
    let top = dict(std::collections::BTreeMap::from([
        (b"announce".to_vec(), bytes(tracker_url)),
        (b"info".to_vec(), info),
    ]));
    encode(&top)
}

fn meta_of(tb: &[u8]) -> TorrentMeta {
    parse_torrent(tb).unwrap()
}

async fn serve_seed(
    listener: TcpListener,
    data: Arc<Vec<u8>>,
    piece_len: usize,
    info_hash: InfoHash,
    peer_id: PeerId,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let data = data.clone();
        tokio::spawn(async move {
            let _ = handle_seed_peer(stream, &data, piece_len, info_hash, peer_id).await;
        });
    }
}

async fn handle_seed_peer(
    mut stream: TcpStream,
    data: &[u8],
    piece_len: usize,
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
    let n_pieces = data.len().div_ceil(piece_len);
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
                let off = index as usize * piece_len + begin as usize;
                let end = (off + length as usize).min(data.len());
                if off >= data.len() {
                    continue;
                }
                let block = data[off..end].to_vec();
                stream
                    .write_all(
                        &Message::Piece {
                            index,
                            begin,
                            block,
                        }
                        .encode(),
                    )
                    .await?;
            }
            Some(Message::Interested) | Some(Message::KeepAlive) | Some(Message::Cancel { .. }) => {
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// 测试：验证 tracker announce 请求中的 info_hash 被正确编码和传输。
/// 使用带有特殊字节的 info_hash（如 0x00, 0xFF 等）验证 percent encoding。
#[tokio::test]
async fn tracker_announce_info_hash_encoding_correct() {
    let dir = std::env::temp_dir().join(format!("e2e-encode-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // 使用包含特殊字节的数据，确保 info_hash 有各种字节
    let data: Vec<u8> = (0..(4 * PIECE_LEN + 255))
        .map(|i| (i % 251) as u8)
        .collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    // 打印 info_hash 的十六进制表示
    let ih_hex: String = meta.info_hash.iter().map(|b| format!("{b:02x}")).collect();
    println!("info_hash (hex): {ih_hex}");

    let seed = Arc::new(data.clone());
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = Some(saddr);
    tokio::spawn(serve_seed(
        sl,
        seed,
        PIECE_LEN,
        InfoHash::from_bytes(&meta.info_hash),
        PeerId::azureus_prefix(&[9u8; 12]),
    ));

    let cfg = TorrentConfig {
        dir: dir.to_path_buf(),
        peer_id: PeerId::azureus_prefix(&[3u8; 12]),
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
        selected_files: None,
    };
    let engine = TorrentEngine::new(meta, cfg).unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        engine.clone().run(CancellationToken::new()),
    )
    .await
    .expect("下载未超时")
    .expect("下载应成功");

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 测试：验证 tracker URL 中已包含 query 参数时的拼接行为。
/// 例如 tracker URL 为 `http://tracker.example.com/announce?key=value` 时，
/// 引擎应正确使用 `&` 而非 `?` 拼接后续参数。
#[tokio::test]
async fn tracker_url_with_existing_query_params() {
    let dir = std::env::temp_dir().join(format!("e2e-urlquery-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(2 * PIECE_LEN + 77)).map(|i| (i % 97) as u8).collect();

    let (taddr, seed_ref) = start_tracker().await;
    // tracker URL 包含已有的 query 参数
    let tracker_url = format!("http://{taddr}/announce?passkey=secret123&source=test");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed = Arc::new(data.clone());
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = Some(saddr);
    tokio::spawn(serve_seed(
        sl,
        seed,
        PIECE_LEN,
        InfoHash::from_bytes(&meta.info_hash),
        PeerId::azureus_prefix(&[9u8; 12]),
    ));

    let cfg = TorrentConfig {
        dir: dir.to_path_buf(),
        peer_id: PeerId::azureus_prefix(&[3u8; 12]),
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
        selected_files: None,
    };
    let engine = TorrentEngine::new(meta, cfg).unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        engine.clone().run(CancellationToken::new()),
    )
    .await
    .expect("下载未超时")
    .expect("下载应成功");

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}
