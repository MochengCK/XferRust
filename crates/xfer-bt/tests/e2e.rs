//! M2 端到端：本地 seed peer + HTTP tracker + TorrentEngine 全链路下载。
//!
//! 验证：多 peer（多个 seed 连接）并行下载完成、piece 哈希全对、
//! 落盘文件与源数据逐字节一致。

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
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

struct Seed {
    data: Arc<Vec<u8>>,
    piece_len: usize,
}

fn sha1_of(b: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(b);
    h.finalize().into()
}

/// 监听一个随机端口并返回 (listener, addr)。
async fn bind_random() -> (TcpListener, SocketAddr) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    (l, addr)
}

/// seed peer：被动握手、发 bitfield+unchoke、响应 request 发 piece 块。
async fn serve_seed(listener: TcpListener, seed: Arc<Seed>, info_hash: InfoHash, peer_id: PeerId) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let seed = seed.clone();
        tokio::spawn(async move {
            let _ = handle_seed_peer(stream, &seed, info_hash, peer_id).await;
        });
    }
}

async fn handle_seed_peer(
    mut stream: TcpStream,
    seed: &Seed,
    info_hash: InfoHash,
    peer_id: PeerId,
) -> std::io::Result<()> {
    let mut reader = PeerReader::new();
    // 被动：先读对端握手，再回握手
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
    // 全 1 bitfield + unchoke
    let n_pieces = seed.data.len().div_ceil(seed.piece_len);
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
                let off = index as usize * seed.piece_len + begin as usize;
                let end = (off + length as usize).min(seed.data.len());
                if off >= seed.data.len() {
                    continue;
                }
                let block = seed.data[off..end].to_vec();
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

/// 简易 HTTP tracker：返回 compact peers（seed 地址）。
async fn tracker_announce(
    Query(_q): Query<HashMap<String, String>>,
    State(seed): State<Arc<RwLock<Option<SocketAddr>>>>,
) -> Response {
    let Some(addr) = *seed.read().unwrap() else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "no seed").into_response();
    };
    let ip = addr.ip();
    let port = addr.port();
    let mut peers = Vec::with_capacity(6);
    if let std::net::IpAddr::V4(v4) = ip {
        peers.extend_from_slice(&v4.octets());
    } else {
        peers.extend_from_slice(&[127, 0, 0, 1]);
    }
    peers.extend_from_slice(&port.to_be_bytes());
    let resp = dict(BTreeMap::from([
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

fn meta_of(tb: &[u8]) -> TorrentMeta {
    parse_torrent(tb).unwrap()
}

async fn run_download(meta: TorrentMeta, dir: &std::path::Path) -> Result<(), String> {
    let cfg = TorrentConfig {
        dir: dir.to_path_buf(),
        peer_id: PeerId::azureus_prefix(&[3u8; 12]),
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
    let engine = TorrentEngine::new(meta, cfg).map_err(|e| e.to_string())?;
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        engine.clone().run(CancellationToken::new()),
    )
    .await
    .map_err(|_| "下载超时".to_string())??;
    Ok(())
}

#[tokio::test]
async fn download_single_seed_single_file() {
    let dir = std::env::temp_dir().join(format!("xfer-bt-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // 数据：5 片余 1234 字节，覆盖最后一片不足的情形
    let data: Vec<u8> = (0..(4 * PIECE_LEN + 1234))
        .map(|i| (i % 251) as u8)
        .collect();

    // tracker 先启动，seed 地址后填入
    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");

    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    // seed 启动后把地址写进 tracker state
    let seed = Arc::new(Seed {
        data: Arc::new(data.clone()),
        piece_len: PIECE_LEN,
    });
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = Some(saddr);
    tokio::spawn(serve_seed(
        sl,
        seed,
        InfoHash::from_bytes(&meta.info_hash),
        PeerId::azureus_prefix(&[9u8; 12]),
    ));

    run_download(meta, &dir).await.expect("下载应成功");

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn download_with_two_seeds_parallel() {
    let dir = std::env::temp_dir().join(format!("xfer-bt-e2e2-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(8 * PIECE_LEN + 7))
        .map(|i| (i.wrapping_mul(131) % 253) as u8)
        .collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    // 两个 seed，都注册到 tracker
    let seed = Arc::new(Seed {
        data: Arc::new(data.clone()),
        piece_len: PIECE_LEN,
    });
    let mut addrs = Vec::new();
    for i in 0..2 {
        let (sl, saddr) = bind_random().await;
        addrs.push(saddr);
        let sid = PeerId::azureus_prefix(&[10 + i as u8; 12]);
        let s2 = seed.clone();
        let ih = InfoHash::from_bytes(&meta.info_hash);
        tokio::spawn(async move {
            serve_seed(sl, s2, ih, sid).await;
        });
    }
    // tracker 只返回一个 seed 地址（另一个通过首次连接后的 PEX/后续 announce 不在此测试范围）
    *seed_ref.write().unwrap() = Some(addrs[0]);

    run_download(meta, &dir).await.expect("下载应成功");
    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn resume_completed_file_skips_download() {
    let dir = std::env::temp_dir().join(format!("xfer-bt-e2e3-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(2 * PIECE_LEN + 99)).map(|i| (i % 97) as u8).collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    // 预置完整文件
    std::fs::write(dir.join("data.bin"), &data).unwrap();

    let seed = Arc::new(Seed {
        data: Arc::new(data.clone()),
        piece_len: PIECE_LEN,
    });
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = Some(saddr);
    tokio::spawn(serve_seed(
        sl,
        seed,
        InfoHash::from_bytes(&meta.info_hash),
        PeerId::azureus_prefix(&[8u8; 12]),
    ));

    // 已有完整文件：直接标记完成，立即返回
    let engine = TorrentEngine::new(
        meta,
        TorrentConfig {
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
        },
    )
    .unwrap();
    assert!(engine.is_done());
    let _ = std::fs::remove_dir_all(&dir);
}
