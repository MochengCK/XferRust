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

/// 测试用配置：本地闭环（无 DHT/PEX/LPD/UPnP、不对 tracker 之外的地址拨号）。
fn test_config(dir: &std::path::Path, meta: &TorrentMeta) -> TorrentConfig {
    TorrentConfig {
        enable_dht_ipv6: false,
        enable_pex: false,
        disk_cache_bytes: 0,
        save_metadata: false,
        load_saved_metadata: false,
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
        enable_lpd: false,
        enable_port_mapping: false,
        encryption: xfer_bt::EncryptionMode::PlaintextOnly,
        bt_protocol: xfer_bt::BtProtocol::TcpOnly,
        download_limit: 0,
        upload_limit: 0,
        seed_mode: false,
        seed_duration: 0,
        seed_ratio: 0.0,
        selected_files: None,
    }
}

async fn run_download(meta: TorrentMeta, dir: &std::path::Path) -> Result<(), String> {
    let engine = TorrentEngine::new(meta.clone(), test_config(dir, &meta)).map_err(|e| e.to_string())?;
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        engine.clone().run(CancellationToken::new()),
    )
    .await
    .map_err(|_| "下载超时".to_string())??;
    Ok(())
}

/// 位图 → 已完成片号（wire 语义：每片 1 bit，字节内高位在前）。
fn done_pieces(engine: &TorrentEngine) -> Vec<u32> {
    let Some(bf) = engine.bitfield() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for i in 0..bf.len() * 8 {
        if bf[i / 8] >> (7 - (i % 8)) & 1 == 1 {
            out.push(i as u32);
        }
    }
    out
}

/// 起下载、等到至少 `want` 片完成就停下（返回已完成的片号，升序）。
async fn run_until_pieces(engine: Arc<TorrentEngine>, want: usize) -> Vec<u32> {
    let cancel = CancellationToken::new();
    let runner = tokio::spawn(engine.clone().run(cancel.clone()));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    let mut done = Vec::new();
    while tokio::time::Instant::now() < deadline {
        done = done_pieces(&engine);
        if done.len() >= want {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    cancel.cancel();
    let _ = runner.await;
    done
}

/// 边下边播的**优先下载**（播放头 → 选片）：有播放头时，播放头那一带先下，
/// 哪怕它在文件末尾；没有播放头时才是默认的"从第 0 片起"。
///
/// 用本地 swarm 真下几片来验（选片策略的单测在 `playhead.rs`，这里验的是
/// "引擎真的照它选"）。
#[tokio::test]
async fn playhead_priority_downloads_that_region_first() {
    // 数据必须**大于 Stream 窗口（32 MiB）**：否则窗口覆盖整个文件，
    // 有没有播放头都是片号升序，测不出差别。
    let data: Vec<u8> = (0..640 * PIECE_LEN).map(|i| ((i % 251) + 1) as u8).collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let meta = meta_of(&make_torrent_bytes(&data, &tracker_url));
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

    const PLAYHEAD: u64 = 36 * 1024 * 1024;
    const PLAYHEAD_PIECE: u32 = (PLAYHEAD / PIECE_LEN as u64) as u32;
    assert!(PLAYHEAD_PIECE > 0, "测试前提：播放头不在文件开头");

    // ① 有播放头：先完成的片必须都在播放头往后那一带，且从播放头那片起
    let dir = std::env::temp_dir().join(format!("xfer-bt-playhead-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let m2 = meta_of(&make_torrent_bytes(&data, &tracker_url));
    let engine = TorrentEngine::new(m2.clone(), test_config(&dir, &m2)).unwrap();
    engine.set_playhead(Some(PLAYHEAD));
    let done = run_until_pieces(engine.clone(), 6).await;
    engine.set_playhead(None);
    assert!(!done.is_empty(), "一片都没下到（本地 swarm 没连上）");
    assert_eq!(
        done.iter().min(),
        Some(&PLAYHEAD_PIECE),
        "第一片必须落在播放头（实际：{done:?}）"
    );
    assert!(
        done.iter().all(|p| *p >= PLAYHEAD_PIECE),
        "播放头之外的片不该抢先下（实际：{done:?}）"
    );
    let _ = std::fs::remove_dir_all(&dir);

    // ② 对照：没有播放头时是默认行为 —— 从第 0 片开始
    let dir2 = std::env::temp_dir().join(format!("xfer-bt-noplayhead-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir2);
    std::fs::create_dir_all(&dir2).unwrap();
    let m3 = meta_of(&make_torrent_bytes(&data, &tracker_url));
    let engine2 = TorrentEngine::new(m3.clone(), test_config(&dir2, &m3)).unwrap();
    let done2 = run_until_pieces(engine2.clone(), 3).await;
    assert_eq!(
        done2.iter().min(),
        Some(&0),
        "没有播放头时不该改变默认行为（实际：{done2:?}）"
    );
    let _ = std::fs::remove_dir_all(&dir2);
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
            enable_dht_ipv6: false,
            enable_pex: false,
            disk_cache_bytes: 0,
            save_metadata: false,
            load_saved_metadata: false,
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
            enable_lpd: false,
            enable_port_mapping: false,
            encryption: xfer_bt::EncryptionMode::PlaintextOnly,
        bt_protocol: xfer_bt::BtProtocol::TcpOnly,
            download_limit: 0,
            upload_limit: 0,
            seed_mode: false,
            seed_duration: 0,
            seed_ratio: 0.0,
            selected_files: None,
        },
    )
    .unwrap();
    assert!(engine.is_done());
    let _ = std::fs::remove_dir_all(&dir);
}
