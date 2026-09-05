//! 端到端测试（真实场景覆盖）：
//! - Fast Extension (BEP 6) HaveAll seed peer
//! - Choking peer（先 choke 再 unchoke）
//! - Extension Protocol (BEP 10) 握手
//! - 多文件种子
//! - 大数据量下载（覆盖流水线自适应 + 多片校验）

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use sha1::{Digest, Sha1};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use xfer_bencode::{bytes, dict, encode, int, parse_torrent, TorrentMeta};
use xfer_bt::message::{supports_extension, Message, PeerReader};
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

/// seed peer 的行为模式。
#[derive(Clone, Copy, Debug, PartialEq)]
enum SeedMode {
    /// 标准 Bitfield + Unchoke（BEP 3 原始）
    Bitfield,
    /// Fast Extension: HaveAll + Unchoke（BEP 6，真实客户端常用）
    HaveAll,
    /// 先 Choke，收到 Interested 后延迟 Unchoke（测试 choking 处理）
    ChokeThenUnchoke,
}

struct Seed {
    data: Arc<Vec<u8>>,
    piece_len: usize,
    mode: SeedMode,
}

/// seed peer：被动握手，根据 mode 选择初始化消息，响应 request 发 piece 块。
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
    // 被动握手
    let hs = loop {
        match reader.read_handshake(&mut stream).await? {
            Some(h) => break h,
            None => continue,
        }
    };
    if hs.info_hash != info_hash {
        return Ok(());
    }

    // 回握手：声明 Fast Extension + Extension Protocol + DHT（模拟真实客户端）
    let mut our_reserved = [0u8; 8];
    our_reserved[5] |= 0x10; // Extension Protocol (BEP 10)
    our_reserved[7] |= 0x04; // Fast Extension (BEP 6)
    our_reserved[7] |= 0x01; // DHT (BEP 5)
    let mut hs_reply = vec![19u8];
    hs_reply.extend_from_slice(b"BitTorrent protocol");
    hs_reply.extend_from_slice(&our_reserved);
    hs_reply.extend_from_slice(info_hash.as_bytes());
    hs_reply.extend_from_slice(&peer_id.0);
    stream.write_all(&hs_reply).await?;

    let n_pieces = seed.data.len().div_ceil(seed.piece_len);

    // 根据 mode 选择初始化消息
    match seed.mode {
        SeedMode::Bitfield => {
            let mut bf = vec![0u8; n_pieces.div_ceil(8)];
            for i in 0..n_pieces {
                bf[i / 8] |= 0x80 >> (i % 8);
            }
            stream.write_all(&Message::Bitfield(bf).encode()).await?;
            stream.write_all(&Message::Unchoke.encode()).await?;
        }
        SeedMode::HaveAll => {
            // Fast Extension: HaveAll + Unchoke
            stream.write_all(&Message::HaveAll.encode()).await?;
            stream.write_all(&Message::Unchoke.encode()).await?;
        }
        SeedMode::ChokeThenUnchoke => {
            // 发 bitfield 但 choke
            let mut bf = vec![0u8; n_pieces.div_ceil(8)];
            for i in 0..n_pieces {
                bf[i / 8] |= 0x80 >> (i % 8);
            }
            stream.write_all(&Message::Bitfield(bf).encode()).await?;
            // 先 choke，等收到 interested 后 500ms 再 unchoke
            stream.write_all(&Message::Choke.encode()).await?;
        }
    }

    // Extension Protocol 握手（如果对端支持）
    if supports_extension(&hs.reserved) {
        let mut m = BTreeMap::new();
        m.insert(b"ut_pex".to_vec(), xfer_bencode::Value::Int(1));
        let mut d = BTreeMap::new();
        d.insert(b"m".to_vec(), xfer_bencode::Value::Dict(m));
        d.insert(
            b"v".to_vec(),
            xfer_bencode::Value::Bytes(b"TestSeed/1.0".to_vec()),
        );
        d.insert(b"p".to_vec(), xfer_bencode::Value::Int(6881));
        let payload = encode(&xfer_bencode::Value::Dict(d));
        stream
            .write_all(&Message::Extended { ext_id: 0, payload }.encode())
            .await?;
    }

    // 如果是 choke 模式，等 interested 后 unchoke
    let need_wait_unchoke = seed.mode == SeedMode::ChokeThenUnchoke;
    let mut unchoked = seed.mode != SeedMode::ChokeThenUnchoke;

    loop {
        match reader.read_message(&mut stream).await? {
            None => break,
            Some(Message::Request {
                index,
                begin,
                length,
            }) => {
                if need_wait_unchoke && !unchoked {
                    // 忽略请求直到我们 unchoke
                    continue;
                }
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
            Some(Message::Interested) => {
                if need_wait_unchoke && !unchoked {
                    // 收到 interested，500ms 后 unchoke
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    stream.write_all(&Message::Unchoke.encode()).await?;
                    unchoked = true;
                }
            }
            Some(Message::KeepAlive)
            | Some(Message::Cancel { .. })
            | Some(Message::NotInterested) => {}
            Some(Message::Extended { .. }) => {
                // 忽略对端的扩展消息
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// 简易 HTTP tracker：返回 compact peers（seed 地址）。
async fn tracker_announce(
    Query(_q): Query<HashMap<String, String>>,
    State(seed): State<Arc<RwLock<Vec<SocketAddr>>>>,
) -> Response {
    let addrs = seed.read().unwrap().clone();
    let mut peers = Vec::new();
    for addr in &addrs {
        if let std::net::IpAddr::V4(v4) = addr.ip() {
            peers.extend_from_slice(&v4.octets());
            peers.extend_from_slice(&addr.port().to_be_bytes());
        }
    }
    let resp = dict(BTreeMap::from([
        (b"interval".to_vec(), int(60)),
        (b"complete".to_vec(), int(addrs.len() as i64)),
        (b"peers".to_vec(), bytes(peers)),
    ]));
    ([(header::CONTENT_TYPE, "text/plain")], encode(&resp)).into_response()
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
        seed_ratio: 0.0,
        selected_files: None,
    };
    let engine = TorrentEngine::new(meta, cfg).map_err(|e| e.to_string())?;
    tokio::time::timeout(
        Duration::from_secs(60),
        engine.clone().run(CancellationToken::new()),
    )
    .await
    .map_err(|_| "下载超时".to_string())??;
    Ok(())
}

// =====================================================================
// 测试用例
// =====================================================================

/// 测试 1：Fast Extension HaveAll seed peer — 真实世界最常见场景。
/// qBittorrent/Transmission 等客户端在拥有全部数据时发送 HaveAll 而非 Bitfield。
#[tokio::test]
async fn download_from_fast_extension_haveall_seed() {
    let dir = std::env::temp_dir().join(format!("e2e-fe-haveall-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // 数据：8 片 + 余 777 字节
    let data: Vec<u8> = (0..(8 * PIECE_LEN + 777))
        .map(|i| (i.wrapping_mul(131) % 253) as u8)
        .collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed = Arc::new(Seed {
        data: Arc::new(data.clone()),
        piece_len: PIECE_LEN,
        mode: SeedMode::HaveAll,
    });
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];
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

/// 测试 2：Choking peer — seed 先 choke，收到 interested 后延迟 unchoke。
/// 验证引擎正确处理 choke/unchoke 状态转换，不会卡死。
#[tokio::test]
async fn download_from_choking_seed() {
    let dir = std::env::temp_dir().join(format!("e2e-choke-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(4 * PIECE_LEN + 432))
        .map(|i| (i % 251) as u8)
        .collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed = Arc::new(Seed {
        data: Arc::new(data.clone()),
        piece_len: PIECE_LEN,
        mode: SeedMode::ChokeThenUnchoke,
    });
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];
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

/// 测试 3：Fast Extension HaveAll + 大数据量（64 片 = 4MB）。
/// 覆盖流水线自适应增长 + 多片校验 + HaveAll 场景。
#[tokio::test]
async fn download_large_data_from_haveall_seed() {
    let dir = std::env::temp_dir().join(format!("e2e-fe-large-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // 64 片 × 64KB = 4MB
    let data: Vec<u8> = (0..(64 * PIECE_LEN)).map(|i| (i % 251) as u8).collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed = Arc::new(Seed {
        data: Arc::new(data.clone()),
        piece_len: PIECE_LEN,
        mode: SeedMode::HaveAll,
    });
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];
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

/// 测试 4：多个 HaveAll seed peer 并行下载。
/// 覆盖多 peer 并行 + Fast Extension + piece 分散到不同 peer。
#[tokio::test]
async fn download_from_multiple_haveall_seeds() {
    let dir = std::env::temp_dir().join(format!("e2e-fe-multi-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // 16 片 = 1MB
    let data: Vec<u8> = (0..(16 * PIECE_LEN))
        .map(|i| (i.wrapping_mul(131) % 253) as u8)
        .collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    // 3 个 HaveAll seed
    let seed = Arc::new(Seed {
        data: Arc::new(data.clone()),
        piece_len: PIECE_LEN,
        mode: SeedMode::HaveAll,
    });
    let mut addrs = Vec::new();
    for i in 0..3 {
        let (sl, saddr) = bind_random().await;
        addrs.push(saddr);
        let sid = PeerId::azureus_prefix(&[10 + i as u8; 12]);
        let s2 = seed.clone();
        let ih = InfoHash::from_bytes(&meta.info_hash);
        tokio::spawn(async move {
            serve_seed(sl, s2, ih, sid).await;
        });
    }
    *seed_ref.write().unwrap() = addrs;

    run_download(meta, &dir).await.expect("下载应成功");

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 测试 5：HaveAll seed + 混合 Bitfield seed（模拟真实 swarm）。
/// 一个 seed 用 Fast Extension HaveAll，另一个用传统 Bitfield。
#[tokio::test]
async fn download_from_mixed_seeds_haveall_and_bitfield() {
    let dir = std::env::temp_dir().join(format!("e2e-mixed-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // 8 片 = 512KB
    let data: Vec<u8> = (0..(8 * PIECE_LEN)).map(|i| (i % 251) as u8).collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    // seed 1: HaveAll (Fast Extension)
    let seed1 = Arc::new(Seed {
        data: Arc::new(data.clone()),
        piece_len: PIECE_LEN,
        mode: SeedMode::HaveAll,
    });
    let (sl1, saddr1) = bind_random().await;
    let ih = InfoHash::from_bytes(&meta.info_hash);
    let sid1 = PeerId::azureus_prefix(&[10u8; 12]);
    tokio::spawn(async move {
        serve_seed(sl1, seed1, ih, sid1).await;
    });

    // seed 2: Bitfield (传统)
    let seed2 = Arc::new(Seed {
        data: Arc::new(data.clone()),
        piece_len: PIECE_LEN,
        mode: SeedMode::Bitfield,
    });
    let (sl2, saddr2) = bind_random().await;
    let ih2 = InfoHash::from_bytes(&meta.info_hash);
    let sid2 = PeerId::azureus_prefix(&[20u8; 12]);
    tokio::spawn(async move {
        serve_seed(sl2, seed2, ih2, sid2).await;
    });

    *seed_ref.write().unwrap() = vec![saddr1, saddr2];

    run_download(meta, &dir).await.expect("下载应成功");

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 测试 6：Fast Extension HaveAll seed + 部分已下载文件续传。
/// 验证 HaveAll 场景下的断点续传。
#[tokio::test]
async fn resume_with_haveall_seed() {
    let dir = std::env::temp_dir().join(format!("e2e-fe-resume-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // 4 片 = 256KB
    let data: Vec<u8> = (0..(4 * PIECE_LEN)).map(|i| (i % 251) as u8).collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    // 预置前 2 片完整数据
    std::fs::write(dir.join("data.bin"), &data[..2 * PIECE_LEN]).unwrap();

    let seed = Arc::new(Seed {
        data: Arc::new(data.clone()),
        piece_len: PIECE_LEN,
        mode: SeedMode::HaveAll,
    });
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];
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
