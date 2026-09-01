//! 端到端测试（真实客户端行为仿真）：
//!
//! 现有 e2e 测试全部使用配合型 seed（任意请求长度都供块），与真实网络
//! 差异巨大。本文件仿真真实客户端行为，覆盖线上"有节点无速度"的高危路径：
//!
//! 1. 多块大 piece + seed 先 choke、收到 Interested 后才 Unchoke
//!    （qBittorrent/libtorrent 的典型行为）
//! 2. seed 不发 bitfield，Unchoke 之后才逐个发 Have（老式客户端/部分场景）
//! 3. 传输中途被 re-choke（choking 算法轮换），之后恢复
//! 5. **请求块大小标准约束**：真实客户端（Transmission: MAX_BLOCK_SIZE=16KiB、
//!    libtorrent 等）对超过 2^14 字节的 request 一律拒绝（RejectRequest）或
//!    静默忽略。引擎若发 64KB 请求 → 真实种子永不应答 → 有节点无速度。
//!    旧引擎（aria2）请求端固定 16KiB（Piece::BLOCK_LENGTH），64KB 只是
//!    「响应」对端请求的上限（MAX_BLOCK_LENGTH）。
//!
//! 任一测试卡住（下载超时）即说明引擎在真实行为下零速度。

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
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
use xfer_bt::message::{encode_handshake, Message, PeerReader, BLOCK_SIZE};
use xfer_bt::{TorrentConfig, TorrentEngine};
use xfer_types::{InfoHash, PeerId};

/// 真实种子典型：256KB piece = 16 个 16KiB 标准块。
const PIECE_LEN: usize = 256 * 1024;

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

// ---------------------------------------------------------------------------
// tracker（复用 e2e_real 的简单实现）
// ---------------------------------------------------------------------------

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
    run_download_with_timeout(meta, dir, Duration::from_secs(30)).await
}

async fn run_download_with_timeout(
    meta: TorrentMeta,
    dir: &std::path::Path,
    timeout: Duration,
) -> Result<(), String> {
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
    };
    let engine = TorrentEngine::new(meta, cfg).map_err(|e| e.to_string())?;
    tokio::time::timeout(timeout, engine.clone().run(CancellationToken::new()))
        .await
        .map_err(|_| "下载超时（有节点无速度）".to_string())??;
    Ok(())
}

// ---------------------------------------------------------------------------
// 真实行为 seed 1：HaveAll + 收到 Interested 后延迟 Unchoke（qBittorrent 风格）
// ---------------------------------------------------------------------------

async fn handle_realistic_seed(
    mut stream: TcpStream,
    data: &[u8],
    piece_len: usize,
    info_hash: InfoHash,
    peer_id: PeerId,
    unchoke_delay: Duration,
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

    // 真实 seeder：握手后立即 HaveAll（支持 Fast Extension 时）
    stream.write_all(&Message::HaveAll.encode()).await?;

    // 关键：初始保持 choke，直到收到 Interested 才延迟 Unchoke
    loop {
        let Some(msg) = reader.read_message(&mut stream).await? else {
            return Ok(());
        };
        match msg {
            Message::Interested => break,
            Message::KeepAlive
            | Message::NotInterested
            | Message::Cancel { .. }
            | Message::Request { .. } => {
                // 被 choke 期间收到 request：真实客户端会忽略
            }
            _ => {}
        }
    }
    tokio::time::sleep(unchoke_delay).await;
    stream.write_all(&Message::Unchoke.encode()).await?;

    // 正常供块
    loop {
        match reader.read_message(&mut stream).await? {
            None => break,
            Some(Message::Request {
                index,
                begin,
                length,
            }) => {
                let off = index as usize * piece_len + begin as usize;
                if off >= data.len() {
                    continue;
                }
                let end = (off + length as usize).min(data.len());
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
            Some(_) => {}
        }
    }
    Ok(())
}

async fn serve_realistic_seed(
    listener: TcpListener,
    data: Arc<Vec<u8>>,
    piece_len: usize,
    info_hash: InfoHash,
    peer_id: PeerId,
    unchoke_delay: Duration,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let data = data.clone();
        tokio::spawn(async move {
            let _ = handle_realistic_seed(
                stream,
                &data,
                piece_len,
                info_hash,
                peer_id,
                unchoke_delay,
            )
            .await;
        });
    }
}

/// 测试 1：多块大 piece + Interested 门槛 + 延迟 Unchoke。
/// 这是真实种子下载的最常见场景；若引擎卡死在此即"有节点无速度"。
#[tokio::test]
async fn multi_block_piece_with_interested_gated_unchoke() {
    let dir = std::env::temp_dir().join(format!("e2e-realistic-1-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // 5 片 + 尾部零头：覆盖整片与短尾片
    let data: Vec<u8> = (0..(5 * PIECE_LEN + 1234))
        .map(|i| (i % 251) as u8)
        .collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed = Arc::new(data.clone());
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];

    tokio::spawn(serve_realistic_seed(
        sl,
        seed,
        PIECE_LEN,
        InfoHash::from_bytes(&meta.info_hash),
        PeerId::azureus_prefix(&[9u8; 12]),
        Duration::from_millis(500),
    ));

    run_download(meta, &dir)
        .await
        .expect("多块 piece + Interested 门槛下下载应成功");

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 真实行为 seed 2：不发 bitfield，先 Unchoke 再逐个发 Have
// ---------------------------------------------------------------------------

async fn handle_have_after_unchoke_seed(
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
    // 不声明 Fast Extension / BEP10：手工构造 reserved 全零的握手，
    // 模拟老式客户端（引擎将不会收到 HaveAll，只能靠 Have）
    let mut hs_bytes = encode_handshake(&info_hash, &peer_id);
    hs_bytes[20..28].fill(0); // reserved 清零
    stream.write_all(&hs_bytes).await?;

    // 先 Unchoke（乐观 unchoke 场景），此时引擎还不知道我们有任何片
    stream.write_all(&Message::Unchoke.encode()).await?;

    // 稍后逐个通告 Have —— 引擎必须在收到 Have 后主动发起请求
    let n_pieces = data.len().div_ceil(piece_len) as u32;
    for i in 0..n_pieces {
        stream.write_all(&Message::Have(i).encode()).await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    loop {
        match reader.read_message(&mut stream).await? {
            None => break,
            Some(Message::Request {
                index,
                begin,
                length,
            }) => {
                let off = index as usize * piece_len + begin as usize;
                if off >= data.len() {
                    continue;
                }
                let end = (off + length as usize).min(data.len());
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
            Some(_) => {}
        }
    }
    Ok(())
}

/// 测试 2：Unchoke 先于 Have 到达（乐观 unchoke + 无 bitfield 的老式客户端）。
/// Have 到达时引擎必须：声明 interested + 立即发起请求。
#[tokio::test]
async fn have_messages_after_unchoke_still_download() {
    let dir = std::env::temp_dir().join(format!("e2e-realistic-2-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(3 * PIECE_LEN + 77)).map(|i| (i % 97) as u8).collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed = Arc::new(data.clone());
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];

    let ih = InfoHash::from_bytes(&meta.info_hash);
    let pid = PeerId::azureus_prefix(&[9u8; 12]);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = sl.accept().await else {
                break;
            };
            let data = seed.clone();
            tokio::spawn(async move {
                let _ = handle_have_after_unchoke_seed(stream, &data, PIECE_LEN, ih, pid).await;
            });
        }
    });

    run_download(meta, &dir)
        .await
        .expect("Unchoke 先到 + Have 逐个通告时下载应成功");

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 真实行为 seed 3：传输中途 re-choke（choking 算法轮换）后恢复
// ---------------------------------------------------------------------------

async fn handle_rechoke_seed(
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
    stream.write_all(&Message::HaveAll.encode()).await?;

    // 等 Interested
    loop {
        let Some(msg) = reader.read_message(&mut stream).await? else {
            return Ok(());
        };
        if matches!(msg, Message::Interested) {
            break;
        }
    }
    stream.write_all(&Message::Unchoke.encode()).await?;

    // 供块：每发满 2 个 piece 的块量后，choke 300ms 再 unchoke
    let blocks_per_piece = piece_len.div_ceil(BLOCK_SIZE as usize);
    let mut blocks_sent = 0usize;
    let mut rechoke_left = 2; // 最多 re-choke 两次，避免无限拖延
    loop {
        match reader.read_message(&mut stream).await? {
            None => break,
            Some(Message::Request {
                index,
                begin,
                length,
            }) => {
                let off = index as usize * piece_len + begin as usize;
                if off >= data.len() {
                    continue;
                }
                let end = (off + length as usize).min(data.len());
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
                blocks_sent += 1;
                if rechoke_left > 0 && blocks_sent % (blocks_per_piece * 2) == 0 {
                    stream.write_all(&Message::Choke.encode()).await?;
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    stream.write_all(&Message::Unchoke.encode()).await?;
                    rechoke_left -= 1;
                }
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// 测试 3：中途 re-choke。引擎应丢弃在途请求并在 unchoke 后恢复下载。
#[tokio::test]
async fn download_survives_rechoke_mid_piece() {
    let dir = std::env::temp_dir().join(format!("e2e-realistic-3-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(6 * PIECE_LEN)).map(|i| (i % 251) as u8).collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed = Arc::new(data.clone());
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];

    let ih = InfoHash::from_bytes(&meta.info_hash);
    let pid = PeerId::azureus_prefix(&[9u8; 12]);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = sl.accept().await else {
                break;
            };
            let data = seed.clone();
            tokio::spawn(async move {
                let _ = handle_rechoke_seed(stream, &data, PIECE_LEN, ih, pid).await;
            });
        }
    });

    run_download(meta, &dir)
        .await
        .expect("re-choke 后下载应恢复并完成");

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 真实行为场景 4：独占分片被释放后，空闲连接必须能自行恢复下载
// ---------------------------------------------------------------------------

/// 选择性 seed：只拥有部分片（或全部），plain 握手（无 fast extension），
/// 先 Unchoke 后 Have（乐观 unchoke 顺序）。可选：
/// - `handshake_delay`：延迟握手（模拟晚到的连接）
/// - `block_delay`：每块服务延迟（模拟慢速节点）
/// - `close_at`：从握手完成起计时，到点即断开（模拟中途掉线）
async fn handle_selective_seed(
    mut stream: TcpStream,
    data: &[u8],
    piece_len: usize,
    info_hash: InfoHash,
    peer_id: PeerId,
    only_piece: Option<u32>,
    handshake_delay: Duration,
    block_delay: Duration,
    close_at: Option<Duration>,
    max_blocks: Option<usize>,
) -> std::io::Result<()> {
    if !handshake_delay.is_zero() {
        tokio::time::sleep(handshake_delay).await;
    }
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
    // reserved 清零：模拟不支持 fast extension / BEP10 的老式客户端
    let mut hs_bytes = encode_handshake(&info_hash, &peer_id);
    hs_bytes[20..28].fill(0);
    stream.write_all(&hs_bytes).await?;

    // 乐观 unchoke 顺序：先 Unchoke，之后才逐个 Have
    stream.write_all(&Message::Unchoke.encode()).await?;
    let n_pieces = data.len().div_ceil(piece_len) as u32;
    for i in 0..n_pieces {
        if only_piece.is_none_or(|p| p == i) {
            stream.write_all(&Message::Have(i).encode()).await?;
        }
    }

    let deadline = close_at.map(|d| tokio::time::Instant::now() + d);
    let mut served = 0usize;
    loop {
        if deadline.is_some_and(|t| tokio::time::Instant::now() >= t) {
            return Ok(()); // 模拟中途掉线
        }
        match reader.read_message(&mut stream).await? {
            None => break,
            Some(Message::Request {
                index,
                begin,
                length,
            }) => {
                // 只服务自己拥有的片
                if only_piece.is_some_and(|p| p != index) {
                    continue;
                }
                if deadline.is_some_and(|t| tokio::time::Instant::now() >= t) {
                    return Ok(());
                }
                // 超过供块上限后装死（模拟卡住的 peer），等待 close_at 掉线
                if max_blocks.is_some_and(|m| served >= m) {
                    continue;
                }
                let off = index as usize * piece_len + begin as usize;
                if off >= data.len() {
                    continue;
                }
                let end = (off + length as usize).min(data.len());
                let block = data[off..end].to_vec();
                if !block_delay.is_zero() {
                    tokio::time::sleep(block_delay).await;
                }
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
                served += 1;
            }
            Some(_) => {}
        }
    }
    Ok(())
}

async fn serve_selective_seed(
    listener: TcpListener,
    data: Arc<Vec<u8>>,
    piece_len: usize,
    info_hash: InfoHash,
    peer_id: PeerId,
    only_piece: Option<u32>,
    handshake_delay: Duration,
    block_delay: Duration,
    close_at: Option<Duration>,
    max_blocks: Option<usize>,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let data = data.clone();
        tokio::spawn(async move {
            let _ = handle_selective_seed(
                stream,
                &data,
                piece_len,
                info_hash,
                peer_id,
                only_piece,
                handshake_delay,
                block_delay,
                close_at,
                max_blocks,
            )
            .await;
        });
    }
}

/// 测试 4：3 片种子、4 个选择性 seed。
/// S1 只持有片 0（供 2/16 块后装死并掉线）、S2 只持有片 1（慢速）、
/// S3 只持有片 2（慢速）、S4 全片但晚到 5s（此时 3 片已被 S1..S3 独占
/// → S4 无片可分而空闲）。
/// S1 掉线释放未完成的片 0 后，**空闲的 S4 必须通过每轮兜底补发自行恢复**
/// 下载片 0；若无兜底机制，只能等慢速 S2 花 ~24s 完成片 1 后接手再花
/// ~24s 补片 0 → 远超超时。
#[tokio::test]
async fn idle_peer_recovers_freed_piece() {
    let dir = std::env::temp_dir().join(format!("e2e-realistic-4-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(3 * PIECE_LEN)).map(|i| (i % 251) as u8).collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);
    let ih = InfoHash::from_bytes(&meta.info_hash);

    let seed = Arc::new(data.clone());
    let (sl1, s1) = bind_random().await;
    let (sl2, s2) = bind_random().await;
    let (sl3, s3) = bind_random().await;
    let (sl4, s4) = bind_random().await;
    *seed_ref.write().unwrap() = vec![s1, s2, s3, s4];

    // S1：只有片 0，供 2/16 块后装死，第 6 秒掉线（片 0 未完成即被释放）
    tokio::spawn(serve_selective_seed(
        sl1,
        seed.clone(),
        PIECE_LEN,
        ih,
        PeerId::azureus_prefix(&[11u8; 12]),
        Some(0),
        Duration::ZERO,
        Duration::ZERO,
        Some(Duration::from_secs(6)),
        Some(2),
    ));
    // S2/S3：分别只有片 1/片 2，慢速供块（16 块 × 1.5s → 单片约 24s）
    tokio::spawn(serve_selective_seed(
        sl2,
        seed.clone(),
        PIECE_LEN,
        ih,
        PeerId::azureus_prefix(&[12u8; 12]),
        Some(1),
        Duration::ZERO,
        Duration::from_millis(1500),
        None,
        None,
    ));
    tokio::spawn(serve_selective_seed(
        sl3,
        seed.clone(),
        PIECE_LEN,
        ih,
        PeerId::azureus_prefix(&[13u8; 12]),
        Some(2),
        Duration::ZERO,
        Duration::from_millis(1500),
        None,
        None,
    ));
    // S4：全片，但握手晚到 5s —— 届时 3 片均已被独占，S4 将处于空闲
    tokio::spawn(serve_selective_seed(
        sl4,
        seed.clone(),
        PIECE_LEN,
        ih,
        PeerId::azureus_prefix(&[14u8; 12]),
        None,
        Duration::from_secs(5),
        Duration::ZERO,
        None,
        None,
    ));

    run_download_with_timeout(meta, &dir, Duration::from_secs(40))
        .await
        .expect("下载应成功（空闲连接须在兜底轮恢复下载）");

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 真实行为场景 5：BT 标准请求块大小约束（2^14 = 16KiB）
// ---------------------------------------------------------------------------
//
// 生态事实标准：request 块大小为 16384 字节。主流真实客户端对更大的请求
// 直接拒绝或忽略：
//   - Transmission: MAX_BLOCK_SIZE = 16384，超限请求被拒绝（Fast Extension
//     下发 RejectRequest）或静默忽略
//   - libtorrent（qBittorrent/Deluge）：超过 block_size 的请求被拒绝
//   - 旧引擎 XferCore（aria2）：请求端固定 16KiB（Piece::BLOCK_LENGTH），
//     64KB（MAX_BLOCK_LENGTH）只是「响应」对端请求的上限
//
// 引擎若发出超过 16KiB 的 request，真实种子永不应答 → 有节点无速度。

/// 真实客户端的请求块上限（2^14）。
const REAL_MAX_BLOCK: u32 = 16 * 1024;

/// 严格标准 seed：超过 16KiB 的 request 按真实客户端行为处理——
/// 协商了 Fast Extension 则回 RejectRequest，否则静默忽略。
/// 合规请求正常供块。`oversized` 记录收到的超限请求数。
async fn handle_strict_block_size_seed(
    mut stream: TcpStream,
    data: &[u8],
    piece_len: usize,
    info_hash: InfoHash,
    peer_id: PeerId,
    fast_ext: bool,
    oversized: Arc<AtomicUsize>,
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
    let mut hs_bytes = encode_handshake(&info_hash, &peer_id);
    if !fast_ext {
        hs_bytes[20..28].fill(0); // 老式客户端：不声明任何扩展
    }
    stream.write_all(&hs_bytes).await?;

    // 全片持有：Fast Extension 用 HaveAll，老式客户端用 bitfield
    if fast_ext {
        stream.write_all(&Message::HaveAll.encode()).await?;
    } else {
        let n_pieces = data.len().div_ceil(piece_len);
        let mut bf = vec![0u8; n_pieces.div_ceil(8)];
        for i in 0..n_pieces {
            bf[i / 8] |= 0x80 >> (i % 8);
        }
        stream.write_all(&Message::Bitfield(bf).encode()).await?;
    }

    // 收到 Interested 后才 Unchoke（真实种子的典型门槛）
    loop {
        let Some(msg) = reader.read_message(&mut stream).await? else {
            return Ok(());
        };
        if matches!(msg, Message::Interested) {
            break;
        }
    }
    stream.write_all(&Message::Unchoke.encode()).await?;

    loop {
        match reader.read_message(&mut stream).await? {
            None => break,
            Some(Message::Request {
                index,
                begin,
                length,
            }) => {
                // 真实客户端行为：超过 16KiB 的请求一律不应答
                if length > REAL_MAX_BLOCK {
                    oversized.fetch_add(1, Ordering::Relaxed);
                    if fast_ext {
                        stream
                            .write_all(
                                &Message::RejectRequest {
                                    index,
                                    begin,
                                    length,
                                }
                                .encode(),
                            )
                            .await?;
                    }
                    continue; // 无 Fast Extension 时静默忽略
                }
                let off = index as usize * piece_len + begin as usize;
                if off >= data.len() {
                    continue;
                }
                let end = (off + length as usize).min(data.len());
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
            Some(_) => {}
        }
    }
    Ok(())
}

async fn serve_strict_block_size_seed(
    listener: TcpListener,
    data: Arc<Vec<u8>>,
    piece_len: usize,
    info_hash: InfoHash,
    peer_id: PeerId,
    fast_ext: bool,
    oversized: Arc<AtomicUsize>,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let data = data.clone();
        let oversized = oversized.clone();
        tokio::spawn(async move {
            let _ = handle_strict_block_size_seed(
                stream,
                &data,
                piece_len,
                info_hash,
                peer_id,
                fast_ext,
                oversized,
            )
            .await;
        });
    }
}

/// 测试 5a：Transmission 风格真实种子（协商 Fast Extension）。
/// 超过 16KiB 的 request 会被 RejectRequest 拒绝，只有合规请求供块。
/// 若引擎请求块大小不符合 16KiB 标准 → 全部请求被拒 → 下载超时。
#[tokio::test]
async fn real_client_rejects_oversized_requests() {
    let dir = std::env::temp_dir().join(format!("e2e-realistic-5a-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(3 * PIECE_LEN)).map(|i| (i % 251) as u8).collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed = Arc::new(data.clone());
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];
    let oversized = Arc::new(AtomicUsize::new(0));

    tokio::spawn(serve_strict_block_size_seed(
        sl,
        seed,
        PIECE_LEN,
        InfoHash::from_bytes(&meta.info_hash),
        PeerId::azureus_prefix(&[15u8; 12]),
        true,
        oversized.clone(),
    ));

    run_download(meta, &dir)
        .await
        .expect("真实种子（16KiB 请求上限 + RejectRequest）下下载应成功");

    assert_eq!(
        oversized.load(Ordering::Relaxed),
        0,
        "引擎发出的 request 块大小必须 ≤ 16KiB（真实客户端会拒绝更大请求 → 有节点无速度）"
    );

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 测试 5b：老式客户端（无 Fast Extension）：超限请求被静默忽略。
/// 若引擎请求 64KB 块：in_flight 永久占满且无重发机制 → 连接空转至超时。
#[tokio::test]
async fn legacy_client_ignores_oversized_requests() {
    let dir = std::env::temp_dir().join(format!("e2e-realistic-5b-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(3 * PIECE_LEN)).map(|i| (i % 97) as u8).collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = meta_of(&tb);

    let seed = Arc::new(data.clone());
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = vec![saddr];
    let oversized = Arc::new(AtomicUsize::new(0));

    tokio::spawn(serve_strict_block_size_seed(
        sl,
        seed,
        PIECE_LEN,
        InfoHash::from_bytes(&meta.info_hash),
        PeerId::azureus_prefix(&[16u8; 12]),
        false,
        oversized.clone(),
    ));

    run_download(meta, &dir)
        .await
        .expect("老式客户端（静默忽略超限请求）下下载应成功");

    assert_eq!(
        oversized.load(Ordering::Relaxed),
        0,
        "引擎发出的 request 块大小必须 ≤ 16KiB（老式客户端会静默忽略更大请求 → 有节点无速度）"
    );

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "下载文件与源数据不一致");
    let _ = std::fs::remove_dir_all(&dir);
}
