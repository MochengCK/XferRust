//! 磁力链接（BEP 9 ut_metadata）端到端测试。
//!
//! mock seed 通过扩展协议提供 .torrent 元数据 + piece 数据，
//! 验证磁力引擎从零（仅 info_hash）获取元数据并完成下载，文件逐字节一致。

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use sha1::{Digest, Sha1};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use xfer_bencode::{bytes, dict, encode, int, Value};
use xfer_bt::message::{encode_handshake, Message, PeerReader};
use xfer_bt::{TorrentConfig, TorrentEngine};
use xfer_types::{InfoHash, PeerId};

const PIECE_LEN: usize = 64 * 1024;
/// BEP 9 元数据分片大小。
const META_PIECE: usize = 16 * 1024;

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

/// 磁力 seed：扩展握手声明 ut_metadata=1，响应 metadata request，
/// 并像普通 seed 一样响应 piece request。
async fn serve_magnet_seed(
    listener: TcpListener,
    seed: Arc<Vec<u8>>,
    info_bytes: Arc<Vec<u8>>,
    info_hash: InfoHash,
    peer_id: PeerId,
    conns: Arc<AtomicUsize>,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        conns.fetch_add(1, Ordering::SeqCst);
        let (s, ib, ih, pid) = (seed.clone(), info_bytes.clone(), info_hash, peer_id);
        tokio::spawn(async move {
            let _ = handle_magnet_seed_peer(stream, &s, &ib, ih, pid).await;
        });
    }
}

async fn handle_magnet_seed_peer(
    mut stream: TcpStream,
    seed: &[u8],
    info_bytes: &[u8],
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

    // 扩展握手：声明 ut_metadata = 1（对端以 ext_id=1 请求元数据）
    let mut m = BTreeMap::new();
    m.insert(b"ut_metadata".to_vec(), Value::Int(1));
    let mut d = BTreeMap::new();
    d.insert(b"m".to_vec(), Value::Dict(m));
    let payload = encode(&Value::Dict(d));
    stream
        .write_all(&Message::Extended { ext_id: 0, payload }.encode())
        .await?;

    // 全 1 bitfield + unchoke
    let n_pieces = seed.len().div_ceil(PIECE_LEN);
    let mut bf = vec![0u8; n_pieces.div_ceil(8)];
    for i in 0..n_pieces {
        bf[i / 8] |= 0x80 >> (i % 8);
    }
    stream.write_all(&Message::Bitfield(bf).encode()).await?;
    stream.write_all(&Message::Unchoke.encode()).await?;

    // 引擎为 ut_metadata 广告的 ext_id（BEP 10：回包必须用对端广告的 id）。
    let mut engine_meta_id: u8 = 2;

    loop {
        match reader.read_message(&mut stream).await? {
            None => break,
            Some(Message::Extended { ext_id: 0, payload }) => {
                // 对端扩展握手：解析其 ut_metadata ext_id
                if let Ok(v) = xfer_bencode::decode(&payload) {
                    if let Some(d) = v.as_dict() {
                        if let Some(Value::Dict(m)) = d.get(b"m".as_slice()) {
                            if let Some(Value::Int(id)) = m.get(b"ut_metadata".as_slice()) {
                                engine_meta_id = *id as u8;
                            }
                        }
                    }
                }
            }
            Some(Message::Extended { ext_id, payload }) => {
                // 仅处理 ut_metadata 消息（扩展握手声明的 ext_id=1）
                if ext_id != 1 {
                    continue;
                }
                // ut_metadata request → 回 data 分片
                if let Ok(v) = xfer_bencode::decode(&payload) {
                    if let Some(d) = v.as_dict() {
                        let msg_type = d
                            .get(b"msg_type".as_slice())
                            .and_then(Value::as_int)
                            .unwrap_or(-1);
                        if msg_type == 0 {
                            let piece = d
                                .get(b"piece".as_slice())
                                .and_then(Value::as_int)
                                .unwrap_or(0) as usize;
                            let start = piece * META_PIECE;
                            let end = (start + META_PIECE).min(info_bytes.len());
                            if start >= info_bytes.len() {
                                continue;
                            }
                            let seg = &info_bytes[start..end];
                            let mut head = BTreeMap::new();
                            head.insert(b"msg_type".to_vec(), Value::Int(1));
                            head.insert(b"piece".to_vec(), Value::Int(piece as i64));
                            head.insert(
                                b"total_size".to_vec(),
                                Value::Int(info_bytes.len() as i64),
                            );
                            let mut body = encode(&Value::Dict(head));
                            body.extend_from_slice(seg);
                            stream
                                .write_all(
                                    &Message::Extended {
                                        // BEP 10：使用引擎广告的 ext_id
                                        ext_id: engine_meta_id,
                                        payload: body,
                                    }
                                    .encode(),
                                )
                                .await?;
                        }
                    }
                }
            }
            Some(Message::Request {
                index,
                begin,
                length,
            }) => {
                let off = index as usize * PIECE_LEN + begin as usize;
                let end = (off + length as usize).min(seed.len());
                if off >= seed.len() {
                    continue;
                }
                let block = seed[off..end].to_vec();
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
    Query(_q): Query<std::collections::HashMap<String, String>>,
    State(seed): State<Arc<RwLock<Option<SocketAddr>>>>,
) -> Response {
    let Some(addr) = *seed.read().unwrap() else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "no seed").into_response();
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

async fn start_tracker() -> (SocketAddr, Arc<RwLock<Option<SocketAddr>>>) {
    let state: Arc<RwLock<Option<SocketAddr>>> = Arc::new(RwLock::new(None));
    let app = axum::Router::new()
        .route("/announce", get(tracker_announce))
        .with_state(state.clone());
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(l, app).await;
    });
    (addr, state)
}

#[tokio::test]
async fn magnet_download_end_to_end() {
    // 数据 + 合法 info 字典（piece 哈希真实计算）
    let data: Vec<u8> = (0..(3 * PIECE_LEN + 77)).map(|i| (i % 97) as u8).collect();
    let pieces: Vec<u8> = data.chunks(PIECE_LEN).flat_map(sha1_of).collect();
    let info = dict(BTreeMap::from([
        (b"name".to_vec(), bytes("data.bin")),
        (b"piece length".to_vec(), int(PIECE_LEN as i64)),
        (b"length".to_vec(), int(data.len() as i64)),
        (b"pieces".to_vec(), bytes(pieces)),
    ]));
    let info_bytes = encode(&info);
    let info_hash = sha1_of(&info_bytes);

    // seed server（元数据 + 数据）
    let (sl, saddr) = bind_random().await;
    let conns = Arc::new(AtomicUsize::new(0));
    tokio::spawn(serve_magnet_seed(
        sl,
        Arc::new(data.clone()),
        Arc::new(info_bytes.clone()),
        InfoHash::from_bytes(&info_hash),
        PeerId::azureus_prefix(&[9u8; 12]),
        conns.clone(),
    ));

    // tracker 返回 seed 地址
    let (taddr, seed_ref) = start_tracker().await;
    *seed_ref.write().unwrap() = Some(saddr);

    // 磁力引擎：只有 info_hash + tracker URL
    let dir = std::env::temp_dir().join(format!("magnet-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = TorrentConfig {
        dir: dir.clone(),
        peer_id: PeerId::azureus_prefix(&[3u8; 12]),
        listen_port: 0,
        max_peers: 8,
        adaptive: false,
        numwant: 50,
        announce_urls: vec![format!("http://{taddr}/announce")],
        udp_announce_urls: Vec::new(),
        pipeline: 0,
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
    let engine = TorrentEngine::new_magnet(info_hash, cfg).unwrap();
    let cancel = CancellationToken::new();
    let r = engine.clone().run(cancel).await;
    assert!(r.is_ok(), "磁力下载失败: {r:?}");

    // 元数据就绪 + 文件逐字节一致
    assert!(engine.has_metadata());
    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 回归测试：元数据就绪后必须 **复用同一条连接** 继续下载，不能断开重连。
///
/// 曾经的实现在 `run_metadata_exchange` 成功后 `return`，导致所有正在做元数据
/// 交换的连接全部关闭；真实网络下必须等下一轮 announce（tracker interval 常为
/// 30~60 分钟）才能重连，表现为"有节点但速度恒为 0"。
/// 同时覆盖第二个缺陷：元数据阶段收到的对端 bitfield 不得丢弃，否则片集合为空
/// → 选片失败 → 同样无速度。
#[tokio::test]
async fn magnet_reuses_connection_after_metadata() {
    let data: Vec<u8> = (0..(2 * PIECE_LEN + 11)).map(|i| (i % 251) as u8).collect();
    let pieces: Vec<u8> = data.chunks(PIECE_LEN).flat_map(sha1_of).collect();
    let info = dict(BTreeMap::from([
        (b"name".to_vec(), bytes("data.bin")),
        (b"piece length".to_vec(), int(PIECE_LEN as i64)),
        (b"length".to_vec(), int(data.len() as i64)),
        (b"pieces".to_vec(), bytes(pieces)),
    ]));
    let info_bytes = encode(&info);
    let info_hash = sha1_of(&info_bytes);

    let (sl, saddr) = bind_random().await;
    let conns = Arc::new(AtomicUsize::new(0));
    tokio::spawn(serve_magnet_seed(
        sl,
        Arc::new(data.clone()),
        Arc::new(info_bytes.clone()),
        InfoHash::from_bytes(&info_hash),
        PeerId::azureus_prefix(&[7u8; 12]),
        conns.clone(),
    ));

    let (taddr, seed_ref) = start_tracker().await;
    *seed_ref.write().unwrap() = Some(saddr);

    let dir = std::env::temp_dir().join(format!("magnet-reuse-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = TorrentConfig {
        dir: dir.clone(),
        peer_id: PeerId::azureus_prefix(&[5u8; 12]),
        listen_port: 0,
        max_peers: 8,
        adaptive: false,
        numwant: 50,
        announce_urls: vec![format!("http://{taddr}/announce")],
        udp_announce_urls: Vec::new(),
        pipeline: 0,
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
    let engine = TorrentEngine::new_magnet(info_hash, cfg).unwrap();
    let r = engine.clone().run(CancellationToken::new()).await;
    assert!(r.is_ok(), "磁力下载失败: {r:?}");

    // 关键断言：整轮下载只应建立 1 条连接（元数据交换 + 数据下载复用同一条）
    assert_eq!(
        conns.load(Ordering::SeqCst),
        1,
        "元数据就绪后不应断开重连：期望 1 次连接，实际 {} 次",
        conns.load(Ordering::SeqCst)
    );
    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data);
    let _ = std::fs::remove_dir_all(&dir);
}
