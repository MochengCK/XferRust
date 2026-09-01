//! BT 断点续传端到端：部分下载 → 取消 → 重启引擎从控制文件恢复。
//!
//! 验证：
//! 1. 阶段一：seed 仅提供前 4 片，下载到位后取消，控制文件（.xfer）已落盘；
//! 2. 阶段二：同一目录新建引擎，从控制文件恢复已完成的片，
//!    只向后半部分发请求（前 4 片请求数为 0），最终文件逐字节一致；
//! 3. 完成后控制文件被清理。

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};

use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use sha1::{Digest, Sha1};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use xfer_bencode::{bytes, dict, encode, int, parse_torrent};
use xfer_bt::message::{encode_handshake, Message, PeerReader};
use xfer_bt::{TorrentConfig, TorrentEngine};
use xfer_types::{InfoHash, PeerId};

const PIECE_LEN: usize = 64 * 1024;
const N_PIECES: usize = 8;

struct Seed {
    data: Arc<Vec<u8>>,
    piece_len: usize,
    /// 每片是否对外提供（bitfield 与 request 响应一致）。
    avail: Vec<bool>,
    /// 每片收到的 request 计数（含重复请求）。
    counts: Arc<Mutex<Vec<u32>>>,
}

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
    // bitfield 仅标记可提供片段
    let n_pieces = seed.data.len().div_ceil(seed.piece_len);
    let mut bf = vec![0u8; n_pieces.div_ceil(8)];
    for i in 0..n_pieces {
        if seed.avail.get(i).copied().unwrap_or(false) {
            bf[i / 8] |= 0x80 >> (i % 8);
        }
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
                if !seed.avail.get(index as usize).copied().unwrap_or(false) {
                    continue;
                }
                seed.counts.lock().unwrap()[index as usize] += 1;
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

fn make_config(dir: &std::path::Path, tracker_url: &str) -> TorrentConfig {
    TorrentConfig {
        dir: dir.to_path_buf(),
        peer_id: PeerId::azureus_prefix(&[3u8; 12]),
        listen_port: 0,
        max_peers: 8,
        adaptive: false,
        numwant: 50,
        announce_urls: vec![tracker_url.to_string()],
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
    }
}

async fn spawn_seed(
    data: Arc<Vec<u8>>,
    avail: Vec<bool>,
    info_hash: InfoHash,
    seed_ref: &Arc<RwLock<Option<SocketAddr>>>,
) -> Arc<Mutex<Vec<u32>>> {
    let counts = Arc::new(Mutex::new(vec![0u32; N_PIECES]));
    let seed = Arc::new(Seed {
        data,
        piece_len: PIECE_LEN,
        avail,
        counts: counts.clone(),
    });
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = Some(saddr);
    tokio::spawn(serve_seed(
        sl,
        seed,
        info_hash,
        PeerId::azureus_prefix(&[9u8; 12]),
    ));
    counts
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_after_cancel_skips_completed_pieces() {
    // 控制文件隔离目录（进程内唯一测试，环境变量安全）
    std::env::set_var(
        "XFER_CTRL_DIR",
        std::env::temp_dir()
            .join(format!("xfer-bt-resume-ctrl-{}", std::process::id()))
            .to_string_lossy()
            .to_string(),
    );

    let dir = std::env::temp_dir().join(format!("xfer-bt-resume-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(N_PIECES * PIECE_LEN))
        .map(|i| (i.wrapping_mul(31) % 251) as u8)
        .collect();

    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb = make_torrent_bytes(&data, &tracker_url);
    let meta = parse_torrent(&tb).unwrap();
    let ih = InfoHash::from_bytes(&meta.info_hash);
    let data_arc = Arc::new(data.clone());
    let ctrl = xfer_storage::ctrl_path(&dir.join("data.bin"));

    // ---------- 阶段一：仅前 4 片可用，下载到位后取消 ----------
    let mut avail = vec![false; N_PIECES];
    for i in 0..4 {
        avail[i] = true;
    }
    let counts1 = spawn_seed(data_arc.clone(), avail, ih, &seed_ref).await;

    let cancel1 = CancellationToken::new();
    let engine1 =
        TorrentEngine::new(meta.clone(), make_config(&dir, &tracker_url)).unwrap();
    let e1 = engine1.clone();
    let c1 = cancel1.clone();
    let run1 = tokio::spawn(async move { e1.run(c1).await });

    // 等待前 4 片全部落盘
    let got4 = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let p = engine1.progress();
            if p.done >= (4 * PIECE_LEN) as u64 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .is_ok();
    assert!(got4, "阶段一未能下载到前 4 片");

    cancel1.cancel();
    let r1 = tokio::time::timeout(std::time::Duration::from_secs(10), run1)
        .await
        .expect("阶段一引擎退出超时")
        .unwrap();
    assert!(r1.is_err(), "取消应使 run 返回错误");
    assert!(ctrl.exists(), "取消后应保留续传控制文件: {ctrl:?}");
    let c1 = counts1.lock().unwrap().clone();
    assert!(c1[..4].iter().sum::<u32>() > 0, "阶段一应有下载请求");
    assert_eq!(c1[4..].iter().sum::<u32>(), 0, "阶段一 seed 不提供后 4 片");

    // ---------- 阶段二：全量 seed + 新引擎，验证恢复 ----------
    let avail2 = vec![true; N_PIECES];
    let counts2 = spawn_seed(data_arc.clone(), avail2, ih, &seed_ref).await;

    let engine2 =
        TorrentEngine::new(meta.clone(), make_config(&dir, &tracker_url)).unwrap();
    // 恢复发生在引擎构造期：进度应直接体现已完成的 4 片
    let p0 = engine2.progress();
    assert_eq!(
        p0.done,
        (4 * PIECE_LEN) as u64,
        "新引擎应从控制文件恢复 4 片进度，实际 done={}",
        p0.done
    );

    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        engine2.clone().run(CancellationToken::new()),
    )
    .await
    .expect("阶段二下载超时")
    .expect("阶段二下载应成功");

    let out = std::fs::read(dir.join("data.bin")).unwrap();
    assert_eq!(out, data, "续传后文件与源数据不一致");

    let c2 = counts2.lock().unwrap().clone();
    assert_eq!(
        c2[..4].iter().sum::<u32>(),
        0,
        "已恢复的前 4 片不应再次请求: {c2:?}"
    );
    assert!(
        c2[4..].iter().all(|&n| n > 0),
        "后 4 片应逐片请求: {c2:?}"
    );
    assert!(!ctrl.exists(), "完成后控制文件应被清理: {ctrl:?}");

    let _ = std::fs::remove_dir_all(&dir);
}
