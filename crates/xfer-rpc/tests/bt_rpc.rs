//! RPC 层 BT：原生 task.add（torrent 形式）与 task.getPeers、
//! 前端兼容 addTorrent/getPeers，经真实 tracker + seed 全链路验证。

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use base64::Engine;
use sha1::{Digest, Sha1};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use xfer_bencode::{bytes, dict, encode, int, parse_torrent};
use xfer_bt::message::{encode_handshake, Message, PeerReader};
use xfer_engine::TaskManager;
use xfer_rpc::Router as RpcRouter;
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

async fn serve_seed(
    listener: TcpListener,
    data: Arc<Vec<u8>>,
    info_hash: InfoHash,
    peer_id: PeerId,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let data = data.clone();
        tokio::spawn(async move {
            let _ = handle_seed_peer(stream, &data, info_hash, peer_id).await;
        });
    }
}

async fn handle_seed_peer(
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
    let mut peers = Vec::with_capacity(6);
    if let std::net::IpAddr::V4(v4) = addr.ip() {
        peers.extend_from_slice(&v4.octets());
    } else {
        peers.extend_from_slice(&[127, 0, 0, 1]);
    }
    peers.extend_from_slice(&addr.port().to_be_bytes());
    let resp = dict(BTreeMap::from([
        (b"interval".to_vec(), int(60)),
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

fn make_torrent_b64(data: &[u8], tracker_url: &str) -> String {
    let pieces: Vec<u8> = data.chunks(PIECE_LEN).flat_map(sha1_of).collect();
    let info = dict(BTreeMap::from([
        (b"name".to_vec(), bytes("rpc-bt.bin")),
        (b"piece length".to_vec(), int(PIECE_LEN as i64)),
        (b"length".to_vec(), int(data.len() as i64)),
        (b"pieces".to_vec(), bytes(pieces)),
    ]));
    let top = dict(BTreeMap::from([
        (b"announce".to_vec(), bytes(tracker_url)),
        (b"info".to_vec(), info),
    ]));
    base64::engine::general_purpose::STANDARD.encode(encode(&top))
}

async fn setup_download_env(data: &[u8]) -> (String, InfoHash) {
    let (taddr, seed_ref) = start_tracker().await;
    let tracker_url = format!("http://{taddr}/announce");
    let tb64 = make_torrent_b64(data, &tracker_url);
    let meta = parse_torrent(
        &base64::engine::general_purpose::STANDARD
            .decode(&tb64)
            .unwrap(),
    )
    .unwrap();
    let ih = InfoHash::from_bytes(&meta.info_hash);
    let data_arc = Arc::new(data.to_vec());
    let (sl, saddr) = bind_random().await;
    *seed_ref.write().unwrap() = Some(saddr);
    tokio::spawn(serve_seed(
        sl,
        data_arc,
        ih,
        PeerId::azureus_prefix(&[0x5A; 12]),
    ));
    (tb64, ih)
}

async fn wait_complete(mgr: &TaskManager, gid: &str) {
    let g = xfer_types::Gid::parse(gid).unwrap();
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let st = mgr.tell_status_native(&g, None).unwrap();
        if st["status"] == "complete" {
            return;
        }
        if st["status"] == "error" {
            panic!("任务进入 error: {:?}", st);
        }
    }
    panic!("30s 内未完成");
}

#[tokio::test]
async fn native_task_add_torrent_and_get_peers() {
    let dir = std::env::temp_dir().join(format!("xfer-rpc-bt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(2 * PIECE_LEN + 333))
        .map(|i| (i % 211) as u8)
        .collect();
    let (tb64, _ih) = setup_download_env(&data).await;

    let mgr = TaskManager::start(dir.clone(), 2);
    let events = mgr.events();
    let router = Arc::new(RpcRouter::new(None, mgr.clone(), events));

    let body = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"task.add","params":{{"torrent":"{tb64}","dir":"{}"}}}}"#,
        dir.display()
    );
    let resp = router.handle(&body).response.expect("应有响应");
    assert_eq!(
        resp["error"],
        serde_json::Value::Null,
        "task.add 失败: {resp}"
    );
    let gid = resp["result"]["gid"].as_str().unwrap().to_string();

    wait_complete(&mgr, &gid).await;

    // 文件校验
    let out = std::fs::read(dir.join("rpc-bt.bin")).unwrap();
    assert_eq!(out, data);

    // getPeers
    let body = format!(
        r#"{{"jsonrpc":"2.0","id":2,"method":"task.getPeers","params":{{"gid":"{gid}"}}}}"#
    );
    let resp = router.handle(&body).response.unwrap();
    assert!(
        !resp["result"].as_array().unwrap().is_empty(),
        "getPeers 应为空数组以外: {resp}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn compat_add_torrent_and_get_peers() {
    let dir = std::env::temp_dir().join(format!("xfer-rpc-bt2-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let data: Vec<u8> = (0..(2 * PIECE_LEN + 55))
        .map(|i| (i.wrapping_mul(17) % 233) as u8)
        .collect();
    let (tb64, _ih) = setup_download_env(&data).await;

    let mgr = TaskManager::start(dir.clone(), 2);
    let events = mgr.events();
    let router = Arc::new(RpcRouter::new(None, mgr.clone(), events));

    // aria2.addTorrent(torrent_b64, {dir}, position)
    let body = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"aria2.addTorrent","params":["{tb64}",{{"dir":"{}"}}]}}"#,
        dir.display()
    );
    let resp = router.handle(&body).response.expect("应有响应");
    assert_eq!(
        resp["error"],
        serde_json::Value::Null,
        "addTorrent 失败: {resp}"
    );
    let gid = resp["result"].as_str().unwrap().to_string();

    wait_complete(&mgr, &gid).await;
    let out = std::fs::read(dir.join("rpc-bt.bin")).unwrap();
    assert_eq!(out, data);

    // aria2.getPeers(gid)
    let body =
        format!(r#"{{"jsonrpc":"2.0","id":2,"method":"aria2.getPeers","params":["{gid}"]}}"#);
    let resp = router.handle(&body).response.unwrap();
    assert!(!resp["result"].as_array().unwrap().is_empty(), "{resp}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn task_add_torrent_invalid_base64_rejected() {
    let dir = std::env::temp_dir().join(format!("xfer-rpc-bt3-{}", std::process::id()));
    let mgr = TaskManager::start(dir.clone(), 2);
    let events = mgr.events();
    let router = Arc::new(RpcRouter::new(None, mgr.clone(), events));
    let resp = router
        .handle(r#"{"jsonrpc":"2.0","id":1,"method":"task.add","params":{"torrent":"!!bad!!"}}"#)
        .response
        .unwrap();
    assert!(
        resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("torrent"),
        "{resp}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
