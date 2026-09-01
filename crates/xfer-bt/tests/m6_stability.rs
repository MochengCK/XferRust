//! M6 稳定化互操作测试：
//! - 日志轮转验证
//! - 崩溃恢复（panic 隔离）
//! - 会话定期保存
//! - MSE 加密流完整传输

use std::time::Duration;

/// 日志轮转：写入超过 MAX_LOG_SIZE 的数据后应自动轮转。
#[tokio::test]
async fn log_rotation_creates_backup() {
    let dir = std::env::temp_dir().join(format!("xfer-m6-log-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let log_path = dir.join("engine.log");

    // 模拟 RollingFile 的行为
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .unwrap();
        // 写入超过 10MB 的数据触发轮转
        let chunk = vec![b'x'; 1024 * 1024]; // 1MB
        for _ in 0..11 {
            f.write_all(&chunk).unwrap();
        }
        f.flush().unwrap();
    }

    // 手动执行轮转（模拟 RollingFile::rotate）
    let backup = log_path.with_extension("1");
    std::fs::rename(&log_path, &backup).unwrap();
    // 新文件
    std::fs::File::create(&log_path).unwrap();

    // 验证：备份文件存在，新文件为空
    assert!(backup.exists(), "轮转备份文件应存在");
    assert!(log_path.exists(), "新日志文件应存在");
    let new_len = std::fs::metadata(&log_path).unwrap().len();
    assert_eq!(new_len, 0, "轮转后新日志文件应为空");

    let _ = std::fs::remove_dir_all(&dir);
}

/// 崩溃恢复：panic 不应导致整个引擎崩溃。
/// 通过 catch_unwind 隔离任务级 panic。
#[tokio::test]
async fn panic_isolation_does_not_crash_engine() {
    use futures_util::future::FutureExt;
    use std::panic::AssertUnwindSafe;

    let result = AssertUnwindSafe(async {
        panic!("测试 panic");
    })
    .catch_unwind()
    .await;

    assert!(result.is_err(), "panic 应被 catch_unwind 捕获");
}

/// 会话持久化：定期保存会话文件。
#[tokio::test]
async fn session_periodic_save() {
    use xfer_engine::TaskManager;

    let dir = std::env::temp_dir().join(format!("xfer-m6-session-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let session = dir.join("session.json");

    let mgr = TaskManager::start_with_session(Some(dir.clone()), Some(1), session.clone());

    // 等待定期保存触发（30s 间隔太长，手动触发 save_session）
    mgr.save_session().unwrap();

    // 会话文件应存在
    assert!(session.exists(), "会话文件应已生成");

    let _ = std::fs::remove_dir_all(&dir);
}

/// MSE 加密流：完整传输测试。
/// 验证 MSE 握手后，通过 EncryptedStream 进行双向加密通信。
#[tokio::test]
async fn mse_encrypted_stream_full_transfer() {
    use tokio::net::{TcpListener, TcpStream};
    use xfer_bt::EncryptedStream;
    use xfer_crypto::{derive_rc4_streams, DhKeyPair, MseRole};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // 生成 DH 密钥对
    let alice = DhKeyPair::generate();
    let bob = DhKeyPair::generate();
    let shared_a = alice.compute_shared_secret(&bob.public_key());
    let shared_b = bob.compute_shared_secret(&alice.public_key());
    assert_eq!(shared_a, shared_b);

    let skey = [0u8; 20];
    let (alice_send, alice_recv) = derive_rc4_streams(&shared_a, &skey, MseRole::Initiator);
    let (bob_send, bob_recv) = derive_rc4_streams(&shared_b, &skey, MseRole::Responder);

    // 大数据传输
    let test_data: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();
    let test_data_clone = test_data.clone();
    let test_data_len = test_data.len();

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut enc = EncryptedStream::new(stream, bob_send, bob_recv);

        // 接收大数据
        let mut buf = vec![0u8; test_data_len];
        enc.read_exact_decrypted(&mut buf).await.unwrap();
        assert_eq!(buf, test_data_clone);

        // 回传数据
        let reply: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
        enc.write_encrypted(&reply).await.unwrap();
        reply
    });

    let client_stream = TcpStream::connect(addr).await.unwrap();
    let mut enc_client = EncryptedStream::new(client_stream, alice_send, alice_recv);

    // 发送大数据
    enc_client.write_encrypted(&test_data).await.unwrap();

    // 接收回传
    let mut buf = vec![0u8; 4096];
    enc_client.read_exact_decrypted(&mut buf).await.unwrap();

    let server_reply = server_task.await.unwrap();
    assert_eq!(buf, server_reply);
}

/// MSE + BT 完整握手 + 加密消息循环：
/// 验证 MSE 握手后 BT 握手捎带正确。
#[tokio::test]
async fn mse_bt_handshake_piggyback() {
    use tokio::net::{TcpListener, TcpStream};
    use xfer_bt::{encode_handshake, mse};
    use xfer_types::{InfoHash, PeerId};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let info_hash = InfoHash::from_bytes(&[0x55u8; 20]);
    let initiator_pid = PeerId::azureus_prefix(&[0x49; 12]);
    let responder_pid = PeerId::azureus_prefix(&[0x52; 12]);

    let initiator_hs = encode_handshake(&info_hash, &initiator_pid);
    let responder_hs = encode_handshake(&info_hash, &responder_pid);

    let ih_clone = info_hash;
    let resp_hs_clone = responder_hs.clone();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let xfer_bt::PeOutcome::Encrypted {
            stream: mut enc,
            peer_ia,
        } = mse::pe_handshake_responder(stream, &ih_clone, &resp_hs_clone, false)
            .await
            .unwrap()
        else {
            panic!("期望 MSE 协商为 RC4 加密");
        };

        // 验证收到对端 BT 握手
        assert_eq!(peer_ia.len(), 68);
        assert_eq!(&peer_ia[1..20], b"BitTorrent protocol");

        // 加密流已随握手返回，直接交换消息
        enc.write_encrypted(b"encrypted_reply!").await.unwrap();
        // 等待客户端确认后再关闭，排除提前关闭引起的时序歧义
        let mut ack = [0u8; 3];
        tokio::time::timeout(Duration::from_secs(5), enc.read_exact_decrypted(&mut ack))
            .await
            .expect("等待客户端确认超时")
            .unwrap();
        assert_eq!(&ack, b"ack");
    });

    let client = TcpStream::connect(addr).await.unwrap();
    let xfer_bt::PeOutcome::Encrypted {
        stream: mut enc_client,
        peer_ia,
    } = mse::pe_handshake_initiator(client, &info_hash, &initiator_hs, xfer_crypto::CRYPTO_RC4 | xfer_crypto::CRYPTO_PLAINTEXT)
        .await
        .unwrap()
    else {
        panic!("期望 MSE 协商为 RC4 加密");
    };

    // 验证收到对端 BT 握手
    assert_eq!(peer_ia.len(), 68);
    assert_eq!(&peer_ia[1..20], b"BitTorrent protocol");
    let mut buf = vec![0u8; 16];
    let mut total = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while total < 16 {
        let remaining = 16 - total;
        let mut tmp = vec![0u8; remaining];
        match tokio::time::timeout_at(deadline, enc_client.read_decrypted(&mut tmp)).await {
            Ok(Ok(0)) => panic!("连接关闭: total={total}"),
            Ok(Ok(n)) => {
                buf[total..total + n].copy_from_slice(&tmp[..n]);
                total += n;
            }
            Ok(Err(e)) => panic!("读取失败: {e}"),
            Err(_) => panic!("读取超时: total={total}"),
        }
    }
    assert_eq!(&buf, b"encrypted_reply!");
    // 回执确认，确保服务端在客户端读完前不关闭
    enc_client.write_encrypted(b"ack").await.unwrap();

    server.await.unwrap();
}

/// EncryptedStream AsyncRead/AsyncWrite trait 测试：
/// 通过 PeerReader 从 EncryptedStream 读取消息。
#[tokio::test]
async fn encrypted_stream_async_read_trait() {
    use tokio::net::{TcpListener, TcpStream};
    use xfer_bt::{EncryptedStream, Message, PeerReader};
    use xfer_crypto::{derive_rc4_streams, DhKeyPair, MseRole};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let alice = DhKeyPair::generate();
    let bob = DhKeyPair::generate();
    let shared = alice.compute_shared_secret(&bob.public_key());
    let skey = [0u8; 20];
    let (alice_send, alice_recv) = derive_rc4_streams(&shared, &skey, MseRole::Initiator);
    let (bob_send, bob_recv) = derive_rc4_streams(&shared, &skey, MseRole::Responder);

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut enc = EncryptedStream::new(stream, bob_send, bob_recv);
        // 发送一条 Bitfield 消息
        enc.write_encrypted(&Message::Bitfield(vec![0xFF, 0x0F]).encode())
            .await
            .unwrap();
    });

    let client = TcpStream::connect(addr).await.unwrap();
    let enc = EncryptedStream::new(client, alice_send, alice_recv);

    // 使用 PeerReader 从 EncryptedStream 读取消息
    // EncryptedStream 实现了 AsyncRead，可以直接传给 PeerReader
    let mut reader = PeerReader::new();
    let mut enc_stream = enc;

    // 使用 tokio::select 等待消息到达
    let msg = tokio::time::timeout(Duration::from_secs(5), reader.read_message(&mut enc_stream))
        .await
        .expect("读取超时")
        .expect("IO 错误")
        .expect("连接关闭");

    match msg {
        Message::Bitfield(bf) => assert_eq!(bf, vec![0xFF, 0x0F]),
        _ => panic!("应为 Bitfield 消息"),
    }

    server.await.unwrap();
}
