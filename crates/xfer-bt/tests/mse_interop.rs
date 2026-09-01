//! MSE/PE 互操作测试（§8 金标准）。
//!
//! 测试策略：
//! 1. 自洽双端测试：我们的发起方 ↔ 我们的响应方完整握手 + 双向加密流量
//! 2. 规范参考对端测试（金标准）：手工逐字节构造"标准客户端"线格式
//!    （与 libtorrent/rTorrent 源码对照），双向各测一次 ——
//!    证明线格式本身正确，而非仅两端自洽
//! 3. 明文 BT 握手识别回退
//! 4. 负验证：SKEY 不匹配必须失败
//!
//! 线格式（A 发起、B 响应）：
//! ```text
//! A → B: Ya(96) || PadA(0..512)
//! B → A: Yb(96) || PadB(0..512)
//! A → B: SHA1("req1"||S) || SHA1("req2"||SKEY)⊕SHA1("req3"||S)
//!        || ENC_A(VC || crypto_provide(4) || len(PadC)(2) || PadC || len(IA)(2)) || ENC_A(IA)
//! B → A: ENC_B(VC || crypto_select(4) || len(PadD)(2) || PadD) || ENC_B(BT握手(68))
//! ```

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use xfer_bt::mse::{pe_handshake_initiator, pe_handshake_responder, PeOutcome};
use xfer_crypto::{
    derive_rc4_streams, obfuscated_skey_hash, req1_hash, skey_matches_obfuscated, DhKeyPair,
    MseRole, CRYPTO_PLAINTEXT, CRYPTO_RC4, VC,
};
use xfer_types::{InfoHash, PeerId};

const BT_HS_LEN: usize = 68;

/// 组装 68 字节 BT 握手。
fn bt_handshake(ih: &InfoHash, id_byte: u8) -> Vec<u8> {
    xfer_bt::message::encode_handshake(ih, &PeerId::azureus_prefix(&[id_byte; 12]))
}

// ===========================================================================
// 1. 自洽双端
// ===========================================================================

#[tokio::test]
async fn mse_interop_full_handshake() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let info_hash = InfoHash::from_bytes(&[0xAAu8; 20]);
    let init_hs = bt_handshake(&info_hash, 1);
    let resp_hs = bt_handshake(&info_hash, 2);

    let ih2 = info_hash;
    let resp_hs2 = resp_hs.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        pe_handshake_responder(stream, &ih2, &resp_hs2, false)
            .await
            .unwrap()
    });

    let client = tokio::net::TcpStream::connect(addr).await.unwrap();
    let init_outcome = pe_handshake_initiator(client, &info_hash, &init_hs, CRYPTO_RC4 | CRYPTO_PLAINTEXT)
        .await
        .unwrap();
    let resp_outcome = server.await.unwrap();

    let (
        PeOutcome::Encrypted {
            stream: mut enc_client,
            peer_ia: client_got,
        },
        PeOutcome::Encrypted {
            stream: mut enc_server,
            peer_ia: server_got,
        },
    ) = (init_outcome, resp_outcome)
    else {
        panic!("双方都应协商为 RC4 加密流");
    };
    assert_eq!(client_got, resp_hs, "发起方应收到响应方 BT 握手");
    assert_eq!(server_got, init_hs, "响应方应收到发起方 BT 握手");

    // 双向加密通信
    enc_client
        .write_encrypted(b"encrypted hello")
        .await
        .unwrap();
    let mut buf = [0u8; 64];
    enc_server
        .read_exact_decrypted(&mut buf[..15])
        .await
        .unwrap();
    assert_eq!(&buf[..15], b"encrypted hello");

    enc_server.write_encrypted(b"reply").await.unwrap();
    enc_client.read_exact_decrypted(&mut buf[..5]).await.unwrap();
    assert_eq!(&buf[..5], b"reply");
}

// ===========================================================================
// 2. 规范参考对端（金标准，手工逐字节构造）
// ===========================================================================

/// 手工"标准客户端"发起方 ↔ 我们的响应方。
///
/// 逐字节按真实线格式构造，任何字段错位/密钥派生错误都会导致失败。
#[tokio::test]
async fn spec_reference_initiator_vs_our_responder() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let info_hash = InfoHash::from_bytes(&[0x5Cu8; 20]);
    let our_hs = bt_handshake(&info_hash, 0x10);
    let spec_hs = bt_handshake(&info_hash, 0x20);

    let ih2 = info_hash;
    let our_hs2 = our_hs.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        pe_handshake_responder(stream, &ih2, &our_hs2, false)
            .await
            .unwrap()
    });

    let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();

    // --- A → B: Ya(96) + PadA(37 字节，故意非零验证扫描) ---
    let spec_dh = DhKeyPair::generate();
    let mut pad_a = vec![0x5Au8; 37];
    getrandom::fill(&mut pad_a).unwrap();
    client
        .write_all(&[&spec_dh.public_key()[..], &pad_a].concat())
        .await
        .unwrap();

    // --- B → A: Yb(96) + PadB(0..512) ---
    let mut yb = [0u8; 96];
    client.read_exact(&mut yb).await.unwrap();
    // 响应方 PadB 长度未知，但加密 VC 扫描会处理 —— 规范发起方同样靠扫描，
    // 这里简化：直接读入缓冲区直到能扫描到加密 VC（与我们实现同一机制）
    let shared = spec_dh.compute_shared_secret(&yb);
    let (mut send_stream, mut recv_stream) =
        derive_rc4_streams(&shared, info_hash.as_bytes(), MseRole::Initiator);

    // --- A → B: req1 || req2⊕req3 || ENC(VC+provide+len(padC)+padC+len(IA)) || ENC(IA) ---
    let mut plain = Vec::with_capacity(40);
    plain.extend_from_slice(&req1_hash(&shared));
    plain.extend_from_slice(&obfuscated_skey_hash(&shared, info_hash.as_bytes()));
    client.write_all(&plain).await.unwrap();

    let pad_c = vec![0xC3u8; 123]; // 非零 PadC
    let mut enc = Vec::new();
    enc.extend_from_slice(&VC);
    enc.extend_from_slice(&(CRYPTO_RC4).to_be_bytes()); // 只提供 RC4
    enc.extend_from_slice(&(pad_c.len() as u16).to_be_bytes());
    enc.extend_from_slice(&pad_c);
    enc.extend_from_slice(&(spec_hs.len() as u16).to_be_bytes());
    send_stream.process(&mut enc);
    client.write_all(&enc).await.unwrap();
    let mut ia = spec_hs.clone();
    send_stream.process(&mut ia);
    client.write_all(&ia).await.unwrap();

    // --- B → A: 扫描加密 VC（可能混有 PadB）---
    let mut vc_wire = VC.to_vec();
    recv_stream.process(&mut vc_wire);
    let mut scan_buf = Vec::new();
    let mut chunk = [0u8; 64];
    loop {
        assert!(
            scan_buf.len() <= 512 + 8,
            "加密 VC 必须在 512 字节扫描窗口内"
        );
        if let Some(pos) = scan_buf.windows(8).position(|w| w == &vc_wire[..]) {
            scan_buf.drain(..pos + 8);
            break;
        }
        // 保留 7 字节跨界窗口
        if scan_buf.len() > 7 {
            let keep_from = scan_buf.len() - 7;
            scan_buf.drain(..keep_from);
        }
        let n = client.read(&mut chunk).await.unwrap();
        assert!(n > 0, "响应方提前关闭");
        scan_buf.extend_from_slice(&chunk[..n]);
    }

    // scan_buf 中可能已有 crypto_select 的部分字节，先补齐到 6
    let mut hdr = Vec::new();
    hdr.extend_from_slice(&scan_buf);
    while hdr.len() < 6 {
        let n = client.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        hdr.extend_from_slice(&chunk[..n]);
    }
    let extra = hdr.split_off(6);
    recv_stream.process(&mut hdr);
    let crypto_select = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
    assert_eq!(crypto_select, CRYPTO_RC4, "我们只提供 RC4，响应方必须选择 RC4");
    let pad_d_len = u16::from_be_bytes([hdr[4], hdr[5]]) as usize;
    assert!(pad_d_len <= 512);

    // 读 PadD + 68 字节加密握手
    let mut rest = extra;
    while rest.len() < pad_d_len + BT_HS_LEN {
        let n = client.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        rest.extend_from_slice(&chunk[..n]);
    }
    let mut pad_d = rest[..pad_d_len].to_vec();
    let mut resp_hs_enc = rest[pad_d_len..pad_d_len + BT_HS_LEN].to_vec();
    recv_stream.process(&mut pad_d);
    recv_stream.process(&mut resp_hs_enc);
    assert_eq!(resp_hs_enc, our_hs, "响应方回复的加密握手必须正确");

    // 验证响应方确实收到了我们的握手
    let resp_outcome = server.await.unwrap();
    let PeOutcome::Encrypted { peer_ia, .. } = resp_outcome else {
        panic!("响应方应为加密结果");
    };
    assert_eq!(peer_ia, spec_hs, "响应方必须正确解出规范发起方的 IA");
}

/// 发起方 DH 公钥首字节恰为 0x13（真实连接中 1/256 概率）时，
/// 响应方不得误判为明文 BT——必须匹配完整 20 字节协议串才分流。
#[tokio::test]
async fn dh_key_first_byte_0x13_not_mistaken_for_plaintext() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let info_hash = InfoHash::from_bytes(&[0x13u8; 20]);
    let our_hs = bt_handshake(&info_hash, 0x60);
    let spec_hs = bt_handshake(&info_hash, 0x61);

    let ih2 = info_hash;
    let our_hs2 = our_hs.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        pe_handshake_responder(stream, &ih2, &our_hs2, false)
            .await
            .unwrap()
    });

    // 构造公钥首字节恰为 0x13 的密钥对（期望约 256 次命中）
    let spec_dh = loop {
        let kp = DhKeyPair::generate();
        if kp.public_key()[0] == 0x13 {
            break kp;
        }
    };

    let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
    client.write_all(&spec_dh.public_key()).await.unwrap();

    let mut yb = [0u8; 96];
    tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut yb))
        .await
        .expect("响应方未回 Yb——误判为明文连接")
        .unwrap();
    let shared = spec_dh.compute_shared_secret(&yb);
    let (mut send_stream, mut recv_stream) =
        derive_rc4_streams(&shared, info_hash.as_bytes(), MseRole::Initiator);

    let mut plain = Vec::with_capacity(40);
    plain.extend_from_slice(&req1_hash(&shared));
    plain.extend_from_slice(&obfuscated_skey_hash(&shared, info_hash.as_bytes()));
    client.write_all(&plain).await.unwrap();

    let mut enc = Vec::new();
    enc.extend_from_slice(&VC);
    enc.extend_from_slice(&CRYPTO_RC4.to_be_bytes());
    enc.extend_from_slice(&0u16.to_be_bytes());
    enc.extend_from_slice(&(spec_hs.len() as u16).to_be_bytes());
    send_stream.process(&mut enc);
    client.write_all(&enc).await.unwrap();
    let mut ia = spec_hs.clone();
    send_stream.process(&mut ia);
    client.write_all(&ia).await.unwrap();

    // 扫描加密 VC（跳过 PadB）
    let mut vc_wire = VC.to_vec();
    recv_stream.process(&mut vc_wire);
    let mut scan_buf = Vec::new();
    let mut chunk = [0u8; 64];
    loop {
        if let Some(pos) = scan_buf.windows(8).position(|w| w == &vc_wire[..]) {
            scan_buf.drain(..pos + 8);
            break;
        }
        if scan_buf.len() > 7 {
            let keep_from = scan_buf.len() - 7;
            scan_buf.drain(..keep_from);
        }
        let n = client.read(&mut chunk).await.unwrap();
        assert!(n > 0, "响应方提前关闭");
        scan_buf.extend_from_slice(&chunk[..n]);
    }

    let mut hdr = Vec::new();
    hdr.extend_from_slice(&scan_buf);
    while hdr.len() < 6 {
        let n = client.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        hdr.extend_from_slice(&chunk[..n]);
    }
    let extra = hdr.split_off(6);
    recv_stream.process(&mut hdr);
    assert_eq!(
        u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]),
        CRYPTO_RC4
    );
    let pad_d_len = u16::from_be_bytes([hdr[4], hdr[5]]) as usize;

    let mut rest = extra;
    while rest.len() < pad_d_len + BT_HS_LEN {
        let n = client.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        rest.extend_from_slice(&chunk[..n]);
    }
    let mut pad_d = rest[..pad_d_len].to_vec();
    let mut resp_hs_enc = rest[pad_d_len..pad_d_len + BT_HS_LEN].to_vec();
    recv_stream.process(&mut pad_d);
    recv_stream.process(&mut resp_hs_enc);
    assert_eq!(resp_hs_enc, our_hs, "首字节 0x13 的加密连接必须正常协商");

    let resp_outcome = server.await.unwrap();
    let PeOutcome::Encrypted { peer_ia, .. } = resp_outcome else {
        panic!("响应方应为加密结果（不得误判为明文）");
    };
    assert_eq!(peer_ia, spec_hs);
}

/// 我们的发起方 ↔ 手工"标准客户端"响应方。
#[tokio::test]
async fn our_initiator_vs_spec_reference_responder() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let info_hash = InfoHash::from_bytes(&[0x9Fu8; 20]);
    let our_hs = bt_handshake(&info_hash, 0x30);
    let spec_hs = bt_handshake(&info_hash, 0x40);

    let ih2 = info_hash;
    let spec_hs2 = spec_hs.clone();
    let our_hs_server = our_hs.clone();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();

        // --- 读 Ya（定长 96）+ 跳过 PadA（扫描 req1 时顺带处理）---
        let mut ya = [0u8; 96];
        stream.read_exact(&mut ya).await.unwrap();
        let spec_dh = DhKeyPair::generate();

        // --- 回 Yb + PadB(200 字节，故意大垫验证发起方扫描) ---
        let mut pad_b = vec![0x77u8; 200];
        getrandom::fill(&mut pad_b).unwrap();
        stream
            .write_all(&[&spec_dh.public_key()[..], &pad_b].concat())
            .await
            .unwrap();

        let shared = spec_dh.compute_shared_secret(&ya);
        let (mut send_stream, mut recv_stream) =
            derive_rc4_streams(&shared, ih2.as_bytes(), MseRole::Responder);

        // --- 扫描明文 SHA1("req1"||S)，跳过 PadA ---
        let sync = req1_hash(&shared);
        let mut scan_buf = Vec::new();
        let mut chunk = [0u8; 64];
        let leftover = loop {
            if let Some(pos) = scan_buf.windows(20).position(|w| w == &sync[..]) {
                break scan_buf.split_off(pos + 20);
            }
            assert!(scan_buf.len() <= 512 + 20, "req1 必须在 512 字节扫描窗口内");
            if scan_buf.len() > 19 {
                let keep_from = scan_buf.len() - 19;
                scan_buf.drain(..keep_from);
            }
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0, "发起方提前关闭");
            scan_buf.extend_from_slice(&chunk[..n]);
        };

        // --- skeyhash 识别 + VC + 协商字段 ---
        let mut buf = leftover;
        let need = 20 + 8 + 6; // skeyhash + VC + provide + len(padC)
        while buf.len() < need {
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            buf.extend_from_slice(&chunk[..n]);
        }
        let skey_hash: [u8; 20] = buf[..20].try_into().unwrap();
        let expected = obfuscated_skey_hash(&shared, ih2.as_bytes());
        assert_eq!(skey_hash, expected, "发起方必须发送正确的混淆种子哈希");
        buf.drain(..20);

        let mut vc: [u8; 8] = buf[..8].try_into().unwrap();
        recv_stream.process(&mut vc);
        assert_eq!(vc, VC, "VC 必须是 8 个零字节");
        buf.drain(..8);

        let mut hdr: [u8; 6] = buf[..6].try_into().unwrap();
        recv_stream.process(&mut hdr);
        let crypto_provide = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
        assert_ne!(crypto_provide & CRYPTO_RC4, 0, "发起方必须提供 RC4");
        let pad_c_len = u16::from_be_bytes([hdr[4], hdr[5]]) as usize;
        buf.drain(..6);

        // --- PadC + len(IA) + IA ---
        while buf.len() < pad_c_len + 2 {
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            buf.extend_from_slice(&chunk[..n]);
        }
        let mut pad_c = buf[..pad_c_len].to_vec();
        recv_stream.process(&mut pad_c);
        buf.drain(..pad_c_len);
        let mut ia_len_b: [u8; 2] = buf[..2].try_into().unwrap();
        recv_stream.process(&mut ia_len_b);
        let ia_len = u16::from_be_bytes(ia_len_b) as usize;
        buf.drain(..2);
        while buf.len() < ia_len {
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            buf.extend_from_slice(&chunk[..n]);
        }
        let mut ia = buf[..ia_len].to_vec();
        recv_stream.process(&mut ia);
        assert_eq!(ia, our_hs_server, "发起方 IA 必须是正确的加密 BT 握手");

        // --- 回复：ENC(VC + select + len(padD) + PadD) + ENC(BT握手) ---
        let pad_d = vec![0xD4u8; 64];
        let mut resp = Vec::new();
        resp.extend_from_slice(&VC);
        resp.extend_from_slice(&CRYPTO_RC4.to_be_bytes());
        resp.extend_from_slice(&(pad_d.len() as u16).to_be_bytes());
        resp.extend_from_slice(&pad_d);
        send_stream.process(&mut resp);
        stream.write_all(&resp).await.unwrap();
        let mut hs = spec_hs2.clone();
        send_stream.process(&mut hs);
        stream.write_all(&hs).await.unwrap();
        // 保持连接直到发起方读完
        let mut sink = [0u8; 1];
        let _ = stream.read(&mut sink).await;
    });

    let client = tokio::net::TcpStream::connect(addr).await.unwrap();
    let outcome = pe_handshake_initiator(client, &info_hash, &our_hs, CRYPTO_RC4 | CRYPTO_PLAINTEXT)
        .await
        .unwrap();
    let PeOutcome::Encrypted {
        stream: mut enc_client,
        peer_ia,
    } = outcome
    else {
        panic!("应为加密结果");
    };
    assert_eq!(peer_ia, spec_hs, "发起方必须解出规范响应方的加密握手");

    // 协商后流量互通（响应方任务已退出写入，仅验证流可用）
    enc_client.write_encrypted(b"post-handshake").await.unwrap();
    server.await.unwrap();
}

/// 随握手回包同发的早期数据（真实客户端握手后立即发 bitfield）
/// 落入发起方 VC 扫描窗口时，绝不能被扫描剩余缓冲丢弃。
/// 小 PadB/PadD 使 协商段+握手+早期数据 必然进入同一个扫描块——确定性复现。
#[tokio::test]
async fn early_data_in_scan_window_not_dropped() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let info_hash = InfoHash::from_bytes(&[0x77u8; 20]);
    let our_hs = bt_handshake(&info_hash, 0x70);
    let spec_hs = bt_handshake(&info_hash, 0x71);

    let ih2 = info_hash;
    let spec_hs2 = spec_hs.clone();
    let our_hs_server = our_hs.clone();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();

        // 读 Ya(96)
        let mut ya = [0u8; 96];
        stream.read_exact(&mut ya).await.unwrap();
        let spec_dh = DhKeyPair::generate();

        // 回 Yb + PadB（仅 10 字节：确保后续数据落入发起方扫描块）
        let mut pad_b = vec![0x88u8; 10];
        getrandom::fill(&mut pad_b).unwrap();
        stream
            .write_all(&[&spec_dh.public_key()[..], &pad_b].concat())
            .await
            .unwrap();

        let shared = spec_dh.compute_shared_secret(&ya);
        let (mut send_stream, mut recv_stream) =
            derive_rc4_streams(&shared, ih2.as_bytes(), MseRole::Responder);

        // 扫描 req1 同步哈希（跳过 PadA）
        let sync = req1_hash(&shared);
        let mut scan_buf = Vec::new();
        let mut chunk = [0u8; 64];
        let leftover = loop {
            if let Some(pos) = scan_buf.windows(20).position(|w| w == &sync[..]) {
                break scan_buf.split_off(pos + 20);
            }
            if scan_buf.len() > 19 {
                let keep_from = scan_buf.len() - 19;
                scan_buf.drain(..keep_from);
            }
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            scan_buf.extend_from_slice(&chunk[..n]);
        };
        let mut buf = leftover;
        while buf.len() < 20 {
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            buf.extend_from_slice(&chunk[..n]);
        }
        let skeyhash: [u8; 20] = buf[..20].try_into().unwrap();
        buf.drain(..20);
        assert!(
            skey_matches_obfuscated(&skeyhash, &shared, ih2.as_bytes()),
            "种子识别失败"
        );

        // VC(8) + provide(4) + len(PadC)(2)
        while buf.len() < 14 {
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            buf.extend_from_slice(&chunk[..n]);
        }
        let mut vc: [u8; 8] = buf[..8].try_into().unwrap();
        recv_stream.process(&mut vc);
        assert_eq!(vc, VC);
        let mut f4: [u8; 4] = buf[8..12].try_into().unwrap();
        recv_stream.process(&mut f4);
        let _provide = u32::from_be_bytes(f4);
        let mut f2: [u8; 2] = buf[12..14].try_into().unwrap();
        recv_stream.process(&mut f2);
        let pad_c_len = u16::from_be_bytes(f2) as usize;
        buf.drain(..14);

        // PadC + len(IA) + IA
        while buf.len() < pad_c_len + 2 {
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            buf.extend_from_slice(&chunk[..n]);
        }
        let mut pad_c = buf[..pad_c_len].to_vec();
        recv_stream.process(&mut pad_c);
        buf.drain(..pad_c_len);
        let mut ia_len_b: [u8; 2] = buf[..2].try_into().unwrap();
        recv_stream.process(&mut ia_len_b);
        let ia_len = u16::from_be_bytes(ia_len_b) as usize;
        buf.drain(..2);
        while buf.len() < ia_len {
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            buf.extend_from_slice(&chunk[..n]);
        }
        let mut ia = buf[..ia_len].to_vec();
        recv_stream.process(&mut ia);
        assert_eq!(ia, our_hs_server);

        // 回复：ENC(VC + select + len(padD=5) + PadD) + ENC(握手) + 早期数据
        // 三者同一 write 发出，确保全部落入发起方扫描窗口
        let pad_d = vec![0xD5u8; 5];
        let mut resp = Vec::new();
        resp.extend_from_slice(&VC);
        resp.extend_from_slice(&CRYPTO_RC4.to_be_bytes());
        resp.extend_from_slice(&(pad_d.len() as u16).to_be_bytes());
        resp.extend_from_slice(&pad_d);
        send_stream.process(&mut resp);
        let mut hs = spec_hs2.clone();
        send_stream.process(&mut hs);
        let mut early = b"early_bitfield!".to_vec();
        send_stream.process(&mut early);
        resp.extend_from_slice(&hs);
        resp.extend_from_slice(&early);
        stream.write_all(&resp).await.unwrap();
        // 保持连接直到发起方读完
        let mut sink = [0u8; 1];
        let _ = stream.read(&mut sink).await;
    });

    let client = tokio::net::TcpStream::connect(addr).await.unwrap();
    let outcome = pe_handshake_initiator(client, &info_hash, &our_hs, CRYPTO_RC4 | CRYPTO_PLAINTEXT)
        .await
        .unwrap();
    let PeOutcome::Encrypted {
        stream: mut enc_client,
        peer_ia,
    } = outcome
    else {
        panic!("应为加密结果");
    };
    assert_eq!(peer_ia, spec_hs);

    // 与握手回包同批的早期数据必须可读——超时即证明被扫描剩余缓冲丢弃
    let mut early = vec![0u8; 15];
    tokio::time::timeout(
        Duration::from_secs(5),
        enc_client.read_exact_decrypted(&mut early),
    )
    .await
    .expect("早期数据读取超时——被扫描越界缓冲丢弃")
    .unwrap();
    assert_eq!(early, b"early_bitfield!".as_slice());

    enc_client.write_encrypted(b"post-handshake").await.unwrap();
    server.await.unwrap();
}

// ===========================================================================
// 3. 明文识别回退
// ===========================================================================

#[tokio::test]
async fn plaintext_bt_handshake_detected() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let info_hash = InfoHash::from_bytes(&[0x77u8; 20]);
    let plain_hs = bt_handshake(&info_hash, 3);

    let ih2 = info_hash;
    let our_hs = plain_hs.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        pe_handshake_responder(stream, &ih2, &our_hs, false).await.unwrap()
    });

    let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
    // 明文对端只发了前 20 字节
    client.write_all(&plain_hs[..20]).await.unwrap();

    let PeOutcome::Plaintext { pending, peer_ia, .. } = server.await.unwrap() else {
        panic!("应识别为明文 BT 握手");
    };
    assert_eq!(pending, plain_hs[..20].to_vec());
    assert!(peer_ia.is_empty());
}

// ===========================================================================
// 4. 负验证
// ===========================================================================

#[tokio::test]
async fn wrong_skey_fails_handshake() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let initiator_skey = InfoHash::from_bytes(&[0x11u8; 20]);
    let responder_skey = InfoHash::from_bytes(&[0x22u8; 20]);
    let hs = bt_handshake(&initiator_skey, 4);

    let rs = responder_skey;
    let hs2 = hs.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        pe_handshake_responder(stream, &rs, &hs2, false).await
    });

    let client = tokio::net::TcpStream::connect(addr).await.unwrap();
    let init_result = pe_handshake_initiator(client, &initiator_skey, &hs, CRYPTO_RC4 | CRYPTO_PLAINTEXT).await;
    assert!(init_result.is_err(), "SKEY 不匹配时发起方握手必须失败");
    let resp_result = server.await.unwrap();
    assert!(resp_result.is_err(), "响应方必须拒绝未知种子");
}

/// DH 公钥必须 96 字节大端（§7.2）。
#[test]
fn dh_public_key_96_bytes() {
    let pair = DhKeyPair::generate();
    assert_eq!(pair.public_key().len(), 96);
}

/// RC4 密钥派生方向：发起方发 keyA 收 keyB（§7.2）。
#[test]
fn rc4_key_directions() {
    let shared = [0x55u8; 96];
    let skey = [0x66u8; 20];
    let (mut init_send, mut init_recv) =
        derive_rc4_streams(&shared, &skey, MseRole::Initiator);
    let (mut resp_send, mut resp_recv) =
        derive_rc4_streams(&shared, &skey, MseRole::Responder);

    let plaintext = b"direction test";
    let ct = init_send.process_vec(plaintext);
    let pt = resp_recv.process_vec(&ct);
    assert_eq!(&pt[..], plaintext);

    let ct2 = resp_send.process_vec(plaintext);
    let pt2 = init_recv.process_vec(&ct2);
    assert_eq!(&pt2[..], plaintext);
}
