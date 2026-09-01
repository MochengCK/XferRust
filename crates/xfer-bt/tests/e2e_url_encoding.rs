//! 诊断测试：验证 reqwest 对 tracker announce URL 不会进行二次编码。
//!
//! 这个测试通过创建一个原始 TCP tracker 来捕获 reqwest 实际发送的 HTTP 请求行，
//! 检查 info_hash 参数是否被正确编码（单次编码，非双重编码）。

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use xfer_bt::tracker::{announce, AnnounceRequest};
use xfer_types::{InfoHash, PeerId};

/// 测试：验证 reqwest 发送的 announce 请求行中 info_hash 未被双重编码。
#[tokio::test]
async fn reqwest_does_not_double_encode_info_hash() {
    // 启动一个原始 TCP "tracker" 来捕获 HTTP 请求行
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // info_hash 包含特殊字节：0x00, 0xFF, 0x20(空格), 0x25(%), 0x26(&)
    let raw_ih = [
        0x00u8, 0xFF, 0x20, 0x25, 0x26, 0x3F, 0x23, 0x2B, 0x7E, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46,
        0x47, 0x48, 0x49, 0x4A, 0x4B,
    ];
    let info_hash = InfoHash::from_bytes(&raw_ih);
    let peer_id = PeerId::azureus_prefix(&[0u8; 12]);

    // 构造期望的 percent-encoded info_hash
    let expected_encoded: String = raw_ih
        .iter()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                format!("{}", *b as char)
            }
            _ => format!("%{b:02X}"),
        })
        .collect();

    println!("raw info_hash bytes: {raw_ih:02X?}");
    println!("expected encoded: {expected_encoded}");

    let captured_request: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
    let captured_clone = captured_request.clone();

    // 启动 tracker 服务
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8192];
        let n = stream.read(&mut buf).await.unwrap();
        let request = String::from_utf8_lossy(&buf[..n]).to_string();
        println!("captured HTTP request:\n{request}");
        *captured_clone.lock().unwrap() = request;

        // 返回一个最小的合法 bencode 响应
        let body = b"d8:intervali60e8:completei1e5:peers6:\x7f\x00\x00\x01\x1a\xe1e";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
    });

    // 使用 reqwest 发送 announce 请求
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let url = format!("http://{addr}/announce");
    let req = AnnounceRequest {
        info_hash: &info_hash,
        peer_id: &peer_id,
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: 1024,
        event: Some("started"),
        numwant: 50,
    };

    let result = announce(&client, &url, &req).await;
    assert!(result.is_ok(), "announce 应成功: {:?}", result);

    // 检查捕获的请求
    let captured = captured_request.lock().unwrap().clone();

    // 验证 info_hash 参数在请求行中只被编码了一次
    // 如果双重编码，%25 会变成 %2525
    if captured.contains("%2525") || captured.contains("%2500") {
        panic!("检测到双重编码！请求行: {captured}");
    }

    // 验证请求行包含正确的 percent-encoded info_hash
    // 检查 GET 请求行中包含 info_hash= 参数
    let get_line = captured.lines().next().unwrap_or("");
    println!("GET line: {get_line}");

    assert!(
        get_line.contains("info_hash="),
        "请求行应包含 info_hash 参数: {get_line}"
    );

    // 提取 info_hash 参数值
    if let Some(idx) = get_line.find("info_hash=") {
        let rest = &get_line[idx + "info_hash=".len()..];
        let end = rest.find('&').unwrap_or(rest.len());
        let actual_encoded = &rest[..end];
        println!("actual info_hash in request: {actual_encoded}");
        println!("expected info_hash:          {expected_encoded}");

        assert_eq!(
            actual_encoded, expected_encoded,
            "info_hash 编码不匹配！\n期望: {expected_encoded}\n实际: {actual_encoded}"
        );
    } else {
        panic!("请求行中未找到 info_hash 参数");
    }
}

/// 测试：验证 tracker URL 中已包含 query 参数时正确拼接。
#[tokio::test]
async fn tracker_url_with_query_uses_ampersand() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let captured_request: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
    let captured_clone = captured_request.clone();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 8192];
        let n = stream.read(&mut buf).await.unwrap();
        let request = String::from_utf8_lossy(&buf[..n]).to_string();
        *captured_clone.lock().unwrap() = request;

        let body = b"d8:intervali60e5:peers0:e";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
    });

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    // URL 已包含 query 参数
    let url = format!("http://{addr}/announce?passkey=secret&source=test");
    let info_hash = InfoHash::from_bytes(&[0x42u8; 20]);
    let peer_id = PeerId::azureus_prefix(&[0u8; 12]);
    let req = AnnounceRequest {
        info_hash: &info_hash,
        peer_id: &peer_id,
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: 1024,
        event: None,
        numwant: 50,
    };

    let result = announce(&client, &url, &req).await;
    assert!(result.is_ok(), "announce 应成功: {:?}", result);

    let captured = captured_request.lock().unwrap().clone();
    let get_line = captured.lines().next().unwrap_or("");

    println!("GET line with existing query: {get_line}");

    // 验证使用 & 拼接（而非 ?）
    assert!(
        get_line.contains("&info_hash="),
        "应使用 & 拼接 info_hash 参数: {get_line}"
    );
    assert!(
        get_line.contains("passkey=secret"),
        "应保留原有 passkey 参数: {get_line}"
    );
}
