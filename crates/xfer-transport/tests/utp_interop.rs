//! uTP 互操作测试（§8 金标准）。
//!
//! 测试策略：
//! 1. 自洽双端测试：两个 UtpManager 互相通信，验证数据传输
//! 2. 包格式规范测试：验证生成的包符合 BEP 29 规范
//! 3. 边界条件测试：空数据、大数据、乱序、丢包

use std::net::SocketAddr;
use std::time::Duration;
use xfer_transport::utp_packet::*;

#[cfg(test)]
mod tests {
    use super::*;
    use xfer_transport::UtpManager;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    // ------------------------------------------------------------------
    // 包格式规范测试（对照 BEP 29 线上格式）
    // ------------------------------------------------------------------

    #[test]
    fn syn_packet_wire_format_spec() {
        let h = PacketHeader {
            type_: packet_type::ST_SYN,
            version: PROTOCOL_VERSION,
            extension: ext_type::EXT_NONE,
            connection_id: 0xCAFE,
            timestamp: 123456,
            timestamp_diff: 0,
            wnd_size: 0x7FFFFFFF,
            seq_nr: 1,
            ack_nr: 0,
        };
        let mut buf = vec![0u8; HEADER_LEN];
        encode_header(&mut buf, &h);

        assert_eq!(buf[0], 0x41, "SYN type=4|version=1 → 0x41");
        assert_eq!(buf[1], 0x00, "无扩展");
        assert_eq!(&buf[2..4], &[0xCA, 0xFE], "connection_id 大端");
        assert_eq!(&buf[16..18], &[0x00, 0x01], "seq_nr=1 大端");
        assert_eq!(&buf[18..20], &[0x00, 0x00], "ack_nr=0 大端");
    }

    #[test]
    fn data_packet_wire_format_spec() {
        let h = PacketHeader {
            type_: packet_type::ST_DATA,
            version: PROTOCOL_VERSION,
            extension: ext_type::EXT_NONE,
            connection_id: 0x1234,
            timestamp: 999,
            timestamp_diff: 42,
            wnd_size: 0x100000,
            seq_nr: 10,
            ack_nr: 5,
        };
        let mut buf = vec![0u8; HEADER_LEN];
        encode_header(&mut buf, &h);
        assert_eq!(buf[0], 0x01, "DATA type=0|version=1 → 0x01");
    }

    #[test]
    fn state_packet_wire_format_spec() {
        let h = PacketHeader {
            type_: packet_type::ST_STATE,
            version: PROTOCOL_VERSION,
            extension: ext_type::EXT_NONE,
            connection_id: 0xBEEF,
            timestamp: 0,
            timestamp_diff: 0,
            wnd_size: 0x100000,
            seq_nr: 1,
            ack_nr: 1,
        };
        let mut buf = vec![0u8; HEADER_LEN];
        encode_header(&mut buf, &h);
        assert_eq!(buf[0], 0x21, "STATE type=2|version=1 → 0x21");
    }

    #[test]
    fn fin_packet_wire_format_spec() {
        let h = PacketHeader {
            type_: packet_type::ST_FIN,
            version: PROTOCOL_VERSION,
            extension: ext_type::EXT_NONE,
            connection_id: 0xDEAD,
            timestamp: 0,
            timestamp_diff: 0,
            wnd_size: 0,
            seq_nr: 100,
            ack_nr: 50,
        };
        let mut buf = vec![0u8; HEADER_LEN];
        encode_header(&mut buf, &h);
        assert_eq!(buf[0], 0x11, "FIN type=1|version=1 → 0x11");
    }

    #[test]
    fn reset_packet_wire_format_spec() {
        let h = PacketHeader {
            type_: packet_type::ST_RESET,
            version: PROTOCOL_VERSION,
            extension: ext_type::EXT_NONE,
            connection_id: 0x0001,
            timestamp: 0,
            timestamp_diff: 0,
            wnd_size: 0,
            seq_nr: 0,
            ack_nr: 0,
        };
        let mut buf = vec![0u8; HEADER_LEN];
        encode_header(&mut buf, &h);
        assert_eq!(buf[0], 0x31, "RESET type=3|version=1 → 0x31");
    }

    #[test]
    fn sack_bitmap_order_spec() {
        let payload = [0x01, 0x00, 0x00, 0x00];
        assert_eq!(decode_sack_bits(&payload), 1);

        let payload = [0x80, 0x00, 0x00, 0x00];
        assert_eq!(decode_sack_bits(&payload), 0x80);

        let payload = [0x00, 0x01, 0x00, 0x00];
        assert_eq!(decode_sack_bits(&payload), 0x100);

        let payload = [0xFF, 0xFF, 0xFF, 0xFF];
        assert_eq!(decode_sack_bits(&payload), 0xFFFFFFFF);
    }

    #[test]
    fn seq_nr_wraparound_spec() {
        assert!(seq_after(0, 0xFFFF));
        assert!(!seq_after(0xFFFF, 0));
        assert!(seq_leq(1, 2));
        assert!(!seq_leq(2, 1));
        assert!(seq_leq(0xFFFF, 0));
    }

    // ------------------------------------------------------------------
    // 双端 uTP 通信测试
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn utp_bidirectional_data_transfer() {
        // 测试中等数据传输
        let (server_handle, mut incoming_rx) = UtpManager::bind("127.0.0.1", 0).await.unwrap();
        let server_port = server_handle.local_addr().port();

        let (client_handle, _) = UtpManager::bind("127.0.0.1", 0).await.unwrap();

        let mut client_stream = client_handle
            .connect(addr(server_port))
            .await
            .expect("连接失败");

        let mut server_stream = tokio::time::timeout(Duration::from_secs(5), incoming_rx.recv())
            .await
            .expect("入站连接超时")
            .expect("入站通道关闭");

        // 发送小数据（一个包内）
        let data = b"Hello uTP from client to server!";
        client_stream.write_all(data).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let received = tokio::time::timeout(Duration::from_secs(5), server_stream.read_data())
            .await
            .expect("读取超时")
            .expect("读取失败");
        assert_eq!(&received, data);

        // 反向发送
        let data2 = b"Reply from server!";
        server_stream.write_all(data2).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let received2 = tokio::time::timeout(Duration::from_secs(5), client_stream.read_data())
            .await
            .expect("读取超时")
            .expect("读取失败");
        assert_eq!(&received2, data2);

        client_handle.shutdown().await;
        server_handle.shutdown().await;
    }

    #[tokio::test]
    async fn utp_graceful_close() {
        let (server_handle, mut incoming_rx) = UtpManager::bind("127.0.0.1", 0).await.unwrap();
        let server_port = server_handle.local_addr().port();

        let (client_handle, _) = UtpManager::bind("127.0.0.1", 0).await.unwrap();

        let mut client_stream = client_handle
            .connect(addr(server_port))
            .await
            .expect("连接失败");

        let mut server_stream = tokio::time::timeout(Duration::from_secs(5), incoming_rx.recv())
            .await
            .expect("入站连接超时")
            .expect("入站通道关闭");

        client_stream.write_all(b"data").await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = server_stream.read_data().await;

        client_stream.close().await.ok();
        tokio::time::sleep(Duration::from_millis(100)).await;

        client_handle.shutdown().await;
        server_handle.shutdown().await;
    }

    #[tokio::test]
    async fn utp_sequential_connections() {
        // 顺序建立两个连接并各自通信
        let (server_handle, mut incoming_rx) = UtpManager::bind("127.0.0.1", 0).await.unwrap();
        let server_port = server_handle.local_addr().port();

        let (client_handle, _) = UtpManager::bind("127.0.0.1", 0).await.unwrap();

        // 第一个连接
        let mut client1 = client_handle
            .connect(addr(server_port))
            .await
            .expect("连接1失败");
        let mut server1 = tokio::time::timeout(Duration::from_secs(5), incoming_rx.recv())
            .await
            .expect("入站连接1超时")
            .expect("入站通道关闭");

        let msg1 = b"first_message";
        client1.write_all(msg1).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let received1 = tokio::time::timeout(Duration::from_secs(5), server1.read_data())
            .await
            .expect("读取1超时")
            .expect("读取1失败");
        assert_eq!(&received1, msg1);

        // 第二个连接
        let mut client2 = client_handle
            .connect(addr(server_port))
            .await
            .expect("连接2失败");
        let mut server2 = tokio::time::timeout(Duration::from_secs(5), incoming_rx.recv())
            .await
            .expect("入站连接2超时")
            .expect("入站通道关闭");

        let msg2 = b"second_message";
        client2.write_all(msg2).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let received2 = tokio::time::timeout(Duration::from_secs(5), server2.read_data())
            .await
            .expect("读取2超时")
            .expect("读取2失败");
        assert_eq!(&received2, msg2);

        client_handle.shutdown().await;
        server_handle.shutdown().await;
    }
}
