//! Fuzz target: uTP 包解析。
//!
//! 确保任意输入不会导致 panic（parse_packet 返回 Some 或 None 均可）。

#![no_main]

use libfuzzer_sys::fuzz_target;
use xfer_transport::utp_packet::{parse_packet, decode_sack_bits};

fuzz_target!(|data: &[u8]| {
    if let Some((header, exts, payload_offset)) = parse_packet(data) {
        // 验证 payload_offset 不越界
        assert!(payload_offset <= data.len(), "payload_offset 越界");

        // 触发 SACK 解码
        for (_, payload) in &exts {
            let _ = decode_sack_bits(payload);
        }

        // encode_header roundtrip
        let mut buf = [0u8; 20];
        xfer_transport::utp_packet::encode_header(&mut buf, &header);
        // 重新解析应得到一致结果
        if let Some((h2, _, _)) = parse_packet(&buf) {
            assert_eq!(header.type_, h2.type_);
            assert_eq!(header.version, h2.version);
            assert_eq!(header.extension, h2.extension);
            assert_eq!(header.connection_id, h2.connection_id);
            assert_eq!(header.timestamp, h2.timestamp);
            assert_eq!(header.timestamp_diff, h2.timestamp_diff);
            assert_eq!(header.wnd_size, h2.wnd_size);
            assert_eq!(header.seq_nr, h2.seq_nr);
            assert_eq!(header.ack_nr, h2.ack_nr);
        }
    }
});
