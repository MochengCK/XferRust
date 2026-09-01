//! Fuzz target: DHT KRPC 消息解析。
//!
//! 确保任意输入不会导致 panic（parse_response / parse_query 返回 Ok 或 Err 均可）。

#![no_main]

use libfuzzer_sys::fuzz_target;
use xfer_dht::krpc::{parse_query, parse_response, parse_get_peers_response};

fuzz_target!(|data: &[u8]| {
    let tid = [0x42u8, 0x43];
    // parse_query：不应 panic
    let _ = parse_query(data);
    // parse_response：不应 panic
    let _ = parse_response(data, &tid);
    // parse_get_peers_response：不应 panic
    let _ = parse_get_peers_response(data, &tid);
});
