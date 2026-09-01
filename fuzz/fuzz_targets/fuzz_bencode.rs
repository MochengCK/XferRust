//! Fuzz target: bencode 解析。
//!
//! 确保任意输入不会导致 panic（返回 Ok 或 Err 均可，但不能 abort）。
//! 同时验证 roundtrip：解码成功后重新编码，再解码应得到相同值。

#![no_main]

use libfuzzer_sys::fuzz_target;
use xfer_bencode::{decode, encode, Value};

fuzz_target!(|data: &[u8]| {
    // 基本 decode：不应 panic
    if let Ok(v) = decode(data) {
        // roundtrip：encode → decode 应得到相同值
        let enc = encode(&v);
        if let Ok(v2) = decode(&enc) {
            assert_eq!(v, v2, "roundtrip 失败");
        }
        // 深层访问不应 panic
        touch_value(&v);
    }

    // decode_prefix：不应 panic
    let _ = xfer_bencode::decode_prefix(data);
});

/// 递归遍历值树，触发所有 accessor 方法。
fn touch_value(v: &Value) {
    match v {
        Value::Int(_) => { let _ = v.as_int(); }
        Value::Bytes(_) => { let _ = v.as_bytes(); let _ = v.as_str(); }
        Value::List(items) => {
            let _ = v.as_list();
            for it in items {
                touch_value(it);
            }
        }
        Value::Dict(map) => {
            let _ = v.as_dict();
            let _ = v.dict_get("nonexistent");
            for (_, val) in map {
                touch_value(val);
            }
        }
    }
}
