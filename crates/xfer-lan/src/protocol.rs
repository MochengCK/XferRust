//! 线上报文（协议第 1、2 节）：字段名与协议文档逐字对应。
//!
//! v2 起用中立的协议标记 `proto:"lan-transfer"`（不再有应用身份字段），
//! 任何按文档实现的客户端都能握手；安全性由 ECDH + AES-GCM + SAS 提供。

use serde::{Deserialize, Serialize};

/// 协议标记（`proto != "lan-transfer"` 或 `v != 2` 一律丢弃）。
pub(crate) const PROTO: &str = "lan-transfer";
pub(crate) const VERSION: u32 = 2;

/// UDP 发现报文（`discover` / `hello` 同构）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DiscoverMsg {
    pub proto: String,
    pub v: u32,
    #[serde(rename = "type")]
    pub kind: String,
    pub id: String,
    pub name: String,
    pub platform: String,
    pub port: u16,
}

impl DiscoverMsg {
    pub(crate) fn new(kind: &str, id: &str, name: &str, platform: &str, port: u16) -> Self {
        Self {
            proto: PROTO.to_string(),
            v: VERSION,
            kind: kind.to_string(),
            id: id.to_string(),
            name: name.to_string(),
            platform: platform.to_string(),
            port,
        }
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.proto == PROTO && self.v == VERSION && self.port != 0
    }
}

/// TCP 握手首行（发送端 → 接收端，明文）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Offer {
    pub proto: String,
    pub v: u32,
    #[serde(rename = "type")]
    pub kind: String,
    pub id: String,
    pub name: String,
    /// 字节数；-1 表示未知（靠结束帧收尾）。
    pub size: i64,
    pub sender_id: String,
    pub sender_name: String,
    /// base64(未压缩点 `04 || X || Y`)。
    pub pk: String,
}

impl Offer {
    pub(crate) fn is_valid(&self) -> bool {
        self.proto == PROTO && self.v == VERSION && self.kind == "offer" && self.size >= -1
    }
}

/// 握手应答（明文行）：`accept` 或 `reject`。
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct HandshakeReply {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub pk: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

/// 应答：接受（带自己的临时公钥）。
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Accept {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub pk: String,
}

impl Accept {
    pub(crate) fn new(pk: String) -> Self {
        Self { kind: "accept", pk }
    }
}

/// 应答：拒绝（带原因）。
#[derive(Debug, Clone, Serialize)]
pub(crate) struct Reject {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub reason: String,
}

impl Reject {
    pub(crate) fn new(reason: impl Into<String>) -> Self {
        Self {
            kind: "reject",
            reason: reason.into(),
        }
    }
}

/// 接收端完成回执（加密帧里的 JSON）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Done {
    #[serde(rename = "type")]
    pub kind: String,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Done {
    pub(crate) fn success() -> Self {
        Self {
            kind: "done".into(),
            ok: true,
            error: None,
        }
    }

    pub(crate) fn failure(error: impl Into<String>) -> Self {
        Self {
            kind: "done".into(),
            ok: false,
            error: Some(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discover_fields_match_protocol() {
        let msg = DiscoverMsg::new("discover", "abc", "Mac", "macos", 51234);
        let json: serde_json::Value = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["proto"], "lan-transfer");
        assert_eq!(json["v"], 2);
        assert_eq!(json["type"], "discover");
        assert_eq!(json["port"], 51234);
        assert!(json["app"].is_null());
        assert!(msg.is_valid());

        // 未知字段必须被忽略（兼容性第 4 节）
        let parsed: DiscoverMsg = serde_json::from_str(
            r#"{"proto":"lan-transfer","v":2,"type":"hello","id":"x","name":"n","platform":"linux","port":1,"extra":true}"#,
        )
        .unwrap();
        assert_eq!(parsed.kind, "hello");

        // 旧的 app 字段 / 旧版本号都不再被接受
        assert!(serde_json::from_str::<DiscoverMsg>(
            r#"{"app":"lerxu","v":1,"type":"hello","id":"x","name":"n","platform":"linux","port":1}"#
        )
        .is_err());
        let mut old = msg.clone();
        old.v = 1;
        assert!(!old.is_valid());
    }

    #[test]
    fn offer_size_minus_one_is_valid() {
        let offer: Offer = serde_json::from_str(
            r#"{"proto":"lan-transfer","v":2,"type":"offer","id":"t1","name":"a.bin","size":-1,
                "senderId":"d1","senderName":"Phone","pk":"AAAA"}"#,
        )
        .unwrap();
        assert_eq!(offer.size, -1);
        assert!(offer.is_valid());
        assert_eq!(offer.sender_id, "d1");
        assert_eq!(offer.sender_name, "Phone");
    }
}
