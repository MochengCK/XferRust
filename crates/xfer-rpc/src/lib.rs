//! JSON-RPC 2.0 协议层：传输 + 原生协议 + 前端线上协议适配。
//!
//! 原生协议（`task.*` / `engine.*` / `events.*`）是引擎的第一公民接口：
//! 命名参数、真实数值类型、订阅式事件推送（免轮询）。
//! 前端线上协议由 `compat` 模块适配（仅翻译，不含引擎逻辑）。
//! 传输层单端口同时承载两者：HTTP POST 单发 + WebSocket 单连接复用，
//! 按连接首条请求自动识别协议族、只推送对应格式的事件帧。

mod compat;
mod native;
mod router;
mod transport;

pub use router::{Proto, Router};
pub use transport::serve;

#[cfg(test)]
mod tests;
