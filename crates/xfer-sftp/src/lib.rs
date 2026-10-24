//! SFTP（SSH File Transfer Protocol）下载。
//!
//! 与 HTTP 驱动同构：先探测远端文件大小，再按 1 MiB 片多通道并行读取、
//! 直接落盘到预分配文件的对应偏移；控制文件记录已完成片位图，暂停 / 断线
//! 后从缺片继续。SSH 连接复用一条 TCP，多通道各自开一个 `sftp` 子系统。
//!
//! 认证支持密码（URL userinfo 或 `sftp-password`）与 OpenSSH 私钥
//! （`sftp-private-key` + 可选口令）。主机密钥按 TOFU 校验：优先信任
//! `~/.ssh/known_hosts`，未知主机记录到引擎自己的 known_hosts，指纹变化
//! 直接失败；`sftp-strict-host-key=true` 可禁止首次自动信任。

mod client;
mod ctrl;
mod download;
mod known_hosts;
mod url;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

pub use client::SftpClient;
pub use ctrl::{can_resume, ctrl_path, resume_bytes};
pub use download::{download, SftpDone};
pub use url::{is_sftp_url, SftpUrl};

/// SFTP 下载错误。可重试性见 [`SftpError::is_retryable`]。
#[derive(Debug, Clone, thiserror::Error)]
pub enum SftpError {
    #[error("SFTP 地址无效: {0}")]
    Url(String),
    #[error("连接服务器失败: {0}")]
    Connect(String),
    #[error("连接超时")]
    Timeout,
    #[error("认证失败: {0}")]
    Auth(String),
    #[error("主机密钥校验失败: {0}")]
    HostKey(String),
    #[error("资源不存在或不可访问: {0}")]
    NotFound(String),
    #[error("权限不足: {0}")]
    Permission(String),
    #[error("服务器响应异常: {0}")]
    Protocol(String),
    /// EOF 早于预期总长（服务器中途断流 / 远端文件被截短）。
    #[error("响应体短于请求区间")]
    ShortRead,
    #[error("本地写入失败: {0}")]
    Io(String),
    #[error("已取消")]
    Cancelled,
}

impl SftpError {
    /// 是否为可重试的瞬时失败（断线 / 超时 / 协议抖动 / 短读）。
    ///
    /// 认证、主机密钥、资源不存在、权限、本地 IO 都不重试：重试只会重复
    /// 同一个确定性结果。
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Connect(_) | Self::Timeout | Self::Protocol(_) | Self::ShortRead
        )
    }

    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }

    /// 任务错误码（0 无错；2 超时；3 资源不存在；5 网络问题；1 其他）。
    pub fn error_code(&self) -> i64 {
        match self {
            Self::Cancelled => 0,
            Self::Timeout => 2,
            Self::NotFound(_) => 3,
            Self::Connect(_) | Self::Protocol(_) | Self::ShortRead => 5,
            _ => 1,
        }
    }
}

impl From<russh::Error> for SftpError {
    fn from(e: russh::Error) -> Self {
        use russh::Error as E;
        match e {
            E::ConnectionTimeout | E::KeepaliveTimeout | E::InactivityTimeout | E::Elapsed(_) => {
                Self::Timeout
            }
            E::HUP | E::Disconnect | E::SendError | E::RecvError | E::IO(_) => {
                Self::Connect(e.to_string())
            }
            E::Keys(err) => Self::Auth(err.to_string()),
            other => Self::Protocol(other.to_string()),
        }
    }
}

impl From<russh_sftp::client::error::Error> for SftpError {
    fn from(e: russh_sftp::client::error::Error) -> Self {
        use russh_sftp::client::error::Error as E;
        use russh_sftp::protocol::StatusCode;
        match e {
            E::Timeout => Self::Timeout,
            E::Status(status) => match status.status_code {
                StatusCode::NoSuchFile => Self::NotFound(status.error_message),
                StatusCode::PermissionDenied => Self::Permission(status.error_message),
                StatusCode::ConnectionLost | StatusCode::NoConnection => {
                    Self::Protocol(format!("连接已断开: {}", status.error_message))
                }
                _ => Self::Protocol(format!("{}: {}", status.status_code, status.error_message)),
            },
            E::IO(msg) | E::Limited(msg) => Self::Connect(msg),
            E::UnexpectedPacket | E::UnexpectedBehavior(_) => Self::Protocol(e.to_string()),
        }
    }
}

/// SFTP 下载选项（任务选项优先于全局选项，由引擎合成后传入）。
#[derive(Clone)]
pub struct SftpOptions {
    /// 覆盖 URL 中的用户名。
    pub user: Option<String>,
    /// 覆盖 URL 中的密码；Some("") 表示显式空密码。
    pub password: Option<String>,
    /// OpenSSH 私钥文件路径。
    pub private_key: Option<PathBuf>,
    /// 私钥口令（私钥加密时必需）。
    pub private_key_passphrase: Option<String>,
    /// 引擎自己的 known_hosts 存储（TOFU 记录写这里）。
    pub known_hosts: PathBuf,
    /// 只信任已记录的主机密钥（未知主机直接失败）。
    pub strict_host_key: bool,
    /// 并行 SFTP 通道数（1~8）。
    pub connections: usize,
    /// 单次请求 / 连接超时（秒）。
    pub timeout_secs: u64,
}

impl std::fmt::Debug for SftpOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 密码与私钥口令绝不进日志
        f.debug_struct("SftpOptions")
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("private_key", &self.private_key)
            .field(
                "private_key_passphrase",
                &self.private_key_passphrase.as_ref().map(|_| "***"),
            )
            .field("known_hosts", &self.known_hosts)
            .field("strict_host_key", &self.strict_host_key)
            .field("connections", &self.connections)
            .field("timeout_secs", &self.timeout_secs)
            .finish()
    }
}

/// 远端探测结果。
#[derive(Debug, Clone)]
pub struct SftpProbe {
    /// 展开 `~` 后的绝对远程路径（重连/续传时与这里比对）。
    pub remote_path: String,
    /// 远端文件字节数。
    pub size: u64,
    /// 默认本地文件名（URL 末段）。
    pub filename: String,
}

/// 下载统计（无锁，供任务进度采样）。
#[derive(Debug)]
pub struct SftpStats {
    /// 已完成字节数（含续传继承的片）。
    pub completed: AtomicU64,
    /// 本次运行从网络收到的字节数（速度采样用）。
    pub received: AtomicU64,
    /// 当前活跃通道数。
    pub connections: AtomicU64,
}

impl SftpStats {
    pub fn new(resumed: u64) -> Self {
        Self {
            completed: AtomicU64::new(resumed),
            received: AtomicU64::new(0),
            connections: AtomicU64::new(0),
        }
    }

    pub fn completed(&self) -> u64 {
        self.completed.load(Ordering::Relaxed)
    }

    pub fn received(&self) -> u64 {
        self.received.load(Ordering::Relaxed)
    }

    pub fn connections(&self) -> u64 {
        self.connections.load(Ordering::Relaxed)
    }
}
