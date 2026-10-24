//! SSH 连接、认证与 SFTP 会话管理。

use std::sync::Arc;
use std::time::Duration;

use russh::client::{self, Handle};
use russh::keys::{load_secret_key, HashAlg, PrivateKeyWithHashAlg, PublicKeyOrCertificate};
use russh_sftp::client::SftpSession;
use tokio::time::timeout;

use crate::known_hosts::HostKeyPolicy;
use crate::url::{expand_home, resolve_login, Login, SftpUrl};
use crate::{SftpError, SftpOptions, SftpProbe};

/// 纯校验主机密钥的客户端回调：不做端口转发/命令执行。
pub(crate) struct HostKeyHandler {
    policy: HostKeyPolicy,
}

impl client::Handler for HostKeyHandler {
    type Error = SftpError;

    async fn check_server_key(
        &mut self,
        server_public_key: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        self.policy.check(&server_public_key.public_key())?;
        Ok(true)
    }
}

/// 已认证的 SFTP 连接。多通道在 [`Self::open_session`] 里各自开子系统。
pub struct SftpClient {
    handle: Handle<HostKeyHandler>,
    /// 诊断用（不参与协议）。
    host: String,
    port: u16,
}

impl SftpClient {
    pub async fn connect(url: &SftpUrl, opts: &SftpOptions) -> Result<Self, SftpError> {
        let login = resolve_login(url, opts.user.as_deref(), opts.password.as_deref())?;
        let policy = HostKeyPolicy::new(
            &url.host,
            url.port,
            opts.known_hosts.clone(),
            opts.strict_host_key,
        );
        let connect_timeout = Duration::from_secs(opts.timeout_secs.clamp(5, 120));
        let config = Arc::new(client::Config {
            // 读大文件时 SSH 连接持续有流量；长时间完全无包才判死。
            inactivity_timeout: Some(Duration::from_secs(
                opts.timeout_secs.clamp(10, 300).saturating_mul(4),
            )),
            keepalive_interval: Some(Duration::from_secs(20)),
            keepalive_max: 3,
            nodelay: true,
            ..Default::default()
        });
        let mut handle = timeout(
            connect_timeout,
            client::connect(
                config,
                (url.host.as_str(), url.port),
                HostKeyHandler { policy },
            ),
        )
        .await
        .map_err(|_| SftpError::Timeout)??;

        authenticate(&mut handle, &login, opts, connect_timeout).await?;
        tracing::info!(
            host = %url.host,
            port = url.port,
            user = %login.user,
            "SFTP 连接已认证"
        );
        Ok(Self {
            handle,
            host: url.host.clone(),
            port: url.port,
        })
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// 打开一条新的 SFTP 子系统通道。
    ///
    /// 每条通道有自己的并发读请求队列；下载器按 `split` 开多条并行。
    pub async fn open_session(&self, opts: &SftpOptions) -> Result<SftpSession, SftpError> {
        let dur = Duration::from_secs(opts.timeout_secs.clamp(5, 120));
        let channel = timeout(dur, self.handle.channel_open_session())
            .await
            .map_err(|_| SftpError::Timeout)?
            .map_err(|e| SftpError::Connect(e.to_string()))?;
        timeout(dur, channel.request_subsystem(true, "sftp"))
            .await
            .map_err(|_| SftpError::Timeout)?
            .map_err(|e| SftpError::Connect(e.to_string()))?;
        let cfg = russh_sftp::client::Config {
            request_timeout_secs: opts.timeout_secs.clamp(5, 300),
            max_concurrent_reads: 8,
            max_packet_len: 256 * 1024,
            ..Default::default()
        };
        timeout(
            dur,
            SftpSession::new_with_config(channel.into_stream(), cfg),
        )
        .await
        .map_err(|_| SftpError::Timeout)?
        .map_err(Into::into)
    }

    /// 探测远端文件：展开 `~`、校验是普通文件、取总长与文件名。
    pub async fn probe(
        &self,
        url: &SftpUrl,
        opts: &SftpOptions,
        remote_path: &str,
    ) -> Result<SftpProbe, SftpError> {
        let sftp = self.open_session(opts).await?;
        let canonical = if url.needs_home_expansion() {
            let home = sftp.canonicalize(".").await.map_err(SftpError::from)?;
            expand_home(remote_path, &home)?
        } else {
            remote_path.to_string()
        };
        let meta = sftp
            .metadata(canonical.clone())
            .await
            .map_err(SftpError::from)?;
        if meta.is_dir() {
            let _ = sftp.close().await;
            return Err(SftpError::NotFound(
                "目标是目录；SFTP 任务当前只支持单文件下载".into(),
            ));
        }
        let size = meta
            .size
            .ok_or_else(|| SftpError::Protocol("服务器未返回远程文件大小".into()))?;
        let _ = sftp.close().await;
        Ok(SftpProbe {
            remote_path: canonical,
            size,
            filename: url.filename()?,
        })
    }

    pub async fn disconnect(&self) {
        let _ = self
            .handle
            .disconnect(russh::Disconnect::ByApplication, "", "")
            .await;
    }
}

async fn authenticate(
    handle: &mut Handle<HostKeyHandler>,
    login: &Login,
    opts: &SftpOptions,
    dur: Duration,
) -> Result<(), SftpError> {
    let mut key_error: Option<SftpError> = None;

    if let Some(path) = &opts.private_key {
        match try_publickey(
            handle,
            &login.user,
            path,
            opts.private_key_passphrase.as_deref(),
            dur,
        )
        .await
        {
            Ok(true) => return Ok(()),
            Ok(false) => {
                key_error = Some(SftpError::Auth("私钥认证被服务器拒绝".into()));
            }
            Err(e @ SftpError::Auth(_)) => key_error = Some(e),
            Err(e) => return Err(e),
        }
    }

    if let Some(password) = &login.password {
        let res = timeout(
            dur,
            handle.authenticate_password(&login.user, password.clone()),
        )
        .await
        .map_err(|_| SftpError::Timeout)??;
        if res.success() {
            return Ok(());
        }
        return Err(SftpError::Auth(match key_error {
            Some(_) => "私钥与密码认证均失败".into(),
            None => "密码认证失败".into(),
        }));
    }

    if key_error.is_none() {
        // 少数内网服务器允许 none 认证；失败再报"需要凭据"。
        let res = timeout(dur, handle.authenticate_none(&login.user))
            .await
            .map_err(|_| SftpError::Timeout)??;
        if res.success() {
            return Ok(());
        }
    }

    Err(key_error.unwrap_or_else(|| {
        SftpError::Auth(
            "服务器要求 SSH 认证：请在 URL 中写 user:password，或提供 sftp-private-key".into(),
        )
    }))
}

async fn try_publickey(
    handle: &mut Handle<HostKeyHandler>,
    user: &str,
    path: &std::path::Path,
    passphrase: Option<&str>,
    dur: Duration,
) -> Result<bool, SftpError> {
    let key = load_secret_key(path, passphrase).map_err(|e| match e {
        russh::keys::Error::KeyIsEncrypted => SftpError::Auth(format!(
            "私钥 {} 已加密，请通过 sftp-private-key-passphrase 提供口令",
            path.display()
        )),
        other => SftpError::Auth(format!("读取私钥 {} 失败: {other}", path.display())),
    })?;
    let key = Arc::new(key);
    // RSA 需要协商 rsa-sha2-*；服务器没通告 server-sig-algs 时退回传统 sha1。
    let hash_alg: Option<HashAlg> = if key.algorithm().is_rsa() {
        timeout(dur, handle.best_supported_rsa_hash())
            .await
            .ok()
            .and_then(Result::ok)
            .flatten()
            .flatten()
    } else {
        None
    };
    let res = timeout(
        dur,
        handle.authenticate_publickey(user, PrivateKeyWithHashAlg::new(key, hash_alg)),
    )
    .await
    .map_err(|_| SftpError::Timeout)??;
    Ok(res.success())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SftpUrl;

    #[test]
    fn login_resolution_prefers_options() {
        let u = SftpUrl::parse("sftp://url_user:url_pw@host/a").unwrap();
        let l = resolve_login(&u, Some("opt_user"), Some("opt_pw")).unwrap();
        assert_eq!(l.user, "opt_user");
        assert_eq!(l.password.as_deref(), Some("opt_pw"));
    }

    #[test]
    fn login_requires_user() {
        let u = SftpUrl::parse("sftp://host/a").unwrap();
        assert!(resolve_login(&u, None, None).is_err());
    }
}
