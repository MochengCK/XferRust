//! 主机密钥校验（TOFU + known_hosts）。
//!
//! 信任来源分两级：
//! 1. 用户标准 `~/.ssh/known_hosts`（只读）——用户 ssh 过的主机直接信任；
//! 2. 引擎自己的 known_hosts（默认在会话文件同目录，见引擎侧拼接）——
//!    首次连接时写入，之后同一主机指纹变化即失败。
//!
//! `strict` 模式下未知主机不自动信任，直接报错并给出 SHA256 指纹。

use std::path::PathBuf;

use russh::keys::known_hosts::{known_host_keys_path, learn_known_hosts_path};
use russh::keys::{HashAlg, PublicKey};
use xfer_types::home_dir;

use crate::SftpError;

#[derive(Debug, Clone)]
pub(crate) struct HostKeyPolicy {
    host: String,
    port: u16,
    store: PathBuf,
    strict: bool,
}

impl HostKeyPolicy {
    pub fn new(host: &str, port: u16, store: PathBuf, strict: bool) -> Self {
        Self {
            host: host.to_string(),
            port,
            store,
            strict,
        }
    }

    /// 校验服务端主机密钥。未知主机按 TOFU 记录；指纹变化一律失败。
    pub fn check(&self, key: &PublicKey) -> Result<(), SftpError> {
        let fingerprint = key.fingerprint(HashAlg::Sha256);

        match check_file(&self.store, &self.host, self.port, key)? {
            Trust::Trusted => return Ok(()),
            Trust::Changed(line) => {
                return Err(SftpError::HostKey(format!(
                    "{}:{} 的主机密钥与 known_hosts 记录不一致（{} 第 {line} 行）；\
                     当前密钥 {fingerprint}。可能遭遇中间人攻击，已拒绝连接",
                    self.host,
                    self.port,
                    self.store.display()
                )))
            }
            Trust::Unknown => {}
        }

        // 用户自己的 OpenSSH known_hosts 作为只读信任来源。
        if let Some(user_known) = user_known_hosts() {
            if user_known != self.store {
                match check_file(&user_known, &self.host, self.port, key)? {
                    Trust::Trusted => return Ok(()),
                    Trust::Changed(line) => {
                        return Err(SftpError::HostKey(format!(
                            "{}:{} 的主机密钥与 {} 第 {line} 行不一致；当前密钥 {fingerprint}。\
                             可能遭遇中间人攻击，已拒绝连接",
                            self.host,
                            self.port,
                            user_known.display()
                        )))
                    }
                    Trust::Unknown => {}
                }
            }
        }

        if self.strict {
            return Err(SftpError::HostKey(format!(
                "{}:{} 是未知主机（{fingerprint}）。sftp-strict-host-key 已开启：\
                 请先把该指纹写入 {} 再重试",
                self.host,
                self.port,
                self.store.display()
            )));
        }

        learn_known_hosts_path(&self.host, self.port, key, &self.store).map_err(|e| {
            SftpError::HostKey(format!(
                "首次连接 {}:{}，记录主机密钥到 {} 失败: {e}",
                self.host,
                self.port,
                self.store.display()
            ))
        })?;
        tracing::info!(
            host = %self.host,
            port = self.port,
            fingerprint = %fingerprint,
            "首次连接 SFTP 主机，已记录主机密钥"
        );
        Ok(())
    }
}

enum Trust {
    Trusted,
    Changed(usize),
    Unknown,
}

/// 与 `check_known_hosts_path` 不同：同一主机下**任何一条记录与当前密钥
/// 不同都算变更**（包括算法类型变化）。官方实现只在算法相同才报 KeyChanged，
/// 服务器换了密钥类型时会被当成"未知主机"重新 TOFU —— 那等于绕过了中间人
/// 防护，这里收紧。
fn check_file(
    path: &std::path::Path,
    host: &str,
    port: u16,
    key: &PublicKey,
) -> Result<Trust, SftpError> {
    let recorded = known_host_keys_path(host, port, path).map_err(|e| {
        SftpError::HostKey(format!("读取 known_hosts 失败（{}）: {e}", path.display()))
    })?;
    if recorded.is_empty() {
        return Ok(Trust::Unknown);
    }
    if recorded.iter().any(|(_, k)| k == key) {
        return Ok(Trust::Trusted);
    }
    Ok(Trust::Changed(recorded[0].0))
}

fn user_known_hosts() -> Option<PathBuf> {
    home_dir().map(|h| h.join(".ssh").join("known_hosts"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use russh::keys::{Algorithm, PrivateKey};

    fn tmp_store(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "xfer-sftp-kh-{tag}-{}-{}.known_hosts",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn key() -> PrivateKey {
        PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519).unwrap()
    }

    #[test]
    fn tofu_learns_then_rejects_changed_key() {
        let store = tmp_store("tofu");
        let policy = HostKeyPolicy::new("example.test", 22, store.clone(), false);
        let k1 = key();
        policy.check(k1.public_key()).unwrap();
        // 同一密钥再来一次：直接命中记录
        policy.check(k1.public_key()).unwrap();
        // 换了密钥：必须失败
        let k2 = key();
        let err = policy.check(k2.public_key()).unwrap_err();
        assert!(matches!(err, SftpError::HostKey(_)), "{err:?}");
        std::fs::remove_file(&store).ok();
    }

    #[test]
    fn strict_mode_rejects_unknown_host() {
        let store = tmp_store("strict");
        let policy = HostKeyPolicy::new("strict.test", 2222, store.clone(), true);
        let err = policy.check(key().public_key()).unwrap_err();
        assert!(
            matches!(err, SftpError::HostKey(ref m) if m.contains("未知主机")),
            "{err:?}"
        );
        assert!(!store.exists(), "严格模式不应写入信任记录");
    }

    #[test]
    fn port_is_part_of_identity() {
        let store = tmp_store("port");
        let k = key();
        HostKeyPolicy::new("p.test", 22, store.clone(), false)
            .check(k.public_key())
            .unwrap();
        // 不同端口是不同身份：同样密钥仍需重新记录（不报错，TOFU 写入）
        HostKeyPolicy::new("p.test", 2200, store.clone(), false)
            .check(k.public_key())
            .unwrap();
        std::fs::remove_file(&store).ok();
    }
}
