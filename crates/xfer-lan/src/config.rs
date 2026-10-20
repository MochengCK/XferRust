//! 互传用户配置：`<data_dir>/lan-config.json`。
//!
//! 只放用户可改的三项：总开关、设备名、接收目录。发现端口与协议常量
//! 由协议固定，不在这里。

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{LanError, RECV_DIR_NAME};

const CONFIG_FILE: &str = "lan-config.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct LanUserConfig {
    pub enabled: bool,
    /// 空串 = 沿用设备身份里已有的名字（首次启动由系统主机名生成）。
    pub device_name: String,
    /// 空串 = 默认接收目录 `<下载目录>/LerxuTransfer/`。
    pub recv_dir: String,
}

impl Default for LanUserConfig {
    fn default() -> Self {
        Self {
            // 互传涉及本机被发现与接收文件，默认关闭；首次去设置里开启
            enabled: false,
            device_name: String::new(),
            recv_dir: String::new(),
        }
    }
}

impl LanUserConfig {
    pub fn load(data_dir: &Path) -> Self {
        let path = data_dir.join(CONFIG_FILE);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Self::default(),
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "读取互传配置失败，使用默认值");
                return Self::default();
            }
        };
        match serde_json::from_slice::<Self>(&bytes) {
            Ok(cfg) => cfg,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "互传配置损坏，使用默认值");
                Self::default()
            }
        }
    }

    pub fn save(&self, data_dir: &Path) -> Result<(), LanError> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join(CONFIG_FILE);
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|e| LanError::Msg(format!("序列化互传配置失败：{e}")))?;
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    pub fn device_name_trimmed(&self) -> Option<&str> {
        let name = self.device_name.trim();
        if name.is_empty() { None } else { Some(name) }
    }
}

/// 用户配置的接收目录；空串落到默认目录（下载目录下的 `LerxuTransfer/`）。
pub fn effective_recv_dir(download_dir: &Path, configured: &str) -> PathBuf {
    let raw = configured.trim();
    if raw.is_empty() {
        download_dir.join(RECV_DIR_NAME)
    } else {
        PathBuf::from(raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "xfer-lan-config-{tag}-{}-{}",
            std::process::id(),
            crate::random_hex(4)
        ))
    }

    #[test]
    fn defaults_are_disabled_with_empty_overrides() {
        let cfg = LanUserConfig::default();
        assert!(!cfg.enabled);
        assert!(cfg.device_name.is_empty());
        assert!(cfg.recv_dir.is_empty());
        assert_eq!(cfg.device_name_trimmed(), None);
    }

    #[test]
    fn missing_or_broken_file_falls_back_to_defaults() {
        let dir = temp_dir("missing");
        assert!(!LanUserConfig::load(&dir).enabled);

        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(CONFIG_FILE), b"{not-json").unwrap();
        let cfg = LanUserConfig::load(&dir);
        assert!(!cfg.enabled);
        assert!(cfg.device_name.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn roundtrip_persists_user_values() {
        let dir = temp_dir("roundtrip");
        let cfg = LanUserConfig {
            enabled: false,
            device_name: " 书房 Mac ".to_string(),
            recv_dir: "/tmp/received".to_string(),
        };
        cfg.save(&dir).unwrap();
        let loaded = LanUserConfig::load(&dir);
        assert!(!loaded.enabled);
        assert_eq!(loaded.device_name_trimmed(), Some("书房 Mac"));
        assert_eq!(loaded.recv_dir, "/tmp/received");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn configured_recv_dir_resolves_or_falls_back() {
        let download = Path::new("/downloads");
        assert_eq!(
            effective_recv_dir(download, "  "),
            PathBuf::from("/downloads/LerxuTransfer")
        );
        assert_eq!(
            effective_recv_dir(download, "/data/received"),
            PathBuf::from("/data/received")
        );
    }
}
