//! 本机设备身份：id / 名称持久化到 `<data_dir>/lan-device.json`。
//!
//! id 必须跨重启稳定（对端靠它去重与定位），所以第一次生成后就落盘。

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::LanError;

const DEVICE_FILE: &str = "lan-device.json";
const MAX_NAME_CHARS: usize = 64;

#[derive(Debug, Clone)]
pub(crate) struct Device {
    pub id: String,
    pub name: String,
    pub platform: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct DeviceFile {
    #[serde(default)]
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    platform: String,
}

impl Device {
    /// 读取已有身份；缺失字段补齐后写回（始终写回，保证 platform 与当前平台一致）。
    ///
    /// [name_override] 非空时覆盖并落盘设备名（设置页改的就是这个值）；
    /// `None` 表示沿用身份文件里的名字。
    pub(crate) fn load_or_create(data_dir: &Path, name_override: Option<&str>) -> Result<Self, LanError> {
        let device = Self::peek(data_dir, name_override);
        if let Err(e) = write_device_file(data_dir, &device) {
            // 身份写不进去不该阻断传输（本次运行照常可用），只是重启后会换名字
            tracing::warn!(error = %e, "写入设备身份失败");
        }
        Ok(device)
    }

    /// 只读解析身份（设置页轮询状态时用，不产生写盘）。
    pub(crate) fn peek(data_dir: &Path, name_override: Option<&str>) -> Self {
        let path = data_dir.join(DEVICE_FILE);
        let mut file = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<DeviceFile>(&bytes).unwrap_or_default(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => DeviceFile::default(),
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "读取设备身份失败，将重新生成");
                DeviceFile::default()
            }
        };
        if file.id.trim().is_empty() {
            file.id = crate::random_hex(16);
        }
        if let Some(name) = name_override.map(str::trim).filter(|n| !n.is_empty()) {
            file.name = clamp_name(name);
        } else if file.name.trim().is_empty() {
            file.name = default_name();
        }
        Self {
            id: file.id,
            name: clamp_name(file.name.trim()),
            platform: platform().to_string(),
        }
    }
}

fn write_device_file(data_dir: &Path, device: &Device) -> std::io::Result<()> {
    std::fs::create_dir_all(data_dir)?;
    let path: PathBuf = data_dir.join(DEVICE_FILE);
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(&DeviceFile {
        id: device.id.clone(),
        name: device.name.clone(),
        platform: device.platform.clone(),
    })
    .map_err(|e| std::io::Error::other(format!("序列化设备身份失败：{e}")))?;
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, &path)
}

/// 平台标识（只能取协议里的四个值）。
pub(crate) fn platform() -> &'static str {
    if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "windows") {
        "windows"
    } else if cfg!(target_os = "android") {
        "android"
    } else {
        "linux"
    }
}

/// 默认设备名：环境里的主机名，取不到就退 "Lerxu"。
fn default_name() -> String {
    for key in ["COMPUTERNAME", "HOSTNAME"] {
        if let Ok(value) = std::env::var(key) {
            let value = value.trim();
            if !value.is_empty() {
                return clamp_name(value);
            }
        }
    }
    "Lerxu".to_string()
}

fn clamp_name(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .filter(|c| !c.is_control())
        .take(MAX_NAME_CHARS)
        .collect();
    let cleaned = cleaned.trim();
    if cleaned.is_empty() {
        "Lerxu".to_string()
    } else {
        cleaned.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_is_stable_across_loads() {
        let dir = std::env::temp_dir().join(format!(
            "xfer-lan-device-{}-{}",
            std::process::id(),
            crate::random_hex(4)
        ));
        let first = Device::load_or_create(&dir, None).unwrap();
        let second = Device::load_or_create(&dir, None).unwrap();
        assert_eq!(first.id, second.id);
        assert!(!first.id.is_empty());
        assert!(!first.name.is_empty());
        assert!(matches!(
            first.platform.as_str(),
            "macos" | "windows" | "linux" | "android"
        ));
        std::fs::remove_dir_all(&dir).ok();
    }
}
