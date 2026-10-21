//! 互传运行期控制器：总开关 / 设备名 / 接收目录。
//!
//! 引擎启动时先读 `<data_dir>/lan-config.json`；关闭状态下不拉起发现与接收，
//! 但仍保留控制器，设置页随时可以再打开。设备名与接收目录在运行期直接改，
//! 不需要重启引擎。

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;

use crate::config::{self, LanUserConfig};
use crate::device;
use crate::{LanConfig, LanError, LanService};

const MAX_NAME_CHARS: usize = 64;

/// 设置页提交的补丁：只带本次改动的字段。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct LanConfigPatch {
    pub enabled: Option<bool>,
    pub device_name: Option<String>,
    pub recv_dir: Option<String>,
}

/// 设置页读取的完整状态（含只读信息）。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LanStatus {
    pub enabled: bool,
    pub running: bool,
    pub device_name: String,
    pub device_id: String,
    pub platform: String,
    /// 生效中的接收目录（绝对路径）。
    pub recv_dir: String,
    /// 默认接收目录 `<下载目录>/LerxuTransfer/`。
    pub default_recv_dir: String,
    /// 启动失败时的可读原因；正常为空。
    pub error: String,
}

pub struct LanController {
    download_dir: PathBuf,
    data_dir: PathBuf,
    config: Mutex<LanUserConfig>,
    service: Mutex<Option<Arc<LanService>>>,
    last_error: Mutex<String>,
    /// 串行化启停/改配置，避免开关快速切换时并发 start/stop。
    op_lock: AsyncMutex<()>,
}

impl LanController {
    /// 读取配置；开启时立即拉起服务（失败不阻断引擎启动）。
    pub async fn start(
        download_dir: impl Into<PathBuf>,
        data_dir: impl Into<PathBuf>,
    ) -> Result<Arc<Self>, LanError> {
        Self::start_inner(download_dir, data_dir, true).await
    }

    /// 只读配置、**不**拉起服务：给"可能与应用的引擎同时活着"的独立进程用。
    ///
    /// 发现端口（UDP 41234）同时只能有一个监听者：独立进程先绑走，之后启动的
    /// 应用就会按协议降级成"只发不收"，界面上表现为两台设备互相看不见。所以
    /// 独立进程改成按需启动——第一次真正调用互传接口时再 [`Self::ensure_started`]，
    /// 平时（只下文件）不占用任何端口。
    pub async fn start_deferred(
        download_dir: impl Into<PathBuf>,
        data_dir: impl Into<PathBuf>,
    ) -> Result<Arc<Self>, LanError> {
        Self::start_inner(download_dir, data_dir, false).await
    }

    async fn start_inner(
        download_dir: impl Into<PathBuf>,
        data_dir: impl Into<PathBuf>,
        autostart: bool,
    ) -> Result<Arc<Self>, LanError> {
        let download_dir = download_dir.into();
        let data_dir = data_dir.into();
        let cfg = LanUserConfig::load(&data_dir);
        let controller = Arc::new(Self {
            download_dir,
            data_dir,
            config: Mutex::new(cfg),
            service: Mutex::new(None),
            last_error: Mutex::new(String::new()),
            op_lock: AsyncMutex::new(()),
        });
        if autostart && controller.enabled() {
            if let Err(e) = controller.start_service().await {
                let message = e.to_string();
                tracing::warn!(error = %message, "局域网互传启动失败");
                *controller.last_error.lock().expect("错误信息锁中毒") = message;
            }
        }
        Ok(controller)
    }

    /// 按需拉起服务（[`Self::start_deferred`] 之后第一次用到互传时调用）。
    pub async fn ensure_started(&self) -> Result<(), String> {
        if !self.enabled() {
            return Err("互传已关闭".to_string());
        }
        if self.service.lock().expect("互传服务锁中毒").is_some() {
            return Ok(());
        }
        let _guard = self.op_lock.lock().await;
        if self.service.lock().expect("互传服务锁中毒").is_some() {
            return Ok(());
        }
        match self.start_service().await {
            Ok(()) => Ok(()),
            Err(e) => {
                let message = e.to_string();
                tracing::warn!(error = %message, "局域网互传启动失败");
                *self.last_error.lock().expect("错误信息锁中毒") = message.clone();
                Err(format!("互传服务启动失败：{message}"))
            }
        }
    }

    pub fn enabled(&self) -> bool {
        self.config.lock().expect("互传配置锁中毒").enabled
    }

    /// 供发现/任务/发送类 RPC 使用：关闭或未启动时返回明确错误。
    pub fn service(&self) -> Result<Arc<LanService>, String> {
        if !self.enabled() {
            return Err("互传已关闭".to_string());
        }
        if let Some(service) = self.service.lock().expect("互传服务锁中毒").as_ref() {
            return Ok(service.clone());
        }
        let error = self.last_error.lock().expect("错误信息锁中毒").clone();
        Err(if error.is_empty() {
            "互传服务未启动".to_string()
        } else {
            format!("互传服务未启动：{error}")
        })
    }

    pub fn status(&self) -> LanStatus {
        let cfg = self.config.lock().expect("互传配置锁中毒").clone();
        let (running, live_name, live_dir) = {
            let guard = self.service.lock().expect("互传服务锁中毒");
            match guard.as_ref() {
                Some(service) => (
                    true,
                    Some(service.device_name()),
                    Some(service.recv_dir()),
                ),
                None => (false, None, None),
            }
        };
        // 只读：状态查询（主进程每 2.5s 拉一次）不能每次都写设备身份文件
        let device = device::Device::peek(&self.data_dir, cfg.device_name_trimmed());
        let recv_dir = live_dir.unwrap_or_else(|| {
            config::effective_recv_dir(&self.download_dir, &cfg.recv_dir)
        });
        LanStatus {
            enabled: cfg.enabled,
            running,
            device_name: live_name.unwrap_or(device.name),
            device_id: device.id,
            platform: device.platform,
            recv_dir: recv_dir.to_string_lossy().to_string(),
            default_recv_dir: self
                .download_dir
                .join(crate::RECV_DIR_NAME)
                .to_string_lossy()
                .to_string(),
            error: self.last_error.lock().expect("错误信息锁中毒").clone(),
        }
    }

    async fn start_service(&self) -> Result<(), LanError> {
        let cfg = self.config.lock().expect("互传配置锁中毒").clone();
        let recv_dir = config::effective_recv_dir(&self.download_dir, &cfg.recv_dir);
        let service = LanService::start(LanConfig {
            download_dir: self.download_dir.clone(),
            data_dir: self.data_dir.clone(),
            device_name: cfg.device_name_trimmed().map(str::to_string),
            recv_dir: Some(recv_dir),
        })
        .await?;
        *self.service.lock().expect("互传服务锁中毒") = Some(service);
        *self.last_error.lock().expect("错误信息锁中毒") = String::new();
        Ok(())
    }

    /// 应用设置页补丁：先校验并落盘，再按需启停服务或热改字段。
    pub async fn set_config(&self, patch: LanConfigPatch) -> Result<LanStatus, String> {
        let _guard = self.op_lock.lock().await;

        let mut next = self.config.lock().expect("互传配置锁中毒").clone();
        if let Some(enabled) = patch.enabled {
            next.enabled = enabled;
        }
        if let Some(name) = patch.device_name {
            let name = name.trim().to_string();
            if name.chars().count() > MAX_NAME_CHARS {
                return Err(format!("设备名最多 {MAX_NAME_CHARS} 个字符"));
            }
            next.device_name = name;
        }
        if let Some(dir) = patch.recv_dir {
            let raw = dir.trim();
            if raw.is_empty() {
                next.recv_dir = String::new();
            } else {
                let path = PathBuf::from(raw);
                if !path.is_absolute() {
                    return Err("接收目录必须是绝对路径".to_string());
                }
                std::fs::create_dir_all(&path)
                    .map_err(|e| format!("创建接收目录失败：{e}"))?;
                if !path.is_dir() {
                    return Err("接收目录不是文件夹".to_string());
                }
                next.recv_dir = path.to_string_lossy().to_string();
            }
        }

        next.save(&self.data_dir).map_err(|e| e.to_string())?;
        *self.config.lock().expect("互传配置锁中毒") = next.clone();

        if !next.enabled {
            let service = self.service.lock().expect("互传服务锁中毒").take();
            if let Some(service) = service {
                service.stop().await;
            }
            *self.last_error.lock().expect("错误信息锁中毒") = String::new();
            return Ok(self.status());
        }

        let running = self.service.lock().expect("互传服务锁中毒").clone();
        if let Some(service) = running {
            if !next.device_name.is_empty() {
                if let Err(e) = service.set_device_name(&next.device_name) {
                    let message = e.to_string();
                    *self.last_error.lock().expect("错误信息锁中毒") = message.clone();
                    return Err(format!("修改设备名失败：{message}"));
                }
            }
            let recv_dir = config::effective_recv_dir(&self.download_dir, &next.recv_dir);
            if let Err(e) = service.set_recv_dir(&recv_dir) {
                let message = e.to_string();
                *self.last_error.lock().expect("错误信息锁中毒") = message.clone();
                return Err(format!("修改接收目录失败：{message}"));
            }
            *self.last_error.lock().expect("错误信息锁中毒") = String::new();
        } else if let Err(e) = self.start_service().await {
            let message = e.to_string();
            *self.last_error.lock().expect("错误信息锁中毒") = message.clone();
            return Err(format!("启动互传失败：{message}"));
        }

        Ok(self.status())
    }

    pub async fn stop(&self) {
        let service = self.service.lock().expect("互传服务锁中毒").take();
        if let Some(service) = service {
            service.stop().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn enable_disable_and_hot_edit_settings() {
        let root = std::env::temp_dir().join(format!(
            "xfer-lan-controller-{}-{}",
            std::process::id(),
            crate::random_hex(4)
        ));
        let download = root.join("downloads");
        let data = root.join("data");
        let recv = root.join("received");
        std::fs::create_dir_all(&download).unwrap();

        let controller = LanController::start(&download, &data).await.unwrap();
        // 默认关闭；先确认关闭状态下可改设置，再开启
        let status = controller
            .set_config(LanConfigPatch {
                enabled: Some(false),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(!status.enabled);
        assert!(!status.running);

        // 关闭状态下也能改设备名与接收目录
        let status = controller
            .set_config(LanConfigPatch {
                device_name: Some("书房 Mac".to_string()),
                recv_dir: Some(recv.to_string_lossy().to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(status.device_name, "书房 Mac");
        assert_eq!(PathBuf::from(&status.recv_dir), recv);
        assert!(!status.running);

        // 重新开启后应立即运行，并使用刚保存的设置
        let status = controller
            .set_config(LanConfigPatch {
                enabled: Some(true),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(status.enabled);
        assert!(status.running);
        assert_eq!(status.device_name, "书房 Mac");
        assert_eq!(PathBuf::from(&status.recv_dir), recv);

        // 运行期改名不需要重启
        let status = controller
            .set_config(LanConfigPatch {
                device_name: Some("客厅 iMac".to_string()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(status.running);
        assert_eq!(status.device_name, "客厅 iMac");

        controller.stop().await;
        std::fs::remove_dir_all(&root).ok();
    }
}
