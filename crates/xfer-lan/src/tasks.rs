//! 传输任务表（协议第 3 节）与本地持久化。
//!
//! 任务对象是 RPC 与客户端卡片的共同数据结构，字段名（camelCase）必须与协议一致；
//! `finishedAt` / `sourcePath` / `peerId` 是协议允许的**本地字段**（不上线）。

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// 任务表上限（协议第 3 节）。
pub(crate) const MAX_TASKS: usize = 50;

const TASKS_FILE: &str = "lan-tasks.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Out,
    In,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Waiting,
    Active,
    Done,
    Error,
}

/// 一条局域网传输任务。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct LanTask {
    pub id: String,
    pub direction: Direction,
    pub name: String,
    /// 字节数；-1 = 未知（发送方未声明）。
    pub size: i64,
    pub transferred: i64,
    pub status: TaskStatus,
    pub peer_name: String,
    pub peer_address: String,
    pub error: String,
    /// 短认证串（6 位数字）：握手派生会话密钥时写入，供两端用户比对防中间人。
    /// 发送任务在收到 accept 前为空。
    pub sas: String,
    pub created_at: i64,
    /// 进入 done / error 的时刻（本地字段，仅用于卡片显示）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<i64>,
    /// 接收端落盘路径（仅 in）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// 对方设备 id（本地字段，供重试定位设备）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<String>,
    /// 源文件路径（本地字段，仅 out，供重试）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
}

impl Default for Direction {
    fn default() -> Self {
        Self::Out
    }
}

impl Default for TaskStatus {
    fn default() -> Self {
        Self::Waiting
    }
}

impl LanTask {
    /// 是否已进入终态（done / error）。
    pub fn is_finished(&self) -> bool {
        matches!(self.status, TaskStatus::Done | TaskStatus::Error)
    }
}

/// 启动时迁移：waiting / active 一律视为被上次退出打断 → error「interrupted」。
///
/// 进程重启后这两类任务既没有连接也没有队列，留着只会永远卡在"传输中"。
pub(crate) fn migrate_on_startup(mut tasks: Vec<LanTask>) -> Vec<LanTask> {
    let now = crate::now_ms();
    for task in &mut tasks {
        if matches!(task.status, TaskStatus::Waiting | TaskStatus::Active) {
            task.status = TaskStatus::Error;
            task.error = "interrupted".to_string();
            task.finished_at = Some(now);
        }
    }
    // 最新在前，且只保留最近 50 条
    tasks.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    tasks.truncate(MAX_TASKS);
    tasks
}

/// 超出上限时从**最旧的终态任务**开始丢，未完成的任务不丢（否则排队中的文件会凭空消失）。
pub(crate) fn trim(mut tasks: Vec<LanTask>) -> Vec<LanTask> {
    while tasks.len() > MAX_TASKS {
        match tasks.iter().rposition(LanTask::is_finished) {
            Some(idx) => {
                tasks.remove(idx);
            }
            // 全是未完成任务：宁可暂时超出上限，也不能丢用户排队的文件
            None => break,
        }
    }
    tasks
}

fn tasks_path(data_dir: &Path) -> PathBuf {
    data_dir.join(TASKS_FILE)
}

/// 读取任务表；文件缺失/损坏都退回空表（不让引擎启动失败）。
pub(crate) fn load(data_dir: &Path) -> Vec<LanTask> {
    let path = tasks_path(data_dir);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "读取局域网任务表失败，按空表启动");
            return Vec::new();
        }
    };
    match serde_json::from_slice::<Vec<LanTask>>(&bytes) {
        Ok(tasks) => migrate_on_startup(tasks),
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "解析局域网任务表失败，按空表启动");
            Vec::new()
        }
    }
}

/// 写盘：临时文件 + rename，避免半截 JSON 覆盖掉上一份可用状态。
pub(crate) fn save(data_dir: &Path, tasks: &[LanTask]) -> std::io::Result<()> {
    std::fs::create_dir_all(data_dir)?;
    let path = tasks_path(data_dir);
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec(tasks)
        .map_err(|e| std::io::Error::other(format!("序列化任务表失败：{e}")))?;
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, &path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(id: &str, status: TaskStatus, created_at: i64) -> LanTask {
        LanTask {
            id: id.to_string(),
            direction: Direction::In,
            name: format!("{id}.bin"),
            size: 10,
            status,
            created_at,
            ..Default::default()
        }
    }

    #[test]
    fn interrupted_tasks_become_error() {
        let tasks = vec![
            task("waiting", TaskStatus::Waiting, 3),
            task("active", TaskStatus::Active, 2),
            task("done", TaskStatus::Done, 1),
            task("error", TaskStatus::Error, 0),
        ];
        let migrated = migrate_on_startup(tasks);
        assert_eq!(migrated.len(), 4);
        // 最新在前
        assert_eq!(migrated[0].id, "waiting");
        for t in &migrated {
            match t.id.as_str() {
                "waiting" | "active" => {
                    assert_eq!(t.status, TaskStatus::Error);
                    assert_eq!(t.error, "interrupted");
                    assert!(t.finished_at.is_some());
                }
                _ => assert!(t.finished_at.is_none()),
            }
        }
    }

    #[test]
    fn persistence_roundtrip() {
        let dir = temp_dir("roundtrip");
        let tasks = vec![
            task("a", TaskStatus::Done, 2),
            task("b", TaskStatus::Error, 1),
        ];
        save(&dir, &tasks).unwrap();
        let loaded = load(&dir);
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id, "a");
        assert_eq!(loaded[0].status, TaskStatus::Done);
        assert_eq!(loaded[1].error, "");
        // 再存一次（waiting → 重启后必须变 interrupted）
        save(&dir, &[task("c", TaskStatus::Waiting, 3)]).unwrap();
        let loaded = load(&dir);
        assert_eq!(loaded[0].status, TaskStatus::Error);
        assert_eq!(loaded[0].error, "interrupted");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn keeps_at_most_fifty_and_prefers_dropping_old_finished() {
        // 约定：任务表最新在前，所以 t59 在前、t0 在最旧的一端
        let mut tasks: Vec<LanTask> = (0..60)
            .rev()
            .map(|i| task(&format!("t{i}"), TaskStatus::Done, i))
            .collect();
        tasks.push(task("queued", TaskStatus::Waiting, 100));
        let trimmed = trim(tasks);
        assert_eq!(trimmed.len(), MAX_TASKS);
        assert!(trimmed.iter().any(|t| t.id == "queued"));
        assert!(!trimmed.iter().any(|t| t.id == "t0"));
    }

    #[test]
    fn missing_file_loads_empty() {
        let dir = temp_dir("missing");
        std::fs::create_dir_all(&dir).unwrap();
        assert!(load(&dir).is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// RPC 直接返回这个结构，字段名必须与协议第 3 节一致（含本地字段 sas）。
    #[test]
    fn task_json_matches_protocol_shape() {
        let mut task = task("t1", TaskStatus::Done, 1_710_000_000_000);
        task.sas = "042317".to_string();
        task.path = Some("/dl/a.bin".to_string());
        let value = serde_json::to_value(&task).unwrap();
        assert_eq!(value["id"], "t1");
        assert_eq!(value["direction"], "in");
        assert_eq!(value["status"], "done");
        assert_eq!(value["sas"], "042317");
        assert_eq!(value["path"], "/dl/a.bin");
        assert_eq!(value["createdAt"], 1_710_000_000_000i64);
        // 未设置的本地字段不出现（客户端按可选字段处理）
        assert!(value.get("finishedAt").is_none());
        assert!(value.get("sourcePath").is_none());
        assert!(value.get("peerId").is_none());
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "xfer-lan-tasks-{tag}-{}-{}",
            std::process::id(),
            crate::random_hex(4)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
