//! 测试共享设施（仅 `cfg(test)` 编译）。
//!
//! # 为什么控制文件目录必须"全进程初始化一次"
//!
//! `ctrl_path()` 的基目录取自进程级环境变量 `XFER_CTRL_DIR`，而 `cargo test`
//! 默认在**同一个进程**里并行跑整个测试二进制的所有测试。若每个测试各自把
//! 它改写成"自己的"目录，就会与并行执行的别的测试互相干扰：
//!
//! - 一个耗时数秒的集成用例（分片重试、退避、超时）在运行途中被别的测试
//!   改写环境变量，于是 `ctrl_path()` 在同一个用例里先后算出**两个目录**；
//! - 表现为"控制文件明明写进去了，断言却查不到"的间歇性失败——本机串行
//!   时几乎不出现，CI 上（机器慢、并行度高、窗口更宽）必现。
//!
//! 因此本模块提供唯一入口 [`init_ctrl_dir`]：第一次调用时建目录、清理旧
//! 残留并把环境变量钉死，之后所有调用都返回同一个路径。这样整个测试二进制
//! 内 `ctrl_path()` 的结果是确定的，而各用例之间靠"路径哈希 + 各自的下载
//! 目录"天然互不冲突（不需要按用例分目录）。

use std::path::PathBuf;
use std::sync::OnceLock;

/// 初始化（或取得）本测试进程共享的控制文件目录。
///
/// 首次调用会删除并重建目录：`std::process::id()` 会被系统复用，残留的
/// 控制文件可能让"从头下载"的用例被误判为可续传（指纹恰好一致时）——
/// 那比"目录不干净"更难排查。
pub(crate) fn init_ctrl_dir() -> PathBuf {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("xfer-http-ctrl-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("创建测试控制文件目录失败");
        std::env::set_var("XFER_CTRL_DIR", &dir);
        dir
    })
    .clone()
}
