//! HTTP 分片下载吞吐基准：本地回环服务 + 引擎全链路实测。
//!
//! 用法：
//!   cargo run --release --example http_bench [size_mb] [split]
//!
//! 流程：内存构造确定性数据 → 极简 HTTP/1.1 Range 服务（全速回环）→
//! `TaskManager` 生产路径下载（分片 + 自适应调度 + 控制文件）→
//! 内容逐字节校验 → 输出平均/峰值吞吐。用于验证下载管线
//! （网络 → 通道 → 写线程 → 磁盘）无系统性瓶颈。

use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use xfer_engine::TaskManager;

/// 确定性数据（位置敏感：任何错位写都会被校验出来）。
fn pattern_at(i: usize) -> u8 {
    (i % 251) as u8
}

/// 极简 HTTP/1.1 服务：支持 `Range: bytes=a-b`，全速流式应答，
/// 连接复用（keep-alive 连续请求）。
async fn serve(mut sock: TcpStream, data: Arc<Vec<u8>>) {
    let total = data.len();
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 2048];
    loop {
        // 读到一个完整请求头
        loop {
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
            match sock.read(&mut tmp).await {
                Ok(0) => return,
                Ok(n) => buf.extend_from_slice(&tmp[..n]),
                Err(_) => return,
            }
        }
        let head_end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
        let head = String::from_utf8_lossy(&buf[..head_end]).to_ascii_lowercase();
        buf.drain(..head_end);

        // 解析 Range（缺省 = 全量）
        let (from, to) = head
            .lines()
            .find_map(|l| l.strip_prefix("range: bytes="))
            .and_then(|r| r.split_once('-'))
            .map(|(f, t)| {
                let f = f.parse::<usize>().unwrap_or(0).min(total);
                let t = if t.is_empty() {
                    total
                } else {
                    (t.parse::<usize>().unwrap_or(total - 1) + 1).min(total)
                };
                (f, t.max(f))
            })
            .unwrap_or((0, total));
        let len = to - from;
        let status = if from == 0 && to == total {
            "200 OK"
        } else {
            "206 Partial Content"
        };
        let mut resp = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {len}\r\nAccept-Ranges: bytes\r\nConnection: keep-alive\r\n"
        );
        if from > 0 || to < total {
            resp.push_str(&format!("Content-Range: bytes {from}-{}/{total}\r\n", to - 1));
        }
        resp.push_str("\r\n");
        if sock.write_all(resp.as_bytes()).await.is_err() {
            return;
        }
        // 全速流式发送数据区
        let mut off = from;
        while off < to {
            let n = (to - off).min(128 * 1024);
            if sock.write_all(&data[off..off + n]).await.is_err() {
                return;
            }
            off += n;
        }
        if sock.flush().await.is_err() {
            return;
        }
    }
}

fn main() {
    let size_mb: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(512);
    let split: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);
    let total = size_mb.saturating_mul(1024 * 1024);
    println!("基准：{size_mb}MB / {split} 连接（引擎全链路：分片+自适应+落盘）");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let data: Arc<Vec<u8>> = Arc::new((0..total).map(pattern_at).collect());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (sock, _) = match listener.accept().await {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let d = data.clone();
                tokio::spawn(serve(sock, d));
            }
        });

        let dir = std::env::temp_dir().join(format!("xfer-bench-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mgr = TaskManager::start(dir.clone(), 1);
        let url = format!("http://{addr}/bench.bin");
        let gid = mgr
            .add_uri(
                vec![url],
                &json!({
                    "dir": dir.to_string_lossy(),
                    "split": split.to_string(),
                    "min-split-size": "4M",
                }),
                None,
            )
            .expect("addUri 失败");

        let t0 = Instant::now();
        let mut peak_speed = 0u64;
        let mut last_print = Instant::now();
        let path: PathBuf;
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let st = mgr.tell_status_native(&gid, None).unwrap();
            peak_speed = peak_speed.max(st["downloadSpeed"].as_u64().unwrap_or(0));
            let status = st["status"].as_str().unwrap_or_default();
            if last_print.elapsed() >= Duration::from_secs(1) {
                last_print = Instant::now();
                let done = st["completedLength"].as_u64().unwrap_or(0);
                println!(
                    "  {:.1}%  瞬时 {:.1} MB/s",
                    done as f64 / total as f64 * 100.0,
                    st["downloadSpeed"].as_u64().unwrap_or(0) as f64 / 1048576.0
                );
            }
            if status == "complete" {
                path = PathBuf::from(st["files"][0]["path"].as_str().unwrap_or(""));
                break;
            }
            if status == "error" {
                panic!("任务失败: {st}");
            }
        }
        let secs = t0.elapsed().as_secs_f64();
        let avg = total as f64 / secs / 1048576.0;
        println!(
            "完成：{:.2}s  平均 {:.1} MB/s  峰值 {:.1} MB/s",
            secs,
            avg,
            peak_speed as f64 / 1048576.0
        );

        // 内容逐块校验（模式比对，避免整文件双份内存）
        let t1 = Instant::now();
        let mut f = std::fs::File::open(&path).unwrap();
        let mut buf = vec![0u8; 1024 * 1024];
        let mut pos = 0usize;
        loop {
            let n = f.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            for (i, b) in buf[..n].iter().enumerate() {
                assert_eq!(*b, pattern_at(pos + i), "内容错位 @{}", pos + i);
            }
            pos += n;
        }
        assert_eq!(pos, total, "文件长度不一致");
        println!(
            "内容校验通过（{}MB，{:.2}s）",
            total / 1048576,
            t1.elapsed().as_secs_f64()
        );
        let _ = std::fs::remove_dir_all(&dir);
    });
}
