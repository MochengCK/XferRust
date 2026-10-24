//! 端到端：进程内起一个最小 SFTP 服务器（russh + russh-sftp server），
//! 验证密码/私钥认证、主机密钥 TOFU、多通道下载、取消后续传。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Error;
use russh::server::{Auth, Msg, Server, Session};
use russh::{Channel, ChannelId};
use russh_sftp::protocol::{
    Attrs, Data, File, FileAttributes, Handle, Name, Status, StatusCode, Version,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use xfer_sftp::{
    can_resume, ctrl_path, download, is_sftp_url, SftpClient, SftpOptions, SftpStats, SftpUrl,
};
use xfer_types::rate::RateLimiter;

// ---------------------------------------------------------------------------
// 最小 SFTP 服务器：一个固定内容文件，忽略路径
// ---------------------------------------------------------------------------

struct Content {
    data: Vec<u8>,
    delay: AtomicU64,
    reads: AtomicU64,
}

#[derive(Clone)]
struct TestServer {
    content: Arc<Content>,
}

struct SshSession {
    content: Arc<Content>,
    channels: Arc<Mutex<HashMap<ChannelId, Channel<Msg>>>>,
    accept_key: bool,
}

impl Server for TestServer {
    type Handler = SshSession;

    fn new_client(&mut self, _: Option<std::net::SocketAddr>) -> Self::Handler {
        SshSession {
            content: self.content.clone(),
            channels: Arc::new(Mutex::new(HashMap::new())),
            accept_key: true,
        }
    }
}

impl russh::server::Handler for SshSession {
    type Error = Error;

    async fn auth_password(&mut self, user: &str, password: &str) -> Result<Auth, Self::Error> {
        if user == "test" && password == "secret" {
            Ok(Auth::Accept)
        } else {
            Ok(Auth::Reject {
                proceed_with_methods: None,
                partial_success: false,
            })
        }
    }

    async fn auth_publickey(
        &mut self,
        user: &str,
        _key: &russh::keys::ssh_key::PublicKey,
    ) -> Result<Auth, Self::Error> {
        if user == "test" && self.accept_key {
            Ok(Auth::Accept)
        } else {
            Ok(Auth::Reject {
                proceed_with_methods: None,
                partial_success: false,
            })
        }
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        reply: russh::server::ChannelOpenHandle,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.channels.lock().unwrap().insert(channel.id(), channel);
        reply.accept().await;
        Ok(())
    }

    async fn subsystem_request(
        &mut self,
        channel_id: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if name != "sftp" {
            session.channel_failure(channel_id)?;
            return Ok(());
        }
        let channel = self.channels.lock().unwrap().remove(&channel_id);
        let Some(channel) = channel else {
            session.channel_failure(channel_id)?;
            return Ok(());
        };
        session.channel_success(channel_id)?;
        let content = self.content.clone();
        // 每条子系统独立跑，否则多通道会被第一条通道堵死。
        tokio::spawn(async move {
            russh_sftp::server::run(channel.into_stream(), SftpHandler { content }).await;
        });
        Ok(())
    }
}

struct SftpHandler {
    content: Arc<Content>,
}

impl russh_sftp::server::Handler for SftpHandler {
    type Error = StatusCode;

    fn unimplemented(&self) -> Self::Error {
        StatusCode::OpUnsupported
    }

    async fn init(
        &mut self,
        _version: u32,
        _extensions: HashMap<String, String>,
    ) -> Result<Version, Self::Error> {
        Ok(Version::new())
    }

    async fn open(
        &mut self,
        id: u32,
        _filename: String,
        _pflags: russh_sftp::protocol::OpenFlags,
        _attrs: FileAttributes,
    ) -> Result<Handle, Self::Error> {
        Ok(Handle {
            id,
            handle: "file".into(),
        })
    }

    async fn read(
        &mut self,
        id: u32,
        _handle: String,
        offset: u64,
        len: u32,
    ) -> Result<Data, Self::Error> {
        let delay = self.content.delay.load(Ordering::Relaxed);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        self.content.reads.fetch_add(1, Ordering::Relaxed);
        let data = &self.content.data;
        if offset as usize >= data.len() {
            return Err(StatusCode::Eof);
        }
        let end = (offset as usize + len as usize).min(data.len());
        Ok(Data {
            id,
            data: data[offset as usize..end].to_vec(),
        })
    }

    async fn close(&mut self, id: u32, _handle: String) -> Result<Status, Self::Error> {
        Ok(ok_status(id))
    }

    async fn stat(&mut self, id: u32, _path: String) -> Result<Attrs, Self::Error> {
        Ok(Attrs {
            id,
            attrs: file_attrs(self.content.data.len() as u64),
        })
    }

    async fn fstat(&mut self, id: u32, _handle: String) -> Result<Attrs, Self::Error> {
        Ok(Attrs {
            id,
            attrs: file_attrs(self.content.data.len() as u64),
        })
    }

    async fn realpath(&mut self, id: u32, _path: String) -> Result<Name, Self::Error> {
        Ok(Name {
            id,
            files: vec![File::dummy("/home/test")],
        })
    }
}

fn file_attrs(size: u64) -> FileAttributes {
    FileAttributes {
        size: Some(size),
        permissions: Some(0o100644),
        ..Default::default()
    }
}

fn ok_status(id: u32) -> Status {
    Status {
        id,
        status_code: StatusCode::Ok,
        error_message: "Ok".into(),
        language_tag: "en-US".into(),
    }
}

async fn start_server(content: Vec<u8>, delay_ms: u64) -> (u16, Arc<Content>) {
    // 先占一个端口再释放，交给 run_on_address（russh 的 run_on_socket 借用了
    // listener，不方便跨任务持有）。
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let content = Arc::new(Content {
        data: content,
        delay: AtomicU64::new(delay_ms),
        reads: AtomicU64::new(0),
    });
    let host_key =
        russh::keys::PrivateKey::random(&mut rand::rng(), russh::keys::Algorithm::Ed25519).unwrap();
    let config = russh::server::Config {
        auth_rejection_time: Duration::from_millis(10),
        auth_rejection_time_initial: Some(Duration::from_millis(0)),
        keys: vec![host_key],
        ..Default::default()
    };
    let mut server = TestServer {
        content: content.clone(),
    };
    tokio::spawn(async move {
        let _ = server
            .run_on_address(Arc::new(config), ("127.0.0.1", port))
            .await;
    });
    // 等监听就绪
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    (port, content)
}

fn opts(known_hosts: &std::path::Path, connections: usize) -> SftpOptions {
    SftpOptions {
        user: None,
        password: None,
        private_key: None,
        private_key_passphrase: None,
        known_hosts: known_hosts.to_path_buf(),
        strict_host_key: false,
        connections,
        timeout_secs: 10,
    }
}

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "xfer-sftp-e2e-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn content(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 31 % 251) as u8).collect()
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn password_auth_multi_channel_download_and_tofu() {
    let data = content(5 * 1024 * 1024 + 123);
    let (port, _content) = start_server(data.clone(), 0).await;
    let dir = tmpdir("multi");
    let known = dir.join("known_hosts");
    let target = dir.join("out.bin");
    let o = opts(&known, 4);
    let url = SftpUrl::parse(&format!("sftp://test:secret@127.0.0.1:{port}/var/file.bin")).unwrap();
    assert!(is_sftp_url(&url.raw));

    let client = SftpClient::connect(&url, &o).await.unwrap();
    assert!(known.exists(), "首次连接应写入 TOFU 主机密钥");
    let probe = client.probe(&url, &o, &url.path).await.unwrap();
    assert_eq!(probe.size, data.len() as u64);
    assert_eq!(probe.filename, "file.bin");

    let stats = Arc::new(SftpStats::new(0));
    let done = download(
        &client,
        &target,
        &probe.remote_path,
        probe.size,
        &o,
        RateLimiter::new(0),
        &CancellationToken::new(),
        stats.clone(),
    )
    .await
    .unwrap();
    assert_eq!(done.total, data.len() as u64);
    assert_eq!(done.resumed, 0);
    assert_eq!(std::fs::read(&target).unwrap(), data);
    assert!(!ctrl_path(&target).exists(), "完成后应删除控制文件");
    assert_eq!(stats.completed(), data.len() as u64);

    client.disconnect().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_key_auth_works() {
    let data = content(1024 * 1024);
    let (port, _content) = start_server(data.clone(), 0).await;
    let dir = tmpdir("key");
    let known = dir.join("known_hosts");

    let key =
        russh::keys::PrivateKey::random(&mut rand::rng(), russh::keys::Algorithm::Ed25519).unwrap();
    let key_path = dir.join("id_ed25519");
    std::fs::write(
        &key_path,
        key.to_openssh(russh::keys::ssh_key::LineEnding::LF)
            .unwrap()
            .as_bytes(),
    )
    .unwrap();

    let mut o = opts(&known, 2);
    o.private_key = Some(key_path);
    let url = SftpUrl::parse(&format!("sftp://test@127.0.0.1:{port}/file.bin")).unwrap();
    let client = SftpClient::connect(&url, &o).await.unwrap();
    let probe = client.probe(&url, &o, &url.path).await.unwrap();
    let target = dir.join("out.bin");
    download(
        &client,
        &target,
        &probe.remote_path,
        probe.size,
        &o,
        RateLimiter::new(0),
        &CancellationToken::new(),
        Arc::new(SftpStats::new(0)),
    )
    .await
    .unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), data);
    client.disconnect().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wrong_password_is_auth_error() {
    let (port, _content) = start_server(content(1024), 0).await;
    let dir = tmpdir("badpass");
    let known = dir.join("known_hosts");
    let o = opts(&known, 1);
    let url = SftpUrl::parse(&format!("sftp://test:nope@127.0.0.1:{port}/f")).unwrap();
    let err = match SftpClient::connect(&url, &o).await {
        Ok(_) => panic!("错误密码不应连接成功"),
        Err(e) => e,
    };
    assert!(matches!(err, xfer_sftp::SftpError::Auth(_)), "{err:?}");
    assert!(!err.is_retryable());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_then_resume_from_control_file() {
    let data = content(12 * 1024 * 1024);
    let (port, server) = start_server(data.clone(), 40).await;
    let dir = tmpdir("resume");
    let known = dir.join("known_hosts");
    let target = dir.join("out.bin");
    let o = opts(&known, 4);
    let url = SftpUrl::parse(&format!("sftp://test:secret@127.0.0.1:{port}/big.bin")).unwrap();
    let client = SftpClient::connect(&url, &o).await.unwrap();
    let probe = client.probe(&url, &o, &url.path).await.unwrap();

    let cancel = CancellationToken::new();
    let stats = Arc::new(SftpStats::new(0));
    let dl = download(
        &client,
        &target,
        &probe.remote_path,
        probe.size,
        &o,
        RateLimiter::new(0),
        &cancel,
        stats,
    );
    tokio::pin!(dl);
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(1200)) => {}
        r = &mut dl => panic!("下载在取消前就结束了: {r:?}"),
    }
    cancel.cancel();
    let err = dl.await.unwrap_err();
    assert!(matches!(err, xfer_sftp::SftpError::Cancelled), "{err:?}");
    assert!(ctrl_path(&target).exists(), "取消后应留下续传控制文件");
    assert!(can_resume(&target, probe.size, &probe.remote_path));

    // 第二轮不再延迟，并重新握手（生产里暂停恢复就是新连接），验证从已有片继续
    server.delay.store(0, Ordering::Relaxed);
    client.disconnect().await;
    let client = SftpClient::connect(&url, &o).await.unwrap();
    let done = download(
        &client,
        &target,
        &probe.remote_path,
        probe.size,
        &o,
        RateLimiter::new(0),
        &CancellationToken::new(),
        Arc::new(SftpStats::new(0)),
    )
    .await
    .unwrap();
    assert!(done.resumed > 0, "第二轮应继承至少一片");
    assert_eq!(std::fs::read(&target).unwrap(), data);
    assert!(!ctrl_path(&target).exists());
    client.disconnect().await;
    let _ = std::fs::remove_dir_all(&dir);
}
