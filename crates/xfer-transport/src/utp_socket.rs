//! uTP (BEP 29) async UDP socket + 连接管理器。
//!
//! 功能：
//! - 绑定一个 UDP socket，复用所有 uTP 连接；
//! - 入站数据报按 connection_id 分发到对应 `UtpConnection`；
//! - 出站数据报从各连接的 outbox 收集后批量发送；
//! - 1ms tick 驱动所有连接的 `process_tick()`；
//! - 提供 `UtpStream` 接口供上层使用（write_all / read_data / close）。
//!
//! §7.6：uTP 与 TCP 监听同端口（对端把 uTP SYN 发到通告的监听端口）。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::debug;

use crate::utp_connection::UtpConnection;
use crate::utp_packet::{packet_type, parse_packet};

/// uTP 流（用户持有的双向数据通道）。
///
/// 通过 `write_all()` 发送数据，`read_data()` 接收数据，`close()` 关闭连接。
/// 同时实现 tokio `AsyncRead`/`AsyncWrite`，可直接接入泛型化的
/// MSE/peer-wire 读写路径（消息式通道桥接为字节流）。
pub struct UtpStream {
    /// 命令通道：向 socket 管理器发指令（Write/Close）。
    cmd_tx: mpsc::Sender<UtpCmd>,
    /// 数据接收通道：管理器向用户推送收到的数据。
    data_rx: mpsc::Receiver<io::Result<Vec<u8>>>,
    /// AsyncRead 残留缓冲：收到的块大于调用方 buf 时保留剩余字节。
    residual: Vec<u8>,
    /// AsyncRead 已见错误/EOF 后置位，后续读立即返回。
    read_done: bool,
    remote_addr: SocketAddr,
}

/// socket 管理器命令（用户 → 管理器）。
enum UtpCmd {
    Write(Vec<u8>),
    Close,
}

impl UtpStream {
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    /// 写入数据（非阻塞，缓冲到连接 pending_send）。
    pub async fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        self.cmd_tx
            .send(UtpCmd::Write(data.to_vec()))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::ConnectionReset, "uTP 已关闭"))?;
        Ok(())
    }

    /// 读取数据。返回 Ok(Vec) 表示收到数据，Err 表示连接关闭。
    pub async fn read_data(&mut self) -> io::Result<Vec<u8>> {
        match self.data_rx.recv().await {
            Some(Ok(data)) => Ok(data),
            Some(Err(e)) => Err(e),
            None => Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "uTP 连接已关闭",
            )),
        }
    }

    /// 关闭连接（发送 FIN）。
    pub async fn close(&mut self) -> io::Result<()> {
        let _ = self.cmd_tx.send(UtpCmd::Close).await;
        Ok(())
    }
}

/// AsyncRead：把消息式数据通道桥接成字节流。
///
/// - 先消费残留缓冲，再 `poll_recv` 数据通道；
/// - 通道关闭（None）→ EOF（Ok 且 0 字节）；管理器推送的 Err 原样返回。
impl AsyncRead for UtpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.read_done {
            return Poll::Ready(Ok(()));
        }
        if !self.residual.is_empty() {
            let n = buf.remaining().min(self.residual.len());
            buf.put_slice(&self.residual[..n]);
            self.residual.drain(..n);
            return Poll::Ready(Ok(()));
        }
        match self.data_rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(data))) => {
                if data.is_empty() {
                    // 空块：再次轮询，避免向调用方返回假 EOF/空读
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                let n = buf.remaining().min(data.len());
                buf.put_slice(&data[..n]);
                if n < data.len() {
                    self.residual = data[n..].to_vec();
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Err(e))) => {
                self.read_done = true;
                Poll::Ready(Err(e))
            }
            Poll::Ready(None) => {
                self.read_done = true;
                Poll::Ready(Ok(())) // EOF
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// AsyncWrite：写入经命令通道下发，`try_reserve` 提供背压。
impl AsyncWrite for UtpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match this.cmd_tx.try_reserve() {
            Ok(permit) => {
                permit.send(UtpCmd::Write(buf.to_vec()));
                Poll::Ready(Ok(buf.len()))
            }
            Err(mpsc::error::TrySendError::Full(())) => {
                // 命令通道暂满：管理器每 1ms tick 消费，唤醒重试
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(mpsc::error::TrySendError::Closed(())) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "uTP 已关闭",
            ))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // 命令通道即送达语义，无独立刷盘步骤
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match this.cmd_tx.try_reserve() {
            Ok(permit) => {
                permit.send(UtpCmd::Close);
                Poll::Ready(Ok(()))
            }
            Err(mpsc::error::TrySendError::Full(())) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(mpsc::error::TrySendError::Closed(())) => Poll::Ready(Ok(())), // 已关闭
        }
    }
}

/// uTP 连接阶段（出站拨号等待握手完成用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UtpPhase {
    /// SYN 已发、握手未完成。
    Connecting,
    /// 握手完成（SYN 被确认）。
    Established,
    /// 连接在握手完成前关闭/出错。
    Failed,
}

/// 单个连接的上下文（管理器内部使用）。
struct ConnContext {
    conn: UtpConnection,
    /// 命令接收端（用户通过 UtpStream 发来的 Write/Close）。
    cmd_rx: mpsc::Receiver<UtpCmd>,
    /// 数据发送端（管理器向用户推送收到的数据）。
    data_tx: mpsc::Sender<io::Result<Vec<u8>>>,
    /// 连接阶段广播（出站 `connect_established` 等待）。
    phase_tx: tokio::sync::watch::Sender<UtpPhase>,
    /// 最近一次已广播的阶段（避免重复 send）。
    phase: UtpPhase,
    /// 连接 `pending_send` 满（MAX_PENDING_SEND）时 `conn.write`
    /// 只接受部分字节，剩余数据在此排队，每个 tick 重试灌入。
    /// 之前剩余字节被直接丢弃，而 `UtpStream::write_all` 已向调用方返回
    /// Ok —— 上行流出现缺口 → 流损坏。
    write_backlog: Vec<u8>,
}

impl ConnContext {
    /// 依据连接状态同步阶段广播（建立/失败），供 `connect_established` 等待。
    fn sync_phase(&mut self) {
        let next = if self.conn.is_closed() || self.conn.has_error() {
            UtpPhase::Failed
        } else if self.conn.is_connected() {
            UtpPhase::Established
        } else {
            UtpPhase::Connecting
        };
        if next != self.phase {
            self.phase = next;
            let _ = self.phase_tx.send(next);
        }
    }
}

/// uTP socket 管理器。
///
/// 绑定一个 UDP socket，管理所有 uTP 连接。
/// 通过 `UtpManagerHandle` 与外部交互。
pub struct UtpManager {
    socket: UdpSocket,
    /// 已建立的连接：recv_id → ConnContext。
    connections: HashMap<u16, ConnContext>,
    /// 入站连接通知通道。
    incoming_tx: mpsc::Sender<UtpStream>,
    /// tick 间隔。
    tick_interval: Duration,
}

impl UtpManager {
    /// 创建并启动 uTP socket 管理器。
    ///
    /// 返回 (handle, incoming_receiver)。
    /// `incoming_receiver` 接收新入站连接。
    pub async fn bind(
        addr: &str,
        port: u16,
    ) -> io::Result<(UtpManagerHandle, mpsc::Receiver<UtpStream>)> {
        let socket = UdpSocket::bind((addr, port)).await?;
        let local_addr = socket.local_addr()?;

        let (incoming_tx, incoming_rx) = mpsc::channel(64);
        let (cmd_tx, cmd_rx) = mpsc::channel(256);

        let manager = UtpManager {
            socket,
            connections: HashMap::new(),
            incoming_tx,
            tick_interval: Duration::from_millis(1),
        };

        let handle = UtpManagerHandle { local_addr, cmd_tx };

        // 启动管理器任务
        tokio::spawn(async move {
            manager.run(cmd_rx).await;
        });

        Ok((handle, incoming_rx))
    }

    /// 创建一个出站 uTP 连接（管理器内部方法）。
    ///
    /// 返回 (stream, 阶段接收端)；阶段接收端供 `connect_established`
    /// 等待握手完成。
    fn do_connect(&mut self, remote: SocketAddr) -> io::Result<(UtpStream, tokio::sync::watch::Receiver<UtpPhase>)> {
        let now = Instant::now();
        let conn = UtpConnection::new_outbound(remote, now);

        // 获取 recv_id 用于路由
        let recv_id = conn.recv_id();

        // 创建 stream 通道
        let (data_tx, data_rx) = mpsc::channel(256);
        let (stream_cmd_tx, stream_cmd_rx) = mpsc::channel(256);
        let (phase_tx, phase_rx) = tokio::sync::watch::channel(UtpPhase::Connecting);

        let ctx = ConnContext {
            conn,
            cmd_rx: stream_cmd_rx,
            data_tx,
            phase_tx,
            phase: UtpPhase::Connecting,
            write_backlog: Vec::new(),
        };

        // 16-bit id 碰撞时不能静默覆盖旧连接 —— 放弃本次拨号
        // （上层引擎会回退 TCP），而不是破坏在用会话。
        if self.connections.contains_key(&recv_id) {
            debug!(recv_id, "uTP 拨号连接 id 冲突，放弃（回退 TCP）");
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "uTP 连接 id 冲突",
            ));
        }
        self.connections.insert(recv_id, ctx);

        // 立即处理 tick 以编码 SYN 到 outbox
        self.process_connection_tick(recv_id, now);

        Ok((
            UtpStream {
                cmd_tx: stream_cmd_tx,
                data_rx,
                residual: Vec::new(),
                read_done: false,
                remote_addr: remote,
            },
            phase_rx,
        ))
    }

    async fn run(mut self, mut cmd_rx: mpsc::Receiver<ManagerCmd>) {
        let mut tick = interval(self.tick_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut buf = [0u8; 2048];

        loop {
            tokio::select! {
                // 处理入站 UDP 数据报
                Ok((len, src)) = self.socket.recv_from(&mut buf) => {
                    self.handle_datagram(&buf[..len], src);
                }
                // tick 驱动
                _ = tick.tick() => {
                    self.process_all_ticks();
                }
                // 处理管理器命令
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        ManagerCmd::Connect { remote, tx } => {
                            match self.do_connect(remote) {
                                Ok(pair) => {
                                    let _ = tx.send(Ok(pair));
                                }
                                Err(e) => {
                                    let _ = tx.send(Err(e));
                                }
                            }
                        }
                        ManagerCmd::Shutdown => {
                            break;
                        }
                    }
                }
            }
        }
    }

    /// 处理一个入站 UDP 数据报。
    fn handle_datagram(&mut self, data: &[u8], src: SocketAddr) {
        let (hdr, _, _payload_off) = match parse_packet(data) {
            Some(v) => v,
            None => return,
        };

        let now = Instant::now();

        if hdr.type_ == packet_type::ST_SYN {
            let peer_recv_id = hdr.connection_id;
            // 重传的 SYN 携带 SYN.conn_id（= 响应方的 send_id），不会命中
            // 按 recv_id（= conn_id + 1）注册的连接；回找已接受该 SYN 的
            // 连接交给它重发 SYN-ACK，避免把重传 SYN 误当新连接重复建链。
            let prospective_recv_id = peer_recv_id.wrapping_add(1);
            let is_retrans = self
                .connections
                .get(&prospective_recv_id)
                .map(|ctx| ctx.conn.remote_addr() == src)
                .unwrap_or(false);
            if is_retrans {
                if let Some(ctx) = self.connections.get_mut(&prospective_recv_id) {
                    ctx.conn.handle_packet(data, now);
                    ctx.sync_phase();
                }
                self.process_connection_tick(prospective_recv_id, now);
                return;
            }

            // 新入站连接
            let syn_seq = hdr.seq_nr;

            let conn = UtpConnection::new_inbound(src, peer_recv_id, syn_seq, now);
            let recv_id = conn.recv_id();

            // 创建 stream 通道
            let (data_tx, data_rx) = mpsc::channel(256);
            let (stream_cmd_tx, stream_cmd_rx) = mpsc::channel(256);
            let (phase_tx, _phase_rx) = tokio::sync::watch::channel(UtpPhase::Connecting);

            let ctx = ConnContext {
                conn,
                cmd_rx: stream_cmd_rx,
                data_tx,
                phase_tx,
                phase: UtpPhase::Connecting,
                write_backlog: Vec::new(),
            };

            // 入站 SYN 的 recv_id 与现有连接冲突时丢弃该 SYN，
            // 不覆盖在用连接（对端重连时会换 id 重试）。
            if self.connections.contains_key(&recv_id) {
                debug!(recv_id, "uTP 入站 SYN 连接 id 冲突，丢弃");
                return;
            }
            self.connections.insert(recv_id, ctx);

            // 创建 UtpStream 推入 incoming 通道
            let stream = UtpStream {
                cmd_tx: stream_cmd_tx,
                data_rx,
                residual: Vec::new(),
                read_done: false,
                remote_addr: src,
            };

            // 非阻塞推入
            let _ = self.incoming_tx.try_send(stream);

            // 处理 tick（回送 ACK + 发送 outbox）
            self.process_connection_tick(recv_id, now);
        } else {
            // 路由到现有连接
            let conn_id = hdr.connection_id;

            // 路由命中后必须校验源地址 —— 不比对 src 时任意主机
            // 伪造匹配 id 的包即可注入数据/伪造 ACK，破坏在用连接的窗口。
            if let Some(ctx) = self.connections.get(&conn_id) {
                if ctx.conn.remote_addr() != src {
                    debug!(conn_id, ?src, "uTP 数据报源地址与连接不匹配，丢弃");
                    return;
                }
            } else {
                // 没有匹配的连接，丢弃
                return;
            }

            // 先处理包，再推送可读数据（带容量检查）与关闭检查
            let is_closed = {
                let Some(ctx) = self.connections.get_mut(&conn_id) else {
                    return;
                };
                ctx.conn.handle_packet(data, now);
                ctx.sync_phase();
                ctx.conn.is_closed() || ctx.conn.has_error()
            };

            // 立即推送可读数据（低时延）；通道满时数据留在 recv_out，
            // 由 1ms tick 的 process_readable_data 重试
            self.process_readable_data(conn_id);

            // 关闭检查
            if is_closed {
                self.finalize_closed_connection(conn_id);
            }
        }
    }

    /// 连接关闭后的统一收尾：广播失败阶段、通知读端、移除连接。
    fn finalize_closed_connection(&mut self, conn_id: u16) {
        if let Some(ctx) = self.connections.get_mut(&conn_id) {
            ctx.sync_phase();
            let _ = ctx.data_tx.try_send(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "uTP 连接已关闭",
            )));
        }
        self.connections.remove(&conn_id);
    }

    /// 处理所有连接的 tick。
    fn process_all_ticks(&mut self) {
        let now = Instant::now();
        let conn_ids: Vec<u16> = self.connections.keys().copied().collect();

        for conn_id in conn_ids {
            self.process_connection_tick(conn_id, now);
            self.process_stream_commands(conn_id, now);
            self.process_readable_data(conn_id);

            // 检查连接是否关闭
            let is_closed = self
                .connections
                .get(&conn_id)
                .map(|ctx| ctx.conn.is_closed() || ctx.conn.has_error())
                .unwrap_or(true);

            if is_closed {
                self.finalize_closed_connection(conn_id);
            }
        }
    }

    /// 处理单个连接的 tick + outbox 刷新。
    fn process_connection_tick(&mut self, conn_id: u16, now: Instant) {
        if let Some(ctx) = self.connections.get_mut(&conn_id) {
            ctx.conn.process_tick(now);
            ctx.sync_phase();

            // 从 outbox 取出包并发送
            let remote = ctx.conn.remote_addr();
            let outbox = ctx.conn.drain_outbox();
            for pkt in &outbox {
                if let Err(e) = self.socket.try_send_to(pkt, remote) {
                    debug!(error = %e, "uTP 发送失败");
                }
            }
        }
    }

    /// 处理 stream 命令（write/close）。
    fn process_stream_commands(&mut self, conn_id: u16, now: Instant) {
        // 先重试上次未接受完的积压数据（保持用户写入顺序）。
        // pending_send 腾出空间后逐步灌入；灌不进去就等下个 tick。
        if let Some(ctx) = self.connections.get_mut(&conn_id) {
            while !ctx.write_backlog.is_empty() {
                let accepted = ctx.conn.write(&ctx.write_backlog);
                if accepted == 0 {
                    break;
                }
                ctx.write_backlog.drain(..accepted);
            }
        }

        // 收集所有命令，避免可变借用冲突
        let commands: Vec<UtpCmd> = {
            let Some(ctx) = self.connections.get_mut(&conn_id) else {
                return;
            };
            let mut cmds = Vec::new();
            while let Ok(cmd) = ctx.cmd_rx.try_recv() {
                cmds.push(cmd);
            }
            cmds
        };

        for cmd in commands {
            match cmd {
                UtpCmd::Write(data) => {
                    if let Some(ctx) = self.connections.get_mut(&conn_id) {
                        // 连接只接受部分字节（pending_send 满）时，
                        // 未接受部分进入 backlog 等 tick 重试，绝不丢弃。
                        let accepted = ctx.conn.write(&data);
                        if accepted < data.len() {
                            ctx.write_backlog.extend_from_slice(&data[accepted..]);
                        }
                    }
                    self.process_connection_tick(conn_id, now);
                }
                UtpCmd::Close => {
                    if let Some(ctx) = self.connections.get_mut(&conn_id) {
                        ctx.conn.close();
                    }
                    self.process_connection_tick(conn_id, now);
                }
            }
        }
    }

    /// 处理可读数据。
    fn process_readable_data(&mut self, conn_id: u16) {
        let data_tx = match self.connections.get(&conn_id) {
            Some(ctx) => ctx.data_tx.clone(),
            None => return,
        };

        if let Some(ctx) = self.connections.get_mut(&conn_id) {
            // 读之前检查通道容量。字节一旦从 recv_out 取走就视为
            // 已交付（对端已 ACK），try_send 失败再丢弃会破坏有序可靠流
            // （报文边界错位 → 校验失败/连接反复断开）。容量不足时把数据
            // 留在 recv_out —— 通告窗口（recv_buffered + recv_out）自动
            // 收缩，对端自然减速，这正是 uTP 流控的正确姿势。
            while ctx.conn.want_read() && data_tx.capacity() > 0 {
                let mut buf = vec![0u8; 65536];
                let n = ctx.conn.read(&mut buf);
                if n == 0 {
                    if ctx.conn.eof_received() || ctx.conn.is_closed() {
                        let _ = data_tx.try_send(Err(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "uTP 连接已关闭",
                        )));
                    }
                    break;
                }
                buf.truncate(n);
                // capacity > 0 已检查且管理器单线程，必然成功
                if data_tx.try_send(Ok(buf)).is_err() {
                    break;
                }
            }
        }
    }
}

/// 管理器命令。
enum ManagerCmd {
    Connect {
        remote: SocketAddr,
        tx: tokio::sync::oneshot::Sender<
            io::Result<(UtpStream, tokio::sync::watch::Receiver<UtpPhase>)>,
        >,
    },
    Shutdown,
}

/// uTP 管理器句柄（Clone，用于外部操作）。
#[derive(Clone)]
pub struct UtpManagerHandle {
    local_addr: SocketAddr,
    cmd_tx: mpsc::Sender<ManagerCmd>,
}

impl UtpManagerHandle {
    /// 本地绑定地址。
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// 发起出站 uTP 连接（SYN 发出即返回，握手可能尚未完成）。
    ///
    /// 返回 UtpStream（可 read_data / write_all / close）。
    /// 需要确认握手完成时用 [`Self::connect_established`]。
    pub async fn connect(&self, remote: SocketAddr) -> io::Result<UtpStream> {
        let (stream, _phase) = self.connect_inner(remote).await?;
        Ok(stream)
    }

    /// 发起出站 uTP 连接并等待握手完成（或失败/超时）。
    ///
    /// 引擎拨号用：uTP 握手失败要能快速回退 TCP，不能等满整个
    /// BT 握手超时。握手在 `timeout` 内未完成按失败处理。
    pub async fn connect_established(
        &self,
        remote: SocketAddr,
        timeout: Duration,
    ) -> io::Result<UtpStream> {
        let (mut stream, mut phase_rx) = self.connect_inner(remote).await?;
        let wait = async {
            loop {
                let phase = *phase_rx.borrow_and_update();
                match phase {
                    UtpPhase::Established => return Ok(()),
                    UtpPhase::Failed => {
                        return Err(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "uTP 握手失败",
                        ))
                    }
                    UtpPhase::Connecting => {
                        if phase_rx.changed().await.is_err() {
                            return Err(io::Error::new(
                                io::ErrorKind::ConnectionReset,
                                "uTP 管理器已关闭",
                            ));
                        }
                    }
                }
            }
        };
        match tokio::time::timeout(timeout, wait).await {
            Ok(Ok(())) => Ok(stream),
            Ok(Err(e)) => {
                let _ = stream.close().await;
                Err(e)
            }
            Err(_) => {
                let _ = stream.close().await;
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "uTP 握手超时",
                ))
            }
        }
    }

    async fn connect_inner(
        &self,
        remote: SocketAddr,
    ) -> io::Result<(UtpStream, tokio::sync::watch::Receiver<UtpPhase>)> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.cmd_tx
            .send(ManagerCmd::Connect { remote, tx })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::ConnectionReset, "uTP 管理器已关闭"))?;
        rx.await
            .map_err(|_| io::Error::new(io::ErrorKind::ConnectionReset, "uTP 连接请求超时"))?
    }

    /// 关闭管理器。
    pub async fn shutdown(&self) {
        let _ = self.cmd_tx.send(ManagerCmd::Shutdown).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn utp_socket_bind() {
        let (handle, _rx) = UtpManager::bind("127.0.0.1", 0).await.unwrap();
        assert!(handle.local_addr().port() > 0);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn utp_loopback_data_transfer() {
        // 两个 uTP socket 互相通信
        let (server_handle, mut incoming_rx) = UtpManager::bind("127.0.0.1", 0).await.unwrap();
        let server_port = server_handle.local_addr().port();

        let (client_handle, _client_rx) = UtpManager::bind("127.0.0.1", 0).await.unwrap();

        // 客户端发起连接
        let mut client_stream = client_handle
            .connect(SocketAddr::from(([127, 0, 0, 1], server_port)))
            .await
            .expect("连接失败");

        // 等待服务端接收入站连接
        let mut server_stream = tokio::time::timeout(Duration::from_secs(5), incoming_rx.recv())
            .await
            .expect("入站连接超时")
            .expect("入站通道关闭");

        // 客户端 → 服务端
        let msg = b"Hello uTP from client!";
        client_stream.write_all(msg).await.unwrap();

        // 给 tick 时间处理
        tokio::time::sleep(Duration::from_millis(50)).await;

        let received = server_stream.read_data().await.expect("读取失败");
        assert_eq!(&received, msg);

        // 服务端 → 客户端
        let msg2 = b"Reply from server!";
        server_stream.write_all(msg2).await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;

        let received2 = client_stream.read_data().await.expect("读取失败");
        assert_eq!(&received2, msg2);

        // 关闭
        client_stream.close().await.ok();
        server_handle.shutdown().await;
        client_handle.shutdown().await;
    }

    /// AsyncRead/AsyncWrite 桥接：泛型读写路径（tokio::io）可直接使用
    /// UtpStream（引擎 MSE/peer-wire 接入的前提）。覆盖多包传输（>初始拥塞
    /// 窗口），依赖 RTTVAR 有符号运算修复（否则第二个 ACK 即下溢 panic）。
    #[tokio::test]
    async fn utp_stream_async_read_write_bridge() {
        use tokio::io::AsyncReadExt;

        let (server_handle, mut incoming_rx) = UtpManager::bind("127.0.0.1", 0).await.unwrap();
        let server_port = server_handle.local_addr().port();
        let (client_handle, _client_rx) = UtpManager::bind("127.0.0.1", 0).await.unwrap();

        let mut client = client_handle
            .connect_established(SocketAddr::from(([127, 0, 0, 1], server_port)), Duration::from_secs(5))
            .await
            .expect("connect_established 失败");
        let mut server = tokio::time::timeout(Duration::from_secs(5), incoming_rx.recv())
            .await
            .expect("入站连接超时")
            .expect("入站通道关闭");

        // 客户端用 AsyncWriteExt 写（全限定调用，绕过同名 inherent 方法，
        // 与引擎经泛型走 trait poll_write 的路径一致），服务端用
        // AsyncReadExt 分小块读（覆盖残留缓冲路径：写入块 > 读缓冲）
        let payload: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        tokio::io::AsyncWriteExt::write_all(&mut client, &payload)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut got = Vec::new();
        let mut buf = [0u8; 1024]; // 故意小于数据块，触发残留缓冲
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while got.len() < payload.len() {
            let n = tokio::time::timeout_at(deadline, server.read(&mut buf))
                .await
                .expect("读取超时")
                .expect("读取失败");
            if n == 0 {
                panic!("意外的 EOF，已读 {}", got.len());
            }
            got.extend_from_slice(&buf[..n]);
        }
        assert_eq!(got, payload, "AsyncRead 桥接数据不一致");

        client.close().await.ok();
        server_handle.shutdown().await;
        client_handle.shutdown().await;
    }

    /// connect_established 对无响应对端必须在超时内失败（引擎快速回退的前提）。
    #[tokio::test]
    async fn utp_connect_established_times_out_on_silence() {
        let (client_handle, _client_rx) = UtpManager::bind("127.0.0.1", 0).await.unwrap();
        // 127.0.0.1:1 上无 uTP 监听，SYN 永远得不到回应
        let start = Instant::now();
        let r = client_handle
            .connect_established(SocketAddr::from(([127, 0, 0, 1], 1)), Duration::from_millis(400))
            .await;
        assert!(r.is_err(), "无响应对端应握手失败");
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "失败应在超时附近快速返回，实际 {:?}",
            start.elapsed()
        );
        client_handle.shutdown().await;
    }


}
