//! uTP (BEP 29) 连接状态机。
//!
//! 功能：
//! - SYN/STATE/DATA/FIN/RESET 完整握手与数据传输；
//! - 连接 ID 方向：发起方所有出站包用自选 id C，响应方用 C+1；
//! - LEDBAT 延迟拥塞控制（100ms 目标，2min 基线窗口）；
//! - SACK + 快速重传 + RTO（RFC 6298 EWMA，500ms 下限）。
//!
//! 参考规范：
//! - BEP 29 (uTorrent Transport Protocol)
//! - RFC 6817 (LEDBAT)
//! - RFC 6298 (RTO 计算)
//!
//! 设计：tick 驱动——`process_tick()` 推进定时器/重传/拥塞窗口，
//! `handle_packet()` 处理入站包，`drain_outbox()` 取出已编码的出站包。

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use super::utp_packet::*;

/// 默认数据包负载大小（≤1400 防 IP 分片）。
const DEFAULT_PACKET_SIZE: u32 = 1400;
/// 最小包大小（超时后收缩到此值）。
const MIN_PACKET_SIZE: u32 = 150;
/// 接收窗口（1MB，§7.9：≥1MB）。
const RECV_WINDOW: u32 = 1024 * 1024;
/// LEDBAT 目标单向延迟（100ms = 100_000µs）。
const CC_TARGET_US: u32 = 100_000;
/// 基线延迟窗口（2 分钟）。
const BASE_DELAY_WINDOW: Duration = Duration::from_secs(120);
/// RTO 下限（500ms）。
const RTO_MIN: Duration = Duration::from_millis(500);
/// RTO 上限（60s）。
const RTO_MAX: Duration = Duration::from_secs(60);
/// 最大 cwnd 增长率（每 RTT 增长 1 个包）。
const MAX_CWND_INCREASE_PER_RTT: f64 = 1.0;
/// 待发送数据上限（1MB，防止对端不消费导致内存膨胀）。
const MAX_PENDING_SEND: usize = 1024 * 1024;

/// uTP 连接状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UtpState {
    /// 发起方已发送 SYN，等待响应。
    SynSent,
    /// 响应方收到 SYN，已回送 ACK。
    SynRecv,
    /// 连接已建立。
    Connected,
    /// 已发送 FIN。
    FinSent,
    /// 连接已关闭。
    Closed,
}

/// 出站数据包。
#[derive(Clone)]
struct OutPacket {
    payload: Vec<u8>,
    seq: u16,
    send_time: Instant,
    size: u32,
    retransmits: u32,
}

/// 入站数据包（乱序缓冲）。
struct InPacket {
    payload: Vec<u8>,
    #[allow(dead_code)]
    received_at: Instant,
}

/// uTP 连接。
pub struct UtpConnection {
    remote_addr: SocketAddr,

    // wire identity
    recv_id: u16, // 匹配入站包
    send_id: u16, // 我方出站包的 connection_id

    state: UtpState,
    error: bool,

    // sequence
    seq: u16, // 最后使用的序列号
    ack: u16, // 最后连续接收的序列号
    syn_acked: bool,

    // send side
    pending_send: VecDeque<u8>,
    send_queue: VecDeque<OutPacket>,
    cur_window: u32, // 在途字节数
    max_window: u32, // 拥塞窗口（字节）
    packet_size: u32,
    peer_wnd: u32,
    want_write: bool,
    ack_pending: bool,
    fin_sent: bool,
    fin_pending: bool,
    fin_acked: bool,
    fin_seq: u16,
    syn_attempts: u32,

    // receive side
    recv_out: VecDeque<u8>,
    recv_reorder: BTreeMap<u16, InPacket>,
    recv_buffered: u32,
    eof: bool,
    #[allow(dead_code)]
    eof_pkt: u16,
    want_read: bool,

    // RTT / RTO
    rtt: Duration,
    rtt_var: Duration,
    timeout: Duration,
    last_recv: Option<Instant>,
    last_send: Option<Instant>,
    consecutive_timeouts: u32,

    // LEDBAT delay-based CC
    base_delay: Option<Duration>,
    base_delay_set: Option<Instant>,
    last_delay_sample: Duration,
    last_ack_nr: u16,
    dup_ack_count: u32,
    last_fast_recovery_seq: u16,
    fast_recovery_valid: bool,

    // timestamps echoed from peer
    ts_diff_echo: u32,

    // outbox: 已编码的出站包
    outbox: Vec<Vec<u8>>,
}

impl UtpConnection {
    /// 创建发起方（出站）连接。
    ///
    /// SYN 在 `process_tick()` 中被编码到 outbox。
    /// `syn_timeout` 为可选的 SYN 总预算（None = 默认 4 次 RTO 倍增重试）。
    pub fn new_outbound(remote_addr: SocketAddr, now: Instant) -> Self {
        // 随机连接 ID
        let mut id_bytes = [0u8; 2];
        let _ = getrandom::fill(&mut id_bytes);
        let send_id = u16::from_be_bytes(id_bytes);
        let recv_id = send_id.wrapping_add(1);
        let mut conn = Self {
            remote_addr,
            recv_id,
            send_id,
            state: UtpState::SynSent,
            error: false,
            seq: 1,
            ack: 0,
            syn_acked: false,
            pending_send: VecDeque::new(),
            send_queue: VecDeque::new(),
            cur_window: 0,
            max_window: DEFAULT_PACKET_SIZE * 2,
            packet_size: DEFAULT_PACKET_SIZE,
            peer_wnd: 0x7FFF_FFFF,
            want_write: false,
            ack_pending: false,
            fin_sent: false,
            fin_pending: false,
            fin_acked: false,
            fin_seq: 0,
            syn_attempts: 0,
            recv_out: VecDeque::new(),
            recv_reorder: BTreeMap::new(),
            recv_buffered: 0,
            eof: false,
            eof_pkt: 0,
            want_read: false,
            rtt: Duration::ZERO,
            rtt_var: Duration::ZERO,
            timeout: Duration::from_secs(1),
            last_recv: None,
            last_send: Some(now),
            consecutive_timeouts: 0,
            base_delay: None,
            base_delay_set: None,
            last_delay_sample: Duration::ZERO,
            last_ack_nr: 0,
            dup_ack_count: 0,
            last_fast_recovery_seq: 0,
            fast_recovery_valid: false,
            ts_diff_echo: 0,
            outbox: Vec::new(),
        };
        // 在构造时直接编码 SYN 到 outbox
        conn.queue_control(packet_type::ST_SYN, now, 0);
        conn
    }

    /// 创建响应方（入站）连接。
    ///
    /// `peer_recv_id` 为 SYN 包中的 connection_id，
    /// `ack_nr` 为 SYN 的序列号。
    pub fn new_inbound(
        remote_addr: SocketAddr,
        peer_recv_id: u16,
        ack_nr: u16,
        now: Instant,
    ) -> Self {
        // 响应方：入站包用对端 SYN 中的 id；出站包用 id+1
        let recv_id = peer_recv_id;
        let send_id = peer_recv_id.wrapping_add(1);
        let mut conn = Self {
            remote_addr,
            recv_id,
            send_id,
            state: UtpState::SynRecv,
            error: false,
            seq: 0, // 会在 queue_control 中递增
            ack: ack_nr,
            syn_acked: false,
            pending_send: VecDeque::new(),
            send_queue: VecDeque::new(),
            cur_window: 0,
            max_window: DEFAULT_PACKET_SIZE * 2,
            packet_size: DEFAULT_PACKET_SIZE,
            peer_wnd: 0x7FFF_FFFF,
            want_write: false,
            ack_pending: false,
            fin_sent: false,
            fin_pending: false,
            fin_acked: false,
            fin_seq: 0,
            syn_attempts: 0,
            recv_out: VecDeque::new(),
            recv_reorder: BTreeMap::new(),
            recv_buffered: 0,
            eof: false,
            eof_pkt: 0,
            want_read: false,
            rtt: Duration::ZERO,
            rtt_var: Duration::ZERO,
            timeout: Duration::from_secs(1),
            last_recv: Some(now),
            last_send: Some(now),
            consecutive_timeouts: 0,
            base_delay: None,
            base_delay_set: None,
            last_delay_sample: Duration::ZERO,
            last_ack_nr: 0,
            dup_ack_count: 0,
            last_fast_recovery_seq: 0,
            fast_recovery_valid: false,
            ts_diff_echo: 0,
            outbox: Vec::new(),
        };
        // 回送纯 ACK (ST_STATE)
        conn.queue_control(packet_type::ST_STATE, now, 0);
        conn
    }

    // ------------------------------------------------------------------
    // Stream API
    // ------------------------------------------------------------------

    /// 缓冲用户数据等待发送。返回接受的字节数。
    pub fn write(&mut self, data: &[u8]) -> usize {
        self.want_write = false;
        if self.state == UtpState::Closed || self.error {
            return 0;
        }
        let mut accepted = 0;
        while accepted < data.len() && self.pending_send.len() < MAX_PENDING_SEND {
            self.pending_send.push_back(data[accepted]);
            accepted += 1;
        }
        if accepted < data.len() {
            self.want_write = true;
        }
        accepted
    }

    /// 读取已按序到达的数据。
    pub fn read(&mut self, buf: &mut [u8]) -> usize {
        self.want_read = false;
        let mut n = 0;
        while n < buf.len() {
            if let Some(&b) = self.recv_out.front() {
                buf[n] = b;
                self.recv_out.pop_front();
                n += 1;
            } else {
                break;
            }
        }
        if n == 0
            && self.recv_out.is_empty()
            && !self.eof_received()
            && !self.is_closed()
            && !self.has_error()
        {
            self.want_read = true;
        }
        n
    }

    pub fn want_read(&self) -> bool {
        self.want_read
    }
    pub fn want_write(&self) -> bool {
        self.want_write
    }
    pub fn is_connected(&self) -> bool {
        matches!(self.state, UtpState::Connected | UtpState::FinSent)
    }
    pub fn eof_received(&self) -> bool {
        self.eof && self.recv_out.is_empty() && self.recv_reorder.is_empty()
    }
    pub fn is_closed(&self) -> bool {
        self.state == UtpState::Closed
    }
    pub fn has_error(&self) -> bool {
        self.error
    }
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }
    pub fn recv_id(&self) -> u16 {
        self.recv_id
    }

    // ------------------------------------------------------------------
    // Packet reception
    // ------------------------------------------------------------------

    /// 处理一个入站 UDP 数据报。
    pub fn handle_packet(&mut self, data: &[u8], now: Instant) {
        let (hdr, exts, payload_off) = match parse_packet(data) {
            Some(v) => v,
            None => return,
        };

        self.last_recv = Some(now);
        self.consecutive_timeouts = 0;

        // 时间戳记账
        let ts_diff = delay_sample_from(hdr.timestamp);
        self.ts_diff_echo = ts_diff;
        self.peer_wnd = hdr.wnd_size;

        if hdr.type_ != packet_type::ST_SYN {
            self.update_delay(ts_diff, now);
        }

        // 解析 SACK
        let mut sack_bits = 0u32;
        for (ext_type, ext_payload) in &exts {
            if *ext_type == ext_type::EXT_SACK {
                sack_bits = decode_sack_bits(ext_payload);
            }
        }

        match hdr.type_ {
            packet_type::ST_SYN => {
                // 重传 SYN：回送 ACK
                if self.state == UtpState::SynRecv || self.state == UtpState::Connected {
                    self.queue_control(packet_type::ST_STATE, now, 0);
                }
            }
            packet_type::ST_RESET => {
                self.error = true;
                self.state = UtpState::Closed;
                return;
            }
            packet_type::ST_STATE => {
                if self.state == UtpState::SynSent {
                    self.ack = hdr.seq_nr;
                    self.syn_acked = true;
                    self.state = UtpState::Connected;
                }
                self.on_ack(hdr.ack_nr, sack_bits, now);
            }
            packet_type::ST_DATA | packet_type::ST_FIN => {
                if self.state == UtpState::SynRecv {
                    self.state = UtpState::Connected;
                } else if self.state == UtpState::SynSent {
                    self.ack = hdr.seq_nr;
                    self.syn_acked = true;
                    self.state = UtpState::Connected;
                }
                if hdr.type_ == packet_type::ST_DATA {
                    let payload = &data[payload_off..];
                    self.on_data(&hdr, payload, now);
                } else {
                    // ST_FIN 占用一个序列号
                    if hdr.seq_nr == self.ack.wrapping_add(1) && !self.eof {
                        self.ack = hdr.seq_nr;
                        self.eof = true;
                        self.eof_pkt = hdr.seq_nr;
                    }
                    self.ack_pending = true;
                }
                self.on_ack(hdr.ack_nr, sack_bits, now);
            }
            _ => {}
        }

        if self.ack_pending {
            self.send_ack(now);
        }
    }

    fn on_data(&mut self, hdr: &PacketHeader, payload: &[u8], now: Instant) {
        let s = hdr.seq_nr;
        if payload.is_empty() {
            return;
        }
        if seq_leq(s, self.ack) {
            return; // 重复/旧包
        }
        if s == self.ack.wrapping_add(1) {
            // 连续：交付，然后排空乱序缓冲
            self.recv_out.extend(payload);
            self.ack = s;
            while let Some((&next_seq, _)) = self.recv_reorder.first_key_value() {
                if next_seq != self.ack.wrapping_add(1) {
                    break;
                }
                let p = self.recv_reorder.remove(&next_seq).unwrap();
                self.recv_buffered -= p.payload.len() as u32;
                self.recv_out.extend(p.payload);
                self.ack = next_seq;
            }
            self.ack_pending = true;
        } else {
            // 乱序：缓冲，受窗口约束
            if self.recv_buffered + payload.len() as u32 <= RECV_WINDOW {
                // 同 seq 重复乱序包：先扣旧
                if let Some(dup) = self.recv_reorder.remove(&s) {
                    self.recv_buffered -= dup.payload.len() as u32;
                }
                self.recv_reorder.insert(
                    s,
                    InPacket {
                        payload: payload.to_vec(),
                        received_at: now,
                    },
                );
                self.recv_buffered += payload.len() as u32;
            }
            self.ack_pending = true;
        }
        self.want_read = true;
    }

    fn on_ack(&mut self, ack_nr: u16, sack_bits: u32, now: Instant) {
        if self.state == UtpState::SynSent && !self.syn_acked {
            return;
        }

        // 重复 ACK 检测
        if ack_nr == self.last_ack_nr {
            self.dup_ack_count += 1;
        } else {
            self.last_ack_nr = ack_nr;
            self.dup_ack_count = 0;
        }

        // 移除已确认的包，更新 RTT + 窗口
        let mut rtt_sample: Option<Duration> = None;
        let mut freed = 0u32;
        while let Some(front) = self.send_queue.front() {
            if !seq_leq(front.seq, ack_nr) {
                break;
            }
            if rtt_sample.is_none() && front.retransmits == 0 {
                rtt_sample = Some(now.duration_since(front.send_time));
            }
            freed += front.size;
            if front.seq == self.fin_seq && self.fin_sent {
                self.fin_acked = true;
            }
            self.send_queue.pop_front();
        }
        if freed > 0 {
            self.cur_window = self.cur_window.saturating_sub(freed);
            if let Some(rtt) = rtt_sample {
                self.update_rtt(rtt);
            }
            self.update_window(now, freed);
            self.want_write = true;
        }

        // FIN 的累计确认
        if self.fin_sent && !self.fin_acked && seq_leq(self.fin_seq, ack_nr) {
            self.fin_acked = true;
        }

        // SACK 快速重传
        if let Some(front) = self.send_queue.front() {
            let oldest = front.seq;
            let oldest_unacked = seq_after(oldest, ack_nr);
            let mut sacks_after = 0u32;
            for bit in 0..32u32 {
                if sack_bits & (1 << bit) != 0 {
                    let s = ack_nr.wrapping_add(2 + bit as u16);
                    if seq_after(s, oldest) {
                        sacks_after += 1;
                    }
                }
            }
            if oldest_unacked
                && sacks_after >= 3
                && !(self.fast_recovery_valid && self.last_fast_recovery_seq == oldest)
            {
                let pkt_to_retransmit = front.clone();
                self.retransmit_packet_clone(&pkt_to_retransmit, now);
                self.max_window = (self.max_window / 2).max(self.packet_size);
                self.last_fast_recovery_seq = oldest;
                self.fast_recovery_valid = true;
            }
        }

        // 3 次重复 ACK → 快速重传 ack_nr+1
        if self.dup_ack_count >= 3 {
            let target = ack_nr.wrapping_add(1);
            // 查找目标包并克隆其数据（避免可变借用冲突）
            let found = self.send_queue.iter().find(|p| p.seq == target).cloned();
            if let Some(pkt_to_retransmit) = found {
                if !(self.fast_recovery_valid && self.last_fast_recovery_seq == target) {
                    self.retransmit_packet_clone(&pkt_to_retransmit, now);
                    // 标记原包重传计数
                    if let Some(p) = self.send_queue.iter_mut().find(|p| p.seq == target) {
                        p.retransmits += 1;
                        p.send_time = now;
                    }
                    self.max_window = (self.max_window / 2).max(self.packet_size);
                    self.last_fast_recovery_seq = target;
                    self.fast_recovery_valid = true;
                }
            }
            self.dup_ack_count = 0;
        }

        if self.fin_sent && self.fin_acked {
            self.finish_close_if_idle();
        }
    }

    // ------------------------------------------------------------------
    // Timers / CC
    // ------------------------------------------------------------------

    /// 推进定时器/重传/拥塞窗口，并刷新出站包到 outbox。
    pub fn process_tick(&mut self, now: Instant) {
        if self.state == UtpState::Closed {
            return;
        }

        // 刷新 FIN
        if self.fin_pending {
            self.fin_pending = false;
            self.fin_sent = true;
            self.fin_seq = self.next_seq();
            self.seq = self.fin_seq;
            if self.state == UtpState::SynSent {
                self.state = UtpState::Closed;
                return;
            } else {
                self.state = UtpState::FinSent;
            }
            self.queue_control(packet_type::ST_FIN, now, 0);
            if self.state == UtpState::Closed {
                return;
            }
        }

        // RTO / 握手超时
        let fin_in_flight = self.fin_sent && !self.fin_acked && self.state == UtpState::FinSent;
        let need_rto = self.state == UtpState::SynSent
            || (self.last_recv.is_some() && (!self.send_queue.is_empty() || fin_in_flight));
        if need_rto {
            let since_last = if self.state == UtpState::SynSent {
                now.duration_since(self.last_send.unwrap_or(now))
            } else {
                now.duration_since(self.last_recv.unwrap_or(now))
            };
            if since_last > self.timeout {
                self.handle_timeout(now);
                return;
            }
        }

        // 刷新待发送数据（受 CC + 流控窗口约束）
        if self.state == UtpState::Connected || self.state == UtpState::FinSent {
            while !self.pending_send.is_empty() {
                if !self.can_send_data() {
                    self.want_write = true;
                    break;
                }
                let chunk = self.pending_send.len().min(self.packet_size as usize);
                let payload: Vec<u8> = self.pending_send.drain(..chunk).collect();
                let op = OutPacket {
                    payload: payload.clone(),
                    seq: self.next_seq(),
                    send_time: now,
                    size: payload.len() as u32,
                    retransmits: 0,
                };
                self.seq = op.seq;
                self.queue_data_packet(op, now);
            }
        }

        // 刷新待发 ACK
        if self.ack_pending {
            self.send_ack(now);
        }

        self.want_read = self.want_read || !self.recv_out.is_empty() || self.eof_received();
    }

    fn handle_timeout(&mut self, now: Instant) {
        if self.state == UtpState::SynSent {
            self.syn_attempts += 1;
            self.timeout = (self.timeout * 2).min(RTO_MAX);
            if self.syn_attempts >= 4 {
                self.error = true;
                self.state = UtpState::Closed;
                return;
            }
            self.queue_control(packet_type::ST_SYN, now, 0);
            self.last_send = Some(now);
            return;
        }

        self.consecutive_timeouts += 1;
        self.timeout = (self.timeout * 2).min(RTO_MAX);
        if self.consecutive_timeouts >= 2 {
            self.error = true;
            self.state = UtpState::Closed;
            return;
        }
        // 超时：收缩包大小到最小，cwnd 收缩到一个包
        self.packet_size = MIN_PACKET_SIZE;
        self.max_window = self.packet_size;
        self.last_recv = Some(now);
        self.retransmit_unacked(now);
        // FIN 不在 send_queue 中，需单独重传
        if self.fin_sent && !self.fin_acked && self.state == UtpState::FinSent {
            self.queue_control(packet_type::ST_FIN, now, 0);
        }
    }

    #[allow(dead_code)]
    fn retransmit_packet(&mut self, index: usize, now: Instant) {
        let p = &mut self.send_queue[index];
        p.send_time = now;
        p.retransmits += 1;
        let payload = p.payload.clone();
        let seq = p.seq;
        let mut pkt = vec![0u8; HEADER_LEN + payload.len()];
        let h = PacketHeader {
            type_: packet_type::ST_DATA,
            version: PROTOCOL_VERSION,
            extension: ext_type::EXT_NONE,
            connection_id: self.send_id,
            timestamp: now_micros(),
            timestamp_diff: self.ts_diff_echo,
            wnd_size: self.advertised_wnd(),
            seq_nr: seq,
            ack_nr: self.ack,
        };
        encode_header(&mut pkt, &h);
        pkt[HEADER_LEN..].copy_from_slice(&payload);
        self.outbox.push(pkt);
        self.last_send = Some(now);
    }

    /// 重传包的副本（用于无法可变借用的场景，如从 SACK 路径）。
    fn retransmit_packet_clone(&mut self, p: &OutPacket, now: Instant) {
        let payload = p.payload.clone();
        let seq = p.seq;
        let mut pkt = vec![0u8; HEADER_LEN + payload.len()];
        let h = PacketHeader {
            type_: packet_type::ST_DATA,
            version: PROTOCOL_VERSION,
            extension: ext_type::EXT_NONE,
            connection_id: self.send_id,
            timestamp: now_micros(),
            timestamp_diff: self.ts_diff_echo,
            wnd_size: self.advertised_wnd(),
            seq_nr: seq,
            ack_nr: self.ack,
        };
        encode_header(&mut pkt, &h);
        pkt[HEADER_LEN..].copy_from_slice(&payload);
        self.outbox.push(pkt);
        self.last_send = Some(now);
    }

    fn retransmit_unacked(&mut self, now: Instant) {
        // 收集需要重传的包的索引和数据，避免可变借用问题
        let entries: Vec<(usize, u16, Vec<u8>)> = self
            .send_queue
            .iter()
            .enumerate()
            .map(|(i, p)| (i, p.seq, p.payload.clone()))
            .collect();
        for (idx, seq, payload) in entries {
            if let Some(p) = self.send_queue.get_mut(idx) {
                p.send_time = now;
                p.retransmits += 1;
            }
            let mut pkt = vec![0u8; HEADER_LEN + payload.len()];
            let h = PacketHeader {
                type_: packet_type::ST_DATA,
                version: PROTOCOL_VERSION,
                extension: ext_type::EXT_NONE,
                connection_id: self.send_id,
                timestamp: now_micros(),
                timestamp_diff: self.ts_diff_echo,
                wnd_size: self.advertised_wnd(),
                seq_nr: seq,
                ack_nr: self.ack,
            };
            encode_header(&mut pkt, &h);
            pkt[HEADER_LEN..].copy_from_slice(&payload);
            self.outbox.push(pkt);
        }
    }

    fn update_rtt(&mut self, packet_rtt: Duration) {
        if packet_rtt.is_zero() || packet_rtt > RTO_MAX {
            return;
        }
        if self.rtt.is_zero() {
            self.rtt = packet_rtt;
            self.rtt_var = packet_rtt / 2;
        } else {
            // RFC 6298 EWMA：RTTVAR ← RTTVAR + (|SRTT-R'| - RTTVAR)/4。
            // 样本比当前方差更"准"时括号内为负，必须用有符号运算，
            // 否则 Duration 减法下溢 panic（管理器任务崩溃、连接全停）。
            let delta = self.rtt.abs_diff(packet_rtt);
            let var_diff = delta.as_nanos() as i64 - self.rtt_var.as_nanos() as i64;
            let new_var = self.rtt_var.as_nanos() as i64 + var_diff / 4;
            self.rtt_var = Duration::from_nanos(new_var.max(0) as u64);
            // 有符号运算：样本可能小于均值，RTT 应能双向调整
            let diff = packet_rtt.as_nanos() as i64 - self.rtt.as_nanos() as i64;
            let new_rtt_nanos = self.rtt.as_nanos() as i64 + diff / 8;
            self.rtt = Duration::from_nanos(new_rtt_nanos.max(0) as u64);
        }
        self.timeout = (self.rtt + self.rtt_var * 4).max(RTO_MIN).min(RTO_MAX);
    }

    fn update_delay(&mut self, sample_us: u32, now: Instant) {
        let sample = Duration::from_micros(sample_us as u64);
        self.last_delay_sample = sample;
        let need_reset = self.base_delay.is_none()
            || sample < self.base_delay.unwrap()
            || self
                .base_delay_set
                .is_some_and(|set| now.duration_since(set) > BASE_DELAY_WINDOW);
        if need_reset {
            self.base_delay = Some(sample);
            self.base_delay_set = Some(now);
        }
    }

    fn update_window(&mut self, _now: Instant, newly_acked: u32) {
        // LEDBAT: off_target = target - our_delay
        let our_delay = self
            .base_delay
            .filter(|&b| self.last_delay_sample > b)
            .map(|b| self.last_delay_sample - b)
            .unwrap_or(Duration::ZERO);
        let off_target = CC_TARGET_US.saturating_sub(our_delay.as_micros() as u32);
        let delay_factor = off_target as f64 / CC_TARGET_US as f64;
        // 按新确认字节数等比增长（libutp 语义）
        let delta = (MAX_CWND_INCREASE_PER_RTT * delay_factor * newly_acked as f64) as i64;
        // 无基线时启动加速（slow start）
        let delta = if self.base_delay.is_none() {
            delta + self.packet_size as i64
        } else {
            delta
        };
        let new_window = self.max_window as i64 + delta;
        self.max_window = new_window.max(self.packet_size as i64) as u32;
    }

    fn can_send_data(&self) -> bool {
        let cap = self.max_window.min(self.peer_wnd);
        self.cur_window + self.packet_size <= cap
    }

    fn advertised_wnd(&self) -> u32 {
        let used = self.recv_buffered + self.recv_out.len() as u32;
        RECV_WINDOW.saturating_sub(used)
    }

    fn queue_data_packet(&mut self, op: OutPacket, now: Instant) {
        self.cur_window += op.size;
        let mut pkt = vec![0u8; HEADER_LEN + op.payload.len()];
        let h = PacketHeader {
            type_: packet_type::ST_DATA,
            version: PROTOCOL_VERSION,
            extension: ext_type::EXT_NONE,
            connection_id: self.send_id,
            timestamp: now_micros(),
            timestamp_diff: self.ts_diff_echo,
            wnd_size: self.advertised_wnd(),
            seq_nr: op.seq,
            ack_nr: self.ack,
        };
        encode_header(&mut pkt, &h);
        pkt[HEADER_LEN..].copy_from_slice(&op.payload);
        self.outbox.push(pkt);
        self.last_send = Some(now);
        self.send_queue.push_back(op);
    }

    fn send_ack(&mut self, now: Instant) {
        self.queue_control(packet_type::ST_STATE, now, 0);
        self.ack_pending = false;
    }

    fn queue_control(&mut self, type_: u8, now: Instant, seq_for_ack: u16) {
        let seq_field = if type_ == packet_type::ST_FIN {
            self.fin_seq
        } else {
            self.seq
        };
        let have_gaps = !self.recv_reorder.is_empty();
        let sack_size = if have_gaps { 6 } else { 0 };
        let total = HEADER_LEN + sack_size;
        let mut pkt = vec![0u8; total];
        let h = PacketHeader {
            type_,
            version: PROTOCOL_VERSION,
            extension: if have_gaps {
                ext_type::EXT_SACK
            } else {
                ext_type::EXT_NONE
            },
            connection_id: self.send_id,
            timestamp: now_micros(),
            timestamp_diff: self.ts_diff_echo,
            wnd_size: self.advertised_wnd(),
            seq_nr: seq_field,
            ack_nr: if seq_for_ack == 0 {
                self.ack
            } else {
                seq_for_ack
            },
        };
        encode_header(&mut pkt, &h);
        if have_gaps {
            let mut bits = 0u32;
            for &s in self.recv_reorder.keys() {
                let off = s.wrapping_sub(self.ack.wrapping_add(2)) as i32;
                if (0..32).contains(&off) {
                    bits |= 1u32 << off;
                }
            }
            // SACK 扩展：[next_type=0][len=4][4-byte bitmap LE]
            pkt[HEADER_LEN] = ext_type::EXT_NONE;
            pkt[HEADER_LEN + 1] = 4;
            for i in 0..4 {
                pkt[HEADER_LEN + 2 + i] = ((bits >> (8 * i)) & 0xFF) as u8;
            }
        }
        self.outbox.push(pkt);
        self.last_send = Some(now);
    }

    fn finish_close_if_idle(&mut self) {
        if self.fin_sent && self.fin_acked && self.eof_received() {
            self.state = UtpState::Closed;
        }
    }

    /// 优雅关闭：发送 FIN。
    pub fn close(&mut self) {
        if self.fin_pending || self.fin_sent || self.state == UtpState::Closed {
            return;
        }
        if self.state == UtpState::SynSent {
            self.state = UtpState::Closed;
            return;
        }
        self.fin_pending = true;
    }

    /// 取出已编码的出站包（供 UDP socket 发送）。
    pub fn drain_outbox(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.outbox)
    }

    fn next_seq(&self) -> u16 {
        self.seq.wrapping_add(1)
    }
}

/// 获取当前微秒时间戳（用于 uTP 包头部）。
fn now_micros() -> u32 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| (d.as_micros() as u64 & 0xFFFF_FFFF) as u32)
        .unwrap_or(0)
}

/// 计算相对于 uTP 时间戳的差值（单向延迟样本）。
///
/// BEP 29 timestamp 是微秒级时钟（32 位，会回绕）。
/// 差值 = 本地微秒时钟 - 对端包内时间戳。
/// 两个值都在同一时钟域（本地时钟），差值用于单向延迟估计（LEDBAT）。
fn delay_sample_from(timestamp: u32) -> u32 {
    now_micros().wrapping_sub(timestamp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    #[test]
    fn outbound_init_sends_syn() {
        let now = Instant::now();
        let mut conn = UtpConnection::new_outbound(addr(6881), now);
        conn.process_tick(now);
        let outbox = conn.drain_outbox();
        assert_eq!(outbox.len(), 1);
        let (hdr, _, _) = parse_packet(&outbox[0]).unwrap();
        assert_eq!(hdr.type_, packet_type::ST_SYN);
        assert_eq!(hdr.seq_nr, 1);
        assert_eq!(hdr.timestamp_diff, 0); // SYN 时无延迟样本
    }

    #[test]
    fn handshake_initiator() {
        let now = Instant::now();
        let mut init = UtpConnection::new_outbound(addr(6881), now);
        init.process_tick(now);
        let syn = init.drain_outbox().pop().unwrap();

        let (syn_hdr, _, _) = parse_packet(&syn).unwrap();

        // 响应方收到 SYN
        let mut resp =
            UtpConnection::new_inbound(addr(6881), syn_hdr.connection_id, syn_hdr.seq_nr, now);
        let resp_packets = resp.drain_outbox();
        assert_eq!(resp_packets.len(), 1);
        let (ack_hdr, _, _) = parse_packet(&resp_packets[0]).unwrap();
        assert_eq!(ack_hdr.type_, packet_type::ST_STATE);
        assert_eq!(ack_hdr.ack_nr, syn_hdr.seq_nr);

        // 发起方收到 SYN-ACK
        init.handle_packet(&resp_packets[0], now);
        assert!(init.is_connected());
    }

    #[test]
    fn data_transfer_initiator_to_responder() {
        let now = Instant::now();
        let mut init = UtpConnection::new_outbound(addr(6881), now);
        init.process_tick(now);
        let syn = init.drain_outbox().pop().unwrap();
        let (syn_hdr, _, _) = parse_packet(&syn).unwrap();

        let mut resp =
            UtpConnection::new_inbound(addr(6881), syn_hdr.connection_id, syn_hdr.seq_nr, now);

        // 发起方收到 SYN-ACK → CONNECTED
        let resp_ack = resp.drain_outbox().pop().unwrap();
        init.handle_packet(&resp_ack, now);
        assert!(init.is_connected());

        // 发起方发送数据
        let data = b"Hello uTP!";
        init.write(data);
        init.process_tick(now);
        let data_pkt = init.drain_outbox().pop().unwrap();
        let (dhdr, _, payload_off) = parse_packet(&data_pkt).unwrap();
        assert_eq!(dhdr.type_, packet_type::ST_DATA);
        assert_eq!(&data_pkt[payload_off..], data);

        // 响应方收到数据
        resp.handle_packet(&data_pkt, now);
        let mut buf = [0u8; 256];
        let n = resp.read(&mut buf);
        assert_eq!(&buf[..n], data);

        // 响应方回送 ACK
        resp.process_tick(now);
        let ack_pkt = resp.drain_outbox().pop().unwrap();
        init.handle_packet(&ack_pkt, now);
        // 发起方的 send_queue 应该被清空
        assert!(init.send_queue.is_empty());
    }

    #[test]
    fn fin_graceful_close() {
        let now = Instant::now();
        let mut init = UtpConnection::new_outbound(addr(6881), now);
        init.process_tick(now);
        let syn = init.drain_outbox().pop().unwrap();
        let (syn_hdr, _, _) = parse_packet(&syn).unwrap();

        let mut resp =
            UtpConnection::new_inbound(addr(6881), syn_hdr.connection_id, syn_hdr.seq_nr, now);

        // 完成握手
        let resp_ack = resp.drain_outbox().pop().unwrap();
        init.handle_packet(&resp_ack, now);
        assert!(init.is_connected());

        // 发起方发送 FIN
        init.close();
        init.process_tick(now);
        let fin_pkt = init.drain_outbox().pop().unwrap();
        let (fin_hdr, _, _) = parse_packet(&fin_pkt).unwrap();
        assert_eq!(fin_hdr.type_, packet_type::ST_FIN);

        // 响应方收到 FIN
        resp.handle_packet(&fin_pkt, now);
        // 响应方回送 ACK
        resp.process_tick(now);
        let ack = resp.drain_outbox().pop().unwrap();
        init.handle_packet(&ack, now);
        assert!(init.fin_acked);
    }

    #[test]
    fn sack_bits_on_reorder() {
        let now = Instant::now();
        let mut init = UtpConnection::new_outbound(addr(6881), now);
        init.process_tick(now);
        let syn = init.drain_outbox().pop().unwrap();
        let (syn_hdr, _, _) = parse_packet(&syn).unwrap();

        let mut resp =
            UtpConnection::new_inbound(addr(6881), syn_hdr.connection_id, syn_hdr.seq_nr, now);

        // 完成握手
        let resp_ack = resp.drain_outbox().pop().unwrap();
        init.handle_packet(&resp_ack, now);
        assert!(init.is_connected());

        // 发送足够数据使其分成 2 个包（每包 ≤1400 字节）
        let data = vec![0xABu8; 2800];
        init.write(&data);
        init.process_tick(now);
        let packets = init.drain_outbox();
        assert_eq!(packets.len(), 2, "应该分成 2 个数据包");

        // 响应方只收到包 2（跳过包 1）
        // 这会在乱序缓冲中产生一个条目，触发 SACK
        resp.handle_packet(&packets[1], now);

        // 响应方应该有乱序缓冲，ACK 中带 SACK
        resp.process_tick(now);
        let ack = resp.drain_outbox().pop().unwrap();
        let (ack_hdr, exts, _) = parse_packet(&ack).unwrap();
        assert_eq!(ack_hdr.type_, packet_type::ST_STATE);
        // 包 2 乱序到达，应该在 SACK 中标记
        assert!(!exts.is_empty(), "应该有 SACK 扩展");
        let sack_bits = exts
            .iter()
            .find(|(t, _)| *t == ext_type::EXT_SACK)
            .map(|(_, p)| decode_sack_bits(p))
            .unwrap_or(0);
        assert_ne!(sack_bits, 0, "SACK bitmap 不应为 0");
    }

    #[test]
    fn connection_id_direction() {
        // BEP 29: 发起方所有出站包用 id C，响应方用 C+1
        let now = Instant::now();
        let init = UtpConnection::new_outbound(addr(6881), now);
        // 发起方 send_id = C，recv_id = C+1
        assert_eq!(init.recv_id, init.send_id.wrapping_add(1));

        // 模拟对端：响应方收到 SYN（携带 id=C）
        // 响应方 recv_id = C，send_id = C+1
        let resp = UtpConnection::new_inbound(addr(6881), init.send_id, 1, now);
        assert_eq!(resp.recv_id, init.send_id); // 响应方收 C
        assert_eq!(resp.send_id, init.send_id.wrapping_add(1)); // 响应方发 C+1
    }

    #[test]
    fn large_data_transfer() {
        let now = Instant::now();
        let mut init = UtpConnection::new_outbound(addr(6881), now);
        init.process_tick(now);
        let syn = init.drain_outbox().pop().unwrap();
        let (syn_hdr, _, _) = parse_packet(&syn).unwrap();

        let mut resp =
            UtpConnection::new_inbound(addr(6881), syn_hdr.connection_id, syn_hdr.seq_nr, now);

        // 握手
        let resp_ack = resp.drain_outbox().pop().unwrap();
        init.handle_packet(&resp_ack, now);
        assert!(init.is_connected());

        // 发送 5000 字节数据（跨多个包）
        let data: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();
        init.write(&data);
        init.process_tick(now);

        // 交换包直到数据全部到达
        let mut received = Vec::new();
        let mut round = 0;
        while received.len() < data.len() && round < 20 {
            let init_outbox = init.drain_outbox();
            for pkt in &init_outbox {
                resp.handle_packet(pkt, now);
            }
            let resp_outbox = resp.drain_outbox();
            for pkt in &resp_outbox {
                init.handle_packet(pkt, now);
            }
            let mut buf = [0u8; 8192];
            let n = resp.read(&mut buf);
            received.extend_from_slice(&buf[..n]);
            init.process_tick(now);
            resp.process_tick(now);
            round += 1;
        }
        assert_eq!(received, data);
    }
}
