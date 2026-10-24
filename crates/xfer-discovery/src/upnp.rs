//! UPnP IGD / NAT-PMP 端口映射。
//!
//! 功能：
//! - UPnP IGD（SSDP 发现 → SOAP 控制）；
//! - NAT-PMP（RFC 6886，直接查询 5351 端口）。
//!
//! 关键正确性（§7.5）：
//! - 必须同时映射 TCP 和 UDP（UDP 是入站 uTP 的前提）。
//! - uTP 与 TCP 监听同端口（§7.6）。
//!
//! 当前实现：NAT-PMP（轻量、可靠）+ UPnP SSDP 发现 + SOAP AddPortMapping。
//! 两者并行尝试，任一成功即可。

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;
use xfer_types::ENGINE_NAME;

/// 端口映射协议。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortMappingProtocol {
    Tcp,
    Udp,
}

impl PortMappingProtocol {
    /// NAT-PMP opcode（RFC 6886 §3.3）：0 = 查询外部地址，
    /// 1 = 映射 UDP，2 = 映射 TCP。此前 TCP 写成 6（既非 1 也非 2），
    /// 网关一律回 unsupported/ignore——NAT-PMP 的 TCP 映射从未成功过，
    /// 全靠 UPnP 回退兜底（只有 UPnP 不可用时才暴露为「TCP 端口映射失败」）。
    fn nat_pmp_code(&self) -> u16 {
        match self {
            PortMappingProtocol::Tcp => 2, // NAT-PMP op 2 = map TCP
            PortMappingProtocol::Udp => 1, // NAT-PMP op 1 = map UDP
        }
    }

    fn upnp_protocol_str(&self) -> &'static str {
        match self {
            PortMappingProtocol::Tcp => "TCP",
            PortMappingProtocol::Udp => "UDP",
        }
    }
}

impl std::fmt::Display for PortMappingProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.upnp_protocol_str())
    }
}

/// NAT 端口映射结果。
#[derive(Debug, Clone)]
pub struct NatPortMapping {
    pub external_ip: IpAddr,
    pub external_port: u16,
    pub internal_port: u16,
    pub protocol: PortMappingProtocol,
    pub lifetime: u32,
    /// 来源（"nat-pmp" 或 "upnp"）。
    pub source: String,
}

/// UPnP/NAT-PMP 客户端。
pub struct UpnpClient {
    /// 网关地址（None = 自动发现）。
    gateway: Option<IpAddr>,
}

impl UpnpClient {
    /// 创建客户端（自动发现网关）。
    pub fn new() -> Self {
        Self { gateway: None }
    }

    /// 创建客户端（指定网关地址）。
    pub fn with_gateway(gateway: IpAddr) -> Self {
        Self {
            gateway: Some(gateway),
        }
    }

    /// 尝试同时映射 TCP 和 UDP 端口（§7.5 正确性要求）。
    ///
    /// 每个协议独立尝试 NAT-PMP，失败时回退到 UPnP。
    /// 这确保即使 NAT-PMP 只支持其中一个协议，另一个仍可通过 UPnP 映射，
    /// 满足「必须同时映射 TCP 和 UDP」的正确性要求。
    pub async fn map_port(
        &self,
        internal_port: u16,
        external_port: u16,
        lifetime: u32,
    ) -> Vec<Result<NatPortMapping, String>> {
        let mut results = Vec::new();
        // 网关只解析一次：`resolve_gateway` 会盲探几个常见网关地址
        // （每个 600ms 超时），此前 TCP/UDP 各探一遍。解析不出来就说明
        // 本网没有 NAT-PMP 网关，直接跳过它走 UPnP（SSDP 不依赖网关地址）。
        let gateway = match self.gateway {
            Some(gw) => Some(gw),
            None => match resolve_gateway(None).await {
                Ok(gw) => Some(gw),
                Err(e) => {
                    tracing::debug!(error = %e, "未发现 NAT-PMP 网关，跳过 NAT-PMP");
                    None
                }
            },
        };
        // SSDP 发现也只做一次，TCP/UDP 两个协议复用（此前每个协议各扫一遍，
        // 白白多花一次 3 秒超时）。
        let mut igd: Option<Igd> = None;

        for proto in [PortMappingProtocol::Tcp, PortMappingProtocol::Udp] {
            // 先尝试 NAT-PMP（轻量、一次往返；不少路由器/机场基站只支持它）
            let nat_result = match gateway {
                Some(gw) => {
                    nat_pmp_map_port(Some(gw), internal_port, external_port, proto, lifetime)
                        .await
                        .map_err(|e| e.to_string())
                }
                None => Err("无 NAT-PMP 网关".to_string()),
            };
            match nat_result {
                Ok(mapping) => {
                    tracing::info!(
                        proto = ?proto,
                        external_port = mapping.external_port,
                        "NAT-PMP 端口映射成功"
                    );
                    results.push(Ok(mapping));
                }
                Err(e) => {
                    tracing::debug!(proto = ?proto, error = %e, "NAT-PMP 映射失败，尝试 UPnP");
                    // 只有 NAT-PMP 真的失败后才做 SSDP：它要 3 秒超时，
                    // 不能挡在 NAT-PMP 前面。
                    if igd.is_none() {
                        match upnp_ssdp_discover().await {
                            Ok(found) => igd = Some(found),
                            Err(err) => {
                                tracing::debug!(error = %err, "SSDP 未发现 IGD");
                            }
                        }
                    }
                    // NAT-PMP 失败，回退到 UPnP 确保该协议仍可被映射
                    match upnp_map_port(igd.as_ref(), internal_port, external_port, proto, lifetime)
                        .await
                    {
                        Ok(mapping) => {
                            tracing::info!(
                                proto = ?proto,
                                external_port = mapping.external_port,
                                "UPnP 端口映射成功"
                            );
                            results.push(Ok(mapping));
                        }
                        Err(e2) => {
                            tracing::warn!(proto = ?proto, error = %e2, "UPnP 映射也失败");
                            results.push(Err(format!("NAT-PMP: {e}; UPnP: {e2}")));
                        }
                    }
                }
            }
        }

        results
    }

    /// 删除端口映射（清理）。
    pub async fn unmap_port(
        &self,
        external_port: u16,
        protocol: PortMappingProtocol,
    ) -> Result<(), String> {
        let gateway = self.gateway;
        // 先试 NAT-PMP
        if let Err(e) = nat_pmp_unmap_port(gateway, external_port, protocol).await {
            tracing::debug!(error = %e, "NAT-PMP 删除映射失败，尝试 UPnP");
            // 再试 UPnP
            upnp_unmap_port(None, external_port, protocol).await?;
        }
        Ok(())
    }
}

impl Default for UpnpClient {
    fn default() -> Self {
        Self::new()
    }
}

// ----------------------------------------------------------------------
// NAT-PMP (RFC 6886)
// ----------------------------------------------------------------------

const NAT_PMP_PORT: u16 = 5351;
const NAT_PMP_VERSION: u8 = 0;

/// NAT-PMP 映射端口（公共接口，使用默认 5351 端口）。
async fn nat_pmp_map_port(
    gateway: Option<IpAddr>,
    internal_port: u16,
    external_port: u16,
    protocol: PortMappingProtocol,
    lifetime: u32,
) -> Result<NatPortMapping, String> {
    let gateway_ip = resolve_gateway(gateway).await?;
    let gateway_addr = SocketAddr::new(gateway_ip, NAT_PMP_PORT);
    nat_pmp_map_port_to_addr(
        gateway_addr,
        internal_port,
        external_port,
        protocol,
        lifetime,
    )
    .await
}

/// NAT-PMP 映射端口（指定网关地址，含端口）。
async fn nat_pmp_map_port_to_addr(
    gateway_addr: SocketAddr,
    internal_port: u16,
    external_port: u16,
    protocol: PortMappingProtocol,
    lifetime: u32,
) -> Result<NatPortMapping, String> {
    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .map_err(|e| format!("NAT-PMP socket 绑定失败: {e}"))?;

    // 1. 获取外部地址（opcode 0）
    let ext_req = [NAT_PMP_VERSION, 0u8];
    socket
        .send_to(&ext_req, gateway_addr)
        .await
        .map_err(|e| format!("NAT-PMP 发送失败: {e}"))?;

    let mut buf = vec![0u8; 12];
    let n = timeout(Duration::from_secs(1), socket.recv_from(&mut buf))
        .await
        .map_err(|_| "NAT-PMP 外部地址响应超时".to_string())?
        .map_err(|e| format!("NAT-PMP 接收失败: {e}"))?
        .0;

    if n < 12 {
        return Err("NAT-PMP 外部地址响应过短".into());
    }
    let version = buf[0];
    let op = buf[1];
    let result_code = u16::from_be_bytes([buf[2], buf[3]]);
    if version != NAT_PMP_VERSION {
        return Err(format!("NAT-PMP 版本不匹配: {version}"));
    }
    if op != 0 {
        return Err(format!("NAT-PMP opcode 不匹配: {op}"));
    }
    if result_code != 0 {
        return Err(format!("NAT-PMP 错误码: {result_code}"));
    }
    let ip_bytes: [u8; 4] = buf[8..12].try_into().unwrap();
    let external_ip = IpAddr::V4(Ipv4Addr::from(ip_bytes));

    // 2. 映射端口
    let op = protocol.nat_pmp_code();
    let mut req = Vec::with_capacity(12);
    req.push(NAT_PMP_VERSION); // version = 0
    req.push(op as u8); // opcode
    req.extend_from_slice(&[0, 0]); // reserved
    req.extend_from_slice(&internal_port.to_be_bytes());
    req.extend_from_slice(&external_port.to_be_bytes());
    req.extend_from_slice(&lifetime.to_be_bytes());

    socket
        .send_to(&req, gateway_addr)
        .await
        .map_err(|e| format!("NAT-PMP 发送失败: {e}"))?;

    let mut buf = vec![0u8; 16];
    let n = timeout(Duration::from_secs(1), socket.recv_from(&mut buf))
        .await
        .map_err(|_| "NAT-PMP 响应超时".to_string())?
        .map_err(|e| format!("NAT-PMP 接收失败: {e}"))?
        .0;

    parse_nat_pmp_response(
        &buf[..n],
        op,
        external_port,
        internal_port,
        protocol,
        external_ip,
        lifetime,
    )
}

/// NAT-PMP 获取外部地址。
#[allow(dead_code)]
async fn nat_pmp_get_external_addr(gateway: SocketAddr) -> Result<IpAddr, String> {
    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .map_err(|e| format!("NAT-PMP socket 绑定失败: {e}"))?;

    let req = [NAT_PMP_VERSION, 0]; // version=0, opcode=0
    socket
        .send_to(&req, gateway)
        .await
        .map_err(|e| format!("NAT-PMP 发送失败: {e}"))?;

    let mut buf = vec![0u8; 12];
    let n = timeout(Duration::from_secs(1), socket.recv_from(&mut buf))
        .await
        .map_err(|_| "NAT-PMP 外部地址响应超时".to_string())?
        .map_err(|e| format!("NAT-PMP 接收失败: {e}"))?
        .0;

    if n < 12 {
        return Err("NAT-PMP 外部地址响应过短".into());
    }

    let version = buf[0];
    let op = buf[1];
    let result_code = u16::from_be_bytes([buf[2], buf[3]]);

    if version != NAT_PMP_VERSION {
        return Err(format!("NAT-PMP 版本不匹配: {version}"));
    }
    if op != 0 {
        return Err(format!("NAT-PMP opcode 不匹配: {op}"));
    }
    if result_code != 0 {
        return Err(format!("NAT-PMP 错误码: {result_code}"));
    }

    let ip_bytes: [u8; 4] = buf[8..12].try_into().unwrap();
    Ok(IpAddr::V4(Ipv4Addr::from(ip_bytes)))
}

/// 解析 NAT-PMP 映射响应。
fn parse_nat_pmp_response(
    data: &[u8],
    expected_op: u16,
    _external_port: u16,
    internal_port: u16,
    protocol: PortMappingProtocol,
    external_ip: IpAddr,
    _lifetime: u32,
) -> Result<NatPortMapping, String> {
    if data.len() < 16 {
        return Err("NAT-PMP 映射响应过短".into());
    }

    let version = data[0];
    let op = data[1];
    let result_code = u16::from_be_bytes([data[2], data[3]]);

    if version != NAT_PMP_VERSION {
        return Err(format!("NAT-PMP 版本不匹配: {version}"));
    }
    // opcode 响应 = 0x80 + 请求 opcode
    if op != (0x80 | (expected_op as u8)) {
        return Err(format!(
            "NAT-PMP opcode 不匹配: 期望 {}, 实际 {}",
            0x80 | (expected_op as u8),
            op
        ));
    }
    if result_code != 0 {
        return Err(format!("NAT-PMP 错误码: {result_code}"));
    }

    let mapped_external_port = u16::from_be_bytes([data[8], data[9]]);
    let mapped_lifetime = u32::from_be_bytes(data[12..16].try_into().unwrap());

    Ok(NatPortMapping {
        external_ip,
        external_port: mapped_external_port,
        internal_port,
        protocol,
        lifetime: mapped_lifetime,
        source: "nat-pmp".to_string(),
    })
}

/// NAT-PMP 删除映射。
async fn nat_pmp_unmap_port(
    gateway: Option<IpAddr>,
    external_port: u16,
    protocol: PortMappingProtocol,
) -> Result<(), String> {
    let gateway_ip = resolve_gateway(gateway).await?;
    let gateway_addr = SocketAddr::new(gateway_ip, NAT_PMP_PORT);

    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .map_err(|e| format!("NAT-PMP socket 绑定失败: {e}"))?;

    let op = protocol.nat_pmp_code();
    let mut req = Vec::with_capacity(12);
    req.push(NAT_PMP_VERSION);
    req.push(op as u8);
    req.extend_from_slice(&[0, 0]);
    req.extend_from_slice(&0u16.to_be_bytes()); // internal_port = 0
    req.extend_from_slice(&external_port.to_be_bytes());
    req.extend_from_slice(&0u32.to_be_bytes()); // lifetime = 0

    socket
        .send_to(&req, gateway_addr)
        .await
        .map_err(|e| format!("NAT-PMP 发送失败: {e}"))?;

    let mut buf = vec![0u8; 16];
    let n = timeout(Duration::from_secs(1), socket.recv_from(&mut buf))
        .await
        .map_err(|_| "NAT-PMP 响应超时".to_string())?
        .map_err(|e| format!("NAT-PMP 接收失败: {e}"))?
        .0;

    if n < 4 {
        return Err("NAT-PMP 响应过短".into());
    }
    let result_code = u16::from_be_bytes([buf[2], buf[3]]);
    if result_code != 0 {
        return Err(format!("NAT-PMP 错误码: {result_code}"));
    }
    Ok(())
}

// ----------------------------------------------------------------------
// UPnP IGD (SSDP 发现 + SOAP 控制)
// ----------------------------------------------------------------------

/// UPnP SSDP 多播地址。
const SSDP_ADDR: Ipv4Addr = Ipv4Addr::new(239, 255, 255, 250);
const SSDP_PORT: u16 = 1900;

/// 支持的 IGD 服务类型（按优先级）：
/// - `WANIPConnection`：路由器桥接/二级路由，内网口直接持有公网地址；
/// - `WANPPPConnection`：路由器自己拨号（PPPoE）——国内家用路由器绝大多数
///   属于这一类，只认 IPConnection 会直接报「设备描述中未找到服务」。
const IGD_SERVICE_TYPES: [&str; 2] = [
    "urn:schemas-upnp-org:service:WANIPConnection:1",
    "urn:schemas-upnp-org:service:WANPPPConnection:1",
];

/// SSDP 发现的 IGD：设备描述地址 + 响应来源地址（= 路由器内网地址）。
struct Igd {
    location: String,
    ip: IpAddr,
}

/// UPnP SSDP 发现（返回设备描述 URL 与响应来源地址）。
async fn upnp_ssdp_discover() -> Result<Igd, String> {
    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .map_err(|e| format!("SSDP socket 绑定失败: {e}"))?;

    let msg = "M-SEARCH * HTTP/1.1\r\n\
               HOST: 239.255.255.250:1900\r\n\
               MAN: \"ssdp:discover\"\r\n\
               MX: 2\r\n\
               ST: urn:schemas-upnp-org:device:InternetGatewayDevice:1\r\n\
               \r\n";

    let addr = SocketAddr::new(IpAddr::V4(SSDP_ADDR), SSDP_PORT);
    socket
        .send_to(msg.as_bytes(), addr)
        .await
        .map_err(|e| format!("SSDP 发送失败: {e}"))?;

    let mut buf = vec![0u8; 4096];
    // 多播会收到多个设备的响应（路由器 + 电视 + NAS），只取第一个带
    // Location 的；IGD 通常最先回，拿不到再退化为 UPnP 不可用。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err("SSDP 响应超时".into());
        }
        let (n, from) = match timeout(remaining, socket.recv_from(&mut buf)).await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(format!("SSDP 接收失败: {e}")),
            Err(_) => return Err("SSDP 响应超时".into()),
        };
        let text = match std::str::from_utf8(&buf[..n]) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let location = text.lines().find_map(|line| {
            let line = line.trim();
            if line.len() > 9 && line[..9].eq_ignore_ascii_case("location:") {
                Some(line[9..].trim().to_string())
            } else {
                None
            }
        });
        if let Some(location) = location {
            return Ok(Igd {
                location,
                ip: from.ip(),
            });
        }
    }
}

/// 构建 SOAP 用的 HTTP 客户端。
///
/// 同一操作里**只建一次**：客户端初始化要加载根证书（本机实测约 0.8s），
/// 每个请求各建一次会让映射/撤销白花一两秒。
fn http_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| format!("HTTP 客户端构建失败: {e}"))
}

/// 读取设备描述 XML。
async fn fetch_description(client: &reqwest::Client, url: &str) -> Result<String, String> {
    client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("UPnP 设备描述请求失败: {e}"))?
        .text()
        .await
        .map_err(|e| format!("UPnP 设备描述读取失败: {e}"))
}

/// 本机到 `peer` 会使用的源地址（= 内网地址）。
///
/// `NewInternalClient` 必须填这个真实地址：填 `0.0.0.0` 时标准实现一律回
/// **402 Invalid NewInternalClient**（2026-10-24 在 TP-Link 上实测），映射
/// 永远建不起来——引擎此前的实现正是填 0.0.0.0，所以 UPnP 从未生效过。
async fn local_ip_towards(peer: IpAddr) -> Result<IpAddr, String> {
    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .map_err(|e| format!("内网地址探测 socket 绑定失败: {e}"))?;
    // UDP connect 不发包，只让内核按路由表选定出口网卡与源地址
    socket
        .connect(SocketAddr::new(peer, SSDP_PORT))
        .await
        .map_err(|e| format!("内网地址探测失败: {e}"))?;
    let local = socket
        .local_addr()
        .map_err(|e| format!("内网地址读取失败: {e}"))?;
    match local.ip() {
        IpAddr::V4(v4) if !v4.is_unspecified() => Ok(IpAddr::V4(v4)),
        other => Err(format!("无法确定本机内网地址: {other}")),
    }
}

/// UPnP SOAP AddPortMapping。
async fn upnp_map_port(
    igd: Option<&Igd>,
    internal_port: u16,
    external_port: u16,
    protocol: PortMappingProtocol,
    lifetime: u32,
) -> Result<NatPortMapping, String> {
    // 复用调用方已经做过的 SSDP 发现（`map_port` 对 TCP/UDP 只发现一次），
    // 单独调用时再自己发现。
    let discovered;
    let igd: &Igd = match igd {
        Some(found) => found,
        None => {
            discovered = upnp_ssdp_discover().await?;
            &discovered
        }
    };

    let client = http_client()?;
    let desc = fetch_description(&client, &igd.location).await?;
    let (control_url, service_type) = extract_control_url(&desc, &igd.location)?;
    let internal_client = local_ip_towards(igd.ip).await?;

    let soap_body = format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:AddPortMapping xmlns:u="{service_type}">
      <NewRemoteHost></NewRemoteHost>
      <NewExternalPort>{external_port}</NewExternalPort>
      <NewProtocol>{protocol}</NewProtocol>
      <NewInternalPort>{internal_port}</NewInternalPort>
      <NewInternalClient>{internal_client}</NewInternalClient>
      <NewEnabled>1</NewEnabled>
      <NewPortMappingDescription>{ENGINE_NAME}</NewPortMappingDescription>
      <NewLeaseDuration>{lifetime}</NewLeaseDuration>
    </u:AddPortMapping>
  </s:Body>
</s:Envelope>"#
    );

    let resp = client
        .post(&control_url)
        .header("Content-Type", "text/xml; charset=utf-8")
        .header("SOAPAction", format!("\"{service_type}#AddPortMapping\""))
        .body(soap_body)
        .send()
        .await
        .map_err(|e| format!("UPnP SOAP 请求失败: {e}"))?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "UPnP SOAP 错误: {} {}",
            body_error_code(&body).unwrap_or_else(|| "unknown".into()),
            body_error_text(&body).unwrap_or_default()
        ));
    }

    Ok(NatPortMapping {
        // 简化：不额外发 GetExternalIPAddress（多一次往返），外部地址对
        // 映射本身没有影响，日志里以端口为准。
        external_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        external_port,
        internal_port,
        protocol,
        lifetime,
        source: "upnp".to_string(),
    })
}

/// 从 SOAP Fault 里取 UPnP 错误码（用于把人话写进日志）。
fn body_error_code(body: &str) -> Option<String> {
    let start = body.find("<errorCode>")? + "<errorCode>".len();
    let end = body[start..].find("</errorCode>")? + start;
    Some(body[start..end].trim().to_string())
}

fn body_error_text(body: &str) -> Option<String> {
    let start = body.find("<errorDescription>")? + "<errorDescription>".len();
    let end = body[start..].find("</errorDescription>")? + start;
    Some(body[start..end].trim().to_string())
}

/// UPnP 删除映射。
async fn upnp_unmap_port(
    igd: Option<&Igd>,
    external_port: u16,
    protocol: PortMappingProtocol,
) -> Result<(), String> {
    let discovered;
    let igd: &Igd = match igd {
        Some(found) => found,
        None => {
            discovered = upnp_ssdp_discover().await?;
            &discovered
        }
    };
    let client = http_client()?;
    let desc = fetch_description(&client, &igd.location).await?;
    let (control_url, service_type) = extract_control_url(&desc, &igd.location)?;

    let soap_body = format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:DeletePortMapping xmlns:u="{service_type}">
      <NewRemoteHost></NewRemoteHost>
      <NewExternalPort>{external_port}</NewExternalPort>
      <NewProtocol>{protocol}</NewProtocol>
    </u:DeletePortMapping>
  </s:Body>
</s:Envelope>"#
    );

    let resp = client
        .post(&control_url)
        .header("Content-Type", "text/xml; charset=utf-8")
        .header(
            "SOAPAction",
            format!("\"{service_type}#DeletePortMapping\""),
        )
        .body(soap_body)
        .send()
        .await
        .map_err(|e| format!("UPnP SOAP 请求失败: {e}"))?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "UPnP SOAP 错误: {} {}",
            body_error_code(&body).unwrap_or_else(|| "unknown".into()),
            body_error_text(&body).unwrap_or_default()
        ));
    }
    Ok(())
}

/// 从 UPnP 设备描述 XML 中提取控制 URL 与服务类型。
///
/// 注意 `<controlURL>` 必须落在**同一个 `<service>` 块内**：设备描述里
/// 往往有十几个服务，先出现的可能是别的服务的 controlURL。
fn extract_control_url(desc: &str, base_url: &str) -> Result<(String, &'static str), String> {
    for service_type in IGD_SERVICE_TYPES {
        let Some(st_pos) = desc.find(service_type) else {
            continue;
        };
        let after = &desc[st_pos..];
        let block_end = after.find("</service>").unwrap_or(after.len());
        let Some(ctrl_start) = after.find("<controlURL>") else {
            continue;
        };
        if ctrl_start > block_end {
            continue;
        }
        let value_start = ctrl_start + "<controlURL>".len();
        let Some(ctrl_end) = after[value_start..].find("</controlURL>") else {
            continue;
        };
        let path = after[value_start..value_start + ctrl_end].trim();
        if path.is_empty() {
            continue;
        }
        return Ok((join_url(base_url, path), service_type));
    }
    Err("设备描述中未找到 WANIPConnection/WANPPPConnection 服务".into())
}

/// 把（可能是相对的）controlURL 拼到设备描述的 origin 上。
fn join_url(base_url: &str, path: &str) -> String {
    if path.starts_with("http://") || path.starts_with("https://") {
        return path.to_string();
    }
    let base = base_url.split('#').next().unwrap_or(base_url);
    let (origin, base_path) = match base.find("://") {
        Some(scheme_end) => {
            let after_scheme_start = scheme_end + 3;
            match base[after_scheme_start..].find('/') {
                Some(i) => {
                    let split = after_scheme_start + i;
                    (&base[..split], &base[split..])
                }
                None => (base, ""),
            }
        }
        None => (base, ""),
    };
    if path.starts_with('/') {
        return format!("{origin}{path}");
    }
    let dir = match base_path.rfind('/') {
        Some(i) => &base_path[..=i],
        None => "/",
    };
    format!("{origin}{dir}{path}")
}
// ----------------------------------------------------------------------
// 网关发现
// ----------------------------------------------------------------------

/// 解析网关地址（自动发现或使用指定地址）。
async fn resolve_gateway(gateway: Option<IpAddr>) -> Result<IpAddr, String> {
    if let Some(gw) = gateway {
        return Ok(gw);
    }
    // 简化：默认网关 192.168.0.1 / 192.168.1.1（常见家用路由器地址）
    // 实际实现应读取路由表，这里尝试常见地址。
    // 注意：NAT-PMP 使用 UDP 5351，很多网关不监听 TCP 5351，
    // 因此使用 UDP 探测而非 TCP 连接。
    let candidates: [Ipv4Addr; 4] = [
        Ipv4Addr::new(192, 168, 0, 1),
        Ipv4Addr::new(192, 168, 1, 1),
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(10, 1, 1, 1),
    ];
    // 并行探测：串行时 4 个候选地址最坏要等 4×600ms，而这段时间对
    // 「路由器不支持 NAT-PMP」的场景是纯粹的浪费 —— 映射与撤销都要付一次。
    let (a, b, c, d) = tokio::join!(
        probe_nat_pmp_gateway(IpAddr::V4(candidates[0])),
        probe_nat_pmp_gateway(IpAddr::V4(candidates[1])),
        probe_nat_pmp_gateway(IpAddr::V4(candidates[2])),
        probe_nat_pmp_gateway(IpAddr::V4(candidates[3])),
    );
    for (ip, ok) in candidates.into_iter().zip([a, b, c, d]) {
        if ok {
            return Ok(IpAddr::V4(ip));
        }
    }
    Err("无法发现 NAT 网关".into())
}

/// 用 UDP 探测网关是否支持 NAT-PMP（发送 opcode 0 外部地址请求）。
async fn probe_nat_pmp_gateway(gateway: IpAddr) -> bool {
    let socket = match UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(_) => return false,
    };
    let addr = SocketAddr::new(gateway, NAT_PMP_PORT);
    // 发送 NAT-PMP 外部地址请求 (version=0, opcode=0)
    if socket.send_to(&[0u8, 0u8], addr).await.is_err() {
        return false;
    }
    // 局域网内一次 UDP 往返，正常 < 20ms；用 600ms 上限而不是 2s，
    // 否则 4 个候选地址就要盲等 8 秒，把 UPnP 回退一起拖慢。
    let mut buf = vec![0u8; 12];
    timeout(Duration::from_millis(600), socket.recv_from(&mut buf))
        .await
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_values() {
        // NAT-PMP opcode 按 RFC 6886 §3.3：1 = 映射 UDP、2 = 映射 TCP。
        // 此前 TCP 断言为 6（既非 1 也非 2）——网关只回 unsupported，
        // NAT-PMP 的 TCP 映射从未生效，全靠 UPnP 回退兜底。
        assert_eq!(PortMappingProtocol::Udp.nat_pmp_code(), 1);
        assert_eq!(PortMappingProtocol::Tcp.nat_pmp_code(), 2);
        assert_eq!(PortMappingProtocol::Tcp.upnp_protocol_str(), "TCP");
        assert_eq!(PortMappingProtocol::Udp.upnp_protocol_str(), "UDP");
    }

    #[test]
    fn nat_pmp_request_format() {
        // 验证 NAT-PMP 映射请求格式
        let mut req = Vec::with_capacity(12);
        req.push(NAT_PMP_VERSION); // version = 0
        req.push(1u8); // opcode = 1 (UDP map)
        req.extend_from_slice(&[0, 0]); // reserved
        req.extend_from_slice(&6881u16.to_be_bytes()); // internal_port
        req.extend_from_slice(&6881u16.to_be_bytes()); // external_port
        req.extend_from_slice(&3600u32.to_be_bytes()); // lifetime

        assert_eq!(req.len(), 12);
        assert_eq!(req[0], 0); // version
        assert_eq!(req[1], 1); // opcode
    }

    #[test]
    fn parse_nat_pmp_response_success() {
        let mut buf = vec![0u8; 16];
        buf[0] = 0; // version
        buf[1] = 0x81; // opcode (0x80 | 1 = UDP)
        buf[2..4].copy_from_slice(&0u16.to_be_bytes()); // result = success
        buf[4..8].copy_from_slice(&0u32.to_be_bytes()); // epoch
        buf[8..10].copy_from_slice(&6881u16.to_be_bytes()); // external_port
        buf[10..12].copy_from_slice(&[0, 0]); // internal_port
        buf[12..16].copy_from_slice(&3600u32.to_be_bytes()); // lifetime

        let mapping = parse_nat_pmp_response(
            &buf,
            1, // expected_op
            6881,
            6881,
            PortMappingProtocol::Udp,
            IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)),
            3600,
        )
        .unwrap();

        assert_eq!(mapping.external_port, 6881);
        assert_eq!(mapping.lifetime, 3600);
        assert_eq!(mapping.source, "nat-pmp");
        assert_eq!(mapping.protocol, PortMappingProtocol::Udp);
    }

    #[test]
    fn parse_nat_pmp_response_error_code() {
        let mut buf = vec![0u8; 16];
        buf[0] = 0;
        buf[1] = 0x81;
        buf[2..4].copy_from_slice(&5u16.to_be_bytes()); // result = error 5

        let result = parse_nat_pmp_response(
            &buf,
            1,
            6881,
            6881,
            PortMappingProtocol::Udp,
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            3600,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("错误码"));
    }

    #[test]
    fn extract_control_url_from_xml() {
        let desc = r#"<?xml version="1.0"?>
<root>
  <device>
    <serviceList>
      <service>
        <serviceType>urn:schemas-upnp-org:service:WANIPConnection:1</serviceType>
        <controlURL>/upnp/control/WANIPConn1</controlURL>
      </service>
    </serviceList>
  </device>
</root>"#;

        let (url, service) =
            extract_control_url(desc, "http://192.168.1.1:1900/rootDesc.xml").unwrap();
        assert_eq!(url, "http://192.168.1.1:1900/upnp/control/WANIPConn1");
        assert_eq!(service, "urn:schemas-upnp-org:service:WANIPConnection:1");
    }

    /// PPPoE 路由器只暴露 WANPPPConnection：此前代码只认 WANIPConnection，
    /// 这类路由器（国内家用路由器绝大多数）会直接报「未找到服务」。
    #[test]
    fn extract_control_url_ppp_service() {
        let desc = r#"<?xml version="1.0"?>
<root>
  <device>
    <serviceList>
      <service>
        <serviceType>urn:schemas-upnp-org:service:WANPPPConnection:1</serviceType>
        <controlURL>ipc</controlURL>
      </service>
    </serviceList>
  </device>
</root>"#;

        let (url, service) = extract_control_url(desc, "http://192.168.31.1:1900/igd.xml").unwrap();
        // 相对路径（不带前导 /）要按设备描述所在目录拼
        assert_eq!(url, "http://192.168.31.1:1900/ipc");
        assert_eq!(service, "urn:schemas-upnp-org:service:WANPPPConnection:1");
    }

    /// 别处服务的 controlURL 不能被误当成 IGD 的。
    #[test]
    fn extract_control_url_ignores_other_service() {
        let desc = r#"<?xml version="1.0"?>
<root>
  <device>
    <serviceList>
      <service>
        <serviceType>urn:schemas-upnp-org:service:Layer3Forwarding:1</serviceType>
        <controlURL>/l3f</controlURL>
      </service>
      <service>
        <serviceType>urn:schemas-upnp-org:service:WANIPConnection:1</serviceType>
        <controlURL>/ipc</controlURL>
      </service>
    </serviceList>
  </device>
</root>"#;

        let (url, _) = extract_control_url(desc, "http://192.168.0.1:1900/igd.xml").unwrap();
        assert_eq!(url, "http://192.168.0.1:1900/ipc");
    }

    #[test]
    fn extract_control_url_missing_service() {
        let desc = r#"<?xml version="1.0"?><root></root>"#;
        assert!(extract_control_url(desc, "http://192.168.1.1:1900/desc.xml").is_err());
    }

    #[test]
    fn join_url_absolute_and_relative() {
        // 绝对 URL 原样返回
        assert_eq!(
            join_url("http://192.168.1.1:1900/igd.xml", "http://10.0.0.1/ctl"),
            "http://10.0.0.1/ctl"
        );
        // 根路径
        assert_eq!(
            join_url("http://192.168.1.1:1900/igd.xml", "/ctl"),
            "http://192.168.1.1:1900/ctl"
        );
        // 相对路径（目录保留）
        assert_eq!(
            join_url("http://192.168.1.1:1900/upnp/igd.xml", "ctl"),
            "http://192.168.1.1:1900/upnp/ctl"
        );
        // 描述地址没有路径
        assert_eq!(
            join_url("http://192.168.1.1:1900", "ctl"),
            "http://192.168.1.1:1900/ctl"
        );
    }

    /// 模拟 NAT-PMP 服务器测试。
    #[tokio::test]
    async fn mock_nat_pmp_server() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 64];
            // 1. 接收外部地址请求 (opcode 0)
            let (n, client) = server.recv_from(&mut buf).await.unwrap();
            assert_eq!(n, 2);
            assert_eq!(buf[0], 0); // version
            assert_eq!(buf[1], 0); // opcode = 0

            // 响应外部地址
            let mut resp = vec![0u8; 12];
            resp[0] = 0; // version
            resp[1] = 0; // opcode = 0
            resp[2..4].copy_from_slice(&0u16.to_be_bytes()); // success
            resp[4..8].copy_from_slice(&12345u32.to_be_bytes()); // epoch
            resp[8..12].copy_from_slice(&[1, 2, 3, 4]); // external IP
            server.send_to(&resp, client).await.unwrap();

            // 2. 接收 UDP 映射请求 (opcode 1)
            let (n, _) = server.recv_from(&mut buf).await.unwrap();
            assert_eq!(n, 12);
            assert_eq!(buf[1], 1); // opcode = 1 (UDP)

            // 响应映射
            let mut resp = vec![0u8; 16];
            resp[0] = 0;
            resp[1] = 0x81; // 0x80 | 1
            resp[2..4].copy_from_slice(&0u16.to_be_bytes()); // success
            resp[4..8].copy_from_slice(&12345u32.to_be_bytes()); // epoch
            resp[8..10].copy_from_slice(&6881u16.to_be_bytes()); // external_port
            resp[10..12].copy_from_slice(&6881u16.to_be_bytes()); // internal_port
            resp[12..16].copy_from_slice(&3600u32.to_be_bytes()); // lifetime
            server.send_to(&resp, client).await.unwrap();
        });

        // 直接测试 NAT-PMP 映射（指定网关地址含端口）
        let mapping =
            nat_pmp_map_port_to_addr(server_addr, 6881, 6881, PortMappingProtocol::Udp, 3600)
                .await
                .unwrap();

        assert_eq!(mapping.external_port, 6881);
        assert_eq!(mapping.internal_port, 6881);
        assert_eq!(mapping.lifetime, 3600);
        assert_eq!(mapping.protocol, PortMappingProtocol::Udp);
        assert_eq!(mapping.source, "nat-pmp");
        assert_eq!(mapping.external_ip, IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)));

        server_task.await.unwrap();
    }

    /// 真机验证（需要路由器开启 UPnP；默认跳过）：
    ///
    /// ```text
    /// cargo test -p xfer-discovery --lib real_upnp_map -- --ignored --nocapture
    /// ```
    ///
    /// 建立 TCP+UDP 映射后立即撤销，用来确认「内网地址填写 / IGD 服务类型 /
    /// SOAP 动作」在真实路由器上确实能成功（2026-10-24 用 TP-Link 验证：
    /// 旧实现填 0.0.0.0 被回 402，新实现映射成功）。
    #[tokio::test]
    #[ignore]
    async fn real_upnp_map_and_unmap() {
        const PORT: u16 = 45678;
        let client = UpnpClient::new();
        let results = client.map_port(PORT, PORT, 120).await;
        for r in &results {
            eprintln!("map result: {r:?}");
        }
        assert!(
            results.iter().any(|r| r.is_ok()),
            "NAT-PMP 与 UPnP 都失败（路由器未开 UPnP？）"
        );
        for proto in [PortMappingProtocol::Tcp, PortMappingProtocol::Udp] {
            match client.unmap_port(PORT, proto).await {
                Ok(()) => eprintln!("unmap {proto} ok"),
                Err(e) => eprintln!("unmap {proto} fail: {e}"),
            }
        }
    }
}
