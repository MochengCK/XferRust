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
    fn nat_pmp_code(&self) -> u16 {
        match self {
            PortMappingProtocol::Tcp => 6, // NAT-PMP op 6 = map TCP
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
        let gateway = self.gateway;

        for proto in [PortMappingProtocol::Tcp, PortMappingProtocol::Udp] {
            // 先尝试 NAT-PMP
            match nat_pmp_map_port(gateway, internal_port, external_port, proto, lifetime).await {
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
                    // NAT-PMP 失败，回退到 UPnP 确保该协议仍可被映射
                    match upnp_map_port(gateway, internal_port, external_port, proto, lifetime)
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
                            tracing::debug!(proto = ?proto, error = %e2, "UPnP 映射也失败");
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
            upnp_unmap_port(gateway, external_port, protocol).await?;
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
    let n = timeout(Duration::from_secs(5), socket.recv_from(&mut buf))
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
    let n = timeout(Duration::from_secs(5), socket.recv_from(&mut buf))
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
    let n = timeout(Duration::from_secs(5), socket.recv_from(&mut buf))
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
    let n = timeout(Duration::from_secs(5), socket.recv_from(&mut buf))
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

/// UPnP SSDP 发现。
async fn upnp_ssdp_discover() -> Result<String, String> {
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
    let n = timeout(Duration::from_secs(3), socket.recv_from(&mut buf))
        .await
        .map_err(|_| "SSDP 响应超时".to_string())?
        .map_err(|e| format!("SSDP 接收失败: {e}"))?
        .0;

    let text = std::str::from_utf8(&buf[..n]).map_err(|_| "SSDP 响应非 UTF-8".to_string())?;

    // 提取 Location 头
    for line in text.lines() {
        let line = line.trim();
        if line.to_lowercase().starts_with("location:") {
            let url = line[9..].trim().to_string();
            return Ok(url);
        }
    }
    Err("SSDP 响应中未找到 Location".into())
}

/// UPnP SOAP AddPortMapping。
async fn upnp_map_port(
    _gateway: Option<IpAddr>,
    internal_port: u16,
    external_port: u16,
    protocol: PortMappingProtocol,
    lifetime: u32,
) -> Result<NatPortMapping, String> {
    // 1. SSDP 发现
    let location = upnp_ssdp_discover().await?;

    // 2. 获取设备描述 → 提取控制 URL
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| format!("HTTP 客户端构建失败: {e}"))?;

    let desc = client
        .get(&location)
        .send()
        .await
        .map_err(|e| format!("UPnP 设备描述请求失败: {e}"))?
        .text()
        .await
        .map_err(|e| format!("UPnP 设备描述读取失败: {e}"))?;

    // 简化：从 XML 中提取 WANIPConnection 控制 URL
    let control_url = extract_control_url(&desc, &location)?;

    // 3. SOAP AddPortMapping
    let soap_body = format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:AddPortMapping xmlns:u="urn:schemas-upnp-org:service:WANIPConnection:1">
      <NewRemoteHost></NewRemoteHost>
      <NewExternalPort>{external_port}</NewExternalPort>
      <NewProtocol>{protocol}</NewProtocol>
      <NewInternalPort>{internal_port}</NewInternalPort>
      <NewInternalClient>0.0.0.0</NewInternalClient>
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
        .header(
            "SOAPAction",
            r#""urn:schemas-upnp-org:service:WANIPConnection:1#AddPortMapping""#,
        )
        .body(soap_body)
        .send()
        .await
        .map_err(|e| format!("UPnP SOAP 请求失败: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!("UPnP SOAP 错误: {}", resp.status()));
    }

    // 简化：不解析外部 IP（UPnP 需额外 GetExternalIPAddress 调用）
    Ok(NatPortMapping {
        external_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        external_port,
        internal_port,
        protocol,
        lifetime,
        source: "upnp".to_string(),
    })
}

/// UPnP 删除映射。
async fn upnp_unmap_port(
    _gateway: Option<IpAddr>,
    external_port: u16,
    protocol: PortMappingProtocol,
) -> Result<(), String> {
    let location = upnp_ssdp_discover().await?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|e| format!("HTTP 客户端构建失败: {e}"))?;

    let desc = client
        .get(&location)
        .send()
        .await
        .map_err(|e| format!("UPnP 设备描述请求失败: {e}"))?
        .text()
        .await
        .map_err(|e| format!("UPnP 设备描述读取失败: {e}"))?;

    let control_url = extract_control_url(&desc, &location)?;

    let soap_body = format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:DeletePortMapping xmlns:u="urn:schemas-upnp-org:service:WANIPConnection:1">
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
            r#""urn:schemas-upnp-org:service:WANIPConnection:1#DeletePortMapping""#,
        )
        .body(soap_body)
        .send()
        .await
        .map_err(|e| format!("UPnP SOAP 请求失败: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!("UPnP SOAP 错误: {}", resp.status()));
    }
    Ok(())
}

/// 从 UPnP 设备描述 XML 中提取控制 URL。
fn extract_control_url(desc: &str, base_url: &str) -> Result<String, String> {
    // 简化 XML 解析：查找 WANIPConnection 的 controlURL
    // 在实际实现中应使用 XML 解析器，这里用字符串搜索
    let service_type = "urn:schemas-upnp-org:service:WANIPConnection:1";

    // 找到 serviceType 标签位置
    let st_pos = desc
        .find(service_type)
        .ok_or_else(|| "设备描述中未找到 WANIPConnection 服务".to_string())?;

    // 从 st_pos 向后找 controlURL 标签
    let after = &desc[st_pos..];
    let ctrl_start = after
        .find("<controlURL>")
        .ok_or_else(|| "未找到 controlURL 标签".to_string())?;
    let ctrl_end = after[ctrl_start..]
        .find("</controlURL>")
        .ok_or_else(|| "controlURL 标签未闭合".to_string())?;

    let path = &after[ctrl_start + 12..ctrl_start + ctrl_end];
    let path = path.trim();

    // 路径 + base URL 组合
    let base_url = base_url
        .rsplit_once('/')
        .map(|(prefix, _)| prefix.to_string())
        .unwrap_or_else(|| base_url.to_string());

    Ok(format!("{base_url}{path}"))
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
    for ip in candidates {
        if probe_nat_pmp_gateway(IpAddr::V4(ip)).await {
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
    let mut buf = vec![0u8; 12];
    timeout(Duration::from_secs(2), socket.recv_from(&mut buf))
        .await
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_values() {
        assert_eq!(PortMappingProtocol::Tcp.nat_pmp_code(), 6);
        assert_eq!(PortMappingProtocol::Udp.nat_pmp_code(), 1);
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

        let url = extract_control_url(desc, "http://192.168.1.1:1900/rootDesc.xml").unwrap();
        assert_eq!(url, "http://192.168.1.1:1900/upnp/control/WANIPConn1");
    }

    #[test]
    fn extract_control_url_missing_service() {
        let desc = r#"<?xml version="1.0"?><root></root>"#;
        assert!(extract_control_url(desc, "http://192.168.1.1:1900/desc.xml").is_err());
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
}
