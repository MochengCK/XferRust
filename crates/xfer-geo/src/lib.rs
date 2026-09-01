//! ip2region xdb 离线 IP 地理库只读查询器。
//!
//! 移植自 Electron 端使用的 `ip2region.js`（数据格式逐字节对齐），
//! 供 TUI peer 列表显示国家/地区。无任何第三方依赖：整个 xdb 一次
//! 读入内存（v4 库约 11MB），查询为向量索引定位 + 段索引二分，微秒级。
//!
//! 格式要点（头部与指针字段均为小端）：
//! - 头部 256B：version u16@0（2=旧结构恒 IPv4，3=新结构看 ipVersion u16@16），
//!   startIndexPtr u32@8，endIndexPtr u32@12；
//! - 向量索引 @256：256×256×8 字节，`idx=(ip[0]*256+ip[1])*8`，
//!   项为 (sPtr, ePtr) u32LE，指向段索引区间 [sPtr, ePtr)；
//! - 段索引项：IPv4 14B = startIP(u32LE) + endIP(u32LE) + dataLen(u16LE)
//!   + dataPtr(u32LE)；IPv6 38B（IP 为 16B 大端，dataLen@32，dataPtr@34）；
//! - 数据区：dataPtr 处 dataLen 字节原始 UTF-8，`|` 分隔 5 段：
//!   国家|省份|城市|ISP|国家代码，值 "0" 表示空。

use std::cmp::Ordering;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::path::Path;

const HEADER_LEN: usize = 256;
const VECTOR_INDEX_OFF: usize = HEADER_LEN;
const VECTOR_INDEX_ITEM: usize = 8;

const SEG_SIZE_V4: u32 = 14;
const SEG_SIZE_V6: u32 = 38;

/// IP 地理查询结果（"0" 字段已归一化为空字符串）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Region {
    pub country: String,
    pub province: String,
    pub city: String,
    pub isp: String,
    /// ISO 国家代码（如 "CN"），可能为空。
    pub country_code: String,
}

/// 已加载的 xdb 查询器（按文件区分 IPv4/IPv6）。
pub struct GeoDb {
    buf: Vec<u8>,
    ipv6: bool,
}

impl GeoDb {
    /// 读取整个 xdb 文件。文件结构非法时返回错误。
    pub fn load(path: &Path) -> io::Result<Self> {
        let buf = std::fs::read(path)?;
        if buf.len() < HEADER_LEN + 256 * 256 * VECTOR_INDEX_ITEM {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "xdb 文件过小，缺少头部或向量索引",
            ));
        }
        let version = u16_le(&buf, 0);
        let ipv6 = match version {
            2 => false,
            3 => u16_le(&buf, 16) == 6,
            v => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("未知 xdb 结构版本: {v}"),
                ))
            }
        };
        Ok(Self { buf, ipv6 })
    }

    pub fn is_ipv6(&self) -> bool {
        self.ipv6
    }

    /// 查询 IP 地理信息。IP 版本与库不匹配、格式非法或无记录时返回 None。
    pub fn search(&self, ip: &str) -> Option<Region> {
        if self.ipv6 {
            let octets = ip.parse::<Ipv6Addr>().ok()?.octets();
            self.search_bytes(&octets)
        } else {
            let octets = ip.parse::<Ipv4Addr>().ok()?.octets();
            self.search_bytes(&octets)
        }
    }

    fn search_bytes(&self, ip: &[u8]) -> Option<Region> {
        let (seg_size, ip_len) = if self.ipv6 {
            (SEG_SIZE_V6 as usize, 16)
        } else {
            (SEG_SIZE_V4 as usize, 4)
        };
        // 向量索引：按 IP 高 16 位定位段索引区间
        let vi = VECTOR_INDEX_OFF + (ip[0] as usize * 256 + ip[1] as usize) * VECTOR_INDEX_ITEM;
        let s_ptr = u32_le(&self.buf, vi) as usize;
        let e_ptr = u32_le(&self.buf, vi + 4) as usize;
        if s_ptr == 0 || e_ptr <= s_ptr || e_ptr > self.buf.len() {
            return None;
        }
        // 段索引二分
        let count = (e_ptr - s_ptr) / seg_size;
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let off = s_ptr + mid * seg_size;
            if off + seg_size > self.buf.len() {
                return None;
            }
            match self.cmp_ip(ip, off) {
                Ordering::Less => hi = mid,
                Ordering::Greater => lo = mid + 1,
                Ordering::Equal => {
                    let data_len =
                        u16::from_le_bytes([self.buf[off + ip_len * 2], self.buf[off + ip_len * 2 + 1]])
                            as usize;
                    let data_ptr = u32_le(&self.buf, off + ip_len * 2 + 2) as usize;
                    if data_ptr + data_len > self.buf.len() {
                        return None;
                    }
                    let text = std::str::from_utf8(&self.buf[data_ptr..data_ptr + data_len]).ok()?;
                    return Some(parse_region(text));
                }
            }
        }
        None
    }

    /// 比较 ip 与段索引项 [startIP, endIP]：Less = 小于 start，
    /// Greater = 大于 end，Equal = 落在区间内。
    fn cmp_ip(&self, ip: &[u8], seg_off: usize) -> Ordering {
        if self.ipv6 {
            // IPv6：16 字节大端，逐字节比较
            let seg = &self.buf[seg_off..seg_off + 32];
            match ip.cmp(&seg[..16]) {
                Ordering::Less => Ordering::Less,
                Ordering::Greater | Ordering::Equal => match ip.cmp(&seg[16..32]) {
                    Ordering::Greater => Ordering::Greater,
                    _ => Ordering::Equal,
                },
            }
        } else {
            // IPv4：索引中 IP 为小端存储，读成数值与解析结果比较
            let ip_num = u32::from_be_bytes([ip[0], ip[1], ip[2], ip[3]]);
            let start = u32_le(&self.buf, seg_off);
            if ip_num < start {
                return Ordering::Less;
            }
            let end = u32_le(&self.buf, seg_off + 4);
            if ip_num > end {
                return Ordering::Greater;
            }
            Ordering::Equal
        }
    }
}

/// 解析 region 字符串：国家|省份|城市|ISP|国家代码，"0" 视为空。
fn parse_region(text: &str) -> Region {
    let parts: Vec<&str> = text.split('|').collect();
    let field = |i: usize| -> String {
        let v = parts.get(i).copied().unwrap_or("").trim();
        if v.is_empty() || v == "0" {
            String::new()
        } else {
            v.to_string()
        }
    };
    Region {
        country: field(0),
        province: field(1),
        city: field(2),
        isp: field(3),
        country_code: field(4),
    }
}

fn u16_le(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([buf[off], buf[off + 1]])
}

fn u32_le(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn v4_db() -> Option<GeoDb> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../data/ip2region_v4.xdb");
        GeoDb::load(&path).ok()
    }

    #[test]
    fn load_real_v4_db() {
        let db = v4_db().expect("data/ip2region_v4.xdb 应可加载");
        assert!(!db.is_ipv6());
    }

    #[test]
    fn china_dns_ip() {
        let Some(db) = v4_db() else { return };
        let r = db.search("114.114.114.114").expect("114.114.114.114 应有记录");
        assert_eq!(r.country, "中国");
        assert_eq!(r.province, "江苏省");
        assert_eq!(r.city, "南京市");
        assert_eq!(r.country_code, "CN");
    }

    #[test]
    fn google_dns_ip() {
        let Some(db) = v4_db() else { return };
        let r = db.search("8.8.8.8").expect("8.8.8.8 应有记录");
        assert_eq!(r.country, "United States");
        assert_eq!(r.country_code, "US");
    }

    #[test]
    fn aliyun_dns_ip() {
        let Some(db) = v4_db() else { return };
        let r = db.search("223.5.5.5").expect("223.5.5.5 应有记录");
        assert_eq!(r.country, "中国");
        assert_eq!(r.isp, "阿里");
    }

    #[test]
    fn invalid_or_mismatched_input() {
        let Some(db) = v4_db() else { return };
        assert!(db.search("not-an-ip").is_none());
        assert!(db.search("::1").is_none()); // IPv6 查 v4 库
        assert!(db.search("").is_none());
    }
}
