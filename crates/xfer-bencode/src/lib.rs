//! xfer-bencode：bencode 编解码（整数/字符串/列表/字典，字节串键排序）与
//! .torrent 解析、磁力链接解析。
//!
//! 参考 BEP 3（.torrent 文件格式）与 BEP 9（磁力链接 xt 参数）。
//! 本 crate 只依赖 types 之外的零个兄弟 crate：纯函数、可独立测试，
//! 并作为 BT 与 ut_metadata 分片共用的底层。

mod magnet;
mod torrent;

use std::collections::BTreeMap;

pub use magnet::{parse_magnet, Magnet};
pub use torrent::{parse_info_bytes, parse_torrent, FileEntry, Info, TorrentMeta};

/// 顶层字典成员的值字节区间（torrent 解析提取 info 原始字节用）。
pub type DictRanges = std::collections::BTreeMap<Vec<u8>, (usize, usize)>;

/// bencode 值模型。
///
/// 字典键为字节串（`Vec<u8>`），BTreeMap 保证按字节序排序——
/// BEP 3 要求字典键有序，编码时无需再排序。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Dict(BTreeMap<Vec<u8>, Value>),
}

/// bencode 解析/编码错误。
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BencodeError {
    #[error("输入在偏移 {offset} 处提前结束")]
    UnexpectedEof { offset: usize },
    #[error("偏移 {offset} 处非法字符 0x{byte:02x}")]
    InvalidToken { offset: usize, byte: u8 },
    #[error("偏移 {offset} 处整数格式非法")]
    InvalidInteger { offset: usize },
    #[error("偏移 {offset} 处字符串长度非法")]
    InvalidLength { offset: usize },
    #[error("长度 {len} 超出输入剩余 {remaining} 字节")]
    LengthOverflow {
        offset: usize,
        len: u64,
        remaining: usize,
    },
    #[error("负长度 {0}")]
    NegativeLength(i64),
    #[error("嵌套层级过深（超过 {0}）")]
    DepthLimit(usize),
    #[error("解析完成后仍有 {0} 字节尾随数据")]
    TrailingData(usize),
    #[error("{0}")]
    Other(String),
}

/// 最大嵌套深度（防御恶意输入导致的栈溢出）。
pub const MAX_DEPTH: usize = 64;

impl Value {
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        self.as_bytes().and_then(|b| std::str::from_utf8(b).ok())
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_dict(&self) -> Option<&BTreeMap<Vec<u8>, Value>> {
        match self {
            Value::Dict(v) => Some(v),
            _ => None,
        }
    }

    /// 按 ASCII 键取字典成员。
    pub fn dict_get(&self, key: &str) -> Option<&Value> {
        self.as_dict()?.get(key.as_bytes())
    }
}

/// 完整解析 bencode（拒绝尾随字节）。
pub fn decode(bytes: &[u8]) -> Result<Value, BencodeError> {
    let mut p = Parser::new(bytes);
    let v = p.parse_value(0)?;
    if p.pos != bytes.len() {
        return Err(BencodeError::TrailingData(bytes.len() - p.pos));
    }
    Ok(v)
}

/// 从字节流头部解析一个 bencode 值，返回 (值, 消耗字节数)。
/// ut_metadata 分片等"流中取首个值"场景使用。
pub fn decode_prefix(bytes: &[u8]) -> Result<(Value, usize), BencodeError> {
    let mut p = Parser::new(bytes);
    let v = p.parse_value(0)?;
    Ok((v, p.pos))
}

/// 序列化 bencode 值。
pub fn encode(v: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write_value(v, &mut out);
    out
}

fn write_value(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::Int(i) => {
            out.push(b'i');
            out.extend_from_slice(i.to_string().as_bytes());
            out.push(b'e');
        }
        Value::Bytes(b) => {
            out.extend_from_slice(b.len().to_string().as_bytes());
            out.push(b':');
            out.extend_from_slice(b);
        }
        Value::List(items) => {
            out.push(b'l');
            for it in items {
                write_value(it, out);
            }
            out.push(b'e');
        }
        Value::Dict(map) => {
            out.push(b'd');
            for (k, val) in map {
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.push(b':');
                out.extend_from_slice(k);
                write_value(val, out);
            }
            out.push(b'e');
        }
    }
}

struct Parser<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, pos: 0 }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn parse_value(&mut self, depth: usize) -> Result<Value, BencodeError> {
        let (v, _, _) = self.parse_value_full(depth)?;
        Ok(v)
    }

    /// 解析值并返回 (值, 起始偏移, 结束偏移)。结束偏移用于提取
    /// 字典成员（如 torrent 的 info）的原始编码字节以计算哈希。
    fn parse_value_full(&mut self, depth: usize) -> Result<(Value, usize, usize), BencodeError> {
        if depth > MAX_DEPTH {
            return Err(BencodeError::DepthLimit(MAX_DEPTH));
        }
        let start = self.pos;
        let v = match self.peek() {
            None => return Err(BencodeError::UnexpectedEof { offset: self.pos }),
            Some(b'i') => self.parse_int(),
            Some(b'l') => self.parse_list(depth),
            Some(b'd') => self.parse_dict(depth),
            Some(b'0'..=b'9') => self.parse_bytes(),
            Some(other) => {
                return Err(BencodeError::InvalidToken {
                    offset: start,
                    byte: other,
                })
            }
        }?;
        Ok((v, start, self.pos))
    }

    /// 解析顶层字典，并记录每个键对应值的原始字节区间。
    /// 顶层必须是字典；完成后拒绝尾随字节。
    /// 供 .torrent 解析提取 info 原始字节使用。
    pub(crate) fn parse_root(&mut self) -> Result<(Value, DictRanges), BencodeError> {
        if self.peek() != Some(b'd') {
            return Err(BencodeError::InvalidToken {
                offset: self.pos,
                byte: self.peek().unwrap_or(0),
            });
        }
        self.pos += 1; // 'd'
        let mut map = BTreeMap::new();
        let mut ranges = BTreeMap::new();
        loop {
            match self.peek() {
                None => return Err(BencodeError::UnexpectedEof { offset: self.pos }),
                Some(b'e') => {
                    self.pos += 1;
                    break;
                }
                Some(_) => {
                    let key = self.parse_raw_bytes()?;
                    let (val, s, e) = self.parse_value_full(1)?;
                    ranges.insert(key.clone(), (s, e));
                    map.insert(key, val);
                }
            }
        }
        if self.pos != self.input.len() {
            return Err(BencodeError::TrailingData(self.input.len() - self.pos));
        }
        Ok((Value::Dict(map), ranges))
    }

    fn parse_int(&mut self) -> Result<Value, BencodeError> {
        let start = self.pos;
        self.pos += 1; // 'i'
        let digits_start = self.pos;
        while let Some(b) = self.peek() {
            if b == b'e' {
                break;
            }
            self.pos += 1;
        }
        let s = &self.input[digits_start..self.pos];
        if self.peek().is_none() {
            return Err(BencodeError::UnexpectedEof { offset: self.pos });
        }
        self.pos += 1; // 'e'
                       // 规范要求：无前导零（"0" 除外），"-0" 非法
        let text =
            std::str::from_utf8(s).map_err(|_| BencodeError::InvalidInteger { offset: start })?;
        let valid = !text.is_empty()
            && !(text.starts_with('0') && text.len() > 1)
            && !(text.starts_with("-0"))
            && !(text.starts_with('-') && text.len() == 1);
        if !valid {
            return Err(BencodeError::InvalidInteger { offset: start });
        }
        let v = text
            .parse::<i64>()
            .map_err(|_| BencodeError::InvalidInteger { offset: start })?;
        Ok(Value::Int(v))
    }

    fn parse_length(&mut self) -> Result<usize, BencodeError> {
        let start = self.pos;
        while let Some(b) = self.peek() {
            if b == b':' {
                break;
            }
            if !b.is_ascii_digit() {
                return Err(BencodeError::InvalidLength { offset: self.pos });
            }
            self.pos += 1;
        }
        if self.peek().is_none() {
            return Err(BencodeError::UnexpectedEof { offset: self.pos });
        }
        let text = std::str::from_utf8(&self.input[start..self.pos])
            .map_err(|_| BencodeError::InvalidLength { offset: start })?;
        if text.starts_with('0') && text.len() > 1 {
            return Err(BencodeError::InvalidLength { offset: start });
        }
        let len = text
            .parse::<u64>()
            .map_err(|_| BencodeError::InvalidLength { offset: start })?;
        self.pos += 1; // ':'
        Ok(len as usize)
    }

    fn parse_bytes(&mut self) -> Result<Value, BencodeError> {
        Ok(Value::Bytes(self.parse_raw_bytes()?))
    }

    /// 解析原始字节串（字典键等需要原始数据的场景）。
    fn parse_raw_bytes(&mut self) -> Result<Vec<u8>, BencodeError> {
        let len = self.parse_length()?;
        if len > self.input.len().saturating_sub(self.pos) {
            return Err(BencodeError::LengthOverflow {
                offset: self.pos,
                len: len as u64,
                remaining: self.input.len() - self.pos,
            });
        }
        let data = self.input[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(data)
    }

    fn parse_list(&mut self, depth: usize) -> Result<Value, BencodeError> {
        self.pos += 1; // 'l'
        let mut items = Vec::new();
        loop {
            match self.peek() {
                None => return Err(BencodeError::UnexpectedEof { offset: self.pos }),
                Some(b'e') => {
                    self.pos += 1;
                    return Ok(Value::List(items));
                }
                Some(_) => items.push(self.parse_value(depth + 1)?),
            }
        }
    }

    fn parse_dict(&mut self, depth: usize) -> Result<Value, BencodeError> {
        self.pos += 1; // 'd'
        let mut map = BTreeMap::new();
        loop {
            match self.peek() {
                None => return Err(BencodeError::UnexpectedEof { offset: self.pos }),
                Some(b'e') => {
                    self.pos += 1;
                    return Ok(Value::Dict(map));
                }
                Some(b @ b'0'..=b'9') => {
                    let _ = b;
                    let key = self.parse_raw_bytes()?;
                    let val = self.parse_value(depth + 1)?;
                    map.insert(key, val);
                }
                Some(other) => {
                    return Err(BencodeError::InvalidToken {
                        offset: self.pos,
                        byte: other,
                    })
                }
            }
        }
    }
}

/// 便捷构造器（测试与上层解析使用）。
pub fn int(v: i64) -> Value {
    Value::Int(v)
}
pub fn bytes(v: impl Into<Vec<u8>>) -> Value {
    Value::Bytes(v.into())
}
pub fn list(items: Vec<Value>) -> Value {
    Value::List(items)
}
pub fn dict(map: BTreeMap<Vec<u8>, Value>) -> Value {
    Value::Dict(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn decode_simple_values() {
        assert_eq!(decode(b"i42e").unwrap(), Value::Int(42));
        assert_eq!(decode(b"i-17e").unwrap(), Value::Int(-17));
        assert_eq!(decode(b"i0e").unwrap(), Value::Int(0));
        assert_eq!(decode(b"4:spam").unwrap(), Value::Bytes(b("spam")));
        assert_eq!(decode(b"0:").unwrap(), Value::Bytes(vec![]));
        assert_eq!(decode(b"le").unwrap(), Value::List(vec![]));
        assert_eq!(
            decode(b"li1ei2ee").unwrap(),
            Value::List(vec![int(1), int(2)])
        );
        assert_eq!(
            decode(b"d3:bar4:spam3:fooi42ee").unwrap(),
            Value::Dict(BTreeMap::from([
                (b("bar"), bytes(b("spam"))),
                (b("foo"), int(42)),
            ]))
        );
    }

    #[test]
    fn decode_rejects_malformed() {
        assert!(decode(b"i-0e").is_err());
        assert!(decode(b"i03e").is_err());
        assert!(decode(b"i").is_err());
        assert!(decode(b"i12").is_err());
        assert!(decode(b"5:abc").is_err());
        assert!(decode(b"01:a").is_err());
        assert!(decode(b"3:ab").is_err());
        assert!(decode(b"d3:bar4:spam").is_err());
        assert!(decode(b"x").is_err());
        assert!(decode(b"i42ei0e").is_err()); // 尾随数据
        assert!(decode(b"di1e").is_err()); // 字典键非字符串
        assert!(decode(b"l").is_err());
    }

    #[test]
    fn encode_roundtrip() {
        let cases: Vec<Value> = vec![
            Value::Int(-1),
            Value::Int(0),
            Value::Int(i64::MAX),
            Value::Bytes(b("")),
            Value::Bytes(b("hello world")),
            Value::List(vec![int(1), bytes("ab"), Value::List(vec![int(2)])]),
            Value::Dict(BTreeMap::from([
                (b("z").to_vec(), int(1)),
                (b("a").to_vec(), bytes("v")),
                (
                    b("m").to_vec(),
                    Value::Dict(BTreeMap::from([(b("k").to_vec(), int(9))])),
                ),
            ])),
        ];
        for v in cases {
            let enc = encode(&v);
            assert_eq!(decode(&enc).unwrap(), v, "roundtrip 失败: {v:?}");
        }
    }

    #[test]
    fn dict_keys_are_sorted_in_encoding() {
        let v = Value::Dict(BTreeMap::from([
            (b("z").to_vec(), int(1)),
            (b("a").to_vec(), int(2)),
        ]));
        // 键按字节序输出（BEP 3 要求）
        assert_eq!(encode(&v), b"d1:ai2e1:zi1ee");
    }

    #[test]
    fn decode_prefix_stops_at_first_value() {
        let (v, n) = decode_prefix(b"i1ei2e").unwrap();
        assert_eq!(v, Value::Int(1));
        assert_eq!(n, 3);
    }

    #[test]
    fn deep_nesting_rejected() {
        let depth = MAX_DEPTH + 2;
        let mut buf = Vec::new();
        buf.extend(std::iter::repeat_n(b'l', depth));
        buf.push(b'i');
        buf.push(b'1');
        buf.push(b'e');
        buf.extend(std::iter::repeat_n(b'e', depth));
        assert!(decode(&buf).is_err());
    }

    proptest::proptest! {
        #[test]
        fn proptest_roundtrip_arbitrary(
            ints in proptest::collection::vec(proptest::num::i64::ANY, 0..8),
            strs in proptest::collection::vec(proptest::collection::vec(proptest::num::u8::ANY, 0..32), 0..8),
        ) {
            let v = Value::List(ints.into_iter().map(Value::Int).collect());
            assert_eq!(decode(&encode(&v)).unwrap(), v);
            let v2 = Value::List(strs.into_iter().map(Value::Bytes).collect());
            assert_eq!(decode(&encode(&v2)).unwrap(), v2);
        }
    }
}
