//! `sftp://` 地址解析与登录信息。

use percent_encoding::percent_decode_str;
use url::Url;

use crate::SftpError;

/// 解析后的 SFTP 地址。
///
/// 远程路径保留 URL 的百分号解码结果，`~` / `~/...` 原样保留，
/// 由连接建立后按服务器 home 目录展开（需要先执行 realpath(".")）。
#[derive(Clone)]
pub struct SftpUrl {
    /// 用户名（百分号解码后；可能为空）。
    pub user: Option<String>,
    /// URL 内嵌密码（百分号解码后）。
    pub password: Option<String>,
    /// 主机名或 IP（IPv6 不带方括号）。
    pub host: String,
    /// SSH 端口（默认 22）。
    pub port: u16,
    /// 远程路径（百分号解码后的 UTF-8）。
    pub path: String,
    /// 原始地址（日志中必须使用 [`Self::redacted`]）。
    pub raw: String,
}

impl std::fmt::Debug for SftpUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 避免 `{:?}` 把明文密码带进日志
        f.debug_struct("SftpUrl")
            .field("url", &self.redacted())
            .finish()
    }
}

impl SftpUrl {
    pub fn parse(raw: &str) -> Result<Self, SftpError> {
        let raw = raw.trim();
        // 不回显原始地址：解析失败的 URL 里可能带着明文密码。
        let url = Url::parse(raw).map_err(|e| SftpError::Url(e.to_string()))?;
        if !url.scheme().eq_ignore_ascii_case("sftp") {
            return Err(SftpError::Url(format!(
                "不是 sftp:// 地址: {}",
                url.scheme()
            )));
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(SftpError::Url(
                "SFTP 地址不支持查询串或锚点；路径中的 ? / # 请使用百分号编码".into(),
            ));
        }
        // url::Url 的 host_str 对 IPv6 会带方括号，直接拿去建 TCP 连接
        // 会解析失败；Host 枚举给出干净地址。
        let host = match url.host() {
            Some(url::Host::Ipv6(addr)) => addr.to_string(),
            Some(url::Host::Ipv4(addr)) => addr.to_string(),
            Some(url::Host::Domain(d)) if !d.is_empty() => d.to_string(),
            _ => return Err(SftpError::Url("SFTP 地址缺少主机名".into())),
        };
        let user = decode_opt(url.username()).filter(|s| !s.is_empty());
        let password = url.password().map(decode).transpose()?;
        let path = decode(url.path())?;
        if path.is_empty() || path == "/" {
            return Err(SftpError::Url("SFTP 地址缺少远程文件路径".into()));
        }
        if path.ends_with('/') {
            return Err(SftpError::Url(
                "SFTP 地址指向目录；当前只支持单文件下载".into(),
            ));
        }
        Ok(Self {
            user,
            password,
            host,
            port: url.port().unwrap_or(22),
            path,
            raw: raw.to_string(),
        })
    }

    /// 日志/错误信息里使用的脱敏地址（隐藏密码）。
    pub fn redacted(&self) -> String {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        let auth = match &self.user {
            Some(u) if self.password.is_some() => format!("{u}:***@"),
            Some(u) => format!("{u}@"),
            None => String::new(),
        };
        let port = if self.port == 22 {
            String::new()
        } else {
            format!(":{}", self.port)
        };
        format!("sftp://{auth}{host}{port}{}", self.path)
    }

    /// 远程文件在本地使用的默认文件名。
    pub fn filename(&self) -> Result<String, SftpError> {
        let trimmed = self.path.trim_end_matches('/');
        let name = trimmed.rsplit('/').next().unwrap_or("");
        validate_filename(name)
    }

    /// 远程路径是否以 `~` 开头（连接后需要展开）。URL 路径以 `/` 开头，
    /// 因此同时认 `/~/...` 与 `~/...` 两种写法。
    pub fn needs_home_expansion(&self) -> bool {
        tilde_rest(&self.path).is_some()
    }
}

/// 主机名或 IP 是否像一个 SFTP 地址（用于任务种类判定，不做完整解析）。
pub fn is_sftp_url(u: &str) -> bool {
    u.trim()
        .get(..7)
        .is_some_and(|p| p.eq_ignore_ascii_case("sftp://"))
}

/// 用户名/密码选项与 URL 的合成结果。
#[derive(Debug, Clone)]
pub struct Login {
    pub user: String,
    pub password: Option<String>,
}

pub(crate) fn resolve_login(
    url: &SftpUrl,
    user_option: Option<&str>,
    password_option: Option<&str>,
) -> Result<Login, SftpError> {
    let user = user_option
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| url.user.clone())
        .ok_or_else(|| {
            SftpError::Auth(format!(
                "{} 缺少用户名（可写在 URL 里，或用 sftp-user 选项指定）",
                url.redacted()
            ))
        })?;
    // 选项里的空字符串是"显式空密码"，与"未提供"区分。
    let password = password_option
        .map(String::from)
        .or_else(|| url.password.clone());
    Ok(Login { user, password })
}

/// 本地文件名安全校验：SFTP 路径末段已经过 basename 截取，这里只挡
/// 空名与 `.` / `..`（防止把远程目录映射成本地目录本身）。
pub(crate) fn validate_filename(name: &str) -> Result<String, SftpError> {
    if name.is_empty() || name == "." || name == ".." {
        return Err(SftpError::Url(
            "SFTP 地址的远程文件名为空或非法；目录下载不在支持范围内".into(),
        ));
    }
    Ok(name.to_string())
}

/// `/~/a`、`~/a`、`/~`、`~` → `Some("a")` / `Some("")`。
fn tilde_rest(path: &str) -> Option<&str> {
    if let Some(rest) = path.strip_prefix("/~/") {
        Some(rest)
    } else if let Some(rest) = path.strip_prefix("~/") {
        Some(rest)
    } else if path == "/~" || path == "~" {
        Some("")
    } else {
        None
    }
}

/// 连接后把 `~` 展开成服务器 home 目录下的绝对路径。
pub(crate) fn expand_home(path: &str, home: &str) -> Result<String, SftpError> {
    if let Some(rest) = tilde_rest(path) {
        let home = home.trim_end_matches('/');
        return Ok(if rest.is_empty() {
            home.to_string()
        } else {
            format!("{home}/{}", rest.trim_start_matches('/'))
        });
    }
    Ok(path.to_string())
}

fn decode(s: &str) -> Result<String, SftpError> {
    percent_decode_str(s)
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|_| SftpError::Url("SFTP 地址包含非 UTF-8 的百分号编码".into()))
}

fn decode_opt(s: &str) -> Option<String> {
    decode(s).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_user_password_port_and_decodes() {
        let u = SftpUrl::parse("sftp://alice:p%40ss@example.com:2222/var/tmp/a%20b.bin").unwrap();
        assert_eq!(u.user.as_deref(), Some("alice"));
        assert_eq!(u.password.as_deref(), Some("p@ss"));
        assert_eq!(u.host, "example.com");
        assert_eq!(u.port, 2222);
        assert_eq!(u.path, "/var/tmp/a b.bin");
        assert_eq!(u.filename().unwrap(), "a b.bin");
    }

    #[test]
    fn default_port_and_tilde() {
        let u = SftpUrl::parse("sftp://bob@nas/~/media/movie.mkv").unwrap();
        assert_eq!(u.port, 22);
        assert!(u.needs_home_expansion());
        assert_eq!(u.filename().unwrap(), "movie.mkv");
        assert_eq!(
            expand_home(&u.path, "/home/bob").unwrap(),
            "/home/bob/media/movie.mkv"
        );
    }

    #[test]
    fn empty_user_can_come_from_option() {
        let u = SftpUrl::parse("sftp://nas/file.zip").unwrap();
        assert!(u.user.is_none());
        let l = resolve_login(&u, Some("bob"), Some("pw")).unwrap();
        assert_eq!(l.user, "bob");
        assert_eq!(l.password.as_deref(), Some("pw"));
    }

    #[test]
    fn rejects_non_sftp_and_missing_path() {
        assert!(SftpUrl::parse("http://example.com/a").is_err());
        assert!(SftpUrl::parse("sftp://example.com/").is_err());
        assert!(SftpUrl::parse("sftp://example.com/dir/").is_err());
    }

    #[test]
    fn redacts_password() {
        let u = SftpUrl::parse("sftp://a:secret@h:2200/x").unwrap();
        assert_eq!(u.redacted(), "sftp://a:***@h:2200/x");
        assert!(!u.redacted().contains("secret"));
    }

    #[test]
    fn ipv6_host_has_no_brackets() {
        let u = SftpUrl::parse("sftp://user@[::1]:2222/a.bin").unwrap();
        assert_eq!(u.host, "::1");
        assert_eq!(u.port, 2222);
        assert_eq!(u.redacted(), "sftp://user@[::1]:2222/a.bin");
    }

    #[test]
    fn scheme_detection_is_case_insensitive() {
        assert!(is_sftp_url("SFTP://host/a"));
        assert!(!is_sftp_url("ftp://host/a"));
    }
}
