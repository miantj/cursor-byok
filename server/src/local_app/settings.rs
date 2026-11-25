//! Integrates local application settings.
use std::{collections::BTreeMap, fs, path::PathBuf};

use serde_json::Value;

use crate::{Error, Result};

const NO_PROXY_KEY: &str = "http.noProxy";
const PROXY_KEY: &str = "http.proxy";

fn managed_values(url: &str) -> [(&'static str, Value); 5] {
    [
        (PROXY_KEY, Value::from(url)),
        ("http.proxyKerberosServicePrincipal", Value::from(url)),
        ("http.proxySupport", Value::from("on")),
        ("cursor.general.disableHttp2", Value::Bool(true)),
        ("http.experimental.systemCertificatesV2", Value::Bool(true)),
    ]
}

fn path() -> Result<PathBuf> {
    let home = dirs::home_dir()
        .ok_or_else(|| Error::Config("cannot resolve user home directory".into()))?;
    match std::env::consts::OS {
        "macos" => Ok(home.join("Library/Application Support/Cursor/User/settings.json")),
        "windows" => Ok(std::env::var_os("APPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join("AppData/Roaming"))
            .join("Cursor/User/settings.json")),
        "linux" => Ok(std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".config"))
            .join("Cursor/User/settings.json")),
        platform => Err(Error::Config(format!(
            "Cursor settings are unsupported on {platform}"
        ))),
    }
}

fn read() -> Result<BTreeMap<String, Value>> {
    let path = path()?;
    let data = match fs::read_to_string(path) {
        Ok(data) => data,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error.into()),
    };
    if data.trim().is_empty() {
        return Ok(BTreeMap::new());
    }
    json5::from_str(&data)
        .map_err(|error| Error::Config(format!("parse Cursor settings JSONC: {error}")))
}

fn write(settings: &BTreeMap<String, Value>) -> Result<()> {
    let path = path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let data = serde_json::to_vec_pretty(settings)?;
    let temp = path.with_extension("json.tmp");
    fs::write(&temp, [data.as_slice(), b"\n"].concat())?;
    fs::rename(temp, path)?;
    Ok(())
}

pub fn write_proxy_settings(proxy_url: &str) -> Result<()> {
    let mut settings = read()?;
    settings.remove(NO_PROXY_KEY);
    settings.extend(managed_values(proxy_url).map(|(key, value)| (key.into(), value)));
    write(&settings)
}

pub fn clear_proxy_settings(url: &str) -> Result<()> {
    let mut settings = read()?;
    if strip_managed_values(&mut settings, url) {
        write(&settings)?;
    }
    Ok(())
}

fn strip_managed_values(settings: &mut BTreeMap<String, Value>, url: &str) -> bool {
    if settings.get(PROXY_KEY).and_then(Value::as_str) != Some(url) {
        return false;
    }
    for (key, value) in managed_values(url) {
        if settings.get(key) == Some(&value) {
            settings.remove(key);
        }
    }
    true
}

pub fn settings_match(proxy_url: &str) -> Result<bool> {
    Ok(has_managed_values(&read()?, proxy_url))
}

fn has_managed_values(settings: &BTreeMap<String, Value>, url: &str) -> bool {
    managed_values(url)
        .iter()
        .all(|(key, value)| settings.get(*key) == Some(value))
}

pub fn clear_stale_managed_settings() -> Result<()> {
    let mut settings = read()?;
    let Some(url) = settings
        .get(PROXY_KEY)
        .and_then(Value::as_str)
        .map(str::to_owned)
    else {
        return Ok(());
    };
    let loopback = url
        .parse::<reqwest::Url>()
        .is_ok_and(|url| matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]")));
    if loopback && has_managed_values(&settings, &url) {
        strip_managed_values(&mut settings, &url);
        write(&settings)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_only_removes_values_still_owned_by_the_proxy() {
        let url = "http://127.0.0.1:9";
        let mut settings =
            BTreeMap::from(managed_values(url).map(|(key, value)| (key.into(), value)));
        settings.insert("editor.fontSize".into(), Value::from(14));
        settings.insert("cursor.general.disableHttp2".into(), Value::Bool(false));
        assert!(!strip_managed_values(&mut settings, "http://127.0.0.1:10"));
        assert_eq!(settings.get(PROXY_KEY), Some(&Value::from(url)));
        assert!(strip_managed_values(&mut settings, url));
        assert_eq!(settings.len(), 2);
        assert_eq!(settings["editor.fontSize"], 14);
        assert_eq!(settings["cursor.general.disableHttp2"], false);
        assert!(!strip_managed_values(&mut settings, url));
    }
}
