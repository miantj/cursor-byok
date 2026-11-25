//! Clears Cursor's proxy settings after this process exits, including a hard kill.
use std::{
    fs::{File, OpenOptions, TryLockError},
    process::Stdio,
};

use tokio::process::{Child, Command};

use crate::{config, Error, Result};

use super::settings;

const ARG: &str = "--release-cursor-proxy";

/// Runs as the releaser when spawned with [`ARG`]; returns whether the caller should exit.
pub fn release_proxy_if_requested() -> bool {
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref() != Some(std::ffi::OsStr::new(ARG)) {
        return false;
    }
    #[cfg(target_os = "macos")]
    hide_from_dock();
    let result = args
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(|| Error::Config("missing Cursor proxy URL".into()))
        .and_then(|url| wait_until_owner_exits_and_clear(&url));
    if let Err(error) = result {
        eprintln!("failed to release Cursor proxy settings: {error}");
    }
    true
}

pub(super) struct ProxyRelease {
    _ownership: File,
    child: Child,
}

impl ProxyRelease {
    pub(super) async fn disarm(mut self) {
        let _ = self.child.kill().await;
    }
}

pub(super) fn arm(url: &str) -> Result<Option<ProxyRelease>> {
    if cfg!(test) {
        return Ok(None);
    }
    let file = try_lock()?
        .ok_or_else(|| Error::Config("another process is managing Cursor proxy settings".into()))?;
    Ok(Some(ProxyRelease {
        _ownership: file,
        child: spawn_releaser(url)?,
    }))
}

fn wait_until_owner_exits_and_clear(url: &str) -> Result<()> {
    let file = open_lock()?;
    file.lock()?;
    settings::clear_proxy_settings(url)
}

pub(super) fn try_lock() -> Result<Option<File>> {
    let file = open_lock()?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(error)) => Err(error.into()),
    }
}

fn open_lock() -> Result<File> {
    Ok(OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(config::managed_data_dir()?.join("cursor-proxy.lock"))?)
}

fn spawn_releaser(url: &str) -> Result<Child> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .arg(ARG)
        .arg(url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(unix)]
    command.process_group(0);
    #[cfg(windows)]
    {
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }
    Ok(command.spawn()?)
}

#[cfg(target_os = "macos")]
fn hide_from_dock() {
    #[repr(C)]
    struct ProcessSerialNumber {
        high: u32,
        low: u32,
    }
    #[link(name = "ApplicationServices", kind = "framework")]
    extern "C" {
        fn GetCurrentProcess(psn: *mut ProcessSerialNumber) -> i32;
        fn TransformProcessType(psn: *mut ProcessSerialNumber, transform: u32) -> i32;
    }
    const BACKGROUND: u32 = 2;
    unsafe {
        let mut psn = ProcessSerialNumber { high: 0, low: 0 };
        if GetCurrentProcess(&mut psn) == 0 {
            let _ = TransformProcessType(&mut psn, BACKGROUND);
        }
    }
}
