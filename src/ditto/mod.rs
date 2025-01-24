pub mod advertise_stream;
pub mod find_stream;
pub mod stream_client;
pub mod stream_server;

use anyhow::Result;
use dittolive_ditto::prelude::*;
use std::str::FromStr;

const APP_ID: &str = "316d9de7-20e8-4035-8d55-90e702ba7291";
const OFFLINE_TEST_TOKEN: &str = "o2d1c2VyX2lkdTEwNjY2MzIwMDM3NDExNDA1MDEyN2ZleHBpcnl4GDIwMjUtMDItMTRUMDc6NTk6NTkuOTk5WmlzaWduYXR1cmV4WE81VVpVMDBDZFlMUlVwT3k4WThQWm9tTHdnYmU2Ujd1c1kxTUd3NkhHc0Z2Wmx3RGJsWXg4eDhmL2dLVWZRZm1nMWxLOEZTZ1ZVYkJLM09qUGo2SFFRPT0=";

pub fn init_ditto() -> Result<Ditto> {
    let app_id = AppId::from_str(APP_ID)?;
    let ditto = Ditto::builder()
        .with_temp_dir()
        .with_minimum_log_level(LogLevel::Warning)
        .with_identity(move |ditto_root| OfflinePlayground::new(ditto_root, app_id))?
        .build()?;
    ditto.set_offline_only_license_token(OFFLINE_TEST_TOKEN)?;
    ditto.disable_sync_with_v3()?;

    ditto.start_sync()?;
    Ok(ditto)
}
