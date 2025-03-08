use std::{net::SocketAddr, str::FromStr};

use crate::{APP_ID, OFFLINE_TEST_TOKEN};
use anyhow::Result;
use dittolive_ditto::prelude::*;
use pretty_assertions::Comparison;
use tracing::debug;

pub fn init_ditto() -> Result<Ditto> {
    let app_id = AppId::from_str(APP_ID)?;
    let ditto = Ditto::builder()
        .with_temp_dir()
        .with_minimum_log_level(LogLevel::Debug)
        .with_identity(move |ditto_root| OfflinePlayground::new(ditto_root, app_id))?
        .build()?;
    ditto.set_offline_only_license_token(OFFLINE_TEST_TOKEN)?;
    ditto.disable_sync_with_v3()?;

    ditto.start_sync()?;
    Ok(ditto)
}

pub fn shape_mesh(
    ditto: &Ditto,
    connect: &Vec<String>,
    listen: &Option<String>,
) -> anyhow::Result<()> {
    let old_transport = ditto.transport_config();
    let mut new_transport = TransportConfig::new();
    new_transport.peer_to_peer.lan.enabled = true;
    new_transport.peer_to_peer.lan.mdns_enabled = false;
    new_transport.peer_to_peer.lan.multicast_enabled = false;
    for connect_addr in connect {
        // Check if the provided string can be parsed as a TCP IP.
        let _: SocketAddr = connect_addr.parse()?;
        new_transport
            .connect
            .tcp_servers
            .insert(connect_addr.clone());
    }
    if let Some(listen_addr) = listen {
        let parsed_addr: SocketAddr = listen_addr.parse()?;
        new_transport.listen.tcp.enabled = true;
        new_transport.listen.tcp.interface_ip = parsed_addr.ip().to_string();
        new_transport.listen.tcp.port = parsed_addr.port();
    }
    let diff = Comparison::new(&old_transport, &new_transport);
    debug!(%diff, "Setting custom tracing config");
    ditto.set_transport_config(new_transport);

    Ok(())
}
