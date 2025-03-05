use std::str::FromStr;
use std::thread::sleep;
use std::time::Duration;

use dittolive_ditto::experimental::peer_pubkey::PeerPubkey;
use dittolive_ditto::Ditto;
use serde_json::Value;
use tracing::debug;

pub fn find_stream(ditto: &Ditto, stream_name: &str) -> PeerPubkey {
    let mut out_opt: Option<PeerPubkey> = None;
    while out_opt.is_none() {
        debug!("Searching for stream server peer");
        sleep(Duration::from_secs(1));
        for peer in ditto.presence().graph().remote_peers {
            let peer_metadata = &peer.peer_metadata;
            if peer_metadata
                .get("potato_stream_server")
                .unwrap_or(&Value::Bool(false))
                .as_bool()
                != Some(true)
            {
                continue;
            }
            if let Some(name) = peer_metadata
                .get("stream_name")
                .unwrap_or(&Value::String(String::new()))
                .as_str()
            {
                if name == stream_name {
                    debug!("Found peer candidate for stream:\n{peer:#?}");
                    out_opt = PeerPubkey::from_str(&peer.peer_key_string).ok();
                }
            }
        }
    }
    out_opt.unwrap()
}
