use anyhow::Result;
use dittolive_ditto::Ditto;
use serde_json::json;
pub fn advertise_stream(ditto: &Ditto, stream_name: String) -> Result<()> {
    let peer_data = json!({
        "potato_stream_server":true,
        "stream_name":stream_name
    });
    ditto.presence().set_peer_metadata(&peer_data)?;
    Ok(())
}
