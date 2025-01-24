use anyhow::Result;
use dittolive_ditto::{
    experimental::{bus::Reliability, peer_pubkey::PeerPubkey},
    Ditto,
};
use opencv::imgcodecs::imdecode;
use opencv::{imgcodecs::ImreadModes, prelude::*};
use tokio::sync::{mpsc, watch};

pub async fn start_stream_client(
    ditto: Ditto,
    peer: PeerPubkey,
    frame_tx: watch::Sender<Mat>,
) -> Result<()> {
    let bus = ditto.bus();
    let mut stream = bus
        .connect(peer, "potatostream")
        .reliability(Reliability::Unreliable)
        .on_receive_factory(mpsc::unbounded_channel)
        .finish_async()
        .await
        .expect("Unable to open stream client");
    while let Some(inbound) = stream.recv().await {
        let message = inbound.payload();
        if let Ok(mat) = imdecode(&&message[..], ImreadModes::IMREAD_UNCHANGED as i32) {
            frame_tx.send_replace(mat);
        }
    }

    Ok(())
}

// async fn handle_stream_recv()
