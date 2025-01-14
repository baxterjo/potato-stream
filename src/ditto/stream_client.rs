use anyhow::Result;
use dittolive_ditto::{
    experimental::{bus::Reliability, peer_pubkey::PeerPubkey},
    Ditto,
};
use opencv::prelude::*;
use tokio::sync::{mpsc, watch};

pub async fn start_stream_client(
    ditto: Ditto,
    peer: PeerPubkey,
    frame_tx: watch::Sender<Mat>,
) -> Result<()> {
    let bus = ditto.bus().expect("The bus must have been enabled using `DittoBuilder::with_experimental_bus` to use this feature");
    let mut stream = bus
        .connect(peer, "potato-stream")
        .reliability(Reliability::Unreliable)
        .on_receive_factory(mpsc::unbounded_channel)
        .finish_async()
        .await
        .expect("Unable to open stream client");

    let message = stream.recv().await;
    Ok(())
}

// async fn handle_stream_recv()
