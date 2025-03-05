use std::time::Duration;

use anyhow::Result;
use dittolive_ditto::{
    experimental::{bus::Reliability, peer_pubkey::PeerPubkey},
    Ditto,
};
use opencv::imgcodecs::imdecode;
use opencv::{imgcodecs::ImreadModes, prelude::*};
use tokio::{
    sync::{mpsc, watch},
    time,
};
use tracing::{debug, error};

pub async fn start_stream_client(
    ditto: Ditto,
    peer: PeerPubkey,
    frame_tx: watch::Sender<Mat>,
) -> Result<()> {
    let bus = ditto.bus();
    let mut stream = bus
        .connect(peer, "potatostream")
        .reliability(Reliability::Reliable)
        .on_receive_factory(mpsc::unbounded_channel)
        .finish_async()
        .await
        .expect("Unable to open stream client");
    let mut interval = time::interval(Duration::from_secs(5));
    let mut bytes_sum: usize = 0;

    loop {
        tokio::select! {
            inbound_opt = stream.recv()=>{
                if let Some(inbound) = inbound_opt {
                    let message = inbound.payload();
                    bytes_sum += message.len();
                    match imdecode(&&message[..], ImreadModes::IMREAD_UNCHANGED as i32) {
                        Ok(mat)=>{ frame_tx.send_replace(mat); },
                        Err(err)=> { error!(%err, "Error decoding frame"); },
                    }
                } else {
                    break;
                }
            }
            _ = interval.tick() =>{
                debug!("Received {} B over the last 5 seconds", bytes_sum);
                bytes_sum = 0;
            }
        }
    }

    Ok(())
}

// async fn handle_stream_recv()
