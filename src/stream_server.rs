use std::collections::HashMap;

use anyhow::Result;
use dittolive_ditto::{
    experimental::bus::{Reliability, StreamCandidate},
    Ditto,
};
use opencv::{core::Vector, imgcodecs, prelude::*};
use tokio::{
    sync::{mpsc, watch},
    task::{AbortHandle, Id, JoinError},
};

pub async fn start_stream_server(ditto: Ditto, frame_rx: watch::Receiver<Mat>) -> Result<()> {
    let bus = ditto.bus().expect("The bus must have been enabled using `DittoBuilder::with_experimental_bus` to use this feature");
    let mut acceptor = bus
        .bind_topic("potato-stream")
        .reliability(Reliability::Unreliable)
        .finish(mpsc::unbounded_channel())
        .expect("Unable to bind topic.");
    let mut join_set = tokio::task::JoinSet::new();
    let mut abort_handles = HashMap::new();

    loop {
        tokio::select! {
            stream_candidate_opt = acceptor.recv()=>{
                if let Some(stream_candidate) = stream_candidate_opt{
                    let handle = join_set.spawn(handle_connection(stream_candidate, frame_rx.clone()));
                    abort_handles.insert(handle.id(), handle);
                }
            }
            join_result_opt = join_set.join_next_with_id() =>{
                if let Some(join_result) = join_result_opt {
                    handle_join_result(join_result, &mut abort_handles).await
                } else {
                    // TODO: This is inelegant, but ensures speedy first connections. More desirable pattern
                    // would be for a non blocking sleep to occur if join set is empty.
                    abort_handles.clear();
                    if let Some(stream_candidate) = acceptor.recv().await {
                        let handle = join_set.spawn(handle_connection(stream_candidate, frame_rx.clone()));
                        abort_handles.insert(handle.id(), handle);
                    }
                }
            }

        }
    }
}

pub async fn handle_connection(
    stream_candidate: StreamCandidate,
    mut frame_rx: watch::Receiver<Mat>,
) {
    let stream = stream_candidate.open_write_only();
    let mut buf: Vector<u8> = Vector::new();

    loop {
        frame_rx.changed().await.expect("Frame sender was dropped");
        let frame = frame_rx.borrow_and_update();
        buf.clear();
        imgcodecs::imencode(".jpg", &*frame, &mut buf, &Vector::new())
            .expect("Failed to encode image");
        {
            stream.message(buf.clone().to_vec()).send();
        };
    }
}

pub async fn handle_join_result(
    result: Result<(Id, ()), JoinError>,
    abort_handles: &mut HashMap<Id, AbortHandle>,
) {
}
