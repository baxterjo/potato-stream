use anyhow::Result;
use dittolive_ditto::{
    experimental::bus::{Reliability, StreamCandidate},
    Ditto,
};
use opencv::{core::Vector, imgcodecs, prelude::*};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

pub async fn start_stream_server(ditto: Ditto, frame_rx: watch::Receiver<Mat>) -> Result<()> {
    let bus = ditto.bus();
    let mut acceptor = bus
        .bind_topic("potatostream")
        .reliability(Reliability::Unreliable)
        .finish(mpsc::unbounded_channel())
        .expect("Unable to bind topic");
    let mut join_set = JoinSet::new();

    loop {
        tokio::select! {
            stream_candidate_opt = acceptor.recv()=>{
                if let Some(stream_candidate) = stream_candidate_opt{
                    join_set.spawn(handle_connection(stream_candidate, frame_rx.clone()));
                }
            }
            join_result_opt = join_set.join_next_with_id() =>{
                if let Some(result) = join_result_opt {
                    result.expect("Join error");
                }else{
                    // TODO: This is inelegant, but ensures speedy first connections. More desirable pattern
                    // would be for a non blocking sleep to occur if join set is empty.
                    if let Some(stream_candidate) = acceptor.recv().await {
                        join_set.spawn(handle_connection(stream_candidate, frame_rx.clone()));
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
    let mut closed = stream.closed();
    let mut buf: Vector<u8> = Vector::new();

    loop {
        tokio::select! {
            _ = &mut closed =>{
                break;
            }
            _ = frame_rx.changed() =>{
                let frame = frame_rx.borrow_and_update();
                buf.clear();
                imgcodecs::imencode(".jpg", &*frame, &mut buf, &Vector::new())
                    .expect("Failed to encode image");
                {
                    stream.message(buf.clone().to_vec()).send();
                };
            }
        }
    }
}
