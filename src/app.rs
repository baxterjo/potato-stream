use crate::capture::start_capture;
use crate::display::start_display;
use crate::ditto::find_stream::find_stream;
use crate::ditto::stream_client::start_stream_client;
use crate::ditto::stream_server::start_stream_server;
use crate::ditto::{advertise_stream::advertise_stream, init_ditto};
use crate::PotatoArgs;
use crate::PotatoCommand;

use crate::join_map::JoinMap;
use anyhow::Result;
use opencv::prelude::*;
use tracing::{error, info};

pub async fn start_app(args: PotatoArgs) -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let frame = Mat::default();

    let (frame_tx, frame_rx) = tokio::sync::watch::channel(frame);
    let mut join_map = JoinMap::new();

    let ditto = init_ditto().expect("Failed to init ditto");
    match args.command {
        PotatoCommand::Stream { loopback } => {
            advertise_stream(&ditto, args.name.clone()).expect("Failed to advertise stream.");
            join_map.spawn("video_capture", start_capture(30, frame_tx));
            if loopback {
                join_map.spawn("video_display", start_display(true, frame_rx.clone()));
            }
            join_map.spawn("video_server", start_stream_server(ditto, frame_rx));
        }
        PotatoCommand::Watch => {
            let pub_key = find_stream(&ditto, &args.name);
            join_map.spawn("video_display", start_display(false, frame_rx));
            join_map.spawn(
                "video_client",
                start_stream_client(ditto, pub_key, frame_tx),
            );
        }
    }

    tokio::select! {
        result_opt = join_map.join_next()=>{
            let (name, result) = result_opt.expect("Empty join map was polled!");
            error!(?result, name, "join handle returned unexpectedly");
        }
        _sig = tokio::signal::ctrl_c()=>{
            info!("SIGINT received, exiting...")
        }
    }

    join_map.abort_all();

    while let Some((name, result)) = join_map.join_next().await {
        info!(?result, name, "join handle returned after abort");
    }

    Ok(())
}
