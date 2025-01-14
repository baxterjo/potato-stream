use crate::ditto::stream_server::start_stream_server;
use crate::PotatoCommand;
use crate::{capture::start_capture, display::start_display, PotatoArgs};

use crate::join_map::JoinMap;
use anyhow::Result;
use dittolive_ditto::{AppId, Ditto};
use opencv::prelude::*;
use tracing::{error, info};

pub async fn start_app(args: PotatoArgs) -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let frame = Mat::default();

    let (frame_tx, frame_rx) = tokio::sync::watch::channel(frame);
    let mut join_map = JoinMap::new();

    let ditto = Ditto::new(AppId::generate());
    match args.command {
        PotatoCommand::Stream { loopback } => {
            join_map.spawn("video_capture", start_capture(30, frame_tx));
            if loopback {
                join_map.spawn("video_display", start_display(frame_rx.clone()));
            }
            join_map.spawn("video_server", start_stream_server(ditto, frame_rx));
        }
        PotatoCommand::Watch => {

            join_map.spawn("video_display", start_display(frame_rx));
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
