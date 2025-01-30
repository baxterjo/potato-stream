use std::ops::Not;

use crate::ditto::shape_mesh;

use crate::ditto::init_ditto;
use crate::PotatoArgs;

use crate::join_map::JoinMap;
use anyhow::Result;
use dittolive_ditto::Ditto;
use tracing::{error, info};

#[cfg(not(feature = "media"))]
use std::future;

pub async fn start_app(args: PotatoArgs) -> Result<()> {
    let builder = tracing_subscriber::fmt()
        .pretty()
        .with_line_number(false)
        .with_file(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env());
    builder
        .try_init()
        .expect("Failed to initialize tracing subscriber.");

    let mut join_map = JoinMap::new();

    let ditto = init_ditto().expect("Failed to init ditto");

    if args.connect.is_empty().not() || args.listen.is_some() {
        shape_mesh(&ditto, &args.connect, &args.listen)?;
    }

    #[cfg(feature = "media")]
    start_media(&mut join_map, ditto, &args)?;

    #[cfg(not(feature = "media"))]
    join_map.spawn(
        "the_neverending_stoorrrrryyyyyyy",
        future::pending::<Result<()>>(),
    );

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

#[cfg(feature = "media")]
fn start_media(
    join_map: &mut JoinMap<anyhow::Result<()>>,
    ditto: Ditto,
    args: &PotatoArgs,
) -> Result<()> {
    use crate::ditto::advertise_stream::advertise_stream;
    use crate::ditto::find_stream::find_stream;
    use crate::ditto::stream_client::start_stream_client;
    use crate::ditto::stream_server::start_stream_server;
    use crate::media::capture::start_capture;
    use crate::media::display::start_display;
    use crate::PotatoCommand;
    use opencv::prelude::*;

    let frame = Mat::default();

    let (frame_tx, frame_rx) = tokio::sync::watch::channel(frame);

    match args.command {
        PotatoCommand::Stream {
            loopback,
            framerate,
        } => {
            advertise_stream(&ditto, args.name.clone()).expect("Failed to advertise stream.");
            join_map.spawn("video_capture", start_capture(framerate, frame_tx));
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

    Ok(())
}
