use crate::{capture::start_capture, display::start_display};

use anyhow::Result;
use opencv::prelude::*;
use tokio::task::JoinError;
use tracing::error;

pub async fn start_app() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let frame = Mat::default();

    let (frame_tx, frame_rx) = tokio::sync::watch::channel(frame);
    let cap_handle = tokio::spawn(start_capture(30, frame_tx));
    let display_handle = tokio::spawn(start_display(frame_rx));

    tokio::select! {
        res = cap_handle =>{
            handle_join_result("capture_handle", res);
        },
        res = display_handle =>{
            handle_join_result("display_handle", res);
        }
    }

    Ok(())
}

pub fn handle_join_result(handle_name: &str, res: Result<Result<(), anyhow::Error>, JoinError>) {
    error!(?res, handle_name, "join handle returned unexpectedly");
    res.unwrap().unwrap()
}
