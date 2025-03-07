use std::time::Instant;

use anyhow::Result;
use opencv::highgui;
use opencv::prelude::*;
use tokio::sync::watch;
use tracing::info;

pub async fn start_display(
    streamer: bool,
    mut frame_rx: watch::Receiver<(Mat, Instant)>,
) -> Result<()> {
    let window_name = if streamer { "streamer" } else { "watcher" };
    // Open a GUI window
    highgui::named_window(&window_name, highgui::WINDOW_FULLSCREEN)?;

    loop {
        let _ = frame_rx.changed().await;
        let frame = { frame_rx.borrow_and_update().clone() };
        highgui::imshow(&window_name, &frame.0)?;
        if highgui::poll_key()? != -1 {
            info!("Key pressed, closing window");
            return Ok(());
        }
    }
}
