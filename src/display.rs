use anyhow::Result;
use opencv::highgui;
use opencv::prelude::*;
use tokio::sync::watch;
use tracing::info;

pub async fn start_display(mut frame_rx: watch::Receiver<Mat>) -> Result<()> {
    // Open a GUI window
    highgui::named_window("window", highgui::WINDOW_FULLSCREEN)?;

    loop {
        let _ = frame_rx.changed().await;
        let frame = { frame_rx.borrow_and_update().clone() };

        highgui::imshow("window", &frame)?;
        if highgui::poll_key()? != -1 {
            info!("Key pressed, closing window");
            return Ok(());
        }
    }
}
