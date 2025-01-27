use std::time::Duration;

use anyhow::Result;
use opencv::{prelude::*, videoio};
use tokio::sync::watch;

pub async fn start_capture(fps: u64, frame_tx: watch::Sender<Mat>) -> Result<()> {
    // Open the web-camera (assuming you have one)
    let mut cam = videoio::VideoCapture::new(0, videoio::CAP_ANY)?;
    let loop_pause_millis = 1000 / fps;
    loop {
        // TODO: Modify frame in place, this method temporarily creates two frames.
        let mut new_frame = Mat::default();
        cam.read(&mut new_frame)?;
        frame_tx.send_replace(new_frame);
        tokio::time::sleep(Duration::from_millis(loop_pause_millis)).await;
    }
}
