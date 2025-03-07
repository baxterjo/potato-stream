use std::time::{Duration, Instant};

use anyhow::Result;
use opencv::{prelude::*, videoio};
use tokio::sync::watch;

pub async fn start_capture(fps: f32, frame_tx: watch::Sender<(Mat, Instant)>) -> Result<()> {
    // Open the web-camera (assuming you have one)
    let mut cam = videoio::VideoCapture::new(0, videoio::CAP_ANY)?;
    let start = Instant::now();
    let loop_pause_millis = (1000f32 / fps) as u64;
    loop {
        // TODO: Modify frame in place, this method temporarily creates two frames.
        let mut new_frame = Mat::default();
        cam.read(&mut new_frame)?;
        frame_tx.send_replace((new_frame, start));
        tokio::time::sleep(Duration::from_millis(loop_pause_millis)).await;
    }
}
