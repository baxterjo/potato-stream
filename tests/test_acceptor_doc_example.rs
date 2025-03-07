use std::time::Duration;

use dittolive_ditto::experimental::bus::{Reliability, SendStatus};
use potato_stream::utils::init_ditto;
#[tokio::test]
pub async fn test_acceptor_doc_example_works_as_written() {
    use tokio::sync::mpsc::unbounded_channel as channel; // (`std::sync::mpsc::channel` would also work)
    let ditto = init_ditto().unwrap();
    let bus = ditto.bus();
    let mut acceptor = bus
        .bind_topic("ping")
        .reliability(Reliability::Reliable)
        .on_receive_factory(channel)
        .finish(channel())
        .expect("Topic should be unique");
    while let Some(mut stream) = acceptor.recv().await {
        tokio::task::spawn(async move {
            while let Some(packet) = stream.recv().await {
                let send_handle = stream.message(packet).send();
                while send_handle.current_status() == SendStatus::Pending {
                    tokio::time::sleep(Duration::from_millis(100)).await
                }
            }
        });
    }
}
