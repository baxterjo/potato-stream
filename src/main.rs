use potato_stream::app::start_app;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    start_app().await.unwrap();
}
