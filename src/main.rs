use clap::Parser;
use potato_stream::{app::start_app, PotatoArgs};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = PotatoArgs::parse();
    start_app(args).await.unwrap();
}
