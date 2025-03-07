pub mod app;
pub mod join_map;
pub mod stream;
pub mod utils;

#[cfg(feature = "media")]
pub mod media;

use clap::{Parser, Subcommand};

const APP_ID: &str = "316d9de7-20e8-4035-8d55-90e702ba7291";
const OFFLINE_TEST_TOKEN: &str = "o2d1c2VyX2lkdTEwNjY2MzIwMDM3NDExNDA1MDEyN2ZleHBpcnl4GDIwMjUtMDQtMDZUMDY6NTk6NTkuOTk5WmlzaWduYXR1cmV4WGFkbE5wWVNyL2kxMStET1RRZkRDUXN6bG82dzRjK2UyMWV3SDIrenYvSUduTWNDNGJ3Zm1IR1FreWtRVE1LQlp3QWhJNm0rck5qQ0xsR28wblZqNzlnPT0=";

#[derive(Debug, Parser, Clone)]
#[command(version, about)]
pub struct PotatoArgs {
    /// What would you like to do on potatostream?
    #[command(subcommand)]
    command: PotatoCommand,

    /// What is the name of the stream?
    #[arg(short, long)]
    name: String,

    /// If you want to shape the mesh, add a list of connection addresses here.
    #[arg(short, long)]
    connect: Vec<String>,

    /// If you want to shape the mesh, add a listen address here.
    #[arg(short, long)]
    listen: Option<String>,
}

#[derive(Debug, Subcommand, Clone)]
pub enum PotatoCommand {
    /// Stream video
    Stream {
        /// Do you want to display the recorded video on the streaming device?
        #[arg(short, long)]
        loopback: bool,
        /// Frame rate to capture the stream
        #[arg(short, long)]
        framerate: f32,
    },
    /// Watch a streamed video
    Watch,
}
