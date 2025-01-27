pub mod app;
pub mod ditto;
pub mod join_map;

#[cfg(feature = "media")]
pub mod media;

use clap::{Parser, Subcommand};

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
    },
    /// Watch a streamed video
    Watch,
}
