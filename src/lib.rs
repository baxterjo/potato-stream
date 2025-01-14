pub mod app;
pub mod capture;
pub mod display;
pub mod ditto;
pub mod join_map;

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
