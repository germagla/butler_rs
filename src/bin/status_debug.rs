#![allow(dead_code)]

//! Developer-only live Minecraft status probe.
//!
//! Usage: `cargo run --bin status_debug -- <host[:port]>`.

#[path = "../config.rs"]
mod config;
#[path = "../minecraft.rs"]
mod minecraft;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Some(addr) = std::env::args().nth(1) else {
        eprintln!("usage: cargo run --bin status_debug -- <host[:port]>");
        std::process::exit(2);
    };
    let status = minecraft::get_status_for_addr(&addr).await?;
    println!("{status}");
    Ok(())
}
