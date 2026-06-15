#![allow(dead_code)]

#[path = "../config.rs"]
mod config;
#[path = "../minecraft.rs"]
mod minecraft;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "MinecrafterUni.aternos.me:25565".to_string());
    let status = minecraft::get_status_for_addr(&addr).await?;
    println!("{status}");
    Ok(())
}
