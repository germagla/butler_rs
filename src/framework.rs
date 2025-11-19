use crate::commands;
use crate::state::BotState;

pub type Context<'a> = poise::Context<'a, BotState, anyhow::Error>;

pub fn create_framework() -> poise::Framework<BotState, anyhow::Error> {
    poise::Framework::builder()
        .options(poise::FrameworkOptions {
            commands: vec![
                commands::ping::ping(),
                commands::aternos::aternos_start(),
                commands::aternos::aternos_status(),
            ],
            ..Default::default()
        })
        .setup(|ctx, _ready, framework| {
            Box::pin(async move {
                println!("Logged in as {}", _ready.user.name);

                // Register slash commands globally
                poise::builtins::register_globally(ctx, &framework.options().commands).await?;

                Ok(BotState::new())
            })
        })
        .build()
}
