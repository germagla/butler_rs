use crate::{commands, state::BotState, terminal};

pub type Context<'a> = poise::Context<'a, BotState, anyhow::Error>;

pub fn create_framework(state: BotState) -> poise::Framework<BotState, anyhow::Error> {
    poise::Framework::builder()
        .options(poise::FrameworkOptions {
            commands: vec![
                commands::ping::ping(),
                commands::server::server(),
                commands::bot::bot(),
            ],
            on_error: |error| {
                Box::pin(async move {
                    match error {
                        poise::FrameworkError::Command { error, ctx, .. } => {
                            let guild_name = ctx
                                .guild()
                                .map(|guild| guild.name.clone())
                                .unwrap_or_else(|| "DM".to_string());
                            let channel_name = ctx.guild().and_then(|guild| {
                                guild
                                    .channels
                                    .get(&ctx.channel_id())
                                    .map(|channel| channel.name.clone())
                            });
                            terminal::emit(terminal::line(
                                "FAIL",
                                &format!("/{}", ctx.command().name),
                                &ctx.author().name,
                                &guild_name,
                                channel_name.as_deref(),
                                format!("error {}", terminal::clean(&format!("{error:?}"))),
                            ));
                        }
                        other => {
                            if let Err(error) = poise::builtins::on_error(other).await {
                                terminal::emit(terminal::line(
                                    "FAIL",
                                    "framework",
                                    "",
                                    "",
                                    None,
                                    terminal::clean(&format!("{error:?}")),
                                ));
                            }
                        }
                    }
                })
            },
            ..Default::default()
        })
        .setup(move |ctx, ready, framework| {
            let state = state.clone();
            Box::pin(async move {
                let commands = &framework.options().commands;

                for guild in &ready.guilds {
                    terminal::emit_debug(terminal::line(
                        "SETUP",
                        "register guild commands",
                        "",
                        "",
                        None,
                        guild.id.to_string(),
                    ));
                    poise::builtins::register_in_guild(ctx, commands, guild.id).await?;
                }
                terminal::emit_debug(terminal::line(
                    "SETUP",
                    "guild commands",
                    "",
                    "",
                    None,
                    "complete",
                ));

                poise::builtins::register_globally(ctx, commands).await?;
                terminal::emit_debug(terminal::line(
                    "SETUP",
                    "global commands",
                    "",
                    "",
                    None,
                    "complete",
                ));

                terminal::emit(terminal::ready(&ready.user.name));

                Ok(state)
            })
        })
        .build()
}
