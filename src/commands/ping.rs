use crate::{framework::Context, terminal};

/// Replies with "Pong!"
// [LEARNING] Command Attribute
// This macro marks the function as a command.
// `slash_command`: Tells Poise to register this as a Discord slash command.
#[poise::command(slash_command)]
pub async fn ping(
    // [LEARNING] Context Argument
    // Every command takes a Context as its first argument.
    // It provides access to the bot's state, the user who ran the command, and methods to reply.
    ctx: Context<'_>,
) -> Result<(), anyhow::Error> {
    // [LEARNING] Sending a Reply
    // `ctx.say` sends a message back to the channel where the command was run.
    // It returns a Future, so we must `.await` it.
    // The `?` operator propagates any error if the message fails to send.
    ctx.say("🏓 Pong!").await?;
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
        "OK",
        "/ping",
        &ctx.author().name,
        &guild_name,
        channel_name.as_deref(),
        "",
    ));

    Ok(())
}
