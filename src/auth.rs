use crate::framework::Context;
use anyhow::Result;
use std::collections::HashSet;

const RESTRICTED_COMMAND_MESSAGE: &str =
    "This command is restricted to server administrators and configured bot owners.";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SensitiveCommandAccess {
    Owner,
    GuildAdministrator { guild_id: String },
}

pub async fn require_sensitive_command_access(
    ctx: Context<'_>,
) -> Result<Option<SensitiveCommandAccess>> {
    let user_id = ctx.author().id.to_string();
    let guild_id = ctx.guild_id().map(|id| id.to_string());
    let has_administrator = if guild_id.is_some() {
        author_has_administrator(ctx).await
    } else {
        false
    };

    if let Some(access) = sensitive_command_access_for(
        &ctx.data().config.owner_user_ids,
        &user_id,
        guild_id.as_deref(),
        has_administrator,
    ) {
        return Ok(Some(access));
    }

    ctx.send(
        poise::CreateReply::default()
            .content(RESTRICTED_COMMAND_MESSAGE)
            .ephemeral(true),
    )
    .await?;
    Ok(None)
}

async fn author_has_administrator(ctx: Context<'_>) -> bool {
    let Some(member) = ctx.author_member().await else {
        return false;
    };
    let Some(guild) = ctx.guild() else {
        return false;
    };

    guild.owner_id == member.user.id
        || member.roles.iter().any(|role_id| {
            guild
                .roles
                .get(role_id)
                .map(|role| role.permissions.administrator())
                .unwrap_or(false)
        })
}

#[cfg(test)]
pub fn is_sensitive_command_authorized(
    owner_user_ids: &HashSet<String>,
    user_id: &str,
    in_guild: bool,
    has_administrator: bool,
) -> bool {
    owner_user_ids.contains(user_id) || (in_guild && has_administrator)
}

pub fn sensitive_command_access_for(
    owner_user_ids: &HashSet<String>,
    user_id: &str,
    guild_id: Option<&str>,
    has_administrator: bool,
) -> Option<SensitiveCommandAccess> {
    if owner_user_ids.contains(user_id) {
        return Some(SensitiveCommandAccess::Owner);
    }
    if has_administrator {
        return guild_id.map(|guild_id| SensitiveCommandAccess::GuildAdministrator {
            guild_id: guild_id.to_string(),
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owners(ids: &[&str]) -> HashSet<String> {
        ids.iter().map(|id| (*id).to_string()).collect()
    }

    #[test]
    fn owner_is_authorized_in_guild_without_admin() {
        assert!(is_sensitive_command_authorized(
            &owners(&["42"]),
            "42",
            true,
            false
        ));
        assert_eq!(
            sensitive_command_access_for(&owners(&["42"]), "42", Some("guild-1"), false),
            Some(SensitiveCommandAccess::Owner)
        );
    }

    #[test]
    fn administrator_is_authorized_in_guild() {
        assert!(is_sensitive_command_authorized(
            &owners(&[]),
            "42",
            true,
            true
        ));
        assert_eq!(
            sensitive_command_access_for(&owners(&[]), "42", Some("guild-1"), true),
            Some(SensitiveCommandAccess::GuildAdministrator {
                guild_id: "guild-1".to_string()
            })
        );
    }

    #[test]
    fn non_admin_non_owner_is_denied_in_guild() {
        assert!(!is_sensitive_command_authorized(
            &owners(&["7"]),
            "42",
            true,
            false
        ));
        assert_eq!(
            sensitive_command_access_for(&owners(&["7"]), "42", Some("guild-1"), false),
            None
        );
    }

    #[test]
    fn dm_authorizes_owners_only() {
        assert!(is_sensitive_command_authorized(
            &owners(&["42"]),
            "42",
            false,
            false
        ));
        assert!(!is_sensitive_command_authorized(
            &owners(&[]),
            "42",
            false,
            true
        ));
    }
}
