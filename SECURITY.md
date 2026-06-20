# Security

Butler is a personal operations bot that interacts with Discord, Minecraft server status, and browser-backed provider workflows. Treat local configuration and diagnostics as sensitive.

## Secrets

- Never commit `.env`, Discord tokens, Aternos credentials, server IDs, or personal Discord identifiers.
- Use `.env.example` as the tracked template and keep real values only in `.env`.
- Rotate any token or password that may have been committed or included in a shared artifact.

## Runtime Artifacts

Butler can write local run diagnostics under `ARTIFACT_DIR` for troubleshooting. These files may include Discord metadata, local paths, screenshots, and HTML from an authenticated browser session. They are local diagnostics only and are ignored by git.

Defaults keep screenshots available on success and failure, capture HTML only on failure, attach screenshots in Discord when available, persist JSONL events, and redact Discord IDs and display names in those JSONL events. Public `/server start` may post authenticated dashboard screenshots when screenshot attachment is enabled; this is an explicit operational tradeoff. Set `ARTIFACT_CAPTURE=off` to disable local browser artifacts or `BUTLER_PERSIST_RUN_EVENTS=false` to keep step events in memory only.

Restricted diagnostics and run-history commands require a configured bot owner in `BUTLER_OWNER_IDS` or Discord Administrator permissions in the current guild. Owners can inspect all runs; guild Administrators can inspect raw metadata and artifact paths only for same-guild runs. DM-originated run history is owner-only.

## Reporting

This repository is currently a portfolio project. If you find a security issue, open a private channel with the maintainer rather than posting credentials, screenshots, or logs in a public issue.
