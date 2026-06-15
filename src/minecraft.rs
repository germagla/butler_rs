use crate::config::Config;
use anyhow::{Context, Result, anyhow};
use regex::Regex;
use serde_json::Value;
use std::{collections::HashSet, fs, net::SocketAddr, sync::OnceLock, time::Duration};
use tokio::{net::UdpSocket, time::timeout};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerStatus {
    Offline,
    Unreachable {
        reason: String,
    },
    Queued,
    Starting,
    Preparing,
    Loading,
    Online {
        online: u32,
        max: u32,
        players: Vec<String>,
    },
}

impl ServerStatus {
    pub fn is_offline_like(&self) -> bool {
        matches!(self, Self::Offline | Self::Unreachable { .. })
    }
}

impl std::fmt::Display for ServerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Offline => write!(f, "Offline"),
            Self::Unreachable { reason } => write!(f, "Offline ({reason})"),
            Self::Queued => write!(f, "In Queue"),
            Self::Starting => write!(f, "Starting"),
            Self::Preparing => write!(f, "Preparing"),
            Self::Loading => write!(f, "Loading"),
            Self::Online {
                online,
                max,
                players,
            } => {
                let player_list = if players.is_empty() {
                    "None".to_string()
                } else {
                    players.join(", ")
                };
                write!(f, "Online ({online}/{max} players)\nPlayers: {player_list}")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerAddress {
    pub original: String,
    pub host: String,
    pub explicit_port: Option<u16>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SrvRecord {
    pub priority: u16,
    pub weight: u16,
    pub port: u16,
    pub target: String,
}

pub async fn get_configured_status(config: &Config) -> Result<ServerStatus> {
    get_status_for_addr(&config.minecraft_server_addr).await
}

pub async fn get_status_for_addr(addr: &str) -> Result<ServerStatus> {
    let parsed = parse_address(addr)?;
    let mut attempts = Vec::new();
    let should_lookup_srv = parsed.explicit_port.is_none() || should_prefer_srv(&parsed);
    let (srv_record, srv_error) = if should_lookup_srv {
        match lookup_minecraft_srv(&parsed.host).await {
            Ok(record) => (record, None),
            Err(error) => (None, Some(error.to_string())),
        }
    } else {
        (None, None)
    };

    if should_prefer_srv(&parsed) {
        if let Some(srv) = srv_record {
            attempts.push((srv.target.clone(), srv.port, parsed.host.clone()));
            attempts.push((srv.target.clone(), srv.port, srv.target));
        }
        if let Some(port) = parsed.explicit_port {
            attempts.push((parsed.host.clone(), port, parsed.host.clone()));
        }
    } else if let Some(port) = parsed.explicit_port {
        attempts.push((parsed.host.clone(), port, parsed.host.clone()));
    } else if let Some(srv) = srv_record {
        attempts.push((srv.target.clone(), srv.port, parsed.host.clone()));
        attempts.push((srv.target.clone(), srv.port, srv.target));
    } else {
        attempts.push((parsed.host.clone(), 25565, parsed.host.clone()));
    }

    let mut last_error = srv_error;
    let mut seen = HashSet::new();
    let mut index = 0;
    while index < attempts.len() {
        let (connect_host, port, handshake_host) = attempts[index].clone();
        index += 1;

        if !seen.insert((connect_host.clone(), port, handshake_host.clone())) {
            continue;
        }

        match ping_once(&connect_host, port, &handshake_host).await {
            Ok(PingResult::Status(status)) => return Ok(status),
            Ok(PingResult::Redirect { host, port }) => {
                attempts.push((host.clone(), port, host));
            }
            Err(error) => last_error = Some(error.to_string()),
        }
    }

    Ok(ServerStatus::Unreachable {
        reason: last_error.unwrap_or_else(|| "Unreachable".to_string()),
    })
}

fn should_prefer_srv(address: &ServerAddress) -> bool {
    address.explicit_port == Some(25565)
        && address.host.to_ascii_lowercase().ends_with(".aternos.me")
}

pub fn parse_address(addr: &str) -> Result<ServerAddress> {
    let trimmed = addr.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("Minecraft server address must not be empty"));
    }

    if let Some(stripped) = trimmed.strip_prefix('[') {
        let (host, rest) = stripped
            .split_once(']')
            .ok_or_else(|| anyhow!("Bracketed IPv6 address is missing closing ']'"))?;
        validate_host(host)?;

        let explicit_port = if rest.is_empty() {
            None
        } else if let Some(port) = rest.strip_prefix(':') {
            Some(
                port.parse::<u16>()
                    .with_context(|| format!("Invalid Minecraft server port in {addr}"))?,
            )
        } else {
            return Err(anyhow!("Unexpected text after bracketed IPv6 address"));
        };

        return Ok(ServerAddress {
            original: trimmed.to_string(),
            host: host.to_string(),
            explicit_port,
        });
    }

    if let Some((host, port)) = trimmed.rsplit_once(':')
        && !host.contains(':')
    {
        validate_host(host)?;
        let port = port
            .parse::<u16>()
            .with_context(|| format!("Invalid Minecraft server port in {addr}"))?;
        return Ok(ServerAddress {
            original: trimmed.to_string(),
            host: host.to_string(),
            explicit_port: Some(port),
        });
    }

    validate_host(trimmed)?;

    Ok(ServerAddress {
        original: trimmed.to_string(),
        host: trimmed.to_string(),
        explicit_port: None,
    })
}

fn validate_host(host: &str) -> Result<()> {
    if host.trim().is_empty() {
        return Err(anyhow!("Minecraft server host must not be empty"));
    }
    if host.chars().any(char::is_whitespace) {
        return Err(anyhow!("Minecraft server host must not contain whitespace"));
    }
    Ok(())
}

enum PingResult {
    Status(ServerStatus),
    Redirect { host: String, port: u16 },
}

async fn ping_once(connect_host: &str, port: u16, handshake_host: &str) -> Result<PingResult> {
    let response = timeout(Duration::from_secs(5), async {
        let mut stream = tokio::net::TcpStream::connect((connect_host, port)).await?;
        let response = craftping::tokio::ping(&mut stream, handshake_host, port).await?;
        Ok::<craftping::Response, anyhow::Error>(response)
    })
    .await
    .context("Minecraft ping timed out")??;

    Ok(classify_response(response))
}

fn classify_response(response: craftping::Response) -> PingResult {
    let description_text = flatten_description(&response.description);
    let clean_description = strip_minecraft_color_codes(&description_text);
    let lower_version = response.version.to_lowercase();
    let lower_desc = clean_description.to_lowercase();

    if let Some((host, port)) = extract_redirect(&clean_description) {
        return PingResult::Redirect { host, port };
    }

    if lower_version.contains("offline") || lower_desc.contains("offline") {
        return PingResult::Status(ServerStatus::Offline);
    }
    if lower_version.contains("starting") || lower_desc.contains("starting") {
        return PingResult::Status(ServerStatus::Starting);
    }
    if lower_version.contains("loading") || lower_desc.contains("loading") {
        return PingResult::Status(ServerStatus::Loading);
    }
    if lower_version.contains("queue") || lower_desc.contains("queue") {
        return PingResult::Status(ServerStatus::Queued);
    }
    if lower_version.contains("preparing") || lower_desc.contains("preparing") {
        return PingResult::Status(ServerStatus::Preparing);
    }

    let players = response
        .sample
        .unwrap_or_default()
        .iter()
        .map(|player| player.name.clone())
        .collect();

    PingResult::Status(ServerStatus::Online {
        online: response.online_players as u32,
        max: response.max_players as u32,
        players,
    })
}

fn flatten_description(value: &Option<Value>) -> String {
    match value {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Object(map)) => {
            let mut text = String::new();
            if let Some(Value::String(part)) = map.get("text") {
                text.push_str(part);
            }
            if let Some(Value::Array(parts)) = map.get("extra") {
                for part in parts {
                    text.push_str(&flatten_description(&Some(part.clone())));
                }
            }
            if text.is_empty() {
                serde_json::to_string(value).unwrap_or_default()
            } else {
                text
            }
        }
        Some(other) => serde_json::to_string(other).unwrap_or_default(),
        None => String::new(),
    }
}

fn strip_minecraft_color_codes(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if ch == '§' {
            chars.next();
        } else {
            output.push(ch);
        }
    }
    output
}

fn extract_redirect(text: &str) -> Option<(String, u16)> {
    static HOST_PORT_RE: OnceLock<Regex> = OnceLock::new();
    let re = HOST_PORT_RE.get_or_init(|| {
        Regex::new(r"(?i)\b([a-z0-9.-]+\.[a-z]{2,}):(\d{1,5})\b")
            .expect("redirect host:port regex must compile")
    });

    re.captures_iter(text).find_map(|captures| {
        let host = captures.get(1)?.as_str().to_string();
        let port = captures.get(2)?.as_str().parse::<u16>().ok()?;
        Some((host, port))
    })
}

async fn lookup_minecraft_srv(host: &str) -> Result<Option<SrvRecord>> {
    let query_name = format!("_minecraft._tcp.{host}");
    let resolver = system_resolver()
        .or_else(|| "1.1.1.1:53".parse().ok())
        .context("Could not determine a DNS resolver for Minecraft SRV lookup")?;

    let query_id = rand::random::<u16>();
    let packet = build_srv_query(query_id, &query_name)?;
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.send_to(&packet, resolver).await?;

    let mut buf = [0_u8; 1500];
    let (len, _) = timeout(Duration::from_secs(3), socket.recv_from(&mut buf)).await??;
    parse_srv_response(query_id, &buf[..len])
}

fn system_resolver() -> Option<SocketAddr> {
    let resolv_conf = fs::read_to_string("/etc/resolv.conf").ok()?;
    resolv_conf.lines().map(str::trim).find_map(|line| {
        line.strip_prefix("nameserver")
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|ip| format!("{ip}:53").parse().ok())
    })
}

fn build_srv_query(query_id: u16, name: &str) -> Result<Vec<u8>> {
    let mut packet = Vec::new();
    packet.extend_from_slice(&query_id.to_be_bytes());
    packet.extend_from_slice(&0x0100_u16.to_be_bytes());
    packet.extend_from_slice(&1_u16.to_be_bytes());
    packet.extend_from_slice(&0_u16.to_be_bytes());
    packet.extend_from_slice(&0_u16.to_be_bytes());
    packet.extend_from_slice(&0_u16.to_be_bytes());

    for label in name.trim_end_matches('.').split('.') {
        if label.len() > 63 {
            return Err(anyhow!("DNS label is too long in {name}"));
        }
        packet.push(label.len() as u8);
        packet.extend_from_slice(label.as_bytes());
    }
    packet.push(0);
    packet.extend_from_slice(&33_u16.to_be_bytes());
    packet.extend_from_slice(&1_u16.to_be_bytes());
    Ok(packet)
}

fn parse_srv_response(query_id: u16, packet: &[u8]) -> Result<Option<SrvRecord>> {
    if packet.len() < 12 {
        return Err(anyhow!("DNS response too short"));
    }
    if u16::from_be_bytes([packet[0], packet[1]]) != query_id {
        return Err(anyhow!("DNS response ID did not match request"));
    }

    let question_count = read_u16(packet, 4)?;
    let answer_count = read_u16(packet, 6)?;
    let mut offset = 12;

    for _ in 0..question_count {
        let (_, next) = read_dns_name(packet, offset)?;
        if next + 4 > packet.len() {
            return Err(anyhow!("DNS question is truncated"));
        }
        offset = next + 4;
    }

    let mut records = Vec::new();
    for _ in 0..answer_count {
        let (_, next) = read_dns_name(packet, offset)?;
        offset = next;
        let record_type = read_u16(packet, offset)?;
        let class = read_u16(packet, offset + 2)?;
        let rdlen = read_u16(packet, offset + 8)? as usize;
        offset += 10;
        if offset + rdlen > packet.len() {
            return Err(anyhow!("DNS response record is truncated"));
        }

        if record_type == 33 && class == 1 && rdlen >= 7 {
            let priority = read_u16(packet, offset)?;
            let weight = read_u16(packet, offset + 2)?;
            let port = read_u16(packet, offset + 4)?;
            let (target, _) = read_dns_name(packet, offset + 6)?;
            records.push(SrvRecord {
                priority,
                weight,
                port,
                target: target.trim_end_matches('.').to_string(),
            });
        }

        offset += rdlen;
    }

    records.sort_by_key(|record| (record.priority, std::cmp::Reverse(record.weight)));
    Ok(records.into_iter().next())
}

fn read_u16(packet: &[u8], offset: usize) -> Result<u16> {
    if offset + 2 > packet.len() {
        return Err(anyhow!("DNS response is truncated"));
    }
    Ok(u16::from_be_bytes([packet[offset], packet[offset + 1]]))
}

fn read_dns_name(packet: &[u8], mut offset: usize) -> Result<(String, usize)> {
    let mut labels = Vec::new();
    let mut jumped = false;
    let mut next_offset = offset;
    let mut jumps = 0;

    loop {
        if offset >= packet.len() {
            return Err(anyhow!("DNS name is truncated"));
        }
        let len = packet[offset];
        if len & 0b1100_0000 == 0b1100_0000 {
            if offset + 1 >= packet.len() {
                return Err(anyhow!("DNS compression pointer is truncated"));
            }
            let pointer = (((len & 0b0011_1111) as usize) << 8) | packet[offset + 1] as usize;
            if !jumped {
                next_offset = offset + 2;
            }
            jumped = true;
            offset = pointer;
            jumps += 1;
            if jumps > 8 {
                return Err(anyhow!("DNS compression pointer loop detected"));
            }
            continue;
        }
        if len & 0b1100_0000 != 0 {
            return Err(anyhow!("DNS label uses reserved length bits"));
        }
        if len == 0 {
            if !jumped {
                next_offset = offset + 1;
            }
            break;
        }

        offset += 1;
        let end = offset + len as usize;
        if end > packet.len() {
            return Err(anyhow!("DNS label is truncated"));
        }
        labels.push(String::from_utf8_lossy(&packet[offset..end]).to_string());
        offset = end;
        if !jumped {
            next_offset = offset;
        }
    }

    Ok((labels.join("."), next_offset))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[test]
    fn parses_explicit_port_without_stripping_it() {
        let parsed = parse_address("example.aternos.me:49123").unwrap();
        assert_eq!(parsed.host, "example.aternos.me");
        assert_eq!(parsed.explicit_port, Some(49123));
    }

    #[test]
    fn parses_host_without_port() {
        let parsed = parse_address("example.aternos.me").unwrap();
        assert_eq!(parsed.host, "example.aternos.me");
        assert_eq!(parsed.explicit_port, None);
    }

    #[test]
    fn parses_bracketed_ipv6_with_port() {
        let parsed = parse_address("[::1]:25565").unwrap();
        assert_eq!(parsed.host, "::1");
        assert_eq!(parsed.explicit_port, Some(25565));
    }

    #[test]
    fn rejects_malformed_addresses() {
        assert!(parse_address("").is_err());
        assert!(parse_address(":25565").is_err());
        assert!(parse_address("example.com:notaport").is_err());
        assert!(parse_address("bad host.example:25565").is_err());
        assert!(parse_address("[::1").is_err());
    }

    #[test]
    fn strips_minecraft_color_codes() {
        assert_eq!(strip_minecraft_color_codes("§aOnline §cNow"), "Online Now");
    }

    #[test]
    fn extracts_aternos_redirect_from_motd() {
        assert_eq!(
            extract_redirect("Connect to minecrafteruni.aternos.me:41801"),
            Some(("minecrafteruni.aternos.me".to_string(), 41801))
        );
    }

    #[test]
    fn skips_invalid_redirect_ports() {
        assert_eq!(
            extract_redirect("bad.example:99999 then good.aternos.me:41801"),
            Some(("good.aternos.me".to_string(), 41801))
        );
    }

    #[test]
    fn prefers_srv_for_aternos_default_port() {
        let parsed = parse_address("MinecrafterUni.aternos.me:25565").unwrap();
        assert!(should_prefer_srv(&parsed));
    }

    #[test]
    fn does_not_prefer_srv_for_aternos_non_default_port() {
        let parsed = parse_address("MinecrafterUni.aternos.me:41801").unwrap();
        assert!(!should_prefer_srv(&parsed));
    }

    #[test]
    fn flattens_json_description_extra_parts() {
        let description = json!({
            "text": "§aStarting ",
            "extra": [
                { "text": "soon" },
                "!"
            ]
        });

        assert_eq!(
            strip_minecraft_color_codes(&flatten_description(&Some(description))),
            "Starting soon!"
        );
    }

    #[test]
    fn classifies_mocked_transitional_response() {
        let response = mock_response("Paper", json!("§6Preparing server"), 0, 20, Value::Null);
        assert!(matches!(
            classify_response(response),
            PingResult::Status(ServerStatus::Preparing)
        ));
    }

    #[test]
    fn classifies_mocked_online_response_with_players() {
        let response = mock_response(
            "Paper",
            json!("Welcome"),
            1,
            20,
            json!([{ "name": "PlayerOne", "id": "00000000-0000-0000-0000-000000000000" }]),
        );

        match classify_response(response) {
            PingResult::Status(ServerStatus::Online {
                online,
                max,
                players,
            }) => {
                assert_eq!(online, 1);
                assert_eq!(max, 20);
                assert_eq!(players, vec!["PlayerOne"]);
            }
            _ => panic!("expected online status"),
        }
    }

    #[test]
    fn classifies_redirect_before_status_keywords() {
        let response = mock_response(
            "Offline",
            json!("Offline. Connect to good.aternos.me:41801"),
            0,
            20,
            Value::Null,
        );

        match classify_response(response) {
            PingResult::Redirect { host, port } => {
                assert_eq!(host, "good.aternos.me");
                assert_eq!(port, 41801);
            }
            _ => panic!("expected redirect"),
        }
    }

    #[test]
    fn parses_srv_response_and_prefers_higher_weight_for_same_priority() {
        let mut packet = dns_header(0x1234, 1, 2);
        push_dns_name(&mut packet, "_minecraft._tcp.example.com");
        packet.extend_from_slice(&33_u16.to_be_bytes());
        packet.extend_from_slice(&1_u16.to_be_bytes());
        push_srv_answer(&mut packet, 10, 1, 25565, "low.example.com");
        push_srv_answer(&mut packet, 10, 20, 25566, "high.example.com");

        let record = parse_srv_response(0x1234, &packet).unwrap().unwrap();
        assert_eq!(
            record,
            SrvRecord {
                priority: 10,
                weight: 20,
                port: 25566,
                target: "high.example.com".to_string(),
            }
        );
    }

    #[test]
    fn rejects_dns_parser_edge_cases() {
        assert!(parse_srv_response(0x9999, &dns_header(0x1234, 0, 0)).is_err());
        assert!(read_dns_name(&[0x40, 0], 0).is_err());
        assert!(read_dns_name(&[0xC0, 0x00], 0).is_err());

        let mut truncated_question = dns_header(0x1234, 1, 0);
        push_dns_name(&mut truncated_question, "_minecraft._tcp.example.com");
        truncated_question.extend_from_slice(&33_u16.to_be_bytes());
        assert!(parse_srv_response(0x1234, &truncated_question).is_err());
    }

    fn mock_response(
        version: &str,
        description: Value,
        online_players: usize,
        max_players: usize,
        sample: Value,
    ) -> craftping::Response {
        serde_json::from_value(json!({
            "version": version,
            "protocol": 765,
            "enforces_secure_chat": null,
            "previews_chat": null,
            "max_players": max_players,
            "online_players": online_players,
            "sample": sample,
            "description": description,
            "favicon": null,
            "mod_info": null,
            "forge_data": null
        }))
        .unwrap()
    }

    fn dns_header(query_id: u16, question_count: u16, answer_count: u16) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.extend_from_slice(&query_id.to_be_bytes());
        packet.extend_from_slice(&0x8180_u16.to_be_bytes());
        packet.extend_from_slice(&question_count.to_be_bytes());
        packet.extend_from_slice(&answer_count.to_be_bytes());
        packet.extend_from_slice(&0_u16.to_be_bytes());
        packet.extend_from_slice(&0_u16.to_be_bytes());
        packet
    }

    fn push_dns_name(packet: &mut Vec<u8>, name: &str) {
        if name == "." {
            packet.push(0);
            return;
        }

        for label in name.split('.') {
            packet.push(label.len() as u8);
            packet.extend_from_slice(label.as_bytes());
        }
        packet.push(0);
    }

    fn push_srv_answer(packet: &mut Vec<u8>, priority: u16, weight: u16, port: u16, target: &str) {
        packet.extend_from_slice(&[0xC0, 0x0C]);
        packet.extend_from_slice(&33_u16.to_be_bytes());
        packet.extend_from_slice(&1_u16.to_be_bytes());
        packet.extend_from_slice(&0_u32.to_be_bytes());

        let mut rdata = Vec::new();
        rdata.extend_from_slice(&priority.to_be_bytes());
        rdata.extend_from_slice(&weight.to_be_bytes());
        rdata.extend_from_slice(&port.to_be_bytes());
        push_dns_name(&mut rdata, target);

        packet.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
        packet.extend_from_slice(&rdata);
    }
}
