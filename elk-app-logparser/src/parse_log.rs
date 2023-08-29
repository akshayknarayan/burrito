//! Parse server log lines for Client IP and timestamp.

use std::net::IpAddr;

use chrono::{DateTime, Utc};

pub struct ParsedLine {
    client_ip: IpAddr,
    timestamp: DateTime<Utc>,
    text: String,
}

/// TODO what format are the log lines going to be in?
pub fn parse_lines(lines: impl Iterator<Item = String>) -> impl Iterator<Item = ParsedLine> {
    std::iter::empty()
}
