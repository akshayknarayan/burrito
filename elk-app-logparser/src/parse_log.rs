//! Parse server log lines for Client IP and timestamp.

use std::net::{IpAddr, Ipv4Addr};

use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ParsedLine {
    pub client_ip: IpAddr,
    pub timestamp: DateTime<Utc>,
    pub text: String,
}

const SAMPLE_IPS: [IpAddr; 10] = [
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)),
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 4)),
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)),
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 6)),
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7)),
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 8)),
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 9)),
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 10)),
];

pub fn sample_lines() -> impl Iterator<Item = ParsedLine> {
    let sample_start_time: DateTime<Utc> =
        DateTime::parse_from_rfc2822("Wed, 18 Feb 2015 23:16:09 GMT")
            .unwrap()
            .into();
    (0usize..).map(move |i| {
        let ips_idx = i % SAMPLE_IPS.len();
        let timestamp = sample_start_time + Duration::milliseconds((i * 19) as i64);
        ParsedLine {
            client_ip: SAMPLE_IPS[ips_idx],
            timestamp,
            text: "foo".to_owned(),
        }
    })
}

/// TODO what format are the log lines going to be in?
pub fn parse_lines(_lines: impl Iterator<Item = String>) -> impl Iterator<Item = ParsedLine> {
    std::iter::empty()
}
