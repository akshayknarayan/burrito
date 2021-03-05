//! Steer messages to the right queue.

use color_eyre::eyre::{bail, eyre, Report};

#[derive(Debug, Clone)]
pub enum QueueAddr {
    Aws(String),
    Azure(String),
    Gcp(String),
}

impl std::str::FromStr for QueueAddr {
    type Err = Report;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let idx = s
            .find(':')
            .ok_or_else(|| eyre!("Malformed QueueAddr: {:?}", s))?;
        let sp = [&s[0..idx], &s[idx + 1..]];
        match sp {
            ["aws", g] | ["AWS", g] => Ok(QueueAddr::Aws(g.to_string())),
            ["az", g] | ["Azure", g] => Ok(QueueAddr::Azure(g.to_string())),
            ["gcp", g] | ["GCP", g] => Ok(QueueAddr::Gcp(g.to_string())),
            _ => bail!("Unkown addr {:?} -> {:?}", s, sp),
        }
    }
}

pub trait SetGroup {
    fn set_group(&mut self, group: String);
}

impl SetGroup for sqs::SqsAddr {
    fn set_group(&mut self, group: String) {
        self.group = Some(group);
    }
}

impl SetGroup for gcp_pubsub::PubSubAddr {
    fn set_group(&mut self, group: String) {
        self.group = Some(group);
    }
}
