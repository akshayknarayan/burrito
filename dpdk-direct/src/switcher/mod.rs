use ahash::HashMap;
use color_eyre::eyre::eyre;
use color_eyre::Report;
use std::net::SocketAddrV4;
use std::str::FromStr;

/// A single active connection. Used in `DatapathConnectionMigrator`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum ActiveConnection {
    /// A connection bound only locally (i.e. to any remote address)
    UnConnected { local_port: u16 },
    /// A connection bound to a specific remote address (i.e. provided by accept())
    Connected {
        local_port: u16,
        remote_addr: SocketAddrV4,
    },
}

/// Transition connections between datapaths.
///
/// Datapaths implement this trait. Migration between datapaths involves tearing down the first
/// datapath and starting the second. When starting the second, we need to migrate the connection
/// state -- i.e., the active connections and their associated plumbing -- to the new datapath. So
/// the sequence of operations is:
///
/// 1. Get the active connections from OldDatapath
/// (downtime starts)
/// 2. Tear down OldDatapath.
/// 3. Start up NewDatapath
/// 4. Load connection state from (1) into NewDatapath
/// (downtime ends)
pub trait DatapathConnectionMigrator {
    type Conn;
    type Error;

    fn shut_down(&mut self) -> Result<(), Report>;

    /// Construct connection state (and connection objects) corresponding to the provided set of
    /// `ActiveConnection`s.
    fn load_connections(
        &mut self,
        conns: Vec<ActiveConnection>,
    ) -> Result<HashMap<ActiveConnection, Self::Conn>, Self::Error>;
}

#[derive(Clone, Copy, Debug)]
pub enum DpdkDatapathChoice {
    Thread,
    Inline { num_threads: usize },
}

impl FromStr for DpdkDatapathChoice {
    type Err = Report;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let l = s.to_lowercase();
        match l.chars().next().ok_or(eyre!("got empty string"))? {
            't' => Ok(DpdkDatapathChoice::Thread),
            'i' => {
                let parts: Vec<&str> = l.split(':').collect();
                if parts.len() == 1 {
                    Ok(DpdkDatapathChoice::Inline { num_threads: 0 })
                } else if parts.len() == 2 {
                    let num_threads = parts[1].parse()?;
                    Ok(DpdkDatapathChoice::Inline { num_threads })
                } else {
                    Err(eyre!("unknown specifier {:?}", s))
                }
            }
            x => Err(eyre!("unknown specifier {:?}", x)),
        }
    }
}
