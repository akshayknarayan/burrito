use color_eyre::eyre::{bail, Report};
use std::str::FromStr;

#[derive(Clone, Copy, Debug)]
pub enum Mode {
    BestEffort,
    /// if num_groups = Some(1), all keys are in the same group, so there is total ordering.  
    ///
    /// if num_groups = Some(n > 1), number of client threads = number of ordering groups (measure
    /// effect on throughput).  
    ///
    /// if num_groups = None, each key is in its own ordering group (so there can be is
    /// at-most-once delivery without ordering).
    ///
    /// Some(0) is invalid.
    Ordered {
        num_groups: Option<usize>,
    },
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Mode::BestEffort => f.write_str("BestEffort")?,
            Mode::Ordered {
                num_groups: Some(n),
            } => write!(f, "Ordered:{}", n)?,
            Mode::Ordered { num_groups: None } => f.write_str("AtMostOnce")?,
        };

        Ok(())
    }
}

impl FromStr for Mode {
    type Err = Report;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let sp: Vec<_> = s.split(':').collect();
        match sp.as_slice() {
            ["BestEffort"] | ["besteffort"] | ["be"] => Ok(Mode::BestEffort),
            ["Ordered", g] | ["ordered", g] | ["ord", g] => {
                let g: usize = g.parse()?;
                Ok(Mode::Ordered {
                    num_groups: if g == 0 { None } else { Some(g) },
                })
            }
            _ => bail!("Unkown mode {:?}", s),
        }
    }
}
