use color_eyre::eyre::{eyre, Report};

#[derive(Debug, Clone)]
pub enum Op {
    Get(usize, String),
    Update(usize, String, String),
}

impl Op {
    pub fn client_id(&self) -> usize {
        match self {
            Op::Get(i, _) | Op::Update(i, _, _) => *i,
        }
    }

    pub fn key(&self) -> &str {
        match self {
            Op::Get(_, k) | Op::Update(_, k, _) => k,
        }
    }

    pub async fn exec(
        self,
        cl: &kvstore::KvClient<
            impl bertha::ChunnelConnection<Data = kvstore::Msg> + Send + Sync + 'static,
        >,
    ) -> Result<Option<String>, Report> {
        match self {
            Op::Get(_, k) => cl.get(k).await,
            Op::Update(_, k, v) => cl.update(k, v).await,
        }
    }
}

impl std::str::FromStr for Op {
    type Err = Report;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let sp: Vec<&str> = s.split_whitespace().collect();
        Ok(if sp.len() == 3 && sp[1] == "GET" {
            Op::Get(sp[0].parse()?, sp[2].into())
        } else if sp.len() == 4 && sp[1] == "UPDATE" {
            Op::Update(sp[0].parse()?, sp[2].into(), sp[3].into())
        } else {
            return Err(eyre!("Invalid line: {:?}", s));
        })
    }
}

pub fn ops(f: std::path::PathBuf) -> Result<Vec<Op>, Report> {
    use std::io::BufRead;
    let f = std::fs::File::open(f)?;
    let f = std::io::BufReader::new(f);
    Ok(f.lines().filter_map(|l| l.ok()?.parse().ok()).collect())
}

// TODO invoke ycsbc-mock
//pub fn generate(specfile: impl AsRef<std::path::Path>) {
//    // hardcode workload b for now
//    let out = std::process::Command::new("./ycsbc-mock/ycsbc")
//        .args(&[
//            "-db",
//            "mock",
//            "-threads",
//            "1",
//            "-P",
//            specfile.as_ref().to_str().unwrap(),
//        ])
//        .output()
//        .unwrap();
//}
