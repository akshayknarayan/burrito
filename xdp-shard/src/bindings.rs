#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(unused)]
pub mod libbpf {
    // libbpf rust bindings
    include!(concat!(env!("OUT_DIR"), "/libbpf.rs"));
}

#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(unused)]
pub mod bpf {
    // bpf rust bindings
    include!(concat!(env!("OUT_DIR"), "/bpf.rs"));
}

#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(unused)]
pub mod if_link {
    // if_link.h bindings
    // used only for consts
    include!(concat!(env!("OUT_DIR"), "/if_link.rs"));
}

#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
pub mod xdp_shard {
    include!(concat!(env!("OUT_DIR"), "/xdp_shard.rs"));

    pub type AvailableShards = available_shards;
    pub type ShardRules = shard_rules;
    pub type Datarec = datarec;
}
