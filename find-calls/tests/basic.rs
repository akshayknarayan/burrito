use bertha::ChunnelConnection;
use find_calls::*;

#[find_calls]
fn do_chunnel_connection_stuff(cn: impl ChunnelConnection<Data = ()>) {
    cn.send(());
    cn.recv();
}

#[test]
fn main() {
    let cn = bertha::util::NeverCn::default();
    do_chunnel_connection_stuff(cn);
}
