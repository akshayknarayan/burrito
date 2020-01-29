fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("rerun-if-changed=./proto/ping.proto");
    tonic_build::compile_protos("./proto/ping.proto")?;
    Ok(())
}
