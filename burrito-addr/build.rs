fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("rerun-if-changed=../proto/burrito.proto");
    tonic_build::compile_protos("../proto/burrito.proto")?;
    Ok(())
}
