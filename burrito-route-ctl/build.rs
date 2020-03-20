fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("rerun-if-changed=../proto/burrito.proto");
    tonic_build::compile_protos("../proto/burrito.proto")?;

    use std::path::Path;
    println!("rerun-if-changed=../proto/burrito.fbs");
    flatc_rust::run(flatc_rust::Args {
        inputs: &[Path::new("../proto/burrito.fbs")],
        out_dir: Path::new(&std::env::var("OUT_DIR").unwrap()),
        ..Default::default()
    })?;
    Ok(())
}
