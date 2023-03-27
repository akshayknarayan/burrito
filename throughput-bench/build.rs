use std::path::PathBuf;
fn main() {
    if cfg!(feature = "use_shenango") {
        // the ../../../ is target/(debug|release)/build
        // println!("cargo:rustc-link-arg=-T../../../shenango-chunnel/caladan/base/base.ld");
        let manifest_path: PathBuf = std::env::var("CARGO_MANIFEST_DIR")
            .unwrap()
            .parse()
            .unwrap();
        // manifest_path is now .../burrito/throughput-bench
        let link_script_path = manifest_path
            .parent()
            .unwrap()
            .join("shenango-chunnel/caladan/base/base.ld");
        println!(
            "cargo:rustc-link-arg=-T{}",
            link_script_path.to_str().unwrap()
        );
    }
}
