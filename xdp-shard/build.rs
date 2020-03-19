use std::env;
use std::path::PathBuf;

fn main() {
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    println!("cargo:rerun-if-changed=./src/libbpf");
    std::process::Command::new("make")
        .current_dir("./src/libbpf/src")
        .spawn()
        .expect("libbpf spawn make")
        .wait()
        .expect("libbpf spawn make");

    let libbpf_bindings = bindgen::Builder::default()
        .header("./src/libbpf/src/libbpf.h")
        .whitelist_function("bpf_prog_load_xattr")
        .whitelist_function("bpf_object__find_map_by_name")
        .whitelist_function("bpf_object__find_map_fd_by_name")
        .whitelist_function("bpf_object__unload")
        .whitelist_function("bpf_map__fd")
        .whitelist_function("bpf_map__def")
        .whitelist_function("bpf_set_link_xdp_fd")
        .whitelist_function("libbpf_num_possible_cpus")
        .blacklist_type(r#"u\d+"#)
        .generate()
        .expect("Unable to generate bindings");
    libbpf_bindings
        .write_to_file(out_path.join("libbpf.rs"))
        .expect("Unable to write bindings");

    println!("cargo:rustc-link-search=./src/libbpf/src/");
    println!("cargo:rustc-link-lib=static=bpf");
    println!("cargo:rustc-link-lib=z");
    println!("cargo:rustc-link-lib=elf");

    let bpf_bindings = bindgen::Builder::default()
        .header("./src/libbpf/src/bpf.h")
        .whitelist_function("bpf_map_update_elem")
        .whitelist_function("bpf_map_lookup_elem")
        .whitelist_function("bpf_get_map_by_name")
        .blacklist_type(r#"u\d+"#)
        .generate()
        .expect("Unable to generate bindings");
    bpf_bindings
        .write_to_file(out_path.join("bpf.rs"))
        .expect("Unable to write bindings");

    let if_link_bindings = bindgen::Builder::default()
        .header("./src/libbpf/include/uapi/linux/if_link.h")
        .whitelist_var("XDP_FLAGS_.*")
        .blacklist_type(r#"u\d+"#)
        .generate()
        .expect("Unable to generate bindings");
    if_link_bindings
        .write_to_file(out_path.join("if_link.rs"))
        .expect("Unable to write bindings");

    let if_xdp_bindings = bindgen::Builder::default()
        .header("./src/libbpf/include/uapi/linux/if_xdp.h")
        .derive_default(true)
        .blacklist_type(r#"u\d+"#)
        .generate()
        .expect("Unable to generate bindings");
    if_xdp_bindings
        .write_to_file(out_path.join("if_xdp.rs"))
        .expect("Unable to write bindings");

    let xdp_prog_types_bindings = bindgen::Builder::default()
        .header("./src/xdp_shard.h")
        .derive_default(true)
        .blacklist_type(r#"u\d+"#)
        .whitelist_type(r#"datarec"#)
        .whitelist_type(r#"available_shards"#)
        .generate()
        .expect("Unable to generate bindings");
    xdp_prog_types_bindings
        .write_to_file(out_path.join("xdp_shard.rs"))
        .expect("Unable to write bindings");

    let clang_include = std::process::Command::new("clang")
        .arg("-print-file-name=include")
        .output()
        .expect("clang get include path")
        .stdout;

    let clang_include = std::string::String::from_utf8(clang_include).unwrap();

    println!("cargo:rerun-if-changed=./src/xdp_shard.c");
    println!("cargo:rerun-if-changed=./src/xdp_shard.h");
    if !std::process::Command::new("clang")
        .current_dir("./src")
        .args(&[
            "-nostdinc",
            "-isystem",
            &clang_include,
            "-D__KERNEL__",
            "-D__ASM_SYSREG_H",
            "-I/usr/include",
            "-Werror",
            "-Wall",
            "-gdwarf",
            "-O1",
            "-emit-llvm",
            "-c",
            "xdp_shard.c",
            "-o",
            "xdp_shard.d",
        ])
        .spawn()
        .expect("clang xdp_shard.c")
        .wait()
        .expect("clang xdp_shard.c")
        .success()
    {
        panic!("clang errored");
    }

    if !std::process::Command::new("llc")
        .current_dir("./src")
        .args(&[
            "-march=bpf",
            "-filetype=obj",
            "-o",
            "xdp_shard.o",
            "xdp_shard.d",
        ])
        .spawn()
        .expect("llc xdp_shard.d")
        .wait()
        .expect("llc xdp_shard.d")
        .success()
    {
        panic!("llc errored");
    }

    std::process::Command::new("rm")
        .current_dir("./src")
        .args(&["-f", "xdp_shard.d"])
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

    eprintln!("{:?}", out_path.join("xdp_shard.o"));

    if !std::process::Command::new("mv")
        .arg("src/xdp_shard.o")
        .arg(out_path.join("xdp_shard.o"))
        .spawn()
        .expect("kernel ebpf program move to outdir")
        .wait()
        .expect("kernel ebpf program move to outdir")
        .success()
    {
        panic!("mv errored");
    }
}
