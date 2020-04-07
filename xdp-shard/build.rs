use std::env;
use std::path::{Path, PathBuf};

fn main() {
    if cfg!(feature = "ebpf") {
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

        println!("cargo:rustc-link-search=./xdp-shard/src/libbpf/src/");
        println!("cargo:rustc-link-lib=static=bpf");
        println!("cargo:rustc-link-lib=z");
        println!("cargo:rustc-link-lib=elf");

        let bpf_bindings = bindgen::Builder::default()
            .header("./src/libbpf/src/bpf.h")
            .whitelist_function("bpf_map_update_elem")
            .whitelist_function("bpf_map_delete_elem")
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

        println!("cargo:rerun-if-changed=./src/xdp_shard.h");
        let xdp_prog_types_bindings = bindgen::Builder::default()
            .header("./src/xdp_shard.h")
            .derive_default(true)
            .blacklist_type(r#"u\d+"#)
            .whitelist_type(r#"datarec"#)
            .whitelist_var(r#"NUM_PORTS"#)
            .whitelist_type(r#"available_shards"#)
            .generate()
            .expect("Unable to generate bindings");
        xdp_prog_types_bindings
            .write_to_file(out_path.join("xdp_shard.rs"))
            .expect("Unable to write bindings");

        let p: PathBuf = "./src/xdp_shard_ingress.c".parse().unwrap();
        compile_ebpf_prog(&p);
    }
}

fn compile_ebpf_prog(name: &Path) {
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    println!(
        "cargo:rerun-if-changed={}",
        name.as_os_str().to_str().unwrap()
    );
    let clang_include = std::process::Command::new("clang")
        .arg("-print-file-name=include")
        .output()
        .expect("clang get include path")
        .stdout;
    let clang_include = std::string::String::from_utf8(clang_include).unwrap();
    if !std::process::Command::new("clang")
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
            name.to_str().unwrap(),
            "-o",
            name.with_extension("d").to_str().unwrap(),
        ])
        .spawn()
        .expect("clang .c")
        .wait()
        .expect("clang .c")
        .success()
    {
        panic!("clang errored");
    }

    if !std::process::Command::new("llc")
        .args(&[
            "-march=bpf",
            "-filetype=obj",
            "-o",
            name.with_extension("o").to_str().unwrap(),
            name.with_extension("d").to_str().unwrap(),
        ])
        .spawn()
        .expect("llc")
        .wait()
        .expect("llc")
        .success()
    {
        panic!("llc errored");
    }

    std::process::Command::new("rm")
        .args(&["-f", name.with_extension("d").to_str().unwrap()])
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

    if !std::process::Command::new("mv")
        .arg(name.with_extension("o").to_str().unwrap())
        .arg(
            out_path.join(
                name.with_extension("o")
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap(),
            ),
        )
        .spawn()
        .expect("kernel ebpf program move to outdir")
        .wait()
        .expect("kernel ebpf program move to outdir")
        .success()
    {
        panic!("mv errored");
    }
}
