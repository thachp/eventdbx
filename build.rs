use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/replication.capnp");
    println!("cargo:rerun-if-changed=proto/replication.proto");
    println!("cargo:rerun-if-changed=proto/api.proto");
    println!("cargo:rerun-if-changed=proto/plugin.capnp");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    capnpc::CompilerCommand::new()
        .file("proto/replication.capnp")
        .file("proto/plugin.capnp")
        .output_path(out_dir.as_path())
        .run()?;

    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["proto/replication.proto", "proto/api.proto"], &["proto"])?;
    Ok(())
}
