use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/plugin.capnp");
    println!("cargo:rerun-if-changed=proto/control.capnp");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    capnpc::CompilerCommand::new()
        .file("proto/plugin.capnp")
        .file("proto/control.capnp")
        .output_path(out_dir.as_path())
        .run()?;
    Ok(())
}
