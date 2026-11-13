use prost::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = "../../proto";
    let proto = format!("{root}/sanctum/v1/sanctum.proto");
    println!("cargo:rerun-if-changed={root}");
    let descriptors = protox::compile([proto], [root])?;
    let out = std::path::PathBuf::from(std::env::var_os("OUT_DIR").expect("Cargo sets OUT_DIR"));
    std::fs::write(out.join("sanctum-v1.bin"), descriptors.encode_to_vec())?;
    tonic_prost_build::configure().compile_fds(descriptors)?;
    Ok(())
}
