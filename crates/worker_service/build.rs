fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc_path =
        protoc_bin_vendored::protoc_bin_path().expect("failed to find vendored protoc binary");

    unsafe {
        std::env::set_var("PROTOC", protoc_path);
    }

    println!("cargo:rerun-if-changed=../../proto/storage.proto");
    tonic_prost_build::compile_protos("../../proto/storage.proto")?;
    Ok(())
}
