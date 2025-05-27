fn main() {
    tonic_build::compile_protos("proto/log.v1.proto")
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
