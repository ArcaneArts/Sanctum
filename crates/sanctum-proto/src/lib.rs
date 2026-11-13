pub mod v1 {
    tonic::include_proto!("sanctum.v1");

    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("sanctum-v1");
}

pub mod upload;
