pub mod api {
    tonic::include_proto!("log.v1");
}

pub use api::*;
