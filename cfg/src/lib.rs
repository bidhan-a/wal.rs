use std::path::Path;

/// The main configuration.
#[derive(serde::Deserialize, Clone, Copy)]
pub struct Config {
    // Configuration for the log.
    pub log: LogConfig,
}

/// The log configuration.
#[derive(serde::Deserialize, Clone, Copy)]
pub struct LogConfig {
    /// Configuration for the segment.
    pub segment: SegmentConfig,
    /// Durability policy for fsync operations.
    pub durability: DurabilityPolicy,
}

/// Durability policy for controlling fsync behavior.
#[derive(serde::Deserialize, Clone, Copy, Debug, PartialEq)]
pub enum DurabilityPolicy {
    /// Always fsync after each write.
    Always,
    /// Fsync at regular intervals (milliseconds).
    Interval(u64),
    /// Only fsync when rotating segments.
    OnRotate,
    /// Never fsync (for testing only).
    Never,
}

/// The segment configuration.
#[derive(serde::Deserialize, Clone, Copy)]
pub struct SegmentConfig {
    /// The maximum size of the store in a segment (in bytes).
    pub max_store_size: u64,
    /// The maximum size of the index in a segment (in bytes).
    pub max_index_size: u64,
    /// The initial offset of the segment.
    pub initial_offset: u64,
}

/// Load the configuration from the provided config file.
pub fn load_config<P: AsRef<Path>>(file_path: P) -> Result<Config, config::ConfigError> {
    let cfg = config::Config::builder()
        .add_source(config::File::new(
            file_path.as_ref().to_str().unwrap(),
            // Only .toml file is supported for now.
            config::FileFormat::Toml,
        ))
        .build()?;
    cfg.try_deserialize::<Config>()
}
