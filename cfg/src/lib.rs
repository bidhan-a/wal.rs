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
