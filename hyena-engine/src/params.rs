/// The size of a single block file
pub(crate) const BLOCK_SIZE: usize = (1 << 20); // 1 MiB

/// The name of partition metadata file
pub(crate) const PARTITION_METADATA: &str = "meta.data";

/// The type of source_id column
pub type SourceId = u32;

#[cfg(test)]
pub(crate) mod tests {
    /// The size of a single block file for test code
    pub(crate) const BLOCK_SIZE: usize = 1 << 20; // 1 MiB

}
