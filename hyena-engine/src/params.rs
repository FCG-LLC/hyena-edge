// pub(crate) const BLOCK_SIZE: usize = (1 << 20) * 100;	// 100 MiB
pub(crate) const BLOCK_SIZE: usize = (1 << 20); // 1 MiB

#[cfg(test)]
pub(crate) mod tests {
    pub(crate) const BLOCK_SIZE: usize = 1 << 20; // 1 MiB

}
