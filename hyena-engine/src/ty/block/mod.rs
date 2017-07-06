#[macro_use]
mod numeric;


pub enum Block<'block> {
    Memory(memory::Block<'block>),
    #[cfg(feature = "mmap")]
    Memmap(mmap::Block<'block>),
}


mod memory {
    use storage::Storage;
    use storage::memory::PagedMemoryStorage;

    numeric_block_impl!(PagedMemoryStorage);
}

#[cfg(feature = "mmap")]
mod mmap {
    use storage::Storage;
    use storage::mmap::MemmapStorage;

    numeric_block_impl!(MemmapStorage);
}
