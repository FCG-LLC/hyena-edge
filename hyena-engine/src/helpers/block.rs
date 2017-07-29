use error::*;

macro_rules! block_apply {
    (mut expect physical $ty: ident, $self: expr, $block: ident, $physblock:ident, $what: block) =>
    {{
        use ty::block::Block;
        use error::*;

        let mut lock = acquire!(write $self);

        match *lock {
            Block::Memory(ref mut $block) => {
                use ty::block::memory;

                if let memory::Block::$ty(ref mut $physblock) = *$block {
                    $what
                } else {
                    panic!("Unexpected block type")
                }
            }
            #[cfg(feature = "mmap")]
            Block::Memmap(ref mut $block) => {
                use ty::block::mmap;

                if let mmap::Block::$ty(ref mut $physblock) = *$block {
                    $what
                } else {
                    panic!("Unexpected block type")
                }
            }
        }
    }};

    (expect physical $ty: ident, $self: expr, $block: ident, $physblock:ident, $what: block) => {{
        use ty::block::Block;
        use error::*;

        let lock = acquire!(read $self);

        match *lock {
            Block::Memory(ref $block) => {
                use ty::block::memory;

                if let memory::Block::$ty(ref $physblock) = *$block {
                    $what
                } else {
                    panic!("Unexpected block type")
                }
            }
            #[cfg(feature = "mmap")]
            Block::Memmap(ref $block) => {
                use ty::block::mmap;

                if let mmap::Block::$ty(ref $physblock) = *$block {
                    $what
                } else {
                    panic!("Unexpected block type")
                }
            }
        }
    }};

    (mut map physical $self: expr, $block: ident, $physblock:ident, $what: block) => {{
        use ty::block::Block;
        use error::*;

        let mut lock = acquire!(write $self);

        match *lock {
            Block::Memory(ref mut $block) => {
                use ty::block::memory::Block::*;

                match *$block {
                    I8Dense(ref mut $physblock) => $what,
                    I16Dense(ref mut $physblock) => $what,
                    I32Dense(ref mut $physblock) => $what,
                    I64Dense(ref mut $physblock) => $what,
                    #[cfg(feature = "block_128")]
                    I128Dense(ref mut $physblock) => $what,

                    // Dense, Unsigned
                    U8Dense(ref mut $physblock) => $what,
                    U16Dense(ref mut $physblock) => $what,
                    U32Dense(ref mut $physblock) => $what,
                    U64Dense(ref mut $physblock) => $what,
                    #[cfg(feature = "block_128")]
                    U128Dense(ref mut $physblock) => $what,

                    // Sparse, Signed
                    I8Sparse(ref mut $physblock) => $what,
                    I16Sparse(ref mut $physblock) => $what,
                    I32Sparse(ref mut $physblock) => $what,
                    I64Sparse(ref mut $physblock) => $what,
                    #[cfg(feature = "block_128")]
                    I128Sparse(ref mut $physblock) => $what,

                    // Sparse, Unsigned
                    U8Sparse(ref mut $physblock) => $what,
                    U16Sparse(ref mut $physblock) => $what,
                    U32Sparse(ref mut $physblock) => $what,
                    U64Sparse(ref mut $physblock) => $what,
                    #[cfg(feature = "block_128")]
                    U128Sparse(ref mut $physblock) => $what,
                }
            }
            #[cfg(feature = "mmap")]
            Block::Memmap(ref mut $block) => {
                use ty::block::mmap::Block::*;

                match *$block {
                    I8Dense(ref mut $physblock) => $what,
                    I16Dense(ref mut $physblock) => $what,
                    I32Dense(ref mut $physblock) => $what,
                    I64Dense(ref mut $physblock) => $what,
                    #[cfg(feature = "block_128")]
                    I128Dense(ref mut $physblock) => $what,

                    // Dense, Unsigned
                    U8Dense(ref mut $physblock) => $what,
                    U16Dense(ref mut $physblock) => $what,
                    U32Dense(ref mut $physblock) => $what,
                    U64Dense(ref mut $physblock) => $what,
                    #[cfg(feature = "block_128")]
                    U128Dense(ref mut $physblock) => $what,

                    // Sparse, Signed
                    I8Sparse(ref mut $physblock) => $what,
                    I16Sparse(ref mut $physblock) => $what,
                    I32Sparse(ref mut $physblock) => $what,
                    I64Sparse(ref mut $physblock) => $what,
                    #[cfg(feature = "block_128")]
                    I128Sparse(ref mut $physblock) => $what,

                    // Sparse, Unsigned
                    U8Sparse(ref mut $physblock) => $what,
                    U16Sparse(ref mut $physblock) => $what,
                    U32Sparse(ref mut $physblock) => $what,
                    U64Sparse(ref mut $physblock) => $what,
                    #[cfg(feature = "block_128")]
                    U128Sparse(ref mut $physblock) => $what,
                }
            }
        }
    }};

    (map physical $self: expr, $block: ident, $physblock:ident, $what: block) => {{
        use ty::block::Block;
        use error::*;

        let lock = acquire!(read $self);

        match *lock {
            Block::Memory(ref $block) => {
                use ty::block::memory::Block::*;

                match *$block {
                    I8Dense(ref $physblock) => $what,
                    I16Dense(ref $physblock) => $what,
                    I32Dense(ref $physblock) => $what,
                    I64Dense(ref $physblock) => $what,
                    #[cfg(feature = "block_128")]
                    I128Dense(ref $physblock) => $what,

                    // Dense, Unsigned
                    U8Dense(ref $physblock) => $what,
                    U16Dense(ref $physblock) => $what,
                    U32Dense(ref $physblock) => $what,
                    U64Dense(ref $physblock) => $what,
                    #[cfg(feature = "block_128")]
                    U128Dense(ref $physblock) => $what,

                    // Sparse, Signed
                    I8Sparse(ref $physblock) => $what,
                    I16Sparse(ref $physblock) => $what,
                    I32Sparse(ref $physblock) => $what,
                    I64Sparse(ref $physblock) => $what,
                    #[cfg(feature = "block_128")]
                    I128Sparse(ref $physblock) => $what,

                    // Sparse, Unsigned
                    U8Sparse(ref $physblock) => $what,
                    U16Sparse(ref $physblock) => $what,
                    U32Sparse(ref $physblock) => $what,
                    U64Sparse(ref $physblock) => $what,
                    #[cfg(feature = "block_128")]
                    U128Sparse(ref $physblock) => $what,
                }
            }
            #[cfg(feature = "mmap")]
            Block::Memmap(ref $block) => {
                use ty::block::mmap::Block::*;

                match *$block {
                    I8Dense(ref $physblock) => $what,
                    I16Dense(ref $physblock) => $what,
                    I32Dense(ref $physblock) => $what,
                    I64Dense(ref $physblock) => $what,
                    #[cfg(feature = "block_128")]
                    I128Dense(ref $physblock) => $what,

                    // Dense, Unsigned
                    U8Dense(ref $physblock) => $what,
                    U16Dense(ref $physblock) => $what,
                    U32Dense(ref $physblock) => $what,
                    U64Dense(ref $physblock) => $what,
                    #[cfg(feature = "block_128")]
                    U128Dense(ref $physblock) => $what,

                    // Sparse, Signed
                    I8Sparse(ref $physblock) => $what,
                    I16Sparse(ref $physblock) => $what,
                    I32Sparse(ref $physblock) => $what,
                    I64Sparse(ref $physblock) => $what,
                    #[cfg(feature = "block_128")]
                    I128Sparse(ref $physblock) => $what,

                    // Sparse, Unsigned
                    U8Sparse(ref $physblock) => $what,
                    U16Sparse(ref $physblock) => $what,
                    U32Sparse(ref $physblock) => $what,
                    U64Sparse(ref $physblock) => $what,
                    #[cfg(feature = "block_128")]
                    U128Sparse(ref $physblock) => $what,
                }
            }
        }
    }};

    (map $self: expr, $block: ident, $what: block) => {{
        use ty::block::Block;
        use error::*;

        let mut lock = acquire!(read $self);

        match *lock {
            Block::Memory(ref $block) => $what,
            #[cfg(feature = "mmap")]
            Block::Memmap(ref $block) => $what,
        }
    }};
}
