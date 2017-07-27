use error::*;

macro_rules! block_apply {
    (mut expect physical $ty: ident, $self: expr, $block: ident, $physblock:ident, $what: block) =>
    {{
        use ty::block::Block;

        match *$self {
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

        match *$self {
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

    ($self: expr, $block: ident, $physblock:ident, $what: block) => {{
        use ty::block::Block;

        match *$self {
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

    ($self: expr, $block: ident, $what: block) => {
        use ty::block::Block;

        match *$self {
            Block::Memory(ref $block) => $what,
            #[cfg(feature = "mmap")]
            Block::Memmap(ref $block) => $what,
        }
    };

    ($self: expr, $block: ident, $physblock:ident, $what: expr) => {
        block_apply!($self, $block, $physblock, {$what})
    };

    ($self: expr, $block: ident, $what: expr) => {
        block_apply!($self, $block, {$what})
    };
}
