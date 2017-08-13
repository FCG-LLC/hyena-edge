use error::*;

macro_rules! block_apply {
    (mut expect physical $ty: ident, $self: expr, $block: ident, $physblock:ident, $what: block) =>
    {{
        use ty::block::Block;
        use error::*;

        let mut lock = acquire!(raw write $self);

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

        let lock = acquire!(raw read $self);

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

        let mut lock = acquire!(raw write $self);

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

        let lock = acquire!(raw read $self);

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

        let mut lock = acquire!(raw read $self);

        match *lock {
            Block::Memory(ref $block) => $what,
            #[cfg(feature = "mmap")]
            Block::Memmap(ref $block) => $what,
        }
    }};
}


/// Unwrap Block and Fragment, verifying that the underlying types are the same
///
/// After both types are unwrapped given block is executed within the unwrapped context
///
/// # Arguments
///
/// `$block` - `ty::block::Block` to unwrap
/// `$frag` - `ty::fragment::Fragment` to combine with the block
/// `$bid` - unwrapped block identifier, for use within `$what`
/// `$fid` - unwrapped fragment's block identifier, for use within `$what`
/// `$fidx` - unwrapped fragment's index identifier (for sparse blocks)
/// `$what` - an expression block to eval
///
/// # Return values
///
/// Result<`$what`, `error::Error`>

macro_rules! map_fragment {

    (mut map owned
        $block: expr,
        $frag: expr,
        $bid: ident,
        $fid: ident,
        $fidx: ident,
        $dense: block,
        $sparse: block) => {
        map_fragment!(mut map $block, $frag, $bid, $fid, $fidx, $dense, $sparse, Fragment)
    };

    (map owned
        $block: expr,
        $frag: expr,
        $bid: ident,
        $fid: ident,
        $fidx: ident,
        $dense: block,
        $sparse: block) => {
        map_fragment!(map $block, $frag, $bid, $fid, $fidx, $dense, $sparse, Fragment)
    };

    (mut map ref
        $block: expr,
        $frag: expr,
        $bid: ident,
        $fid: ident,
        $fidx: ident,
        $dense: block,
        $sparse: block) => {
        map_fragment!(mut map $block, $frag, $bid, $fid, $fidx, $dense, $sparse, FragmentRef)
    };

    (map ref
        $block: expr,
        $frag: expr,
        $bid: ident,
        $fid: ident,
        $fidx: ident,
        $dense: block,
        $sparse: block) => {
        map_fragment!(map $block, $frag, $bid, $fid, $fidx, $dense, $sparse, FragmentRef)
    };

    // `public`, map mutable block
    // will call `@cfg map physical` private API with every available Block variant

    (mut map
        $block: expr,
        $frag: expr,
        $bid: ident,
        $fid: ident,
        $fidx: ident,
        $dense: block,
        $sparse: block,
        $ty: ident) => {{

        cfg_if! {
            if #[cfg(feature = "mmap")] {
                macro_rules! __cond_mmap_mut {
                    () => {{
                        map_fragment!(@cfg map $block, $frag, $bid, $fid, $fidx, $dense, $sparse,
                        $ty,
                        [
                            [ Block::Memory, use ty::block::memory::Block::*; ],
                            [ Block::Memmap, use ty::block::mmap::Block::*; ]
                        ],
                        mut)
                    }};
                }
            } else {
                macro_rules! __cond_mmap_mut {
                    () => {{
                        map_fragment!(@cfg map $block, $frag, $bid, $fid, $fidx, $dense, $sparse,
                        $ty,
                        [ [ Block::Memory, use ty::block::memory::Block::*; ] ],
                        mut)
                    }};
                }
            }
        }

        __cond_mmap_mut!()
    }};

    // `public`, map immutable block
    // will call `@cfg map physical` private API with every available Block variant

    (map
        $block: expr,
        $frag: expr,
        $bid: ident,
        $fid: ident,
        $fidx: ident,
        $dense: block,
        $sparse: block,
        $ty: ident) => {{

        cfg_if! {
            if #[cfg(feature = "mmap")] {
                macro_rules! __cond_mmap {
                    () => {{
                        map_fragment!(@cfg map $block, $frag, $bid, $fid, $fidx, $dense, $sparse,
                        $ty,
                        [
                            [ Block::Memory, use ty::block::memory::Block::*; ],
                            [ Block::Memmap, use ty::block::mmap::Block::*; ]
                        ],
                        map)
                    }};
                }
            } else {
                macro_rules! __cond_mmap {
                    () => {{
                        map_fragment!(@cfg map $block, $frag, $bid, $fid, $fidx, $dense, $sparse,
                        $ty,
                        [ [ Block::Memory, use ty::block::memory::Block::*; ] ],
                        map)
                    }};
                }
            }
        }

        __cond_mmap!()
    }};

    // `private`, map mutable/immutable block, determined by `$modifiers`
    // will call `@ map physical` private API with every available Block variant
    // and BlockType / Fragment variants combination

    (@cfg map
        $block: expr,
        $frag: expr,
        $bid: ident,
        $fid: ident,
        $fidx: ident,
        $dense: block,
        $sparse: block,
        $ty: ident,
        [ $( [ $bvars: path, $use: item ] ),* $(,)* ],
        $modifiers: tt) => {{

        cfg_if! {
            if #[cfg(feature = "block_128")] {
                macro_rules! __cond {
                    (map) => {{
                        map_fragment!(@ map $block, $frag, $bid, $fid, $fidx, $dense, $sparse, $ty,

                            $(
                                $bvars, $use,

                                dense [
                                    I8Dense, I16Dense, I32Dense, I64Dense, I128Dense,
                                    U8Dense, U16Dense, U32Dense, U64Dense, U128Dense
                                ]

                                sparse [
                                    I8Sparse, I16Sparse, I32Sparse, I64Sparse, I128Sparse,
                                    U8Sparse, U16Sparse, U32Sparse, U64Sparse, U128Sparse
                                ]
                            )*
                        )
                    }};

                    (mut) => {{
                        map_fragment!(@ mut map $block, $frag, $bid, $fid, $fidx, $dense, $sparse,
                            $ty,
                            $(
                                $bvars, $use,

                                dense [
                                    I8Dense, I16Dense, I32Dense, I64Dense, I128Dense,
                                    U8Dense, U16Dense, U32Dense, U64Dense, U128Dense
                                ]

                                sparse [
                                    I8Sparse, I16Sparse, I32Sparse, I64Sparse, I128Sparse,
                                    U8Sparse, U16Sparse, U32Sparse, U64Sparse, U128Sparse
                                ]
                            )*
                        )
                    }};
                }
            } else {
                macro_rules! __cond {
                    (map) => {{
                        map_fragment!(@ map $block, $frag, $bid, $fid, $fidx, $dense, $sparse, $ty,
                            $(
                                $bvars, $use,

                                dense [
                                    I8Dense, I16Dense, I32Dense, I64Dense,
                                    U8Dense, U16Dense, U32Dense, U64Dense
                                ]

                                sparse [
                                    I8Sparse, I16Sparse, I32Sparse, I64Sparse,
                                    U8Sparse, U16Sparse, U32Sparse, U64Sparse
                                ]
                            )*
                        )
                    }};

                    (mut) => {{
                        map_fragment!(@ mut map $block, $frag, $bid, $fid, $fidx, $dense, $sparse,
                            $ty,
                            $(
                                $bvars, $use,


                                dense [
                                    I8Dense, I16Dense, I32Dense, I64Dense,
                                    U8Dense, U16Dense, U32Dense, U64Dense
                                ]

                                sparse [
                                    I8Sparse, I16Sparse, I32Sparse, I64Sparse,
                                    U8Sparse, U16Sparse, U32Sparse, U64Sparse
                                ]
                            )*
                        )
                    }};
                }
            }
        }

        __cond!($modifiers)
    }};

    // `private`, map mutable block given Block variants and types combination

    (@ mut map
        $block: expr,
        $frag: expr,
        $bid: ident,
        $fid: ident,
        $fidx: ident,
        $dense: block,
        $sparse: block,
        $ty: ident,
        $(
            $variant: path,
            $use: item,
            dense [ $($dense_var: ident),+ $(,)* ]
            sparse [ $($sparse_var: ident),+ $(,)* ]
        )*

        ) => {{

        use ty::block::Block;
        use ty::fragment::$ty;
        use error::*;


        match *$block {
            $(
            $variant(ref mut block) => {
                $use;

                match *block {
                    // Dense

                    $(
                    $dense_var(ref mut $bid) => {
                        if let $ty::$dense_var(ref $fid) = *$frag {
                            Ok($dense)
                        } else {
                            Err::<_, Error>("Incompatible block and fragment types".into())
                        }
                    }
                    )+

                    // Sparse

                    $(
                    $sparse_var(ref mut $bid) => {
                        if let $ty::$sparse_var(ref $fid, ref $fidx) = *$frag {
                            Ok($sparse)
                        } else {
                            Err("Incompatible block and fragment types".into())
                        }
                    }
                    )+
                }
            }
            )*
        }
    }};

    // `private`, map immutable block given Block variants and types combination

    (@ map
        $block: expr,
        $frag: expr,
        $bid: ident,
        $fid: ident,
        $fidx: ident,
        $dense: block,
        $sparse: block,
        $ty: ident,
        $(
            $variant: path,
            $use: item,
            dense [ $($dense_var: ident),+ $(,)* ]
            sparse [ $($sparse_var: ident),+ $(,)* ]
        )*
    )
        => {{
        use ty::block::Block;
        use ty::fragment::$ty;
        use error::*;


        match *$block {
            $(
            $variant(ref block) => {
                $use;

                match *block {
                    // Dense

                    $(
                    $dense_var(ref $bid) => {
                        if let $ty::$dense_var(ref $fid) = *$frag {
                            Ok($dense)
                        } else {
                            Err::<_, Error>("Incompatible block and fragment types".into())
                        }
                    }
                    )+

                    // Sparse

                    $(
                    $sparse_var(ref $bid) => {
                        if let $ty::$sparse_var(ref $fid, ref $fidx) = *$frag {
                            Ok($sparse)
                        } else {
                            Err("Incompatible block and fragment types".into())
                        }
                    }
                    )+
                }
            }
            )*
        }
    }};
}
