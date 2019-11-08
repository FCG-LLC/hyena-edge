
macro_rules! block_apply {
    (mut expect physical $ty: ident, $self: expr, $block: ident, $physblock:ident, $what: block) =>
    {{
        use crate::ty::block::Block;
        use crate::error::*;

        let mut lock = acquire!(raw write $self);

        match *lock {
            Block::Memory(ref mut $block) => {
                use crate::ty::block::memory;

                if let memory::Block::$ty(ref mut $physblock) = *$block {
                    $what
                } else {
                    panic!("Unexpected block type")
                }
            }
            #[cfg(feature = "mmap")]
            Block::Memmap(ref mut $block) => {
                use crate::ty::block::mmap;

                if let mmap::Block::$ty(ref mut $physblock) = *$block {
                    $what
                } else {
                    panic!("Unexpected block type")
                }
            }
        }
    }};

    (expect physical $ty: ident, $self: expr, $block: ident, $physblock:ident, $what: block) => {{
        use crate::ty::block::Block;
        use crate::error::*;

        let lock = acquire!(raw read $self);

        match *lock {
            Block::Memory(ref $block) => {
                use crate::ty::block::memory;

                if let memory::Block::$ty(ref $physblock) = *$block {
                    $what
                } else {
                    panic!("Unexpected block type")
                }
            }
            #[cfg(feature = "mmap")]
            Block::Memmap(ref $block) => {
                use crate::ty::block::mmap;

                if let mmap::Block::$ty(ref $physblock) = *$block {
                    $what
                } else {
                    panic!("Unexpected block type")
                }
            }
        }
    }};

    (mut map physical $self: expr, $block: ident, $physblock:ident, $what: block) => {{
        use crate::ty::block::Block;
        use crate::error::*;

        let mut lock = acquire!(raw write $self);

        match *lock {
            Block::Memory(ref mut $block) => {
                use crate::ty::block::memory::Block::*;

                match *$block {
                    I8Dense(ref mut $physblock) => $what,
                    I16Dense(ref mut $physblock) => $what,
                    I32Dense(ref mut $physblock) => $what,
                    I64Dense(ref mut $physblock) => $what,
                    I128Dense(ref mut $physblock) => $what,

                    // Dense, Unsigned
                    U8Dense(ref mut $physblock) => $what,
                    U16Dense(ref mut $physblock) => $what,
                    U32Dense(ref mut $physblock) => $what,
                    U64Dense(ref mut $physblock) => $what,
                    U128Dense(ref mut $physblock) => $what,

                    // String
                    StringDense(ref mut $physblock) => $what,

                    // Sparse, Signed
                    I8Sparse(ref mut $physblock) => $what,
                    I16Sparse(ref mut $physblock) => $what,
                    I32Sparse(ref mut $physblock) => $what,
                    I64Sparse(ref mut $physblock) => $what,
                    I128Sparse(ref mut $physblock) => $what,

                    // Sparse, Unsigned
                    U8Sparse(ref mut $physblock) => $what,
                    U16Sparse(ref mut $physblock) => $what,
                    U32Sparse(ref mut $physblock) => $what,
                    U64Sparse(ref mut $physblock) => $what,
                    U128Sparse(ref mut $physblock) => $what,
                }
            }
            #[cfg(feature = "mmap")]
            Block::Memmap(ref mut $block) => {
                use crate::ty::block::mmap::Block::*;

                match *$block {
                    I8Dense(ref mut $physblock) => $what,
                    I16Dense(ref mut $physblock) => $what,
                    I32Dense(ref mut $physblock) => $what,
                    I64Dense(ref mut $physblock) => $what,
                    I128Dense(ref mut $physblock) => $what,

                    // Dense, Unsigned
                    U8Dense(ref mut $physblock) => $what,
                    U16Dense(ref mut $physblock) => $what,
                    U32Dense(ref mut $physblock) => $what,
                    U64Dense(ref mut $physblock) => $what,
                    U128Dense(ref mut $physblock) => $what,

                    // String
                    StringDense(ref mut $physblock) => $what,

                    // Sparse, Signed
                    I8Sparse(ref mut $physblock) => $what,
                    I16Sparse(ref mut $physblock) => $what,
                    I32Sparse(ref mut $physblock) => $what,
                    I64Sparse(ref mut $physblock) => $what,
                    I128Sparse(ref mut $physblock) => $what,

                    // Sparse, Unsigned
                    U8Sparse(ref mut $physblock) => $what,
                    U16Sparse(ref mut $physblock) => $what,
                    U32Sparse(ref mut $physblock) => $what,
                    U64Sparse(ref mut $physblock) => $what,
                    U128Sparse(ref mut $physblock) => $what,
                }
            }
        }
    }};

    (map physical $self: expr, $block: ident, $physblock:ident, $what: block) => {{
        use crate::ty::block::Block;
        use crate::error::*;

        let lock = acquire!(raw read $self);

        match *lock {
            Block::Memory(ref $block) => {
                use crate::ty::block::memory::Block::*;

                match *$block {
                    I8Dense(ref $physblock) => $what,
                    I16Dense(ref $physblock) => $what,
                    I32Dense(ref $physblock) => $what,
                    I64Dense(ref $physblock) => $what,
                    I128Dense(ref $physblock) => $what,

                    // Dense, Unsigned
                    U8Dense(ref $physblock) => $what,
                    U16Dense(ref $physblock) => $what,
                    U32Dense(ref $physblock) => $what,
                    U64Dense(ref $physblock) => $what,
                    U128Dense(ref $physblock) => $what,

                    // String
                    StringDense(ref $physblock) => $what,

                    // Sparse, Signed
                    I8Sparse(ref $physblock) => $what,
                    I16Sparse(ref $physblock) => $what,
                    I32Sparse(ref $physblock) => $what,
                    I64Sparse(ref $physblock) => $what,
                    I128Sparse(ref $physblock) => $what,

                    // Sparse, Unsigned
                    U8Sparse(ref $physblock) => $what,
                    U16Sparse(ref $physblock) => $what,
                    U32Sparse(ref $physblock) => $what,
                    U64Sparse(ref $physblock) => $what,
                    U128Sparse(ref $physblock) => $what,
                }
            }
            #[cfg(feature = "mmap")]
            Block::Memmap(ref $block) => {
                use crate::ty::block::mmap::Block::*;

                match *$block {
                    I8Dense(ref $physblock) => $what,
                    I16Dense(ref $physblock) => $what,
                    I32Dense(ref $physblock) => $what,
                    I64Dense(ref $physblock) => $what,
                    I128Dense(ref $physblock) => $what,

                    // Dense, Unsigned
                    U8Dense(ref $physblock) => $what,
                    U16Dense(ref $physblock) => $what,
                    U32Dense(ref $physblock) => $what,
                    U64Dense(ref $physblock) => $what,
                    U128Dense(ref $physblock) => $what,

                    // String
                    StringDense(ref $physblock) => $what,

                    // Sparse, Signed
                    I8Sparse(ref $physblock) => $what,
                    I16Sparse(ref $physblock) => $what,
                    I32Sparse(ref $physblock) => $what,
                    I64Sparse(ref $physblock) => $what,
                    I128Sparse(ref $physblock) => $what,

                    // Sparse, Unsigned
                    U8Sparse(ref $physblock) => $what,
                    U16Sparse(ref $physblock) => $what,
                    U32Sparse(ref $physblock) => $what,
                    U64Sparse(ref $physblock) => $what,
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

macro_rules! map_block {

    // `public`, map mutable block
    // will call `@cfg map physical` private API with every available Block variant

    (mut map
        $block: expr,
        $bid: ident,
        $dense: block,
        $sparse: block,
        $pooled_dense: block) => {{

        cfg_if! {
            if #[cfg(feature = "mmap")] {
                macro_rules! __cond_mmap_mut {
                    () => {{
                        map_block!(@cfg map $block, $bid, $dense, $sparse, $pooled_dense,
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
                        map_block!(@cfg map $block, $bid, $dense, $sparse, $pooled_dense,
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
        $bid: ident,
        $dense: block,
        $sparse: block,
        $pooled_dense: block) => {{

        cfg_if! {
            if #[cfg(feature = "mmap")] {
                macro_rules! __cond_mmap {
                    () => {{
                        map_block!(@cfg map $block, $bid, $dense, $sparse, $pooled_dense,
                        [
                            [ Block::Memory, use crate::ty::block::memory::Block::*; ],
                            [ Block::Memmap, use crate::ty::block::mmap::Block::*; ]
                        ],
                        map)
                    }};
                }
            } else {
                macro_rules! __cond_mmap {
                    () => {{
                        map_block!(@cfg map $block, $bid, $dense, $sparse, $pooled_dense,
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
        $bid: ident,
        $dense: block,
        $sparse: block,
        $pooled_dense: block,
        [ $( [ $bvars: path, $use: item ] ),* $(,)* ],
        $modifiers: tt) => {{

        macro_rules! __cond {
            (map) => {{
                map_block!(@ map $block, $bid, $dense, $sparse, $pooled_dense,
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

                        pooled dense [
                            StringDense
                        ]
                    )*
                )
            }};

            (mut) => {{
                map_block!(@ mut map $block, $bid, $dense, $sparse, $pooled_dense,
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

                        pooled dense [
                            StringDense
                        ]
                    )*
                )
            }};
        }

        __cond!($modifiers)
    }};

    // `private`, map mutable block given Block variants and types combination

    (@ mut map
        $block: expr,
        $bid: ident,
        $dense: block,
        $sparse: block,
        $pooled_dense: block,
        $(
            $variant: path,
            $use: item,
            dense [ $($dense_var: ident),+ $(,)* ]
            sparse [ $($sparse_var: ident),+ $(,)* ]
            pooled dense [ $($pooled_dense_var: ident),+ $(,)* ]
        )*

    ) => {{

        use ty::block::Block;
        use error::*;


        match *$block {
            $(
            $variant(ref mut block) => {
                $use;

                match *block {
                    // Dense

                    $(
                    $dense_var(ref mut $bid) => $dense,
                    )+

                    // Sparse

                    $(
                    $sparse_var(ref mut $bid) => $sparse,
                    )+

                    // Pooled dense
                    $(
                    $pooled_dense_var(ref mut $bid) => $pooled_dense,
                    )+
                }
            }
            )*
        }
    }};

    // `private`, map immutable block given Block variants and types combination

    (@ map
        $block: expr,
        $bid: ident,
        $dense: block,
        $sparse: block,
        $pooled_dense: block,
        $(
            $variant: path,
            $use: item,
            dense [ $($dense_var: ident),+ $(,)* ]
            sparse [ $($sparse_var: ident),+ $(,)* ]
            pooled dense [ $($pooled_dense_var: ident),+ $(,)* ]
        )*
    )
        => {{
        use crate::ty::block::Block;

        match *$block {
            $(
            $variant(ref block) => {
                $use;

                match *block {
                    // Dense

                    $(
                    $dense_var(ref $bid) => $dense,
                    )+

                    // Sparse

                    $(
                    $sparse_var(ref $bid) => $sparse,
                    )+

                    // Pooled dense
                    $(
                    $pooled_dense_var(ref $bid) => $pooled_dense,
                    )+
                }
            }
            )*
        }
    }};
}

/// Unwrap Block and Fragment, verifying that the underlying types are the same
///
/// After both types are unwrapped given block is executed within the unwrapped context
///
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

    // @todo: refactor this to use map_block
    // right now it's not trivial, as this macro requires access to $dense_var and $sparse_var
    // which cannot be easily accessed from outside the macro

    (mut map owned
        $block: expr,
        $frag: expr,
        $bid: ident,
        $fid: ident,
        $fidx: ident,
        $dense: block,
        $sparse: block,
        $pooled_dense: block) => {
        map_fragment!(mut map $block, $frag, $bid, $fid, $fidx,
            $dense, $sparse, $pooled_dense, Fragment)
    };

    (map owned
        $block: expr,
        $frag: expr,
        $bid: ident,
        $fid: ident,
        $fidx: ident,
        $dense: block,
        $sparse: block,
        $pooled_dense: block) => {
        map_fragment!(map $block, $frag, $bid, $fid, $fidx,
            $dense, $sparse, $pooled_dense, Fragment)
    };

    (mut map ref
        $block: expr,
        $frag: expr,
        $bid: ident,
        $fid: ident,
        $fidx: ident,
        $dense: block,
        $sparse: block,
        $pooled_dense: block) => {
        map_fragment!(mut map $block, $frag, $bid, $fid, $fidx,
            $dense, $sparse, $pooled_dense, FragmentRef)
    };

    (map ref
        $block: expr,
        $frag: expr,
        $bid: ident,
        $fid: ident,
        $fidx: ident,
        $dense: block,
        $sparse: block,
        $pooled_dense: block) => {
        map_fragment!(map $block, $frag, $bid, $fid, $fidx,
            $dense, $sparse, $pooled_dense, FragmentRef)
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
        $pooled_dense: block,
        $ty: ident) => {{

        cfg_if! {
            if #[cfg(feature = "mmap")] {
                macro_rules! __cond_mmap_mut {
                    () => {{
                        map_fragment!(@cfg map $block, $frag, $bid, $fid, $fidx,
                        $dense, $sparse, $pooled_dense, $ty,
                        [
                            [ Block::Memory, use crate::ty::block::memory::Block::*; ],
                            [ Block::Memmap, use crate::ty::block::mmap::Block::*; ]
                        ],
                        mut)
                    }};
                }
            } else {
                macro_rules! __cond_mmap_mut {
                    () => {{
                        map_fragment!(@cfg map $block, $frag, $bid, $fid, $fidx,
                        $dense, $sparse, $pooled_dense, $ty,
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
        $pooled_dense: block,
        $ty: ident) => {{

        cfg_if! {
            if #[cfg(feature = "mmap")] {
                macro_rules! __cond_mmap {
                    () => {{
                        map_fragment!(@cfg map $block, $frag, $bid, $fid, $fidx,
                        $dense, $sparse, $pooled_dense, $ty,
                        [
                            [ Block::Memory, use crate::ty::block::memory::Block::*; ],
                            [ Block::Memmap, use crate::ty::block::mmap::Block::*; ]
                        ],
                        map)
                    }};
                }
            } else {
                macro_rules! __cond_mmap {
                    () => {{
                        map_fragment!(@cfg map $block, $frag, $bid, $fid, $fidx,
                        $dense, $sparse, $pooled_dense, $ty,
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
        $pooled_dense: block,
        $ty: ident,
        [ $( [ $bvars: path, $use: item ] ),* $(,)* ],
        $modifiers: tt) => {{

        macro_rules! __cond {
            (map) => {{
                map_fragment!(@ map $block, $frag, $bid, $fid, $fidx,
                    $dense, $sparse, $pooled_dense, $ty,

                    $(
                        $bvars, $use,

                        dense [
                            I8Dense, I16Dense, I32Dense, I64Dense, I128Dense,
                            U8Dense, U16Dense, U32Dense, U64Dense, U128Dense,
                        ]

                        sparse [
                            I8Sparse, I16Sparse, I32Sparse, I64Sparse, I128Sparse,
                            U8Sparse, U16Sparse, U32Sparse, U64Sparse, U128Sparse
                        ]

                        pooled dense [ StringDense ]
                    )*
                )
            }};

            (mut) => {{
                map_fragment!(@ mut map $block, $frag, $bid, $fid, $fidx,
                    $dense, $sparse, $pooled_dense, $ty,
                    $(
                        $bvars, $use,

                        dense [
                            I8Dense, I16Dense, I32Dense, I64Dense, I128Dense,
                            U8Dense, U16Dense, U32Dense, U64Dense, U128Dense,
                        ]

                        sparse [
                            I8Sparse, I16Sparse, I32Sparse, I64Sparse, I128Sparse,
                            U8Sparse, U16Sparse, U32Sparse, U64Sparse, U128Sparse
                        ]

                        pooled dense [ StringDense ]

                    )*
                )
            }};
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
        $pooled_dense: block,
        $ty: ident,
        $(
            $variant: path,
            $use: item,
            dense [ $($dense_var: ident),+ $(,)* ]
            sparse [ $($sparse_var: ident),+ $(,)* ]
            pooled dense [ $($pooled_dense_var: ident),+ $(,)* ]
        )*

        ) => {{

        use crate::ty::block::Block;
        use crate::ty::fragment::$ty;
        use crate::error::*;


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
                            Err::<_, Error>(err_msg("Incompatible block and fragment types"))
                        }
                    }
                    )+

                    // Sparse

                    $(
                    $sparse_var(ref mut $bid) => {
                        if let $ty::$sparse_var(ref $fid, ref $fidx) = *$frag {
                            Ok($sparse)
                        } else {
                            Err(err_msg("Incompatible block and fragment types"))
                        }
                    }
                    )+

                    // Pooled dense (e.g. String)

                    $(
                    $pooled_dense_var(ref mut $bid) => {
                        if let $ty::$pooled_dense_var(ref $fid) = *$frag {
                            Ok($pooled_dense)
                        } else {
                            Err::<_, Error>(err_msg("Incompatible block and fragment types"))
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
        $pooled_dense: block,
        $ty: ident,
        $(
            $variant: path,
            $use: item,
            dense [ $($dense_var: ident),+ $(,)* ]
            sparse [ $($sparse_var: ident),+ $(,)* ]
            pooled dense [ $($pooled_dense_var: ident),+ $(,)* ]
        )*
    )
        => {{
        use crate::ty::block::Block;
        use crate::ty::fragment::$ty;
        use crate::error::*;


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
                            Err::<_, Error>(err_msg("Incompatible block and fragment types"))
                        }
                    }
                    )+

                    // Sparse

                    $(
                    $sparse_var(ref $bid) => {
                        if let $ty::$sparse_var(ref $fid, ref $fidx) = *$frag {
                            Ok($sparse)
                        } else {
                            Err(err_msg("Incompatible block and fragment types"))
                        }
                    }
                    )+

                    // Pooled dense (e.g. String)

                    $(
                    $pooled_dense_var(ref $bid) => {
                        if let $ty::$pooled_dense_var(ref $fid) = *$frag {
                            Ok($pooled_dense)
                        } else {
                            Err::<_, Error>(err_msg("Incompatible block and fragment types"))
                        }
                    }
                    )+
                }
            }
            )*
        }
    }};
}
