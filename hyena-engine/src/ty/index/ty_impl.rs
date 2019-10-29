use crate::block::index::BloomIndexBlock;

macro_rules! column_index_impl {
    ($ST:ty) => {
        use crate::block::BlockData;
        use serde::{Serialize, Serializer};
        use std::result::Result as StdResult;
        use std::sync::RwLock;
        use crate::ty::index::ty_impl::*;

        #[derive(Debug)]
        pub enum ColumnIndex<'idx> {
            Bloom(BloomColumnIndexBlock<'idx, $ST>),
        }

        impl<'idx> ColumnIndex<'idx> {
            #[inline]
            pub(crate) fn append_value<'v, T>(&mut self, value: T)
            where
                T: 'v + AsRef<[u8]> + Debug,
            {
                use self::ColumnIndex::*;

                column_index_map_expr!(mut *self, idx, {
                                    idx.index_append_value(value)
                                })
            }

            #[inline]
            pub(crate) fn iter(&'idx self) -> Box<dyn Iterator<Item = &'idx BloomValue> + 'idx> {
                use self::ColumnIndex::*;

                column_index_map_expr!(*self, idx, {
                    Box::new(<_ as ScanIndex<&str>>::index_iter(idx))
                })
            }

            #[inline]
            pub(crate) fn set_head(&mut self, head: usize) -> Result<()> {
                use self::ColumnIndex::*;

                column_index_map_expr!(mut *self, idx, {
                                    use crate::block::BufferHead;

                                    if head > idx.size() {
                                        bail!("bad head pointer for column index block");
                                    }

                                    *idx.mut_head() = head;

                                    Ok(())
                                })
            }
        }

        impl<'idx> Serialize for ColumnIndex<'idx> {
            fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
            where
                S: Serializer,
            {
                use self::ColumnIndex::*;

                column_index_map_expr!(*self, idx, { idx.as_ref().serialize(serializer) })
            }
        }

        impl<'idx> From<BloomColumnIndexBlock<'idx, $ST>> for ColumnIndex<'idx> {
            fn from(block: BloomColumnIndexBlock<'idx, $ST>) -> ColumnIndex<'idx> {
                ColumnIndex::Bloom(block)
            }
        }

        use super::ColumnIndexType;

        impl<'idx, 'a> From<&'a ColumnIndex<'idx>> for ColumnIndexType {
            fn from(block: &ColumnIndex) -> ColumnIndexType {
                use self::ColumnIndex::*;

                match *block {
                    Bloom(..) => ColumnIndexType::Bloom,
                }
            }
        }

        impl<'idx, 'a> From<&'a RwLock<ColumnIndex<'idx>>> for ColumnIndexType {
            fn from(block: &RwLock<ColumnIndex>) -> ColumnIndexType {
                (acquire!(read block)).into()
            }
        }
    };
}

pub(crate) type BloomColumnIndexBlock<'idx, S> = BloomIndexBlock<'idx, S>;

macro_rules! column_index_map_expr {
    (@ mut $idx: expr, $blockref: ident, $body: block [ $($variant: ident),+ $(,)* ]) => {
        match $idx {
            $(
                $variant(ref mut $blockref) => $body,
            )+
        }
    };

    (@ $idx: expr, $blockref: ident, $body: block [ $($variant: ident),+ $(,)* ]) => {
        match $idx {
            $(
                $variant(ref $blockref) => $body,
            )+
        }
    };

    (mut $idx: expr, $blockref: ident, $body: block) => {{
        column_index_map_expr!(@ mut $idx, $blockref, $body
                        [
                            Bloom
                        ]
                    )
    }};

    ($idx: expr, $blockref: ident, $body: block) => {{
        column_index_map_expr!(@ $idx, $blockref, $body
                        [
                            Bloom
                        ]
                    )
    }};
}
