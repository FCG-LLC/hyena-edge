use error::*;
use uuid::Uuid;

use block::BlockData;
use ty::{Timestamp, Block, BlockMap, BlockType, BlockTypeMap};
use std::path::{Path, PathBuf};
use ty::block::memory::BlockType as MemoryBlockType;
#[cfg(feature = "mmap")]
use ty::block::mmap::BlockType as MmapBlockType;


const PARTITION_METADATA: &str = "meta.data";

#[derive(Debug, Serialize, Deserialize)]
pub struct Partition<'part> {
    id: Uuid,
    ts_min: Timestamp,
    ts_max: Timestamp,

    #[serde(skip)]
    blocks: BlockMap<'part>,
    #[serde(skip)]
    data_root: PathBuf,
}

impl<'part> Partition<'part> {
    pub fn new<P: AsRef<Path>, TS: Into<Timestamp>>(root: P, ts: TS) -> Result<Partition<'part>> {
        let root = root.as_ref().to_path_buf();

        let meta = root.join(PARTITION_METADATA);

        if meta.exists() {
            bail!("Partition metadata already exists {:?}", meta);
        }

        let ts = ts.into();

        Ok(Partition {
            id: Partition::gen_id(),
            ts_min: ts,
            ts_max: ts,
            blocks: Default::default(),
            data_root: root,
        })
    }

    pub fn with_data<P: AsRef<Path>>(root: P) -> Result<Partition<'part>> {
        let root = root.as_ref().to_path_buf();

        let meta = root.join(PARTITION_METADATA);

        Partition::deserialize(&meta, &root)
    }

    pub fn ensure_blocks(&mut self, type_map: &BlockTypeMap) -> Result<()> {
        let blocks = Partition::prepare_blocks(&self.data_root, type_map)
            .chain_err(|| "Unable to create block map")?;

        self.blocks.extend(blocks);

        Ok(())
    }

    #[inline]
    fn gen_id() -> Uuid {
        Uuid::new_v4()
    }

    fn prepare_blocks<P: AsRef<Path>>(root: P, type_map: &BlockTypeMap) -> Result<BlockMap<'part>> {
        type_map
            .iter()
            .map(|(block_id, block_type)| match *block_type {
                BlockType::Memory(bty) => {
                    use ty::block::memory::Block;

                    Block::create(bty)
                        .map(|block| (*block_id, block.into()))
                        .chain_err(|| "Unable to create in-memory block")
                }
                BlockType::Memmap(bty) => {
                    use ty::block::mmap::Block;

                    Block::create(&root, bty, *block_id)
                        .map(|block| (*block_id, block.into()))
                        .chain_err(|| "Unable to create mmap block")
                }
            })
            .collect()
    }

    fn flush(&self) -> Result<()> {
        // TODO: add dirty flag
        let meta = self.data_root.join(PARTITION_METADATA);

        Partition::serialize(self, &meta)
    }

    fn serialize<P: AsRef<Path>>(partition: &Partition<'part>, meta: P) -> Result<()> {
        let meta = meta.as_ref();

        let blocks: BlockTypeMap = (&partition.blocks).into();

        let data = (partition, blocks);

        serialize!(file meta, &data).chain_err(|| "Failed to serialize partition metadata")
    }

    fn deserialize<P: AsRef<Path>, R: AsRef<Path>>(meta: P, root: R) -> Result<Partition<'part>> {
        let meta = meta.as_ref();

        if !meta.exists() {
            bail!("Cannot find partition metadata {:?}", meta);
        }

        let (mut partition, blocks): (Partition, BlockTypeMap) = deserialize!(file meta)
            .chain_err(|| "Failed to read partition metadata")?;

        partition.blocks = Partition::prepare_blocks(&root, &blocks)
            .chain_err(|| "Failed to read block data")?;

        partition.data_root = root.as_ref().to_path_buf();

        Ok(partition)
    }
}


impl<'part> Drop for Partition<'part> {
    fn drop(&mut self) {
        self.flush()
            .chain_err(|| "Failed to flush data during drop")
            .unwrap();
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::HashMap;
    use ty::BlockId;
    use std::fmt::Debug;


    macro_rules! block_test_impl {
        ($base: ident, $($variant: ident),* $(,)*) => {{
            let mut idx = 0;

            hashmap! {
                $(
                    {idx += 1; idx} => $base::$variant,
                )*
            }
        }};

        ($base: ident) => {
            map_block_type_variants!(block_test_impl, $base)
        };
    }

    mod serialize {
        use super::*;


        pub(super) fn block<'block, T>(type_map: HashMap<BlockId, T>)
        where
            T: Debug + Clone,
            Block<'block>: PartialEq<T>,
            BlockType: From<T>,
            BlockTypeMap: From<HashMap<BlockId, T>>,
        {
            let root = tempdir!();
            let ts = <Timestamp as Default>::default();

            let mut part = Partition::new(&root, *ts)
                .chain_err(|| "Failed to create partition")
                .unwrap();

            part.ensure_blocks(&(type_map.clone().into()))
                .chain_err(|| "Failed to create blocks")
                .unwrap();

            assert_eq!(part.ts_min, ts);
            assert_eq!(part.ts_max, ts);
            assert_eq!(part.blocks.len(), type_map.len());
            for (block_id, block_type) in &type_map {
                assert_eq!(part.blocks[block_id], *block_type);
            }
        }

        #[test]
        fn meta() {
            let root = tempdir!();
            let ts = <Timestamp as Default>::default();

            {
                Partition::new(&root, *ts)
                    .chain_err(|| "Failed to create partition")
                    .unwrap();
            }

            let meta = root.as_ref().join(PARTITION_METADATA);

            assert!(meta.exists());
        }
    }

    mod deserialize {
        use super::*;


        pub(super) fn block<'block, T>(type_map: HashMap<BlockId, T>)
        where
            T: Debug + Clone,
            Block<'block>: PartialEq<T>,
            BlockType: From<T>,
            BlockTypeMap: From<HashMap<BlockId, T>>,
        {
            let root = tempdir!();
            let ts = <Timestamp as Default>::default();

            {
                let mut part = Partition::new(&root, *ts)
                    .chain_err(|| "Failed to create partition")
                    .unwrap();

                part.ensure_blocks(&(type_map.clone().into()))
                    .chain_err(|| "Failed to create blocks")
                    .unwrap();
            }

            let part = Partition::with_data(&root)
                .chain_err(|| "Failed to read partition data")
                .unwrap();

            assert_eq!(part.blocks.len(), type_map.len());
            for (block_id, block_type) in &type_map {
                assert_eq!(part.blocks[block_id], *block_type);
            }
        }

        #[test]
        fn meta() {
            let root = tempdir!();
            let ts = <Timestamp as Default>::default();

            let id = {
                let part = Partition::new(&root, *ts)
                    .chain_err(|| "Failed to create partition")
                    .unwrap();

                part.id
            };

            let part = Partition::with_data(&root)
                .chain_err(|| "Failed to read partition data")
                .unwrap();

            assert_eq!(part.id, id);
            assert_eq!(part.ts_min, ts);
            assert_eq!(part.ts_max, ts);
        }
    }

    mod memory {
        use super::*;

        mod serialize {
            use super::*;

            #[test]
            fn block() {
                super::super::serialize::block(block_test_impl!(MemoryBlockType));
            }
        }

        mod deserialize {
            use super::*;

            #[test]
            fn block() {
                super::super::deserialize::block(block_test_impl!(MemoryBlockType));
            }
        }
    }


    #[cfg(feature = "mmap")]
    mod mmap {
        use super::*;

        mod serialize {
            use super::*;

            #[test]
            fn block() {
                super::super::serialize::block(block_test_impl!(MmapBlockType));
            }
        }

        mod deserialize {
            use super::*;

            #[test]
            fn block() {
                super::super::deserialize::block(block_test_impl!(MmapBlockType));
            }
        }
    }
}
