extern crate hyena_engine;
extern crate failure;

use failure::ResultExt;

use hyena_engine::{Append, BlockData, BlockStorage, BlockType, Catalog, Column, ColumnMap,
                   Fragment, Scan, ScanFilter, ScanFilterOp, ScanResult,
                   SparseIndex, Timestamp, TimestampFragment};

use std::iter::repeat;
use std::collections::{HashMap, HashSet};

#[macro_use]
mod common;

use common::{catalog_dir, get_columns, wrap_result};

#[test]
fn it_creates_catalog() {
    wrap_result! {{
        let dir = catalog_dir()?;
        let cat = Catalog::new(&dir)?;

        let columns = get_columns(&cat);

        let expected = [
            (0_usize, "timestamp".to_owned()),
            (1, "source_id".to_owned()),
        ];

        assert_eq!(&columns[..], &expected[..]);
    }}
}

#[test]
fn it_adds_new_column_catalog() {
    wrap_result! {{
        let dir = catalog_dir()?;
        let mut cat = Catalog::new(&dir)?;

        let mut column_map = ColumnMap::new();

        column_map.insert(2, Column::new(
            BlockStorage::Memmap(BlockType::U64Dense), "dense1"));
        column_map.insert(3, Column::new(
            BlockStorage::Memmap(BlockType::U64Sparse), "sparse1"));

        cat.add_columns(column_map)?;

        let columns = get_columns(&cat);

        let expected = [
            (0_usize, "timestamp".to_owned()),
            (1, "source_id".to_owned()),
            (2, "dense1".to_owned()),
            (3, "sparse1".to_owned()),
        ];

        assert_eq!(&columns[..], &expected[..]);
    }}
}

fn create_append_data(now: Timestamp, record_count: usize) -> (TimestampFragment, BlockData) {
    let tsfrag = TimestampFragment::from(
        repeat(())
            .take(record_count)
            .enumerate()
            .map(|(i, _)| *now + i as u64)
            .collect::<Vec<u64>>(),
    );

    let densefrag = Fragment::from(
        repeat(())
            .take(record_count)
            .enumerate()
            .map(|(i, _)| i as u64)
            .collect::<Vec<u64>>(),
    );

    let sparsefrag = Fragment::from((
        repeat(())
            .take(record_count / 2)
            .enumerate()
            .map(|(i, _)| i as u64)
            .collect::<Vec<u64>>(),
        repeat(())
            .take(record_count / 2)
            .enumerate()
            .map(|(i, _)| i as u32 * 2)
            .collect::<Vec<SparseIndex>>(),
    ));

    let mut data = BlockData::new();

    data.insert(2, densefrag);
    data.insert(3, sparsefrag);

    (tsfrag, data)
}

#[test]
fn it_appends_data() {
    wrap_result! {{
        let dir = catalog_dir()?;
        let mut cat = Catalog::new(&dir)?;

        let mut column_map = ColumnMap::new();

        column_map.insert(2, Column::new(
            BlockStorage::Memmap(BlockType::U64Dense), "dense1"));
        column_map.insert(3, Column::new(
            BlockStorage::Memmap(BlockType::U64Sparse), "sparse1"));

        cat.add_columns(column_map)?;

        let source_id = 1;

        cat.add_partition_group(source_id)?;

        let now = <Timestamp as Default>::default();
        let record_count = 100;

        let (tsfrag, data) = create_append_data(now, record_count);

        let append = Append::new(tsfrag, source_id, data);

        let added = cat.append(&append)?;

        let expected = 100;

        assert_eq!(expected, added);
    }}
}

#[test]
fn it_scans() {
    wrap_result! {{
        let dir = catalog_dir()?;
        let mut cat = Catalog::new(&dir)?;

        let mut column_map = ColumnMap::new();

        column_map.insert(2, Column::new(
            BlockStorage::Memmap(BlockType::U64Dense), "dense1"));
        column_map.insert(3, Column::new(
            BlockStorage::Memmap(BlockType::U64Sparse), "sparse1"));

        cat.add_columns(column_map)?;

        let source_id = 1;

        cat.add_partition_group(source_id)?;

        let now = <Timestamp as Default>::default();
        let record_count = 100;

        let (tsfrag, data) = create_append_data(now, record_count);

        let append = Append::new(tsfrag, source_id, data);

        let _ = cat.append(&append)?;

        //     select
        //         ts,
        //         sparse1
        //     from
        //         hyena
        //     where
        //         dense1 between 20 and 30
        //         and sparse1 in
        //         (
        //             11,
        //             13,
        //             15
        //         )
        //     )

        let scan = Scan::new(
            {
                let mut filters = HashMap::new();

                filters.insert(2, vec![
                    ScanFilter::U64(ScanFilterOp::GtEq(20)),
                    ScanFilter::U64(ScanFilterOp::LtEq(30)),
                ]);

                filters.insert(3, vec![
                    ScanFilter::U64(ScanFilterOp::In({
                        let mut set = HashSet::new();

                        set.insert(11);
                        set.insert(13);
                        set.insert(15);

                        set
                    }))
                ]);

                filters
            },
            Some(vec![0, 3]),
            None,
            None,
            None,
        );

        let result = cat.scan(&scan).with_context(|_| "scan failed")?;

        let expected = {
            let mut result = HashMap::new();
            result.insert(0, Some(Fragment::from(vec![*now + 22, *now + 26, *now + 30])));
            result.insert(3, Some(Fragment::from((vec![11_u64, 13, 15], vec![0_u32, 1, 2]))));

            ScanResult::from(result)
        };

        assert_eq!(expected, result);
    }}
}
