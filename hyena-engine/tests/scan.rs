extern crate hyena_engine;
extern crate hyena_test;
extern crate failure;

use hyena_engine::{Append, BlockData, BlockStorageType, BlockType, Catalog, Column, ColumnMap,
                   Fragment, Result, Scan, ScanFilter, ScanFilterOp, ScanResult,
                   SparseIndex, Timestamp, TimestampFragment};

use hyena_test::tempfile::VolatileTempDir as TempDir;

use std::iter::repeat;
use std::collections::HashMap;

#[macro_use]
mod common;

use common::{catalog_dir, wrap_result};

fn create_append_data(now: Timestamp, record_count: usize) -> (TimestampFragment, BlockData) {
    let tsfrag = TimestampFragment::from(
        repeat(())
            .take(record_count)
            .enumerate()
            .map(|(i, _)| *now + i as u64)
            .collect::<Vec<u64>>(),
    );

    let dense1frag = Fragment::from(
        repeat(())
            .take(record_count)
            .enumerate()
            .map(|(i, _)| i as u64)
            .collect::<Vec<_>>(),
    );

    let dense2frag = Fragment::from(
        repeat(())
            .take(record_count)
            .enumerate()
            .map(|(i, _)| i as i32)
            .collect::<Vec<_>>(),
    );

    let sparse1frag = Fragment::from((
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

    let sparse2frag = Fragment::from((
        repeat(())
            .take(record_count / 2)
            .enumerate()
            .map(|(i, _)| i as i64)
            .collect::<Vec<_>>(),
        repeat(())
            .take(record_count / 2)
            .enumerate()
            .map(|(i, _)| i as u32 * 2)
            .collect::<Vec<SparseIndex>>(),
    ));

    let mut data = BlockData::new();

    data.insert(2, dense1frag);
    data.insert(3, dense2frag);
    data.insert(4, sparse1frag);
    data.insert(5, sparse2frag);

    (tsfrag, data)
}

fn prepare_data<'cat>(record_count: usize) -> Result<(Timestamp, TempDir, Catalog<'cat>)> {
    let dir = catalog_dir()?;
    let mut cat = Catalog::new(&dir)?;

    let mut column_map = ColumnMap::new();

    column_map.insert(2, Column::new(
        BlockStorageType::Memmap(BlockType::U64Dense), "dense1"));
    column_map.insert(3, Column::new(
        BlockStorageType::Memmap(BlockType::I32Dense), "dense2"));
    column_map.insert(4, Column::new(
        BlockStorageType::Memmap(BlockType::U64Sparse), "sparse1"));
    column_map.insert(5, Column::new(
        BlockStorageType::Memmap(BlockType::I64Sparse), "sparse2"));

    cat.add_columns(column_map)?;

    let source_id = 10;

    cat.add_partition_group(source_id)?;

    let now = <Timestamp as Default>::default();

    let (tsfrag, data) = create_append_data(now, record_count);

    let append = Append::new(tsfrag, source_id, data);

    let added = cat.append(&append)?;

    assert_eq!(added, record_count);

    Ok((now, dir, cat))
}

#[test]
fn it_scans_with_dense_filter() {
    wrap_result! {{
        let (now, _dir, cat) = prepare_data(100)?;

        //     select
        //         ts,
        //         dense2,
        //         sparse2
        //     from
        //         hyena
        //     where
        //         dense1 > 10 and dense1 <= 25

        let scan = Scan::new(
            {
                let mut filters = HashMap::new();

                filters.insert(2, vec![
                    ScanFilter::U64(ScanFilterOp::Gt(10)),
                    ScanFilter::U64(ScanFilterOp::LtEq(25)),
                ]);

                filters
            },
            Some(vec![0, 3, 5]),
            None,
            None,
            None,
        );

        let result = cat.scan(&scan)?;

        let expected = {
            let mut result = HashMap::new();
            result.insert(0, Some(Fragment::from(
                (11..26).map(|i| *now + i).collect::<Vec<u64>>()
            )));
            result.insert(3, Some(Fragment::from((11_i32..26).collect::<Vec<_>>())));
            result.insert(5, Some(Fragment::from((
                vec![6_i64, 7, 8, 9, 10, 11, 12],
                vec![1_u32, 3, 5, 7, 9, 11, 13]))
            ));

            ScanResult::from(result)
        };

        assert_eq!(expected, result);
    }}
}

#[test]
fn it_scans_with_sparse_filter() {
    wrap_result! {{
        let (now, _dir, cat) = prepare_data(100)?;

        //     select
        //         ts,
        //         dense2,
        //         sparse2
        //     from
        //         hyena
        //     where
        //         sparse1 > 10 and sparse1 <= 25

        let scan = Scan::new(
            {
                let mut filters = HashMap::new();

                filters.insert(4, vec![
                    ScanFilter::U64(ScanFilterOp::Gt(10)),
                    ScanFilter::U64(ScanFilterOp::LtEq(25)),
                ]);

                filters
            },
            Some(vec![0, 3, 5]),
            None,
            None,
            None,
        );

        let result = cat.scan(&scan)?;

        let expected = {
            let mut result = HashMap::new();
            result.insert(0, Some(Fragment::from(
                (0_u64..15).map(|i| 22 + *now + i * 2).collect::<Vec<u64>>()
            )));
            result.insert(3, Some(Fragment::from(
                (0_i32..15).map(|i| 22 + i * 2).collect::<Vec<_>>()
            )));
            result.insert(5, Some(Fragment::from((
                (11_i64..26).collect::<Vec<_>>(),
                (0..15).collect::<Vec<_>>()))
            ));

            ScanResult::from(result)
        };

        assert_eq!(expected, result);
    }}
}
