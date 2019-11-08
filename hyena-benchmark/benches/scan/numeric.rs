use hyena_engine::{Append, BlockData, BlockStorage, BlockType, Catalog, Column,
                   Fragment, Result, Scan, ScanFilter, ScanFilterOp, SparseIndex, Timestamp,
                   TimestampFragment, StreamConfig};

use std::iter::repeat;
use hyena_common::collections::HashMap;

use criterion::Criterion;

use crate::config::{catalog_dir, TempDir};

//  ID |   Name    |        Type
// ----+-----------+---------------------
//  0  | timestamp |    Memmap(U64Dense)
//  1  | source_id |    Memory(U32Dense)
//  2  | dense1    |    Memmap(U64Dense)
//  3  | dense2    |    Memmap(I32Dense)
//  4  | sparse1   |    Memmap(U64Sparse)
//  5  | sparse2   |    Memmap(I64Sparse)

const DENSE1_COLUMN: usize = 2;
const DENSE2_COLUMN: usize = 3;
const SPARSE1_COLUMN: usize = 4;
const SPARSE2_COLUMN: usize = 5;

fn create_append_data(now: Timestamp, record_count: usize, fill_ratio: u32) ->
(TimestampFragment, BlockData) {
    let tsfrag = TimestampFragment::from(
        repeat(())
            .take(record_count)
            .enumerate()
            .map(|(i, _)| *now + i as u64)
            .collect::<Vec<_>>(),
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
            .take(record_count / fill_ratio as usize)
            .enumerate()
            .map(|(i, _)| i as u64)
            .collect::<Vec<u64>>(),
        repeat(())
            .take(record_count / fill_ratio as usize)
            .enumerate()
            .map(|(i, _)| i as u32 * fill_ratio)
            .collect::<Vec<SparseIndex>>(),
    ));

    let sparse2frag = Fragment::from((
        repeat(())
            .take(record_count / fill_ratio as usize)
            .enumerate()
            .map(|(i, _)| i as i64)
            .collect::<Vec<_>>(),
        repeat(())
            .take(record_count / fill_ratio as usize)
            .enumerate()
            .map(|(i, _)| i as u32 * fill_ratio)
            .collect::<Vec<SparseIndex>>(),
    ));

    let data = hashmap! {
        DENSE1_COLUMN => dense1frag,
        DENSE2_COLUMN => dense2frag,
        SPARSE1_COLUMN => sparse1frag,
        SPARSE2_COLUMN => sparse2frag,
    };

    (tsfrag, data)
}

fn prepare_data<'cat>(record_count: usize) -> Result<(Timestamp, TempDir, Catalog<'cat>)> {
    let dir = catalog_dir()?;
    let mut cat = Catalog::new(&dir)?;

    const FILL_RATIO: u32 = 2;

    let column_map = hashmap! {
        DENSE1_COLUMN => Column::new(BlockStorage::Memmap(BlockType::U64Dense), "dense1"),
        DENSE2_COLUMN => Column::new(BlockStorage::Memmap(BlockType::I32Dense), "dense2"),
        SPARSE1_COLUMN => Column::new(BlockStorage::Memmap(BlockType::U64Sparse), "sparse1"),
        SPARSE2_COLUMN => Column::new(BlockStorage::Memmap(BlockType::I64Sparse), "sparse2"),
    };

    cat.add_columns(column_map)?;

    let source_id = 10;

    cat.add_partition_group(source_id)?;

    let now = <Timestamp as Default>::default();

    let (tsfrag, data) = create_append_data(now, record_count, FILL_RATIO);

    let append = Append::new(tsfrag, source_id, data);

    let added = cat.append(&append)?;

    assert_eq!(added, record_count);

    Ok((now, dir, cat))
}

fn filter_dense_bench(c: &mut Criterion) {
    c.bench_function_over_inputs("filter numeric dense", |b, &&size| {
        let (_now, _dir, cat) = prepare_data(size).unwrap();

        let scan = Scan::new(
            {
                let mut filters = HashMap::new();

                filters.insert(
                    DENSE1_COLUMN,
                    vec![
                        vec![
                            ScanFilter::U64(ScanFilterOp::Gt(size as u64 / 6)),
                            ScanFilter::U64(ScanFilterOp::LtEq(size as u64 / 6 * 4)),
                        ]
                    ],
                );

                Some(filters)
            },
            Some(vec![]),
            None,
            None,
            None,
            None,
        );

        b.iter(move || {
            cat.scan(&scan).unwrap();
        })
    }, &[10_000, 100_000, 500_000, 1_000_000]);
}

fn filter_materialize_dense_bench(c: &mut Criterion) {
    c.bench_function_over_inputs("filter and materialize numeric dense", |b, &&size| {
        let (_now, _dir, cat) = prepare_data(size).unwrap();

        let scan = Scan::new(
            {
                let mut filters = HashMap::new();

                filters.insert(
                    DENSE1_COLUMN,
                    vec![
                        vec![
                            ScanFilter::U64(ScanFilterOp::Gt(size as u64 / 6)),
                            ScanFilter::U64(ScanFilterOp::LtEq(size as u64 / 6 * 4)),
                        ]
                    ],
                );

                Some(filters)
            },
            Some(vec![DENSE1_COLUMN, DENSE2_COLUMN]),
            None,
            None,
            None,
            None,
        );

        b.iter(move || {
            cat.scan(&scan).unwrap();
        })
    }, &[10_000, 100_000, 500_000, 1_000_000]);
}

fn filter_materialize_stream_dense_bench(c: &mut Criterion) {
    const STREAM_ROW_LIMIT: usize = 131072;
    const STREAM_THRESHOLD: usize = 1_000;

    c.bench_function_over_inputs("streaming filter and materialize numeric dense", |b, &&size| {
        let (_now, _dir, cat) = prepare_data(size).unwrap();

        let scan = Scan::new(
            {
                let mut filters = HashMap::new();

                filters.insert(
                    2,
                    vec![
                        vec![
                            ScanFilter::U64(ScanFilterOp::Gt(size as u64 / 6)),
                            ScanFilter::U64(ScanFilterOp::LtEq(size as u64 / 6 * 4)),
                        ]
                    ],
                );

                Some(filters)
            },
            Some(vec![2, 3]),
            None,
            None,
            None,
            Some(StreamConfig::new(STREAM_ROW_LIMIT, STREAM_THRESHOLD, None)),
        );

        b.iter(move || {
            let mut scan = scan.clone();

            while {
                let result = cat.scan(&scan).unwrap();
                scan.set_stream_state(result.stream_state_data()).unwrap()
            } {};
        })
    }, &[10_000, 100_000, 500_000, 1_000_000]);
}

fn filter_sparse_bench(c: &mut Criterion) {
    c.bench_function_over_inputs("filter numeric sparse", |b, &&size| {
        let (_now, _dir, cat) = prepare_data(size).unwrap();

        let scan = Scan::new(
            {
                let mut filters = HashMap::new();

                filters.insert(
                    SPARSE1_COLUMN,
                    vec![
                        vec![
                            ScanFilter::U64(ScanFilterOp::Gt(size as u64 / 6)),
                            ScanFilter::U64(ScanFilterOp::LtEq(size as u64 / 6 * 4)),
                        ]
                    ],
                );

                Some(filters)
            },
            Some(vec![]),
            None,
            None,
            None,
            None,
        );

        b.iter(move || {
            cat.scan(&scan).unwrap();
        })
    }, &[10_000, 100_000, 500_000, 1_000_000]);
}

fn filter_materialize_sparse_bench(c: &mut Criterion) {
    c.bench_function_over_inputs("filter and materialize numeric sparse", |b, &&size| {
        let (_now, _dir, cat) = prepare_data(size).unwrap();

        let scan = Scan::new(
            {
                let mut filters = HashMap::new();

                filters.insert(
                    SPARSE1_COLUMN,
                    vec![
                        vec![
                            ScanFilter::U64(ScanFilterOp::Gt(size as u64 / 6)),
                            ScanFilter::U64(ScanFilterOp::LtEq(size as u64 / 6 * 4)),
                        ]
                    ],
                );

                Some(filters)
            },
            Some(vec![SPARSE1_COLUMN, SPARSE2_COLUMN]),
            None,
            None,
            None,
            None,
        );

        b.iter(move || {
            cat.scan(&scan).unwrap();
        })
    }, &[10_000, 100_000, 500_000, 1_000_000]);
}

fn filter_materialize_stream_sparse_bench(c: &mut Criterion) {
    const STREAM_ROW_LIMIT: usize = 131072;
    const STREAM_THRESHOLD: usize = 1_000;

    c.bench_function_over_inputs("streaming filter and materialize numeric sparse", |b, &&size| {
        let (_now, _dir, cat) = prepare_data(size).unwrap();

        let scan = Scan::new(
            {
                let mut filters = HashMap::new();

                filters.insert(
                    4,
                    vec![
                        vec![
                            ScanFilter::U64(ScanFilterOp::Gt(size as u64 / 6)),
                            ScanFilter::U64(ScanFilterOp::LtEq(size as u64 / 6 * 4)),
                        ]
                    ],
                );

                Some(filters)
            },
            Some(vec![4, 5]),
            None,
            None,
            None,
            Some(StreamConfig::new(STREAM_ROW_LIMIT, STREAM_THRESHOLD, None)),
        );

        b.iter(move || {
            let mut scan = scan.clone();

            while {
                let result = cat.scan(&scan).unwrap();
                scan.set_stream_state(result.stream_state_data()).unwrap()
            } {};
        })
    }, &[10_000, 100_000, 500_000, 1_000_000]);
}

criterion_group!(
    benches,
    filter_dense_bench,
    filter_sparse_bench,
    filter_materialize_dense_bench,
    filter_materialize_sparse_bench,
    filter_materialize_stream_dense_bench,
    filter_materialize_stream_sparse_bench,
);
