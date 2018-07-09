use hyena_engine::{Append, BlockData, BlockStorage, BlockType, Catalog, Column, ColumnMap,
                   Fragment, Result, Scan, ScanFilter, ScanFilterOp, SparseIndex, Timestamp,
                   TimestampFragment};

use std::iter::repeat;
use hyena_common::collections::HashMap;

use criterion::Criterion;

use config::{catalog_dir, TempDir};

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

    column_map.insert(
        2,
        Column::new(BlockStorage::Memmap(BlockType::U64Dense), "dense1"),
    );
    column_map.insert(
        3,
        Column::new(BlockStorage::Memmap(BlockType::I32Dense), "dense2"),
    );
    column_map.insert(
        4,
        Column::new(BlockStorage::Memmap(BlockType::U64Sparse), "sparse1"),
    );
    column_map.insert(
        5,
        Column::new(BlockStorage::Memmap(BlockType::I64Sparse), "sparse2"),
    );

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

fn filter_bench(c: &mut Criterion) {
    let (_now, _dir, cat) = prepare_data(10_000).unwrap();

    let scan = Scan::new(
        {
            let mut filters = HashMap::new();

            filters.insert(
                2,
                vec![
                    vec![
                        ScanFilter::U64(ScanFilterOp::Gt(10)),
                        ScanFilter::U64(ScanFilterOp::LtEq(25)),
                    ]
                ],
            );

            Some(filters)
        },
        None,
        None,
        None,
        None,
    );

    c.bench_function("filter 10k", move |b| {
        b.iter(|| {
            cat.scan(&scan).unwrap();
        })
    });
}

fn filter_materialize_dense_bench(c: &mut Criterion) {
    let (_now, _dir, cat) = prepare_data(10_000).unwrap();

    let scan = Scan::new(
        {
            let mut filters = HashMap::new();

            filters.insert(
                2,
                vec![
                    vec![
                        ScanFilter::U64(ScanFilterOp::Gt(10)),
                        ScanFilter::U64(ScanFilterOp::LtEq(25)),
                    ]
                ],
            );

            Some(filters)
        },
        Some(vec![0]),
        None,
        None,
        None,
    );

    c.bench_function("filter and materialize dense(u64) 10k", move |b| {
        b.iter(|| {
            cat.scan(&scan).unwrap();
        })
    });
}

fn filter_materialize_sparse_bench(c: &mut Criterion) {
    let (_now, _dir, cat) = prepare_data(10_000).unwrap();

    let scan = Scan::new(
        {
            let mut filters = HashMap::new();

            filters.insert(
                2,
                vec![
                    vec![
                        ScanFilter::U64(ScanFilterOp::Gt(10)),
                        ScanFilter::U64(ScanFilterOp::LtEq(25)),
                    ]
                ],
            );

            Some(filters)
        },
        Some(vec![4]),
        None,
        None,
        None,
    );

    c.bench_function("filter and materialize sparse(u64) 10k", move |b| {
        b.iter(|| {
            cat.scan(&scan).unwrap();
        })
    });
}

criterion_group!(
    benches,
    filter_bench,
    filter_materialize_dense_bench,
    filter_materialize_sparse_bench
);
