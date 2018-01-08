extern crate hyena_engine;
extern crate tempdir;

use hyena_engine::{Append, BlockData, BlockStorageType, BlockType, Catalog, Column, ColumnMap,
                   Fragment, SparseIndex, Timestamp, TimestampFragment};
use std::iter::{once, repeat};

#[macro_use]
mod common;

use common::{catalog_dir, get_columns, wrap_result};

// create 100 dense columns
// and 1k sparse columns of type U64
fn build_schema() -> ColumnMap {
    (2..102).map(|idx| {
        let name = format!("dense{}", idx);
        (idx, Column::new(BlockStorageType::Memmap(BlockType::U64Dense), &name))
    }).chain(
        (102..1102).map(|idx| {
            let name = format!("sparse{}", idx);
            (idx, Column::new(BlockStorageType::Memmap(BlockType::U64Sparse), &name))
        })
    ).collect()
}

#[test]
fn it_adds_100_dense_1k_sparse_columns() {
    wrap_result! {{
        let dir = catalog_dir()?;
        let mut cat = Catalog::new(&dir)?;

        let column_map = build_schema();

        cat.add_columns(column_map)?;

        let columns = get_columns(&cat);

        let expected = once((0_usize, "timestamp".to_string())).chain(
            once((1, "source_id".to_string()))
        ).chain(
            (2..102).map(|idx| (idx, format!("dense{}", idx)))
        ).chain((102..1102).map(|idx| (idx, format!("sparse{}", idx))))
        .collect::<Vec<_>>();

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

    let data = (2..102).map(|idx| (idx, densefrag.clone()))
    .chain((102..1102).filter_map(|idx| if (idx - 2) % 10 == 0 {
            Some( (idx, sparsefrag.clone()))
        } else {
            None
        }))
    .collect::<BlockData>();

    (tsfrag, data)
}

#[test]
fn it_fills_100_dense_1k_sparse_columns() {
    wrap_result! {{
        let dir = catalog_dir()?;
        let mut cat = Catalog::new(&dir)?;

        let column_map = build_schema();

        cat.add_columns(column_map)?;

        let source_id = 1;
        let record_count = 1_000;

        cat.add_partition_group(source_id)?;

        let (tsfrag, data) = create_append_data(Default::default(), record_count);

        let append = Append::new(tsfrag, source_id, data);
        let added = cat.append(&append)?;

        assert_eq!(added, record_count);
    }}
}
