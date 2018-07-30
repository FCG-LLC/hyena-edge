use hyena_engine::{Append, BlockData, BlockStorage, BlockType, Catalog, Column,
                   Fragment, Result, Scan, ScanFilter, ScanFilterOp, Timestamp,
                   TimestampFragment, Regex};

use std::iter::repeat;
use hyena_common::collections::HashMap;

use criterion::Criterion;

use config::{catalog_dir, TempDir};

mod data;

fn gen_ip() -> impl Iterator<Item = (u64, u64)> {
    use self::data::IP_ADDRESSES;

    IP_ADDRESSES
        .chunks(2)
        .cycle()
        .map(|ip| (ip[0], ip[1]))
}

fn gen_text() -> impl Iterator<Item = &'static str> {
    use self::data::TEXT_DATA;

    TEXT_DATA
        .iter()
        .map(|s| *s)
        .cycle()
}


//  ID |   Name    |        Type
// ----+-----------+---------------------
//  0  | timestamp |    Memmap(U64Dense)
//  1  | source_id |    Memory(U32Dense)
//  2  | ip1       |    Memmap(U64Dense)
//  3  | ip2       |    Memmap(U64Dense)
//  4  | text      | Memmap(StringDense)

const IP1_COLUMN: usize = 2;
const IP2_COLUMN: usize = 3;
const TEXT_COLUMN: usize = 4;

fn create_append_data(now: Timestamp, record_count: usize) -> (TimestampFragment, BlockData)
{
    let tsfrag = TimestampFragment::from(
        repeat(())
            .take(record_count)
            .enumerate()
            .map(|(i, _)| *now + i as u64)
            .collect::<Vec<_>>(),
    );

    let (ip1, ip2) = gen_ip()
        .take(record_count)
        .unzip::<_, _, Vec<_>, Vec<_>>();

    let ip1 = Fragment::from(ip1);
    let ip2 = Fragment::from(ip2);

    let textfrag = Fragment::from(
        gen_text().take(record_count).map(ToOwned::to_owned).collect::<Vec<String>>());

    let data = hashmap! {
        IP1_COLUMN => ip1,
        IP2_COLUMN => ip2,
        TEXT_COLUMN => textfrag,
    };

    (tsfrag, data)
}

fn prepare_data<'cat>(record_count: usize) -> Result<(Timestamp, TempDir, Catalog<'cat>)>
{
    let dir = catalog_dir()?;
    let mut cat = Catalog::new(&dir)?;

    let column_map = hashmap! {
        IP1_COLUMN => Column::new(BlockStorage::Memmap(BlockType::U64Dense), "ip1"),
        IP2_COLUMN => Column::new(BlockStorage::Memmap(BlockType::U64Dense), "ip2"),
        TEXT_COLUMN => Column::new(BlockStorage::Memmap(BlockType::StringDense), "text"),
    };

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

fn filter_string_bench(c: &mut Criterion) {
    c.bench_function_over_inputs("filter string(185)", |b, &&size| {
        let (_now, _dir, cat) = prepare_data(size).unwrap();

        let scan = Scan::new(
            {
                let mut filters = HashMap::new();

                filters.insert(
                    TEXT_COLUMN,
                    vec![
                        vec![ScanFilter::String(ScanFilterOp::Matches(
                                Regex::new("201188").unwrap())),
                        ]
                    ],
                );

                Some(filters)
            },
            Some(vec![]),
            None,
            None,
            None,
        );

        b.iter(move || {
            cat.scan(&scan).unwrap();
        })
    }, &[10_000, 100_000, 500_000, 1_000_000]);
}

fn filter_materialize_string_bench(c: &mut Criterion) {
    c.bench_function_over_inputs("filter and materialize string(185)", |b, &&size| {
        let (_now, _dir, cat) = prepare_data(size).unwrap();

        let scan = Scan::new(
            {
                let mut filters = HashMap::new();

                filters.insert(
                    TEXT_COLUMN,
                    vec![
                        vec![ScanFilter::String(ScanFilterOp::Matches(
                                Regex::new("201188").unwrap())),
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

        b.iter(move || {
            cat.scan(&scan).unwrap();
        })
    }, &[10_000, 100_000, 500_000, 1_000_000]);
}


criterion_group!(
    benches,
    filter_string_bench,
    filter_materialize_string_bench,
);
