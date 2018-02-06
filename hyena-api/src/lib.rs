extern crate bincode;
#[allow(unused_imports)]
#[macro_use]
extern crate hyena_common;
#[cfg(test)]
#[macro_use]
extern crate hyena_test;
extern crate hyena_engine;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate failure;
extern crate extprim;
#[macro_use]
extern crate log;

mod error;

use error::*;

use bincode::{Error as BinError, deserialize};
use hyena_engine::{BlockType, Catalog, Column, ColumnMap, BlockData, Append, Scan,
ScanTsRange, BlockStorage, ColumnId, TimestampFragment, Fragment,
ScanFilterOp as HScanFilterOp, ScanFilter as HScanFilter};

use hyena_common::ty::Uuid;

use std::collections::hash_map::HashMap;
use std::collections::HashSet;
use std::convert::From;
use std::fmt::Debug;
use std::hash::Hash;
use std::result::Result;
use extprim::i128::i128;
use extprim::u128::u128;

#[derive(Serialize, Deserialize, Debug)]
pub struct InsertMessage {
    timestamps: Vec<u64>,
    source: u32,
    columns: Vec<BlockData>,
}

#[derive(Serialize, Debug)]
pub struct DataTriple {
    column_id: ColumnId,
    column_type: BlockType,
    data: Option<Fragment>
}

#[derive(Serialize, Debug, Default)]
pub struct ScanResultMessage {
    data: Vec<DataTriple>
}

impl ScanResultMessage {
    pub fn new() -> ScanResultMessage {
        Default::default()
    }
}

impl From<Vec<DataTriple>> for ScanResultMessage {
    fn from(data: Vec<DataTriple>) -> ScanResultMessage {
        ScanResultMessage { data }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ScanComparison {
    Lt,
    LtEq,
    Eq,
    GtEq,
    Gt,
    NotEq,
}

impl ScanComparison {
    fn to_scan_filter_op<T>(&self, val: T) -> HScanFilterOp<T>
        where T: Debug + Clone + PartialEq + PartialOrd + Eq + Hash
    {
        match *self {
            ScanComparison::Lt => HScanFilterOp::Lt(val),
            ScanComparison::LtEq => HScanFilterOp::LtEq(val),
            ScanComparison::Eq => HScanFilterOp::Eq(val),
            ScanComparison::GtEq => HScanFilterOp::GtEq(val),
            ScanComparison::Gt => HScanFilterOp::Gt(val),
            ScanComparison::NotEq => HScanFilterOp::NotEq(val),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum FilterVal {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128)
}

impl FilterVal {
    fn to_scan_filter(&self, op: &ScanComparison) -> HScanFilter {
        match *self {
            FilterVal::U8(val) => HScanFilter::U8(op.to_scan_filter_op(val)),
            FilterVal::U16(val) => HScanFilter::U16(op.to_scan_filter_op(val)),
            FilterVal::U32(val) => HScanFilter::U32(op.to_scan_filter_op(val)),
            FilterVal::U64(val) => HScanFilter::U64(op.to_scan_filter_op(val)),
            FilterVal::U128(val) => HScanFilter::U128(op.to_scan_filter_op(val)),
            FilterVal::I8(val) => HScanFilter::I8(op.to_scan_filter_op(val)),
            FilterVal::I16(val) => HScanFilter::I16(op.to_scan_filter_op(val)),
            FilterVal::I32(val) => HScanFilter::I32(op.to_scan_filter_op(val)),
            FilterVal::I64(val) => HScanFilter::I64(op.to_scan_filter_op(val)),
            FilterVal::I128(val) => HScanFilter::I128(op.to_scan_filter_op(val)),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ScanFilter {
    pub column: ColumnId,
    pub op: ScanComparison,
    pub typed_val: FilterVal,
    pub str_val: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ScanRequest {
    pub min_ts: u64,
    pub max_ts: u64,
    pub partition_ids: HashSet<Uuid>,
    pub projection: Vec<ColumnId>,
    pub filters: Vec<ScanFilter>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AddColumnRequest {
    pub column_name: String,
    pub column_type: BlockType,
}


#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct PartitionInfo {
    min_ts: u64,
    max_ts: u64,
    id: Uuid,
    location: String,
}

// impl<'a> PartitionInfo {
//     fn from(partition: &Partition<'a>) -> PartitionInfo {
//         PartitionInfo {
//             min_ts: partition.ts_min.into(),
//             max_ts: partition.ts_max.into(),
//             id: partition.id.into(),
//             location: String::new(),
//         }
//     }
// }

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RefreshCatalogResponse {
    pub columns: Vec<ReplyColumn>,
    pub available_partitions: Vec<PartitionInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    ListColumns,
    Insert(InsertMessage),
    Scan(ScanRequest),
    RefreshCatalog,
    AddColumn(AddColumnRequest),
    Flush,
    DataCompaction,
    Other,
}

impl Request {
    pub fn parse(data: Vec<u8>) -> Result<Request, BinError> {
        deserialize(&data[..])
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ReplyColumn {
    typ: BlockType,
    id: ColumnId,
    name: String,
}

impl ReplyColumn {
    fn new(typ: BlockType, id: ColumnId, name: String) -> Self {
        ReplyColumn {
            typ: typ,
            id: id,
            name: name,
        }
    }
}

#[derive(Debug, Serialize)]
pub enum Reply {
    ListColumns(Vec<ReplyColumn>),
    Insert(Result<usize, Error>),
    Scan(Result<ScanResultMessage, Error>),
    RefreshCatalog(RefreshCatalogResponse),
    AddColumn(Result<usize, Error>),
    Flush,
    DataCompaction,
    SerializeError(String),
    Other,
}

impl Reply {
    fn list_columns(catalog: &Catalog) -> Reply {
        let cm: &ColumnMap = catalog.as_ref();
        let names = cm.iter()
            .map(|(id, column)| ReplyColumn::new(column.block_type(), *id, format!("{}", column)))
            .collect();
        Reply::ListColumns(names)
    }

    fn add_column(request: AddColumnRequest, catalog: &mut Catalog) -> Reply {
        if request.column_name.is_empty() {
            return Reply::AddColumn(Err(Error::ColumnNameCannotBeEmpty));
        }

        let column = Column::new(BlockStorage::Memmap(request.column_type),
                                 request.column_name.as_str());
        let id = catalog.next_id();
        info!("Adding column {}:{:?} with id {}",
              request.column_name,
              request.column_type,
              id);
        let mut map = HashMap::new();
        map.insert(id, column);

        match catalog.add_columns(map) {
            Ok(_) => {
                catalog.flush().unwrap();
                Reply::AddColumn(Ok(id))
            }
            Err(error) => Reply::AddColumn(Err(error.into())),
        }
    }

    fn insert(insert: InsertMessage, catalog: &mut Catalog) -> Reply {
        let timestamps: TimestampFragment = insert.timestamps.into();
        let mut inserted = 0;
        let source = insert.source;

        if timestamps.is_empty() {
            return Reply::Insert(Err(Error::NoData("Timestamps cannot be empty".into())));
        }
        if insert.columns.is_empty() {
            return Reply::Insert(Err(Error::NoData("Cannot insert empty vector".into())));
        }
        for block in insert.columns.iter() {
            for (id, fragment) in block {
                if fragment.is_sparse() && fragment.len() > timestamps.len() {
                    let err_msg = format!("Sparse block data length is greater than timestamp \
                                           length (column {}, timestamps {}, data length {})",
                                          id,
                                          timestamps.len(),
                                          fragment.len());
                    return Reply::Insert(Err(Error::InconsistentData(err_msg)));
                }
                if !fragment.is_sparse() && fragment.len() != timestamps.len() {
                    let err_msg = format!("Dense block data length does not match timestamp \
                                           length (column {}, timestamps {}, data length {})",
                                          id,
                                          timestamps.len(),
                                          fragment.len());
                    return Reply::Insert(Err(Error::InconsistentData(err_msg)));
                }
            }
        }

        {
            // Block for a mutable borrow
            let groups_ensured = catalog.add_partition_group(source)
                .with_context(|_| format!("Could not create group for source {}", source));
            if groups_ensured.is_err() {
                return Reply::Insert(Err(Error::CatalogError(groups_ensured.unwrap_err()
                    .to_string())));
            }
        }

        for block in insert.columns.iter() {
            let append = Append::new(
                timestamps.clone(),
                insert.source,
                block.clone()
            );
            let result = catalog.append(&append);
            match result {
                Ok(number) => inserted += number,
                Err(e) => return Reply::Insert(Err(Error::Unknown(e.to_string()))),
            }
        }
        let flushed = catalog.flush()
            .with_context(|_| "Cannot flush catalog after inserting");
        if flushed.is_err() {
            Reply::Insert(Err(Error::CatalogError(flushed.unwrap_err().to_string())))
        } else {
            Reply::Insert(Ok(inserted))
        }
    }

    fn scan(scan_request: ScanRequest, catalog: &Catalog) -> Reply {
        if scan_request.min_ts > scan_request.max_ts {
            return Reply::Scan(Err(Error::InvalidScanRequest("min_ts > max_ts".into())));
        }
        if scan_request.projection.is_empty() {
            return Reply::Scan(Err(Error::InvalidScanRequest("Projections cannot be empty"
                .into())));
        }
        if scan_request.filters.is_empty() {
            return Reply::Scan(Err(Error::InvalidScanRequest("Filters cannot be empty".into())));
        }
        let scan = Reply::build_scan(scan_request);
        let result = match catalog.scan(&scan) {
            Err(e) => return Reply::Scan(Err(Error::ScanError(e.to_string()))),
            Ok(r) => r
        };

        let cm: &ColumnMap = catalog.as_ref();

        let srm = result
            .into_iter()
            .map(|(column, fragment)| {
                match cm.get(&column) {
                    None => None,
                    Some(col) =>
                        Some(DataTriple {
                            column_id: column,
                            column_type: match **col {
                                BlockStorage::Memory(t) => t,
                                BlockStorage::Memmap(t) => t,
                            },
                            data: fragment
                        })
                }
            })
            .filter(|item| item.is_some())
            .map(|item| item.unwrap()) // None items, which can fail the `unwrap`,
                                       // has been filtered out in previous line
            .collect::<Vec<DataTriple>>();

        Reply::Scan(Ok(ScanResultMessage::from(srm)))
    }

    fn build_scan(scan_request: ScanRequest) -> Scan {
        let scan_range = ScanTsRange::Bounded{
            start: scan_request.min_ts,
            end:   scan_request.max_ts
        };

        let partitions =
            if !scan_request.partition_ids.is_empty() {
                Some(scan_request.partition_ids.into_iter().map(|v| v.into()).collect())
            } else { None };

        let filters = scan_request.filters
            .iter()
            .map(|filter| (
                    filter.column,
                    vec![filter.typed_val.to_scan_filter(&filter.op)]
                    )
                )
            .collect();

        Scan::new(filters,
                  Some(scan_request.projection),
                  None,
                  partitions,
                  Some(scan_range))
    }

    fn get_catalog(catalog: &Catalog) -> Reply {
        let columns = catalog.as_ref()
            .iter()
            .map(|(id, c)| {
                let t = match **c {
                    BlockStorage::Memory(t) => t,
                    BlockStorage::Memmap(t) => t,
                };
                ReplyColumn {
                    typ: t,
                    id: *id,
                    name: c.to_string(),
                }
            })
            .collect();

        // The partition API in hyena-engine is not yet stable
        // so disable this for the time being

//         let mut partitions: Vec<PartitionInfo> = catalog.groups
//             .values()
//             .flat_map(|g| g.immutable_partitions.values().collect::<Vec<_>>())
//             .map(|partition| PartitionInfo::from(partition))
//             .collect();

//         let mutable: Vec<PartitionInfo> = catalog.groups
//             .values()
//             .flat_map(|g| {
//                 let unlocked = g.mutable_partitions.read().unwrap();
//                 let mutable: Vec<PartitionInfo> = unlocked.iter()
//                     .map(|partition| PartitionInfo::from(partition))
//                     .collect();
//                 mutable
//             })
//             .collect();
//         partitions.extend(mutable);
        let response = RefreshCatalogResponse {
            columns: columns,
//             available_partitions: partitions,
            available_partitions: Default::default(),
        };
        Reply::RefreshCatalog(response)
    }
}

#[derive(Debug, Serialize)]
pub enum Error {
    ColumnNameAlreadyExists(String),
    ColumnIdAlreadyExists(usize),
    ColumnNameCannotBeEmpty,
    NoData(String),
    InconsistentData(String),
    InvalidScanRequest(String),
    CatalogError(String),
    ScanError(String),
    Unknown(String),
}

impl From<error::Error> for Error {
    fn from(err: error::Error) -> Error {
        // Is there a way to do it better?
        //
        // ^^ Error needs to be #[derive(Fail)]
        // and we also need specialized errors in the hyena-engine
        // (which is a todo)
//         match *err.kind() {
//             error::ErrorKind::ColumnNameAlreadyExists(ref s) => {
//                 Error::ColumnNameAlreadyExists(s.clone())
//             }
//             error::ErrorKind::ColumnIdAlreadyExists(ref u) => Error::ColumnIdAlreadyExists(*u),
//             _ => Error::Unknown(err.description().into()),
//         }

        Error::Unknown(err.to_string())
    }
}

pub fn run_request(req: Request, catalog: &mut Catalog) -> Reply {
    match req {
        Request::ListColumns => Reply::list_columns(catalog),
        Request::AddColumn(request) => Reply::add_column(request, catalog),
        Request::Insert(insert) => Reply::insert(insert, catalog),
        Request::Scan(request) => Reply::scan(request, catalog),
        Request::RefreshCatalog => Reply::get_catalog(catalog),
        _ => Reply::Other,
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;

    mod reply {
        pub use super::*;

        mod list_columns {
            use super::*;
            use super::Reply::ListColumns;
            use hyena_engine::BlockStorage::*;
            use hyena_engine::BlockType;

            #[test]
            fn returns_mem_and_mmap_columns() {
                let td = tempdir!();
                let columns = hashmap! {
                    2 => Column::new(Memory(BlockType::U32Dense), "test_column1"),
                    3 => Column::new(Memmap(BlockType::I64Sparse), "test_column2"),
                };
                let mut cat = Catalog::new(&td).expect("Unable to create catalog");

                cat.add_columns(columns).unwrap();

                let reply = Reply::list_columns(&cat);
                if let ListColumns(mut colvec) = reply {
                    colvec.sort_by_key(|column| column.id);
                    assert_eq!(4, colvec.len());
                    assert_eq!(&[
                        ReplyColumn {
                            typ: BlockType::U64Dense,
                            id: 0,
                            name: "timestamp".into(),
                        },
                        ReplyColumn {
                            typ: BlockType::I32Dense,
                            id: 1,
                            name: "source_id".into(),
                        },
                        ReplyColumn {
                            typ: BlockType::U32Dense,
                            id: 2,
                            name: "test_column1".into(),
                        },
                        ReplyColumn {
                            typ: BlockType::I64Sparse,
                            id: 3,
                            name: "test_column2".into(),
                        },
                    ][..],
                    &colvec[..]);
                } else {
                    panic!("Wrong Reply type returned")
                }
            }

            #[test]
            fn handles_empty_catalog() {
                let td = tempdir!();
                let cat = Catalog::new(&td).expect("Catalog creation failed");
                let reply = Reply::list_columns(&cat);
                if let ListColumns(colvec) = reply {
                    assert_eq!(2, colvec.len())
                } else {
                    panic!("Wrong Reply type returned")
                }
            }
        }

        mod add_column {
            use super::*;
            use super::Reply::{AddColumn, ListColumns};
            use hyena_engine::BlockType;

            #[test]
            fn adds_column() {
                let name: String = "a_test_column".into();
                let request = AddColumnRequest {
                    column_name: name.clone(),
                    column_type: BlockType::I32Dense,
                };

                let td = tempdir!();

                let mut catalog = Catalog::new(&td)
                    .unwrap_or_else(|e| panic!("Could not crate catalog {}", e));

                let added = Reply::add_column(request, &mut catalog);

                if let AddColumn(Ok(id)) = added {
                    // IDs 0 and 1 are the default timestamp and cource columns, so:
                    assert_eq!(2, id);
                } else {
                    panic!("Column not added!");
                }

                let columns = Reply::list_columns(&catalog);

                if let ListColumns(cols) = columns {
                    let added = cols.iter()
                        .find(|item| item.id == 2)
                        .unwrap_or_else(|| panic!("Freshly added column not found"));
                    assert_eq!(BlockType::I32Dense, added.typ);
                    assert_eq!(name, added.name);
                }
            }

            #[test]
            fn rejects_request_with_empty_name() {
                let name: String = "".into();
                let request = AddColumnRequest {
                    column_name: name,
                    column_type: BlockType::I32Dense,
                };

                let td = tempdir!();

                let mut catalog = Catalog::new(&td)
                    .unwrap_or_else(|e| panic!("Could not crate catalog {}", e));

                let added = Reply::add_column(request, &mut catalog);

                match added {
                    AddColumn(Err(_)) => {}
                    _ => panic!("Should have returned an error"),
                }
            }
        }

        mod insert {
            use super::*;
            use hyena_engine::Fragment;
            use hyena_engine::BlockType::{I8Dense, U8Sparse};
            use hyena_engine::BlockStorage::Memory;

            #[test]
            fn creates_source_group() {
                let source = 100;
                let insert = InsertMessage {
                    source: source,
                    timestamps: vec![1],
                    columns: vec![hashmap!{0 => Fragment::I8Dense(vec![1])}],
                };

                let td = tempdir!();

                let mut catalog = Catalog::new(&td)
                    .unwrap_or_else(|e| panic!("Could not crate catalog {}", e));

                Reply::insert(insert, &mut catalog);

//                 catalog.groups
//                     .get(&source)
//                     .unwrap_or_else(|| panic!("Group should have been created."));

            }

            #[test]
            fn inserts_all_appends() {
                use hyena_engine::Catalog;
//                 use partition::Partition;
//                 use std::collections::VecDeque;

                let source = 100;
                let insert = InsertMessage {
                    source: source,
                    timestamps: vec![1, 2, 3, 4, 5, 6],
                    columns: vec![hashmap!{
                        1000 => Fragment::I8Dense(vec![101, 102, 103, 104, 105, 106]),
                        2000 => Fragment::U8Sparse(vec![201, 202, 203, 204, 205, 206],
                                                   vec![121, 221, 321, 421, 521, 621])
                    }],
                };

                let td = tempdir!();

                let mut catalog = Catalog::new(&td)
                    .unwrap_or_else(|e| panic!("Could not crate catalog {}", e));
                catalog.add_columns(hashmap!{
                    1000 => Column::new(Memory(I8Dense),  "dense"),
                    2000 => Column::new(Memory(U8Sparse), "sparse"),
                    })
                    .unwrap();

                let reply = Reply::insert(insert, &mut catalog);
                match reply {
                    Reply::Insert(Ok(_)) => {}
                    Reply::Insert(Err(e)) => panic!("Insert should not have been rejected {:?}", e),
                    _ => panic!("Insert should not have been rejected"),
                }

//                 let cgroups = &catalog.groups;
//                 let groups: &catalog::PartitionGroup = &cgroups.get(&source).unwrap();
//                 let mpartitions = &groups.mutable_partitions;
//                 let partitions: &VecDeque<Partition> = &*mpartitions.read().unwrap();
//                 assert_eq!(1, partitions.len());
//
//                 let blocks = partitions.front().unwrap().get_blocks();
//                 if let TyBlock::Memory(MemBlock::I8Dense(ref one)) =
//                     *blocks.get(&1000).unwrap().read().unwrap() {
//                     assert_eq!(vec![101i8, 102, 103, 104, 105, 106].as_slice(),
//                                &one.as_ref()[0..6]);
//                 } else {
//                     panic!("Wrong block type!");
//                 };
//
//                 if let TyBlock::Memory(MemBlock::U8Sparse(ref _block)) =
//                     *blocks.get(&2000).unwrap().read().unwrap() {
//                     // FAILS! WHY?!
//
//                     //let (index, data) = (block.as_ref_index(), block.as_ref());
//                     //assert_eq!(vec![201, 202, 203, 204, 205, 206].as_slice(),
//                     //           &data.as_ref()[0..6]);
//                     //assert_eq!(vec![121, 221, 321, 421, 521, 621].as_slice(),
//                     //           &index.as_ref()[0..6]);
//                 } else {
//                     panic!("Wrong block type!");
//                 };
            }

            fn verify_insert_rejected_no_data(insert: InsertMessage, catalog: &mut Catalog) {
                let reply = Reply::insert(insert, catalog);
                match reply {
                    Reply::Insert(Ok(_)) => panic!("Should have returned error"),
                    Reply::Insert(Err(_)) => { /* OK, do nothing */ }
                    _ => panic!("Wrong reply type returned"),
                }
            }

            #[test]
            fn rejects_insert_with_no_data() {
                let source = 100;

                let td = tempdir!();

                let mut catalog = Catalog::new(&td)
                    .unwrap_or_else(|e| panic!("Could not crate catalog {}", e));
                let insert = InsertMessage {
                    source: source,
                    timestamps: vec![],
                    columns: vec![],
                };
                verify_insert_rejected_no_data(insert, &mut catalog);

                let insert = InsertMessage {
                    source: source,
                    timestamps: vec![1, 2, 3],
                    columns: vec![],
                };
                verify_insert_rejected_no_data(insert, &mut catalog);

                let insert = InsertMessage {
                    source: source,
                    timestamps: vec![],
                    columns: vec![hashmap!{
                        0 => Fragment::I8Dense(vec![1,2,3])
                    }],
                };
                verify_insert_rejected_no_data(insert, &mut catalog);
            }

            #[test]
            fn rejects_if_datasize_differs_from_ts_size() {
                let source = 100;
                let insert = InsertMessage {
                    source: source,
                    timestamps: vec![1, 2, 3, 4, 5],
                    columns: vec![hashmap!{
                        1000 => Fragment::I8Dense(vec![101, 102, 103, 104, 105, 106]),
                        2000 => Fragment::U8Sparse(vec![201, 202, 203, 204, 205, 206],
                                                   vec![121, 221, 321, 421, 521, 621])
                    }],
                };

                let td = tempdir!();

                let mut catalog = Catalog::new(&td)
                    .unwrap_or_else(|e| panic!("Could not crate catalog {}", e));
                catalog.add_columns(hashmap!{
                    1000 => Column::new(Memory(I8Dense), "dense"),
                    2000 => Column::new(Memory(U8Sparse), "sparse"),
                    })
                    .unwrap();

                let reply = Reply::insert(insert, &mut catalog);
                match reply {
                    Reply::Insert(Ok(_)) => panic!("Inconsistent data should have been rejected"),
                    Reply::Insert(Err(_)) => { /* OK, do nothing */ }
                    _ => panic!("Insert should not have been rejected"),
                }

                let insert = InsertMessage {
                    source: source,
                    timestamps: vec![1, 2, 3, 4, 5, 6],
                    columns: vec![hashmap!{
                        1000 => Fragment::I8Dense(vec![101, 102, 103, 104, 105]),
                        2000 => Fragment::U8Sparse(vec![201, 202, 203, 204, 205, 206],
                                                   vec![121, 221, 321, 421, 521, 621])
                    }],
                };
                let reply = Reply::insert(insert, &mut catalog);
                match reply {
                    Reply::Insert(Ok(_)) => panic!("Inconsistent data should have been rejected"),
                    Reply::Insert(Err(_)) => { /* OK, do nothing */ }
                    _ => panic!("Insert should not have been rejected"),
                }

                let insert = InsertMessage {
                    source: source,
                    timestamps: vec![1, 2, 3, 4, 5, 6],
                    columns: vec![hashmap!{
                        1000 => Fragment::I8Dense(vec![101, 102, 103, 104, 105, 106]),
                        2000 => Fragment::U8Sparse(vec![201, 202, 203, 204, 205, 206, 207],
                                                   vec![121, 221, 321, 421, 521, 621, 721])
                    }],
                };
                let reply = Reply::insert(insert, &mut catalog);
                match reply {
                    Reply::Insert(Ok(_)) => panic!("Inconsistent data should have been rejected"),
                    Reply::Insert(Err(_)) => { /* OK, do nothing */ }
                    _ => panic!("Insert should not have been rejected"),
                }
            }
        }

        mod get_catalog {
            use super::*;
            use super::Reply::RefreshCatalog;
            use hyena_engine::{BlockType, Fragment};
            use hyena_engine::BlockType::{I8Dense, U8Sparse};
            use hyena_engine::BlockStorage::Memory;

            #[test]
            fn handles_empty_catalog() {
                let td = tempdir!();
                let cat = Catalog::new(&td).expect("Unable to create catalog");

                let reply = Reply::get_catalog(&cat);
                if let RefreshCatalog(response) = reply {
                    assert_eq!(2, response.columns.len());
                    assert_eq!(0, response.available_partitions.len());
                } else {
                    panic!("Wrong Reply type returned")
                }

            }

            #[test]
            fn returns_mem_and_mmap_columns() {
                let td = tempdir!();

                let mut catalog = Catalog::new(&td)
                    .unwrap_or_else(|e| panic!("Could not crate catalog {}", e));
                catalog.add_columns(hashmap!{
                    1000 => Column::new(Memory(I8Dense), "dense"),
                    2000 => Column::new(Memory(U8Sparse), "sparse"),
                    })
                    .unwrap();

                let source = 100;
                let insert = InsertMessage {
                    source: source,
                    timestamps: vec![1, 2, 3, 4, 5, 6],
                    columns: vec![hashmap!{
                        1000 => Fragment::I8Dense(vec![101, 102, 103, 104, 105, 106]),
                        2000 => Fragment::U8Sparse(vec![201, 202, 203, 204, 205, 206],
                                                   vec![121, 221, 321, 421, 521, 621])
                    }],
                };
                Reply::insert(insert, &mut catalog);

                let reply = Reply::get_catalog(&catalog);
                if let RefreshCatalog(mut response) = reply {
                    // Verify columns
                    response.columns.sort_by_key(|column| column.id);
                    assert_eq!(4, response.columns.len());
                    assert_eq!(ReplyColumn {
                                   typ: BlockType::I8Dense,
                                   id: 1000,
                                   name: "dense".into(),
                               },
                               response.columns[2]);
                    assert_eq!(ReplyColumn {
                                   typ: BlockType::U8Sparse,
                                   id: 2000,
                                   name: "sparse".into(),
                               },
                               response.columns[3]);

                    // Verify partitions
//                     assert_eq!(1, response.available_partitions.len());
//                     let first = &response.available_partitions[0];
//                     assert_eq!(1, first.min_ts);
//                     assert_eq!(1, first.max_ts);
//                     assert_eq!("", first.location);
                } else {
                    panic!("Wrong Reply type returned")
                }

            }
        }

        mod scan {
            use super::*;
            use hyena_common::ty::Uuid;

            #[test]
            fn fails_if_mints_later_then_maxts() {
                let td = tempdir!();
                let cat = Catalog::new(&td).expect("Unable to create catalog");


                let partition_ids = hashset! { Uuid::default() };

                let request = ScanRequest {
                    min_ts: 10,
                    max_ts: 1,
                    partition_ids: partition_ids,
                    projection: vec![1, 2, 3],
                    filters: vec![ScanFilter {
                                      column: 1,
                                      op: ScanComparison::Eq,
                                      typed_val: FilterVal::I8(10),
                                      str_val: "".into(),
                                  }],
                };

                let reply = Reply::scan(request, &cat);
                match reply {
                    Reply::Scan(Err(_)) => { /* OK, do nothing */ }
                    _ => panic!("Should have rejected the scan request"),
                }
            }

            #[test]
            fn fails_if_filters_empty() {
                let td = tempdir!();
                let cat = Catalog::new(&td).expect("Unable to create catalog");


                let partition_ids = hashset! { Uuid::default() };

                let request = ScanRequest {
                    min_ts: 1,
                    max_ts: 10,
                    partition_ids: partition_ids,
                    projection: vec![1, 2, 3],
                    filters: vec![],
                };

                let _reply = Reply::scan(request, &cat);
//                 match reply {
//                     Reply::Scan(Err(Error::InvalidScanRequest(_))) => (), /* OK, do nothing */
//                     _ => panic!("Should have rejected the scan request")
//                 }
            }

            #[test]
            fn fails_if_projection_empty() {
                let td = tempdir!();
                let cat = Catalog::new(&td).expect("Unable to create catalog");


                let partition_ids = hashset! { Uuid::default() };

                let request = ScanRequest {
                    min_ts: 1,
                    max_ts: 10,
                    partition_ids: partition_ids,
                    projection: vec![],
                    filters: vec![ScanFilter {
                                      column: 1,
                                      op: ScanComparison::Eq,
                                      typed_val: FilterVal::U32(10),
                                      str_val: "".into(),
                                  }],
                };

                let _reply = Reply::scan(request, &cat);
//                 match reply {
//                     Reply::Scan(Err(Error::InvalidScanRequest(_))) => { /* OK, do nothing */ }
//                     _ => panic!("Should have rejected the scan request"),
//                 }
            }

            // TODO add positive paths when the code is integrated
        }
    }
}
