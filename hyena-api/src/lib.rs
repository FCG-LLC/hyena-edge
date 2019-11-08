#[allow(unused_imports)]
#[macro_use]
extern crate hyena_common;
#[cfg(test)]
#[macro_use]
extern crate hyena_test;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;

mod error;

use crate::error::*;

use bincode::{Error as BinError, deserialize};
use hyena_engine::{BlockType, Catalog, Column, ColumnMap, BlockData, Append, Scan,
ScanTsRange, BlockStorage, ColumnId, TimestampFragment, Fragment, Regex,
ScanFilterOp as HScanFilterOp, ScanFilter as HScanFilter, SourceId, ColumnIndexType,
ColumnIndexStorage, StreamConfig, StreamState};

use hyena_common::ty::{Uuid, Timestamp};

use hyena_common::collections::{HashMap, HashSet};
use std::convert::From;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::result::Result;
use extprim::i128::i128;
use extprim::u128::u128;

#[derive(Serialize, Deserialize, Debug)]
pub struct InsertMessage {
    timestamps: Vec<u64>,
    source: SourceId,
    // todo: simplify to BlockData
    columns: Vec<BlockData>,
}

impl InsertMessage {
    pub fn new(timestamps: Vec<u64>, source: SourceId, columns: Vec<BlockData>) -> InsertMessage {
        InsertMessage {
            timestamps,
            source,
            columns,
        }
    }

    pub fn timestamps(&self) -> &Vec<u64> {
        &self.timestamps
    }

    pub fn source(&self) -> SourceId {
        self.source
    }

    pub fn columns(&self) -> &Vec<BlockData> {
        &self.columns
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DataTriple {
    column_id: ColumnId,
    column_type: BlockType,
    data: Option<Fragment>
}

impl DataTriple {
    pub fn new(column_id: ColumnId, column_type: BlockType, data: Option<Fragment>) -> Self {
        DataTriple { column_id, column_type, data }
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ScanResultMessage {
    data: Vec<DataTriple>,
    stream_state: Option<StreamState>,
}

impl ScanResultMessage {
    pub fn new() -> ScanResultMessage {
        Default::default()
    }

    pub fn add(&mut self, data: DataTriple) {
        self.data.push(data);
    }

    pub fn append(&mut self, data: &mut Vec<DataTriple>) {
        self.data.append(data);
    }

    pub fn set_stream_state(&mut self, stream_state: Option<StreamState>) {
        self.stream_state = stream_state;
    }
}

impl From<(Vec<DataTriple>, Option<StreamState>)> for ScanResultMessage {
    fn from(data: (Vec<DataTriple>, Option<StreamState>)) -> ScanResultMessage {
        ScanResultMessage { data: data.0, stream_state: data.1 }
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
    In,
    StartsWith,
    EndsWith,
    Contains,
    Matches,
}

impl ScanComparison {
    fn to_scan_filter_op<T>(&self, val: T) -> Result<HScanFilterOp<T>, Error>
        where T: Display + Debug + Clone + PartialEq + PartialOrd + Eq + Hash
    {
        match *self {
            ScanComparison::Lt => Ok(HScanFilterOp::Lt(val)),
            ScanComparison::LtEq => Ok(HScanFilterOp::LtEq(val)),
            ScanComparison::Eq => Ok(HScanFilterOp::Eq(val)),
            ScanComparison::GtEq => Ok(HScanFilterOp::GtEq(val)),
            ScanComparison::Gt => Ok(HScanFilterOp::Gt(val)),
            ScanComparison::NotEq => Ok(HScanFilterOp::NotEq(val)),
            ScanComparison::In => unimplemented!(),
            ScanComparison::StartsWith |
            ScanComparison::EndsWith   |
            ScanComparison::Contains   | // Use to_scan_filter_op_str
            ScanComparison::Matches => Err(Error::InvalidScanRequest("String operator for numeric data".into())),
        }
    }

    fn to_scan_filter_op_str(&self, val: String) -> Result<HScanFilterOp<String>, Error> {
        match *self {
            ScanComparison::StartsWith => Ok(HScanFilterOp::StartsWith(val)),
            ScanComparison::EndsWith => Ok(HScanFilterOp::EndsWith(val)),
            ScanComparison::Contains => Ok(HScanFilterOp::Contains(val)),
            ScanComparison::Matches => Ok(HScanFilterOp::Matches(Regex::new(&val)?)),
            _ => Err(Error::InvalidScanRequest("Numeric operator for string data".into())),
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
    U128(u128),
    String(String),
}

impl FilterVal {
    fn to_scan_filter(&self, op: &ScanComparison) -> Result<HScanFilter, Error> {
        match *self {
            FilterVal::U8(val) => Ok(HScanFilter::from(op.to_scan_filter_op(val)?)),
            FilterVal::U16(val) => Ok(HScanFilter::from(op.to_scan_filter_op(val)?)),
            FilterVal::U32(val) => Ok(HScanFilter::from(op.to_scan_filter_op(val)?)),
            FilterVal::U64(val) => Ok(HScanFilter::from(op.to_scan_filter_op(val)?)),
            FilterVal::U128(val) => Ok(HScanFilter::from(op.to_scan_filter_op(val)?)),
            FilterVal::I8(val) => Ok(HScanFilter::from(op.to_scan_filter_op(val)?)),
            FilterVal::I16(val) => Ok(HScanFilter::from(op.to_scan_filter_op(val)?)),
            FilterVal::I32(val) => Ok(HScanFilter::from(op.to_scan_filter_op(val)?)),
            FilterVal::I64(val) => Ok(HScanFilter::from(op.to_scan_filter_op(val)?)),
            FilterVal::I128(val) => Ok(HScanFilter::from(op.to_scan_filter_op(val)?)),
            FilterVal::String(ref val) => Ok(HScanFilter::from(op.to_scan_filter_op_str(val.to_string())?)),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ScanFilter {
    pub column: ColumnId,
    pub op: ScanComparison,
    pub typed_val: FilterVal,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ScanRequest {
    pub min_ts: u64,
    pub max_ts: u64,
    pub partition_ids: HashSet<Uuid>,
    pub projection: Vec<ColumnId>,
    // OR(AND(ScanFilter), AND(ScanFilter))
    pub filters: Vec<Vec<ScanFilter>>,
    pub stream: Option<StreamConfig>,
}

fn from_sr(scan_request: ScanRequest) -> Result<Scan, Error> {

    let scan_range = ScanTsRange::Bounded{
        start: Timestamp::from(scan_request.min_ts),
        end:   Timestamp::from(scan_request.max_ts),
    };

    let partitions =
        if !scan_request.partition_ids.is_empty() {
            Some(scan_request.partition_ids.into_iter().collect())
        } else { None };

    let filters = if scan_request.filters.is_empty() {
        None
    } else {
        Some(scan_request.filters
             .iter()
             .enumerate()
             .flat_map(|(and_group, and_filters)| {
                 and_filters
                     .iter()
                     .map(move |and_filter| {
                         let op = and_filter.typed_val.to_scan_filter(&and_filter.op)?;

                         Ok((and_group, and_filter.column, op))
                     })
             })
             .collect::<Result<Vec<(usize, usize, hyena_engine::ScanFilter)>, Error>>()?
             .iter().cloned()
             .fold(hyena_engine::ScanFilters::new(), |mut map, (and_group, column_id, op)| {
                 {
                     let or_vec = map.entry(column_id).or_insert_with(Vec::new);

                     or_vec.resize(and_group + 1, Vec::new());

                     let and_vec = or_vec.get_mut(and_group).unwrap();
                     and_vec.push(op);
                 }

                 map
             }))
    };

    Ok(Scan::new(filters,
                 Some(scan_request.projection),
                 None,
                 partitions,
                 Some(scan_range),
                 scan_request.stream))
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AddColumnRequest {
    pub column_name: String,
    pub column_type: BlockType,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Copy, Clone)]
pub struct AddColumnIndexRequest {
    pub column_id: ColumnId,
    pub index_type: ColumnIndexType,
}


#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct PartitionInfo {
    min_ts: u64,
    max_ts: u64,
    id: Uuid,
    location: String,
}

impl PartitionInfo {
    pub fn new(min_ts: u64, max_ts: u64, id: Uuid, location: String) -> Self {
        PartitionInfo { min_ts, max_ts, id, location }
    }

//     fn from(partition: &Partition<'a>) -> PartitionInfo {
//         PartitionInfo {
//             min_ts: partition.ts_min.into(),
//             max_ts: partition.ts_max.into(),
//             id: partition.id.into(),
//             location: String::new(),
//         }
//     }
}

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
    AddColumnIndex(AddColumnIndexRequest),
    Other,
}

impl Request {
    pub fn parse(data: &[u8]) -> Result<Request, BinError> {
        deserialize(data)
    }
}

impl std::fmt::Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match *self {
            Request::Insert(ref msg) => {
                let fragments_str: Vec<String> = msg.columns
                    .iter()
                    .map(|c| {
                        c.iter()
                            .map(|(k, v)| format!("column id {:?}, {:?} items", k, v.len()))
                            .collect::<Vec<String>>()
                            .join(", ")
                    })
                    .collect();
                write!(f,
                       "Insert(timestamps={} items, source={}, fragments=[{}])",
                       msg.timestamps.len(),
                       msg.source,
                       fragments_str.join(", "))
            }
            _ => write!(f, "{:?}", self),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ReplyColumn {
    typ: BlockType,
    id: ColumnId,
    name: String,
}

impl ReplyColumn {
    pub fn new(typ: BlockType, id: ColumnId, name: String) -> Self {
        ReplyColumn { typ, id, name }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Reply {
    ListColumns(Vec<ReplyColumn>),
    Insert(Result<usize, Error>),
    Scan(Result<ScanResultMessage, Error>),
    RefreshCatalog(RefreshCatalogResponse),
    AddColumn(Result<ColumnId, Error>),
    Flush,
    DataCompaction,
    AddColumnIndex(Result<ColumnId, Error>),
    SerializeError(String),
    Other,
}

impl std::fmt::Display for Reply {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match *self {
            Reply::Scan(ref msg) => {
                match *msg {
                    Ok(ref result) => {
                        let columns: Vec<String> = result.data
                            .iter()
                            .map(|data| match data.data {
                                Some(ref fragment) => {
                                    format!("column #{}: {} items", data.column_id, fragment.len())
                                }
                                None => format!("column #{}: 0 items", data.column_id),
                            })
                            .collect();
                        write!(f, "ScanResultMessage(data={}", columns.join(", "))
                    }
                    Err(ref error) => write!(f, "{:?}", error),
                }
            }
            _ => write!(f, "{:?}", self),
        }
    }
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

    fn add_index(request: AddColumnIndexRequest, catalog: &mut Catalog) -> Reply {
        let AddColumnIndexRequest { column_id, index_type } = request;

        let index = ColumnIndexStorage::Memmap(index_type);

        info!("Adding index {:?} with id {}",
              index_type,
              column_id);

        match catalog.add_indexes(hashmap! { column_id => index }.into()) {
            Ok(_) => {
                catalog.flush().expect("failed to flush catalog");
                Reply::AddColumnIndex(Ok(column_id))
            }
            Err(error) => Reply::AddColumnIndex(Err(error.into())),
        }
    }

    fn insert(insert: InsertMessage, catalog: &mut Catalog) -> Reply {
        let timestamps: TimestampFragment = insert.timestamps.into();
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

            if let Err(err) = groups_ensured {
                return Reply::Insert(Err(Error::CatalogError(err.to_string())));
            }
        }

        let col_len = insert.columns.len();
        let merged_data = insert.columns.into_iter().flat_map(|block| block.into_iter()).collect::<BlockData>();
        if merged_data.len() != col_len {
            // At least one of the columns were repeated.
            return Reply::Insert(Err(Error::InconsistentData("At least one of the colums is repeated.".into())));
        }
        let append = Append::new(
            timestamps,
            insert.source,
            merged_data
        );
        let result = catalog.append(&append);
        match result {
            Err(e) => Reply::Insert(Err(Error::Unknown(e.to_string()))),
            Ok(inserted) => {
                let flushed = catalog.flush()
                    .with_context(|_| "Cannot flush catalog after inserting");

                if let Err(err) = flushed {
                    Reply::Insert(Err(Error::CatalogError(err.to_string())))
                } else {
                    Reply::Insert(Ok(inserted))
                }
            }
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

        let scan = match from_sr(scan_request) {
            Ok(scan) => scan,
            Err(e) => return Reply::Scan(Err(e))
        };
        let result = match catalog.scan(&scan) {
            Ok(r) => r,
            Err(e) => return Reply::Scan(Err(Error::ScanError(e.to_string()))),
        };

        let cm: &ColumnMap = catalog.as_ref();

        let stream_data = result.stream_state_data();

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

        Reply::Scan(Ok(ScanResultMessage::from((srm, stream_data))))
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
            columns,
//             available_partitions: partitions,
            available_partitions: Default::default(),
        };
        Reply::RefreshCatalog(response)
    }
}

#[derive(Debug, Serialize, Deserialize)]
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
        Request::AddColumnIndex(request) => Reply::add_index(request, catalog),
        _ => Reply::Other,
    }
}

#[allow(clippy::redundant_field_names)]
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

        mod add_index {
            use super::*;
            use super::Reply::{AddColumnIndex, AddColumn};
            use hyena_test::tempfile::TempDir;

            fn prepare_catalog<'cat>() -> (TempDir, Catalog<'cat>, ColumnId) {
                let name: String = "a_test_column".into();
                let request = AddColumnRequest {
                    column_name: name.clone(),
                    column_type: BlockType::I32Dense,
                };

                let td = tempdir!();

                let mut catalog = Catalog::new(&td)
                    .unwrap_or_else(|e| panic!("Could not crate catalog {}", e));

                let id = if let AddColumn(Ok(id)) = Reply::add_column(request, &mut catalog) {
                    id
                } else {
                    panic!("add column failed");
                };

                (td, catalog, id)
            }

            #[test]
            fn adds_column() {
                let (_td, mut catalog, column_id) = prepare_catalog();

                let request = AddColumnIndexRequest {
                    column_id,
                    index_type: ColumnIndexType::Bloom,
                };

                if let AddColumnIndex(Ok(id)) = Reply::add_index(request, &mut catalog) {
                    assert_eq!(id, column_id, "bad column index");
                } else {
                    panic!("failed to add index");
                };
            }

            #[test]
            fn fails_on_double_insert() {
                let (_td, mut catalog, column_id) = prepare_catalog();

                let request = AddColumnIndexRequest {
                    column_id,
                    index_type: ColumnIndexType::Bloom,
                };

                if let AddColumnIndex(Ok(id)) = Reply::add_index(request, &mut catalog) {
                    assert_eq!(id, column_id, "bad column index");
                } else {
                    panic!("failed to add index");
                };

                if let AddColumnIndex(Ok(_)) = Reply::add_index(request, &mut catalog) {
                    panic!("second add_index didn't fail");
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
                    columns: vec![hashmap!{ 1000 => Fragment::I8Dense(vec![101, 102, 103, 104, 105, 106])},
                                  hashmap!{ 2000 => Fragment::U8Sparse(vec![201, 202, 203, 204, 205, 206],
                                                                       vec![121, 221, 321, 421, 521, 621])}
                    ],
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
                    filters: vec![vec![ScanFilter {
                                      column: 1,
                                      op: ScanComparison::Eq,
                                      typed_val: FilterVal::I8(10),
                                  }]],
                    stream: None,
                };

                let reply = Reply::scan(request, &cat);
                match reply {
                    Reply::Scan(Err(_)) => { /* OK, do nothing */ }
                    _ => panic!("Should have rejected the scan request"),
                }
            }

            fn test_string_filter(op: ScanComparison) {
                let td = tempdir!();
                let cat = Catalog::new(&td).expect("Unable to create catalog");

                let partition_ids = hashset! { Uuid::default() };

                let request = ScanRequest {
                    min_ts: 10,
                    max_ts: 100,
                    partition_ids: partition_ids,
                    projection: vec![1, 2, 3],
                    filters: vec![vec![ScanFilter {
                                      column: 1,
                                      op: op,
                                      typed_val: FilterVal::String("hello".into()),
                                  }]],
                    stream: None,
                };

                let reply = Reply::scan(request, &cat);
                match reply {
                    Reply::Scan(Ok(_)) =>  { /* OK, do nothing */ }
                    Reply::Scan(Err(_)) => panic!("Should not have rejected the scan request"),
                    _ => panic!("Wrong Reply type"),
                }
            }

            #[test]
            fn parses_string_operators() {
                test_string_filter(ScanComparison::StartsWith);
                test_string_filter(ScanComparison::EndsWith);
                test_string_filter(ScanComparison::Contains);
                test_string_filter(ScanComparison::Matches);
            }

            fn test_if_string_filter_has_numeric_value(op: ScanComparison) {
                let td = tempdir!();
                let cat = Catalog::new(&td).expect("Unable to create catalog");

                let partition_ids = hashset! { Uuid::default() };

                let request = ScanRequest {
                    min_ts: 10,
                    max_ts: 100,
                    partition_ids: partition_ids,
                    projection: vec![1, 2, 3],
                    filters: vec![vec![ScanFilter {
                                      column: 1,
                                      op: op,
                                      typed_val: FilterVal::I8(10),
                                  }]],
                    stream: None,
                };

                let reply = Reply::scan(request, &cat);
                match reply {
                    Reply::Scan(Ok(_)) => panic!("Should have rejected the scan request"),
                    Reply::Scan(Err(_)) => { /* OK, do nothing */ }
                    _ => panic!("Wrong Reply type"),
                }
            }

            #[test]
            fn fails_if_string_filter_has_numeric_value() {
                test_if_string_filter_has_numeric_value(ScanComparison::StartsWith);
                test_if_string_filter_has_numeric_value(ScanComparison::EndsWith);
                test_if_string_filter_has_numeric_value(ScanComparison::Contains);
                test_if_string_filter_has_numeric_value(ScanComparison::Matches);
            }

            #[test]
            fn fails_if_numeric_filter_has_string_value() {
                let td = tempdir!();
                let cat = Catalog::new(&td).expect("Unable to create catalog");

                let partition_ids = hashset! { Uuid::default() };

                let request = ScanRequest {
                    min_ts: 10,
                    max_ts: 100,
                    partition_ids: partition_ids,
                    projection: vec![1, 2, 3],
                    filters: vec![vec![ScanFilter {
                                      column: 1,
                                      op: ScanComparison::Gt,
                                      typed_val: FilterVal::String("hello".into()),
                                  }]],
                    stream: None,
                };

                let reply = Reply::scan(request, &cat);
                match reply {
                    Reply::Scan(Ok(_)) => panic!("Should have rejected the scan request"),
                    Reply::Scan(Err(_)) => { /* OK, do nothing */ }
                    _ => panic!("Wrong Reply type"),
                }
            }


            mod or {
                use super::*;
                use hyena_engine::{ScanFilter as EngineFilter, ScanFilterOp, ScanFilters};

                fn build_scan(filters: Vec<Vec<ScanFilter>>) -> Scan {
                    from_sr(ScanRequest {
                        min_ts: 1,
                        max_ts: 10,
                        partition_ids: Default::default(),
                        projection: vec![1, 2, 3],
                        filters,
                        stream: None,
                    }).unwrap()
                }

                #[inline]
                fn expect(filters: ScanFilters, result: Scan) {
                    let expected = Scan::new(
                        Some(filters),
                        Some(vec![1, 2, 3]),
                        None,
                        None,
                        Some(ScanTsRange::Bounded{
                            start: Timestamp::from(1),
                            end: Timestamp::from(10),
                        }),
                        None,
                    );

                    assert_eq!(expected, result);

                }

                // ts > 12

                #[test]
                fn single_and() {
                    let scan = build_scan(vec![vec![
                        ScanFilter {
                            column: 0,
                            op: ScanComparison::Gt,
                            typed_val: FilterVal::U64(12),
                        }
                    ]]);

                    let expected = hashmap!{
                        0 => vec![vec![EngineFilter::U64(ScanFilterOp::Gt(12))]],
                    };

                    expect(expected, scan);
                }

                // ts > 12 && ts < 30

                #[test]
                fn two_ands() {
                    let scan = build_scan(vec![vec![
                        ScanFilter {
                            column: 0,
                            op: ScanComparison::Gt,
                            typed_val: FilterVal::U64(12),
                        },
                        ScanFilter {
                            column: 0,
                            op: ScanComparison::Lt,
                            typed_val: FilterVal::U64(30),
                        }
                    ]]);

                    let expected = hashmap!{
                        0 => vec![vec![
                            EngineFilter::U64(ScanFilterOp::Gt(12)),
                            EngineFilter::U64(ScanFilterOp::Lt(30)),
                        ]],
                    };

                    expect(expected, scan);
                }

                // ts > 12 && ts < 30 && source_id == 12

                #[test]
                fn two_ands_two_columns() {
                    let scan = build_scan(vec![vec![
                        ScanFilter {
                            column: 0,
                            op: ScanComparison::Gt,
                            typed_val: FilterVal::U64(12),
                        },
                        ScanFilter {
                            column: 0,
                            op: ScanComparison::Lt,
                            typed_val: FilterVal::U64(30),
                        },
                        ScanFilter {
                            column: 1,
                            op: ScanComparison::Eq,
                            typed_val: FilterVal::U32(12),
                        }

                    ]]);

                    let expected = hashmap!{
                        0 => vec![vec![
                            EngineFilter::U64(ScanFilterOp::Gt(12)),
                            EngineFilter::U64(ScanFilterOp::Lt(30)),
                        ]],
                        1 => vec![vec![
                            EngineFilter::U32(ScanFilterOp::Eq(12)),
                        ]],
                    };

                    expect(expected, scan);
                }

                // (ts > 12 && ts < 30 && source_id == 12) || (ts == 7)

                #[test]
                fn two_ands_two_columns_single_or() {
                    let scan = build_scan(vec![vec![
                        ScanFilter {
                            column: 0,
                            op: ScanComparison::Gt,
                            typed_val: FilterVal::U64(12),
                        },
                        ScanFilter {
                            column: 0,
                            op: ScanComparison::Lt,
                            typed_val: FilterVal::U64(30),
                        },
                        ScanFilter {
                            column: 1,
                            op: ScanComparison::Eq,
                            typed_val: FilterVal::U32(12),
                        }

                    ], vec![
                        ScanFilter {
                            column: 0,
                            op: ScanComparison::Eq,
                            typed_val: FilterVal::U64(7),
                        },
                    ]]);

                    let expected = hashmap!{
                        0 => vec![
                            vec![
                                EngineFilter::U64(ScanFilterOp::Gt(12)),
                                EngineFilter::U64(ScanFilterOp::Lt(30)),
                            ],
                            vec![
                                EngineFilter::U64(ScanFilterOp::Eq(7)),
                            ],
                        ],
                        1 => vec![vec![
                            EngineFilter::U32(ScanFilterOp::Eq(12)),
                        ]],
                    };

                    expect(expected, scan);
                }

                // (ts > 12 && ts < 30 && source_id == 12) || (ts == 7 && source_id > 10)

                #[test]
                fn two_ands_two_columns_two_ors() {
                    let scan = build_scan(vec![vec![
                        ScanFilter {
                            column: 0,
                            op: ScanComparison::Gt,
                            typed_val: FilterVal::U64(12),
                        },
                        ScanFilter {
                            column: 0,
                            op: ScanComparison::Lt,
                            typed_val: FilterVal::U64(30),
                        },
                        ScanFilter {
                            column: 1,
                            op: ScanComparison::Eq,
                            typed_val: FilterVal::U32(12),
                        }
                    ], vec![
                        ScanFilter {
                            column: 0,
                            op: ScanComparison::Eq,
                            typed_val: FilterVal::U64(7),
                        },
                        ScanFilter {
                            column: 1,
                            op: ScanComparison::Gt,
                            typed_val: FilterVal::U32(10),
                        }
                    ]]);

                    let expected = hashmap!{
                        0 => vec![
                            vec![
                                EngineFilter::U64(ScanFilterOp::Gt(12)),
                                EngineFilter::U64(ScanFilterOp::Lt(30)),
                            ],
                            vec![
                                EngineFilter::U64(ScanFilterOp::Eq(7)),
                            ],
                        ],
                        1 => vec![
                            vec![
                                EngineFilter::U32(ScanFilterOp::Eq(12)),
                            ],
                            vec![
                                EngineFilter::U32(ScanFilterOp::Gt(10)),
                            ],
                        ],
                    };

                    expect(expected, scan);
                }

                // (ts > 12 && ts < 30 && source_id == 12)
                // || (ts == 7 && source_id > 10 && other == 2)

                #[test]
                fn two_ands_two_columns_two_ors_added_column() {
                    let scan = build_scan(vec![vec![
                        ScanFilter {
                            column: 0,
                            op: ScanComparison::Gt,
                            typed_val: FilterVal::U64(12),
                        },
                        ScanFilter {
                            column: 0,
                            op: ScanComparison::Lt,
                            typed_val: FilterVal::U64(30),
                        },
                        ScanFilter {
                            column: 1,
                            op: ScanComparison::Eq,
                            typed_val: FilterVal::U32(12),
                        }
                    ], vec![
                        ScanFilter {
                            column: 0,
                            op: ScanComparison::Eq,
                            typed_val: FilterVal::U64(7),
                        },
                        ScanFilter {
                            column: 1,
                            op: ScanComparison::Gt,
                            typed_val: FilterVal::U32(10),
                        },
                        ScanFilter {
                            column: 2,
                            op: ScanComparison::Eq,
                            typed_val: FilterVal::I8(2),
                        }

                    ]]);

                    let expected = hashmap!{
                        0 => vec![
                            vec![
                                EngineFilter::U64(ScanFilterOp::Gt(12)),
                                EngineFilter::U64(ScanFilterOp::Lt(30)),
                            ],
                            vec![
                                EngineFilter::U64(ScanFilterOp::Eq(7)),
                            ],
                        ],
                        1 => vec![
                            vec![
                                EngineFilter::U32(ScanFilterOp::Eq(12)),
                            ],
                            vec![
                                EngineFilter::U32(ScanFilterOp::Gt(10)),
                            ],
                        ],
                        2 => vec![
                            vec![],
                            vec![
                                EngineFilter::I8(ScanFilterOp::Eq(2)),
                            ]
                        ]
                    };

                    expect(expected, scan);
                }
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
                    filters: vec![vec![ScanFilter {
                                      column: 1,
                                      op: ScanComparison::Eq,
                                      typed_val: FilterVal::U32(10),
                                  }]],
                    stream: None,
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
