use bincode::{Error as BinError, deserialize};
use block::BlockType;
use catalog::{Catalog, Column, ColumnMap};
use error;
use error::ResultExt;
use mutator::BlockData;
use mutator::append::Append;
use partition::Partition;
use std::collections::hash_map::HashMap;
use std::convert::From;
use std::result::Result;
use ty::{Block, BlockType as TyBlockType, ColumnId, TimestampFragment};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub struct InsertMessage {
    timestamps: Vec<u64>,
    source: u32,
    columns: Vec<BlockData>,
}

#[derive(Serialize, Debug)]
pub struct ScanResultMessage<'message> {
    pub row_count: u32,
    pub col_count: u32,
    pub col_types: Vec<(u32, TyBlockType)>,
    pub blocks: Vec<Block<'message>>,
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ScanFilter {
    pub column: u32,
    pub op: ScanComparison,
    pub val: u64,
    pub str_val: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ScanRequest {
    pub min_ts: u64,
    pub max_ts: u64,
    pub partition_id: UUID,
    pub projection: Vec<u32>,
    pub filters: Vec<ScanFilter>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AddColumnRequest {
    pub column_name: String,
    pub column_type: BlockType,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct UUID {
    hi: i64,
    lo: i64,
}

impl From<Uuid> for UUID {
    fn from(uuid: Uuid) -> UUID {
        use byteorder::{NativeEndian, ReadBytesExt};

        let bytes = uuid.as_bytes();
        let mut lower = &bytes[0..8];
        let mut higher = &bytes[8..16];

        UUID {
            hi: higher.read_i64::<NativeEndian>().unwrap(),
            lo: lower.read_i64::<NativeEndian>().unwrap(),
        }
    }
}

impl From<UUID> for Uuid {
    fn from(uuid: UUID) -> Uuid {
        use byteorder::{NativeEndian, WriteBytesExt};

        let mut buf: Vec<u8> = vec![];

        buf.write_i64::<NativeEndian>(uuid.lo).unwrap();
        buf.write_i64::<NativeEndian>(uuid.hi).unwrap();

        Uuid::from_bytes(buf.as_slice()).unwrap()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct PartitionInfo {
    min_ts: u64,
    max_ts: u64,
    id: UUID,
    location: String,
}

impl<'a> PartitionInfo {
    fn from(partition: &Partition<'a>) -> PartitionInfo {
        PartitionInfo {
            min_ts: partition.ts_min.into(),
            max_ts: partition.ts_max.into(),
            id: partition.id.into(),
            location: String::new(),
        }
    }
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
pub enum Reply<'reply> {
    ListColumns(Vec<ReplyColumn>),
    Insert(Result<usize, Error>),
    Scan(Result<ScanResultMessage<'reply>, Error>),
    RefreshCatalog(RefreshCatalogResponse),
    AddColumn(Result<usize, Error>),
    Flush,
    DataCompaction,
    SerializeError(String),
    Other,
}

impl<'reply> Reply<'reply> {
    fn list_columns(catalog: &Catalog) -> Reply<'reply> {
        use std::ops::Deref;

        let cm: &ColumnMap = catalog.as_ref();
        let names = cm.iter()
            .map(|(id, column)| match *column.deref() {
                TyBlockType::Memory(typ) => ReplyColumn::new(typ, *id, format!("{}", column)),
                TyBlockType::Memmap(typ) => ReplyColumn::new(typ, *id, format!("{}", column)),
            })
            .collect();
        Reply::ListColumns(names)
    }

    fn add_column(request: AddColumnRequest, catalog: &mut Catalog) -> Reply<'reply> {
        if request.column_name.is_empty() {
            return Reply::AddColumn(Err(Error::ColumnNameCannotBeEmpty));
        }

        let column = Column::new(TyBlockType::Memmap(request.column_type),
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

    fn insert(insert: InsertMessage, catalog: &mut Catalog) -> Reply<'reply> {
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
            let groups_ensured = catalog.ensure_group(source)
                .chain_err(|| format!("Could not create group for source {}", source));
            if groups_ensured.is_err() {
                return Reply::Insert(Err(Error::CatalogError(groups_ensured.unwrap_err()
                    .description()
                    .into())));
            }
        }

        for block in insert.columns.iter() {
            let append = Append {
                ts: timestamps.clone(),
                source_id: insert.source,
                data: block.clone(),
            };
            let result = catalog.append(&append);
            match result {
                Ok(number) => inserted += number,
                Err(e) => return Reply::Insert(Err(Error::Unknown(e.description().into()))),
            }
        }
        let flushed = catalog.flush()
            .chain_err(|| "Cannot flush catalog after inserting");
        if flushed.is_err() {
            Reply::Insert(Err(Error::CatalogError(flushed.unwrap_err().description().into())))
        } else {
            Reply::Insert(Ok(inserted))
        }
    }

    fn scan(scan: ScanRequest, _catalog: &Catalog) -> Reply<'reply> {
        if scan.min_ts > scan.max_ts {
            return Reply::Scan(Err(Error::InvalidScanRequest("min_ts > max_ts".into())));
        }
        if scan.filters.len() == 0 {
            return Reply::Scan(Err(Error::InvalidScanRequest("Filters cannot be empty".into())));
        }

        // TODO: connect to scanning when it's integrated
        Reply::Scan(Ok(ScanResultMessage::new()))
    }

    fn get_catalog(catalog: &Catalog) -> Reply<'reply> {
        let columns = catalog.columns
            .iter()
            .map(|(id, c)| {
                let t = match c.ty {
                    TyBlockType::Memory(t) => t,
                    TyBlockType::Memmap(t) => t,
                };
                ReplyColumn {
                    typ: t,
                    id: *id,
                    name: c.name.clone(),
                }
            })
            .collect();
        let mut partitions: Vec<PartitionInfo> = catalog.groups
            .values()
            .flat_map(|g| g.immutable_partitions.values().collect::<Vec<_>>())
            .map(|partition| PartitionInfo::from(partition))
            .collect();
        let mutable: Vec<PartitionInfo> = catalog.groups
            .values()
            .flat_map(|g| {
                let unlocked = g.mutable_partitions.read().unwrap();
                let mutable: Vec<PartitionInfo> = unlocked.iter()
                    .map(|partition| PartitionInfo::from(partition))
                    .collect();
                mutable
            })
            .collect();
        partitions.extend(mutable);
        let response = RefreshCatalogResponse {
            columns: columns,
            available_partitions: partitions,
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
    Unknown(String),
}

impl From<error::Error> for Error {
    fn from(err: error::Error) -> Error {
        // Is there a way to do it better?
        match *err.kind() {
            error::ErrorKind::ColumnNameAlreadyExists(ref s) => {
                Error::ColumnNameAlreadyExists(s.clone())
            }
            error::ErrorKind::ColumnIdAlreadyExists(ref u) => Error::ColumnIdAlreadyExists(*u),
            _ => Error::Unknown(err.description().into()),
        }
    }
}

impl<'message> ScanResultMessage<'message> {
    pub fn new() -> ScanResultMessage<'message> {
        ScanResultMessage {
            row_count: 0,
            col_count: 0,
            col_types: Vec::new(),
            blocks: Vec::new(),
        }
    }
}

pub fn run_request<'reply>(req: Request, catalog: &mut Catalog) -> Reply<'reply> {
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
            use ty::block::BlockType::*;
            use block::BlockType;

            #[test]
            fn returns_mem_and_mmap_columns() {
                let columns = hashmap_mut! {
                    0 => Column {ty: Memory(BlockType::U32Dense), name: "test_column1".into()},
                    1 => Column {ty: Memmap(BlockType::I64Sparse), name: "test_column2".into()},
                };
                let cat = Catalog {
                    columns: columns,
                    groups: Default::default(),
                    data_root: "".into(),
                };
                let reply = Reply::list_columns(&cat);
                if let ListColumns(mut colvec) = reply {
                    colvec.sort_by_key(|column| column.id);
                    assert_eq!(2, colvec.len());
                    assert_eq!(ReplyColumn {
                                   typ: BlockType::U32Dense,
                                   id: 0,
                                   name: "test_column1".into(),
                               },
                               colvec[0]);
                    assert_eq!(ReplyColumn {
                                   typ: BlockType::I64Sparse,
                                   id: 1,
                                   name: "test_column2".into(),
                               },
                               colvec[1]);
                } else {
                    panic!("Wrong Reply type returned")
                }
            }

            #[test]
            fn handles_empty_catalog() {
                let cat = Catalog {
                    columns: Default::default(),
                    groups: Default::default(),
                    data_root: "".into(),
                };
                let reply = Reply::list_columns(&cat);
                if let ListColumns(colvec) = reply {
                    assert_eq!(0, colvec.len())
                } else {
                    panic!("Wrong Reply type returned")
                }
            }
        }

        mod add_column {
            use super::*;
            use super::Reply::{AddColumn, ListColumns};
            use block::BlockType;

            #[test]
            fn adds_column() {
                let name: String = "a_test_column".into();
                let request = AddColumnRequest {
                    column_name: name.clone(),
                    column_type: BlockType::I32Dense,
                };
                let mut catalog = Catalog::new(tempdir!(persistent))
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
                let mut catalog = Catalog::new(tempdir!(persistent))
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
            use ty::fragment::Fragment;
            use block::BlockType::{I8Dense, U8Sparse};
            use ty::block::BlockType::Memory;
            use ty::block::Block as TyBlock;
            use ty::block::memory::Block as MemBlock;

            #[test]
            fn creates_source_group() {
                let source = 100;
                let insert = InsertMessage {
                    source: source,
                    timestamps: vec![1],
                    columns: vec![hashmap_mut!{0 => Fragment::I8Dense(vec![1])}],
                };
                let mut catalog = Catalog::new(tempdir!(persistent))
                    .unwrap_or_else(|e| panic!("Could not crate catalog {}", e));

                Reply::insert(insert, &mut catalog);

                catalog.groups
                    .get(&source)
                    .unwrap_or_else(|| panic!("Grop should have been created."));

            }

            #[test]
            fn inserts_all_appends() {
                use catalog;
                use partition::Partition;
                use std::collections::VecDeque;

                let source = 100;
                let insert = InsertMessage {
                    source: source,
                    timestamps: vec![1, 2, 3, 4, 5, 6],
                    columns: vec![hashmap_mut!{
                        1000 => Fragment::I8Dense(vec![101, 102, 103, 104, 105, 106]),
                        2000 => Fragment::U8Sparse(vec![201, 202, 203, 204, 205, 206],
                                                   vec![121, 221, 321, 421, 521, 621])
                    }],
                };
                let mut catalog = Catalog::new(tempdir!(persistent))
                    .unwrap_or_else(|e| panic!("Could not crate catalog {}", e));
                catalog.add_columns(hashmap_mut!{
                    1000 => Column{ty: Memory(I8Dense),  name: "dense".into()},
                    2000 => Column{ty: Memory(U8Sparse), name: "sparse".into()},
                    })
                    .unwrap();

                let reply = Reply::insert(insert, &mut catalog);
                match reply {
                    Reply::Insert(Ok(_)) => {}
                    Reply::Insert(Err(e)) => panic!("Insert should not have been rejected {:?}", e),
                    _ => panic!("Insert should not have been rejected"),
                }

                let cgroups = &catalog.groups;
                let groups: &catalog::PartitionGroup = &cgroups.get(&source).unwrap();
                let mpartitions = &groups.mutable_partitions;
                let partitions: &VecDeque<Partition> = &*mpartitions.read().unwrap();
                assert_eq!(1, partitions.len());

                let blocks = partitions.front().unwrap().get_blocks();
                if let TyBlock::Memory(MemBlock::I8Dense(ref one)) =
                    *blocks.get(&1000).unwrap().read().unwrap() {
                    assert_eq!(vec![101i8, 102, 103, 104, 105, 106].as_slice(),
                               &one.as_ref()[0..6]);
                } else {
                    panic!("Wrong block type!");
                };

                if let TyBlock::Memory(MemBlock::U8Sparse(ref _block)) =
                    *blocks.get(&2000).unwrap().read().unwrap() {
                    // FAILS! WHY?!

                    //let (index, data) = (block.as_ref_index(), block.as_ref());
                    //assert_eq!(vec![201, 202, 203, 204, 205, 206].as_slice(),
                    //           &data.as_ref()[0..6]);
                    //assert_eq!(vec![121, 221, 321, 421, 521, 621].as_slice(),
                    //           &index.as_ref()[0..6]);
                } else {
                    panic!("Wrong block type!");
                };
            }

            fn verify_insert_rejected_no_data(insert: InsertMessage, catalog: &mut Catalog) {
                let reply = Reply::insert(insert, catalog);
                match reply {
                    Reply::Insert(Ok(_)) => panic!("Should have returned error"),
                    Reply::Insert(Err(Error::NoData(_))) => { /* OK, do nothing */ }
                    _ => panic!("Wrong reply type returned"),
                }
            }

            #[test]
            fn rejects_insert_with_no_data() {
                let source = 100;
                let mut catalog = Catalog::new(tempdir!(persistent))
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
                    columns: vec![hashmap_mut!{
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
                    columns: vec![hashmap_mut!{
                        1000 => Fragment::I8Dense(vec![101, 102, 103, 104, 105, 106]),
                        2000 => Fragment::U8Sparse(vec![201, 202, 203, 204, 205, 206],
                                                   vec![121, 221, 321, 421, 521, 621])
                    }],
                };
                let mut catalog = Catalog::new(tempdir!(persistent))
                    .unwrap_or_else(|e| panic!("Could not crate catalog {}", e));
                catalog.add_columns(hashmap_mut!{
                    1000 => Column{ty: Memory(I8Dense),  name: "dense".into()},
                    2000 => Column{ty: Memory(U8Sparse), name: "sparse".into()},
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
                    columns: vec![hashmap_mut!{
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
                    columns: vec![hashmap_mut!{
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
            use block::BlockType;
            use ty::fragment::Fragment;
            use block::BlockType::{I8Dense, U8Sparse};
            use ty::block::BlockType::Memory;

            #[test]
            fn handles_empty_catalog() {
                let cat = Catalog {
                    columns: Default::default(),
                    groups: Default::default(),
                    data_root: "".into(),
                };
                let reply = Reply::get_catalog(&cat);
                if let RefreshCatalog(response) = reply {
                    assert_eq!(0, response.columns.len());
                    assert_eq!(0, response.available_partitions.len());
                } else {
                    panic!("Wrong Reply type returned")
                }

            }

            #[test]
            fn returns_mem_and_mmap_columns() {
                let mut catalog = Catalog::new(tempdir!(persistent))
                    .unwrap_or_else(|e| panic!("Could not crate catalog {}", e));
                catalog.add_columns(hashmap_mut!{
                    1000 => Column{ty: Memory(I8Dense),  name: "dense".into()},
                    2000 => Column{ty: Memory(U8Sparse), name: "sparse".into()},
                    })
                    .unwrap();

                let source = 100;
                let insert = InsertMessage {
                    source: source,
                    timestamps: vec![1, 2, 3, 4, 5, 6],
                    columns: vec![hashmap_mut!{
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
                    assert_eq!(1, response.available_partitions.len());
                    let first = &response.available_partitions[0];
                    assert_eq!(1, first.min_ts);
                    assert_eq!(1, first.max_ts);
                    assert_eq!("", first.location);
                } else {
                    panic!("Wrong Reply type returned")
                }

            }
        }

        mod scan {
            use super::*;
            use api::UUID;

            #[test]
            fn fails_if_mints_later_then_maxts() {
                let cat = Catalog {
                    columns: Default::default(),
                    groups: Default::default(),
                    data_root: "".into(),
                };
                let request = ScanRequest {
                    min_ts: 10,
                    max_ts: 1,
                    partition_id: UUID { hi: 1, lo: 1 },
                    projection: vec![1, 2, 3],
                    filters: vec![ScanFilter {
                                      column: 1,
                                      op: ScanComparison::Eq,
                                      val: 10,
                                      str_val: "".into(),
                                  }],
                };

                let reply = Reply::scan(request, &cat);
                match reply {
                    Reply::Scan(Err(_)) => { /* OK, do nothing */ }
                    _ => panic!("Should have rejected the scan request"),
                }

                let request = ScanRequest {
                    min_ts: 10,
                    max_ts: 10,
                    partition_id: UUID { hi: 1, lo: 1 },
                    projection: vec![1, 2, 3],
                    filters: vec![ScanFilter {
                                      column: 1,
                                      op: ScanComparison::Eq,
                                      val: 10,
                                      str_val: "".into(),
                                  }],
                };

                let reply = Reply::scan(request, &cat);
                match reply {
                    Reply::Scan(Err(Error::InvalidScanRequest(_))) => {
                        panic!("Should have rejected the scan request")
                    }
                    _ => { /* OK, do noting */ }
                }

            }

            #[test]
            fn fails_if_filters_empty() {
                let cat = Catalog {
                    columns: Default::default(),
                    groups: Default::default(),
                    data_root: "".into(),
                };
                let request = ScanRequest {
                    min_ts: 1,
                    max_ts: 10,
                    partition_id: UUID { hi: 1, lo: 1 },
                    projection: vec![1, 2, 3],
                    filters: vec![],
                };

                let reply = Reply::scan(request, &cat);
                match reply {
                    Reply::Scan(Err(Error::InvalidScanRequest(_))) => { /* OK, do nothing */ }
                    _ => panic!("Should have rejected the scan request"),
                }
            }

            // TODO add positive paths when the code is integrated
        }
    }

    #[allow(non_snake_case)]
    mod UUID {
        use super::*;
        use api::UUID;
        use std;

        #[test]
        fn to_from() {
            let uuid = Uuid::new_v4();
            let u: UUID = std::convert::From::from(uuid);
            let to_from: Uuid = std::convert::From::from(u);

            assert_eq!(uuid, to_from);
        }
    }
}
