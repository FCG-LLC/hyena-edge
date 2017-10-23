use bincode::{serialize, deserialize, Infinite};
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
    columns: Vec<BlockData>
}

#[derive(Serialize, Debug)]
pub struct ScanResultMessage<'message> {
    pub row_count : u32,
    pub col_count : u32,
    pub col_types : Vec<(u32, TyBlockType)>,
    pub blocks : Vec<Block<'message>> // This can be done right now only because blocks are so trivial
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ScanComparison {
    Lt,
    LtEq,
    Eq,
    GtEq,
    Gt,
    NotEq
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ScanFilter {
    pub column : u32,
    pub op : ScanComparison,
    pub val : u64,
    pub str_val : String
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ScanRequest {
    pub min_ts : u64,
    pub max_ts : u64,
    pub partition_id : u64,
    pub projection : Vec<u32>,
    pub filters : Vec<ScanFilter>
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AddColumnRequest {
    pub column_name: String,
    pub column_type: BlockType
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct GenericResponse {
    pub status : u32
}

impl GenericResponse {
    pub fn create_as_buf(status : u32) -> Vec<u8> {
        let resp = GenericResponse { status: status };
        serialize(&resp, Infinite).unwrap()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct PartitionInfo {
    min_ts: u64,
    max_ts: u64,
    id: Uuid,
    location: String
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RefreshCatalogResponse {
    pub columns: Vec<ReplyColumn>,
    pub available_partitions: Vec<PartitionInfo>
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Operation {
    ListColumns,
    Insert,
    Scan,
    RefreshCatalog,
    AddColumn,
    Flush,
    DataCompaction
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
    Other
}

impl Request {
    pub fn parse(data: Vec<u8>) -> Request {
        let message: Request = deserialize(&data[..]).unwrap();
        message
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ReplyColumn {
    typ: BlockType,
    id: ColumnId,
    name: String
}

impl ReplyColumn {
    fn new(typ: BlockType, id: ColumnId, name: String) -> Self {
        ReplyColumn {typ: typ, id: id, name: name}
    }
}

#[derive(Debug, Serialize)]
pub enum Reply<'reply> {
    ListColumns(Vec<ReplyColumn>),
    Insert(Result<usize, Error>),
    Scan(ScanResultMessage<'reply>),
    RefreshCatalog(RefreshCatalogResponse),
    AddColumn(Result<usize, Error>),
    Flush,
    DataCompaction,
    Other
}

impl <'reply> Reply<'reply> {
    fn list_columns(catalog: & Catalog) -> Reply<'reply> {
        use std::ops::Deref;

        let cm : &ColumnMap = catalog.as_ref();
        let mut names = vec![]; //vec!["dummy".to_owned()];
        for (id, column) in cm.iter() {
            match *column.deref() {
                TyBlockType::Memory(typ) => names.push(ReplyColumn::new(typ, *id, format!("{}", column))),
                _ => () // TODO
            };
        }
        Reply::ListColumns(names)
    }

    fn add_column(request: AddColumnRequest, catalog: &mut Catalog) -> Reply<'reply> {
        let column = Column::new(TyBlockType::Memmap(request.column_type), request.column_name.as_str());
        let id = catalog.next_id();
        info!("Adding column {}:{:?} with id {}", request.column_name, request.column_type, id);
        let mut map = HashMap::new();
        map.insert(id, column);

        match catalog.add_columns(map) {
            Ok(_) => {
                catalog.flush().unwrap();
                Reply::AddColumn(Ok(id))
            },
            Err(error) => Reply::AddColumn(Err(error.into()))
        }
    }

    fn insert(insert: InsertMessage, catalog: &mut Catalog) -> Reply<'reply> {
        let timestamps: TimestampFragment = insert.timestamps.into();
        let mut inserted = 0;
        let source = insert.source;

        catalog
            .ensure_group(source)
            .chain_err(|| format!("Could not create group for source {}", source))
            .unwrap();

        for block in insert.columns.iter() {
            let append = Append {ts: timestamps.clone(), source_id: insert.source, data: block.clone()};
            let result = catalog.append(&append);
            match result {
                Ok(number) => inserted += number,
                Err(e) => return Reply::Insert(Err(Error::Unknown(e.description().into())))
            }
        }
        catalog.flush()
            .chain_err(|| "Cannot flush catalog after inserting")
            .unwrap();
        Reply::Insert(Ok(inserted))}

    fn scan(_scan: ScanRequest, _catalog: &Catalog) -> Reply<'reply> {
        // TODO: connect to scanning when it's integrated
        Reply::Scan(ScanResultMessage::new())
    }

    fn get_catalog(catalog: &Catalog) -> Reply<'reply> {
        let columns = catalog.columns.iter()
            .map(|(id, c)| {
                 let t = match c.ty {
                     TyBlockType::Memory(t) => t,
                     TyBlockType::Memmap(t) => t
                 };
                 ReplyColumn {
                     typ: t,
                     id: *id,
                     name: c.name.clone()
                 }
            })
            .collect();
        let mut immutable: Vec<PartitionInfo> = catalog.groups.values()
            .flat_map(|g| {
                let immutable: Vec<&Partition> = g.immutable_partitions.values().collect();
                immutable
            })
            .map(|partition|
                 PartitionInfo {
                     min_ts: partition.ts_min.into(),
                     max_ts: partition.ts_max.into(),
                     id: partition.id,
                     location: String::new(),
                 }
            )
            .collect();
        let mutable: Vec<PartitionInfo> = catalog.groups.values()
            .flat_map(|g| {
                let unlocked = g.mutable_partitions.read().unwrap();
                let mutable: Vec<PartitionInfo> = unlocked.iter()
                    .map(|partition|
                         PartitionInfo {
                             min_ts: partition.ts_min.into(),
                             max_ts: partition.ts_max.into(),
                             id: partition.id,
                             location: String::new(),
                         }
                    ).collect();
                mutable
            }).collect();
        immutable.extend(mutable);
        let response = RefreshCatalogResponse {
            columns: columns,
            available_partitions: immutable
        };
        Reply::RefreshCatalog(response)
    }
}

#[derive(Debug, Serialize)]
pub enum Error {
    ColumnNameAlreadyExists(String),
    ColumnIdAlreadyExists(usize),
    Unknown(String)
}

impl From<error::Error> for Error {
    fn from(err: error::Error) -> Error {
        // Is there a way to do it better?
        match *err.kind() {
            error::ErrorKind::ColumnNameAlreadyExists(ref s) => Error::ColumnNameAlreadyExists(s.clone()),
            error::ErrorKind::ColumnIdAlreadyExists(ref u)   => Error::ColumnIdAlreadyExists(*u),
            _ => Error::Unknown(err.description().into())
        }
    }
}

impl<'message> ScanResultMessage<'message> {
    pub fn new() -> ScanResultMessage<'message> {
        ScanResultMessage {
            row_count: 0,
            col_count: 0,
            col_types: Vec::new(),
            blocks: Vec::new()
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
        _ => Reply::Other
    }
}

