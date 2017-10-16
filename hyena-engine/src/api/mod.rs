use bincode::{serialize, deserialize, Infinite};
use catalog::{Catalog, Column, ColumnMap};
use ty::{Block, BlockType as TyBlockType};
use block::BlockType;
use std::collections::hash_map::HashMap;

#[derive(Serialize, Debug)]
pub struct InsertMessage<'a> {
    pub row_count : u32,
    pub col_count : u32,
    pub col_types : Vec<(u32, TyBlockType)>,
    pub blocks : Vec<Block<'a>> // This can be done right now only because blocks are so trivial
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
    pub str_val : Vec<u8>
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
    Insert, //(InsertMessage<'a>),
    Scan,
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

#[derive(Debug, Serialize)]
pub struct ReplyColumn {
    typ: BlockType,
    name: String
}

#[derive(Debug, Serialize)]
pub enum Reply {
    ListColumns(Vec<ReplyColumn>),
    Insert,
    Scan,
    RefreshCatalog,
    AddColumn,
    Flush,
    DataCompaction,
    Other
}

impl Reply {
    fn list_columns(catalog: & Catalog) -> Reply {
        use std::ops::Deref;

        let cm : &ColumnMap = catalog.as_ref();
        let mut names = vec![]; //vec!["dummy".to_owned()];
        for column in cm.values() {
            match *column.deref() {
                TyBlockType::Memory(typ) => names.push(ReplyColumn{typ: typ, name: format!("{}", column)}),
                _ => () // TODO
            };
        }
        Reply::ListColumns(names)
    }

    fn add_column(request: AddColumnRequest, catalog: &mut Catalog) -> Reply {
        let column = Column::new(TyBlockType::Memory(request.column_type), request.column_name.as_str());
        let id = 0;
        let mut map = HashMap::new();
        map.insert(id, column);

        catalog.ensure_columns(map).ok();
        catalog.flush().unwrap();
        Reply::AddColumn
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


pub fn run_request(req: Request, catalog: &mut Catalog) -> Reply {
    match req {
        Request::ListColumns => Reply::list_columns(catalog),
        Request::AddColumn(request) => Reply::add_column(request, catalog),
        _ => Reply::Other
    }
}

