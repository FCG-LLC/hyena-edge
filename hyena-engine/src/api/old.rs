use bincode::{serialize, deserialize, Infinite};
use catalog::{Catalog, Column, ColumnMap};
use ty::{Block, BlockType};
use ty::block::memory::BlockType as MemBlockType;
// use storage::manager::{Manager, BlockCache};
// use int_blocks::{Block, Int32SparseBlock, Int64DenseBlock, Int64SparseBlock, Scannable, Deletable, Movable, Upsertable};
// use std::time::Instant;
// use scan::{BlockScanConsumer};
use std::collections::hash_map::HashMap;

#[derive(Serialize, Debug)]
pub struct InsertMessage<'a> {
    pub row_count : u32,
    pub col_count : u32,
    pub col_types : Vec<(u32, BlockType)>,
    pub blocks : Vec<Block<'a>> // This can be done right now only because blocks are so trivial
}

// #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
// pub struct PartialInsertMessage {
//     pub col_count : u32,
//     pub col_types : Vec<(u32, BlockType)>,
//     pub blocks : Vec<Block> // This can be done right now only because blocks are so trivial
// }

#[derive(Serialize, Debug)]
pub struct ScanResultMessage<'message> {
    pub row_count : u32,
    pub col_count : u32,
    pub col_types : Vec<(u32, BlockType)>,
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

// // Typically for log compaction only
// #[derive(Serialize, Deserialize, PartialEq, Debug)]
// pub struct DataCompactionRequest {
//     pub partition_id : u64,
//     // Defines conditions that are to be matched by records subject to compaction
//     pub filters : Vec<ScanFilter>,
//     // E.g. all fields stored in column 1234 are now to be pushed to column 5678
//     pub renamed_columns: Vec<(u32, u32)>,
//     // All fields stored in this columns will be removed
//     pub dropped_columns: Vec<u32>,
//     // Following defines values that will be updated for all matching records
//     pub upserted_data: PartialInsertMessage
// }

// #[derive(Serialize, Deserialize, PartialEq, Debug)]
// pub struct RefreshCatalogResponse {
//     pub columns: Vec<Column>,
//     pub available_partitions: Vec<PartitionInfo>
// }

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AddColumnRequest {
    pub column_name: String,
    pub column_type: MemBlockType
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
pub enum ApiOperation {
    ListColumns,
    Insert,
    Scan,
    RefreshCatalog,
    AddColumn,
    Flush,
    DataCompaction
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ApiMessage {
    pub op_type : ApiOperation,
    pub payload : Vec<u8>
}

impl<'a> ApiMessage {
    pub fn extract_scan_request(&self) -> ScanRequest {
        assert_eq!(self.op_type, ApiOperation::Scan);

        let scan_request = deserialize(&self.payload[..]).unwrap();
        scan_request
    }

    // pub fn extract_insert_message(&self) -> InsertMessage {
    //     assert_eq!(self.op_type, ApiOperation::Insert);

    //     let insert_message = deserialize(&self.payload[..]).unwrap();
    //     insert_message
    // }

    // pub fn extract_data_compaction_request(&self) -> DataCompactionRequest {
    //     assert_eq!(self.op_type, ApiOperation::DataCompaction);

    //     let compaction_request = deserialize(&self.payload[..]).unwrap();
    //     compaction_request
    // }

    pub fn extract_add_column_message(&self) -> AddColumnRequest {
        deserialize(&self.payload[..]).unwrap()
    }

    pub fn to_operation(self) -> Request {
        match self.op_type {
            ApiOperation::ListColumns => Request::ListColumns,
            //Insert    => Request::Insert(self.extract_insert_message()),
            ApiOperation::AddColumn => Request::AddColumn(self.extract_add_column_message()),
            _         => Request::Other
        }
    }
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
    typ: MemBlockType,
    name: String
}

#[derive(Debug, Serialize)]
pub enum ApiReply {
    ListColumns(Vec<ReplyColumn>),
    Insert,
    Scan,
    RefreshCatalog,
    AddColumn,
    Flush,
    DataCompaction,
    Other
}

// impl Serialize for ApiReply {

//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//         where S: Serializer
//     {
//         match *self {
//             ApiReply::ListColumns(ref columns) => self.serialize_columns(&columns, &serializer),
//             ApiReply::Other => serializer.serialize_none()
//         }
//     }
// }

impl ApiReply {

    fn list_columns(catalog: & Catalog) -> ApiReply {
        use std::ops::Deref;

        let cm : &ColumnMap = catalog.as_ref();
        let mut names = vec![]; //vec!["dummy".to_owned()];
        for column in cm.values() {
            match *column.deref() {
                BlockType::Memory(typ) => names.push(ReplyColumn{typ: typ, name: format!("{}", column)}),
                _ => () // TODO
            };
        }
        ApiReply::ListColumns(names)
    }

    fn add_column(request: AddColumnRequest, catalog: &mut Catalog) -> ApiReply {
        let column = Column::new(BlockType::Memory(request.column_type), request.column_name.as_str());
        let id = 0;
        let mut map = HashMap::new();
        map.insert(id, column);

        catalog.ensure_columns(map).ok();
        ApiReply::AddColumn
    }

    // fn serialize_columns<S: Serializer>(&self, columns: &Vec<String>, serializer: &S) -> Result<S::Ok, S::Error> {
    //     serializer.serialize_i64(columns.len() as i64);
    //     for column in columns {
    //         serializer.serialize_i64(column.len() as i64);
    //         serializer.serialize_str(column);
    //     }
    //     serializer.serialize_none()
    // }
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


pub fn run_request(req: Request, catalog: &mut Catalog) -> ApiReply {
    match req {
        Request::ListColumns => ApiReply::list_columns(catalog),
        Request::AddColumn(request) => ApiReply::add_column(request, catalog),
        _ => ApiReply::Other
    }
}

// impl RefreshCatalogResponse {
//     pub fn new(manager: &Manager) -> RefreshCatalogResponse {
//         RefreshCatalogResponse {
//             columns: manager.catalog.columns.to_owned(),
//             available_partitions: manager.catalog.available_partitions.to_owned()
//         }
//     }
// }

// FIXME: this is ugly copypasta

// fn consume_empty_filter<'a>(manager : &Manager, cache : &'a mut BlockCache, consumer : &mut BlockScanConsumer) {
//     let scanned_block = manager.load_block(&cache.partition_info, 0); // ts

//     match &scanned_block {
//         &Block::Int64Dense(ref x) => {
//             for i in 0..x.data.len() {
//                 consumer.matching_offsets.push(i as u32);
//             }
//         },
//         _ => println!("This is unexpected - TS is not here")
//     }

//     cache.cache_block(scanned_block, 0);
// }

// fn consume_filters<'a>(manager : &'a Manager, cache: &'a mut BlockCache, filter: &'a ScanFilter, mut consumer: &mut BlockScanConsumer) {
//     let scanned_block = manager.load_block(&cache.partition_info, filter.column); // ts
//     // String or Int?
//     //manager.catalog.columns[filter.column]

//     match &scanned_block {
//         &Block::StringBlock(ref x) => {
//             let str_value:String = String::from_utf8(filter.str_val.to_owned()).unwrap();
//             scanned_block.scan(filter.op.clone(), &str_value, &mut consumer)
//         },
//         _ => scanned_block.scan(filter.op.clone(), &filter.val, &mut consumer)
//     }

//     cache.cache_block(scanned_block, filter.column);

//     // FIXME: why following doesn't work and we need to use the above way?

// //    let block_maybe = cache.cached_block_maybe(filter.column);
// //    match block_maybe {
// //        None => {
// //            let scanned_block = manager.load_block(&cache.partition_info, filter.column); // ts
// //            scanned_block.scan(filter.op.clone(), &filter.val, &mut consumer);
// //            cache.cache_block(scanned_block, filter.column);
// //        },
// //        Some(ref x) => {
// //            x.scan(filter.op.clone(), &filter.val, &mut consumer);
// //        }
// //    };
// }

// fn part_scan_and_combine(manager: &Manager, part_info : &PartitionInfo, mut cache : &mut BlockCache, req : &ScanRequest) -> BlockScanConsumer {
//     let mut consumers:Vec<BlockScanConsumer> = Vec::new();

//     if req.filters.is_empty() {
//         let mut consumer = BlockScanConsumer{matching_offsets : Vec::new()};
//         consume_empty_filter(manager, &mut cache, &mut consumer);
//         consumers.push(consumer);
//     } else {
//         for filter in &req.filters {
//             let mut consumer = BlockScanConsumer{matching_offsets : Vec::new()};
//             consume_filters(manager, &mut cache, &filter, &mut consumer);
//             consumers.push(consumer);
//         }
//     }

//     BlockScanConsumer::merge_and_scans(&consumers)
// }

// pub fn handle_data_compaction(manager: &Manager, req : &DataCompactionRequest) {
//     let part_info = &manager.find_partition_info(req.partition_id);
//     let mut cache = BlockCache::new(part_info);

//     let scan_req = ScanRequest {
//         min_ts: 0,
//         max_ts: u64::max_value(),
//         partition_id: req.partition_id,
//         filters: req.filters.to_owned(),
//         projection: vec![]
//     };

//     let combined_consumer = part_scan_and_combine(manager, part_info, &mut cache, &scan_req);

//     // We have the list of offsets now, lets modify blocks now

//     // 1. removed blocks
//     for col in &req.dropped_columns {
//         let mut cur = manager.load_block(part_info, *col);
//         cur.delete(&combined_consumer.matching_offsets);
//         manager.save_block(part_info, &cur, *col);
//     }

//     // 2. moved blocks
//     for col_pair in &req.renamed_columns {
//         let mut c0 = manager.load_block(part_info, col_pair.0);
//         let mut c1 = manager.load_block(part_info, col_pair.1);

//         c0.move_data(&mut c1, &combined_consumer);
//         manager.save_block(part_info, &c0, col_pair.0);
//         manager.save_block(part_info, &c1, col_pair.1);
//     }

//     // 3. upserted blocks
//     for col_no in 0..req.upserted_data.col_count {
//         let catalog_col_no = req.upserted_data.col_types[col_no as usize].0;
//         let mut block = manager.load_block(part_info, catalog_col_no);

//         // The input block actually contains just a single value that will be multi-upserted
//         let input_block = &req.upserted_data.blocks[col_no as usize];

//         if input_block.len() != 1 {
//             panic!("The upsert block can have only one record which is copied across all matching entries");
//         }

//         match &mut block {
//             &mut Block::StringBlock(ref mut b) => match input_block {
//                 &Block::StringBlock(ref c) => b.multi_upsert(&combined_consumer.matching_offsets, c.str_data.as_slice()),
//                 _ => panic!("Non matching block types")
//             },
//             &mut Block::Int64Sparse(ref mut b) => match input_block {
//                 &Block::Int64Sparse(ref c) => b.multi_upsert(&combined_consumer.matching_offsets, c.data[0].1),
//                 _ => panic!("Non matching block types")
//             },
//             &mut Block::Int32Sparse(ref mut b) => match input_block {
//                 &Block::Int32Sparse(ref c) => b.multi_upsert(&combined_consumer.matching_offsets, c.data[0].1),
//                 _ => panic!("Non matching block types")
//             },
//             &mut Block::Int16Sparse(ref mut b) => match input_block {
//                 &Block::Int16Sparse(ref c) => b.multi_upsert(&combined_consumer.matching_offsets, c.data[0].1),
//                 _ => panic!("Non matching block types")
//             },
//             &mut Block::Int8Sparse(ref mut b) => match input_block {
//                 &Block::Int8Sparse(ref c) => b.multi_upsert(&combined_consumer.matching_offsets, c.data[0].1),
//                 _ => panic!("Non matching block types")
//             },
//             _ => panic!("Not supported block type")
//         }

//         manager.save_block(part_info, &block, catalog_col_no);
//     }
// }

// pub fn part_scan_and_materialize(manager: &Manager, req : &ScanRequest) -> ScanResultMessage {
//     let scan_duration = Instant::now();

//     let part_info = &manager.find_partition_info(req.partition_id);
//     let mut cache = BlockCache::new(part_info);

//     let combined_consumer = part_scan_and_combine(manager, part_info, &mut cache, req);

//     let mut scan_msg = ScanResultMessage::new();
//     combined_consumer.materialize(&manager, &mut cache, &req.projection, &mut scan_msg);

//     let total_matched = combined_consumer.matching_offsets.len();
//     let total_materialized = scan_msg.row_count;

//     println!("Scanning and matching/materializing {}/{} elements took {:?}", total_matched, total_materialized, scan_duration.elapsed());

//     scan_msg
// }

// #[test]
// fn string_filters() {
//     let input_str_val_bytes:Vec<u8> = vec![84, 101];
//     let filter_val:String = String::from_utf8(input_str_val_bytes).unwrap();
//     assert_eq!("Te", filter_val);
// }

// #[test]
// fn data_compaction_test() {

// }

// #[test]
// fn inserting_works() {
//     let mut test_msg:Vec<u8> = vec![];

//     let base_ts = 1495490000 * 1000000;

//     let insert_msg = InsertMessage {
//         row_count: 3,
//         col_count: 5,
//         col_types: vec![(0, BlockType::Int64Dense), (1, BlockType::Int64Dense), (2, BlockType::Int64Sparse), (4, BlockType::Int64Sparse)],
//         blocks: vec![
//             Block::Int64Dense(Int64DenseBlock{
//                 data: vec![base_ts, base_ts+1000, base_ts+2000]
//             }),
//             Block::Int64Dense(Int64DenseBlock{
//                 data: vec![0, 0, 1, 2]
//             }),
//             Block::Int64Sparse(Int64SparseBlock{
//                 data: vec![(0, 100), (1, 200)]
//             }),
//             Block::Int64Sparse(Int64SparseBlock{
//                 data: vec![(2, 300), (3, 400)]
//             }),
//         ]
//     };
    
//     test_msg.extend(serialize(&insert_msg, Infinite).unwrap());

//     println!("In test {:?}", test_msg);
// //    insert_serialized_request(&test_msg);
// }

// #[test]
// fn api_scan_message_serialization() {
//     let scan_req = ScanRequest {
//         min_ts: 100 as u64,
//         max_ts: 200 as u64,
//         partition_id: 0,
//         filters: vec![
//             ScanFilter {
//                 column: 5,
//                 op: ScanComparison::GtEq,
//                 val: 1000 as u64,
//                 str_val: vec![]
//             }
//         ],
//         projection: vec![0,1,2,3]
//     };

//     let api_msg = ApiMessage {
//         op_type: ApiOperation::Scan,
//         payload: serialize(&scan_req, Infinite).unwrap()
//     };

//     let serialized_msg = serialize(&api_msg, Infinite).unwrap();

//     println!("Filter #1: {:?}", serialize(&scan_req.filters[0], Infinite).unwrap());
//     println!("Filters: {:?}", serialize(&scan_req.filters, Infinite).unwrap());
//     println!("Projection: {:?}", serialize(&scan_req.projection, Infinite).unwrap());
//     println!("Scan request: {:?}", serialize(&scan_req, Infinite).unwrap());
//     println!("Payload length: {}", api_msg.payload.len());
//     println!("Serialized api message for scan: {:?}", serialized_msg);
// }

// #[test]
// fn api_refresh_catalog_serialization() {
//     let pseudo_response = RefreshCatalogResponse{
//         columns: vec![
//             Column {
//                 data_type: BlockType::Int64Dense,
//                 name: String::from("ts")
//             },
//             Column {
//                 data_type: BlockType::Int32Sparse,
//                 name: String::from("source")
//             }
//         ],
//         available_partitions: vec![
//             PartitionInfo{
//                 min_ts: 100,
//                 max_ts: 200,
//                 id: 999,
//                 location: String::from("/foo/bar")
//             }
//         ]
//     };

//     let x:String = String::from("abc");
//     println!("String response: {:?}", serialize(&x, Infinite).unwrap());
//     println!("Pseudo catalog refresh response: {:?}", serialize(&pseudo_response, Infinite).unwrap());
// }
