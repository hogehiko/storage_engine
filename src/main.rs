
#![allow(unused)]

#[macro_use]
extern crate json;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate serde_json;

use std::fs::File;
use std::error::Error;
use std::io::BufReader;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::io::{self, ErrorKind};
use std::io::stdin;
use std::mem;

use std::collections::BTreeMap;

use clap::Clap;

use serde::{Serialize, Deserialize};




#[derive(Clap)]
#[clap(version = "0.0", author = "Takehiko Iwakawa<takrockjp@gmail.com>")]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand
}

#[derive(Clap)]
enum SubCommand {
    #[clap(version = "0.0", author = "Takehiko Iwakawa<takrockjp@gmail.com>")]
    Load(Load),
    Server(Server),
    Create(Create)
}

#[derive(Clap)]
struct Load {
    #[clap(long)]
    file: String,

    #[clap(long)]
    table: String,
}

#[derive(Clap)]
struct Create {
    #[clap(long)]
    name: String,

    #[clap(long)]
    schema: String,

    #[clap(long)]
    primary: String,
}

#[derive(Clap)]
struct Server{
    #[clap(long)]
    table: String,

    #[clap(long)]
    data_json: Option<String>
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
enum DataType{
    Integer,
    Str
}



impl DataType{
    fn from_string(value: &str) -> DataType{
        if value == "i"{
            return DataType::Integer;
        }
        return DataType::Str;
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Field{
    name: String,
    data_type: DataType,
}

impl Field{
    fn extract_as_byte(&self, value:&json::JsonValue) -> Vec<u8>{
        let data = &value[&self.name];
        if(self.data_type == DataType::Integer){
            let i = data.as_i64().unwrap();
            Vec::from(i.to_le_bytes())
        }else if(self.data_type == DataType::Str){
            let d = data.as_str().unwrap();
            String::from(d).into_bytes()
        }else{
            panic!()
            //ok
        }
    }
} 



#[derive(Serialize, Deserialize)]
struct Schema{
    name: String,
    primary: Field,
    fields: Vec<Field>
}


impl Schema{
    fn load(table: &str)->Result<Schema, Box<dyn std::error::Error>>{
        let schema_str = std::fs::read_to_string(format!("{}.def", table))?;
        match serde_json::from_str::<Schema>(&schema_str){
            Ok(x)=> Ok(x),
            Err(e) => Err(Box::new(e))
        }
    }

    fn make_record_from_json(&self, json_str: &str) -> Result<Record, Box<dyn Error>>{
        let json_obj = json::parse(json_str)?;
        let mut all_data = Vec::<u8>::new();
        let mut field_index = Vec::<FieldIndex>::new();
        let mut current_offset = 0;
        for f in self.fields.iter(){
            //let value = &json_obj[&f.name];
            let mut data = f.extract_as_byte(&json_obj);
            all_data.append(&mut data);
            field_index.push(FieldIndex{
                offset: current_offset,
                len: data.len() as u16
            });
            current_offset += data.len() as u16;
        }
        Ok(Record{
            header: field_index,
            body: all_data
        })
    }

    fn get_field_index(&self, name: &str) -> Option<usize>{
        self.fields.iter().position(|x|x.name==name)
    }

    fn get_field_i64(&self, record: &Record, name: &str)->Option<i64>{
        let index = self.get_field_index(name);
        index.map(|i| &record.header[i]).map(
            |f: &FieldIndex| -> i64{
                let mut d1:[u8; 8] = [0;8];
                d1.copy_from_slice(&record.body[(f.offset as usize) .. (f.offset+f.len) as usize]);
                i64::from_ne_bytes(d1)
            }
        )
    }

    fn get_key(&self, record: &Record) -> i64{
        self.get_field_i64(record, &self.primary.name).unwrap()
    }

    
    fn get_field_str<'a>(&self, record: &'a Record, name: &str)->Option<&'a str>{
        let index = self.get_field_index(name);
        index.map(|i| &record.header[i]).map(
            |f: &FieldIndex| -> &str{
                let slice = &record.body[(f.offset as usize) .. (f.offset+f.len) as usize];
                std::str::from_utf8(slice).unwrap()
            }
        )
        
    }
}

#[derive(Clone, Debug)]
struct FieldIndex{
    offset: u16,
    len: u16
}

#[derive(Clone, Debug)]
// 
struct Record{
    header: Vec<FieldIndex>,
    // size: i64
    body: Vec<u8>
}


impl Record{
    fn total_size(&self) -> usize{
        self.header_size()+self.total_size()
    }

    fn header_size(&self)->usize{
        self.header.len() * std::mem::size_of::<FieldIndex>()
    }

    fn body_size(&self) -> usize{
        self.body.len() * std::mem::size_of::<u8>()
    }

    fn record_size(&self)->usize{
        std::mem::size_of::<usize>() * 2 + self.total_size()
    }
}
struct Table{
    schema: Schema,
    in_memory_segment: InMemorySegment,
    file_segment: Vec<FileSegment>
}

impl Table{
    fn open(name: &str)-> Result<Table, Box<dyn Error>>{
        let schema = Schema::load(name)?;
        let memseg = InMemorySegment::new();
        Ok(Table{
            schema: schema,
            in_memory_segment: memseg,
            file_segment: Vec::new()
        })
    }

    fn insert(&mut self, record: &Record){
        self.in_memory_segment.insert(&self.schema, record);
    }

    fn load_data_in_file(&self, filepath: &str)->Result<(), Box<dyn Error>>{
        Ok(())
    }

    fn find(&self, index: i64) -> Option<&Record>{
        self.in_memory_segment.get(index)
    }
}

trait Segment{
    fn get(&self, index: i64)->Option<&Record>;
}

struct InMemorySegment{
    data: BTreeMap<i64, Record>
}

impl Segment for InMemorySegment{
    fn get(&self, index: i64)->Option<&Record> {
        self.data.get(&index)
    }
}

impl InMemorySegment{
    fn new( ) -> InMemorySegment{
        InMemorySegment{
            data: BTreeMap::new()
        }
    }
    fn insert(&mut self, schema: &Schema, record:&Record){
        self.data.insert(schema.get_key(record), record.clone());
    }
}

struct FileSegment{
    bodyfile: File,
    indexfile: File
}

// メモリに詰むための疎なinde
// sort済みデータしか扱わないのでrebalance不要 ->　バイナリサーチで良い
struct IndexEntry{
    key: u64,
    position: usize
}
struct Index{
    index: Vec<IndexEntry>
}

impl Index{
    fn find_nearest_position(&self, key:&u64)->usize{
        fn _inner(index: &[IndexEntry], key: &u64)->usize{
            let center = index.len() / 2;
            if index.len() == 1{
                index[0].position
            }else if index[center].key == *key{
                index[center].position
            }else if index[center].key > *key{
                _inner(&index[0..center-1], key)
            }else{
                _inner(&index[center..], key)
            }
        }
        _inner(&self.index.as_slice(), key)
    }
}



impl FileSegment{
    // fn new(filepath: bodyfile_name, indexfile_name) {
        
    // }
}

impl Segment for FileSegment{
    fn get(&self, index: i64)->Option<&Record> {
        None
    }
}




fn load(table: String, filename: String)->std::io::Result<()>{
    let schema_str = std::fs::read_to_string(format!("{}.def", &table))?;
    let schema = serde_json::from_str::<Schema>(&schema_str);
    
    Ok(())
}

fn create(name: &str, schema: &str, primary: &str)->std::io::Result<()> {
    let fields = to_field(schema);
    let primary_f = fields.iter().find(|a|a.name == *primary);
    
    match primary_f {
        Some(x) => {
            let s = Schema{
                name: name.to_string(),
                primary: x.clone(),
                fields: fields,
            };
            let mut schema_file = File::create(format!("{}.def", name))?;
            schema_file.write_all(serde_json::to_string(&s)?.as_bytes());
            Ok(())
        },
        None => {
            Err(std::io::Error::new(ErrorKind::InvalidInput, "aaa"))
        }
    }
}

fn to_field(schema: &str) -> Vec<Field>{
    let mut result = Vec::new();
    for f in schema.split(","){
       let fs = f.split(":").collect::<Vec<&str>>();
       let name = fs[0];
       let t = DataType::from_string(fs[1]);
       result.push(Field{name: name.to_string(), data_type: t});
    }
    result
}

fn query_loop(table: &str, initial_data: &str)->Result<(), Box<dyn Error>>{
    // initialize data
    let table = Table::open(table)?;
    table.load_data_in_file(initial_data)?;
    loop{
        let mut key: String = String::new();
        stdin().read_line(&mut key);

        if key == "exit"{
            break;
        }else{
            
            let key: i64 = key.parse()?;
            let result = table.find(key);
            println!("{:?}", result);
        }
    }
    Ok(())
}


fn main() -> std::io::Result<()> {
    let opts: Opts = Opts::parse();
    match opts.subcmd {
        SubCommand::Load(l) => {
            println!("Load {}", l.file);
        },
        SubCommand::Server(option) => {
            query_loop(&option.table,  &option.data_json.unwrap());
            ()
        },
        SubCommand::Create(c) => {
            println!("Create {}", c.name);
            create(&(c.name), &(c.schema), &(c.primary));
        },
    }
    // let f = File::open("data.json")?;
    // let buf = BufReader::new(f);

    // for line in buf.lines(){
    //     let obj = json::parse(&(line?));
    //     println!("{}", obj.unwrap());
    // }

    // let mut bt = BTreeMap::new();
    // bt.insert(3, "333");
    // bt.insert(1, "111");
    // bt.insert(2, "222");
    // bt.insert(-100, "222");

    // for (k,v) in bt.into_iter(){
    //     println!("{}", k);
    // }
    Ok(())
}