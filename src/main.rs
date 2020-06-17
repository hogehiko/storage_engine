
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
    Query(Query),
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
struct Query{
    #[clap(long)]
    key: String,

    #[clap(long)]
    eq: Option<String>
}

#[derive(Clone, Serialize, Deserialize)]
enum DataType{
    Integer,
    Str // 256 bytes固定
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
}

struct Record{
    index: i64,
    // size: i64
}
struct Table{
    schema: Schema,
    segments: Vec<Box<dyn Segment>>

}

impl Table{
    fn load(name: &str){
        
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


fn main() -> std::io::Result<()> {
    let opts: Opts = Opts::parse();
    match opts.subcmd {
        SubCommand::Load(l) => {
            println!("Load {}", l.file);
        },
        SubCommand::Query(q) => {
            println!("Query {}", q.key);
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

    let mut bt = BTreeMap::new();
    bt.insert(3, "333");
    bt.insert(1, "111");
    bt.insert(2, "222");
    bt.insert(-100, "222");

    for (k,v) in bt.into_iter(){
        println!("{}", k);
    }
    Ok(())
}