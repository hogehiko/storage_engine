
#![allow(unused)]

#[macro_use]
extern crate json;
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::mem;

use clap::Clap;




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
}

#[derive(Clap)]
struct Query{
    #[clap(long)]
    key: String,

    #[clap(long)]
    eq: Option<String>
}

#[derive(Clone)]
enum DataType{
    Integer,
    Str
}


impl DataType{
    fn from_string(value: &str) -> DataType{
        if value == "Integer"{
            return DataType::Integer;
        }
        return DataType::Str;
    }
}

#[derive(Clone)]
struct Field{
    name: String,
    data_type: DataType,
}

struct Schema{
    name: String,
    primary: Field,
    fields: Vec<Field>
}


fn load(table: String, filename: String){

}

fn create(name: String, schema: String){
    let fields = to_field(&schema);
    let s = Schema{
        name: name,
        
        primary: fields[0].clone(),
        fields: fields,
    };
    ()
}

fn to_field(schema: &String) -> Vec<Field>{
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
            create(c.name, c.schema);
        },
    }
    // let f = File::open("data.json")?;
    // let buf = BufReader::new(f);

    // for line in buf.lines(){
    //     let obj = json::parse(&(line?));
    //     println!("{}", obj.unwrap());
    // }
    Ok(())
}