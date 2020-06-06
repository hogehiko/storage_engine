
#![allow(unused)]

#[macro_use]
extern crate json;

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


fn load(filename: &str){

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