
#![allow(unused)]

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


#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub enum DataType{
    Integer,
    Str
}

impl DataType{
    pub fn from_string(value: &str) -> DataType{
        if value == "i"{
            return DataType::Integer;
        }
        return DataType::Str;
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Field{
    pub name: String,
    pub data_type: DataType,
}

impl Field{
    pub fn extract_as_byte(&self, value:&json::JsonValue) -> Vec<u8>{
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
pub struct Schema{
    pub name: String,
    pub primary_key_name: String,
    pub fields: Vec<Field>
}


impl Schema{
    pub fn primary(&self)->&Field{
        self.get_field(&self.primary_key_name).unwrap() // 存在しないなら落ちてOK
    }

    pub fn get_field(&self, name: &str) -> Option<&Field>{
        self.fields.iter().find(|x|x.name==name)
    }

    pub fn load(table: &str)->Result<Schema, Box<dyn std::error::Error>>{
        let schema_str = std::fs::read_to_string(format!("{}.def", table))?;
        match serde_json::from_str::<Schema>(&schema_str){
            Ok(x)=> Ok(x),
            Err(e) => Err(Box::new(e))
        }
    }

    pub fn make_record_from_json(&self, json_str: &str) -> Result<Record, Box<dyn Error>>{
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

    pub fn get_field_index(&self, name: &str) -> Option<usize>{
        self.fields.iter().position(|x|x.name==name)
    }

    pub fn get_field_i64(&self, record: &Record, name: &str)->Option<i64>{
        let index = self.get_field_index(name);
        index.map(|i| &record.header[i]).map(
            |f: &FieldIndex| -> i64{
                let mut d1:[u8; 8] = [0;8];
                d1.copy_from_slice(&record.body[(f.offset as usize) .. (f.offset+f.len) as usize]);
                i64::from_ne_bytes(d1)
            }
        )
    }

    pub fn get_key(&self, record: &Record) -> i64{
        self.get_field_i64(record, &self.primary().name).unwrap()
    }

    
    pub fn get_field_str<'a>(&self, record: &'a Record, name: &str)->Option<&'a str>{
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
pub struct FieldIndex{
    pub offset: u16,
    pub len: u16
}

#[derive(Clone, Debug)]
// 
pub struct Record{
    pub header: Vec<FieldIndex>,
    // size: i64
    pub body: Vec<u8>
}


impl Record{
    pub fn total_size(&self) -> usize{
        self.header_size()+self.total_size()
    }

    pub fn header_size(&self)->usize{
        self.header.len() * std::mem::size_of::<FieldIndex>()
    }

    pub fn body_size(&self) -> usize{
        self.body.len() * std::mem::size_of::<u8>()
    }

    pub fn record_size(&self)->usize{
        std::mem::size_of::<usize>() * 2 + self.total_size()
    }
}

#[cfg(test)]
mod tests{
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 3);
    }

    #[test]
    fn serialize_deserialize_schema(){
        // let scm = Schema::load(table: &str)
    }
}