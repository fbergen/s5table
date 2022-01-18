extern crate leveldb;

use std::fs;
use std::sync::Arc;
extern crate db_key as key;
use crate::leveldb::compaction::Compaction;
use crate::leveldb::iterator::Iterable;
use leveldb::database::Database;
use leveldb::kv::KV;
use leveldb::options::{Options, ReadOptions, WriteOptions};
use sstable::*;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

mod s3;
use crate::s3::S3File;
mod gcs;
use crate::gcs::GCSFile;

use db_key::Key;

// mod table;
//  use crate::table::Table;

#[derive(Debug)]
struct MyKey(String);

impl MyKey {
    fn new(s: &str) -> MyKey {
        MyKey(s.to_string())
    }
}

impl Key for MyKey {
    fn from_u8(key: &[u8]) -> MyKey {
        MyKey(std::str::from_utf8(key).unwrap().to_string())
    }

    fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T {
        f(self.0.as_bytes())
    }
}

#[tokio::main]
async fn main() {
    // let path = PathBuf::from("./random.sstable");
    // write_table(&path);
    // write_test_db_locally();
    // read_db_from_s3().await;
    read_db().await;
}

fn read_table(file: Box<dyn RandomAccess>, len: usize) -> Result<sstable::Table> {
    use std::time::Instant;
    let start = Instant::now();
    println!("Getting sstable file");
    let mut options = sstable::Options::default();
    options.filter_policy = Arc::new(Box::new(filter::NoFilterPolicy::new()));
    let tr = sstable::Table::new(options, file, len)?;

    println!("Startup: {}µs", start.elapsed().as_micros());
    let start = Instant::now();

    let k = format!("{:010}", 3943243);
    let k = "google/leveldb";
    println!("Get key");
    let res = tr.get(k.as_bytes());

    println!("{:?}", &res);
    println!("Read: {}µs", start.elapsed().as_micros());
    // let mut iter = tr.iter();
    // while iter.advance() {
    //     let (k, v) = sstable::current_key_val(&iter).unwrap();
    //     println!(
    //         "{} => {}",
    //         String::from_utf8(k).unwrap(),
    //         String::from_utf8(v).unwrap()
    //     );
    // }
    Ok(tr)
}

async fn read_db() {
    let _path = PathBuf::from("./test_db/000034.ldb");

    //let f = S3File::new("flyvc", "fberge/000034.ldb").await;
    let _f = S3File::new("flyvc", "fberge/random.sstable").await;

    let f = GCSFile::new("githope-eu", "experimental/test.sstable").await;
    // let f = GCSFile::new("githope-eu", "empty.json").await;

    let mut buffer = [0; 10];
    println!("{:?}", f.async_read_at(0, &mut buffer).await);

    // let f = S3File::new("flyvc", "fberge/test.sstable").await;

    let mut buffer = [0; 10];
    println!("{:?}", f.async_read_at(0, &mut buffer).await);
    let mut buffer = [0; 59];
    println!("{:?}", f.async_read_at(2083698776, &mut buffer).await);
    // println!("{:?}", f.async_read_at(0, &mut buffer).await);
    // println!("{:?}", f.async_read_at(0, &mut buffer).await);
    // println!("{:?}", f.async_read_at(0, &mut buffer).await);

    let l = f.len as usize;

    let tr = read_table(Box::new(f), l).expect("Reading the table failed");
}

async fn read_db_from_s3() {
    //let mut f = S3File::new("flyvc", "fberge/newcompanies.jsonl").await;
    let mut f = S3File::new("flyvc", "fberge/db_structure_company.sql").await;

    let mut buffer = [0; 10];

    let n = f.read(&mut buffer[..]).unwrap();
    println!(
        "The bytes: {:?}",
        std::str::from_utf8(&buffer[..n]).unwrap()
    );
}

fn write_table(p: &Path) -> Result<()> {
    let dst = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(p)?;
    let mut tb = sstable::TableBuilder::new(sstable::Options::default(), dst);

    for i in 0..10000000 {
        let k = format!("{:010}", i);
        let v = format!("{}value", i);
        tb.add(&k.as_bytes().to_vec(), &v.as_bytes().to_vec())?;
    }

    tb.finish()?;
    Ok(())
}

fn write_test_db_locally() {
    let path = Path::new("./test_db2");
    println!("writing to {:?}", path);

    let mut options = Options::new();
    options.create_if_missing = true;
    let database: Database<MyKey> = match Database::open(path, options) {
        Ok(db) => db,
        Err(e) => {
            panic!("failed to open database: {:?}", e)
        }
    };

    let write_opts = WriteOptions::new();
    for i in 0..10000000 {
        let k = format!("{}", i);
        let v = format!("{}value", i);
        match database.put(write_opts, MyKey(k), v.as_bytes()) {
            Ok(_) => (),
            Err(e) => {
                panic!("failed to write to database: {:?}", e)
            }
        };
    }

    println!("compacting...");
    database.compact(&MyKey::new("0"), &MyKey::new("AAAAAAAAAAAAA"));

    database
        .iter(ReadOptions::new())
        .for_each(|(k, v)| println!("{:?}", (k, v)));

    let read_opts = ReadOptions::new();
    let res = database.get(read_opts, &MyKey::new("1"));

    match res {
        Ok(data) => {
            assert!(data.is_some());
            assert_eq!(data, Some(b"1value".to_vec()));
        }
        Err(e) => {
            panic!("failed reading data: {:?}", e)
        }
    }

    //let t = Table::open("./test_db/DBCOPY.ldb");
    //println!("{:?}", t.get("1"));
}
