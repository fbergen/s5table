#[allow(unused_imports)]
#[macro_use]
extern crate rocket;

use rocket::FromForm;
use rocket::State;
use rocket::{get, routes};
use rocket_cors::{AllowedHeaders, AllowedOrigins};
use sstable::filter;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task;

extern crate s5table;
use s5table::gcs::GCSFile;
#[allow(unused_imports)]
use s5table::s3::S3File;

#[derive(FromForm, Debug)]
struct GetParam {
    key: Option<String>,
}

async fn get_table_entry(
    table: &sstable::Table,
    key: String,
) -> Result<Option<Vec<u8>>, sstable::Status> {
    table.get(key.as_bytes())
}

//fn get(table: State<sstable::Table >, params: LenientForm<GetParam>) -> Option<Vec<u8>> {
#[get("/get?<params..>")]
fn get(table: &State<sstable::Table>, params: GetParam) -> Option<Vec<u8>> {
    let k = params.key?;
    println!("Get key {:?}", k);

    let res = task::block_in_place(move || {
        let handle = Handle::current();
        handle.block_on(get_table_entry(table, k))
    });
    res.unwrap()
}

#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    println!("Reading sstable file");

    // let file = GCSFile::new("githope-eu", "experimental/test.sstable").await;
    let file = S3File::new("flyvc", "fberge/test.sstable").await;
    println!("Done Readng sstable file");
    let len = file.len as usize;
    let mut options = sstable::Options::default();
    options.filter_policy = Arc::new(Box::new(filter::NoFilterPolicy::new()));

    let table = sstable::Table::new(options, Box::new(file), len).unwrap();

    let allowed_origins = AllowedOrigins::all();

    let _cors = rocket_cors::CorsOptions {
        allowed_origins,
        allowed_headers: AllowedHeaders::all(),
        allow_credentials: true,
        ..Default::default()
    }
    .to_cors()
    .expect("Valid cors");

    rocket::build()
        //.attach(cors)
        .manage(table)
        .mount("/", routes![get])
        .ignite()
        .await?
        .launch()
        .await
}
