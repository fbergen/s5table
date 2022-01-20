#[allow(unused_imports)]
#[macro_use]
extern crate rocket;

use rocket::FromForm;
use rocket::State;
use rocket::{get, routes};
use rocket_cors::{AllowedHeaders, AllowedOrigins};
use sstable::filter;
use std::sync::Arc;
use tokio::task;

extern crate s5table;
#[allow(unused_imports)]
use s5table::gcs::GCSFile;
#[allow(unused_imports)]
use s5table::s3::S3File;

#[derive(FromForm, Debug)]
struct GetParam {
    key: Option<String>,
}

#[get("/get?<params..>")]
async fn get(table: &State<sstable::Table>, params: GetParam) -> Option<Vec<u8>> {
    let k = params.key?;
    println!("Get key {:?}", k);

    task::block_in_place(move || table.get(k.as_bytes())).unwrap()
}

#[launch]
async fn rocket() -> _ {
    println!("Reading sstable file");

    let file = GCSFile::new("githope-eu", "experimental/test.sstable").await;
    // let file = S3File::new("flyvc", "fberge/test.sstable").await;
    println!("Done Readng sstable file");
    let len = file.len as usize;
    let mut options = sstable::Options::default();
    options.filter_policy = Arc::new(Box::new(filter::NoFilterPolicy::new()));

    let table =
        task::spawn_blocking(move || sstable::Table::new(options, Box::new(file), len).unwrap())
            .await
            .unwrap();

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
}
