extern crate rocket;

use rocket::form::FromForm;
use rocket::http::Method;
use rocket::{get, launch, routes, State};
use rocket_cors::{AllowedHeaders, AllowedOrigins};
use sstable::filter;
use sstable::RandomAccess;
use std::env;
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

async fn get_file(path: &str) -> (Box<dyn RandomAccess>, usize) {
    match path.starts_with("gs://") {
        true => {
            let (bucket, dir) = path.strip_prefix("gs://").unwrap().split_once("/").unwrap();
            let f = GCSFile::new(bucket, dir).await;
            let l = f.len as usize;
            (Box::new(f), l)
        }
        false => {
            let (bucket, dir) = path.strip_prefix("s3://").unwrap().split_once("/").unwrap();
            let f = S3File::new(bucket, dir).await;
            let l = f.len as usize;
            (Box::new(f), l)
        }
    }
}

#[launch]
async fn rocket() -> _ {
    println!("Reading sstable file");
    let file_location = env::var("SSTABLE_FILE").expect("Need to set SSTABLE_FILE env variable");
    let (file, len) = get_file(&file_location).await;

    println!("Done Readng sstable file");
    let mut options = sstable::Options::default();
    options.filter_policy = Arc::new(Box::new(filter::NoFilterPolicy::new()));

    let table = task::spawn_blocking(move || sstable::Table::new(options, file, len).unwrap())
        .await
        .unwrap();

    let allowed_origins = AllowedOrigins::all();

    let cors = rocket_cors::CorsOptions {
        allowed_origins,
        allowed_methods: vec![Method::Get].into_iter().map(From::from).collect(),
        allowed_headers: AllowedHeaders::all(),
        allow_credentials: true,
        ..Default::default()
    }
    .to_cors()
    .expect("Valid cors");

    rocket::build()
        .attach(cors)
        .manage(table)
        .mount("/", routes![get])
}
