use aws_sdk_s3::output::GetObjectOutput;
use aws_sdk_s3::Client;
use bytes::Buf;
use futures::executor::block_on;
use sstable::RandomAccess;
use std::error::Error;
use std::io;
use std::io::{Read, Seek};

pub struct S3File {
    bucket: String,
    path: String,
    pos: i64,
    pub len: i64,
    client: Client,
}

impl S3File {
    pub async fn new(bucket: &str, path: &str) -> S3File {
        let shared_config = aws_config::load_from_env().await;
        let client = Client::new(&shared_config);

        // get length.
        let resp = client
            .head_object()
            .bucket(bucket)
            .key(path)
            .send()
            .await
            .unwrap();
        // println!("{:?}", resp);

        S3File {
            bucket: bucket.to_string(),
            path: path.to_string(),
            pos: 0,
            len: resp.content_length,
            client,
        }
    }

    pub async fn async_read_at(&self, off: i64, buf: &mut [u8]) -> Result<usize, Box<dyn Error>> {
        // read these many bytes
        let len = buf.len() as i64;
        println!("BUF LEN: {}", len);

        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.path)
            .set_range(Some(format!("bytes={}-{}", off, off + len - 1)))
            .send()
            .await?;
        // println!("From client: {:?}", resp);

        let read_bytes = match resp {
            GetObjectOutput {
                body,
                content_length,
                ..
            } => {
                body.collect()
                    .await
                    .map(|mut data| data.copy_to_slice(buf))?;
                content_length as usize
            }
        };

        Ok(read_bytes)
    }
}

impl Read for S3File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let future = self.async_read_at(self.pos, buf);
        block_on(future)
            .map(|o| {
                self.pos += o as i64;
                o
            })
            .map_err(|_e| io::Error::from_raw_os_error(22))
    }
}

impl Seek for S3File {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        match pos {
            io::SeekFrom::Start(x) => self.pos = x as i64,
            io::SeekFrom::End(x) => self.pos = self.len + x as i64,
            io::SeekFrom::Current(x) => self.pos = self.pos + x as i64,
        }
        Ok(self.pos as u64)
    }
}

use sstable::Status;
impl RandomAccess for S3File {
    fn read_at(&self, off: usize, dst: &mut [u8]) -> std::result::Result<usize, Status> {
        let future = self.async_read_at(off as i64, dst);
        block_on(future).map_err(|_e| Status::from(io::Error::from_raw_os_error(22)))
    }
}
