use bytes::Buf;
use cloud_storage::Client;
use futures::executor::block_on;
use sstable::RandomAccess;
use std::error::Error;
use std::io;
use std::io::{Read, Seek};
use tokio::runtime::Handle;

pub struct GCSFile {
    bucket: String,
    path: String,
    pos: i64,
    pub len: i64,
    // client: Client,
    //  Ugh... can't have client as it's not thread safe...
    //  For MVP, simply recreate the client all the time.
}

impl GCSFile {
    pub async fn new(bucket: &str, path: &str) -> GCSFile {
        // let client = Client::default();
        let len = Client::default()
            .object()
            .read(bucket, path)
            .await
            .unwrap()
            .size;
        GCSFile {
            bucket: bucket.to_string(),
            path: path.to_string(),
            pos: 0,
            len: len as i64,
        }
    }

    pub async fn async_read_at(&self, off: i64, buf: &mut [u8]) -> Result<usize, Box<dyn Error>> {
        let client = Client::default();
        let len = buf.len();

        let resp = client
            .object()
            .download_with_range(
                &self.bucket,
                &self.path,
                Some(off as usize),
                Some(off as usize + len - 1),
            )
            .await
            .unwrap();

        (&resp[..]).copy_to_slice(&mut buf[..resp.len()]);

        Ok(resp.len())
    }
}

impl Read for GCSFile {
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

impl Seek for GCSFile {
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
impl RandomAccess for GCSFile {
    fn read_at(&self, off: usize, dst: &mut [u8]) -> std::result::Result<usize, Status> {
        let handle = Handle::current();
        let future = self.async_read_at(off as i64, dst);
        handle
            .block_on(future)
            .map_err(|_e| Status::from(io::Error::from_raw_os_error(22)))
    }
}
