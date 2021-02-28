extern crate rayon;
extern crate soup;

use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read, Result};
use std::path::Path;
use std::sync::mpsc;

use bzip2::bufread::BzDecoder;
use soup::{NodeExt, QueryBuilderExt, Soup};

use super::entity;
use super::files;

pub fn get_wikipedia_producer<P: 'static + AsRef<Path>>(
    index_file: P,
    data_file: P,
    num_concurrency: usize,
) -> Result<mpsc::Receiver<Vec<entity::WikipediaPage>>> {
    let index_reader = match files::get_multi_bz_reader(index_file) {
        Ok(reader) => reader,
        Err(err) => return Err(err)
    };

    let mut data_reader = match File::open(data_file) {
        Ok(file) => file,
        Err(err) => return Err(err),
    };

    let (tx, rx) = mpsc::sync_channel(num_concurrency);
    let pool = rayon::ThreadPoolBuilder::new().build().unwrap();

    std::thread::spawn(move || {
        let mut last_offset = 0;

        for line in index_reader.lines() {
            let line = line.expect("read line");
            let offset: Vec<&str> = line.split(":").collect();
            let offset: usize = offset[0].parse().expect("parse i32");
            let chunk = offset - last_offset;
            if chunk == 0 {
                continue;
            }
            let mut buf = vec![0u8; chunk];
            let tx_clone = mpsc::SyncSender::clone(&tx);
            data_reader.read_exact(&mut buf).unwrap();
            pool.spawn(move || {
                let reader = Cursor::new(buf);
                let reader = BzDecoder::new(reader);
                let reader = BufReader::new(reader);
                let reader = Soup::from_reader(reader);
                if reader.is_err() {
                    println!("chunk err from {} to {}", last_offset, offset);
                    return;
                }
                let reader = reader.unwrap();
                let mut v = Vec::new();
                for page in reader.tag("page").find_all() {
                    let id = page.tag("id").find().expect("id").text();
                    let title = page.tag("title").find().expect("title").text();
                    let content = page.tag("text").find().expect("text").text();
                    v.push(entity::WikipediaPage { id, title, content });
                }
                tx_clone.send(v).unwrap();
                drop(tx_clone);
            });
            last_offset = offset;
        }
        drop(tx);
    });

    Ok(rx)
}
