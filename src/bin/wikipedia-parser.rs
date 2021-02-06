extern crate num_cpus;
extern crate rayon;
extern crate soup;

use std::fs::File;
use std::io::{BufRead, BufReader, Cursor, Read, Write};
use std::sync::mpsc;
use std::thread;

use bzip2::bufread::BzDecoder;
use soup::{NodeExt, QueryBuilderExt, Soup};

use ir_playbook::ir;

fn main() {
    let data_file =
        std::env::var("HOME").unwrap() + "/enwiki-20210120-pages-articles-multistream.xml.bz2";
    let index_file = std::env::var("HOME").unwrap()
        + "/enwiki-20210120-pages-articles-multistream-index.txt.bz2";
    let out_file = std::env::var("HOME").unwrap() + "/wikipedia-trec.xml.bz2";

    let index_reader = ir::files::get_multi_bz_reader(index_file).unwrap();

    let mut data_reader = File::open(data_file).expect("fail to open data file");

    let mut writer = ir::files::get_bz_writer(out_file).unwrap();

    let mut last_offset = 0;

    let num = num_cpus::get();

    let pool = rayon::ThreadPoolBuilder::new().build().unwrap();

    let (tx, rx) = mpsc::channel();
    let (sync_tx, sync_rx) = mpsc::sync_channel(4 * num);

    let sink = thread::spawn(move || {
        for v in rx {
            for (id, title, text) in v {
                writeln!(writer, "<DOC>").unwrap();
                writeln!(writer, "<DOCNO>{}</DOCNO>", id).unwrap();
                writeln!(writer, "<HEADLINE>{}</HEADLINE>", title).unwrap();
                writeln!(writer, "<P>{}</P>", text).unwrap();
                writeln!(writer, "</DOC>").unwrap();
            }
            sync_rx.recv().unwrap();
        }
    });

    for line in index_reader.lines() {
        let line = line.expect("read line");

        let offset: Vec<&str> = line.split(":").collect();
        let offset: usize = offset[0].parse().expect("parse i32");
        let chunk = offset - last_offset;
        if chunk == 0 {
            continue;
        }

        sync_tx.send(0).unwrap();

        let mut buf = vec![0u8; chunk];
        let tx_clone = mpsc::Sender::clone(&tx);
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
                let text = page.tag("text").find().expect("text").text();

                v.push((id, title, text));
            }

            tx_clone.send(v).unwrap();

            drop(tx_clone);
        });

        last_offset = offset;
    }

    drop(tx);

    sink.join().unwrap();
}
