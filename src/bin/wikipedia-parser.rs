extern crate soup;

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Cursor, Read, Write};

use bzip2::bufread::{BzDecoder, MultiBzDecoder};
use bzip2::Compression;
use bzip2::write::BzEncoder;
use soup::{NodeExt, QueryBuilderExt, Soup};

fn main() {
    let data_file = std::env::var("HOME").unwrap() + "/enwiki-20210120-pages-articles-multistream.xml.bz2";
    let index_file = std::env::var("HOME").unwrap() + "/enwiki-20210120-pages-articles-multistream-index.txt.bz2";
    let out_file = std::env::var("HOME").unwrap() + "/wikipedia-trec.xml.bz2";

    let index_reader = File::open(index_file).expect("fail to open index file");
    let index_reader = BufReader::new(index_reader);
    let index_reader = MultiBzDecoder::new(index_reader);
    let index_reader = BufReader::new(index_reader);

    let mut data_reader = File::open(data_file).expect("fail to open data file");

    let writer = File::create(out_file).expect("fail to open file");
    let writer = BufWriter::new(writer);
    let writer = BzEncoder::new(writer, Compression::best());
    let mut writer = BufWriter::new(writer);

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
        data_reader.read_exact(&mut buf).unwrap();

        let reader = Cursor::new(buf);
        let reader = BzDecoder::new(reader);
        let reader = BufReader::new(reader);
        let reader = Soup::from_reader(reader);
        if reader.is_err() {
            println!("chunk err from {} to {}", last_offset, offset);
            continue;
        }
        let reader = reader.unwrap();

        for page in reader.tag("page").find_all() {
            let id = page.tag("id").find().expect("id").text();
            let title = page.tag("title").find().expect("title").text();
            let text = page.tag("text").find().expect("text").text();

            writeln!(writer, "<DOC>").unwrap();
            writeln!(writer, "<DOCNO>{}</DOCNO>", id).unwrap();
            writeln!(writer, "<HEADLINE>{}</HEADLINE>", title).unwrap();
            writeln!(writer, "<P>{}</P>", text).unwrap();
            writeln!(writer, "</DOC>").unwrap();
        }

        last_offset = offset;
    }
}