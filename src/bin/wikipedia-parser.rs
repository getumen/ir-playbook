extern crate quick_xml;

use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::str::from_utf8;

use bzip2::bufread::BzDecoder;
use bzip2::read::MultiBzDecoder;
use quick_xml::events::Event;
use quick_xml::Reader;

fn main() {
    let file_name = std::env::var("HOME").unwrap() + "/enwiki-20210120-pages-articles-multistream.xml.bz2";

    let file = File::open(file_name).expect("fail to open file");
    let reader = BufReader::new(file);
    let reader = MultiBzDecoder::new(reader);
    let reader = BufReader::new(reader);

    let mut reader = Reader::from_reader(reader);

    let mut count = 0;
    let mut txt = String::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event(&mut buf) {
            Ok(Event::Start(ref e)) => {
                match e.name() {
                    b"title" => txt.clear(),
                    b"text" => txt.clear(),
                    _ => (),
                }
            }
            Ok(Event::End(ref e)) => {
                match e.name() {
                    b"title" => println!("title: {}", txt),
                    b"text" => println!("text: {}", txt),
                    b"page" => {
                        count += 1
                    }
                    _ => (),
                }
            }
            Ok(Event::Text(e)) =>
                txt.push_str(
                    e
                        .unescape_and_decode(&reader)
                        .expect("fail to decode")
                        .as_str()),
            Ok(Event::Eof) => break, // exits the loop when reaching end of file
            Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
            _ => (), // There are several other `Event`s we do not consider here
        }

        // if we don't keep a borrow elsewhere, we can clear the buffer to keep memory usage low
        buf.clear();
    }
}
