extern crate xml;

use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::io::Write;

use bzip2::Compression;
use bzip2::read::MultiBzDecoder;
use bzip2::write::BzEncoder;
use xml::EventReader;
use xml::reader::XmlEvent;

fn main() {
    let in_name = std::env::var("HOME").unwrap() + "/enwiki-20210120-pages-articles-multistream.xml.bz2";
    let out_file = std::env::var("HOME").unwrap() + "/wikipedia-trec.xml.bz2";

    let reader = File::open(in_name).expect("fail to open file");
    let reader = BufReader::new(reader);
    let reader = MultiBzDecoder::new(reader);
    let reader = BufReader::new(reader);
    let reader = EventReader::new(reader);

    let writer = File::create(out_file).expect("fail to open file");
    let writer = BufWriter::new(writer);
    let writer = BzEncoder::new(writer, Compression::best());
    let mut writer = BufWriter::new(writer);

    let mut count = 0;
    let mut txt = String::new();
    let mut in_revision = false;

    let mut id = String::new();
    let mut title = String::new();
    let mut text = String::new();

    for e in reader {
        match e {
            Ok(XmlEvent::StartElement { name, .. }) => {
                match name.local_name.as_str() {
                    "id" => {
                        txt.clear();
                    }
                    "title" => {
                        txt.clear();
                    }
                    "text" => {
                        txt.clear();
                    }
                    "revision" => {
                        in_revision = true;
                    }
                    _ => {}
                }
            }
            Ok(XmlEvent::EndElement { name }) => {
                match name.local_name.as_str() {
                    "id" => {
                        if !in_revision {
                            id = txt.clone();
                        }
                    }
                    "title" => {
                        title = txt.clone();
                    }
                    "text" => {
                        text = txt.clone();
                    }
                    "page" => {
                        writeln!(writer, "<DOC>").unwrap();

                        writeln!(writer, "<DOCNO>{}</DOCNO>", id).unwrap();
                        writeln!(writer, "<HEADLINE>{}</HEADLINE>", title).unwrap();
                        writeln!(writer, "<P>{}</P>", text).unwrap();

                        writeln!(writer, "</DOC>").unwrap();
                        count += 1
                    }
                    "revision" => {
                        in_revision = false;
                    }
                    _ => {}
                }
            }
            Ok(XmlEvent::Characters(s)) =>
                txt.push_str(s.as_str()),
            Err(e) => {
                println!("Error: {}", e);
                break;
            }
            _ => {}
        }
    }

    println!("finish write {} pages", count);
}
