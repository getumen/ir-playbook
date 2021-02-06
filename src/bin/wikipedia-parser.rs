extern crate rayon;
extern crate soup;

use std::io::Write;
use std::thread;

use ir_playbook::ir;

fn main() {
    let data_file =
        std::env::var("HOME").unwrap() + "/enwiki-20210120-pages-articles-multistream.xml.bz2";
    let index_file = std::env::var("HOME").unwrap()
        + "/enwiki-20210120-pages-articles-multistream-index.txt.bz2";
    let out_file = std::env::var("HOME").unwrap() + "/wikipedia-trec.xml.bz2";

    let mut writer = ir::files::get_bz_writer(out_file).unwrap();

    let rx = ir::data_producer::get_wikipedia_producer(index_file, data_file, 16).unwrap();

    let sink = thread::spawn(move || {
        for v in rx {
            for wikipedia_page in v {
                writeln!(writer, "<DOC>").unwrap();
                writeln!(writer, "<DOCNO>{}</DOCNO>", wikipedia_page.id).unwrap();
                writeln!(writer, "<HEADLINE>{}</HEADLINE>", wikipedia_page.title).unwrap();
                writeln!(writer, "<P>{}</P>", wikipedia_page.content).unwrap();
                writeln!(writer, "</DOC>").unwrap();
            }
        }
    });

    sink.join().unwrap();
}
