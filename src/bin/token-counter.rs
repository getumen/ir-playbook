extern crate rayon;
extern crate regex;
extern crate soup;

use std::collections::HashMap;
use std::io::Write;
use std::thread;

use ir_playbook::ir;

fn main() {
    let data_file =
        std::env::var("HOME").unwrap() + "/enwiki-20210120-pages-articles-multistream.xml.bz2";
    let index_file = std::env::var("HOME").unwrap()
        + "/enwiki-20210120-pages-articles-multistream-index.txt.bz2";
    let out_file = std::env::var("HOME").unwrap() + "/wikipedia-freq.csv.bz2";

    let mut writer = ir::files::get_bz_writer(out_file).unwrap();

    let rx = ir::data_producer::get_wikipedia_producer(index_file, data_file, 16).unwrap();

    let sink = thread::spawn(move || {
        let mut map: HashMap<String, i64> = HashMap::new();

        for v in rx {
            for wikipedia_page in v {
                for t in tokenizer(wikipedia_page.title) {
                    let count = match map.get(&t) {
                        Some(c) => *c,
                        None => 0,
                    };
                    map.insert(t, count + 1);
                }
                for t in tokenizer(wikipedia_page.content) {
                    let count = match map.get(&t) {
                        Some(c) => *c,
                        None => 0,
                    };
                    map.insert(t, count + 1);
                }
            }
        }
        for e in map.iter() {
            writeln!(writer, "{},{}", e.0, e.1).unwrap();
        }
    });

    sink.join().unwrap();
}

fn tokenizer(text: String) -> Vec<String> {
    let text: String = text
        .chars()
        .filter(|c| c.is_ascii())
        .collect();
    let mut result = Vec::new();
    for t in text.split_whitespace() {
        result.push(t.to_lowercase().to_string());
    }
    result
}