extern crate rayon;
extern crate sled;
extern crate soup;

use std::collections::HashMap;
use std::convert::TryInto;
use std::io::Write;
use std::path::Path;
use std::thread;

use ir_playbook::ir;

fn main() {
    let data_file =
        std::env::var("HOME").unwrap() + "/enwiki-20210120-pages-articles-multistream.xml.bz2";
    let index_file = std::env::var("HOME").unwrap()
        + "/enwiki-20210120-pages-articles-multistream-index.txt.bz2";
    let out_file = std::env::var("HOME").unwrap() + "/wikipedia-freq.csv.bz2";
    let file_name = "/tmp/token-counter";

    let mut writer = ir::files::get_bz_writer(out_file).unwrap();

    let rx = ir::data_producer::get_wikipedia_producer(index_file, data_file, 16).unwrap();

    let sink = thread::spawn(move || {
        let tree = sled::Config::default()
            .path(file_name)
            .create_new(true)
            .cache_capacity(1024 * 1024)
            .flush_every_ms(Some(1000))
            .temporary(true)
            .open()
            .unwrap();

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

                let mut batch = sled::Batch::default();

                for e in map.iter() {
                    let count = match tree.get(e.0.as_str()) {
                        Ok(Some(count_bytes)) => {
                            let (int_bytes, _) = count_bytes.split_at(std::mem::size_of::<i64>());
                            i64::from_le_bytes(int_bytes.try_into().unwrap())
                        }
                        Ok(None) => 0,
                        Err(e) => panic!(e),
                    };
                    batch.insert(e.0.as_str(), sled::IVec::from(&(count + e.1).to_le_bytes()));
                }
                map.clear();
                tree.apply_batch(batch).unwrap();
            }
        }

        for e in tree.iter() {
            let (key, value) = e.unwrap();
            writeln!(writer,
                     "{},{}",
                     std::str::from_utf8(key.as_ref()).unwrap(),
                     i64::from_le_bytes(value.split_at(std::mem::size_of::<i64>()).0.try_into().unwrap())
            ).unwrap();
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