use std::fs;
use std::io;

fn main() {
    println!("Hello, world!");
}

fn read_file(file_name: String) {
    let mut reader = BufReader::new(File::open(file_name));
}
