use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use bzip2::bufread::MultiBzDecoder;
use bzip2::write::BzEncoder;
use bzip2::Compression;

pub fn get_multi_bz_reader<P: AsRef<Path>>(path: P) -> io::Result<impl BufRead> {
    let reader = File::open(path);
    if reader.is_err() {
        return Err(reader.unwrap_err());
    }
    let reader = BufReader::new(reader.unwrap());
    let reader = MultiBzDecoder::new(reader);
    Ok(BufReader::new(reader))
}

pub fn get_bz_writer<P: AsRef<Path>>(path: P) -> io::Result<impl Write> {
    let writer = File::create(path);
    if writer.is_err() {
        return Err(writer.unwrap_err());
    }
    let writer = BufWriter::new(writer.unwrap());
    let writer = BzEncoder::new(writer, Compression::best());
    Ok(BufWriter::new(writer))
}
