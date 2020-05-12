//! # DB encoding
//! Every number will be store in db with little-endian ordering.
//! We will store records in db in a way that the latest (as in, its actual time) record will always be at the end of the file.
//! But we should do something that will periodicly check the sanity of the DB and fix mistakes (i.e, sort the whole DB).
//! This could be definitly be easier by holding the DB in memory and doing any I/O in memory before the DB is commited to the file.

extern crate chrono;

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::error::Error;
use std::fs::File;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::string::String;

#[derive(Debug, PartialEq)]
pub enum EnodError {
    IOError(String),
}

/// A way to store date and time in 56bits / 7 octets.
/// There is no awareness of timezone, everything is assumed to be Utc+0.
#[derive(Debug, Copy, Clone)]
pub struct Timestamp {
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
}

impl From<chrono::DateTime<Utc>> for Timestamp {
    fn from(d: chrono::DateTime<Utc>) -> Timestamp {
        Timestamp {
            year: d.year() as u16,
            month: d.month() as u8,
            day: d.day() as u8,
            hour: d.hour() as u8,
            minute: d.minute() as u8,
            second: d.second() as u8,
        }
    }
}

impl From<&[u8]> for Timestamp {
    fn from(d: &[u8]) -> Timestamp {
        let mut reader = Cursor::new(d);
        Timestamp {
            year: reader.read_u16::<LittleEndian>().unwrap(),
            month: reader.read_u8().unwrap(),
            day: reader.read_u8().unwrap(),
            hour: reader.read_u8().unwrap(),
            minute: reader.read_u8().unwrap(),
            second: reader.read_u8().unwrap(),
        }
    }
}

impl Timestamp {
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut store: Vec<u8> = Vec::with_capacity(7);
        store.write_u16::<LittleEndian>(self.year).unwrap();
        store.push(self.month);
        store.push(self.day);
        store.push(self.hour);
        store.push(self.minute);
        store.push(self.second);
        store
    }
}

/// Represent an entry in the database.
/// `time_offset` represent the number of seconds passed since the origin date of the DB.
/// It's a u32, which means you should be able to store record up to 136 years after the origin date of the DB.
#[derive(Debug, Copy, Clone)]
pub struct RecordInfo {
    time_offset: u32,
    value: u8,
}

impl From<&[u8]> for RecordInfo {
    fn from(d: &[u8]) -> RecordInfo {
        let mut reader = Cursor::new(d);
        RecordInfo {
            time_offset: reader.read_u32::<LittleEndian>().unwrap(),
            value: reader.read_u8().unwrap(),
        }
    }
}

impl RecordInfo {
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut store: Vec<u8> = Vec::with_capacity(4 + 1); // 4 time_offset, 1 value
        store.write_u32::<LittleEndian>(self.time_offset).unwrap();
        store.write_u8(self.value).unwrap();
        store
    }
}

/// The header of a DB file.
/// `origin_date` is the date that will be use has the origin. The DB *cannot* contain any record anterior to this date.
#[derive(Debug, Copy, Clone)]
pub struct DbHeader {
    origin_date: Timestamp,
    records_number: u64,
}

impl From<&[u8]> for DbHeader {
    fn from(d: &[u8]) -> DbHeader {
        let timestamp = Timestamp::from(d);
        let mut reader = Cursor::new(d);
        reader.set_position(7);
        DbHeader {
            origin_date: timestamp,
            records_number: reader.read_u64::<LittleEndian>().unwrap(),
        }
    }
}

impl DbHeader {
    pub fn as_bytes(&self) -> Vec<u8> {
        let mut store: Vec<u8> = Vec::with_capacity(7 + 8); // 7 for timestamp, 8 for record number.
        store.extend(self.origin_date.as_bytes());
        store
            .write_u64::<LittleEndian>(self.records_number)
            .unwrap();
        store
    }
}

/// a DB in file
#[derive(Debug)]
pub struct PhysicalDB {
    path: PathBuf,
    file: Option<File>,
    header: DbHeader,
}

impl PhysicalDB {
    /// This function will create a new database file.
    /// Warning: It will *not* check if there is already a file at `path`, if there is one, it will be overwritten.
    /// The second argument the date with which to initialize the database. It is optional, if you give `None`
    /// it will use the current date and time.
    pub fn create(
        path: &Path,
        origin_date: Option<chrono::DateTime<Utc>>,
    ) -> Result<PhysicalDB, EnodError> {
        let mut file =
            File::create(path).map_err(|e| EnodError::IOError(e.to_string().to_string()))?;

        // Store the origin date using or own time stamp format. See the Timestamp struct for more info.
        // It lose every timezone info, so everything is normalized as utc+0 before being written.
        let date = Timestamp::from(origin_date.unwrap_or(Utc::now()));
        // We always start with an empty DB, so we store 0 for the number of records.
        let header = DbHeader {
            origin_date: date,
            records_number: 0,
        };

        file.write(&header.as_bytes())
            .map_err(|e| EnodError::IOError(e.to_string().to_string()))?;

        Ok(PhysicalDB {
            path: PathBuf::from(path),
            file: None, // don't want to open the file right away.
            header,
        })
    }

    pub fn open(&mut self) -> Result<(), EnodError> {
        if self.file.is_some() {
            return Ok(());
        }

        self.file = Some(
            File::open(&self.path).map_err(|e| EnodError::IOError(e.to_string().to_string()))?,
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::prelude::*;
    use std::error::Error;
    use std::fs;
    use std::io::prelude::*;
    use std::path::Path;

    #[test]
    fn create_db_origin_now() {
        fs::remove_file("create_db_origin_now.db");
        let r = PhysicalDB::create(&Path::new("create_db_origin_now.db"), None);
        assert!(r.is_ok());
        fs::remove_file("create_db_origin_now.db");
    }

    #[test]
    fn create_db_origin_specific() {
        fs::remove_file("create_db_origin_specific.db");

        let origin_date = Utc.ymd(1994, 07, 08).and_hms(6, 55, 34);
        let wr = PhysicalDB::create(
            &Path::new("create_db_origin_specific.db"),
            Some(origin_date),
        );
        assert!(wr.is_ok());

        let mut f = File::open("create_db_origin_specific.db").unwrap();
        let mut buf: Vec<u8> = Vec::with_capacity(7 + 8);
        let rr = f.read_to_end(&mut buf).map_err(|e| e.to_string());
        assert!(rr.is_ok());
        assert!(rr.map(|v| v == (7 + 8)).unwrap_or(false));

        let dbHeader = DbHeader::from(buf.as_slice());
        assert_eq!(dbHeader.records_number, 0);
        assert_eq!(dbHeader.origin_date.year, 1994);
        assert_eq!(dbHeader.origin_date.month, 07);
        assert_eq!(dbHeader.origin_date.day, 08);
        assert_eq!(dbHeader.origin_date.hour, 6);
        assert_eq!(dbHeader.origin_date.minute, 55);
        assert_eq!(dbHeader.origin_date.second, 34);

        fs::remove_file("create_db_origin_specific.db");
    }
}
