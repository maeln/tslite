//! A very simple embedded time-serie database.
//!
//! Right now you can only store data that fit in one octet.
//!
//! All the operation are made directly on the DB file, so this can get very I/O intensive if you do a lot of operation.
//! If you are going to push data and read data a lot, you really shouldn't use it directly.
//!
//! If you intend to do a lot of operation you should have an layer that will operate in-memory and periodacally
//! dump them to the filesystem.
//!
//! # DB encoding
//!
//! Every number will be store in db with little-endian ordering.
//! We will store records in db in a way that the latest (as in, its actual time) record will always be at the end of the file.
//! But we should do something that will periodicly check the sanity of the DB and fix mistakes (i.e, sort the whole DB).
//! This could be definitly be easier by holding the DB in memory and doing any I/O in memory before the DB is commited to the file.
//!
//!
//! # File orga
//!
//! ```text
//! +--------------------------------------------+
//! | HEADER | RECORD1 | RECORD2 | RECORD3 | ... |
//! +--------------------------------------------+
//! ```
//!
//! ```text
//! +-------------------------------------------[HEADER]---------------------------------------------+
//! |--------------------------[TIMESTAMP]------------------------|---------[RECORD COUNT]-----------|
//! |      year      |  month |  day   |  hour  | minute | second |              64bit               |
//! |     16bit      |  8bit  |  8bit  |  8bit  |  8bit  |  8bit  |                                  |
//! +------------------------------------------------------------------------------------------------+
//! ```
//!
//! ```text
//! +-------------------[RECORD]------------+
//! |--------[TIME OFFSET]--------|-[VALUE]-|
//! |            32bit            |   8bit  |
//! +---------------------------------------+
//! ```

extern crate chrono;

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::string::String;

use std::cmp::{Ord, Ordering};

#[derive(Debug, PartialEq)]
pub enum EnodError {
    IOError(String),
}

/// A way to store date and time in 56bits / 7 octets.
/// There is no awareness of timezone, everything is assumed to be Utc+0.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
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

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Timestamp) -> Option<Ordering> {
        Some(
            self.year
                .cmp(&other.year)
                .then(self.month.cmp(&other.month))
                .then(self.day.cmp(&other.day))
                .then(self.hour.cmp(&other.hour))
                .then(self.minute.cmp(&other.minute))
                .then(self.second.cmp(&other.second)),
        )
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.year
            .cmp(&other.year)
            .then(self.month.cmp(&other.month))
            .then(self.day.cmp(&other.day))
            .then(self.hour.cmp(&other.hour))
            .then(self.minute.cmp(&other.minute))
            .then(self.second.cmp(&other.second))
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

    /// Check if a date is valid.
    pub fn is_valid(&self) -> bool {
        let mut valid = true;
        valid &= 1 <= self.month && self.month <= 12;
        valid &= 1 <= self.day;
        valid &= self.hour < 24;
        valid &= self.minute < 60;
        valid &= self.second < 60;

        // As usual, we have to handle febuary as an edge-case.
        if self.month == 2 {
            // We check if this year is a leap year
            let factor = |x| self.year % x == 0;
            let leap = factor(4) && (!factor(100) || factor(400));
            if leap {
                valid &= self.day <= 29;
            } else {
                valid &= self.day <= 28;
            }
        } else {
            valid &=
                (self.month % 2 == 0 && self.day <= 30) || (self.month % 2 == 1 && self.day <= 31);
        }

        valid
    }
}

/// Represent an entry in the database.
/// `time_offset` represent the number of seconds passed since the origin date of the DB.
/// It's a u32, which means you should be able to store record up to 136 years after the origin date of the DB.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
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

impl PartialOrd for RecordInfo {
    fn partial_cmp(&self, other: &RecordInfo) -> Option<Ordering> {
        Some(self.time_offset.cmp(&other.time_offset))
    }
}

impl Ord for RecordInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time_offset.cmp(&other.time_offset)
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

/// Potential Issue in the DB file
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum DbIssue {
    /// If a record is not properly chonologicaly ordered.
    UnorderedRecord,
    /// If the header is corrupted (cannot be fully read or data are wrong).
    HeaderCorrupted,
    /// If the date of the DB is invalid.
    OriginDateInvalid,
    /// If a record is corrupted (cannot be fully read or data are wrong) with its index.
    RecordCorrupted(u64),
    /// If the number of record in the header doesn't match the amount that can be read from the physical file.
    MismatchRecordAmount,
    /// Indicate that there is no known issue
    None,
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

    /// Open the database file in read and write mode.
    pub fn open(&mut self) -> Result<(), EnodError> {
        if self.file.is_some() {
            return Ok(());
        }

        self.file = Some(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.path)
                .map_err(|e| EnodError::IOError(e.to_string().to_string()))?,
        );
        Ok(())
    }

    /// Drop the database file to close it.
    /// Make sure to sync all IO operation before closing it.
    pub fn close(&mut self) -> Result<(), EnodError> {
        if self.file.is_some() {
            self.file
                .as_ref()
                .unwrap()
                .sync_all()
                .map_err(|e| EnodError::IOError(e.to_string().to_string()))?;
            self.file = None; // Files are close when dropped/out of scope.
        }

        Ok(())
    }

    /// Read the header from the file.
    /// Does not update the header in memory.
    pub fn read_header(&mut self) -> Result<DbHeader, EnodError> {
        if self.file.is_none() {
            self.open()?;
        }

        let mut fref = self.file.as_ref().unwrap();
        fref.seek(SeekFrom::Start(0))
            .map_err(|e| EnodError::IOError(e.to_string().to_string()))?;
        let mut buffer = [0; 15]; // Header takes 15 bytes.
        let n = fref
            .read(&mut buffer[..])
            .map_err(|e| EnodError::IOError(e.to_string().to_string()))?;
        if n == 15 {
            let header: DbHeader = DbHeader::from(&buffer[..]);
            return Ok(header);
        }

        Err(EnodError::IOError(
            "Could not read header: not enough octets.".to_string(),
        ))
    }

    /// The size of the header and record are static.
    /// So the position of each record is deterministic.
    /// If `n` is the record id, then its position within the file can be computed with :
    /// pos(n) = (7 + 8) + (5*n)
    pub fn read_record(&mut self, rec_id: u64) -> Result<RecordInfo, EnodError> {
        if self.file.is_none() {
            self.open()?;
        }

        let pos = (7 + 8) + (rec_id * 5);
        let mut fref = self.file.as_ref().unwrap();
        fref.seek(SeekFrom::Start(pos))
            .map_err(|e| EnodError::IOError(e.to_string().to_string()))?;
        let mut buffer = [0; 5]; // Header takes 15 bytes.
        let n = fref
            .read(&mut buffer[..])
            .map_err(|e| EnodError::IOError(e.to_string().to_string()))?;
        if n == 5 {
            let record: RecordInfo = RecordInfo::from(&buffer[..]);
            return Ok(record);
        }

        Err(EnodError::IOError(
            "Could not read record: not enough octets.".to_string(),
        ))
    }

    /// This utility function will update the number of record in the database.
    pub fn update_record_number(&mut self, drn: u64) -> Result<(), EnodError> {
        if self.file.is_none() {
            self.open()?;
        }

        let mut fref = self.file.as_ref().unwrap();
        fref.seek(SeekFrom::Start(7)) // The record number is always at position 7
            .map_err(|e| EnodError::IOError(e.to_string().to_string()))?;
        fref.write_u64::<LittleEndian>(self.header.records_number + drn)
            .map_err(|e| EnodError::IOError(e.to_string().to_string()))?;
        self.header.records_number += drn;

        Ok(())
    }

    /// Add a record in the database.
    pub fn append_record(&mut self, rec_nfo: RecordInfo) -> Result<(), EnodError> {
        if self.file.is_some() {
            self.open()?;
        }

        // write record
        let mut fref = self.file.as_ref().unwrap();
        fref.seek(SeekFrom::End(0))
            .map_err(|e| EnodError::IOError(e.to_string().to_string()))?;
        fref.write(&rec_nfo.as_bytes())
            .map_err(|e| EnodError::IOError(e.to_string().to_string()))?;
        fref.sync_all()
            .map_err(|e| EnodError::IOError(e.to_string().to_string()))?;

        // Update DbHeader
        self.update_record_number(1)?;

        Ok(())
    }

    /// Perform check to find any issue in the database file.
    /// It will return the first issue it find. You might need to run this function
    /// until it return `DbIssue::None` to check for all possible issue.
    pub fn check_db_file(&mut self) -> Result<DbIssue, EnodError> {
        if self.file.is_some() {
            self.open()?;
        }

        // First try to read the header
        let res_header = self.read_header();
        if res_header.is_err() {
            return Ok(DbIssue::HeaderCorrupted);
        }
        let header = res_header.unwrap();
        if !header.origin_date.is_valid() {
            return Ok(DbIssue::OriginDateInvalid);
        }

        let mut time_offset = 0;
        for i in 0..header.records_number {
            let res_record = self.read_record(i);
            if res_record.is_err() {
                return Ok(DbIssue::RecordCorrupted(i));
            }
            if time_offset > res_record.as_ref().unwrap().time_offset {
                return Ok(DbIssue::UnorderedRecord);
            }
            time_offset = res_record.as_ref().unwrap().time_offset;
        }

        let metadata = self
            .file
            .as_ref()
            .unwrap()
            .metadata()
            .map_err(|e| EnodError::IOError(e.to_string().to_string()))?;
        if metadata.len() > (/* header size */120 + /* records size */40 * header.records_number) {
            return Ok(DbIssue::MismatchRecordAmount);
        }

        Ok(DbIssue::None)
    }

    /// Reorder the record in the DB.
    /// Use if your DB records got scrambled for some reason.
    /// Right now it use a simple way :
    /// - Read all the record
    /// - reorder them in-memory
    /// - dump *all* the record in the DB
    /// It means that if you have just one record wrong you end up re-writing the whole DB.
    fn reorder_record(&mut self) -> Result<(), EnodError> {
        if self.file.is_some() {
            self.open()?;
        }

        let mut records: Vec<RecordInfo> = Vec::with_capacity(self.header.records_number as usize);
        for i in 0..(self.header.records_number) {
            records.push(self.read_record(i)?);
        }
        records.sort_unstable();
        let mut fref = self.file.as_ref().unwrap();
        fref.seek(SeekFrom::Start(/* offset header */ 15))
            .map_err(|e| EnodError::IOError(e.to_string().to_string()))?;
        for r in &records {
            fref.write(&r.as_bytes())
                .map_err(|e| EnodError::IOError(e.to_string().to_string()))?;
        }
        fref.sync_all()
            .map_err(|e| EnodError::IOError(e.to_string().to_string()))?;

        Ok(())
    }
}

/// Maybe I can use a in-memory FS for the test instead of dumping files
/// on disk ?
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

    #[test]
    fn append_record() {
        let path = "append_record.db";

        fs::remove_file(path);

        let mut db = PhysicalDB::create(&Path::new(path), None).expect("could not create db.");
        let header = db.read_header().expect("could not read header.");
        assert_eq!(header.records_number, 0);

        let origin_record = RecordInfo {
            time_offset: 5,
            value: 10,
        };

        db.append_record(origin_record)
            .expect("could not append record.");

        let fs_record = db.read_record(0).expect("could not get record.");
        assert_eq!(origin_record, fs_record);

        let header = db.read_header().expect("could not read header.");
        assert_eq!(header.records_number, 1);

        fs::remove_file(path);
    }

    #[test]
    fn today_is_valid() {
        let today = Timestamp::from(Utc::now());
        assert_eq!(today.is_valid(), true);
    }

    #[test]
    fn date_ord() {
        let d1 = Timestamp {
            year: 1994,
            month: 7,
            day: 8,
            hour: 5,
            minute: 24,
            second: 23,
        };
        let d2 = Timestamp {
            year: 1993,
            month: 6,
            day: 18,
            hour: 8,
            minute: 0,
            second: 1,
        };

        assert_eq!(d1 > d2, true);
        assert_eq!(d1 < d2, false);
        assert_eq!(d1 == d2, false);
    }

    #[test]
    fn check_healthy_db() {
        let path = "healthy.db";

        fs::remove_file(path);

        let mut db = PhysicalDB::create(&Path::new(path), None).expect("could not create db.");
        let header = db.read_header().expect("could not read header.");

        // Add 10 record in the DB
        for i in 0..10 {
            let origin_record = RecordInfo {
                time_offset: 5 + i,
                value: i as u8,
            };
            db.append_record(origin_record)
                .expect("could not append record.");
        }

        let err = db.check_db_file().expect("could not check db file.");
        assert_eq!(err, DbIssue::None);

        fs::remove_file(path);
    }

    #[test]
    fn check_unordered_db() {
        let path = "unordered.db";

        fs::remove_file(path);

        let mut db = PhysicalDB::create(&Path::new(path), None).expect("could not create db.");
        let header = db.read_header().expect("could not read header.");

        // Add 10 record in the DB
        for i in 0..10 {
            let origin_record = RecordInfo {
                time_offset: 9 - i,
                value: i as u8,
            };
            db.append_record(origin_record)
                .expect("could not append record.");
        }

        let err = db.check_db_file().expect("could not check db file.");
        assert_eq!(err, DbIssue::UnorderedRecord);

        fs::remove_file(path);
    }

    #[test]
    fn reorder_db() {
        let path = "reordered.db";

        fs::remove_file(path);

        let mut db = PhysicalDB::create(&Path::new(path), None).expect("could not create db.");
        let header = db.read_header().expect("could not read header.");

        // Add 10 record in the DB in reverse order
        for i in 0..10 {
            let origin_record = RecordInfo {
                time_offset: 9 - i,
                value: i as u8,
            };
            db.append_record(origin_record)
                .expect("could not append record.");
        }

        let err = db.check_db_file().expect("could not check db file.");
        assert_eq!(err, DbIssue::UnorderedRecord);

        let res = db.reorder_record();
        assert_eq!(res.is_ok(), true);

        let err = db.check_db_file().expect("could not check db file.");
        assert_eq!(err, DbIssue::None);

        fs::remove_file(path);
    }
}
