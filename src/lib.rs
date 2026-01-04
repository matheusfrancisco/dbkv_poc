#[macro_use]
extern crate serde_derive;

extern crate byteorder;
extern crate crc;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::Path;

//We’ll use the ByteStr type alias for data that tends to be used
//as a string but happens to be in a binary (raw bytes) form.
//Its text-based peer is the built-in str. Unlike str, ByteStr is
//not guaranteed to contain valid UTF-8 text.
//Both str and [u8] (or its alias ByteStr) are seen in the wild
//as &str and &[u8] (or &ByteStr). These are both called slices.
type ByteString = Vec<u8>;
type ByteStr = [u8];

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValeuPair {
    pub key: ByteString,
    pub value: ByteString,
}
#[derive(Debug)]
pub struct DBKV {
    f: File,
    pub index: HashMap<ByteString, u64>, // key to offset in file
}

// the kv on-disk representationl. It is an implementation of a Bitcask storage
// bitcask was developed an implemented by Basho Technologies for use with the Riak
//the file format is Log-Structured Hash Table
//
// Riak is a NoSQL
// Although it was slower than its peers, it guaranteed that it never lost data.
// That guarantee was enabled in part because of its smart choice of a data format.

// Record layout:
// -----------------fixed-with header------------------
// +----------------+----------------+----------------+----------------+----------------+----------------+
// | Checksum CRC   | Key Len        | Valeu Len      | Key (variable) | Value (variable)|
// +----------------+----------------+----------------+----------------+----------------+----------------+
// | 4 bytes        | 4 bytes        | N bytes       | 4 bytes        |  M bytes         |
// +----------------+----------------+----------------+----------------+----------------+----------------+
// |  u32          |   u32          |   u32          | [u8; key_len] | [u8; value_len] |
// every key-valeu pair is prefixed by 12 bytes(headers)
// key_len + val_len and its contenct checksum (crc32)

impl DBKV {
    pub fn open(path: &std::path::Path) -> std::io::Result<Self> {
        // we are using append_only so delete and update will be a variante of insert
        let f = File::options()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;
        let index = HashMap::new();

        Ok(DBKV { f, index })
    }

    /// Assumes that f is already at the right place in the file
    fn process_record<R: Read>(f: &mut R) -> io::Result<KeyValeuPair> {
        let checksum = f.read_u32::<LittleEndian>()?;
        let key_len = f.read_u32::<LittleEndian>()?;
        let value_len = f.read_u32::<LittleEndian>()?;
        let data_len = key_len + value_len;

        let mut data = ByteString::with_capacity(data_len as usize);
        {
            f.by_ref().take(data_len as u64).read_to_end(&mut data)?;
        }
        debug_assert_eq!(data.len() as u32, data_len);
        let checksum_calculated = crc32::checksum_ieee(&data);
        if checksum != checksum_calculated {
            panic!(
                "data corruption checksum mismatch: expected {:08x}, got {:08x}",
                checksum, checksum_calculated
            );
        }
        let valeu = data.split_off(key_len as usize);
        let key = data;
        Ok(KeyValeuPair { key, value: valeu })
    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        let postion = self.insert_but_ignore_index(key, value)?;
        self.index.insert(key.to_vec(), postion);
        Ok(())
    }

    pub fn insert_but_ignore_index(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<u64> {
        let mut f = BufWriter::new(&mut self.f);
        let key_len = key.len() as u32;
        let value_len = value.len() as u32;

        let mut tmp = ByteString::with_capacity((key_len + value_len) as usize);
        for byte in key {
            tmp.push(*byte);
        }

        for byte in value {
            tmp.push(*byte);
        }
        //alternative to for: tmp.extend_from_slice(key);
        //alternative to for: tmp.extend_from_slice(value);
        let checksum = crc32::checksum_ieee(&tmp);

        let next_byte = SeekFrom::End(0);
        let current_position = f.seek(SeekFrom::Current(0))?;
        f.seek(next_byte)?;
        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u32::<LittleEndian>(key_len)?;
        f.write_u32::<LittleEndian>(value_len)?;
        f.write_all(&tmp)?;
        Ok(current_position)
    }

    #[inline]
    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        self.insert(key, value)
    }

    #[inline]
    pub fn delete(&mut self, key: &ByteStr) -> io::Result<()> {
        self.insert(key, b"")
    }

    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<KeyValeuPair>> {
        match self.index.get(key) {
            Some(&position) => {
                let kv = self.get_at(position)?;
                Ok(Some(kv))
            }
            None => Ok(None),
        }
    }
    pub fn get_at(&mut self, position: u64) -> io::Result<KeyValeuPair> {
        let mut f = BufReader::new(&mut self.f);
        f.seek(SeekFrom::Start(position))?;
        let kv = DBKV::process_record(&mut f)?;
        Ok(kv)
    }

    pub fn find(&mut self, target: &ByteStr) -> io::Result<Option<(u64, ByteString)>> {
        let mut f = BufReader::new(&mut self.f);
        let mut found: Option<(u64, ByteString)> = None;

        loop {
            let pos = f.seek(SeekFrom::Current(0))?;
            // read a record in the file at its current position
            let maybe_kv = DBKV::process_record(&mut f);
            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(e) => {
                    match e.kind() {
                        io::ErrorKind::UnexpectedEof => break, // end of file reached
                        _ => return Err(e),                    // propagate other errors
                    }
                }
            };

            if kv.key == target {
                found = Some((pos, kv.value));
                break;
            }
            // keep looping until the end of the file
            // in case the key has been overwritten
        }
        Ok(found)
    }

    pub fn load(&mut self) -> io::Result<()> {
        let mut f = BufReader::new(&mut self.f);

        loop {
            let pos = f.seek(SeekFrom::Current(0))?;
            // read a record in the file at its current position
            let maybe_kv = DBKV::process_record(&mut f);
            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(e) => {
                    match e.kind() {
                        io::ErrorKind::UnexpectedEof => break, // end of file reached
                        _ => return Err(e),                    // propagate other errors
                    }
                }
            };
            self.index.insert(kv.key, pos);
        }
        Ok(())
    }
}
