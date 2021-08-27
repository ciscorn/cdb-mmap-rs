use std::{convert::TryInto, fs::File, io};

use memmap2::{ Mmap, MmapOptions };

pub struct CDBReader {
    mmap: memmap2::Mmap,
    maintable: Vec<(u32, u32)>,
}

pub struct CDBValueIter<'a> {
    mmap: &'a Mmap,  // todo: more abstruction
    key: &'a [u8],
    hashed_key: u32,
    finished: bool,
    curr_pos: usize,
    pos_subtable: usize,
    begin: usize,
    end: usize,
}

impl<'a> CDBValueIter<'a> {
    fn new(reader: &'a CDBReader, key: &'a [u8]) -> Self {
        let hashed_key = hashfunc(key);
        let (pos_subtable, num_entries) = reader.maintable[(hashed_key % 256) as usize];
        let pos_subtable = pos_subtable as usize;
        assert!(pos_subtable >= 2048);

        let hashed_high = hashed_key / 256;
        let begin = pos_subtable + 8 * (hashed_high % num_entries) as usize;
        let end = pos_subtable + 8 * num_entries as usize;
        Self {
            mmap: &reader.mmap,
            key,
            hashed_key,
            finished: false,
            curr_pos: begin,
            pos_subtable,
            begin,
            end,
        }
    }
}

impl<'a> Iterator for CDBValueIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        loop {
            let (h, p) = to_u32pair(&self.mmap[self.curr_pos..(self.curr_pos + 8)]);
            if p == 0 {
                self.finished = true;
                return None;
            }
            if h == self.hashed_key {
                let p = p as usize;
                let (klen, vlen) = to_u32pair(&self.mmap[p..(p + 8)]);
                let pv = p + 8 + (klen as usize);
                if &self.mmap[(p + 8)..pv] == self.key {
                    let value = &self.mmap[pv..(pv + (vlen as usize))];
                    return Some(value);
                }
            }

            self.curr_pos += 8;
            if self.curr_pos == self.begin {
                self.finished = true; // mark finished
                return None
            }
            else if self.curr_pos == self.end {
                self.curr_pos = self.pos_subtable;
            }
        }
    }
}

fn hashfunc(s: &[u8]) -> u32 {
    s.iter().fold(5381, |a, c| a.wrapping_mul(33) ^ *c as u32)
}

fn to_u32pair(x: &[u8]) -> (u32, u32) {
    let (a, b) = x.split_at(4);
    (
        u32::from_le_bytes(a.try_into().unwrap()),
        u32::from_le_bytes(b.try_into().unwrap()),
    )
}

impl CDBReader {
    pub fn new(path: &str) -> io::Result<Self> {
        let mmap = {
            let file = File::open(path)?;
            unsafe { MmapOptions::new().map(&file)? }
        };
        if mmap.len() > 0xffffffff {
            return Err(io::Error::new(io::ErrorKind::Other, "Invalid CDB file"));
        }

        let maintable: Vec<(u32, u32)> = {
            let buf = &mmap[..2048];
            if buf.len() != 2048 {
                return Err(io::Error::new(io::ErrorKind::Other, "Invalid CDB file"));
            }
            buf.chunks_exact(8).map(to_u32pair).collect()
        };

        Ok(CDBReader { mmap, maintable })
    }

    pub fn get<'a>(&'a self, key: &'a [u8]) -> Option<&[u8]> {
        CDBValueIter::new(self, key).next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poor_test() {
        let a = CDBReader::new("./tests/word_variants.cdb").unwrap();
        assert_eq!(a.get(b"allotments").unwrap(), b"allotment");
        assert_eq!(a.get(b"went").unwrap(), b"go\0going\0goes\0gone");
    }
}
