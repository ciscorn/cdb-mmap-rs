use std::{convert::TryInto, fs::File, io};

use memmap2::MmapOptions;

pub struct CDBReader {
    mmap: memmap2::Mmap,
    maintable: Vec<(u32, u32)>,
}

pub struct ValueIter<'a> {
    CDBReader: &'a CDBReader,
}

impl<'a> Iterator for ValueIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
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
    pub fn new(path: &str) -> io::Result<CDBReader> {
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

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let hashed = hashfunc(key);
        let hash_pos_iter = {
            let (pos_subtable, num_entries) = self.maintable[(hashed % 256) as usize];
            let pos_subtable = pos_subtable as usize;
            assert!(pos_subtable >= 2048);

            let hashed_high = hashed / 256;
            let begin = pos_subtable + 8 * (hashed_high % num_entries) as usize;
            let end = pos_subtable + 8 * num_entries as usize;
            let iter1 = self.mmap[begin..(end + 8)].chunks_exact(8).map(to_u32pair);
            let iter2 = self.mmap[pos_subtable..(begin + 8)]
                .chunks_exact(8)
                .map(to_u32pair);
            iter1.chain(iter2)
        };

        for (h, p) in hash_pos_iter.into_iter() {
            if p == 0 {
                return None;
            }
            if h == hashed {
                let p = p as usize;
                let (klen, vlen) = to_u32pair(&self.mmap[p..(p + 8)]);
                let (klen, vlen) = (klen as usize, vlen as usize);
                let pv = p + 8 + klen;
                if &self.mmap[(p + 8)..pv] == key {
                    let value = &self.mmap[pv..(pv + vlen)];
                    return Some(value);
                }
            }
        }
        return None;
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
