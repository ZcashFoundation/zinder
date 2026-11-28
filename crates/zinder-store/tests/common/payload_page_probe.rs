use std::{fs::File, io, ops::Range, path::Path};

use rustix::{
    fs::{Advice, fadvise},
    io::{Errno, ReadWriteFlags, preadv2},
};

pub(super) struct PayloadPageProbe {
    file: File,
    pub(super) page_size: usize,
    pub(super) interior: Range<usize>,
}

impl PayloadPageProbe {
    pub(super) fn open(path: &Path, header_len: usize, index_offset: u64) -> io::Result<Self> {
        let file = File::open(path)?;
        fadvise(&file, 0, None, Advice::Random)?;
        let page_size = rustix::param::page_size();
        let end = usize::try_from(index_offset).map_err(io::Error::other)?;
        let interior = header_len.next_multiple_of(page_size)..(end - end % page_size);
        if interior.is_empty() {
            return Err(io::Error::other("fixture has no interior payload pages"));
        }
        Ok(Self {
            file,
            page_size,
            interior,
        })
    }

    pub(super) fn evict(&self) -> io::Result<()> {
        self.file.sync_all()?;
        Ok(fadvise(&self.file, 0, None, Advice::DontNeed)?)
    }

    pub(super) fn sample(&self) -> io::Result<Option<PageResidency>> {
        let mut resident = 0;
        let mut byte = [0];
        for offset in self.interior.clone().step_by(self.page_size) {
            let mut buffers = [io::IoSliceMut::new(&mut byte)];
            let offset = u64::try_from(offset).map_err(io::Error::other)?;
            match preadv2(&self.file, &mut buffers, offset, ReadWriteFlags::NOWAIT) {
                Ok(n) if n > 0 => resident += 1,
                Ok(_) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
                Err(Errno::AGAIN) => (),
                Err(Errno::OPNOTSUPP | Errno::NOSYS) => return Ok(None),
                Err(error) => return Err(error.into()),
            }
        }
        Ok(Some(PageResidency {
            resident,
            total: self.interior.len() / self.page_size,
        }))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct PageResidency {
    pub(super) resident: usize,
    pub(super) total: usize,
}

impl PageResidency {
    pub(super) const fn confirms_eviction(self) -> bool {
        self.resident * 10 <= self.total
    }

    pub(super) const fn mostly_resident(self) -> bool {
        self.resident * 10 >= self.total * 9
    }
}
