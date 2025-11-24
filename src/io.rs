use crate::memory::page::Page;
use crate::memory::PAGE_SIZE;
use crate::Result;
use std::fs::OpenOptions;
use std::fs::{File};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::{io, slice};

pub mod lbaio;

/// Abstraction over page IO operations. Allows swapping sync file IO, libaio, or exmap-backed paths.
pub trait PageIo: Send + Sync {
    fn read_page(&self, pid: u64, dst: *mut Page) -> Result<()>;
    fn write_page(&self, pid: u64, src: *const Page) -> Result<()>;

    /// Optional batch write; default falls back to per-page writes.
    fn write_pages(&self, pids: &[u64], base: *const Page) -> Result<()> {
        for &pid in pids {
            let ptr = unsafe { base.add(pid as usize) };
            self.write_page(pid, ptr)?;
        }
        Ok(())
    }

    /// Write from an arbitrary buffer; default forwards to `write_page` if sized correctly.
    fn write_buf(&self, pid: u64, buf: &[u8]) -> Result<()> {
        if buf.len() != PAGE_SIZE {
            return Err("write_buf expects PAGE_SIZE bytes".into());
        }
        let ptr = buf.as_ptr() as *const Page;
        self.write_page(pid, ptr)
    }

    /// Optional async batch write with owned buffers; returns a receiver signaled on completion.
    /// Default: None (not supported).
    fn write_pages_async(
        &self,
        _bufs: Vec<(u64, Vec<u8>)>,
    ) -> Option<std::sync::mpsc::Receiver<Result<()>>> {
        None
    }

    /// Optional per-queue IO stats snapshot.
    fn queue_stats(&self) -> Option<IoStatsSnapshot> {
        None
    }
}

#[derive(Default, Clone, Debug)]
pub struct IoQueueSnapshot {
    pub submit: u64,
    pub fail: u64,
    pub timeout: u64,
    pub retries: u64,
}

#[derive(Default, Clone, Debug)]
pub struct IoStatsSnapshot {
    pub queues: Vec<IoQueueSnapshot>,
}

/// Synchronous file-based IO implementation using pread/pwrite.
pub struct SyncFileIo {
    file: File,
}

impl SyncFileIo {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Arc<Self>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(Arc::new(Self { file }))
    }

    pub fn open_with_len<P: AsRef<Path>>(path: P, len: u64) -> Result<Arc<Self>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.set_len(len)?;
        Ok(Arc::new(Self { file }))
    }
}

impl PageIo for SyncFileIo {
    fn read_page(&self, pid: u64, dst: *mut Page) -> Result<()> {
        let offset = pid * PAGE_SIZE as u64;
        let buf = unsafe { slice::from_raw_parts_mut(dst as *mut u8, PAGE_SIZE) };
        let mut done = 0;
        while done < buf.len() {
            let n = self.file.read_at(&mut buf[done..], offset + done as u64)?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read").into());
            }
            done += n;
        }
        Ok(())
    }

    fn write_page(&self, pid: u64, src: *const Page) -> Result<()> {
        let offset = pid * PAGE_SIZE as u64;
        let buf = unsafe { slice::from_raw_parts(src as *const u8, PAGE_SIZE) };
        let mut done = 0;
        while done < buf.len() {
            let n = self.file.write_at(&buf[done..], offset + done as u64)?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "short write").into());
            }
            done += n;
        }
        Ok(())
    }
}
