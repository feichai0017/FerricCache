#[cfg(all(target_os = "linux", feature = "libaio"))]
mod lbaio_impl {
    use crate::memory::{page::Page, PAGE_SIZE};
    use crate::Result;
    use libc::c_void;
    use std::io;
    use std::fs::File;
    use std::os::fd::AsRawFd;
    use std::slice;
    use std::sync::Arc;
    use std::ptr;

    /// Minimal `io_event` mirror (not provided by libc).
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct IoEvent {
        data: u64,
        obj: u64,
        res: i64,
        res2: i64,
    }

    /// Kernel AIO context type.
    type AioContext = u64;

    const IOCB_CMD_PWRITE: u16 = 1;

    #[inline]
    fn io_setup(max_events: usize, ctx: &mut AioContext) -> Result<()> {
        let ret = unsafe { libc::syscall(libc::SYS_io_setup, max_events as libc::c_ulong, ctx) };
        if ret < 0 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(())
    }

    #[inline]
    fn io_destroy(ctx: AioContext) {
        let ret = unsafe { libc::syscall(libc::SYS_io_destroy, ctx) };
        if ret < 0 {
            eprintln!("io_destroy failed: {}", io::Error::last_os_error());
        }
    }

    #[inline]
    fn io_submit(ctx: AioContext, cbs: &mut [*mut libc::iocb]) -> Result<usize> {
        if cbs.is_empty() {
            return Ok(0);
        }
        let ret = unsafe { libc::syscall(libc::SYS_io_submit, ctx, cbs.len() as libc::c_long, cbs.as_mut_ptr()) };
        if ret < 0 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(ret as usize)
    }

    #[inline]
    fn io_getevents(ctx: AioContext, min_nr: usize, nr: usize, events: &mut [IoEvent]) -> Result<usize> {
        debug_assert!(nr <= events.len());
        let ret = unsafe {
            libc::syscall(
                libc::SYS_io_getevents,
                ctx,
                min_nr as libc::c_long,
                nr as libc::c_long,
                events.as_mut_ptr(),
                ptr::null_mut::<libc::timespec>(),
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(ret as usize)
    }

    pub struct LibaioIo {
        file: File,
        ctx: AioContext,
        _max_ios: usize,
    }

    unsafe impl Send for LibaioIo {}
    unsafe impl Sync for LibaioIo {}

    impl Drop for LibaioIo {
        fn drop(&mut self) {
            io_destroy(self.ctx);
        }
    }

    impl LibaioIo {
        pub fn open(file: File, max_ios: usize) -> Result<Arc<Self>> {
            let mut ctx: AioContext = 0;
            io_setup(max_ios, &mut ctx)?;
            Ok(Arc::new(Self { file, ctx, _max_ios: max_ios }))
        }
    }

    impl crate::io::PageIo for LibaioIo {
        fn read_page(&self, pid: u64, dst: *mut Page) -> Result<()> {
            // fallback to pread for reads in this simple implementation
            let fd = self.file.as_raw_fd();
            let buf = unsafe { slice::from_raw_parts_mut(dst as *mut u8, PAGE_SIZE) };
            let mut done = 0;
            while done < buf.len() {
                let n = unsafe { libc::pread(fd, buf.as_mut_ptr().add(done) as *mut c_void, (buf.len() - done) as usize, (pid * PAGE_SIZE as u64 + done as u64) as i64) };
                if n <= 0 {
                    return Err("pread failed".into());
                }
                done += n as usize;
            }
            Ok(())
        }

        fn write_page(&self, pid: u64, src: *const Page) -> Result<()> {
            let fd = self.file.as_raw_fd();
            let buf = unsafe { slice::from_raw_parts(src as *const u8, PAGE_SIZE) };
            let mut done = 0;
            while done < buf.len() {
                let n = unsafe { libc::pwrite(fd, buf.as_ptr().add(done) as *const c_void, (buf.len() - done) as usize, (pid * PAGE_SIZE as u64 + done as u64) as i64) };
                if n <= 0 {
                    return Err("pwrite failed".into());
                }
                done += n as usize;
            }
            Ok(())
        }

        fn write_pages(&self, pids: &[u64], base: *const Page) -> Result<()> {
            // submit batch writes
            let fd = self.file.as_raw_fd();
            let mut cbs: Vec<libc::iocb> = Vec::with_capacity(pids.len());
            let mut cb_ptrs: Vec<*mut libc::iocb> = Vec::with_capacity(pids.len());
            for &pid in pids {
                let mut cb: libc::iocb = unsafe { std::mem::zeroed() };
                cb.aio_fildes = fd as u32;
                cb.aio_lio_opcode = IOCB_CMD_PWRITE as u16;
                cb.aio_data = pid;
                cb.aio_buf = unsafe { base.add(pid as usize) as u64 };
                cb.aio_nbytes = PAGE_SIZE as u64;
                cb.aio_offset = (pid * PAGE_SIZE as u64) as i64;
                cbs.push(cb);
            }
            for cb in &mut cbs {
                cb_ptrs.push(cb as *mut _);
            }
            let mut submitted = 0usize;
            let mut retries = 0;
            while submitted < cb_ptrs.len() {
                let ptr = unsafe { cb_ptrs.as_mut_ptr().add(submitted) };
                let to_submit = cb_ptrs.len() - submitted;
                match io_submit(self.ctx, unsafe {
                    std::slice::from_raw_parts_mut(ptr, to_submit)
                }) {
                    Ok(ret) if ret == 0 => {
                        // nothing submitted; avoid tight loop
                        if retries > 3 {
                            break;
                        }
                        retries += 1;
                        continue;
                    }
                    Ok(ret) => {
                        submitted += ret;
                        retries = 0;
                    }
                    Err(e) => {
                        // fallback to per-page writes for remaining
                        for &pid in &pids[submitted..] {
                            self.write_page(pid, unsafe { base.add(pid as usize) })?;
                        }
                        return Err(e);
                    }
                }
            }

            if submitted == 0 {
                // fallback to synchronous writes if submission failed entirely
                for &pid in pids {
                    self.write_page(pid, unsafe { base.add(pid as usize) })?;
                }
                return Ok(());
            }

            let mut events: Vec<IoEvent> = vec![IoEvent { data: 0, obj: 0, res: 0, res2: 0 }; submitted];
            let mut completed = 0usize;
            while completed < submitted {
                let ret = io_getevents(self.ctx, 1, submitted - completed, &mut events[completed..])?;
                if ret == 0 {
                    break;
                }
                completed += ret;
            }
            let mut failures = Vec::new();
            for ev in events.iter().take(completed) {
                if ev.res != PAGE_SIZE as i64 {
                    failures.push(ev.data);
                }
            }
            if !failures.is_empty() {
                let mut first_err: Option<Box<dyn std::error::Error + Send + Sync>> = None;
                for pid in failures {
                    if let Err(e) = self.write_page(pid, unsafe { base.add(pid as usize) }) {
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
                if let Some(e) = first_err {
                    return Err(e);
                }
            }
            Ok(())
        }
    }
}

#[cfg(not(all(target_os = "linux", feature = "libaio")))]
mod noop {
    use crate::Result;
    use std::fs::File;
    use std::sync::Arc;

    /// Stub for non-Linux or when libaio feature is disabled.
    pub struct LibaioIo;

    impl LibaioIo {
        pub fn open(_file: File, _max_ios: usize) -> Result<Arc<Self>> {
            Err("libaio feature is only available on Linux targets".into())
        }
    }
}

#[cfg(all(target_os = "linux", feature = "libaio"))]
pub use lbaio_impl::LibaioIo;

#[cfg(not(all(target_os = "linux", feature = "libaio")))]
pub use noop::LibaioIo;
