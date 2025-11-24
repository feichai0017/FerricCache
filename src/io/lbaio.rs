#[cfg(all(target_os = "linux", feature = "libaio"))]
mod lbaio_impl {
    use crate::Result;
    use crate::memory::{PAGE_SIZE, page::Page};
    use crate::thread_local::get_worker_id;
    use crossbeam_channel::{Receiver, Sender};
    use libc::c_void;
    use std::fs::File;
    use std::io;
    use std::os::fd::AsRawFd;
    use std::ptr;
    use std::slice;
    use std::sync::Arc;

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
        let _ = ret;
    }

    #[inline]
    fn io_submit(ctx: AioContext, cbs: &mut [*mut libc::iocb]) -> Result<usize> {
        if cbs.is_empty() {
            return Ok(0);
        }
        let ret = unsafe {
            libc::syscall(
                libc::SYS_io_submit,
                ctx,
                cbs.len() as libc::c_long,
                cbs.as_mut_ptr(),
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(ret as usize)
    }

    #[inline]
    fn io_getevents(
        ctx: AioContext,
        min_nr: usize,
        nr: usize,
        events: &mut [IoEvent],
        timeout_ns: i64,
    ) -> Result<usize> {
        debug_assert!(nr <= events.len());
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: timeout_ns,
        };
        let ts_ptr = if timeout_ns > 0 {
            &mut ts as *mut libc::timespec
        } else {
            ptr::null_mut()
        };
        let ret = unsafe {
            libc::syscall(
                libc::SYS_io_getevents,
                ctx,
                min_nr as libc::c_long,
                nr as libc::c_long,
                events.as_mut_ptr(),
                ts_ptr,
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
        async_tx: Vec<Sender<AsyncReq>>,
        affinity: Option<Vec<usize>>,
        queue_stats: Vec<IoQueueCounters>,
    }

    unsafe impl Send for LibaioIo {}
    unsafe impl Sync for LibaioIo {}

    impl Drop for LibaioIo {
        fn drop(&mut self) {
            io_destroy(self.ctx);
        }
    }

    impl LibaioIo {
        pub fn open(
            file: File,
            max_ios: usize,
            workers: usize,
            affinity: Option<Vec<usize>>,
        ) -> Result<Arc<Self>> {
            let mut ctx: AioContext = 0;
            io_setup(max_ios, &mut ctx)?;
            let mut senders = Vec::with_capacity(workers.max(1));
            let mut queue_stats = Vec::with_capacity(workers.max(1));
            for _ in 0..workers.max(1) {
                queue_stats.push(IoQueueCounters::default());
            }
            let io = Arc::new(Self {
                file,
                ctx,
                _max_ios: max_ios,
                async_tx: Vec::new(),
                affinity: affinity.clone(),
                queue_stats,
            });
            for i in 0..workers.max(1) {
                let (tx, rx) = crossbeam_channel::bounded::<AsyncReq>(max_ios.max(1));
                senders.push(tx);
                let io_clone = io.clone();
                let aff_idx = affinity.as_ref().and_then(|v| v.get(i)).copied();
                std::thread::spawn(move || {
                    #[cfg(target_os = "linux")]
                    if let Some(cpu) = aff_idx {
                        let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
                        unsafe {
                            libc::CPU_ZERO(&mut set);
                            libc::CPU_SET(cpu, &mut set);
                            libc::sched_setaffinity(
                                0,
                                std::mem::size_of::<libc::cpu_set_t>(),
                                &set,
                            );
                        }
                    }
                    io_clone.async_worker(rx);
                });
            }
            // Safety: we constructed io with empty async_tx above; replace with senders now.
            let io_ptr = Arc::as_ptr(&io) as *mut LibaioIo;
            unsafe {
                (*io_ptr).async_tx = senders;
            }
            Ok(io)
        }

        fn async_worker(&self, rx: Receiver<AsyncReq>) {
            for req in rx {
                let _ = self.handle_async_req(req);
            }
        }

        fn handle_async_req(&self, req: AsyncReq) -> Result<()> {
            let mut cbs: Vec<libc::iocb> = Vec::with_capacity(req.bufs.len());
            let mut cb_ptrs: Vec<*mut libc::iocb> = Vec::with_capacity(req.bufs.len());
            for (idx, (pid, buf)) in req.bufs.iter().enumerate() {
                let mut cb: libc::iocb = unsafe { std::mem::zeroed() };
                cb.aio_fildes = self.file.as_raw_fd() as u32;
                cb.aio_lio_opcode = IOCB_CMD_PWRITE as u16;
                cb.aio_data = *pid;
                cb.aio_buf = buf.as_ptr() as u64;
                cb.aio_nbytes = buf.len() as u64;
                cb.aio_offset = (pid * PAGE_SIZE as u64) as i64;
                cbs.push(cb);
                cb_ptrs.push(cbs.as_mut_ptr().wrapping_add(idx));
            }
            if cb_ptrs.is_empty() {
                let _ = req.done.send(Ok(()));
                return Ok(());
            }
            let mut submitted = 0usize;
            let mut retries = 0;
            while submitted < cb_ptrs.len() {
                match io_submit(self.ctx, &mut cb_ptrs[submitted..]) {
                    Ok(0) => {
                        retries += 1;
                        if retries > 3 {
                            break;
                        }
                        std::thread::sleep(std::time::Duration::from_micros(50 * retries as u64));
                        continue;
                    }
                    Ok(n) => {
                        submitted += n;
                        retries = 0;
                    }
                    Err(e) => {
                        // fallback sync for remaining
                        for (pid, buf) in &req.bufs[submitted..] {
                            let ptr = buf.as_ptr() as *const Page;
                            let _ = self.write_page(*pid, ptr);
                        }
                        let raw = e.raw_os_error();
                        let err = raw.map(io::Error::from_raw_os_error).unwrap_or(e);
                        let _ = req.done.send(Err(err));
                        self.queue_stats[req.queue_idx]
                            .fail
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        return Err(io::Error::from_raw_os_error(libc::EIO).into());
                    }
                }
            }
            if submitted == 0 {
                // fallback to sync write
                for (pid, buf) in req.bufs {
                    let ptr = buf.as_ptr() as *const Page;
                    let _ = self.write_page(pid, ptr);
                }
                let err = io::Error::from_raw_os_error(libc::EAGAIN);
                let _ = req.done.send(Err(err));
                return Err(io::Error::from_raw_os_error(libc::EAGAIN).into());
            }
            let mut events: Vec<IoEvent> = vec![
                IoEvent {
                    data: 0,
                    obj: 0,
                    res: 0,
                    res2: 0
                };
                submitted
            ];
            let mut completed = 0usize;
            let mut idle_polls = 0;
            while completed < submitted {
                let ret = io_getevents(
                    self.ctx,
                    1,
                    submitted - completed,
                    &mut events[completed..],
                    500_000,
                )?;
                if ret == 0 {
                    idle_polls += 1;
                    if idle_polls > 10 {
                        break;
                    }
                    continue;
                }
                idle_polls = 0;
                completed += ret;
            }
            if completed < submitted {
                for (pid, buf) in req.bufs {
                    let ptr = buf.as_ptr() as *const Page;
                    let _ = self.write_page(pid, ptr);
                }
                let err = io::Error::from_raw_os_error(libc::ETIMEDOUT);
                let _ = req.done.send(Err(err));
                self.queue_stats[req.queue_idx]
                    .timeout
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err(io::Error::from_raw_os_error(libc::ETIMEDOUT).into());
            }
            let mut ok = true;
            for ev in events.iter().take(completed) {
                if ev.res < 0 || ev.res as usize != req.bufs[0].1.len() {
                    ok = false;
                    break;
                }
            }
            if ok {
                let _ = req.done.send(Ok(()));
                Ok(())
            } else {
                // retry per-page synchronously on failure
                for (pid, buf) in req.bufs {
                    let ptr = buf.as_ptr() as *const Page;
                    let _ = self.write_page(pid, ptr);
                }
                let err = io::Error::from_raw_os_error(libc::EIO);
                let _ = req.done.send(Err(err));
                self.queue_stats[req.queue_idx]
                    .retries
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err(io::Error::from_raw_os_error(libc::EIO).into())
            }
        }
    }

    struct AsyncReq {
        queue_idx: usize,
        bufs: Vec<(u64, Vec<u8>)>,
        done: std::sync::mpsc::Sender<Result<()>>,
    }

    #[derive(Default)]
    struct IoQueueCounters {
        submit: std::sync::atomic::AtomicU64,
        fail: std::sync::atomic::AtomicU64,
        timeout: std::sync::atomic::AtomicU64,
        retries: std::sync::atomic::AtomicU64,
    }

    impl crate::io::PageIo for LibaioIo {
        fn read_page(&self, pid: u64, dst: *mut Page) -> Result<()> {
            // fallback to pread for reads in this simple implementation
            let fd = self.file.as_raw_fd();
            let buf = unsafe { slice::from_raw_parts_mut(dst as *mut u8, PAGE_SIZE) };
            let mut done = 0;
            while done < buf.len() {
                let n = unsafe {
                    libc::pread(
                        fd,
                        buf.as_mut_ptr().add(done) as *mut c_void,
                        (buf.len() - done) as usize,
                        (pid * PAGE_SIZE as u64 + done as u64) as i64,
                    )
                };
                if n <= 0 {
                    return Err(io::Error::last_os_error().into());
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
                let n = unsafe {
                    libc::pwrite(
                        fd,
                        buf.as_ptr().add(done) as *const c_void,
                        (buf.len() - done) as usize,
                        (pid * PAGE_SIZE as u64 + done as u64) as i64,
                    )
                };
                if n <= 0 {
                    return Err(io::Error::last_os_error().into());
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
                return Err(io::Error::from_raw_os_error(libc::EAGAIN).into());
            }

            let mut events: Vec<IoEvent> = vec![
                IoEvent {
                    data: 0,
                    obj: 0,
                    res: 0,
                    res2: 0
                };
                submitted
            ];
            let mut completed = 0usize;
            let mut idle_polls = 0;
            // Poll with short timeout; if repeatedly idle, fall back to synchronous completion.
            while completed < submitted {
                let ret = io_getevents(
                    self.ctx,
                    1,
                    submitted - completed,
                    &mut events[completed..],
                    500_000,
                )?; // 0.5 ms timeout
                if ret == 0 {
                    idle_polls += 1;
                    if idle_polls > 10 {
                        break;
                    }
                    continue;
                }
                idle_polls = 0;
                completed += ret;
            }
            if completed < submitted {
                // Fallback to sync writes for remaining to ensure durability.
                for &pid in pids {
                    let ptr = unsafe { base.add(pid as usize) };
                    self.write_page(pid, ptr)?;
                }
                return Err(io::Error::from_raw_os_error(libc::ETIMEDOUT).into());
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
                return Err(
                    first_err.unwrap_or_else(|| io::Error::from_raw_os_error(libc::EIO).into())
                );
            }
            Ok(())
        }

        fn write_pages_async(
            &self,
            bufs: Vec<(u64, Vec<u8>)>,
        ) -> Option<std::sync::mpsc::Receiver<Result<()>>> {
            let (tx, rx) = std::sync::mpsc::channel();
            if self.async_tx.is_empty() {
                return None;
            }
            let idx = (get_worker_id() as usize) % self.async_tx.len();
            let req = AsyncReq {
                queue_idx: idx,
                bufs,
                done: tx,
            };
            self.queue_stats[idx]
                .submit
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            match self.async_tx[idx].try_send(req) {
                Ok(_) => Some(rx),
                Err(_) => {
                    self.queue_stats[idx]
                        .fail
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    None
                }
            }
        }

        fn queue_stats(&self) -> Option<crate::io::IoStatsSnapshot> {
            let mut queues = Vec::with_capacity(self.queue_stats.len());
            for q in &self.queue_stats {
                queues.push(crate::io::IoQueueSnapshot {
                    submit: q.submit.load(std::sync::atomic::Ordering::Relaxed),
                    fail: q.fail.load(std::sync::atomic::Ordering::Relaxed),
                    timeout: q.timeout.load(std::sync::atomic::Ordering::Relaxed),
                    retries: q.retries.load(std::sync::atomic::Ordering::Relaxed),
                });
            }
            Some(crate::io::IoStatsSnapshot { queues })
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
