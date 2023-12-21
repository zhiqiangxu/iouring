#![feature(strict_provenance)]
#![feature(new_uninit)]
#![feature(core_intrinsics)]

use std::hint::spin_loop;
use std::intrinsics::likely;
use std::io;
use std::mem;
use std::mem::MaybeUninit;
use std::os::fd::RawFd;
use std::ptr::from_exposed_addr;
use std::ptr::from_exposed_addr_mut;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use io_uring::opcode;
use io_uring::squeue;
use io_uring::squeue::Flags;
use io_uring::types;
use io_uring::IoUring;
mod queue;
mod send_ptr;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use queue::Queue;
use send_ptr::SendPtr;
use send_ptr::ToSendPtr;

#[derive(Debug)]
pub struct Manager {
    terminate: Arc<AtomicBool>,
    uringloop_handle: Option<JoinHandle<()>>,
    io_q: Arc<Queue<SendPtr<IO>>>,
}

pub const OP_RESULT_PENDING: u8 = 0u8;
pub const OP_RESULT_OK: u8 = 1u8;
pub const OP_RESULT_NG: u8 = 2u8;

impl Manager {
    pub fn new(fds: &[RawFd], queue_size: u32) -> Result<Self> {
        let terminate = Arc::new(AtomicBool::new(false));
        let uringloop =
            UringLoop::new(terminate.clone(), queue_size).context("create uringloop")?;
        let io_q = uringloop.io_q();

        uringloop
            .register_files(fds)
            .context("register files to io_uring")?;

        let uringloop_handle = Some(uringloop.start_loop());

        Ok(Self {
            terminate,
            io_q,
            uringloop_handle,
        })
    }

    pub fn read(&self, buf: &mut [u8], fd_idx: i32, valid: &AtomicU8) -> SendPtr<IO> {
        unsafe {
            let io = self.io_q.alloc();

            let p_io = io.get();
            (*p_io).buf = SendPtr::new(buf.as_mut_ptr());
            (*p_io).fd = fd_idx;
            (*p_io).len = buf.len();
            (*p_io).op_code = OpCode::Read;
            (*p_io).leak = true;
            (*p_io).manager = SendPtr::new(self as *const Manager as *mut Manager);
            (*p_io).user_arg = SendPtr::new(mem::transmute((
                (valid as *const _ as *mut AtomicU8).expose_addr(),
                0,
            )));
            (*p_io).completion_cb = Self::read_completion_cb;
            io
        }
    }

    pub fn terminate_and_wait(&mut self) -> thread::Result<()> {
        if let Some(uringloop_handle) = self.uringloop_handle.take() {
            self.terminate.store(true, Ordering::Release);
            uringloop_handle.join()
        } else {
            Ok(())
        }
    }

    pub fn push(&self, io: SendPtr<IO>) {
        self.io_q.push_to_kernel(io);
    }

    // returns a boolean value indicating whether IO is re-used.
    fn read_completion_cb(io: *mut IO) -> bool {
        unsafe {
            let (valid, _): (usize, usize) = mem::transmute((*io).user_arg.get());
            let valid: *const AtomicU8 = from_exposed_addr(valid);

            if (*io).result <= 0 {
                // dbg!(&*io);
                (*valid).swap(OP_RESULT_NG, Ordering::Release);
            } else if (*io).result as usize == (*io).len {
                (*valid).swap(OP_RESULT_OK, Ordering::Release);
            } else {
                (*io).buf = SendPtr::new((*io).buf.get().offset((*io).result as isize));
                (*io).len -= (*io).result as usize;
                (*io).offset += (*io).result as usize;
                (*(*io).manager.get()).push(SendPtr::new(io));
                return true;
            }
            return false;
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum OpCode {
    Read,
}

#[derive(Debug)]
pub struct IO {
    pub(crate) buf: SendPtr<u8>,

    pub(crate) op_code: OpCode,
    pub(crate) fd: i32,
    pub(crate) len: usize,
    pub offset: usize,
    pub(crate) leak: bool,

    pub(crate) result: i32,
    pub(crate) manager: SendPtr<Manager>,
    pub(crate) user_arg: SendPtr<[()]>,
    pub(crate) completion_cb: fn(*mut IO) -> bool,
}

pub(crate) struct UringLoop {
    terminate: Arc<AtomicBool>,

    io_q: Arc<Queue<SendPtr<IO>>>,
    io_uring: IoUring,
    remain_sqe: Option<squeue::Entry>,

    /// number of pending + submitted
    nring: u32,

    /// hold IO memeory
    _ios: Box<[MaybeUninit<IO>]>,
}

impl UringLoop {
    pub(crate) fn new(terminate: Arc<AtomicBool>, queue_size: u32) -> io::Result<Self> {
        let io_uring = IoUring::builder()
            .setup_sqpoll(1000 /* ms */)
            .build(queue_size)?;

        let io_q = Arc::new(Queue::new(queue_size as _));
        let mut ios = Box::new_uninit_slice(queue_size as _);
        for io in ios.iter_mut() {
            io_q.dealloc(SendPtr::new(io.as_mut_ptr()));
        }

        Ok(Self {
            io_q,
            io_uring,
            terminate,
            remain_sqe: None,
            nring: 0,
            _ios: ios,
        })
    }

    pub(crate) fn io_q(&self) -> Arc<Queue<SendPtr<IO>>> {
        self.io_q.clone()
    }

    pub(crate) fn register_files(&self, fds: &[RawFd]) -> io::Result<()> {
        self.io_uring.submitter().register_files(fds)
    }

    pub(crate) fn start_loop(mut self) -> JoinHandle<()> {
        unsafe {
            thread::spawn(move || {
                if let Err(e) = self.run_loop() {
                    panic!("uring loop panic: {:?}", e);
                }
                self.finish();

                std::thread::sleep(Duration::from_secs(1));
            })
        }
    }

    unsafe fn run_loop(&mut self) -> io::Result<()> {
        while self.should_loop() {
            let mut op = self.remain_sqe.take();
            if likely(op.is_none()) {
                if let Some(io) = self.io_q.to_disk_q.pop() {
                    let io = io.get();

                    op = Some(match (*io).op_code {
                        OpCode::Read => {
                            opcode::Read::new(types::Fd((*io).fd), (*io).buf.get(), (*io).len as _)
                                .offset((*io).offset as _)
                                .build()
                                .flags(Flags::FIXED_FILE | Flags::ASYNC)
                                .user_data(io.expose_addr() as _)
                        }
                        // OpCode::Write => {
                        //     opcode::Write::new(types::Fd((*io).fd), (*io).buf.get(), (*io).len as _)
                        //         .offset((*io).offset as _)
                        //         .build()
                        //         .flags(Flags::FIXED_FILE | Flags::ASYNC)
                        //         .user_data(io.expose_addr() as _)
                        // }
                    });
                }
            }

            if let Some(op) = op {
                match self.io_uring.submission().push(&op) {
                    Ok(()) => {
                        self.nring += 1;
                    }
                    Err(_) => {
                        self.remain_sqe.replace(op);
                    }
                }
            }

            if self.nring > 0 {
                self.submit_to_disk()?;
                if let Some(io) = self.receive_from_kernel() {
                    self.nring -= 1;
                    let reused = ((*io).completion_cb)(io);
                    if !(reused || (*io).leak) {
                        self.io_q.dealloc(io.to_send_ptr());
                    }
                }
            }

            spin_loop();
        }
        Ok(())
    }

    fn submit_to_disk(&self) -> io::Result<u32> {
        loop {
            match self.io_uring.submit_and_wait(0 /* submit all */) {
                Ok(n) => return Ok(n as u32),
                Err(e) => {
                    let kind = e.kind();
                    if kind == io::ErrorKind::Interrupted || kind == io::ErrorKind::WouldBlock {
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    fn receive_from_kernel(&mut self) -> Option<*mut IO> {
        let cqe = self.io_uring.completion().next()?;
        let io: *mut IO = from_exposed_addr_mut(cqe.user_data() as usize);

        unsafe {
            (*io).result = cqe.result();
        }
        Some(io)
    }

    fn finish(&mut self) {
        unsafe {
            let fsync = opcode::Fsync::new(types::Fd(0)).build();
            self.io_uring.submission().push(&fsync).unwrap();
        }
    }

    #[inline]
    fn should_loop(&self) -> bool {
        !self.terminate.load(Ordering::Acquire)
            || self.nring != 0
            || self.remain_sqe.is_some()
            || !self.io_q.to_disk_q.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::sync::atomic::AtomicU8;
    use std::sync::atomic::Ordering;

    use rand;
    use rand::Rng;
    use rustix::fd::AsRawFd;

    use super::*;

    #[test]
    fn test() {
        let tmpdir = tempfile::tempdir().unwrap();
        let test_filename = tmpdir.path().join("test1");

        let mut test_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&test_filename)
            .unwrap();

        let mut rng = rand::thread_rng();
        let size = 10;

        let random_bytes: Vec<u8> = (0..size).map(|_| rng.gen()).collect();
        test_file.write_all(&random_bytes).unwrap();

        let mut manager = Manager::new(&[test_file.as_raw_fd()], 128).unwrap();
        let mut read = vec![0; size];
        let result = AtomicU8::from(OP_RESULT_PENDING);
        let io = manager.read(&mut read, 0, &result);

        unsafe {
            (*io.get()).offset = 0;
        }

        manager.push(io);

        while result.load(Ordering::Acquire) == OP_RESULT_PENDING {
            spin_loop();
        }

        drop(test_file);
        fs::remove_file(test_filename).unwrap();
        manager.terminate_and_wait().unwrap();

        assert_eq!(read, random_bytes);
    }
}
