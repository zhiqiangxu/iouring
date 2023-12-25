#![feature(strict_provenance)]

use std::env;
use std::fs::File;
use std::io::Result;
use std::io::Write;
use std::ptr;

extern crate log;

use std::hint::spin_loop;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use iouring::*;
use rand;
use rand::prelude::*;
use rand::Rng;
use rustix::fd::AsRawFd;
use tempfile::NamedTempFile;

// TODO: why changing READ_SIZE to 8*4096 affects the performance significantly?
const READ_SIZE: usize = 20000;
const N: usize = 1024;

fn random_read_basic(
    temp_file: &NamedTempFile,
    buf: &mut [u8],
    size: usize,
    rng: &mut rand::rngs::ThreadRng,
) {
    for _ in 0..N {
        let offset = rng.gen_range(0..=(size - READ_SIZE) as u32);

        let mut buf = vec![0; READ_SIZE];

        temp_file
            .as_file()
            .read_exact_at(&mut buf, offset as u64)
            .unwrap();
    }
    black_box(buf);
}

fn random_read_with_uring(
    manager: &Manager,
    buf: &mut [u8],
    size: usize,
    rng: &mut rand::rngs::ThreadRng,
) {
    let valid = AtomicUsize::new(0);
    let callback = Callback {
        user_arg1: (&valid as *const AtomicUsize).expose_addr() as u64,
        user_arg2: 0,
        func: |io: &mut IO| unsafe {
            let valid: *const AtomicUsize = ptr::from_exposed_addr(io.user_arg1() as usize);
            (*valid).fetch_add(1, Ordering::Release);
        },
    };
    for i in 0..N {
        let offset = rng.gen_range(0..=(size - READ_SIZE));

        manager.submit_read_exact(
            0,
            &mut buf[i * READ_SIZE..(i + 1) * READ_SIZE],
            offset,
            callback,
        );
    }

    while valid.load(Ordering::Acquire) != N {
        spin_loop();
    }

    black_box(buf);
}

fn bench_random_read(c: &mut Criterion) {
    let mut temp_file = NamedTempFile::new_in(env::var("PLOT_DIR").unwrap_or("/tmp".into()))
        .expect("Failed to create temporary file");

    let mut rng = rand::thread_rng();
    let size = env::var("PLOT_SIZE")
        .unwrap_or("1000000000".into())
        .parse::<usize>()
        .unwrap();

    let mut random_bytes = vec![0; 1000];
    while temp_file.as_file().metadata().unwrap().len() < size as u64 {
        rng.fill_bytes(&mut random_bytes);
        temp_file.write_all(&random_bytes).unwrap();
    }

    temp_file.as_file().advise_random_access().unwrap();
    drop(random_bytes);

    let mut buf = vec![0u8; READ_SIZE * N];
    let manager = Manager::new(1024, &[temp_file.as_raw_fd()], &[&buf]).unwrap();

    log::info!("PLOT_SIZE:{}", size);

    {
        c.bench_function("random_read_basic", |b| {
            b.iter(|| random_read_basic(&temp_file, &mut buf, size, &mut rng))
        });
    }

    {
        c.bench_function("random_read_with_uring", |b| {
            b.iter(|| random_read_with_uring(&manager, &mut buf, size, &mut rng))
        });

        drop(manager);
    }
}

criterion_group!(benches, bench_random_read);
criterion_main!(benches);

/// Extension convenience trait that allows pre-allocating files, suggesting random access pattern
/// and doing cross-platform exact reads/writes
pub trait FileExt {
    /// Read exact number of bytes at a specific offset
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()>;
    /// Advise OS/file system that file will use random access and read-ahead behavior is
    /// undesirable, on Windows this can only be set when file is opened, see [`OpenOptionsExt`]
    fn advise_random_access(&self) -> Result<()>;
}

impl FileExt for File {
    #[cfg(unix)]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(self, buf, offset)
    }

    #[cfg(target_os = "linux")]
    fn advise_random_access(&self) -> Result<()> {
        let err = unsafe { libc::posix_fadvise(self.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM) };
        if err != 0 {
            Err(std::io::Error::from_raw_os_error(err))
        } else {
            Ok(())
        }
    }

    #[cfg(target_os = "macos")]
    fn advise_random_access(&self) -> Result<()> {
        if unsafe { libc::fcntl(self.as_raw_fd(), libc::F_RDAHEAD, 0) } != 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}
