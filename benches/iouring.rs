use std::fs::File;
use std::io::Write;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use iouring::Manager;
use rand;
use rand::Rng;
use tempfile::NamedTempFile;

fn random_read_basic(temp_file: &NamedTempFile) {}

fn random_read_with_uring(temp_file: &NamedTempFile) {}

fn bench_random_read(c: &mut Criterion) {
    let mut temp_file = NamedTempFile::new().expect("Failed to create temporary file");

    {
        let mut rng = rand::thread_rng();
        let size = 10u32.pow(9);
        let random_bytes: Vec<u8> = (0..size).map(|_| rng.gen()).collect();

        temp_file.write_all(&random_bytes).unwrap();
    }

    {
        c.bench_function("random_read_basic", |b| {
            b.iter(|| random_read_basic(&temp_file))
        });
    }

    c.bench_function("random_read_with_uring", |b| {
        b.iter(|| random_read_with_uring(&temp_file))
    });
}

criterion_group!(benches, bench_random_read);
criterion_main!(benches);
