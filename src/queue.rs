use std::hint;

use crossbeam::queue::ArrayQueue;

#[derive(Debug)]
pub(crate) struct Queue<T> {
    free_q: ArrayQueue<T>,
    pub to_disk_q: ArrayQueue<T>,
}

impl<T> Queue<T> {
    pub fn new(queue_size: usize) -> Self {
        Self {
            free_q: ArrayQueue::new(queue_size),
            to_disk_q: ArrayQueue::new(queue_size),
        }
    }

    pub fn alloc(&self) -> T {
        loop {
            // likely
            if let Some(io) = self.free_q.pop() {
                return io;
            }
            hint::spin_loop();
        }
    }

    pub fn dealloc(&self, mut io: T) {
        while let Err(ele) = self.free_q.push(io) {
            io = ele;
            hint::spin_loop();
        }
    }

    pub fn push_to_kernel(&self, mut io: T) {
        while let Err(ele) = self.to_disk_q.push(io) {
            io = ele;
            hint::spin_loop();
        }
    }
}
