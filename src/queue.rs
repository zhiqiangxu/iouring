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
        let mut i = 0;
        loop {
            // likely
            if let Some(io) = self.free_q.pop() {
                return io;
            }
            i += 1;
            if i > 1000000 {
                panic!("alloc hangup");
            }
            hint::spin_loop();
        }
    }

    pub fn dealloc(&self, mut io: T) {
        let mut i = 0;
        while let Err(ele) = self.free_q.push(io) {
            io = ele;
            i += 1;
            if i > 1000000 {
                panic!("dealloc hangup");
            }
            hint::spin_loop();
        }
    }

    pub fn push_to_kernel(&self, mut io: T) {
        let mut i = 0;
        while let Err(ele) = self.to_disk_q.push(io) {
            io = ele;
            i += 1;
            if i > 1000000 {
                panic!("push_to_kernel hangup");
            }
            hint::spin_loop();
        }
    }
}
