/// We need to transmit raw pointers across threads. It is possible to do this
/// without any unsafe code by converting pointers to usize or to AtomicPtr<T>
/// then back to a raw pointer for use. We prefer this approach because code
/// that uses this type is more explicit.
///
/// Unsafe code is still required to dereference the pointer, so this type is
/// not unsound on its own, although it does partly lift the unconditional
/// !Send and !Sync on raw pointers. As always, dereference with care.
#[derive(Debug)]
pub struct SendPtr<T: ?Sized>(*mut T);

// SAFETY: !Send for raw pointers is not for safety, just as a lint
unsafe impl<T: Send + ?Sized> Send for SendPtr<T> {}

// SAFETY: !Sync for raw pointers is not for safety, just as a lint
unsafe impl<T: Send + ?Sized> Sync for SendPtr<T> {}

impl<T: ?Sized> SendPtr<T> {
    pub fn new(p: *mut T) -> Self {
        Self(p)
    }

    // Helper to avoid disjoint captures of `send_ptr.0`
    pub fn get(self) -> *mut T {
        self.0
    }
}

// Implement Clone without the T: Clone bound from the derive
impl<T: ?Sized> Clone for SendPtr<T> {
    fn clone(&self) -> Self {
        Self(self.0)
    }
}

// Implement Copy without the T: Copy bound from the derive
impl<T: ?Sized> Copy for SendPtr<T> {}

pub(crate) trait ToSendPtr<T: ?Sized> {
    fn to_send_ptr(self) -> SendPtr<T>;
}

impl<T: ?Sized> ToSendPtr<T> for *mut T {
    fn to_send_ptr(self) -> SendPtr<T> {
        SendPtr(self)
    }
}
