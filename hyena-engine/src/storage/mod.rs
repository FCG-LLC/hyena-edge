use error::*;

#[cfg(feature = "mmap")]
pub(crate) mod mmap;
#[cfg(feature = "hole_punching")]
mod hole_punch;
pub(crate) mod memory;
pub(crate) mod file;
mod map_type;


pub trait Storage<'stor, T: 'stor>: AsRef<[T]> + AsMut<[T]> {
    fn sync(&mut self) -> Result<()>;

    fn as_ptr(&self) -> *const T {
        self.as_ref().as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut T {
        self.as_mut().as_mut_ptr()
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn bytes_len(&self) -> usize {
        self.len() * ::std::mem::size_of::<T>()
    }

    fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }
}
