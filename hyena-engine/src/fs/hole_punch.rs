use crate::error::*;

use libc::{fallocate, FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE};
use std::os::unix::io::AsRawFd;
use hyena_common::libc::cvt_r;


pub fn punch_hole<F: AsRawFd>(file: &F, size: usize) -> Result<()> {
    let fd = file.as_raw_fd();

    // punch one enormous hole :)
    unsafe {
        cvt_r(|| {
            fallocate(
                fd,
                FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE,
                0,
                size as i64,
            )
        })?;
    }

    Ok(())
}
