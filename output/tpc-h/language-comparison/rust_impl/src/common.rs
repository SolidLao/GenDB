use memmap2::Mmap;
use std::fs::File;
use std::path::Path;
use std::time::Instant;
use rayon::prelude::*;

/// Memory-mapped column — zero-copy access to binary columnar data
pub struct MmapCol {
    pub mmap: Mmap,
}

impl MmapCol {
    pub fn open(path: &str) -> Self {
        let file = File::open(path).unwrap_or_else(|e| panic!("Cannot open {}: {}", path, e));
        let mmap = unsafe { Mmap::map(&file).unwrap() };
        Self { mmap }
    }

    pub fn advise_sequential(&self) {
        #[cfg(unix)]
        unsafe {
            libc::madvise(
                self.mmap.as_ptr() as *mut libc::c_void,
                self.mmap.len(),
                libc::MADV_SEQUENTIAL,
            );
        }
    }

    pub fn advise_random(&self) {
        #[cfg(unix)]
        unsafe {
            libc::madvise(
                self.mmap.as_ptr() as *mut libc::c_void,
                self.mmap.len(),
                libc::MADV_RANDOM,
            );
        }
    }

    pub fn advise_willneed(&self) {
        #[cfg(unix)]
        unsafe {
            libc::madvise(
                self.mmap.as_ptr() as *mut libc::c_void,
                self.mmap.len(),
                libc::MADV_WILLNEED,
            );
        }
    }

    pub fn as_slice<T>(&self) -> &[T] {
        let ptr = self.mmap.as_ptr() as *const T;
        let len = self.mmap.len() / std::mem::size_of::<T>();
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }

    pub fn as_ptr<T>(&self) -> *const T {
        self.mmap.as_ptr() as *const T
    }

    pub fn len(&self) -> usize {
        self.mmap.len()
    }
}

/// RAII timing phase — prints [TIMING] on drop
pub struct PhaseTimer {
    name: &'static str,
    start: Instant,
}

impl PhaseTimer {
    pub fn new(name: &'static str) -> Self {
        Self { name, start: Instant::now() }
    }

    pub fn elapsed_ms(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1000.0
    }
}

impl Drop for PhaseTimer {
    fn drop(&mut self) {
        let ms = self.start.elapsed().as_secs_f64() * 1000.0;
        println!("[TIMING] {}: {:.2} ms", self.name, ms);
    }
}

/// Force rayon thread pool creation, then do a dummy parallel op to spawn all threads.
/// Pass 0 for default (all CPUs).
pub fn prewarm_rayon(num_threads: usize) {
    let mut builder = rayon::ThreadPoolBuilder::new();
    if num_threads > 0 {
        builder = builder.num_threads(num_threads);
    }
    builder.build_global().ok();
    // Force actual thread creation by running a trivial parallel op
    let _: Vec<i32> = (0..rayon::current_num_threads()).into_par_iter().map(|i| i as i32).collect();
}

/// Convert epoch days to year (for Q9)
pub fn year_from_days(z: i32) -> i32 {
    let z = z + 719468;
    let era = if z >= 0 { z / 146097 } else { (z - 146096) / 146097 };
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    y + if m <= 2 { 1 } else { 0 }
}

/// Convert epoch days to "YYYY-MM-DD" string
pub fn epoch_days_to_date_str(days: i32) -> String {
    let z = days + 719468;
    let era = if z >= 0 { z / 146097 } else { (z - 146096) / 146097 };
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = y + if m <= 2 { 1 } else { 0 };
    format!("{:04}-{:02}-{:02}", y, m, d)
}
