use std::{
    cell::Cell,
    io::{self, IsTerminal, Write},
    time::{Duration, Instant},
};

pub(super) struct Progress {
    objects: u64,
    bytes: u64,
    logical: Cell<u64>,
    wire: Cell<u64>,
    finished: Cell<u64>,
    last: Cell<Instant>,
    start: Instant,
    tty: bool,
}
pub(super) struct ObjectProgress<'a> {
    total: &'a Progress,
    accounted: Cell<u64>,
}
impl ObjectProgress<'_> {
    pub fn advance(&self, n: u64, transferred: bool) {
        self.accounted.set(self.accounted.get() + n);
        self.total.advance(n, transferred);
    }
    pub fn finish(&self, size: u64) {
        self.total
            .advance(size.saturating_sub(self.accounted.get()), false);
    }
}
impl Progress {
    pub fn object(&self) -> ObjectProgress<'_> {
        ObjectProgress {
            total: self,
            accounted: Cell::new(0),
        }
    }
    pub fn new(objects: u64, bytes: u64) -> Self {
        let now = Instant::now();
        Self {
            objects,
            bytes,
            logical: Cell::new(0),
            wire: Cell::new(0),
            finished: Cell::new(0),
            last: Cell::new(now),
            start: now,
            tty: io::stderr().is_terminal(),
        }
    }
    pub fn advance(&self, n: u64, transferred: bool) {
        self.logical.set(self.logical.get().saturating_add(n));
        if transferred {
            self.wire.set(self.wire.get().saturating_add(n));
        }
        self.print(false);
    }
    pub fn done(&self) {
        self.finished.set(self.finished.get() + 1);
        self.print(false);
    }
    pub fn transferred(&self) -> u64 {
        self.wire.get()
    }
    pub fn print(&self, force: bool) {
        let interval = if self.tty {
            Duration::from_millis(300)
        } else {
            Duration::from_secs(5)
        };
        if !force && self.last.get().elapsed() < interval {
            return;
        }
        let elapsed = self.start.elapsed().as_secs_f64();
        let logical = self.logical.get();
        let wire = self.wire.get();
        let pct = if self.bytes == 0 {
            if self.finished.get() == self.objects {
                100.0
            } else {
                0.0
            }
        } else {
            (100.0 * logical as f64 / self.bytes as f64).min(100.0)
        };
        let mut line = format!(
            "{}/{} objects   {}/{} B ({pct:.1}%)",
            self.finished.get(),
            self.objects,
            logical,
            self.bytes
        );
        if elapsed >= 1. && wire > 0 {
            let speed = wire as f64 / elapsed;
            line.push_str(&format!(
                "   {:.1} MiB/s   ETA {:.0}s",
                speed / (1024. * 1024.),
                self.bytes.saturating_sub(logical) as f64 / speed
            ));
        }
        let mut stderr = io::stderr().lock();
        if self.tty && !force {
            let _ = write!(stderr, "\r{line}\x1b[K");
        } else {
            let _ = writeln!(stderr, "{}{line}", if self.tty { "\r" } else { "" });
        }
        let _ = stderr.flush();
        self.last.set(Instant::now());
    }
}
