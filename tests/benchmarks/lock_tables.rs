//! Compile with rustc -O against the project's cached Tokio rlib; no database is needed.
#[path = "../../src/runtime/locks.rs"]
mod locks;

use std::{
    collections::HashMap,
    hint::black_box,
    sync::{Arc, Mutex, Weak},
    time::Instant,
};

type Key = (u128, [u8; 32]);
type Lock = Arc<tokio::sync::Mutex<()>>;

#[derive(Default)]
struct Previous(Mutex<HashMap<Key, Weak<tokio::sync::Mutex<()>>>>);

impl Previous {
    fn get(&self, key: Key) -> Lock {
        let mut entries = self.0.lock().unwrap();
        entries.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = entries.get(&key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        entries.insert(key, Arc::downgrade(&lock));
        lock
    }
}

fn key(index: usize) -> Key {
    (index as u128, [index as u8; 32])
}

fn measure(get: impl Fn(Key) -> Lock + Sync, live: usize, threads: usize, churn: bool) -> u128 {
    let keep: Vec<_> = (0..live).map(|index| get(key(index))).collect();
    let start = Instant::now();
    std::thread::scope(|scope| {
        for thread in 0..threads {
            let get = &get;
            scope.spawn(move || {
                for index in 0..100_000 / threads {
                    let id = if churn {
                        live + index * threads + thread
                    } else {
                        index % live
                    };
                    black_box(get(black_box(key(id))));
                }
            });
        }
    });
    black_box(keep);
    start.elapsed().as_nanos() / 100_000
}

fn main() {
    println!("round,live,threads,churn,implementation,ns_per_get");
    for round in 0..3 {
        for live in [1, 64, 4096] {
            for threads in [1, 4] {
                for churn in [false, true] {
                    for candidate in [round % 2 == 0, round % 2 != 0] {
                        let (name, elapsed) = if candidate {
                            let table = locks::Locks::default();
                            (
                                "amortized",
                                measure(|key| table.get(key), live, threads, churn),
                            )
                        } else {
                            let table = Previous::default();
                            (
                                "previous",
                                measure(|key| table.get(key), live, threads, churn),
                            )
                        };
                        println!("{round},{live},{threads},{churn},{name},{elapsed}");
                    }
                }
            }
        }
    }
}
