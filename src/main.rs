mod app;
mod authorization;
mod cli;
mod cluster;
mod config;
mod content;
mod control;
#[cfg(feature = "fault-injection")]
mod faults;
mod http;
mod model;
mod rpc;
mod runtime;
mod status;
mod storage;
mod upload;
mod worker;

fn main() -> anyhow::Result<()> {
    // Set creation permissions before the runtime starts any threads.
    #[cfg(unix)]
    unsafe {
        libc::umask(0o022);
    }
    runtime::run()
}
