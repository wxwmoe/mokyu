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
mod runtime;
mod status;
mod storage;
mod upload;
mod worker;

fn main() -> anyhow::Result<()> {
    runtime::run()
}
