use crate::{
    codec::{MAX, MIN},
    config,
};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    #[default]
    Always,
    Sample,
    FileType,
}

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub strategy: Strategy,
    pub level: i32,
    pub sample_level: i32,
    pub min_savings_percent: u8,
    pub min_savings_bytes: u64,
    pub context_idle_timeout: String,
    pub skip_mime_types: Vec<String>,
}

const SKIP_TYPES: &[&str] = &[
    "image/jpeg",
    "image/png",
    "image/apng",
    "image/gif",
    "image/webp",
    "image/avif",
    "video/mp4",
    "video/webm",
    "audio/mp4",
    "audio/mpeg",
    "audio/aac",
    "audio/ogg",
    "video/ogg",
    "application/ogg",
    "audio/flac",
    "audio/x-flac",
    "application/zip",
    "application/gzip",
    "application/x-gzip",
    "application/x-7z-compressed",
    "application/vnd.rar",
    "application/x-rar-compressed",
    "application/x-xz",
    "application/x-bzip2",
    "application/zstd",
];

impl Default for Config {
    fn default() -> Self {
        Self {
            strategy: Strategy::Always,
            level: 6,
            sample_level: 3,
            min_savings_percent: 2,
            min_savings_bytes: 256,
            context_idle_timeout: "30s".into(),
            skip_mime_types: SKIP_TYPES.iter().map(|s| (*s).into()).collect(),
        }
    }
}

fn valid_mime(value: &str) -> bool {
    value.split_once('/').is_some_and(|(a, b)| {
        [a, b].into_iter().all(|s| {
            !s.is_empty()
                && s.bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"!#$%&'+-.^_`|~".contains(&c))
        })
    })
}

fn extension_type(key: &str) -> Option<&'static str> {
    let (_, ext) = key.rsplit('/').next()?.rsplit_once('.')?;
    Some(match ext.to_ascii_lowercase().as_str() {
        "jpg" | "jpeg" | "jpe" => "image/jpeg",
        "png" => "image/png",
        "apng" => "image/apng",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "avif" => "image/avif",
        "svg" => "image/svg+xml",
        "mp4" | "m4v" => "video/mp4",
        "webm" => "video/webm",
        "m4a" => "audio/mp4",
        "mp3" | "mp2" => "audio/mpeg",
        "aac" => "audio/aac",
        "ogg" | "oga" | "opus" => "audio/ogg",
        "ogv" => "video/ogg",
        "ogx" => "application/ogg",
        "flac" => "audio/flac",
        "zip" => "application/zip",
        "gz" | "tgz" => "application/gzip",
        "7z" => "application/x-7z-compressed",
        "rar" => "application/vnd.rar",
        "xz" => "application/x-xz",
        "bz2" => "application/x-bzip2",
        "zst" | "zstd" => "application/zstd",
        "txt" => "text/plain",
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "js" | "mjs" => "text/javascript",
        "json" => "application/json",
        "xml" => "application/xml",
        _ => return None,
    })
}

impl Config {
    pub fn idle_timeout(&self) -> Result<Duration> {
        if self.context_idle_timeout == "0s" {
            return Ok(Duration::ZERO);
        }
        let seconds = config::seconds(&self.context_idle_timeout)?;
        ensure!(
            seconds <= i64::MAX as u64 / 1000,
            "compression.context_idle_timeout too large"
        );
        Ok(Duration::from_secs(seconds))
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            zstd::compression_level_range().contains(&self.level),
            "compression.level outside Zstd supported range"
        );
        ensure!(
            zstd::compression_level_range().contains(&self.sample_level),
            "compression.sample_level outside Zstd supported range"
        );
        ensure!(
            self.min_savings_percent <= 100,
            "compression.min_savings_percent must be 0..100"
        );
        self.idle_timeout()
            .context("invalid compression.context_idle_timeout")?;
        ensure!(
            self.skip_mime_types.iter().all(|s| valid_mime(s)),
            "compression.skip_mime_types requires exact MIME types without parameters or wildcards"
        );
        Ok(())
    }

    fn separate_sample_level(&self) -> Option<i32> {
        let effective = |level| match level {
            0 => zstd::zstd_safe::CLEVEL_DEFAULT,
            _ => level,
        };
        (self.strategy == Strategy::Sample && effective(self.level) != effective(self.sample_level))
            .then_some(self.sample_level)
    }

    // Includes idle encoder/decoder contexts, output capacity and reallocation overlap.
    pub fn workspace_bytes(&self) -> Result<u64> {
        self.validate()?;
        // Safety: this scalar-only estimator comes from our pinned, statically linked Zstd.
        // It bounds single-threaded compress2 for arbitrary input sizes and levels <= this one.
        let (size, sample, error, decoder) = unsafe {
            let size = zstd::zstd_safe::zstd_sys::ZSTD_estimateCCtxSize(self.level.max(3));
            let sample = match self.separate_sample_level() {
                Some(level) => zstd::zstd_safe::zstd_sys::ZSTD_estimateCCtxSize(level.max(3)),
                None => 0,
            };
            (
                size,
                sample,
                zstd::zstd_safe::zstd_sys::ZSTD_isError(size)
                    | zstd::zstd_safe::zstd_sys::ZSTD_isError(sample),
                zstd::zstd_safe::zstd_sys::ZSTD_estimateDCtxSize(),
            )
        };
        ensure!(error == 0, "Zstd context size estimation failed");
        (size as u64)
            .checked_add(sample as u64)
            .and_then(|n| n.checked_add(decoder as u64))
            .and_then(|n| n.checked_mul(2))
            .and_then(|n| n.checked_add(zstd::zstd_safe::compress_bound(MAX) as u64 + 16))
            .context("compression workspace budget overflow")
    }

    pub fn should_try(&self, content_type: Option<&str>, key: &str) -> bool {
        if self.strategy != Strategy::FileType {
            return true;
        }
        let mime = content_type.map(|v| {
            v.split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase()
        });
        let mime = mime
            .as_deref()
            .filter(|v| valid_mime(v) && *v != "application/octet-stream")
            .or_else(|| extension_type(key));
        mime.is_none_or(|mime| {
            !self
                .skip_mime_types
                .iter()
                .any(|v| v.eq_ignore_ascii_case(mime))
        })
    }

    fn worth_storing(&self, raw: usize, compressed: usize) -> bool {
        compressed < raw
            && (raw - compressed) as u64 >= self.min_savings_bytes
            && (raw - compressed) as u64 * 100 >= raw as u64 * u64::from(self.min_savings_percent)
    }
}

struct Workspace {
    compressor: zstd::bulk::Compressor<'static>,
    sampler: Option<zstd::bulk::Compressor<'static>>,
    output: Vec<u8>,
}
impl Workspace {
    fn new(level: i32) -> Result<Self> {
        Ok(Self {
            compressor: zstd::bulk::Compressor::new(level)?,
            sampler: None,
            output: Vec::new(),
        })
    }
    fn trial(&mut self, input: &[u8], sample_level: Option<i32>) -> Result<usize> {
        self.output.clear();
        // Exact reservation prevents geometric growth beyond the maximum chunk's bound.
        self.output
            .try_reserve_exact(zstd::zstd_safe::compress_bound(input.len()) + 16)?;
        let compressor = match sample_level {
            Some(level) => {
                if self.sampler.is_none() {
                    self.sampler = Some(zstd::bulk::Compressor::new(level)?);
                }
                self.sampler.as_mut().unwrap()
            }
            None => &mut self.compressor,
        };
        Ok(compressor.compress_to_buffer(input, &mut self.output)?)
    }
    fn encode(&mut self, input: &[u8], config: &Config) -> Result<Option<Vec<u8>>> {
        if config.strategy == Strategy::Sample && input.len() >= MIN {
            let len = (input.len() / 64).clamp(16 * 1024, 64 * 1024);
            let sample_level = config.separate_sample_level();
            let mut hit = false;
            for i in 0..4 {
                let start = (input.len() - len) * i / 3;
                if self.trial(&input[start..start + len], sample_level)? < len {
                    hit = true;
                    break;
                }
            }
            if !hit {
                return Ok(None);
            }
        }
        let size = self.trial(input, None)?;
        Ok(config
            .worth_storing(input.len(), size)
            .then(|| std::mem::take(&mut self.output)))
    }
}

pub struct Pool {
    config: Config,
    idle_timeout: Duration,
    // Callers hold the existing CPU permit until this operation finishes, bounding active + idle.
    idle: Mutex<Vec<(Instant, Workspace)>>,
    decoders: Mutex<Vec<(Instant, zstd::bulk::Decompressor<'static>)>>,
    max_idle: usize,
}
impl Pool {
    pub fn workspace_bytes(&self) -> Result<u64> {
        let mut config = self.config.clone();
        config.strategy = Strategy::Sample;
        config.workspace_bytes()
    }
    pub fn new(config: Config, max_idle: usize) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            idle_timeout: config.idle_timeout()?,
            config,
            idle: Mutex::new(Vec::new()),
            decoders: Mutex::new(Vec::new()),
            max_idle,
        })
    }
    pub fn compress(&self, input: &[u8], should_try: bool) -> Result<Option<Vec<u8>>> {
        ensure!(
            (1..=MAX).contains(&input.len()),
            "invalid compression input length"
        );
        self.compress_with(input, should_try, self.config.strategy)
    }
    pub fn compress_with(
        &self,
        input: &[u8],
        should_try: bool,
        strategy: Strategy,
    ) -> Result<Option<Vec<u8>>> {
        ensure!(
            !input.is_empty() && input.len() <= crate::pack::MAX_PAYLOAD,
            "pack compression exceeds bound"
        );
        if !should_try {
            return Ok(None);
        }
        let cached = self.idle.lock().unwrap().pop();
        let mut work = match cached {
            Some((time, work)) if time.elapsed() < self.idle_timeout => work,
            _ => Workspace::new(self.config.level)?,
        };
        // Error or panic drops the workspace instead of returning an uncertain context.
        let mut config = self.config.clone();
        config.strategy = strategy;
        let result = work.encode(input, &config)?;
        if work.output.capacity() > zstd::zstd_safe::compress_bound(MAX) + 16 {
            work.output = Vec::new();
        }
        if !self.idle_timeout.is_zero() {
            let mut idle = self.idle.lock().unwrap();
            if idle.len() < self.max_idle {
                idle.push((Instant::now(), work));
            }
        }
        Ok(result)
    }
    pub fn decompress(&self, input: &[u8], capacity: usize) -> Result<Vec<u8>> {
        ensure!(
            input.len() <= MAX && (1..=MAX).contains(&capacity),
            "invalid decompression input length or capacity"
        );
        self.decompress_pack(input, capacity)
    }
    pub fn decompress_pack(&self, input: &[u8], capacity: usize) -> Result<Vec<u8>> {
        ensure!(
            input.len() <= crate::pack::MAX_PAYLOAD
                && (1..=crate::pack::MAX_PAYLOAD).contains(&capacity),
            "pack decompression exceeds bound"
        );
        let cached = self.decoders.lock().unwrap().pop();
        let mut decoder = match cached {
            Some((time, decoder)) if time.elapsed() < self.idle_timeout => decoder,
            _ => zstd::bulk::Decompressor::new()?,
        };
        // The output belongs to its caller; only the context can return before HTTP completes.
        let result = decoder.decompress(input, capacity)?;
        if !self.idle_timeout.is_zero() {
            let mut idle = self.decoders.lock().unwrap();
            if idle.len() < self.max_idle {
                idle.push((Instant::now(), decoder));
            }
        }
        Ok(result)
    }
    pub async fn run(&self) -> Result<()> {
        let mut timer = tokio::time::interval(Duration::from_secs(1));
        loop {
            timer.tick().await;
            self.idle
                .lock()
                .unwrap()
                .retain(|(time, _)| time.elapsed() < self.idle_timeout);
            self.decoders
                .lock()
                .unwrap()
                .retain(|(time, _)| time.elapsed() < self.idle_timeout);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn configuration_gates_and_type_fallback() {
        let mut c: Config = toml::from_str("").unwrap();
        assert_eq!(
            (
                c.strategy,
                c.level,
                c.sample_level,
                c.min_savings_percent,
                c.min_savings_bytes
            ),
            (Strategy::Always, 6, 3, 2, 256)
        );
        let explicit: Config = toml::from_str("level=3\nsample_level=1").unwrap();
        assert_eq!((explicit.level, explicit.sample_level), (3, 1));
        assert_eq!(c.idle_timeout().unwrap(), Duration::from_secs(30));
        for (raw, saved, expected) in [
            (12800, 256, true),
            (12800, 255, false),
            (12801, 256, false),
            (10000, 200, false),
        ] {
            assert_eq!(c.worth_storing(raw, raw - saved), expected);
        }
        c.min_savings_percent = 0;
        c.min_savings_bytes = 0;
        assert!(!c.worth_storing(100, 100));
        assert!(!c.worth_storing(100, 101));
        assert!(c.worth_storing(100, 99));
        for text in [
            "strategy='disabled'",
            "level=2147483647",
            "sample_level=2147483647",
            "sample_level=-2147483648",
            "min_savings_percent=101",
            "min_savings_bytes=-1",
            "context_idle_timeout='-1s'",
            "skip_mime_types=['image/*']",
            "skip_mime_types=['image/png; x=1']",
        ] {
            assert!(
                toml::from_str::<Config>(text)
                    .and_then(|c| c.validate().map(|_| c).map_err(serde::de::Error::custom))
                    .is_err(),
                "{text}"
            );
        }
        for level in [
            zstd::compression_level_range().start().to_owned(),
            -1,
            0,
            3,
            12,
            zstd::compression_level_range().end().to_owned(),
        ] {
            c.level = level;
            assert!(c.workspace_bytes().unwrap() > MAX as u64);
            c.sample_level = level;
            c.level = 6;
            c.strategy = Strategy::Sample;
            assert!(c.workspace_bytes().unwrap() > MAX as u64);
        }
        c = Config::default();
        let full_budget = c.workspace_bytes().unwrap();
        for strategy in [Strategy::Always, Strategy::FileType] {
            c.strategy = strategy;
            c.sample_level = 12;
            assert_eq!(c.workspace_bytes().unwrap(), full_budget);
        }
        c.strategy = Strategy::Sample;
        c.sample_level = 6;
        assert_eq!(c.workspace_bytes().unwrap(), full_budget);
        c.sample_level = 3;
        let separate_budget = c.workspace_bytes().unwrap();
        assert!(separate_budget > full_budget);
        c.level = 3;
        c.sample_level = 6;
        assert_eq!(c.workspace_bytes().unwrap(), separate_budget);
        c.level = 0;
        c.sample_level = 3;
        c.strategy = Strategy::Always;
        let default_budget = c.workspace_bytes().unwrap();
        c.strategy = Strategy::Sample;
        assert_eq!(c.workspace_bytes().unwrap(), default_budget);
        c = Config::default();
        assert!(c.should_try(Some("image/jpeg"), "a.jpg"));
        c.strategy = Strategy::Sample;
        assert!(c.should_try(Some("image/jpeg"), "a.jpg"));
        c.strategy = Strategy::FileType;
        for (mime, key, expected) in [
            (Some(" IMAGE/JPEG ; charset=x"), "no-extension", false),
            (Some("application/octet-stream"), "a.MP4", false),
            (None, "a.jpg", false),
            (Some("invalid"), "a.png", false),
            (Some("image/svg+xml"), "a.jpg", true),
            (None, "a.svg", true),
            (Some("text/plain"), "a.png", true),
            (None, "a.unknown", true),
            (Some("application/octet-stream"), "opaque-id", true),
        ] {
            assert_eq!(c.should_try(mime, key), expected, "{mime:?} {key}");
        }
        c.skip_mime_types.clear();
        assert!(c.should_try(Some("image/jpeg"), "a.jpg"));
        c.skip_mime_types.push("TEXT/PLAIN".into());
        assert!(!c.should_try(None, "a.txt"));
    }

    #[test]
    fn independent_frames_sampling_and_buffer_reuse() {
        let mut random = vec![0; MAX];
        aws_lc_rs::rand::fill(&mut random).unwrap();
        for level in [-1, 0, 3, 12] {
            let mut work = Workspace::new(level).unwrap();
            let c = Config {
                level,
                ..Default::default()
            };
            for input in [
                &random[..MIN - 1],
                &random[..],
                &vec![42; MAX][..],
                &random[..32],
            ] {
                let fresh = zstd::bulk::compress(input, level).unwrap();
                let got = work.encode(input, &c).unwrap();
                assert_eq!(got.is_some(), c.worth_storing(input.len(), fresh.len()));
                if let Some(got) = got {
                    assert_eq!(got, fresh);
                    assert_eq!(zstd::bulk::decompress(&got, input.len()).unwrap(), input);
                    assert_eq!(work.output.capacity(), 0);
                }
            }
        }
        let c = Config {
            strategy: Strategy::Sample,
            ..Default::default()
        };
        let mut work = Workspace::new(c.level).unwrap();
        assert!(work.encode(&random, &c).unwrap().is_none());
        let pointer = work.output.as_ptr();
        assert!(work.encode(&random, &c).unwrap().is_none());
        assert_eq!(pointer, work.output.as_ptr());
        // The first three samples do not shrink; only the last sample triggers full compression.
        let mut late = random.clone();
        late[MAX - 512 * 1024..].fill(0);
        let encoded = work.encode(&late, &c).unwrap().unwrap();
        assert_eq!(zstd::bulk::decompress(&encoded, MAX).unwrap(), late);
        // Small chunks use a full trial even when their sampled regions would miss repetition.
        assert!(work.encode(&vec![0; MIN - 1], &c).unwrap().is_some());
        let pool = Pool::new(Config::default(), 1).unwrap();
        assert!(pool.compress(&late, false).unwrap().is_none());
        assert!(pool.idle.lock().unwrap().is_empty());
    }

    #[test]
    fn sample_level_is_independent_and_lazy() {
        let input: Vec<_> = (0..MIN).map(|i| (i % 256) as u8).collect();
        let fastest = *zstd::compression_level_range().start();
        // Each original sampling window has identical bytes; the fastest probe misses this pattern.
        assert!(
            zstd::bulk::compress(&input[..16 * 1024], fastest)
                .unwrap()
                .len()
                >= 16 * 1024
        );
        let full = zstd::bulk::compress(&input, 6).unwrap();
        let config = Config {
            strategy: Strategy::Sample,
            sample_level: fastest,
            ..Default::default()
        };
        let mut work = Workspace::new(config.level).unwrap();
        assert!(work.encode(&input, &config).unwrap().is_none());
        assert!(work.sampler.is_some());
        let buffer = work.output.as_ptr();
        assert!(work.encode(&input, &config).unwrap().is_none());
        assert_eq!(buffer, work.output.as_ptr());
        for (level, sample_level, separate) in [
            (6, 3, true),
            (3, 6, true),
            (6, 6, false),
            (0, 3, false),
            (3, 0, false),
            (6, 0, true),
        ] {
            let config = Config {
                level,
                sample_level,
                strategy: Strategy::Sample,
                ..Default::default()
            };
            let mut work = Workspace::new(level).unwrap();
            assert!(work.sampler.is_none());
            let small = &input[..MIN - 1];
            assert_eq!(
                work.encode(small, &config).unwrap().unwrap(),
                zstd::bulk::compress(small, level).unwrap()
            );
            assert!(work.sampler.is_none());
            for _ in 0..2 {
                let encoded = work.encode(&input, &config).unwrap().unwrap();
                assert_eq!(encoded, zstd::bulk::compress(&input, level).unwrap());
                assert_eq!(zstd::bulk::decompress(&encoded, MIN).unwrap(), input);
                assert_eq!(work.sampler.is_some(), separate);
            }
        }
        for strategy in [Strategy::Always, Strategy::FileType] {
            let config = Config {
                strategy,
                sample_level: fastest,
                ..Default::default()
            };
            let mut work = Workspace::new(config.level).unwrap();
            assert_eq!(work.encode(&input, &config).unwrap().unwrap(), full);
            assert!(work.sampler.is_none());
        }
    }

    #[tokio::test]
    async fn bounded_contexts_and_idle_release() {
        let pool = Arc::new(
            Pool::new(
                Config {
                    strategy: Strategy::Sample,
                    ..Default::default()
                },
                2,
            )
            .unwrap(),
        );
        let cpu = Arc::new(tokio::sync::Semaphore::new(2));
        let mut jobs = Vec::new();
        for i in 0..8 {
            let (pool, cpu) = (pool.clone(), cpu.clone());
            jobs.push(tokio::spawn(async move {
                let permit = cpu.acquire_owned().await.unwrap();
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    let raw = vec![i; MIN + i as usize];
                    let encoded = pool.compress(&raw, true).unwrap().unwrap();
                    let first = pool.decompress(&encoded, raw.len()).unwrap();
                    assert_eq!(first, raw);
                    assert_eq!(pool.decompress(&encoded, raw.len()).unwrap(), first);
                    assert_eq!(first, raw);
                })
                .await
                .unwrap();
            }));
        }
        for job in jobs {
            job.await.unwrap();
        }
        assert_eq!(cpu.available_permits(), 2);
        let count = pool.idle.lock().unwrap().len();
        assert!((1..=2).contains(&count));
        assert!(
            pool.idle
                .lock()
                .unwrap()
                .iter()
                .all(|(_, work)| work.sampler.is_some())
        );
        assert!((1..=2).contains(&pool.decoders.lock().unwrap().len()));
        for (time, _) in pool.idle.lock().unwrap().iter_mut() {
            *time = Instant::now() - Duration::from_secs(31);
        }
        for (time, _) in pool.decoders.lock().unwrap().iter_mut() {
            *time = Instant::now() - Duration::from_secs(31);
        }
        // The maintenance timer, without a new request, frees expired contexts.
        assert!(
            tokio::time::timeout(Duration::from_millis(20), pool.run())
                .await
                .is_err()
        );
        assert!(pool.idle.lock().unwrap().is_empty());
        assert!(pool.decoders.lock().unwrap().is_empty());
        let encoded = pool.compress(&vec![0; MIN], true).unwrap().unwrap();
        let held = pool.decompress(&encoded, MIN).unwrap();
        assert!(pool.decompress(b"invalid frame", MIN).is_err());
        assert!(pool.decoders.lock().unwrap().is_empty());
        assert!(pool.decompress(&encoded, MIN - 1).is_err());
        assert!(pool.decoders.lock().unwrap().is_empty());
        assert!(pool.decompress(&encoded, 0).is_err());
        assert!(pool.decompress(&encoded, MAX + 1).is_err());
        assert_eq!(pool.decompress(&encoded, MIN).unwrap(), held);
        assert_eq!(held, vec![0; MIN]);
        assert_eq!(pool.idle.lock().unwrap().len(), 1);
        assert_eq!(pool.decoders.lock().unwrap().len(), 1);
        let no_idle = Pool::new(
            Config {
                strategy: Strategy::Sample,
                context_idle_timeout: "0s".into(),
                ..Default::default()
            },
            1,
        )
        .unwrap();
        assert!(no_idle.compress(&[], true).is_err());
        assert!(no_idle.compress(&vec![0; MIN], true).unwrap().is_some());
        assert!(no_idle.idle.lock().unwrap().is_empty());
        assert_eq!(no_idle.decompress(&encoded, MIN).unwrap(), held);
        assert!(no_idle.decoders.lock().unwrap().is_empty());
    }
}
