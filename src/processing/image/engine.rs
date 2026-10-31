use super::{Format, Operation, Pipeline, Source};
use anyhow::{Context, Result, bail, ensure};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::OnceLock,
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    process::Command,
    time::{Instant, timeout_at},
};
use uuid::Uuid;

const WORKER: &str = "/opt/mokyu/bin/mokyu-image";
pub(crate) fn engine_version() -> Result<&'static str> {
    static VERSION: OnceLock<String> = OnceLock::new();
    if let Some(v) = VERSION.get() {
        return Ok(v);
    }
    let v = std::process::Command::new(WORKER)
        .arg("version")
        .output()
        .context("image processor unavailable")?;
    ensure!(
        v.status.success() && (1..=256).contains(&v.stdout.len()),
        "invalid image processor version"
    );
    let text = String::from_utf8(v.stdout)?;
    ensure!(!text.trim().is_empty(), "empty image processor version");
    let mut identity = blake3::Hasher::new();
    identity.update(text.as_bytes());
    std::io::copy(&mut std::fs::File::open(WORKER)?, &mut identity)?;
    Ok(VERSION.get_or_init(|| identity.finalize().to_hex().to_string()))
}
pub(crate) const MAX_SOURCE_PIXELS: u32 = 150_000_000;
const WORK_PIXELS: u32 = 32_000_000;
#[derive(Clone, Copy)]
pub(crate) struct InputLimits {
    pub bytes: u64,
    pub pixels: u32,
}
impl InputLimits {
    pub const fn new(bytes: u64) -> Self {
        Self {
            bytes,
            pixels: WORK_PIXELS,
        }
    }
    pub fn check(&self, source: &Source) -> Result<()> {
        ensure!(
            source.width > 0
                && source.height > 0
                && source.frames > 0
                && u64::from(source.width) * u64::from(source.height)
                    <= u64::from(self.pixels) / u64::from(source.frames),
            "image exceeds its source pixel limit"
        );
        Ok(())
    }
}
#[derive(Clone, Copy)]
pub(crate) struct Limits {
    pub input: InputLimits,
    pub memory: u64,
    pub pixels: u32,
    pub output: u64,
    pub deadline: Duration,
}
impl Limits {
    pub fn new(input: InputLimits, memory: u64) -> Result<Self> {
        ensure!(
            input.bytes > 0 && memory >= 512 * 1024 * 1024,
            "insufficient image budget"
        );
        Ok(Self {
            input,
            memory,
            pixels: (memory / 24).min(u64::from(WORK_PIXELS)) as u32,
            output: (memory / 3).min(128 * 1024 * 1024),
            deadline: Duration::from_secs(30),
        })
    }
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct Output {
    pub format: Format,
    pub bytes: u64,
}
struct Work(PathBuf);
impl Drop for Work {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn command(limits: Limits) -> Command {
    let mut cmd = Command::new(WORKER);
    cmd.env("MOKYU_IMAGE_INPUT_PIXELS", limits.input.pixels.to_string())
        .env("VIPS_CONCURRENCY", "1")
        .env("VIPS_DISC_THRESHOLD", "4g")
        .env("OMP_NUM_THREADS", "1")
        .env("OPENBLAS_NUM_THREADS", "1")
        .env("MALLOC_ARENA_MAX", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    #[cfg(unix)]
    unsafe {
        cmd.pre_exec(move || {
            macro_rules! limit {
                ($key:expr,$val:expr) => {{
                    let val = $val as libc::rlim_t;
                    if libc::setrlimit(
                        $key,
                        &libc::rlimit {
                            rlim_cur: val,
                            rlim_max: val,
                        },
                    ) != 0
                    {
                        return Err(std::io::Error::last_os_error());
                    }
                }};
            }
            limit!(libc::RLIMIT_AS, limits.memory);
            limit!(libc::RLIMIT_FSIZE, limits.output);
            limit!(libc::RLIMIT_CPU, limits.deadline.as_secs().max(1));
            limit!(libc::RLIMIT_NOFILE, 96);
            Ok(())
        });
    }
    cmd
}
async fn finish(mut child: tokio::process::Child, deadline: Instant) -> Result<()> {
    match timeout_at(deadline, child.wait()).await {
        Ok(status) => ensure!(
            status?.success(),
            "unsupported image, invalid data or exhausted processing budget"
        ),
        Err(_) => {
            let _ = child.start_kill();
            child.wait().await?;
            bail!("image processing deadline exceeded")
        }
    }
    Ok(())
}
async fn check_input(path: &Path, limit: u64) -> Result<()> {
    let meta = tokio::fs::metadata(path).await?;
    ensure!(
        meta.is_file() && meta.len() > 0 && meta.len() <= limit,
        "image input exceeds its budget"
    );
    Ok(())
}
async fn report<T: serde::de::DeserializeOwned>(cmd: &mut Command, deadline: Instant) -> Result<T> {
    let mut child = cmd
        .stdout(Stdio::piped())
        .spawn()
        .context("image processor unavailable")?;
    let stdout = child.stdout.take().context("missing image report")?;
    let mut data = Vec::new();
    let mut reader = stdout.take(4097);
    let (_, ()) = tokio::try_join!(
        async {
            reader
                .read_to_end(&mut data)
                .await
                .map_err(anyhow::Error::from)
        },
        finish(child, deadline)
    )?;
    ensure!(data.len() <= 4096, "invalid image report");
    Ok(serde_json::from_slice(&data)?)
}
pub(crate) async fn probe(path: &Path, limits: Limits) -> Result<Source> {
    check_input(path, limits.input.bytes).await?;
    report(
        command(limits)
            .arg("probe")
            .arg(path)
            .arg(limits.pixels.to_string())
            .arg("-"),
        Instant::now() + limits.deadline,
    )
    .await
}
pub(crate) struct Rendered {
    pub source: Source,
    pub image: Source,
    pub output: Output,
}
pub(crate) async fn render(
    source: &Path,
    destination: &Path,
    pipeline: &Pipeline,
    limits: Limits,
) -> Result<Rendered> {
    pipeline.validate()?;
    let deadline = Instant::now() + limits.deadline;
    check_input(source, limits.input.bytes).await?;
    let dir = destination
        .parent()
        .context("image destination needs a directory")?
        .join(format!(".work-{}", Uuid::new_v4()));
    tokio::fs::create_dir(&dir).await?;
    let work = Work(dir);
    let mut path = source.to_owned();
    let mut rendered: Option<Rendered> = None;
    for (i, stage) in pipeline.stages.iter().enumerate() {
        ensure!(
            stage.format != Format::Auto,
            "resolve the output format before processing"
        );
        let page = stage.operations.iter().find_map(|op| {
            if let Operation::Frame { index } = op {
                Some(*index)
            } else {
                None
            }
        });
        let next = work.0.join(i.to_string());
        let mut cmd = command(if i == 0 {
            limits
        } else {
            Limits {
                input: InputLimits::new(limits.input.bytes),
                ..limits
            }
        });
        cmd.env("TMPDIR", &work.0)
            .arg("render")
            .arg(&path)
            .arg(limits.pixels.to_string())
            .arg(page.map_or_else(|| "-".into(), |v| v.to_string()))
            .arg(&next)
            .arg(stage.format.word())
            .arg(stage.quality_mode.word())
            .arg(stage.quality.unwrap_or_default().to_string())
            .arg(if stage.progressive { "1" } else { "0" });
        for op in &stage.operations {
            match op {
                Operation::Frame { .. } => {}
                Operation::Orient => {
                    cmd.arg("orient");
                }
                Operation::Resize {
                    mode,
                    width,
                    height,
                    upscale,
                } => {
                    cmd.arg("resize")
                        .arg(mode.word())
                        .arg(width.unwrap_or_default().to_string())
                        .arg(height.unwrap_or_default().to_string())
                        .arg(if *upscale { "1" } else { "0" });
                }
                Operation::Crop {
                    width,
                    height,
                    anchor,
                } => {
                    cmd.arg("crop")
                        .arg(width.to_string())
                        .arg(height.to_string())
                        .arg(anchor.word());
                }
                Operation::Background { color } => {
                    cmd.arg("background").arg(color);
                }
            }
        }
        let (input, image): (Source, Source) = report(&mut cmd, deadline).await?;
        let format = image.format.parse()?;
        let expected = if stage.format == Format::Source {
            input.format.parse()?
        } else {
            stage.format
        };
        ensure!(expected == format, "unexpected encoded image format");
        let bytes = tokio::fs::metadata(&next).await?.len();
        ensure!(
            bytes > 0 && bytes <= limits.output,
            "encoded image exceeds its budget"
        );
        rendered = Some(Rendered {
            source: rendered.map_or(input, |previous| previous.source),
            image,
            output: Output { format, bytes },
        });
        if i > 0 {
            tokio::fs::remove_file(path).await?
        }
        path = next;
    }
    tokio::fs::rename(path, destination).await?;
    rendered.context("empty image pipeline")
}
