use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Write, str::FromStr};
use utoipa::ToSchema;

macro_rules! values {
    ($name:ident, $($item:ident => $word:literal),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
        pub(crate) enum $name { $(#[serde(rename=$word)] $item),+ }
        impl $name {
            pub fn word(self) -> &'static str { match self { $(Self::$item => $word),+ } }
        }
        impl FromStr for $name {
            type Err = anyhow::Error;
            fn from_str(value: &str) -> Result<Self> {
                match value { $($word => Ok(Self::$item)),+, _ => bail!("unsupported {}: {value}",stringify!($name)) }
            }
        }
    };
}
values!(Format, Source=>"source", Jpeg=>"jpeg", Png=>"png", Webp=>"webp", Jxl=>"jxl", Avif=>"avif", Auto=>"auto");
values!(Fit, Contain=>"contain", Cover=>"cover");
values!(Anchor, Center=>"center", North=>"north", South=>"south", East=>"east", West=>"west", Northwest=>"northwest", Northeast=>"northeast", Southwest=>"southwest", Southeast=>"southeast");
values!(QualityMode, Exact=>"exact", Cap=>"cap", Relative=>"relative", Source=>"source");

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Operation {
    Orient,
    Frame {
        index: u16,
    },
    Resize {
        mode: Fit,
        width: Option<u32>,
        height: Option<u32>,
        upscale: bool,
    },
    Crop {
        width: u32,
        height: u32,
        anchor: Anchor,
    },
    Background {
        color: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct Stage {
    pub operations: Vec<Operation>,
    pub format: Format,
    pub quality: Option<u8>,
    pub quality_mode: QualityMode,
    pub progressive: bool,
}
impl Default for Stage {
    fn default() -> Self {
        Self {
            operations: vec![],
            format: Format::Source,
            quality: None,
            quality_mode: QualityMode::Exact,
            progressive: false,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct Pipeline {
    pub stages: Vec<Stage>,
}

type Params<'a> = BTreeMap<&'a str, &'a str>;
fn required<'a>(p: &mut Params<'a>, key: &str) -> Result<&'a str> {
    p.remove(key)
        .ok_or_else(|| anyhow::anyhow!("missing image parameter: {key}"))
}
fn native(body: &str) -> Result<Stage> {
    let mut stage = Stage::default();
    let mut encoded = false;
    for token in body.split('/') {
        ensure!(
            !encoded,
            "follow an encode with a pipe before adding more operations"
        );
        let mut words = token.split(',');
        let op = words.next().unwrap_or_default();
        let mut p = Params::new();
        for word in words {
            let (k, v) = word
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("invalid image parameter"))?;
            ensure!(
                !k.is_empty() && !v.is_empty() && p.insert(k, v).is_none(),
                "empty or repeated image parameter: {k}"
            );
        }
        match op {
            "orient" => stage.operations.push(Operation::Orient),
            "frame" => stage.operations.push(Operation::Frame {
                index: required(&mut p, "index")?.parse()?,
            }),
            "resize" => stage.operations.push(Operation::Resize {
                mode: p.remove("mode").unwrap_or("contain").parse()?,
                width: p.remove("width").map(str::parse).transpose()?,
                height: p.remove("height").map(str::parse).transpose()?,
                upscale: p.remove("upscale").unwrap_or("false").parse()?,
            }),
            "crop" => stage.operations.push(Operation::Crop {
                width: required(&mut p, "width")?.parse()?,
                height: required(&mut p, "height")?.parse()?,
                anchor: p.remove("anchor").unwrap_or("center").parse()?,
            }),
            "background" => stage.operations.push(Operation::Background {
                color: required(&mut p, "color")?.to_ascii_lowercase(),
            }),
            "quality" => {
                ensure!(
                    stage.quality.is_none() && stage.quality_mode == QualityMode::Exact,
                    "repeated image quality"
                );
                stage.quality_mode = p.remove("mode").unwrap_or("exact").parse()?;
                if stage.quality_mode != QualityMode::Source {
                    stage.quality = Some(required(&mut p, "value")?.parse()?)
                }
            }
            "encode" => {
                stage.format = required(&mut p, "format")?.parse()?;
                encoded = true;
                if let Some(q) = p.remove("quality") {
                    ensure!(
                        stage.quality.is_none() && stage.quality_mode == QualityMode::Exact,
                        "repeated image quality"
                    );
                    stage.quality = Some(q.parse()?);
                }
                stage.progressive = p.remove("progressive").unwrap_or("false").parse()?;
            }
            _ => bail!("unsupported image operation: {op}"),
        }
        ensure!(p.is_empty(), "unknown parameter in image operation {op}");
    }
    Ok(stage)
}
impl Pipeline {
    pub const MAX_DIM: u32 = 32768;
    pub const MAX_BODY: usize = 8192;
    pub fn parse(body: &str) -> Result<Self> {
        ensure!(
            !body.is_empty()
                && body.len() <= Self::MAX_BODY
                && body.is_ascii()
                && !body.bytes().any(|c| c <= 32 || c == 127),
            "invalid image rule text"
        );
        let stages = body
            .split('|')
            .map(|s| {
                if let Some(body) = s.strip_prefix("mokyu/v1/") {
                    native(body)
                } else {
                    super::compat::stage(s)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let rule = Self { stages };
        rule.validate()?;
        Ok(rule)
    }
    pub fn parse_native(body: &str) -> Result<Self> {
        ensure!(
            body.split('|').all(|s| s.starts_with("mokyu/v1/")),
            "public image parameters require a native rule"
        );
        Self::parse(body)
    }
    pub fn select(
        &self,
        src: &super::Source,
        accept: Option<&str>,
        allowed: &[Format],
    ) -> Result<Format> {
        let mut src = src.clone();
        for stage in &self.stages {
            for op in &stage.operations {
                match op {
                    Operation::Background { .. } => src.alpha = false,
                    Operation::Frame { index } => {
                        ensure!(u32::from(*index) < src.frames, "image frame is unavailable");
                        src.frames = 1;
                    }
                    _ => {}
                }
            }
            if stage.format == Format::Auto {
                return super::negotiate(accept, &src, allowed)
                    .ok_or_else(|| anyhow::anyhow!("no acceptable image format"));
            }
        }
        Ok(self.format())
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.stages.is_empty()
                && self.stages.len() <= 8
                && self
                    .stages
                    .iter()
                    .map(|s| s.operations.len() + 1)
                    .sum::<usize>()
                    <= 24,
            "image pipeline exceeds its operation budget"
        );
        let dim = |n: u32| (1..=Self::MAX_DIM).contains(&n);
        for (i, s) in self.stages.iter().enumerate() {
            ensure!(
                s.format != Format::Auto || i + 1 == self.stages.len(),
                "only the final image format can be auto"
            );
            ensure!(
                match s.quality_mode {
                    QualityMode::Source => s.quality.is_none(),
                    QualityMode::Exact => true,
                    _ => s.quality.is_some(),
                } && s.quality.is_none_or(|v| (1..=100).contains(&v)),
                "invalid image quality"
            );
            ensure!(
                !s.progressive || matches!(s.format, Format::Jpeg | Format::Source | Format::Auto),
                "progressive scans require JPEG"
            );
            for (j, op) in s.operations.iter().enumerate() {
                let ok = match op {
                    Operation::Resize {
                        mode,
                        width,
                        height,
                        ..
                    } => {
                        width.is_none_or(dim)
                            && height.is_none_or(dim)
                            && (width.is_some() || height.is_some())
                            && (*mode != Fit::Cover || width.is_some() && height.is_some())
                    }
                    Operation::Crop { width, height, .. } => dim(*width) && dim(*height),
                    Operation::Background { color } => {
                        color.len() == 6
                            && color
                                .bytes()
                                .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
                    }
                    Operation::Frame { index } => i == 0 && j == 0 && *index < 256,
                    Operation::Orient => true,
                };
                ensure!(ok, "invalid image operation parameters or placement");
            }
        }
        Ok(())
    }
    pub fn format(&self) -> Format {
        self.stages.last().map_or(Format::Source, |s| s.format)
    }
    pub fn text(&self) -> String {
        let mut out = String::new();
        for (i, s) in self.stages.iter().enumerate() {
            if i > 0 {
                out.push('|')
            }
            out.push_str("mokyu/v1/");
            for op in &s.operations {
                match op {
                    Operation::Orient => out.push_str("orient/"),
                    Operation::Frame { index } => {
                        let _ = write!(out, "frame,index={index}/");
                    }
                    Operation::Resize {
                        mode,
                        width,
                        height,
                        upscale,
                    } => {
                        let _ = write!(out, "resize,mode={}", mode.word());
                        if let Some(v) = width {
                            let _ = write!(out, ",width={v}");
                        }
                        if let Some(v) = height {
                            let _ = write!(out, ",height={v}");
                        }
                        let _ = write!(out, ",upscale={upscale}/");
                    }
                    Operation::Crop {
                        width,
                        height,
                        anchor,
                    } => {
                        let _ = write!(
                            out,
                            "crop,width={width},height={height},anchor={}/",
                            anchor.word()
                        );
                    }
                    Operation::Background { color } => {
                        let _ = write!(out, "background,color={color}/");
                    }
                }
            }
            if s.quality_mode == QualityMode::Source {
                out.push_str("quality,mode=source/");
            } else if let Some(q) = s.quality
                && s.quality_mode != QualityMode::Exact
            {
                let _ = write!(out, "quality,mode={},value={q}/", s.quality_mode.word());
            }
            let _ = write!(out, "encode,format={}", s.format.word());
            if let Some(q) = s.quality
                && s.quality_mode == QualityMode::Exact
            {
                let _ = write!(out, ",quality={q}");
            }
            if s.progressive {
                out.push_str(",progressive=true")
            }
        }
        out
    }
}
