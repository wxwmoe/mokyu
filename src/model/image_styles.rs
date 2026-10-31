use crate::{
    authorization::Principal,
    processing::image::{Format, InputLimits, MAX_SOURCE_PIXELS, Pipeline},
};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use utoipa::ToSchema;
use uuid::Uuid;

pub(crate) const PAGE: usize = 128;
#[derive(Clone, Debug)]
pub(crate) struct InvalidImageInput(pub String);
impl std::fmt::Display for InvalidImageInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for InvalidImageInput {}
pub(crate) fn validation_error(error: anyhow::Error) -> anyhow::Error {
    if error
        .downcast_ref::<s3s::S3Error>()
        .is_some_and(|e| e.code() == &s3s::S3ErrorCode::InvalidArgument)
    {
        let detail = InvalidImageInput(error.to_string().chars().take(2048).collect());
        error.context(detail)
    } else {
        error
    }
}
pub(crate) fn name(value: &str) -> Result<()> {
    ensure!(
        (1..=128).contains(&value.len())
            && value != "management-preview"
            && value.as_bytes()[0].is_ascii_alphanumeric()
            && value
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
            && !value.ends_with('.')
            && !value.contains(".."),
        "invalid image style name"
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as=ImageMapping)]
pub(crate) struct Mapping {
    pub auto: String,
    pub formats: BTreeMap<String, String>,
}
impl Mapping {
    pub fn family<'a>(name: &'a str, tag: &str) -> Option<&'a str> {
        name.strip_suffix(tag)
            .filter(|s| s.is_empty() || s.ends_with('.'))
    }
    pub fn validate(&self) -> Result<()> {
        name(&self.auto)?;
        ensure!(
            self.formats.contains_key("jpeg"),
            "JPEG mapping is required for automatic fallback"
        );
        ensure!(
            (1..=5).contains(&self.formats.len()),
            "choose at least one fixed format"
        );
        let mut tags = vec![&self.auto];
        for (key, tag) in &self.formats {
            ensure!(
                matches!(key.as_str(), "jpeg" | "png" | "jxl" | "webp" | "avif"),
                "invalid fixed format"
            );
            name(tag)?;
            ensure!(
                tags.iter()
                    .all(|t| Self::family(t, tag).is_none() && Self::family(tag, t).is_none()),
                "overlapping image mapping names"
            );
            tags.push(tag);
        }
        Ok(())
    }
    pub fn related<'a>(&'a self, name: &'a str) -> Option<&'a str> {
        std::iter::once(&self.auto)
            .chain(self.formats.values())
            .find_map(|tag| Self::family(name, tag))
    }
    pub fn members(&self, family: &str) -> impl Iterator<Item = (String, Format)> + '_ {
        let prefix = family.to_owned();
        std::iter::once((self.auto.as_str(), Format::Auto))
            .chain(
                self.formats
                    .iter()
                    .filter_map(|(k, t)| k.parse().ok().map(|f| (t.as_str(), f))),
            )
            .map(move |(t, f)| (format!("{prefix}{t}"), f))
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as=ImagePolicy)]
pub(crate) struct Policy {
    pub enabled: bool,
    pub protect_original: bool,
    pub input_limit: u64,
    #[serde(default = "default_source_pixels")]
    #[schema(default = 150000000, minimum = 1, maximum = 150000000)]
    pub max_pixels: u32,
    pub delimiters: Vec<String>,
    pub mapping: Option<Mapping>,
}
const fn default_source_pixels() -> u32 {
    MAX_SOURCE_PIXELS
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            enabled: false,
            protect_original: false,
            input_limit: 100_000_000,
            max_pixels: MAX_SOURCE_PIXELS,
            delimiters: vec!["!".into()],
            mapping: None,
        }
    }
}
impl Policy {
    pub fn input(&self) -> InputLimits {
        InputLimits {
            bytes: self.input_limit,
            pixels: self.max_pixels,
        }
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=MAX_SOURCE_PIXELS).contains(&self.max_pixels),
            "source pixel limit must be between 1 and 150000000 pixels"
        );
        ensure!(
            (1..=1_000_000_000).contains(&self.input_limit),
            "image input limit must be between 1 and 1000000000 bytes"
        );
        ensure!(
            self.delimiters.len() <= 4
                && self
                    .delimiters
                    .iter()
                    .all(|s| matches!(s.as_str(), "-" | "_" | "/" | "!"))
                && self.delimiters.iter().collect::<BTreeSet<_>>().len() == self.delimiters.len(),
            "invalid or repeated image delimiter"
        );
        if let Some(m) = &self.mapping {
            ensure!(
                !self.delimiters.is_empty(),
                "image mapping requires a path delimiter"
            );
            m.validate()?;
        }
        Ok(())
    }
}
#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[schema(as=ImageSettings)]
pub(crate) struct Settings {
    pub revision: i64,
    pub policy: Policy,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as=ImageStyle)]
pub(crate) struct Style {
    pub name: String,
    pub enabled: bool,
    pub rule: Pipeline,
}
#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
#[schema(as=ImageStyleEdit)]
pub(crate) enum Edit {
    Put { style: Style },
    Remove { name: String },
}
impl Edit {
    pub fn name(&self) -> &str {
        match self {
            Self::Put { style } => &style.name,
            Self::Remove { name } => name,
        }
    }
}
#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as=ImageUpdate)]
pub(crate) struct Update {
    pub revision: i64,
    pub policy: Option<Policy>,
    #[serde(default)]
    pub edits: Vec<Edit>,
}
impl Update {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.revision >= 0
                && self.edits.len() <= PAGE
                && (self.policy.is_some() || !self.edits.is_empty()),
            "invalid image update"
        );
        ensure!(
            self.policy.is_none() || self.edits.is_empty(),
            "save image settings and styles separately"
        );
        let mut names = BTreeSet::new();
        for edit in &self.edits {
            name(edit.name())?;
            ensure!(names.insert(edit.name()), "repeated style in update");
            if let Edit::Put { style } = edit {
                style.rule.validate()?;
                ensure!(
                    style.rule.text().len() <= Pipeline::MAX_BODY,
                    "canonical image rule too long"
                );
            }
        }
        if let Some(p) = &self.policy {
            p.validate()?;
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize, ToSchema)]
#[schema(as=ImageStylePage)]
pub(crate) struct Page {
    pub settings: Settings,
    pub styles: Vec<Style>,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, ToSchema, clap::ValueEnum, Default)]
#[serde(rename_all = "snake_case")]
#[schema(as=StyleConflict)]
pub enum Conflict {
    #[default]
    #[serde(rename = "error")]
    #[value(name = "error")]
    Reject,
    Skip,
    Replace,
}
#[derive(Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as=StyleImport)]
pub(crate) struct Import {
    pub text: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub conflict: Conflict,
    pub selected: Option<Vec<u16>>,
    pub revision: Option<i64>,
    #[serde(default)]
    pub apply: bool,
}
#[derive(Serialize, Deserialize, ToSchema)]
#[schema(as=StyleImportRow)]
pub(crate) struct Line {
    pub line: u16,
    pub syntax: String,
    pub name: String,
    pub body: Option<String>,
    pub outcome: String,
    pub error: Option<String>,
}
#[derive(Serialize, Deserialize, ToSchema)]
#[schema(as=StyleImportResult)]
pub(crate) struct Imported {
    pub revision: i64,
    pub applied: bool,
    pub rows: Vec<Line>,
}
pub(crate) fn parse_line(text: &str, prefix: &str) -> Result<Style> {
    let (left, right) = text
        .trim()
        .split_once(',')
        .ok_or_else(|| anyhow::anyhow!("expected styleName and styleBody"))?;
    fn field<'a>(part: &'a str, key: &str) -> Result<&'a str> {
        let (label, value) = part
            .trim()
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("missing {key}"))?;
        ensure!(label.trim() == key, "expected {key}");
        Ok(value.trim())
    }
    let n = format!("{prefix}{}", field(left, "styleName")?);
    name(&n)?;
    let rule = Pipeline::parse(field(right, "styleBody")?)?;
    ensure!(
        rule.text().len() <= Pipeline::MAX_BODY,
        "canonical image rule too long"
    );
    Ok(Style {
        name: n,
        enabled: true,
        rule,
    })
}
pub(crate) fn export(rows: &[Style]) -> String {
    use std::fmt::Write;
    let mut text = String::new();
    for s in rows {
        writeln!(text, "styleName: {}, styleBody: {}", s.name, s.rule.text()).unwrap();
    }
    text
}
#[async_trait]
pub(crate) trait ImageStore: Send + Sync {
    async fn resolve(
        &self,
        bucket: Uuid,
        key: &str,
        request: &crate::processing::image::request::Request,
    ) -> Result<crate::processing::image::request::Selection>;
    async fn page(
        &self,
        principal: &Principal,
        bucket: Uuid,
        after: &str,
        prefix: &str,
        limit: u16,
    ) -> Result<Page>;
    async fn selected(&self, principal: &Principal, bucket: Uuid, names: &[String])
    -> Result<Page>;
    async fn update(
        &self,
        principal: &Principal,
        bucket: Uuid,
        update: &Update,
        import: Option<&str>,
        preview: bool,
    ) -> Result<Settings>;
    async fn imported(
        &self,
        principal: &Principal,
        bucket: Uuid,
        digest: &str,
    ) -> Result<Option<i64>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn styles_preserve_imported_operations_and_native_exports() {
        let styles = include_str!("../../tests/fixtures/image-styles.txt")
            .lines()
            .filter(|s| !s.trim().is_empty())
            .map(|s| parse_line(s, ""))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(styles.len(), 50);
        let native = export(&styles);
        let again = native
            .lines()
            .map(|s| parse_line(s, ""))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(styles, again);
        assert!(native.lines().all(|s| s.contains("styleBody: mokyu/v1/")));
        assert!(parse_line("styleName: bad/name, styleBody: image/format,jpg", "").is_err());
    }
    #[test]
    fn mapping_uses_complete_suffixes_and_distinct_formats() {
        let mut mapping = Mapping {
            auto: "Auto.Redirect".into(),
            formats: BTreeMap::from([("jpeg".into(), "Default.format".into())]),
        };
        mapping.validate().unwrap();
        let mut missing = mapping.clone();
        missing.formats = BTreeMap::from([("webp".into(), "Web.format".into())]);
        assert!(missing.validate().is_err());
        assert_eq!(
            Mapping::family("Image.Auto.Redirect", &mapping.auto),
            Some("Image.")
        );
        assert_eq!(Mapping::family("ImageAuto.Redirect", &mapping.auto), None);
        assert_eq!(
            mapping.members("Image.").collect::<Vec<_>>(),
            vec![
                ("Image.Auto.Redirect".into(), Format::Auto),
                ("Image.Default.format".into(), Format::Jpeg)
            ]
        );
        mapping.formats.insert("jxl".into(), "Redirect".into());
        assert!(mapping.validate().is_err());
        Policy {
            enabled: false,
            protect_original: true,
            ..Policy::default()
        }
        .validate()
        .unwrap();
    }
}
