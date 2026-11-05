use super::{Format, Pipeline};
use crate::model::image_styles::{Policy, Settings, Style};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Request {
    pub style: Option<String>,
    pub rule: Option<Pipeline>,
    pub format: Option<Format>,
    pub unsupported: bool,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Selection {
    pub settings: Settings,
    pub key: String,
    pub style: Option<Style>,
    pub delimiter: Option<String>,
    pub rule: Option<Pipeline>,
}
pub(crate) fn state(
    bucket: &crate::app::Bucket,
    stream: Option<&crate::app::StoredStream>,
    image: Option<&Selection>,
) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&(bucket, stream, image))?)
}
impl Request {
    pub fn query(&self) -> String {
        let mut query = self
            .style
            .as_ref()
            .map(|s| format!("style={s}"))
            .or_else(|| self.rule.as_ref().map(Pipeline::text))
            .unwrap_or_default();
        if let Some(format) = self.format {
            if !query.is_empty() {
                query.push('&');
            }
            query.push_str(&format!("format={}", format.word()));
        }
        if self.unsupported {
            if !query.is_empty() {
                query.push('&');
            }
            query.push_str("_unsupported");
        }
        query
    }
    pub fn parse(query: &str) -> Result<Self> {
        ensure!(
            query.len() <= Pipeline::MAX_BODY * 3,
            "image query exceeds bound"
        );
        let mut result = Self::default();
        let mut other = false;
        for part in query.split('&').filter(|s| !s.is_empty()) {
            let part = crate::http::decode_path(part)?;
            if part.starts_with("mokyu/") {
                ensure!(result.rule.is_none(), "repeated image pipeline");
                result.rule = Some(Pipeline::parse_native(&part)?);
            } else {
                let (name, value) = part.split_once('=').unwrap_or((&part, ""));
                match name {
                    name if name
                        .strip_prefix("response-")
                        .is_some_and(|field| crate::rpc::read::OVERRIDES.contains(&field)) =>
                    {
                        return Err(s3s::s3_error!(AccessDenied).into());
                    }
                    "style" => {
                        ensure!(result.style.is_none(), "repeated image style");
                        crate::model::image_styles::name(value)?;
                        result.style = Some(value.into());
                    }
                    "format" => {
                        ensure!(result.format.is_none(), "repeated image format");
                        let format = value.parse()?;
                        ensure!(
                            !matches!(format, Format::Auto | Format::Source),
                            "choose a fixed image format"
                        );
                        result.format = Some(format);
                    }
                    "x-id" if matches!(value, "GetObject" | "HeadObject") => {}
                    _ => other = true,
                }
            }
        }
        ensure!(
            !(result.style.is_some() && result.rule.is_some()),
            "choose a style or a pipeline"
        );
        ensure!(
            !result.explicit() || !other,
            "unsupported image query parameter"
        );
        result.unsupported = other;
        Ok(result)
    }
    pub fn explicit(&self) -> bool {
        self.style.is_some() || self.rule.is_some() || self.format.is_some()
    }
    pub fn candidates(&self, key: &str, policy: &Policy) -> Vec<(String, String, String)> {
        if let Some(style) = &self.style {
            return vec![(key.into(), style.clone(), String::new())];
        }
        if self.rule.is_some() || !policy.enabled {
            return Vec::new();
        }
        key.char_indices()
            .filter_map(|(index, ch)| {
                let delimiter = ch.to_string();
                let tail = &key[index + ch.len_utf8()..];
                (index > 0
                    && policy.delimiters.contains(&delimiter)
                    && crate::model::image_styles::name(tail).is_ok())
                .then(|| (key[..index].to_owned(), tail.to_owned(), delimiter))
            })
            .collect()
    }
    pub fn select(
        &self,
        key: &str,
        settings: Settings,
        styles: &std::collections::BTreeMap<String, Style>,
    ) -> Result<Selection> {
        let selected = self.candidates(key, &settings.policy).into_iter().find_map(
            |(key, name, delimiter)| {
                styles
                    .get(&name)
                    .filter(|s| s.enabled)
                    .map(|s| (key, s.clone(), (!delimiter.is_empty()).then_some(delimiter)))
            },
        );
        ensure!(
            self.style.is_none() || selected.is_some(),
            s3s::s3_error!(InvalidArgument, "image style is unavailable")
        );
        let (key, style, delimiter) = selected.map_or_else(
            || (key.to_owned(), None, None),
            |(key, style, delimiter)| (key, Some(style), delimiter),
        );
        let mut rule = style.as_ref().map(|s| s.rule.clone()).or(self.rule.clone());
        ensure!(
            !self.explicit() || rule.is_some(),
            s3s::s3_error!(InvalidArgument, "image format requires a style or pipeline")
        );
        if let Some(rule) = &mut rule {
            ensure!(
                !self.unsupported,
                s3s::s3_error!(InvalidArgument, "unsupported image query parameter")
            );
            ensure!(settings.policy.enabled, s3s::s3_error!(AccessDenied));
            ensure!(
                !settings.policy.protect_original || style.is_some(),
                s3s::s3_error!(AccessDenied)
            );
            if let Some(format) = self.format {
                ensure!(
                    rule.format() == Format::Auto,
                    s3s::s3_error!(InvalidArgument, "only auto styles accept a format override")
                );
                ensure!(
                    delimiter.is_none()
                        || settings.policy.mapping.as_ref().is_none_or(|m| style
                            .as_ref()
                            .is_none_or(|s| crate::model::image_styles::Mapping::family(
                                &s.name, &m.auto
                            )
                            .is_none())),
                    s3s::s3_error!(InvalidArgument, "mapped styles use fixed style names")
                );
                rule.stages.last_mut().unwrap().format = format;
            }
        }
        Ok(Selection {
            settings,
            key,
            style,
            delimiter,
            rule,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explicit_queries_and_longest_registered_suffix() {
        let rule = Pipeline::parse_native("mokyu/v1/encode,format=auto").unwrap();
        let policy = Policy {
            enabled: true,
            delimiters: vec!["!".into(), "-".into(), "_".into(), "/".into()],
            ..Default::default()
        };
        let settings = Settings {
            revision: 3,
            policy,
        };
        let styles = ["small", "foo-small"]
            .into_iter()
            .map(|name| {
                (
                    name.into(),
                    Style {
                        name: name.into(),
                        enabled: true,
                        rule: rule.clone(),
                    },
                )
            })
            .collect();
        let request = Request::parse("").unwrap();
        let selection = request
            .select("photo.jpg!foo-small", settings.clone(), &styles)
            .unwrap();
        assert_eq!(selection.key, "photo.jpg");
        assert_eq!(selection.style.unwrap().name, "foo-small");
        assert!(
            request
                .select("photo!missing", settings.clone(), &styles)
                .unwrap()
                .rule
                .is_none()
        );
        let explicit = Request::parse("style=small&format=jpeg")
            .unwrap()
            .select("key+%2F!x", settings, &styles)
            .unwrap();
        assert_eq!(explicit.key, "key+%2F!x");
        assert_eq!(explicit.rule.unwrap().format(), Format::Jpeg);
        for query in [
            "style=small&style=small",
            "style=x&format=auto",
            "style=x&width=2",
            "mokyu/v2/encode,format=jpeg",
            "style=x&mokyu/v1/encode,format=jpeg",
            "style=management-preview",
        ] {
            assert!(Request::parse(query).is_err(), "{query}");
        }
    }
}
