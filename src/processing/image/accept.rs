use super::Format;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Range {
    Sdr,
    Pq,
    Hlg,
    GainMap,
    Unknown,
}
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub(crate) struct Source {
    pub width: u32,
    pub height: u32,
    pub format: String,
    pub frames: u32,
    pub alpha: bool,
    pub range: Range,
}
fn weight(s: &str) -> Option<u16> {
    let (a, b) = s.split_once('.').unwrap_or((s, ""));
    if b.len() > 3 || !b.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    match a {
        "1" if b.bytes().all(|v| v == b'0') => Some(1000),
        "0" => Some(
            b.bytes()
                .zip([100, 10, 1])
                .map(|(n, m)| u16::from(n - b'0') * m)
                .sum(),
        ),
        _ => None,
    }
}
fn preference(accept: &str, format: Format) -> Option<u16> {
    let mut best: Option<(u8, u16)> = None;
    for entry in accept.split(',') {
        let mut pieces = entry.split(';').map(str::trim);
        let Some((ty, sub)) = pieces.next().and_then(|v| v.split_once('/')) else {
            continue;
        };
        let specificity =
            if ty.eq_ignore_ascii_case("image") && sub.eq_ignore_ascii_case(format.word()) {
                2
            } else if ty.eq_ignore_ascii_case("image") && sub == "*" {
                1
            } else if ty == "*" && sub == "*" {
                0
            } else {
                continue;
            };
        let mut q = None;
        let mut valid = true;
        for piece in pieces {
            let Some((key, value)) = piece.split_once('=') else {
                valid = false;
                break;
            };
            if key.trim().eq_ignore_ascii_case("q") && q.is_none() {
                q = weight(value.trim());
                if q.is_none() {
                    valid = false;
                    break;
                }
            } else {
                valid = false;
                break;
            }
        }
        if valid && (specificity == 2 || format == Format::Jpeg) {
            let q = q.unwrap_or(1000);
            best = match best {
                Some((s, previous)) if s == specificity => Some((
                    s,
                    if previous == 0 || q == 0 {
                        0
                    } else {
                        previous.max(q)
                    },
                )),
                Some((s, _)) if s > specificity => best,
                _ => Some((specificity, q)),
            };
        }
    }
    best.map(|b| b.1)
}
pub(crate) fn negotiate(
    accept: Option<&str>,
    source: &Source,
    allowed: &[Format],
) -> Option<Format> {
    let accept = accept.unwrap_or("*/*");
    if accept.len() > 8192 || source.frames != 1 {
        return None;
    }
    let order: &[Format] = match (source.range, source.alpha) {
        (Range::Sdr, false) => &[Format::Jxl, Format::Webp, Format::Jpeg],
        (Range::Sdr, true) => &[Format::Jxl, Format::Webp, Format::Png, Format::Jpeg],
        (Range::Pq | Range::Hlg, _) => &[Format::Jxl, Format::Avif, Format::Jpeg],
        (Range::GainMap | Range::Unknown, _) => return None,
    };
    let mut selected = None;
    let mut fallback = None;
    let mut quality = 0;
    for &f in order {
        if allowed.contains(&f) {
            let preference = preference(accept, f);
            if f == Format::Jpeg && preference.is_none() {
                fallback = Some(f);
            }
            let q = preference.unwrap_or(0);
            if q > quality {
                selected = Some(f);
                quality = q;
            }
        }
    }
    selected.or(fallback)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explicit_support_and_source_properties() {
        let mut s = Source {
            width: 20,
            height: 20,
            format: "jpeg".into(),
            frames: 1,
            alpha: false,
            range: Range::Sdr,
        };
        let all = &[
            Format::Jxl,
            Format::Webp,
            Format::Jpeg,
            Format::Avif,
            Format::Png,
        ];
        assert_eq!(
            negotiate(Some("image/avif,image/webp,image/jxl,*/*;q=0.8"), &s, all),
            Some(Format::Jxl)
        );
        assert_eq!(
            negotiate(Some("image/jxl;q=0.1,image/webp;q=0.9"), &s, all),
            Some(Format::Webp)
        );
        assert_eq!(
            negotiate(Some("image/jpeg;q=0,image/*,image/webp;q=0"), &s, all),
            None
        );
        for header in [
            None,
            Some(""),
            Some("*/*"),
            Some("image/*"),
            Some("image/avif,image/heic"),
            Some("image/unknown"),
        ] {
            assert_eq!(negotiate(header, &s, all), Some(Format::Jpeg));
        }
        for header in [
            "image/unknown,image/jpeg;q=0",
            "image/jpeg;q=0,image/jpeg",
            "image/unknown,image/*;q=0",
            "*/*;q=0",
        ] {
            assert_eq!(negotiate(Some(header), &s, all), None);
        }
        assert_eq!(negotiate(Some("image/unknown"), &s, &[Format::Webp]), None);
        let many = format!("*/*,{}image/jpeg;q=0", "text/plain,".repeat(128));
        assert_eq!(negotiate(Some(&many), &s, all), None);
        s.alpha = true;
        for header in [None, Some("image/*"), Some("image/unknown")] {
            assert_eq!(negotiate(header, &s, all), Some(Format::Jpeg));
        }
        assert_eq!(negotiate(Some("image/png"), &s, all), Some(Format::Png));
        assert_eq!(negotiate(Some("image/jpeg;q=0"), &s, all), None);
        s.range = Range::Pq;
        assert_eq!(
            negotiate(Some("image/webp,image/avif"), &s, all),
            Some(Format::Avif)
        );
        for r in [Range::Pq, Range::Hlg] {
            s.range = r;
            assert_eq!(negotiate(None, &s, all), Some(Format::Jpeg));
            assert_eq!(
                negotiate(Some("image/jpeg,*/*"), &s, all),
                Some(Format::Jpeg)
            );
            assert_eq!(negotiate(Some("image/webp,image/jpeg;q=0"), &s, all), None);
        }
        s.range = Range::GainMap;
        assert_eq!(negotiate(Some("image/jxl"), &s, all), None);
    }
}
