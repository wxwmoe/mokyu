use super::rule::*;
use anyhow::{Result, bail, ensure};

fn format(s: &str) -> Result<Format> {
    if s == "jpg" {
        Ok(Format::Jpeg)
    } else {
        s.parse()
    }
}
fn flag(s: &str) -> Result<bool> {
    match s {
        "1" => Ok(true),
        "0" => Ok(false),
        _ => bail!("expected 0 or 1"),
    }
}
fn quality(s: &mut Stage, q: &str, mode: QualityMode) -> Result<()> {
    ensure!(s.quality.is_none(), "repeated image quality");
    s.quality = Some(q.parse()?);
    s.quality_mode = mode;
    Ok(())
}
fn dimensions(s: &str) -> Result<(u32, u32)> {
    let (w, h) = s
        .split_once('x')
        .ok_or_else(|| anyhow::anyhow!("expected width x height"))?;
    Ok((w.parse()?, h.parse()?))
}
fn image(body: &str) -> Result<Stage> {
    let mut stage = Stage::default();
    let mut orientation = None;
    let mut interlace = None;
    for token in body.split('/') {
        let mut words = token.split(',');
        let name = words.next().unwrap_or_default();
        if name == "resize" {
            let (mut width, mut height, mut enlarge, mut mode) = (None, None, None, None);
            for word in words {
                let (k, v) = word
                    .split_once('_')
                    .ok_or_else(|| anyhow::anyhow!("invalid resize parameter"))?;
                match k {
                    "w" if width.is_none() => width = Some(v.parse()?),
                    "h" if height.is_none() => height = Some(v.parse()?),
                    "limit" if enlarge.is_none() => enlarge = Some(!flag(v)?),
                    "m" if mode.is_none() && v == "lfit" => mode = Some(Fit::Contain),
                    _ => bail!("unsupported or repeated resize parameter: {word}"),
                }
            }
            stage.operations.push(Operation::Resize {
                mode: Fit::Contain,
                width,
                height,
                upscale: enlarge.unwrap_or(false),
            });
            continue;
        }
        let value = words
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing image parameter: {name}"))?;
        ensure!(words.next().is_none(), "unexpected extra image parameter");
        match name {
            "format" if stage.format == Format::Source => {
                stage.format = format(value)?;
                ensure!(stage.format != Format::Source, "invalid imported format");
            }
            "interlace" if interlace.is_none() => interlace = Some(flag(value)?),
            "auto-orient" if orientation.is_none() => orientation = Some(flag(value)?),
            "quality" => {
                let (mode, q) = value
                    .split_once('_')
                    .ok_or_else(|| anyhow::anyhow!("invalid image quality"))?;
                quality(
                    &mut stage,
                    q,
                    match mode {
                        "Q" => QualityMode::Cap,
                        "q" => QualityMode::Relative,
                        _ => bail!("invalid image quality mode"),
                    },
                )?;
            }
            _ => bail!("unsupported or repeated image operation: {name}"),
        }
    }
    if orientation.unwrap_or(true) {
        stage.operations.insert(0, Operation::Orient)
    }
    if stage.quality.is_none() {
        stage.quality_mode = QualityMode::Source
    }
    stage.progressive = interlace.unwrap_or(false);
    if stage.format == Format::Jpeg {
        stage.operations.push(Operation::Background {
            color: "ffffff".into(),
        })
    }
    Ok(stage)
}
fn transform(body: &str) -> Result<Stage> {
    let mut stage = Stage::default();
    let mut words = body.split('/');
    let mut anchor = None;
    let mut crop = false;
    let mut interlace = None;
    while let Some(op) = words.next() {
        if op == "auto-orient" {
            stage.operations.push(Operation::Orient);
            continue;
        }
        let v = words
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing parameter: {op}"))?;
        match op {
            "crop" if !crop => {
                let (w, h) = dimensions(v)?;
                crop = true;
                stage.operations.push(Operation::Resize {
                    mode: Fit::Cover,
                    width: Some(w),
                    height: Some(h),
                    upscale: false,
                });
                stage.operations.push(Operation::Crop {
                    width: w,
                    height: h,
                    anchor: Anchor::Center,
                });
            }
            "gravity" if anchor.is_none() => anchor = Some(v.to_ascii_lowercase().parse()?),
            "thumbnail" => {
                let (w, h) = dimensions(v.strip_suffix('>').unwrap_or(v))?;
                stage.operations.push(Operation::Resize {
                    mode: Fit::Contain,
                    width: Some(w),
                    height: Some(h),
                    upscale: false,
                });
            }
            "format" if stage.format == Format::Source => {
                stage.format = format(v)?;
                ensure!(stage.format != Format::Source, "invalid imported format");
            }
            "quality" => quality(&mut stage, v, QualityMode::Exact)?,
            "interlace" if interlace.is_none() => interlace = Some(flag(v)?),
            _ => bail!("unsupported or repeated image operation: {op}"),
        }
    }
    stage.progressive = interlace.unwrap_or(false);
    if let Some(a) = anchor {
        ensure!(crop, "gravity needs a crop");
        for op in &mut stage.operations {
            if let Operation::Crop { anchor, .. } = op {
                *anchor = a
            }
        }
    }
    if stage.format == Format::Jpeg {
        stage.operations.push(Operation::Background {
            color: "ffffff".into(),
        })
    }
    Ok(stage)
}
pub(super) fn stage(s: &str) -> Result<Stage> {
    if let Some(body) = s.strip_prefix("image/") {
        return image(body);
    }
    if let Some(body) = s.strip_prefix("imageMogr2/") {
        return transform(body);
    }
    if let Some(body) = s.strip_prefix("imageView2/2/") {
        let mut words = body.split('/');
        let (mut stage, mut width, mut height) = (Stage::default(), None, None);
        while let Some(op) = words.next() {
            let v = words
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing thumbnail parameter"))?;
            match op {
                "w" if width.is_none() => width = Some(v.parse()?),
                "h" if height.is_none() => height = Some(v.parse()?),
                "format" if stage.format == Format::Source => stage.format = format(v)?,
                "q" => quality(&mut stage, v, QualityMode::Exact)?,
                _ => bail!("unsupported or repeated thumbnail parameter: {op}"),
            }
        }
        stage.operations.push(Operation::Resize {
            mode: Fit::Contain,
            width,
            height,
            upscale: false,
        });
        if stage.format == Format::Jpeg {
            stage.operations.push(Operation::Background {
                color: "ffffff".into(),
            })
        }
        return Ok(stage);
    }
    bail!("unknown image rule syntax or version")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn imported_styles_round_trip() {
        let source = include_str!("../../../tests/fixtures/image-styles.txt");
        let mut count = 0;
        for line in source.lines() {
            let (_, body) = line.split_once("styleBody:").unwrap();
            let p = Pipeline::parse(body.trim()).unwrap();
            let native = p.text();
            assert!(native.starts_with("mokyu/v1/"));
            assert_eq!(Pipeline::parse(&native).unwrap(), p);
            count += 1;
        }
        assert_eq!(count, 50);
        let crop = Pipeline::parse("imageMogr2/crop/400x400").unwrap();
        assert_eq!(
            crop,
            Pipeline::parse("imageMogr2/gravity/center/crop/400x400").unwrap()
        );
        assert!(matches!(
            crop.stages[0].operations[0],
            Operation::Resize {
                mode: Fit::Cover,
                upscale: false,
                ..
            }
        ));
        assert_eq!(Pipeline::parse(&crop.text()).unwrap(), crop);
        assert!(Pipeline::parse("image/resize,w_500,w_20/format,jpg").is_err());
        assert!(Pipeline::parse("mokyu/v1/encode,format=auto|image/format,jpg").is_err());
        assert!(Pipeline::parse("mokyu/v1/resize,width=0/encode,format=jpeg").is_err());
    }
}
