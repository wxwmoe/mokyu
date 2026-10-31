use super::*;
use std::path::PathBuf;

#[tokio::test]
#[ignore = "requires the installed image processor and isolated input and output directories"]
async fn large_source_downscale() {
    let inputs = PathBuf::from(std::env::var_os("MOKYU_TEST_IMAGE_INPUTS").unwrap());
    let dir =
        PathBuf::from(std::env::var_os("MOKYU_TEST_IMAGE_OUTPUTS").unwrap()).join("large-source");
    std::fs::create_dir(&dir).unwrap();
    let source = inputs.join("landscape.jpg");
    let mut limits = Limits::new(InputLimits::new(100_000_000), 768 * 1024 * 1024).unwrap();
    limits.pixels = 1024;
    let small = Pipeline::parse(
        "mokyu/v1/orient/resize,mode=contain,width=32,height=32,upscale=false/encode,format=jpeg",
    )
    .unwrap();
    let path = dir.join("small");
    let result = render(&source, &path, &small, limits).await.unwrap();
    assert!(result.source.width * result.source.height > limits.pixels);
    assert_eq!(result.image.width.max(result.image.height), 32);
    let full = Pipeline::parse("mokyu/v1/encode,format=jpeg").unwrap();
    let denied = dir.join("denied");
    assert!(render(&source, &denied, &full, limits).await.is_err());
    limits.input.pixels = result.source.width * result.source.height - 1;
    assert!(render(&source, &denied, &small, limits).await.is_err());
    assert!(!denied.exists());
}

#[tokio::test]
#[ignore = "requires the installed image processor and isolated input and output directories"]
async fn image_output_matrix() {
    let inputs = PathBuf::from(std::env::var_os("MOKYU_TEST_IMAGE_INPUTS").unwrap());
    let outputs = PathBuf::from(std::env::var_os("MOKYU_TEST_IMAGE_OUTPUTS").unwrap());
    std::fs::create_dir(&outputs).unwrap();
    let limit = Limits::new(InputLimits::new(100_000_000), 768 * 1024 * 1024).unwrap();
    let text = include_str!("../../../tests/fixtures/image-styles.txt");
    assert_eq!(text.lines().count(), 50);
    for (i, line) in text.lines().enumerate() {
        let (_, body) = line.split_once("styleBody:").unwrap();
        let pipeline = Pipeline::parse(body.trim()).unwrap();
        let dir = outputs.join(format!("S{:02}", i + 1));
        std::fs::create_dir(&dir).unwrap();
        for shape in ["landscape", "portrait", "small"] {
            for ext in ["jpg", "png", "webp"] {
                let name = format!("{shape}.{ext}");
                let out = render(
                    &inputs.join(&name),
                    &dir.join(format!("{name}.bin")),
                    &pipeline,
                    limit,
                )
                .await
                .unwrap_or_else(|e| panic!("S{:02} on {name}: {e:#}", i + 1));
                assert_eq!(out.output.format, pipeline.format());
                assert_eq!(out.image.format, out.output.format.word());
                assert!(out.output.bytes > 0);
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires isolated codec, colour, quality and budget fixtures"]
async fn image_boundary_cases() {
    use serde::Deserialize;
    #[derive(Deserialize)]
    struct Case {
        id: String,
        input: PathBuf,
        rule: String,
        accept: Option<String>,
        error: bool,
        memory: Option<u64>,
        pixels: Option<u32>,
        output: Option<u64>,
        timeout_ms: Option<u64>,
    }
    let file = PathBuf::from(std::env::var_os("MOKYU_TEST_IMAGE_CASES").unwrap());
    let cases: Vec<Case> = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
    let out = PathBuf::from(std::env::var_os("MOKYU_TEST_IMAGE_OUTPUTS").unwrap());
    std::fs::create_dir(&out).unwrap();
    let mut records = Vec::new();
    for case in cases {
        assert!(
            !case.id.is_empty()
                && case
                    .id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-')
        );
        let mut limit = Limits::new(
            InputLimits::new(100_000_000),
            case.memory.unwrap_or(768 * 1024 * 1024),
        )
        .unwrap();
        if let Some(n) = case.pixels {
            limit.pixels = n
        }
        if let Some(n) = case.output {
            limit.output = n
        }
        if let Some(n) = case.timeout_ms {
            limit.deadline = std::time::Duration::from_millis(n)
        }
        let result=async{
            let mut rule=Pipeline::parse(&case.rule)?;
            let src=probe(&case.input,limit).await?;
            if rule.format()==Format::Auto{
                let format=rule.select(&src,case.accept.as_deref(),&[Format::Jxl,Format::Avif,Format::Webp,Format::Png,Format::Jpeg])?;
                rule.stages.last_mut().unwrap().format=format;
            }
            let r=render(&case.input,&out.join(&case.id),&rule,limit).await?;
            assert_eq!(serde_json::to_value(&r.source)?, serde_json::to_value(&src)?);
            Ok::<_,anyhow::Error>(serde_json::json!({"source":src,"format":r.output.format,"bytes":r.output.bytes,"rule":rule.text()}))
        }.await;
        assert_eq!(result.is_err(), case.error, "{}: {result:?}", case.id);
        records.push(serde_json::json!({"id":case.id,"result":result.map_err(|e|e.to_string())}));
    }
    assert!(
        !out.read_dir()
            .unwrap()
            .flatten()
            .any(|f| f.file_name().to_string_lossy().starts_with(".work-"))
    );
    std::fs::write(
        out.join("results.json"),
        serde_json::to_vec(&records).unwrap(),
    )
    .unwrap();
}
