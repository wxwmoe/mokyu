use std::{
    env, fs,
    hash::{DefaultHasher, Hash, Hasher},
    path::{Path, PathBuf},
};

fn main() {
    println!("cargo:rerun-if-changed=migrations");
    println!("cargo:rerun-if-env-changed=MOKYU_WEB_DIR");
    if env::var_os("CARGO_FEATURE_WEB_UI").is_none() {
        return;
    }
    let root = env::var_os("MOKYU_WEB_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("web/dist"));
    assert!(
        root.join("index.html").is_file(),
        "Build web assets with `cd web && npm ci && npm run build`, or use --no-default-features for an API-only binary"
    );
    println!("cargo:rerun-if-changed={}", root.display());
    let root = root.canonicalize().expect("web asset directory");
    let mut files = Vec::new();
    collect(&root, &mut files);
    files.sort();
    let mut source = String::from("pub(super) static ASSETS: &[Asset] = &[\n");
    for file in files {
        let path = format!(
            "/{}",
            file.strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/")
        );
        let mime = match file.extension().and_then(|s| s.to_str()).unwrap_or("") {
            "html" => "text/html; charset=utf-8",
            "js" => "text/javascript; charset=utf-8",
            "css" => "text/css; charset=utf-8",
            "txt" => "text/plain; charset=utf-8",
            "json" | "webmanifest" => "application/json",
            "svg" => "image/svg+xml",
            "png" => "image/png",
            "ico" => "image/x-icon",
            "woff2" => "font/woff2",
            "woff" => "font/woff",
            _ => "application/octet-stream",
        };
        let mut hash = DefaultHasher::new();
        fs::read(&file).unwrap().hash(&mut hash);
        let etag = format!("\"{:x}\"", hash.finish());
        let stem = file.file_stem().unwrap().to_string_lossy();
        let stem = stem.as_bytes();
        let immutable = path.starts_with("/assets/")
            && stem.len() > 9
            && stem[stem.len() - 9] == b'-'
            && stem[stem.len() - 8..]
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
            && matches!(
                file.extension().and_then(|s| s.to_str()),
                Some("js" | "css" | "woff" | "woff2")
            );
        source.push_str(&format!("Asset {{ path: {path:?}, mime: {mime:?}, etag: {etag:?}, immutable: {immutable}, data: include_bytes!({:?}) }},\n", file));
    }
    source.push_str("];");
    fs::write(
        PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("web_assets.rs"),
        source,
    )
    .unwrap();
}

fn collect(root: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).expect("read web assets") {
        let entry = entry.unwrap();
        if entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        let kind = entry.file_type().unwrap();
        if kind.is_dir() {
            collect(&entry.path(), files);
        } else if kind.is_file() {
            files.push(entry.path());
        }
    }
}
