"""Check transparent 16-bit SDR and PQ sources using native tools and an independent HDR decoder."""
import json
import os
from pathlib import Path
import struct
import subprocess
import tempfile
import zlib

import numpy as np
from PIL import Image


def run(*args):
    return subprocess.run(list(map(str, args)), capture_output=True, check=True, timeout=45)


def source_png(path, level):
    def chunk(tag, payload):
        return struct.pack("!I", len(payload)) + tag + payload + struct.pack("!I", zlib.crc32(tag + payload))

    row = b"\0" + b"".join(struct.pack("!4H", level, level, level, alpha) * 32 for alpha in (0, 32768, 65535))
    header = struct.pack("!IIBBBBB", 96, 32, 16, 6, 0, 0, 0)
    path.write_bytes(b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", header) + chunk(b"IDAT", zlib.compress(row * 32)) + chunk(b"IEND", b""))


def check(root):
    native = os.environ.get("MOKYU_IMAGE_HELPER", "/opt/mokyu/bin/mokyu-image")

    def jpeg(source, name, *args):
        path = root / (name + ".jpg")
        facts, output = json.loads(run(native, "render", source, "1000000", "-", path, "jpeg", "exact", 95, 0, *args).stdout)
        assert facts["alpha"] and output["format"] == "jpeg" and not output["alpha"]
        return path

    png = root / "alpha.png"
    source_png(png, 0)
    white = jpeg(png, "white")
    assert white.read_bytes() == jpeg(png, "explicit-white", "background", "ffffff").read_bytes()
    with Image.open(white) as image:
        assert min(image.getpixel((12, 16))) >= 250 and max(image.getpixel((80, 16))) <= 5
    with Image.open(jpeg(png, "red", "background", "ff0000")) as image:
        red, green, blue = image.getpixel((12, 16))
        assert red >= 245 and max(green, blue) <= 10

    # The opaque stripe is 1,000 nits, the white background is SDR reference white (203 nits).
    m1, m2, c1, c2, c3 = 2610 / 16384, 2523 / 32, 3424 / 4096, 2413 / 128, 2392 / 128
    value = 0.1 ** m1
    source_png(png, round(65535 * ((c1 + c2 * value) / (1 + c3 * value)) ** m2))
    hdr = root / "pq.avif"
    run("avifenc", "-j", 1, "-d", 10, "-y", "444", "--cicp", "9/16/9", "--min", 0, "--max", 0, png, hdr)
    result = jpeg(hdr, "hdr")
    assert result.read_bytes() == jpeg(hdr, "hdr-white", "background", "ffffff").read_bytes()
    probe = run("ultrahdr_app", "-m", 1, "-j", result, "-P")
    assert b"Ultra HDR Image: Yes" in probe.stdout + probe.stderr
    raw = root / "hdr.raw"
    run("ultrahdr_app", "-m", 1, "-j", result, "-O", 5, "-o", 2, "-z", raw)
    words = np.fromfile(raw, dtype=np.uint32).reshape(32, 96)
    codes = np.stack([(words >> shift) & 1023 for shift in (0, 10, 20)], axis=-1) / 1023
    power = codes ** (1 / m2)
    nits = 10000 * (np.maximum(power - c1, 0) / (c2 - c3 * power)) ** (1 / m1)
    measured = [float(np.median(nits[8:24, x + 8:x + 24])) for x in (0, 32, 64)]
    for actual, expected in zip(measured, (203, 601.5, 1000)):
        assert abs(actual / expected - 1) <= 0.15, measured
    return measured


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        print(json.dumps({"sdr_white_and_override": True, "hdr_nits": check(Path(directory))}))
