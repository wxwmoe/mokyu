# Image processing tests

[Test guide](../README.md) · Run commands from the repository root. Short test paths such as `fixtures/` are relative to `tests/`.

## Inputs, styles and limits

`python -m tests.images.fixtures` requires Pillow and `MOKYU_TEST_IMAGE_ROOT` pointing to a new absolute directory. It validates all 50 rules in `fixtures/image-styles.txt` against their names and independent expected dimensions in `fixtures/image-style-sizes.tsv`. Resolution limits preserve aspect ratio without enlarging the source: `p` limits width, `i` limits height, and `x` limits both. It generates nine static inputs plus transparency, orientation, low-quality, animated and damaged samples. The manifest defines 450 base processing cases. Fixture validation alone does not mean those processing or import/export cases have passed.

For combined public-image and synchronous-replica recovery, call `tests.recovery.bucket_services.run` with independent source and replica fixtures, an already-cached public URL and its expected bytes, a control RPC address, and disposable network, database and readiness hooks. Use two controls and independently observe applied TCP delay and packet drops. The hooks must isolate the RPC path, automatically remove network faults and unpause the database on timeout; the readiness hook must restart terminated controls as a supervisor would. The test checks the cached image during delay, rejects stale reads after session fencing, and independently reads every acknowledged write from both backends after recovery. Set `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only` only for the disposable deployment.

With the packaged native image helper, NumPy, Pillow, avifenc and ultrahdr_app available, run `python3 tests/images/jpeg_background.py` to verify the default and explicit backgrounds on a transparent 16-bit PNG and a PQ image. The independent decoder checks opaque, half-transparent and fully transparent stripes against their expected luminance.

Call `tests.control.image_style_races.run` with an authenticated disposable fixture, image settings request/page helpers, the control URL, and a checks list after importing at least one style. It repeats simultaneous saves through independent HTTP connections, checks one committed revision and one conflict per pair, and verifies that only the winner persisted. Run it against every supported database with `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only`.

Use `tests.images.pixel_limits.run` with an isolated bucket fixture to verify pixel limits across frames, previews, redirects, cache hits and conditional reads. The native `large_source_downscale` case uses the image matrix fixtures to distinguish source size from the transform working budget.
