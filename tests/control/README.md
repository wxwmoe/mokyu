# Management API and Web tests

[Test guide](../README.md) · Run commands from the repository root. Short test paths such as `fixtures/` are relative to `tests/`.

## API and browser fixtures

- `control/database_capabilities.py`: management API/CLI, configuration and cluster capability boundaries.

- `web/web_assets.py`: embedded assets and public schema, three encodings, negotiation, ETags, HEAD and exclusion of sensitive responses. Install Python `brotli` and `zstandard` and run both direct and proxied.

Run `npm run test:ui` from `web` in a container with Playwright browsers. Supply isolated `MOKYU_TEST_WEB`, the state-change opt-in and the test user's password file. `frontend-auth.spec.ts` exercises session refresh and reopening the page.

`file-identities.spec.ts` uses the same browser login and an isolated PostgreSQL instance with a ready pack. Check UUID API responses, pack list/details, related files, wrapping of long IDs and overflow at 320–1440 px.

`http-compression.spec.ts` uses the real Chromium cache to check three encodings and identity, strong/weak ETags and intact bodies after 304. Optional `MOKYU_TEST_HTTP_OBJECT` must be a public compressible text URL. Trust the test CA for HTTPS; ignoring certificate errors can disable browser caching. All-200 responses do not demonstrate revalidation.

`upload-overflow.spec.ts` needs isolated S3 `MOKYU_TEST_BACKEND` and creates its own local backend. Check three locales, themes, keyboard, 320–1440 px, draft save/cancel and disabled local options. `recovery.raw_uploads` checks raw/chunk fallback/reject, 503 SlowDown, preservation after failed overwrite, policy after restart and invalid/omitted options.

`tests/recovery/io_presets.py` checks three server presets, read-only API/CLI previews, explicit save, revision conflict and unrelated settings using single-container or two-data fixtures. `web/tests/io-presets.spec.ts` checks three locales/themes, keyboard, narrow screens, preview cancellation and draft retention on conflict. `web/tests/storage-folds.spec.ts` covers visible presets, collapsed details, changed fields, draft/save state and opening invalid fields on client/server errors; `buffered-storage.spec.ts` covers manual options.

`python -m tests.control.media_previews` uses a running isolated service, one initialized storage, the `tester` account, and `MOKYU_TEST_BINARY`, `MOKYU_TEST_CONFIG`, `MOKYU_TEST_DATA`, `MOKYU_TEST_WEB`, `MOKYU_TEST_ENDPOINT`, `MOKYU_TEST_PASSWORD`, plus the isolated-state guard. Run the service as a non-root user so the failed cache-deletion case exercises actual filesystem permissions. Object versions may be UUIDs or native integer identities. Cache accounting is read from the CLI status contract.
