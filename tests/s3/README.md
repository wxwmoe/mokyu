# S3 and public HTTP tests

[Test guide](../README.md) · Run commands from the repository root. Short test paths such as `fixtures/` are relative to `tests/`.

## HTTP and recovery fixtures

- `integration.py`: S3 PUT, conditions, checksums, ranges and multipart.
- `s3/publication.py`: keep boto3's default checksum handling; exercise file streams, transfer-manager multipart and concurrent PUT/If-None-Match on one key. Body, ETag, length, media type and writer metadata must belong to one generation. Default: 20 rounds with eight writers. Set `MOKYU_TEST_ENDPOINTS` to a JSON array of data endpoints or use `MOKYU_TEST_ENDPOINT`. Count SlowDown separately from successful requests.
- `s3/connections.py`: with Expect, check connection handling on early rejection with no body, partial body and complete body. The first two close; the last may close safely or be reused. Bodyless requests allow reuse with an intact next response. Run 20 of each over a direct isolated HTTP/1.1 port.
- `s3/protocols.py`: all three listeners accept HTTP/1.1 and h2c. Check escaped signed paths, HEAD/ranges, transfers larger than the flow-control window, concurrency, cancellation, early rejection and a slow preface. Install `h2` and set `MOKYU_TEST_PUBLIC`, `MOKYU_TEST_PUBLIC_HOST` (mapped to the bucket) and `MOKYU_TEST_WEB` alongside S3 credentials. Run `python -m tests.s3.protocols`.

- `s3/empty_ranges.py`: signed/anonymous S3, public Web and panel downloads of empty and one-byte objects; suffix/open/closed ranges, HEAD and conditions. A normal GET of an empty object succeeds; an unsatisfiable range returns 416 and `Content-Range: bytes */0`. Keep SDK checksum defaults and test empty copies. Supply `MOKYU_TEST_PASSWORD_FILE` for the panel and `MOKYU_TEST_PUBLIC_WEB`/`MOKYU_TEST_PUBLIC_HOST` for public Web.

- `s3/multipart_admission.py`: use the isolated PostgreSQL integration fixture. Exercise separate completion admission, 503 at capacity, continued foreground reads/writes, release after validation failure and completion after disconnect.

- `s3/read_path.py`: paged SQL and body-free HEAD/304 even when background cache fails or read permits are exhausted. Add `MOKYU_TEST_PUBLIC_WEB` and `MOKYU_TEST_PUBLIC_HOST` to exercise public Web.

- `s3/http_compression.py`: wire bytes on public Web/anonymous S3, signed requests excluded, HEAD/304/ranges, estimates and learning. Use the same decoder dependencies. Optional PostgreSQL DSN enables JSON inspection. `postgres_compression_contract`, `mysql_content_contract` and `sqlite_content_contract` cover merging observations, policy changes and repeats; migration recovery also checks persisted columns.
- `s3/response_cache.py`: enable full-body cache; test reuse across entries/encodings, actual size, ACL, replace/delete and bad mappings. On PostgreSQL enable `pg_stat_statements` to verify hot hits write no source lease. Delete after snapshot with fault injection to check retained bytes. In cluster mode `MOKYU_TEST_STATUS_URL` must target the tested data node; `s3/read_path.py` also supports this. `s3/response_cache_pressure.py` uses `4MiB` and fault injection to test cancellation, late builds, slow responses holding evicted memory and continued uploads.
- `recovery/public_read_cache.py`: own a stopped initialized fault-injection service. Check hard expiry, four route aliases, hot reads without SQL, exclusion of signed requests and immediate local revocation. Unrelated PUT/Copy/Complete/ACL/delete must preserve hot entries and CORS links; retained snapshots refresh and late fills or failed conditional writes cannot restore old objects.
