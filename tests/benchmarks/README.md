# Benchmarks

[Test guide](../README.md) · Run commands from the repository root. Short test paths such as `fixtures/` are relative to `tests/`.

## Public reads

Run `python -m tests.benchmarks.public_reads /config/benchmark.json /results/reads.json` in an independent client container with the state-change opt-in. Record the revision of the service built with `cargo build --release --locked`. Debug and fault-injection builds are unsuitable for performance comparisons. Upload deterministic public samples and retain their bytes. The measured workload only reads; check status, wire encoding, Content-Length and decoded bytes on every request.

First run `python -m tests.benchmarks.public_reads_test` in a Linux cgroup v2 container. The local HTTP fixture checks actual bytes for all four encodings, HTTP/body failures, null percentiles when all fail and early exit on warmup failure. It does not measure Mokyu.

Example fixture:

```json
{
  "label": "cache-on-round-1",
  "revision": "tested-git-revision",
  "build": "release",
  "database_version": "PostgreSQL 18",
  "cache_state": "warm; response cache enabled; chunk cache enabled",
  "seed": 719,
  "requests": 1000,
  "concurrency": 4,
  "warmup": 4,
  "database_file": "/config/postgres-dsn",
  "resources": {
    "client": {"cgroup": "/sys/fs/cgroup"},
    "data": {"cgroup": "/metrics/data", "status_url": "http://data:9002/status"},
    "database": {"cgroup": "/metrics/database"}
  },
  "cases": [{
    "name": "text-1MiB-zstd",
    "urls": ["http://data:9000/test-bucket/text"],
    "expected_file": "/fixtures/text.bin",
    "encoding": "zstd"
  }]
}
```

Mount each participant's own cgroup v2 directory read-only into the client: include the client, data/control/worker, database and shared storage. Record actual CPU/memory limits, CPU and throttling deltas and sampled `memory.current` peak. Node snapshots include RSS, response cache, replica and storage request/byte counters. `memory.current` includes page cache; process peak RSS spans the process lifetime. Optional PostgreSQL `database_file` requires `pg_stat_statements` for primary pagination, snapshot and lease SQL deltas. Other backends report null, not zero.

`cases` can contain multiple objects/endpoints; rotate through at most 4096 `urls` by request number. Test a single hot object separately from a working set larger than cache. `encoding` accepts identity/gzip/br/zstd; install `brotli` and `zstandard` for the latter two. For ranges set `headers.Range`, `status: 206` and an expected_file containing the exact slice. `successful_wire_body_bytes` counts encoded bytes read, excluding headers/framing. Latency includes connection wait through body completion; TTFB ends at first body byte. RPS includes client decoding/validation, so check client CPU saturation.

Optional `seconds` is 1–600 seconds. Report actual completions, still bounded by `requests`. Each case's `method` accepts GET, HEAD or OPTIONS; `response_headers` lists required headers. HEAD/OPTIONS must return empty bodies. Resource output includes anonymous/file/dirty/writeback pages and per-device I/O deltas.

On Linux at concurrency one, list only fixture-owned `evict_files` files. Before each request, `posix_fadvise(DONTNEED)` evicts those pages and `mincore` proves there are no resident pages. Retry busy pages for at most one second, then fail. Never drop global caches. Eviction is outside request latency but inside the measurement window: do not compare this RPS directly with hot throughput. Only the file page cache is proven cold, not the controller, hypervisor or remote storage.

Failed warmup prevents the concurrency barrier from starting. Request/validation failures are recorded and exit nonzero; no successes means null percentiles. Start with a small check, then interleave off/on/on/off. Record external resets/restarts for cold cases and set warmup=0. Repeated reads of one object are not all cold. Report equal-budget and additional-budget comparisons separately; SQL counts, hit rates and small runs do not establish production capacity.

## Source writes

### Locks and connection pools

`tests/benchmarks/lock_tables.rs` compares per-scan and amortized cleanup with 1/64/4096 active keys, one/four threads, hits and key changes. Alternate three rounds and write CSV. Use the same toolchain's release Tokio artifacts inside the isolated build container:

```sh
deps="${CARGO_TARGET_DIR:-target}/release/deps"
tokio_rlib=$(ls -t "$deps"/libtokio-*.rlib | head -1)
rustc --edition=2024 -O tests/benchmarks/lock_tables.rs --extern "tokio=$tokio_rlib" -L "dependency=$deps" -o /tmp/mokyu-lock-tables
/tmp/mokyu-lock-tables
```

First run `cargo build --release --locked`. Ordinary Rust tests cover lock identity, cancellation and idle capacity reclamation. This microbenchmark measures lock table operations; use the object benchmarks for request throughput.

### Single-operation timing

`python -m tests.benchmarks.object_io /config/io.json /results/io.json` measures `put`, `get`, `head` or `multipart` on a release service with `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only`. Run `python -m tests.benchmarks.object_io_test` first. PUT latency stops at ACK; verify final objects with full GETs outside the timing/resource window. GET includes body validation and reports first byte separately. Multipart records Create, UploadPart, Complete and total lifecycle. Report actual async drain time and resources separately.

Record `revision`, `build`, `database_version`, `cache_state`, `seed`, `credentials_file` and `prefix`, optional `resources` or PostgreSQL `database_file`, then one `endpoint`, `operation`, `size`, `seconds`, `concurrency`, `max_operations` and `max_total_bytes`. Bounds: 0–64 MiB, 1–64 workers, 1–600 seconds, at most one million operations. `(max_operations + 1) × size` including warmup must fit the byte budget. Use a dedicated prefix per round; the script never deletes data. Report actual elapsed time when the operation cap ends a case.

For background writes set `raw_drain_seconds` to 1–600 and give `MOKYU_TEST_CONFIG` for the same database. Use storage dedicated to this run. After warmup and timing, wait for raw dedup, CDC, raw/chunk uploads, fsync and pack_inputs, including unreferenced sources; perform full readback after drain. Backlog counts are separate obligations, not a distinct-object sum. Report ACK-window and complete rates, times, CPU/I/O, sampled peak memory and backlog/drain histories. Errors/timeouts are not steady-state completions. Use identical drain sampling for both sides; it costs database work.

`hot_keys` defaults to zero (new key per request); 1–1024 gives each writer a disjoint set, with final successful writes read back after timing. `payload_mode` values: `tail` changes the last 16 bytes, `full` varies the full deterministic content, `compressible` uses repeating patterns and `duplicate` repeats exactly. `repeat_every` defaults to zero (no reuse); positive values repeat sequence numbers periodically. Tail changes are not all-new multi-chunk data. Report generator wall time, thread CPU and initial sample size; generation is outside request latency but inside client/throughput budgets. Overwrite ACK counts differ from final-object readback counts.

`requests_per_second` defaults to zero (unlimited); positive values schedule a total arrival rate, up to one million. Worker count is bounded; no unbounded pending queue. If clients fall behind, report schedule lag and achieved success rate. Exclude failures from success percentiles and retain Retry-After distribution. Multipart uses one UploadPart; do not infer multi-part assembly costs.

Report per-operation percentiles, actual request-window throughput, readbacks/errors, CPU, anonymous/file/dirty cgroup memory, per-device I/O bytes/counts and node status. Do not add overlapping device counters. PostgreSQL SQL deltas include background work over the whole window, not just foreground statements per request; missing counters are null. Use the public-read benchmark for encoded Web/anonymous S3. Record page/application cache, equalize resources and ACK policy and count admission SlowDown as failure.

### Write and immediately read back

`python -m tests.benchmarks.source_writes /config/writes.json /results/writes.json` requires an independent release service, disposable bucket, `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only`, boto3 and psycopg if SQL is sampled. Run `python -m tests.benchmarks.source_writes_test` first; a wrong body cannot count as success.

Reuse `label`, `revision`, `build`, `database_version`, `cache_state`, `seed`, `resources` and optional `database_file` from the public-read fixture. Add:

| Field | Meaning |
| --- | --- |
| `round` | Nonnegative round identifier; use unique values to vary new content. |
| `credentials_file` | Restricted JSON with access_key, secret_key and bucket. |
| `endpoints` | One to eight S3 endpoints; distribute clients evenly. |
| `prefix` | Dedicated prefix for this round. The script does not clean the bucket. |
| `seconds` | Duration per case; default 60, maximum 600. |
| `concurrency` | Concurrent writers; default four, maximum 32. |
| `max_operations` | Per-case cap, default 100000, maximum 1000000. Report actual duration if reached early. |
| `slots_per_worker` | Keys overwritten per writer; default four, maximum 64. |
| `unique_keys` | False by default. For a new key per write, set `max_total_bytes` and bound max_operations times size by it. |
| `drain_seconds` | Default zero. Values 1–600 require PostgreSQL and wait for pending_uploads/pack_inputs after warmup and timing; timeout fails the case. |
| `size` | Source bytes for regular cases; default 32768, maximum 16 MiB. |
| `cases` | Array of `new`, `dedup`, `copy`, `multipart`, `many` and/or `mixed`. |
| `many_source` | Existing object key and expected_file for `many`, at most 64 MiB. Record actual extent count and any synthetic layout. |
| `repair_payloads` | Original files for `mixed`, at most 1024. One is consumed every 32 requests; confirm its source is quarantined before starting. |

`new` varies the tail of seeded content; `dedup` and `copy` reuse one hot source; `many` copies a prepared multi-extent object. `multipart` times Complete, while its full cycle includes initiation, one part and readback. Verify bytes/length of every successful write with a full GET. Throughput includes verification; write latency excludes readback. Request or sampling errors fail the run; empty percentiles are null.

Record deltas for CPU/cgroup memory, node status and backend bytes/operations. Group PostgreSQL statements by userid/dbid/queryid/toplevel; retain full SQL, plan/execute time, rows, buffers and WAL. Fail on stats reset; record track, track_planning, fsync and synchronous_commit. Nested track=all times are not additive. Sample wait class/event/blockers by application_name every 100 ms and backlog every second. These are not exact wait seconds; application_name is not a SQL grouping key. Uninstrumented pool wait is null.

Report request-window and drained SQL/CPU/backend costs, pending queues and ready encodings separately. Delayed persistence is not a speedup. `mixed` separates healthy and repair latency; confirm repair via audit. Start from equivalent history/queues, align warmup, statistics refresh and ANALYZE, and run at least three interleaved pairs. Use detailed tracing only in independent attribution runs. Bounded keys do not bound old physical versions; budget bytes, retention and cleanup before testing.

## CORS cache

`python -m tests.benchmarks.cors_cache` uses a started isolated fixture to compare 100 buckets sharing 100-rule CORS documents. It reports retained memory, parse counts and five HTTP HEAD rounds at concurrency 8; missing objects deliberately keep public object caching out of the measurement.
