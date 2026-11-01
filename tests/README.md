# Test guide

Run builds and tests in isolated Docker containers. Give each database contract its own empty database or new SQLite file. Use dedicated accounts, buckets, data paths and fault directories.

| Scope | Entry and required fixture | State changes |
| --- | --- | --- |
| [Database and recovery](recovery/README.md) | Bootstrap, native contracts, process restart; dedicated database or stopped service | Creates databases, injects faults, kills test processes |
| [Storage](storage/README.md) | Raw/chunks/packs, quarantine, repair and replication; disposable local/S3 backend | Writes, corrupts and reclaims fixture data |
| [RPC and cluster](rpc/README.md) | Separate control/node identities, PostgreSQL, TLS, optional shared S3 | Rotates credentials, disconnects nodes, pauses replica replay |
| [S3 and public HTTP](s3/README.md) | Dedicated public bucket/credentials and optional proxy | Creates objects and changes access |
| [Control and Web](control/README.md) | Management API/CLI, isolated admin and Playwright | Changes fixture settings and accounts |
| [Image processing](images/README.md) | Image helper, isolated image directory and bucket | Produces files, styles and derivatives |
| [Benchmarks](benchmarks/README.md) | Release service, separate client, fixed resource budget | Read-only measurements or writes to an exclusive prefix |

## Rust checks

Install `protoc` in the build container. For the default Web-enabled build, run `npm ci` and `npm run build` in `web/` first. From the repository root:

```sh
cargo fmt --all -- --check
cargo test --locked --no-default-features --all-targets
cargo clippy --locked --all-targets --features fault-injection
cargo build --locked
```

Ordinary `storage::transfer` tests cover raw transfer after a confirmed part, completion reconciliation and rejection of gaps or changed source bytes. `model::transfer` checks digest checkpoints and bounded node plans; `content::transfer` checks selective pack decoding and member identity.

Database contracts are ignored by default. Set `MOKYU_TEST_ALLOW_STATE_CHANGES=isolated-only` and supply `MOKYU_TEST_DATABASE_URL_FILE` (a file containing the PostgreSQL DSN), `MOKYU_TEST_MYSQL_CONFIG` or `MOKYU_TEST_SQLITE_CONFIG`. Each config needs a fixed deployment key and a dedicated data path. Check the test header for additional variables. For example:

```sh
MOKYU_TEST_SQLITE_CONFIG=/config/sqlite-accounts.toml \
  cargo test --locked --no-default-features sqlite_account -- --ignored --nocapture
```

Each native contract exercises its own interface, transactions and recovery. Use native SQL only to arrange or inspect that backend's state. Never run all ignored contracts concurrently on one configuration. Confirm the filter actually selected a test. Use a fresh database for each rerun.

## Integration prerequisites

For Python integration tests start an isolated `fault-injection` build with dedicated local/S3 storage, bucket, chunk key and administrator. Install `boto3` and `requests`, plus `psycopg` for PostgreSQL-specific tests.

Lifecycle tests wait for capacity statistics. Set `metrics.refresh_interval` to a short interval such as `2s`; save and restart the test service before running.

Read the module header and `MOKYU_TEST_*` lookups for required settings. Supply credentials through files and keep passwords/S3 secrets out of logs. Recovery suites kill and restart the configured gateway: run them only in dedicated test containers. Never deploy a fault-injection build.
