# loghouse

`loghouse` is a Go daemon that accepts local JSON Lines (JSONL) log events over a named pipe (FIFO) and continuously batch-inserts them into ClickHouse.

Each incoming line is treated as an **opaque JSON document**. The daemon inserts only a single column (`data`). Any extraction/indexing/enrichment should be done in ClickHouse using `DEFAULT` / `MATERIALIZED` columns.

## Overview

- **Input**: filesystem named pipe (FIFO), JSONL framing (`\n` delimited)
- **Output**: ClickHouse native inserts using `clickhouse-go/v2` batching
- **Backpressure**: bounded channels; when full, pipe writers eventually block
- **Correctness**: 1 non-empty valid JSON line -> 1 inserted row

## ClickHouse table

The daemon inserts `INSERT INTO <db>.<table> (data) VALUES (...)`.

The simplest compatible schema is to store the raw JSON bytes as text:

```sql
CREATE TABLE default.logs (
  data String,
  ingest_ts DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY ingest_ts;
```

From there, derive fields using materialized/default columns, e.g.

```sql
ALTER TABLE default.logs
  ADD COLUMN service LowCardinality(String)
    MATERIALIZED JSONExtractString(data, 'service');
```

Note: `loghouse` currently appends `data` as a Go `string`. If you use ClickHouse `JSON`-typed columns, ensure the driver/type mapping matches your ClickHouse version.

## Build

```bash
go build ./cmd/loghouse
```

## Run

Defaults:
- Pipe: `/run/loghouse/loghouse.pipe`
- Metrics: `127.0.0.1:2112`
- ClickHouse: `127.0.0.1:9000`, database `default`, table `logs` (user `default`, TLS off)

Example:

```bash
./loghouse \
  --clickhouse-addr 127.0.0.1:9000 \
  --clickhouse-database default \
  --clickhouse-table logs
```

Daemonize (double-fork style via re-exec + `setsid`) after creating the named pipe:

```bash
./loghouse --daemon \
  --clickhouse-addr 127.0.0.1:9000 \
  --clickhouse-database default \
  --clickhouse-table logs
```

## Send events

Any process that can write to the FIFO can emit JSONL.

Example:

```bash
printf '{"service":"api","level":"info","msg":"hello"}\n' > /run/loghouse/loghouse.pipe
```

Input rules:
- Empty/whitespace-only lines are ignored.
- Each non-empty line must be valid JSON.
- Maximum line size is enforced (`max_line_bytes`).
- Named-pipe writes are byte-stream based. Writes up to `PIPE_BUF` are atomic; larger concurrent writes can interleave and may be rejected as invalid JSON.

## Configuration

Configuration is via environment variables and/or CLI flags.

Precedence: **flags override environment variables**.

### Environment variables

- `LOGHOUSE_PIPE_PATH` (default: `/run/loghouse/loghouse.pipe`)
- `LOGHOUSE_PIPE_MODE` (default: `0660`)
- `LOGHOUSE_PIPE_GROUP` (default: empty)
- `LOGHOUSE_MAX_LINE_BYTES` (default: `1048576`)
- `LOGHOUSE_INGRESS_QUEUE_SIZE` (default: `8192`)
- `LOGHOUSE_WRITER_QUEUE_SIZE` (default: `128`)
- `LOGHOUSE_MAX_BATCH_ROWS` (default: `10000`)
- `LOGHOUSE_MAX_BATCH_BYTES` (default: `4194304`)
- `LOGHOUSE_FLUSH_INTERVAL` (default: `250ms`)
- `LOGHOUSE_CLICKHOUSE_ADDR` (default: `127.0.0.1:9000`) (host:port for native protocol)
- `LOGHOUSE_CLICKHOUSE_DATABASE` (default: `default`)
- `LOGHOUSE_CLICKHOUSE_TABLE` (default: `logs`)
- `LOGHOUSE_CLICKHOUSE_USERNAME` (default: `default`)
- `LOGHOUSE_CLICKHOUSE_PASSWORD` (default: empty)
- `LOGHOUSE_CLICKHOUSE_TLS` (default: `false`)
- `LOGHOUSE_CLICKHOUSE_TLS_SERVER_NAME` (default: empty; overrides TLS server name/SNI)
- `LOGHOUSE_MAX_RETRIES` (default: `10`)
- `LOGHOUSE_MAX_RETRY_DURATION` (default: `30s`)
- `LOGHOUSE_METRICS_ADDR` (default: `127.0.0.1:2112`)

### Flags

Run `./loghouse -h` for the full set. Key flags mirror the env vars.

Security note: prefer `LOGHOUSE_CLICKHOUSE_PASSWORD` over `--clickhouse-password` to avoid exposing secrets in process listings.

- `--pipe-path`
- `--pipe-mode`
- `--pipe-group`
- `--max-line-bytes`
- `--ingress-queue-size`
- `--writer-queue-size`
- `--max-batch-rows`
- `--max-batch-bytes`
- `--flush-interval`
- `--clickhouse-addr`
- `--clickhouse-database`
- `--clickhouse-table`
- `--clickhouse-user`
- `--clickhouse-password`
- `--clickhouse-tls`
- `--clickhouse-tls-server-name`
- `--max-retries`
- `--max-retry-duration`
- `--metrics-addr`
- `--daemon`

## Observability

Metrics and health are served on `LOGHOUSE_METRICS_ADDR`:

- `GET /live`: liveness (always 200 while server is up)
- `GET /ready`: readiness (200 only when pipe reader/metrics/writer are running)
- `GET /metrics`: `expvar` JSON
- `GET /debug/vars`: same as `/metrics`

Notable counters (under the `loghouse` expvar map):
- `connections_total`, `lines_total`
- `invalid_json_total`, `line_too_large_total`
- `batches_total`, `rows_total`, `batch_bytes_total`
- `insert_success_total`, `insert_errors_total`, `retry_total`
- `poison_pill_total`

## Failure handling

- Transient ClickHouse errors are retried with exponential backoff.
- If an insert fails with a likely per-row/schema/materialization error, the batch is bisected to isolate the offending record.
- Once isolated to a single record, it is counted as a poison pill and dropped so ingestion can continue.

## Test

```bash
go test ./...
```
