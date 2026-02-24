# Loghouse — Design

This document describes the design for a Go daemon that accepts local JSONL log events over a named pipe (FIFO) and continuously batch-inserts them into ClickHouse.

The daemon treats each incoming JSON object as an **opaque JSON document**. It inserts only a **single JSON column** into ClickHouse (e.g. `data`). Any field extraction, indexing, partitioning, or enrichment is handled entirely by the ClickHouse schema via `MATERIALIZED` / `DEFAULT` columns.

## Goals

- **Low-overhead ingestion** from local processes via a filesystem named pipe.
- **High-throughput ClickHouse inserts** using batching.
- **Backpressure**: bounded memory with predictable behavior under load.
- **Operational clarity**: metrics, health checks, structured internal logs.
- **Correctness**: each JSONL line corresponds to a single inserted row.

## Non-goals

- Parsing, normalizing, or enforcing schema on the JSON payload beyond basic JSON validity.
- Multi-tenant routing to arbitrary tables/databases per message (initially single target).
- Remote ingestion over TCP/HTTP (local-only via FIFO).
- Exactly-once delivery semantics.

## External Interfaces

### Named pipe (FIFO)

- **Type**: POSIX named pipe (`mkfifo`) exposed in the filesystem.
- **Path**: configurable, default recommended: `/run/loghouse/loghouse.pipe`.
- **Permissions**:
  - Pipe mode default: `0660`.
  - Pipe group configurable (e.g. `loghouse`), enabling least-privilege client access.
- **Lifecycle**:
  - On startup: ensure parent directory exists; create the pipe when missing; apply `chmod`/`chown` as configured.
  - On shutdown: stop the reader loop and remove the pipe path.

### Wire protocol

- **Protocol**: JSON Lines (JSONL)
- **Record boundary**: newline (`\n`). Each line must be a complete JSON object.
- **Accepted input**: UTF-8 is typical, but daemon treats payload as bytes; ClickHouse stores the JSON document.

#### Input rules

- Empty lines are ignored.
- Each non-empty line must be valid JSON (`json.Valid`). Invalid JSON increments an error counter and is rejected.
- Maximum line size is enforced (`max_line_bytes`) to prevent memory abuse.

## ClickHouse Contract

The daemon inserts only one column: `data`.

- **Insert statement shape**: `INSERT INTO <db>.<table> (data) VALUES`.
- The table may have additional columns, but they are populated via schema expressions (`DEFAULT`, `MATERIALIZED`, etc.).

### Example schema pattern (conceptual)

> Note: concrete JSON extraction syntax depends on ClickHouse version and whether `data` is `JSON` vs `String`.

- `data JSON` (or `String` if using JSON text)
- `ingest_ts DateTime64(3) DEFAULT now64(3)`
- `ts DateTime64(3) MATERIALIZED <extract from data>`
- `level LowCardinality(String) MATERIALIZED <extract from data>`
- `service LowCardinality(String) MATERIALIZED <extract from data>`

Partitioning and ordering should use the derived/materialized columns (e.g. `PARTITION BY toDate(ts)` and `ORDER BY (service, ts)`), without requiring any changes in the Go daemon.

## Internal Architecture

The daemon is a single process with an internal pipeline:

1. **FIFO reader loop** reads newline-delimited records from the named pipe.
2. **Validation** checks JSONL framing and JSON syntax.
3. **Ingress queue** (bounded) buffers accepted events.
4. **Batcher** groups events by count/bytes/time.
5. **ClickHouse writer** performs batched inserts and handles retry/backoff and poison-pill isolation.
6. **Observability** exposes metrics/health and logs internal operation.

### Data model

The internal event is intentionally minimal:

- `Event{ Raw []byte }`

`Raw` is the exact JSON line bytes (without trailing newline). The daemon does not unmarshal or rewrite JSON.

## Concurrency Model

- **FIFO reader goroutine**:
  - Opens the pipe and continuously reads lines with size limits.
  - Applies basic validation.
  - Enqueues events into the ingress queue (blocking when full).
- **Batcher goroutine**:
  - Reads from ingress queue.
  - Aggregates into a batch.
  - Flushes when `max_batch_rows` or `max_batch_bytes` reached, or `flush_interval` timer fires.
  - Emits batches to a bounded writer queue.
- **Writer goroutine(s)**:
  - Reads batches and inserts into ClickHouse.
  - Maintains retry/backoff.

## Backpressure and Load Behavior

Backpressure is enforced by **bounded channels**.

- If the ingress queue is full, the FIFO reader blocks on enqueue.
- This naturally slows clients (writes to the pipe eventually block), preventing unbounded memory usage.

Optional future mode:
- A drop-on-overload mode that drops events when the queue is full (with counters), useful for “best effort” environments.

## Batching

Batch flush triggers:

- `max_batch_rows`: maximum number of events per batch.
- `max_batch_bytes`: approximate maximum bytes per batch (sum of event sizes + overhead).
- `flush_interval`: maximum time an event waits before being flushed.

Rationale:
- JSON documents vary in size; byte-based limits avoid pathological large batches.
- Time-based flush bounds latency in low-traffic periods.

## ClickHouse Writes

### Driver

Use `clickhouse-go/v2` (native protocol) for performance and typed batch support.

### Insert execution

- Use `PrepareBatch` for bulk appends.
- Append exactly one value per event (the `data` column).
- On success: update metrics, continue.

## Failure Handling

ClickHouse failures fall into two broad classes:

1. **Transient/unavailable** (network errors, timeouts, server unavailable)
2. **Permanent/poison-pill** (a specific row triggers schema/materialization failure)

The daemon should be explicit about policy.

### Retry policy (baseline)

- Retries on transient errors using exponential backoff with jitter.
- Retry budget is bounded by `max_retries` and/or `max_retry_duration`.
- If the retry budget is exhausted and the error is not a per-row issue, the batch is logged and dropped. This avoids blocking the pipeline indefinitely on persistent infrastructure errors.

### Poison-pill isolation

When ClickHouse rejects a batch due to a specific bad record (e.g. materialized column extraction throws), retrying the whole batch can deadlock ingestion.

Recommended approach:

- If a batch insert fails with a likely per-row error, **bisect** the batch to isolate failing rows.
- Once isolated to a single record:
  - increment `poison_pill_total`
  - drop the record
  - continue ingestion

This approach trades some CPU during error conditions for continued forward progress.


## Security Considerations

Even though the FIFO is local-only, inputs must be treated as untrusted.

- **Access control**: rely on filesystem permissions for the pipe path (mode + owner/group) to scope which local users can write.
- **Resource limits**:
  - max line size
  - bounded queues
  - idle read timeout
- **No JSON evaluation** in the daemon: it validates JSON syntax but does not interpret fields.

## Observability

### Metrics

Expose metrics via Prometheus or expvar, including:

- connection and line counters (`connections_total`, `lines_total`)
- parse/validation errors (`invalid_json_total`, `line_too_large_total`)
- queue depths (`ingress_queue_depth`, `writer_queue_depth`)
- batching stats (`batches_total`, `rows_total`, `batch_bytes_total`)
- ClickHouse results (`insert_success_total`, `insert_errors_total`, `retry_total`)
- poison-pill handling (`poison_pill_total`)

### Health

- **Readiness**: pipe prepared, reader loop active, internal loops running.
- **Liveness**: process responsive.
- ClickHouse reachability should be reported as a metric/endpoint.

### Internal logging

The daemon logs operational messages to stderr/journald (it must not log to its own ingestion pipe).

## Configuration

Configuration is provided via **CLI arguments and environment variables**.

- **Precedence**: CLI arguments override environment variables.
- **No config file**: configuration is intentionally kept to env/flags for simple deployment.

### Environment variables (recommended)

Suggested env var names:

- `LOGHOUSE_PIPE_PATH`
- `LOGHOUSE_PIPE_MODE` (e.g. `0660`)
- `LOGHOUSE_PIPE_GROUP`
- `LOGHOUSE_MAX_LINE_BYTES`
- `LOGHOUSE_INGRESS_QUEUE_SIZE`
- `LOGHOUSE_MAX_BATCH_ROWS`
- `LOGHOUSE_MAX_BATCH_BYTES`
- `LOGHOUSE_FLUSH_INTERVAL` (e.g. `250ms`, `1s`)
- `LOGHOUSE_CLICKHOUSE_ADDR`
- `LOGHOUSE_CLICKHOUSE_DATABASE`
- `LOGHOUSE_CLICKHOUSE_TABLE`
- `LOGHOUSE_CLICKHOUSE_USERNAME`
- `LOGHOUSE_CLICKHOUSE_PASSWORD`
- `LOGHOUSE_CLICKHOUSE_TLS`
- `LOGHOUSE_CLICKHOUSE_TLS_SERVER_NAME`
- `LOGHOUSE_MAX_RETRIES`
- `LOGHOUSE_MAX_RETRY_DURATION` (e.g. `10s`, `1m`)
- `LOGHOUSE_METRICS_ADDR` (e.g. `127.0.0.1:2112`)

### CLI arguments

Suggested flag names (mirroring env vars):

- `--pipe-path`
- `--pipe-mode`
- `--pipe-group`
- `--max-line-bytes`
- `--ingress-queue-size`
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

Security note: prefer environment variables (especially `LOGHOUSE_CLICKHOUSE_PASSWORD`) over passing secrets via CLI flags.

## Graceful Shutdown

On shutdown signal:

- Stop the FIFO reader loop.
- Drain ingress queue up to a configured deadline.
- Flush the batcher.
- Finish in-flight ClickHouse insert(s).
- Remove pipe and exit.

## Testing Strategy (design intent)

- Unit tests:
  - JSONL framing and max line size enforcement
  - batching triggers (rows/bytes/interval)
  - poison-pill isolation logic (batch split)
- Integration tests (optional):
  - insert into a ClickHouse container and verify row count and schema-derived columns populate
