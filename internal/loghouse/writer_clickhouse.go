package loghouse

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"
)

type ClickHouseWriter struct {
	conn     clickhouse.Conn
	database string
	table    string

	maxRetries       int
	maxRetryDuration time.Duration

	metrics *Metrics
	logger  *zap.Logger
}

func NewClickHouseWriter(conn clickhouse.Conn, database, table string, maxRetries int, maxRetryDuration time.Duration, metrics *Metrics, logger *zap.Logger) *ClickHouseWriter {
	if maxRetries < 0 {
		maxRetries = 0
	}
	if maxRetryDuration < 0 {
		maxRetryDuration = 0
	}
	return &ClickHouseWriter{
		conn:             conn,
		database:         database,
		table:            table,
		maxRetries:       maxRetries,
		maxRetryDuration: maxRetryDuration,
		metrics:          metrics,
		logger:           logger,
	}
}

func (w *ClickHouseWriter) Run(ctx context.Context, in <-chan Batch) {
	for {
		select {
		case <-ctx.Done():
			return
		case b, ok := <-in:
			if !ok {
				return
			}
			w.insertWithPolicy(ctx, b)
		}
	}
}

func (w *ClickHouseWriter) insertWithPolicy(ctx context.Context, b Batch) {
	if len(b.Events) == 0 {
		return
	}
	if ctx.Err() != nil {
		return
	}

	if err := w.insertWithRetryBudget(ctx, b); err == nil {
		w.metrics.InsertSuccessTotal.Add(1)
		return
	} else {
		w.metrics.InsertErrorsTotal.Add(1)
		if likelyPerRowError(err) {
			w.bisectAndInsert(ctx, b)
			return
		}
		w.logger.Error("dropping batch after retries exhausted",
			zap.Int("rows", len(b.Events)),
			zap.Int("bytes", b.Bytes),
			zap.Error(err),
		)
	}
}

func (w *ClickHouseWriter) bisectAndInsert(ctx context.Context, b Batch) {
	if len(b.Events) == 1 {
		w.metrics.PoisonPillTotal.Add(1)
		w.logger.Warn("dropping poison pill", zap.Int("bytes", len(b.Events[0].Raw)))
		return
	}

	mid := len(b.Events) / 2
	left := Batch{Events: b.Events[:mid]}
	right := Batch{Events: b.Events[mid:]}

	// Recompute bytes
	for _, ev := range left.Events {
		left.Bytes += len(ev.Raw)
	}
	for _, ev := range right.Events {
		right.Bytes += len(ev.Raw)
	}

	w.insertWithPolicy(ctx, left)
	w.insertWithPolicy(ctx, right)
}

func (w *ClickHouseWriter) insertWithRetryBudget(ctx context.Context, b Batch) error {
	start := time.Now()
	retries := 0
	delay := 100 * time.Millisecond

	for {
		err := w.insertOnce(ctx, b)
		if err == nil {
			return nil
		}

		if !transientError(err) {
			return err
		}

		retries++
		w.metrics.RetryTotal.Add(1)
		if w.maxRetries > 0 && retries >= w.maxRetries {
			return err
		}
		if w.maxRetryDuration > 0 && time.Since(start) > w.maxRetryDuration {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		j := time.Duration(rand.Int63n(int64(delay / 2)))
		select {
		case <-time.After(delay + j):
		case <-ctx.Done():
			return ctx.Err()
		}
		if delay < 2*time.Second {
			delay *= 2
		}
	}
}

func (w *ClickHouseWriter) insertOnce(ctx context.Context, b Batch) (err error) {
	start := time.Now()
	w.logger.Info(
		"batch insert started",
		zap.Int("rows", len(b.Events)),
		zap.Int("bytes", b.Bytes),
		zap.String("database", w.database),
		zap.String("table", w.table),
	)
	defer func() {
		fields := []zap.Field{
			zap.Int("rows", len(b.Events)),
			zap.Int("bytes", b.Bytes),
			zap.String("database", w.database),
			zap.String("table", w.table),
			zap.Duration("duration", time.Since(start)),
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
			w.logger.Error("batch insert finished with error", fields...)
			return
		}
		w.logger.Info("batch insert finished", fields...)
	}()

	batch, err := w.conn.PrepareBatch(ctx, "INSERT INTO `"+w.database+"`.`"+w.table+"` (data) VALUES")
	if err != nil {
		return err
	}
	for _, ev := range b.Events {
		// Insert JSON document as string.
		if err := batch.Append(string(ev.Raw)); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	err = batch.Send()
	return err
}

func transientError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	// clickhouse-go wraps some connect/timeouts as plain errors.
	s := err.Error()
	if strings.Contains(s, "timeout") || strings.Contains(s, "connection refused") || strings.Contains(s, "broken pipe") {
		return true
	}
	return false
}

func likelyPerRowError(err error) bool {
	var ex *clickhouse.Exception
	if errors.As(err, &ex) {
		// Server/infrastructure errors apply to all rows equally —
		// bisecting would just drop the entire batch as poison pills.
		if serverLevelException(ex.Code) {
			return false
		}
		// Other ClickHouse exceptions (materialization failures, type
		// errors, etc.) are likely caused by specific row data.
		return true
	}
	s := err.Error()
	return strings.Contains(s, "Cannot parse") || strings.Contains(s, "Cannot read")
}

// serverLevelException returns true for ClickHouse error codes that indicate
// a server or infrastructure problem rather than a per-row data issue.
func serverLevelException(code int32) bool {
	switch code {
	case
		60,  // UNKNOWN_TABLE
		81,  // UNKNOWN_DATABASE
		159, // TIMEOUT_EXCEEDED
		164, // READONLY
		202, // TOO_MANY_SIMULTANEOUS_QUERIES
		209, // SOCKET_TIMEOUT
		210, // NETWORK_ERROR
		236, // TABLE_IS_READ_ONLY
		241, // MEMORY_LIMIT_EXCEEDED
		243, // NOT_ENOUGH_SPACE
		497, // DISTRIBUTED_TOO_MANY_PENDING_BYTES
		516: // AUTHENTICATION_FAILED
		return true
	}
	return false
}
