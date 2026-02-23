package loghouse

import (
	"context"
	"time"
)

type BatcherConfig struct {
	MaxBatchRows  int
	MaxBatchBytes int
	FlushInterval time.Duration
}

// RunBatcher reads events from ingress and emits batches to out.
// It closes out when ingress is closed and all remaining events are flushed.
func RunBatcher(ctx context.Context, ingress <-chan Event, out chan<- Batch, cfg BatcherConfig, metrics *Metrics) {
	defer close(out)

	var (
		batch     []Event
		batchSize int
	)

	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		b := Batch{Events: batch, Bytes: batchSize}
		metrics.BatchesTotal.Add(1)
		metrics.RowsTotal.Add(int64(len(batch)))
		metrics.BatchBytesTotal.Add(int64(batchSize))
		select {
		case out <- b:
			batch = nil
			batchSize = 0
			return true
		case <-ctx.Done():
			return false
		}
	}

	timer := time.NewTimer(cfg.FlushInterval)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	resetTimer := func() {
		if cfg.FlushInterval <= 0 {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(cfg.FlushInterval)
	}

	for {
		var timerC <-chan time.Time
		if len(batch) > 0 {
			timerC = timer.C
		}

		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ingress:
			if !ok {
				_ = flush()
				return
			}

			evBytes := len(ev.Raw)
			if len(batch) == 0 {
				resetTimer()
			}

			if cfg.MaxBatchRows > 0 && len(batch) >= cfg.MaxBatchRows {
				if !flush() {
					return
				}
				resetTimer()
			}
			if cfg.MaxBatchBytes > 0 && batchSize > 0 && batchSize+evBytes > cfg.MaxBatchBytes {
				if !flush() {
					return
				}
				resetTimer()
			}

			batch = append(batch, ev)
			batchSize += evBytes

			if cfg.MaxBatchRows > 0 && len(batch) >= cfg.MaxBatchRows {
				if !flush() {
					return
				}
			}
			if cfg.MaxBatchBytes > 0 && batchSize >= cfg.MaxBatchBytes {
				if !flush() {
					return
				}
			}

		case <-timerC:
			if !flush() {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
