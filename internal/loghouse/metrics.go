package loghouse

import (
	"expvar"
	"net/http"
	"sync"
	"sync/atomic"
)

type Metrics struct {
	m *expvar.Map

	ConnectionsTotal  *expvar.Int
	LinesTotal        *expvar.Int
	InvalidJSONTotal  *expvar.Int
	LineTooLargeTotal *expvar.Int

	BatchesTotal    *expvar.Int
	RowsTotal       *expvar.Int
	BatchBytesTotal *expvar.Int

	InsertSuccessTotal *expvar.Int
	InsertErrorsTotal  *expvar.Int
	RetryTotal         *expvar.Int
	PoisonPillTotal    *expvar.Int

	ingressDepth expvar.Func
	writerDepth  expvar.Func

	published atomic.Bool
}

var metricsOnce sync.Once
var publishedMetrics *Metrics

func NewMetrics() *Metrics {
	metricsOnce.Do(func() {
		m := &Metrics{m: new(expvar.Map)}
		m.ConnectionsTotal = new(expvar.Int)
		m.LinesTotal = new(expvar.Int)
		m.InvalidJSONTotal = new(expvar.Int)
		m.LineTooLargeTotal = new(expvar.Int)
		m.BatchesTotal = new(expvar.Int)
		m.RowsTotal = new(expvar.Int)
		m.BatchBytesTotal = new(expvar.Int)
		m.InsertSuccessTotal = new(expvar.Int)
		m.InsertErrorsTotal = new(expvar.Int)
		m.RetryTotal = new(expvar.Int)
		m.PoisonPillTotal = new(expvar.Int)

		m.m.Set("connections_total", m.ConnectionsTotal)
		m.m.Set("lines_total", m.LinesTotal)
		m.m.Set("invalid_json_total", m.InvalidJSONTotal)
		m.m.Set("line_too_large_total", m.LineTooLargeTotal)
		m.m.Set("batches_total", m.BatchesTotal)
		m.m.Set("rows_total", m.RowsTotal)
		m.m.Set("batch_bytes_total", m.BatchBytesTotal)
		m.m.Set("insert_success_total", m.InsertSuccessTotal)
		m.m.Set("insert_errors_total", m.InsertErrorsTotal)
		m.m.Set("retry_total", m.RetryTotal)
		m.m.Set("poison_pill_total", m.PoisonPillTotal)

		expvar.Publish("loghouse", m.m)
		m.published.Store(true)
		publishedMetrics = m
	})
	return publishedMetrics
}

func (m *Metrics) Handler() http.Handler {
	return expvar.Handler()
}

func (m *Metrics) SetIngressQueueDepth(fn func() int) {
	m.ingressDepth = expvar.Func(func() any { return fn() })
	m.m.Set("ingress_queue_depth", m.ingressDepth)
}

func (m *Metrics) SetWriterQueueDepth(fn func() int) {
	m.writerDepth = expvar.Func(func() any { return fn() })
	m.m.Set("writer_queue_depth", m.writerDepth)
}
