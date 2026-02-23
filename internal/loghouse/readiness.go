package loghouse

import "sync/atomic"

type Readiness struct {
	listenerBound  atomic.Bool
	metricsServing atomic.Bool
	writerRunning  atomic.Bool
	accepting      atomic.Bool
}

func NewReadiness() *Readiness {
	return &Readiness{}
}

func (r *Readiness) SetListenerBound(v bool)  { r.listenerBound.Store(v) }
func (r *Readiness) SetMetricsServing(v bool) { r.metricsServing.Store(v) }
func (r *Readiness) SetWriterRunning(v bool)  { r.writerRunning.Store(v) }
func (r *Readiness) SetAccepting(v bool)      { r.accepting.Store(v) }

func (r *Readiness) Ready() bool {
	return r.listenerBound.Load() && r.metricsServing.Load() && r.writerRunning.Load() && r.accepting.Load()
}
