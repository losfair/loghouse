package loghouse

import (
	"bufio"
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestReadLine_MaxBytes(t *testing.T) {
	br := bufio.NewReader(bytes.NewBufferString("{\"a\":1}\n"))
	line, tooLarge, err := readLine(br, 4)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !tooLarge {
		t.Fatalf("expected tooLarge")
	}
	if line != nil {
		t.Fatalf("expected nil line")
	}
}

func TestBatcher_FlushByRows(t *testing.T) {
	m := NewMetrics()
	ingress := make(chan Event, 10)
	out := make(chan Batch, 10)

	go RunBatcher(context.Background(), ingress, out, BatcherConfig{MaxBatchRows: 2, MaxBatchBytes: 1 << 20, FlushInterval: time.Hour}, m)
	ingress <- Event{Raw: []byte("{}")}
	ingress <- Event{Raw: []byte("{}")}
	close(ingress)

	b1, ok := <-out
	if !ok {
		t.Fatalf("expected batch")
	}
	if len(b1.Events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(b1.Events))
	}
	if _, ok := <-out; ok {
		t.Fatalf("expected out closed")
	}
}

func TestReadPipeLoop_CancelUnblocks(t *testing.T) {
	pipePath := filepath.Join(t.TempDir(), "loghouse.pipe")
	if err := EnsureNamedPipe(pipePath, 0600, ""); err != nil {
		t.Fatalf("ensure pipe: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ingress := make(chan Event, 1)
	m := NewMetrics()
	done := make(chan error, 1)

	go func() {
		done <- ReadPipeLoop(ctx, pipePath, ingress, ReaderConfig{
			MaxLineBytes: 1 << 20,
			IdleTimeout:  0,
		}, m)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("read loop returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("read loop did not exit after cancel")
	}
}

func TestBatcher_CancelUnblocksFlush(t *testing.T) {
	m := NewMetrics()
	ingress := make(chan Event, 2)
	out := make(chan Batch) // unbuffered to force flush blocking

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		RunBatcher(ctx, ingress, out, BatcherConfig{MaxBatchRows: 1, MaxBatchBytes: 1 << 20, FlushInterval: time.Hour}, m)
	}()

	// Enqueue an event so batcher tries to flush to out, but don't read from out.
	ingress <- Event{Raw: []byte("{}")}

	// Give the batcher a moment to reach the send.
	time.Sleep(50 * time.Millisecond)
	cancel()

	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()

	select {
	case <-ch:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatalf("batcher did not exit after context cancellation")
	}
}
