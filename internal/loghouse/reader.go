package loghouse

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"time"
)

type ReaderConfig struct {
	MaxLineBytes int
	IdleTimeout  time.Duration
}

func ReadPipeLoop(ctx context.Context, pipePath string, ingress chan<- Event, cfg ReaderConfig, metrics *Metrics) error {
	reader, keepaliveWriter, err := openPipeReader(pipePath)
	if err != nil {
		return err
	}
	metrics.ConnectionsTotal.Add(1)

	stopWake := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			// Wake a blocking read so the loop can observe ctx cancellation.
			_, _ = keepaliveWriter.Write([]byte{'\n'})
			if d, ok := any(reader).(deadlineSetter); ok {
				_ = d.SetReadDeadline(time.Now())
			}
		case <-stopWake:
		}
	}()

	defer close(stopWake)
	defer keepaliveWriter.Close()
	defer reader.Close()

	err = handleReader(ctx, reader, ingress, cfg, metrics)
	if ctx.Err() != nil && (err == nil || isPipeClosedError(err) || errors.Is(err, context.Canceled)) {
		return nil
	}
	return err
}

func openPipeReader(path string) (*os.File, *os.File, error) {
	reader, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}

	keepaliveWriter, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		_ = reader.Close()
		return nil, nil, err
	}

	if err := syscall.SetNonblock(int(reader.Fd()), false); err != nil {
		_ = keepaliveWriter.Close()
		_ = reader.Close()
		return nil, nil, err
	}

	return reader, keepaliveWriter, nil
}

func isPipeClosedError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) || errors.Is(err, syscall.EBADF) {
		return true
	}
	var pathErr *os.PathError
	return errors.As(err, &pathErr) && (errors.Is(pathErr.Err, os.ErrClosed) || errors.Is(pathErr.Err, syscall.EBADF))
}

type deadlineSetter interface {
	SetReadDeadline(t time.Time) error
}

func handleReader(ctx context.Context, reader io.Reader, ingress chan<- Event, cfg ReaderConfig, metrics *Metrics) error {
	setDeadline := func() {
		if cfg.IdleTimeout <= 0 {
			return
		}
		d, ok := reader.(deadlineSetter)
		if !ok {
			return
		}
		_ = d.SetReadDeadline(time.Now().Add(cfg.IdleTimeout))
	}

	setDeadline()
	br := bufio.NewReaderSize(reader, 64*1024)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		line, tooLarge, err := readLine(br, cfg.MaxLineBytes)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if ctx.Err() != nil {
					return nil
				}
				continue
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				if ctx.Err() != nil {
					return nil
				}
				setDeadline()
				continue
			}
			if errors.Is(err, os.ErrDeadlineExceeded) {
				if ctx.Err() != nil {
					return nil
				}
				setDeadline()
				continue
			}
			return err
		}

		metrics.LinesTotal.Add(1)
		if tooLarge {
			metrics.LineTooLargeTotal.Add(1)
			setDeadline()
			continue
		}

		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			setDeadline()
			continue
		}
		if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
			metrics.InvalidJSONTotal.Add(1)
			setDeadline()
			continue
		}
		if !json.Valid(trimmed) {
			metrics.InvalidJSONTotal.Add(1)
			setDeadline()
			continue
		}

		rawCopy := append([]byte(nil), trimmed...)
		select {
		case ingress <- Event{Raw: rawCopy}:
		case <-ctx.Done():
			return ctx.Err()
		}

		setDeadline()
	}
}

// readLine returns a line without trailing '\n'. It also trims a trailing '\r'.
// If the line exceeds maxBytes, tooLarge is true and line is nil.
func readLine(br *bufio.Reader, maxBytes int) (line []byte, tooLarge bool, err error) {
	if maxBytes <= 0 {
		return nil, false, errors.New("maxBytes must be > 0")
	}

	var out []byte
	for {
		frag, e := br.ReadSlice('\n')
		if e == nil {
			out = append(out, frag...)
			break
		}
		if errors.Is(e, bufio.ErrBufferFull) {
			out = append(out, frag...)
			if len(out) > maxBytes {
				for errors.Is(e, bufio.ErrBufferFull) {
					_, e = br.ReadSlice('\n')
				}
				if e != nil && !errors.Is(e, io.EOF) {
					return nil, false, e
				}
				return nil, true, nil
			}
			continue
		}
		if errors.Is(e, io.EOF) {
			out = append(out, frag...)
			if len(out) == 0 {
				return nil, false, io.EOF
			}
			break
		}
		return nil, false, e
	}

	out = bytes.TrimSuffix(out, []byte{'\n'})
	out = bytes.TrimSuffix(out, []byte{'\r'})
	if len(out) > maxBytes {
		return nil, true, nil
	}
	return out, false, nil
}
