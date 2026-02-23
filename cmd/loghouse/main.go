package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"

	"github.com/losfair/loghouse/internal/loghouse"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		_ = logger.Sync()
	}()

	cfg, err := parseConfig()
	if err != nil {
		logger.Fatal("config error", zap.Error(err))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	metrics := loghouse.NewMetrics()

	alreadyDaemonized := os.Getenv("LOGHOUSE_DAEMONIZED") == "1"

	logger.Info("starting loghouse",
		zap.Int("pid", os.Getpid()),
		zap.Bool("daemon", cfg.Daemon),
		zap.Bool("already_daemonized", alreadyDaemonized),
		zap.String("pipe_path", cfg.PipePath),
		zap.String("metrics_addr", cfg.MetricsAddr),
		zap.String("clickhouse_addr", cfg.ClickHouseAddr),
		zap.String("clickhouse_database", cfg.ClickHouseDatabase),
		zap.String("clickhouse_table", cfg.ClickHouseTable),
		zap.String("clickhouse_user", cfg.ClickHouseUsername),
		zap.Bool("clickhouse_tls", cfg.ClickHouseTLS),
		zap.String("clickhouse_tls_server_name", cfg.ClickHouseTLSServerName),
		zap.Int("max_line_bytes", cfg.MaxLineBytes),
		zap.Int("ingress_queue_size", cfg.IngressQueueSize),
		zap.Int("writer_queue_size", cfg.WriterQueueSize),
		zap.Int("max_batch_rows", cfg.MaxBatchRows),
		zap.Int("max_batch_bytes", cfg.MaxBatchBytes),
		zap.Duration("flush_interval", cfg.FlushInterval),
		zap.Int("max_retries", cfg.MaxRetries),
		zap.Duration("max_retry_duration", cfg.MaxRetryDuration),
		zap.Duration("idle_timeout", cfg.IdleTimeout),
		zap.Duration("drain_timeout", cfg.DrainTimeout),
	)

	if err := loghouse.EnsureNamedPipe(cfg.PipePath, cfg.PipeMode, cfg.PipeGroup); err != nil {
		logger.Fatal("create named pipe", zap.Error(err))
	}
	logger.Info("named pipe ready", zap.String("pipe_path", cfg.PipePath))

	if cfg.Daemon && !alreadyDaemonized {
		if err := daemonize(); err != nil {
			_ = os.Remove(cfg.PipePath)
			logger.Fatal("daemonize", zap.Error(err))
		}
		return
	}

	defer func() {
		_ = os.Remove(cfg.PipePath)
	}()

	logger.Info("opening clickhouse", zap.String("addr", cfg.ClickHouseAddr), zap.String("user", cfg.ClickHouseUsername), zap.Bool("tls", cfg.ClickHouseTLS))

	var tlsConf *tls.Config
	if cfg.ClickHouseTLS {
		serverName := cfg.ClickHouseTLSServerName
		if serverName == "" {
			host, _, err := net.SplitHostPort(cfg.ClickHouseAddr)
			if err != nil {
				logger.Fatal(
					"invalid clickhouse-addr for TLS (expected host:port); set --clickhouse-tls-server-name to override",
					zap.Error(err),
				)
			}
			serverName = host
		}
		tlsConf = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.ClickHouseAddr},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouseDatabase,
			Username: cfg.ClickHouseUsername,
			Password: cfg.ClickHousePassword,
		},
		TLS: tlsConf,
	})
	if err != nil {
		logger.Fatal("clickhouse open", zap.Error(err))
	}
	logger.Info("clickhouse ready")
	defer func() {
		_ = conn.Close()
	}()

	writer := loghouse.NewClickHouseWriter(conn, cfg.ClickHouseDatabase, cfg.ClickHouseTable, cfg.MaxRetries, cfg.MaxRetryDuration, metrics, logger)

	ingress := make(chan loghouse.Event, cfg.IngressQueueSize)
	batches := make(chan loghouse.Batch, cfg.WriterQueueSize)

	metrics.SetIngressQueueDepth(func() int { return len(ingress) })
	metrics.SetWriterQueueDepth(func() int { return len(batches) })

	ready := loghouse.NewReadiness()
	ready.SetListenerBound(true)

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/debug/vars", metrics.Handler())
		mux.Handle("/metrics", metrics.Handler())
		mux.HandleFunc("/live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
		mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
			if ready.Ready() {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		})

		metricsLn, err := net.Listen("tcp", cfg.MetricsAddr)
		if err != nil {
			logger.Error("metrics listen error", zap.Error(err))
			return
		}
		ready.SetMetricsServing(true)
		logger.Info("metrics listening", zap.String("addr", cfg.MetricsAddr))

		metricsSrv := &http.Server{Handler: mux}
		go func() {
			<-ctx.Done()
			_ = metricsSrv.Shutdown(context.Background())
		}()
		if err := metricsSrv.Serve(metricsLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server error", zap.Error(err))
		}
		ready.SetMetricsServing(false)
	}()

	// The batcher/writer use a separate context so they can drain queues
	// after the signal cancels ctx (until drainCancel is triggered).
	drainCtx, drainCancel := context.WithCancel(context.Background())
	defer drainCancel()

	go loghouse.RunBatcher(drainCtx, ingress, batches, loghouse.BatcherConfig{
		MaxBatchRows:  cfg.MaxBatchRows,
		MaxBatchBytes: cfg.MaxBatchBytes,
		FlushInterval: cfg.FlushInterval,
	}, metrics)

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		ready.SetWriterRunning(true)
		defer ready.SetWriterRunning(false)
		logger.Info("writer started")
		writer.Run(drainCtx, batches)
		logger.Info("writer stopped")
	}()

	ready.SetAccepting(true)
	acceptErrCh := make(chan error, 1)
	go func() {
		logger.Info("pipe read loop started")
		acceptErrCh <- loghouse.ReadPipeLoop(
			ctx,
			cfg.PipePath,
			ingress,
			loghouse.ReaderConfig{MaxLineBytes: cfg.MaxLineBytes, IdleTimeout: cfg.IdleTimeout},
			metrics,
		)
		logger.Info("pipe read loop stopped")
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
		// Wait for read loop to exit after cancellation.
		_ = <-acceptErrCh
	case err := <-acceptErrCh:
		if err != nil {
			logger.Error("pipe read loop error", zap.Error(err))
			cancel()
		}
	}

	ready.SetAccepting(false)
	logger.Info("stopped accepting")
	close(ingress)

	logger.Info("draining writer", zap.Duration("timeout", cfg.DrainTimeout))
	// Give the writer a bounded time to drain remaining batches.
	if cfg.DrainTimeout > 0 {
		drainTimer := time.AfterFunc(cfg.DrainTimeout, func() {
			logger.Warn("drain timeout exceeded, cancelling writer")
			drainCancel()
		})
		defer drainTimer.Stop()
	}
	<-writerDone
	drainCancel()
	logger.Info("shutdown complete")
}

type config struct {
	PipePath                string
	PipeMode                os.FileMode
	PipeGroup               string
	MaxLineBytes            int
	IngressQueueSize        int
	WriterQueueSize         int
	MaxBatchRows            int
	MaxBatchBytes           int
	FlushInterval           time.Duration
	ClickHouseAddr          string
	ClickHouseDatabase      string
	ClickHouseTable         string
	ClickHouseUsername      string
	ClickHousePassword      string
	ClickHouseTLS           bool
	ClickHouseTLSServerName string
	MaxRetries              int
	MaxRetryDuration        time.Duration
	IdleTimeout             time.Duration
	DrainTimeout            time.Duration
	MetricsAddr             string
	Daemon                  bool
}

func parseConfig() (config, error) {
	var cfg config
	var err error

	cfg.PipePath = envOr("LOGHOUSE_PIPE_PATH", "/run/loghouse/loghouse.pipe")
	cfg.PipeMode = 0660
	if s := os.Getenv("LOGHOUSE_PIPE_MODE"); s != "" {
		v, err := strconv.ParseUint(s, 8, 32)
		if err != nil {
			return cfg, fmt.Errorf("invalid LOGHOUSE_PIPE_MODE: %w", err)
		}
		cfg.PipeMode = os.FileMode(v)
	}
	cfg.PipeGroup = os.Getenv("LOGHOUSE_PIPE_GROUP")
	if cfg.MaxLineBytes, err = envOrInt("LOGHOUSE_MAX_LINE_BYTES", 1<<20); err != nil {
		return cfg, err
	}
	if cfg.IngressQueueSize, err = envOrInt("LOGHOUSE_INGRESS_QUEUE_SIZE", 8192); err != nil {
		return cfg, err
	}
	if cfg.WriterQueueSize, err = envOrInt("LOGHOUSE_WRITER_QUEUE_SIZE", 128); err != nil {
		return cfg, err
	}
	if cfg.MaxBatchRows, err = envOrInt("LOGHOUSE_MAX_BATCH_ROWS", 10000); err != nil {
		return cfg, err
	}
	if cfg.MaxBatchBytes, err = envOrInt("LOGHOUSE_MAX_BATCH_BYTES", 4<<20); err != nil {
		return cfg, err
	}
	if cfg.FlushInterval, err = envOrDuration("LOGHOUSE_FLUSH_INTERVAL", 250*time.Millisecond); err != nil {
		return cfg, err
	}
	cfg.ClickHouseAddr = envOr("LOGHOUSE_CLICKHOUSE_ADDR", "127.0.0.1:9000")
	cfg.ClickHouseDatabase = envOr("LOGHOUSE_CLICKHOUSE_DATABASE", "default")
	cfg.ClickHouseTable = envOr("LOGHOUSE_CLICKHOUSE_TABLE", "logs")
	cfg.ClickHouseUsername = envOr("LOGHOUSE_CLICKHOUSE_USERNAME", "default")
	cfg.ClickHousePassword = os.Getenv("LOGHOUSE_CLICKHOUSE_PASSWORD")
	if cfg.ClickHouseTLS, err = envOrBool("LOGHOUSE_CLICKHOUSE_TLS", false); err != nil {
		return cfg, err
	}
	cfg.ClickHouseTLSServerName = os.Getenv("LOGHOUSE_CLICKHOUSE_TLS_SERVER_NAME")
	if cfg.MaxRetries, err = envOrInt("LOGHOUSE_MAX_RETRIES", 10); err != nil {
		return cfg, err
	}
	if cfg.MaxRetryDuration, err = envOrDuration("LOGHOUSE_MAX_RETRY_DURATION", 30*time.Second); err != nil {
		return cfg, err
	}
	if cfg.IdleTimeout, err = envOrDuration("LOGHOUSE_IDLE_TIMEOUT", 60*time.Second); err != nil {
		return cfg, err
	}
	if cfg.DrainTimeout, err = envOrDuration("LOGHOUSE_DRAIN_TIMEOUT", 10*time.Second); err != nil {
		return cfg, err
	}
	cfg.MetricsAddr = envOr("LOGHOUSE_METRICS_ADDR", "127.0.0.1:2112")

	flag.StringVar(&cfg.PipePath, "pipe-path", cfg.PipePath, "named pipe path")
	modeStr := flag.String("pipe-mode", fmt.Sprintf("%#o", cfg.PipeMode), "named pipe mode (octal like 0660)")
	flag.StringVar(&cfg.PipeGroup, "pipe-group", cfg.PipeGroup, "named pipe group name")
	flag.IntVar(&cfg.MaxLineBytes, "max-line-bytes", cfg.MaxLineBytes, "max JSONL line size")
	flag.IntVar(&cfg.IngressQueueSize, "ingress-queue-size", cfg.IngressQueueSize, "ingress queue size")
	flag.IntVar(&cfg.WriterQueueSize, "writer-queue-size", cfg.WriterQueueSize, "writer queue size")
	flag.IntVar(&cfg.MaxBatchRows, "max-batch-rows", cfg.MaxBatchRows, "max rows per batch")
	flag.IntVar(&cfg.MaxBatchBytes, "max-batch-bytes", cfg.MaxBatchBytes, "max bytes per batch")
	flag.DurationVar(&cfg.FlushInterval, "flush-interval", cfg.FlushInterval, "flush interval")
	flag.StringVar(&cfg.ClickHouseAddr, "clickhouse-addr", cfg.ClickHouseAddr, "clickhouse addr (host:port)")
	flag.StringVar(&cfg.ClickHouseDatabase, "clickhouse-database", cfg.ClickHouseDatabase, "clickhouse database")
	flag.StringVar(&cfg.ClickHouseTable, "clickhouse-table", cfg.ClickHouseTable, "clickhouse table")
	flag.StringVar(&cfg.ClickHouseUsername, "clickhouse-user", cfg.ClickHouseUsername, "clickhouse username")
	flag.StringVar(&cfg.ClickHousePassword, "clickhouse-password", cfg.ClickHousePassword, "clickhouse password")
	flag.BoolVar(&cfg.ClickHouseTLS, "clickhouse-tls", cfg.ClickHouseTLS, "enable clickhouse TLS")
	flag.StringVar(&cfg.ClickHouseTLSServerName, "clickhouse-tls-server-name", cfg.ClickHouseTLSServerName, "clickhouse TLS server name override")
	flag.IntVar(&cfg.MaxRetries, "max-retries", cfg.MaxRetries, "max retries per insert")
	flag.DurationVar(&cfg.MaxRetryDuration, "max-retry-duration", cfg.MaxRetryDuration, "max retry duration per insert")
	flag.DurationVar(&cfg.IdleTimeout, "idle-timeout", cfg.IdleTimeout, "idle pipe read timeout (0 to disable)")
	flag.DurationVar(&cfg.DrainTimeout, "drain-timeout", cfg.DrainTimeout, "max time to drain queues on shutdown (0 to disable)")
	flag.StringVar(&cfg.MetricsAddr, "metrics-addr", cfg.MetricsAddr, "metrics listen addr")
	flag.BoolVar(&cfg.Daemon, "daemon", false, "daemonize after creating named pipe")
	flag.Parse()

	if *modeStr != "" {
		// Accept "0o660", "0660", and "660".
		s := *modeStr
		s = strings.TrimPrefix(s, "0o")
		s = strings.TrimPrefix(s, "0O")
		v, err := strconv.ParseUint(s, 8, 32)
		if err != nil {
			return cfg, fmt.Errorf("invalid --pipe-mode: %w", err)
		}
		cfg.PipeMode = os.FileMode(v)
	}

	if !filepath.IsAbs(cfg.PipePath) {
		return cfg, fmt.Errorf("pipe path must be absolute")
	}
	if cfg.MaxLineBytes <= 0 {
		return cfg, fmt.Errorf("max-line-bytes must be > 0")
	}
	if cfg.IngressQueueSize <= 0 || cfg.WriterQueueSize <= 0 {
		return cfg, fmt.Errorf("queue sizes must be > 0")
	}
	if cfg.MaxBatchRows <= 0 || cfg.MaxBatchBytes <= 0 {
		return cfg, fmt.Errorf("batch limits must be > 0")
	}
	if cfg.FlushInterval <= 0 {
		return cfg, fmt.Errorf("flush-interval must be > 0")
	}
	if cfg.MetricsAddr == "" {
		return cfg, fmt.Errorf("metrics-addr required")
	}
	if _, _, err := net.SplitHostPort(cfg.MetricsAddr); err != nil {
		return cfg, fmt.Errorf("invalid metrics-addr: %w", err)
	}
	return cfg, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envOrInt(k string, def int) (int, error) {
	if v := os.Getenv(k); v != "" {
		i, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("invalid %s: %w", k, err)
		}
		return i, nil
	}
	return def, nil
}

func envOrDuration(k string, def time.Duration) (time.Duration, error) {
	if v := os.Getenv(k); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return 0, fmt.Errorf("invalid %s: %w", k, err)
		}
		return d, nil
	}
	return def, nil
}

func envOrBool(k string, def bool) (bool, error) {
	if v := os.Getenv(k); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return false, fmt.Errorf("invalid %s: %w", k, err)
		}
		return b, nil
	}
	return def, nil
}
