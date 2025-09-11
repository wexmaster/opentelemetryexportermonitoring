package opentelemetryexportermonitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/config/configretry"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var typeStr = component.MustNewType("monitoring")
const stability = component.StabilityLevelBeta

type Config struct {
	// URLs separadas por señal
	TracesURL  string            `mapstructure:"traces_url"`
	MetricsURL string            `mapstructure:"metrics_url"`
	LogsURL    string            `mapstructure:"logs_url"`

	// Cabeceras HTTP opcionales
	Headers map[string]string `mapstructure:"headers"`

	// Nuevos bloques de config del helper
	exporterhelper.TimeoutConfig      `mapstructure:",squash"`
	QueueSettings  exporterhelper.QueueBatchConfig `mapstructure:"sending_queue"`
	RetrySettings  configretry.BackOffConfig       `mapstructure:"retry_on_failure"`
}

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		typeStr,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, stability),
		exporter.WithMetrics(createMetricsExporter, stability),
		exporter.WithLogs(createLogsExporter, stability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		TracesURL:  "http://127.0.0.1:8080/ingest/v1/traces",
		MetricsURL: "http://127.0.0.1:8080/ingest/v1/metrics",
		LogsURL:    "http://127.0.0.1:8080/ingest/v1/logs",
		Headers:    map[string]string{"Content-Type": "application/json"},
		TimeoutConfig: exporterhelper.NewDefaultTimeoutConfig(),
		QueueSettings:  exporterhelper.NewDefaultQueueConfig(),
		RetrySettings:  configretry.NewDefaultBackOffConfig(),
	}
}

func createTracesExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	c := cfg.(*Config)
	exp, err := newMonitoringExporter(c, set.Logger)
	if err != nil { return nil, err }
	return exporterhelper.NewTraces(
		ctx, set, cfg, exp.pushTraces,
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.RetrySettings),
	)
}

func createMetricsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	c := cfg.(*Config)
	exp, err := newMonitoringExporter(c, set.Logger)
	if err != nil { return nil, err }
	return exporterhelper.NewMetrics(
		ctx, set, cfg, exp.pushMetrics,
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.RetrySettings),
	)
}

func createLogsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	c := cfg.(*Config)
	exp, err := newMonitoringExporter(c, set.Logger)
	if err != nil { return nil, err }
	return exporterhelper.NewLogs(
		ctx, set, cfg, exp.pushLogs,
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.RetrySettings),
	)
}

type monitoringExporter struct {
	tracesURL  string
	metricsURL string
	logsURL    string
	headers    map[string]string
	logger     *zap.Logger
	client     *http.Client
}

func newMonitoringExporter(cfg *Config, lg *zap.Logger) (*monitoringExporter, error) {
	if cfg.TracesURL == "" && cfg.MetricsURL == "" && cfg.LogsURL == "" {
		return nil, fmt.Errorf("al menos una URL (traces_url/metrics_url/logs_url) debe especificarse")
	}
	// timeout viene de TimeoutConfig
	cl := &http.Client{ Timeout: cfg.Timeout }
	return &monitoringExporter{
		tracesURL:  cfg.TracesURL,
		metricsURL: cfg.MetricsURL,
		logsURL:    cfg.LogsURL,
		headers:    cfg.Headers,
		logger:     lg,
		client:     cl,
	}, nil
}

func (m *monitoringExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	if m.tracesURL == "" { return nil }
	data, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	if err != nil { return err }
	return m.postJSON(ctx, m.tracesURL, data)
}

func (m *monitoringExporter) pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	if m.metricsURL == "" { return nil }
	data, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(md)
	if err != nil { return err }
	return m.postJSON(ctx, m.metricsURL, data)
}

func (m *monitoringExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	if m.logsURL == "" { return nil }
	data, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
	if err != nil { return err }
	return m.postJSON(ctx, m.logsURL, data)
}

func (m *monitoringExporter) postJSON(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil { return err }
	// Content-Type por defecto + override por headers
	req.Header.Set("Content-Type", "application/json")
	for k, v := range m.headers {
		req.Header.Set(k, v)
	}
	resp, err := m.client.Do(req)
	if err != nil { return err }
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Devolvemos error para que exporterhelper aplique backoff/retry según config
		return fmt.Errorf("monitoring exporter: %s -> HTTP %d", url, resp.StatusCode)
	}
	// Log de depuración (volumen bajo)
	m.logger.Debug("monitoring/exporter POST OK", zap.String("url", url), zap.Int("status", resp.StatusCode), zap.Int("bytes", len(body)))
	return nil
}

// (opcional) si quieres enviar un "envelope" tuyo en vez de OTLP/JSON tal cual:
type envelope struct {
	Timestamp int64                  `json:"timestamp"`
	Payload   json.RawMessage        `json:"payload"`
	Attrs     map[string]string      `json:"attributes,omitempty"`
}
