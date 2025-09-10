package opentelemetryexportermonitoring

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var typeStr = component.MustNewType("monitoring") // <- component.Type

const stability = component.StabilityLevelBeta

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
		Endpoint: "http://127.0.0.1:8080/ingest",
		Timeout:  5 * time.Second,
	}
}

func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,         // <- NOTA: exporter.Settings
	cfg component.Config,
) (exporter.Traces, error) {      // <- retorna exporter.Traces
	c := cfg.(*Config)
	exp, err := newMonitoringExporter(c, set.Logger)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTraces(ctx, set, cfg, exp.pushTraces)
}

func createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	c := cfg.(*Config)
	exp, err := newMonitoringExporter(c, set.Logger)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewMetrics(ctx, set, cfg, exp.pushMetrics)
}

func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	c := cfg.(*Config)
	exp, err := newMonitoringExporter(c, set.Logger)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewLogs(ctx, set, cfg, exp.pushLogs)
}

type monitoringExporter struct {
	endpoint string
	timeout  time.Duration
	logger   *zap.Logger
	client   *http.Client
}

func newMonitoringExporter(cfg *Config, lg *zap.Logger) (*monitoringExporter, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("endpoint must be specified")
	}
	return &monitoringExporter{
		endpoint: cfg.Endpoint,
		timeout:  cfg.Timeout,
		logger:   lg,
		client:   &http.Client{Timeout: cfg.Timeout},
	}, nil
}

func (m *monitoringExporter) pushTraces(_ context.Context, td ptrace.Traces) error {
	m.logger.Info("monitoring/exporter: traces", zap.Int("spans", td.SpanCount()))
	return nil
}
func (m *monitoringExporter) pushMetrics(_ context.Context, md pmetric.Metrics) error {
	m.logger.Info("monitoring/exporter: metrics", zap.Int("metrics", md.MetricCount()))
	return nil
}
func (m *monitoringExporter) pushLogs(_ context.Context, ld plog.Logs) error {
	m.logger.Info("monitoring/exporter: logs", zap.Int("records", ld.LogRecordCount()))
	return nil
}
