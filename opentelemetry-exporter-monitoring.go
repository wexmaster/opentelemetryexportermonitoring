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

const (
	typeStr   = "monitoring"
	stability = component.StabilityLevelBeta
)

func NewFactory() component.ExporterFactory {
	return component.NewExporterFactory(
		typeStr,
		createDefaultConfig,
		component.WithTracesExporter(createTracesExporter, stability),
		component.WithMetricsExporter(createMetricsExporter, stability),
		component.WithLogsExporter(createLogsExporter, stability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Settings: exporter.NewSettings(component.NewID(typeStr)),
		Endpoint: "http://127.0.0.1:8080/ingest",
		Timeout:  5 * time.Second,
	}
}

func createTracesExporter(
	ctx context.Context,
	set component.ExporterCreateSettings,
	cfg component.Config,
) (component.TracesExporter, error) {
	c := cfg.(*Config)
	exp, err := newMonitoringExporter(c, set.Logger)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTracesExporter(
		ctx, set, cfg, exp.pushTraces,
		// opcional:
		// exporterhelper.WithTimeoutSettings(exporterhelper.TimeoutSettings{Timeout: c.Timeout}),
	)
}

func createMetricsExporter(
	ctx context.Context,
	set component.ExporterCreateSettings,
	cfg component.Config,
) (component.MetricsExporter, error) {
	c := cfg.(*Config)
	exp, err := newMonitoringExporter(c, set.Logger)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewMetricsExporter(
		ctx, set, cfg, exp.pushMetrics,
		// exporterhelper.WithTimeoutSettings(exporterhelper.TimeoutSettings{Timeout: c.Timeout}),
	)
}

func createLogsExporter(
	ctx context.Context,
	set component.ExporterCreateSettings,
	cfg component.Config,
) (component.LogsExporter, error) {
	c := cfg.(*Config)
	exp, err := newMonitoringExporter(c, set.Logger)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewLogsExporter(
		ctx, set, cfg, exp.pushLogs,
		// exporterhelper.WithTimeoutSettings(exporterhelper.TimeoutSettings{Timeout: c.Timeout}),
	)
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

func (m *monitoringExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	m.logger.Info("monitoring/exporter: traces", zap.Int("spans", td.SpanCount()))
	return nil
}

func (m *monitoringExporter) pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	m.logger.Info("monitoring/exporter: metrics", zap.Int("metrics", md.MetricCount()))
	return nil
}

func (m *monitoringExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	m.logger.Info("monitoring/exporter: logs", zap.Int("records", ld.LogRecordCount()))
	return nil
}
