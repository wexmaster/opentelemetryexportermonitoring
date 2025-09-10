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

// >>> AQUÍ cambia el tipo de retorno y las funciones helper <<<
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
	return exporterhelper.NewTracesExporter(ctx, set, cfg, exp.pushTraces)
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
	return exporterhelper.NewMetricsExporter(ctx, set, cfg, exp.pushMetrics)
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
	return exporterhelper.NewLogsExporter(ctx, set, cfg, exp.pushLogs)
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
