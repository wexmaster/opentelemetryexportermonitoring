package opentelemetryexportermonitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// -------- Factory --------

var typeStr = component.MustNewType("monitoring")

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
		Headers:  map[string]string{"Content-Type": "application/json", "Accept": "application/json"},
	}
}

func createTracesExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	c := cfg.(*Config)
	exp, err := newMonitoringExporter(c, set.Logger)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTraces(
		ctx, set, cfg, exp.pushTraces,
		exporterhelper.WithRetry(exporterhelper.NewDefaultRetrySettings()),
		exporterhelper.WithQueue(exporterhelper.NewDefaultQueueSettings()),
	)
}

func createMetricsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	c := cfg.(*Config)
	exp, err := newMonitoringExporter(c, set.Logger)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewMetrics(
		ctx, set, cfg, exp.pushMetrics,
		exporterhelper.WithRetry(exporterhelper.NewDefaultRetrySettings()),
		exporterhelper.WithQueue(exporterhelper.NewDefaultQueueSettings()),
	)
}

func createLogsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	c := cfg.(*Config)
	exp, err := newMonitoringExporter(c, set.Logger)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewLogs(
		ctx, set, cfg, exp.pushLogs,
		exporterhelper.WithRetry(exporterhelper.NewDefaultRetrySettings()),
		exporterhelper.WithQueue(exporterhelper.NewDefaultQueueSettings()),
	)
}

// -------- Exporter --------

type monitoringExporter struct {
	logger *zap.Logger
	client *http.Client

	headers                        map[string]string
	tracesURL, metricsURL, logsURL string
}

func newMonitoringExporter(cfg *Config, lg *zap.Logger) (*monitoringExporter, error) {
	// Construye headers (defaults + overrides)
	h := map[string]string{"Content-Type": "application/json", "Accept": "application/json"}
	for k, v := range cfg.Headers {
		h[k] = v
	}

	// Completa URLs por señal:
	base := strings.TrimRight(cfg.Endpoint, "/")

	tURL := strings.TrimSpace(cfg.TracesEndpoint)
	mURL := strings.TrimSpace(cfg.MetricsEndpoint)
	lURL := strings.TrimSpace(cfg.LogsEndpoint)

	if tURL == "" && base != "" {
		tURL = base + "/v1/traces"
	}
	if mURL == "" && base != "" {
		mURL = base + "/v1/metrics"
	}
	if lURL == "" && base != "" {
		lURL = base + "/v1/logs"
	}

	// Valida que al menos cada push tenga destino
	if tURL == "" || mURL == "" || lURL == "" {
		return nil, fmt.Errorf("missing endpoints: traces=%q metrics=%q logs=%q (set specific *_endpoint or a global endpoint)", tURL, mURL, lURL)
	}

	to := cfg.Timeout
	if to <= 0 {
		to = 5 * time.Second
	}

	return &monitoringExporter{
		logger:    lg,
		client:    &http.Client{Timeout: to},
		headers:   h,
		tracesURL: tURL, metricsURL: mURL, logsURL: lURL,
	}, nil
}

func (m *monitoringExporter) doPOST(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	for k, v := range m.headers {
		req.Header.Set(k, v)
	}

	resp, err := m.client.Do(req)
	if err != nil {
		return fmt.Errorf("http post failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("http status=%d body=%q", resp.StatusCode, string(b))
	}
	return nil
}

/* -------- TRACES -------- */

func (m *monitoringExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	// payload mínimo (adáptalo a tu JSON destino)
	names := make([]string, 0, 10)
	rs := td.ResourceSpans()
	for i := 0; i < rs.Len() && len(names) < 10; i++ {
		ss := rs.At(i).ScopeSpans()
		for j := 0; j < ss.Len() && len(names) < 10; j++ {
			spans := ss.At(j).Spans()
			for k := 0; k < spans.Len() && len(names) < 10; k++ {
				names = append(names, spans.At(k).Name())
			}
		}
	}
	payload := map[string]any{"traces": map[string]any{"spans": td.SpanCount(), "sample_names": names}}
	body, _ := json.Marshal(payload)

	m.logger.Debug("POST traces", zap.String("url", m.tracesURL), zap.Int("spans", td.SpanCount()))
	return m.doPOST(ctx, m.tracesURL, body)
}

/* -------- METRICS -------- */

func (m *monitoringExporter) pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	out := map[string]any{"metrics": []any{}}
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		props := attrsToMap(rm.Resource().Attributes(), map[string]string{
			"service.name":           "service",
			"deployment.environment": "environment",
			"cloud.region":           "region",
		})

		values := map[string]any{}
		var ts pcommon.Timestamp

		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				mx := ms.At(k)
				switch mx.Type() {
				case pmetric.MetricTypeGauge:
					dps := mx.Gauge().DataPoints()
					if dps.Len() > 0 {
						dp := dps.At(dps.Len() - 1)
						values[mx.Name()] = numberValue(dp)
						if dp.Timestamp() > ts {
							ts = dp.Timestamp()
						}
					}
				case pmetric.MetricTypeSum:
					dps := mx.Sum().DataPoints()
					if dps.Len() > 0 {
						dp := dps.At(dps.Len() - 1)
						values[mx.Name()] = numberValue(dp)
						if dp.Timestamp() > ts {
							ts = dp.Timestamp()
						}
					}
				}
			}
		}

		out["metrics"] = append(out["metrics"].([]any), map[string]any{
			"timestamp":  int64(ts),
			"properties": props,
			"values":     values,
		})
	}
	body, _ := json.Marshal(out)

	m.logger.Debug("POST metrics", zap.String("url", m.metricsURL))
	return m.doPOST(ctx, m.metricsURL, body)
}

func numberValue(dp pmetric.NumberDataPoint) any {
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		return dp.IntValue()
	}
	return dp.DoubleValue()
}

/* -------- LOGS -------- */

func (m *monitoringExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	out := map[string]any{"logs": []any{}}
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		res := attrsToMap(rl.Resource().Attributes(), nil)
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			lrs := sls.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				lr := lrs.At(k)
				out["logs"] = append(out["logs"].([]any), map[string]any{
					"timestamp": int64(lr.Timestamp()),
					"severity":  lr.SeverityText(),
					"body":      valueToIface(lr.Body()),
					"attrs":     attrsToMap(lr.Attributes(), nil),
					"resource":  res,
				})
			}
		}
	}
	body, _ := json.Marshal(out)

	m.logger.Debug("POST logs", zap.String("url", m.logsURL), zap.Int("records", ld.LogRecordCount()))
	return m.doPOST(ctx, m.logsURL, body)
}

/* -------- helpers -------- */

func attrsToMap(attrs pcommon.Map, rename map[string]string) map[string]any {
	out := make(map[string]any, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		key := k
		if rename != nil {
			if alt, ok := rename[k]; ok {
				key = alt
			}
		}
		out[key] = valueToIface(v)
		return true
	})
	return out
}

func valueToIface(v pcommon.Value) any {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return v.Str()
	case pcommon.ValueTypeInt:
		return v.Int()
	case pcommon.ValueTypeDouble:
		return v.Double()
	case pcommon.ValueTypeBool:
		return v.Bool()
	case pcommon.ValueTypeBytes:
		return v.Bytes().AsRaw()
	case pcommon.ValueTypeSlice:
		s := v.Slice()
		arr := make([]any, 0, s.Len())
		for i := 0; i < s.Len(); i++ {
			arr = append(arr, valueToIface(s.At(i)))
		}
		return arr
	case pcommon.ValueTypeMap:
		m := v.Map()
		obj := make(map[string]any, m.Len())
		m.Range(func(k string, vv pcommon.Value) bool {
			obj[k] = valueToIface(vv)
			return true
		})
		return obj
	default:
		return nil
	}
}
