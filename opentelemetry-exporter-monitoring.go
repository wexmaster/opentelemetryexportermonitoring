package opentelemetryexportermonitoring

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"


	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Prueba de local
var typeStr = component.MustNewType("monitoring")

// define the stability level of the exporter actually "Beta"
const stability = component.StabilityLevelBeta

type Config struct {
	// Namespace de usuario en Atenea (obligatorio)
	NS string `mapstructure:"ns"`
	// Region (obligatorio)
	Region string `mapstructure:"region"`
	// MetricSets (obligatorio para métricas)
	MetricSets string `mapstructure:"metricsets"`
	// URLs separadas por señal
	Traces  bool `mapstructure:"traces"`
	Metrics bool `mapstructure:"metrics"`
	Logs    bool `mapstructure:"logs"`
	// MrId para logs (valor por defecto si no viene en los logs)
	MrId string `mapstructure:"mrid"`
	// eventos log en local
	LogFile      string `mapstructure:"log_file"`
	MaxLines     int    `mapstructure:"max_lines"`
	MaxBodyBytes int    `mapstructure:"max_body_bytes"`

	// Cabeceras HTTP opcionales
	Headers map[string]string `mapstructure:"headers"`

	CaCertFile     string `mapstructure:"ca_cert_file"`
	ClientCertFile string `mapstructure:"client_cert_file"`
	ClientKeyFile  string `mapstructure:"client_key_file"`

	// Nuevos bloques de config del helper
	exporterhelper.TimeoutConfig `mapstructure:",squash"`
	QueueSettings                exporterhelper.QueueBatchConfig `mapstructure:"sending_queue"`
	RetrySettings                configretry.BackOffConfig       `mapstructure:"retry_on_failure"`
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
		NS:             "user.z123456",
		Region:         "work-0x.xxxxxx.xxxxxxx",
		MetricSets:     "opentelemetry_metrics",
		Traces:         false,
		Metrics:        false,
		Logs:           false,
		CaCertFile:     "/workspaces/otelcol-costum/certs/workrootca.crt",
		ClientCertFile: "/workspaces/otelcol-costum/certs/client.crt",
		ClientKeyFile:  "/workspaces/otelcol-costum/certs/client.key",
		Headers:        map[string]string{"Content-Type": "application/json"},
		TimeoutConfig:  exporterhelper.NewDefaultTimeoutConfig(),
		QueueSettings:  exporterhelper.NewDefaultQueueConfig(),
		RetrySettings:  configretry.NewDefaultBackOffConfig(),
		MrId:           "unknown-mr",
		LogFile:        "/var/log/otel_failed_requests.log",
		MaxLines:       30000,
		MaxBodyBytes:   2048,
	}
}

func sanitizeName(name string) string {
	// Reemplazar caracteres no permitidos por Atenea
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, ":", "_")
	return name
}

func createTracesExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	c := cfg.(*Config)
	exp, err := newMonitoringExporter(c, set.Logger)
	if err != nil {
		return nil, err
	}
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
	if err != nil {
		return nil, err
	}
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
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewLogs(
		ctx, set, cfg, exp.pushLogs,
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.RetrySettings),
	)
}

type monitoringExporter struct {
	ns           string
	region       string
	metricsets   string
	traces       bool
	metrics      bool
	logs         bool
	headers      map[string]string
	logger       *zap.Logger
	client       *http.Client
	mrid         string
	LogFile      string
	MaxLines     int
	MaxBodyBytes int
}

func newMonitoringExporter(cfg *Config, lg *zap.Logger) (*monitoringExporter, error) {

	// Crear transporte HTTP con soporte para certificados CA personalizados
	var transport *http.Transport
	if cfg.CaCertFile != "" {
		caCert, err := os.ReadFile(cfg.CaCertFile)
		if err != nil {
			return nil, fmt.Errorf("error al leer el archivo de certificado CA: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("error al cargar el certificado CA")
		}

		if len(cfg.Headers) == 1 {
			// Cargar certificado y clave del cliente
			clientCert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
			if err != nil {
				return nil, fmt.Errorf("error al cargar el certificado de cliente: %w", err)
			}
			// Configuración TLS
			tlsConfig := &tls.Config{
				Certificates: []tls.Certificate{clientCert},
				RootCAs:      caCertPool,
				MinVersion:   tls.VersionTLS12,
			}

			transport = &http.Transport{
				TLSClientConfig:       tlsConfig,
				TLSHandshakeTimeout:   35 * time.Second,
				IdleConnTimeout:       90 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			}

		} else {
			transport = &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs: caCertPool,
				},
			}
		}

	} else {
		transport = http.DefaultTransport.(*http.Transport)
	}

	// Crear cliente HTTP con el transporte configurado
	httpClient := &http.Client{
		Timeout:   cfg.Timeout,
		Transport: transport,
	}

	return &monitoringExporter{
		ns:           cfg.NS,
		region:       cfg.Region,
		metricsets:   cfg.MetricSets,
		traces:       cfg.Traces,
		metrics:      cfg.Metrics,
		logs:         cfg.Logs,
		headers:      cfg.Headers,
		logger:       lg,
		client:       httpClient,
		mrid:         cfg.MrId,
		LogFile:      cfg.LogFile,
		MaxLines:     cfg.MaxLines,
		MaxBodyBytes: cfg.MaxBodyBytes,
	}, nil
}

// Trace Started

type outSpan struct {
	MRID           string                 `json:"mrId"`
	SpanID         string                 `json:"spanId"`
	StartDate      uint64                 `json:"startDate"`
	FinishDate     uint64                 `json:"finishDate"`
	Name           string                 `json:"name"`
	TraceID        string                 `json:"traceId"`
	ParentSpan     string                 `json:"parentSpan,omitempty"`
	Properties     map[string]interface{} `json:"properties,omitempty"`
	ParentSpanTest string                 `json:"parentSpanTest,omitempty"`
	CreateUrl      string                 `json:"CreateUrl,omitempty"`
}

// Config opcional para construir el parentSpan
type transformCfg struct {
	UserNamespace string // ej: "user.123456"
	// IncludeResourceAttrs bool // activa si quieres mezclar attrs de resource en properties
}

func spanHexToUUID(hexStr string) string {
	// si es traceId (32 hex chars = 16 bytes)
	s := strings.ToLower(strings.TrimSpace(hexStr))
	// quitar guiones por si viniera ya con forma uuid
	s = strings.ReplaceAll(s, "-", "")

	switch len(s) {
	case 32: // traceId
		return fmt.Sprintf("%s", s[0:32])
	case 16: // spanId -> pad a 32
		return fmt.Sprintf("%s", s[0:16])
	default:
		return ""
	}
}

// Transforma OTel  -> Atenea JSON
func (m *monitoringExporter) transformTraces(td ptrace.Traces, cfg transformCfg) ([]byte, []string, error) {
	var out []outSpan
	var createUrls []string
	rsSlice := td.ResourceSpans()
	for i := 0; i < rsSlice.Len(); i++ {
		rs := rsSlice.At(i)

		// Extra opcional: atributos de resource para properties
		resAttrs := rs.Resource().Attributes()

		ssSlice := rs.ScopeSpans()
		for j := 0; j < ssSlice.Len(); j++ {
			ss := ssSlice.At(j)

			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				sp := spans.At(k)

				// MRID: 1) atributo de span "mrid" 2) default m.mrid del resource configurado en collector-config.yaml
				mrID := getAttrString(sp.Attributes(), "mrid")
				if mrID == "" {
					mrID = getAttrString(resAttrs, "mrid")
				}
				if mrID == "" {
					mrID = m.mrid // usar el mrid como config default
				}

				// capturamos parentSpan
				parentSpanAtt := getAttrString(sp.Attributes(), "parentspan")
				if parentSpanAtt == "" {
					parentSpanAtt = getAttrString(resAttrs, "parentspan")
				}
				if parentSpanAtt == "" {
					parentSpanAtt = m.mrid // usar mrid de config default si no viene por ningun sitio este dato
				}
				// capturamos el namespace
				nsAtt := getAttrString(sp.Attributes(), "ns")
				if nsAtt == "" {
					nsAtt = getAttrString(resAttrs, "ns")
				}
				if nsAtt == "" {
					nsAtt = m.ns // usar namespace de config default
				}
				// capturamos region
				regionAtt := getAttrString(sp.Attributes(), "region")
				if regionAtt == "" {
					regionAtt = getAttrString(resAttrs, "region")
				}
				if regionAtt == "" {
					regionAtt = m.region // usar region de config default
				}

				// Properties: copia atributos del span salvo los internos
				props := map[string]interface{}{}
				sp.Attributes().Range(func(k string, v pcommon.Value) bool {
					// Limpiar el nombre de la clave
					cleanKey := sanitizeName(k)
					props[cleanKey] = v.AsRaw()
					return true
				})
				// no duplicar mrid (ya lo usamos como mrId)
				delete(props, "mrid")
				delete(props, "parentspan")
				delete(props, "ns")
				delete(props, "region")

				item := outSpan{
					MRID:       mrID,
					SpanID:     spanHexToUUID(sp.SpanID().String()), // hex de 16 bytes
					StartDate:  uint64(sp.StartTimestamp()),         // ya viene en ns
					FinishDate: uint64(sp.EndTimestamp()),           // ns
					Name:       sp.Name(),
					TraceID:    spanHexToUUID(sp.TraceID().String()), // hex de 16 bytes (32 chars)
				}
				if len(props) > 0 {
					item.Properties = props
				}

				if regionAtt != "" && regionAtt != "unknown" && nsAtt != "" && nsAtt != "unknown" {
					createUrls = append(createUrls, fmt.Sprintf("https://rho.%s/v1/ns/%s/mrs/%s/spans", regionAtt, nsAtt, mrID)) // Guardar CreateUrl

				}
				if !sp.ParentSpanID().IsEmpty() {
					item.ParentSpan = fmt.Sprintf("ns/%s/mrs/%s/spans/%s", nsAtt, parentSpanAtt, spanHexToUUID(sp.ParentSpanID().String()))

				}

				out = append(out, item)
			}
		}
	}

	// Serializar el JSON transformado
	outJSON, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return nil, nil, fmt.Errorf("error serializing transformed traces: %w", err)
	}

	return outJSON, createUrls, nil
}

func getAttrString(attrs pcommon.Map, key string) string {
	if v, ok := attrs.Get(key); ok {
		return v.AsString()
	}
	return ""
}

func (m *monitoringExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	if !m.traces { // Verificar si el envío de traces está habilitado
		m.logger.Sugar().Warnln("El envío de traces está deshabilitado, no se realizará el POST.")
		return nil
	}

	// Transformar al formato requerido
	out, createUrls, err := m.transformTraces(td, transformCfg{UserNamespace: m.ns})
	if err != nil {
		fmt.Printf("transformTraces ERROR: %v\n", err)
		return err
	}

	// Deserializar `out` (JSON) a un slice de `outSpan`
	var spans []outSpan
	if err := json.Unmarshal(out, &spans); err != nil {
		return fmt.Errorf("error unmarshaling transformed traces: %w", err)
	}

	// Agrupar los datos por URL
	urlToBody := make(map[string][]outSpan)
	for i, url := range createUrls {
		urlToBody[url] = append(urlToBody[url], spans[i])
	}

	// Enviar los datos agrupados
	for url, spans := range urlToBody {
		body, err := json.Marshal(spans)
		if err != nil {
			return fmt.Errorf("error marshaling spans for URL %s: %w", url, err)
		}

		// Enviar los datos a la URL correspondiente
		if err := m.postJSON(ctx, url, body); err != nil {
			return fmt.Errorf("error sending data to URL %s: %w", url, err)
		}
	}

	return nil
}

func (m *monitoringExporter) processMetrics(md pmetric.Metrics) ([]byte, error) {
	// Crear una estructura para las métricas transformadas
	type transformedMetric struct {
		Timestamp  int64                  `json:"timestamp"`
		Properties map[string]interface{} `json:"properties"`
		Values     map[string]interface{} `json:"values"`
	}

	var transformedMetrics []transformedMetric

	// Iterar sobre las métricas para transformarlas
	resourceMetrics := md.ResourceMetrics()
	for i := 0; i < resourceMetrics.Len(); i++ {
		resourceMetric := resourceMetrics.At(i)
		resourceAttrs := resourceMetric.Resource().Attributes().AsRaw()

		scopeMetrics := resourceMetric.ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			scopeMetric := scopeMetrics.At(j)
			metrics := scopeMetric.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)

				// Iterar sobre los puntos de datos de la métrica
				switch metric.Type() {
				case pmetric.MetricTypeSum:
					dataPoints := metric.Sum().DataPoints()
					for l := 0; l < dataPoints.Len(); l++ {
						dataPoint := dataPoints.At(l)
						properties := make(map[string]interface{})
						for k, v := range resourceAttrs {
							// formateamos los . por _ en los nombres de las keys
							cleanKey := sanitizeName(k)
							properties[cleanKey] = v

						}
						properties["name"] = metric.Name()
						dataPoint.Attributes().Range(func(k string, v pcommon.Value) bool {
							cleanKey := sanitizeName(k)
							properties[cleanKey] = v.AsRaw()

							return true
						})

						values := map[string]interface{}{
							metric.Name(): dataPoint.IntValue(),
						}

						transformedMetrics = append(transformedMetrics, transformedMetric{
							Timestamp:  dataPoint.Timestamp().AsTime().UnixNano(),
							Properties: properties,
							Values:     values,
						})
					}
				case pmetric.MetricTypeGauge:
					dataPoints := metric.Gauge().DataPoints()
					for l := 0; l < dataPoints.Len(); l++ {
						dataPoint := dataPoints.At(l)
						properties := make(map[string]interface{})
						for k, v := range resourceAttrs {
							cleanKey := sanitizeName(k)
							properties[cleanKey] = v

						}
						properties["name"] = metric.Name()
						dataPoint.Attributes().Range(func(k string, v pcommon.Value) bool {
							cleanKey := sanitizeName(k)
							properties[cleanKey] = v.AsRaw()

							return true
						})

						values := map[string]interface{}{
							metric.Name(): dataPoint.IntValue(),
						}

						transformedMetrics = append(transformedMetrics, transformedMetric{
							Timestamp:  dataPoint.Timestamp().AsTime().UnixNano(),
							Properties: properties,
							Values:     values,
						})
					}
				}
			}
		}
	}

	// Serializar las metricas transformadas a JSON
	data, err := json.Marshal(map[string]interface{}{"metrics": transformedMetrics})
	if err != nil {
		return nil, fmt.Errorf("error al transformar métricas: %w", err)
	}
    urlcomose := fmt.Sprintf(("https://mu.%s/v0/ns/%s/metric-sets/%s:addMeasurements"), m.region, m.ns, m.metricsets)
	return data, nil
}

func (m *monitoringExporter) pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	if !m.metrics { // Verificar si el envío de metricas esta habilitado
		m.logger.Sugar().Warnln("El envío de métricas está deshabilitado, no se realizará el POST.")
		return nil
	}

	// Procesar las metricas antes de enviarlas
	data, err := m.processMetrics(md)
	if err != nil {
		return err
	}

	// Enviar los datos procesados a postJSON
	return m.postJSON(ctx, urlcomose, data)
}

// Crear una estructura para los logs transformados
type transformedLog struct {
	MrId         string                 `json:"mrid"`
	Level        string                 `json:"level"`
	Message      string                 `json:"message"`
	CreationDate int64                  `json:"creationDate"`
	SpanId       string                 `json:"spanId"`
	TraceId      string                 `json:"traceId"`
	Properties   map[string]interface{} `json:"properties"`
}

func (m *monitoringExporter) processLogs(ld plog.Logs) ([]byte, error) {

	var transformedLogs []transformedLog

	// Iterar sobre los logs para transformarlos
	resourceLogs := ld.ResourceLogs()
	for i := 0; i < resourceLogs.Len(); i++ {
		resourceLog := resourceLogs.At(i)
		scopeLogs := resourceLog.ScopeLogs()
		for j := 0; j < scopeLogs.Len(); j++ {
			scopeLog := scopeLogs.At(j)
			logRecords := scopeLog.LogRecords()
			for k := 0; k < logRecords.Len(); k++ {
				logRecord := logRecords.At(k)

				// Crear un mapa para las propiedades
				properties := make(map[string]interface{})
				logRecord.Attributes().Range(func(k string, v pcommon.Value) bool {
					cleanKey := sanitizeName(k)
					properties[cleanKey] = v.AsRaw()
					return true
				})

				// Crear el log transformado
				transformedLogs = append(transformedLogs, transformedLog{
					MrId:         m.mrid, // Valor predeterminado
					Level:        logRecord.SeverityText(),
					Message:      logRecord.Body().AsString(),
					CreationDate: logRecord.Timestamp().AsTime().UnixNano(),
					SpanId:       spanHexToUUID(logRecord.SpanID().String()),  // Cambiado a String()
					TraceId:      spanHexToUUID(logRecord.TraceID().String()), // Cambiado a String()
					Properties:   properties,
				})
			}
		}
	}

	// Serializar los logs transformados a JSON
	data, err := json.Marshal(transformedLogs)
	if err != nil {
		return nil, fmt.Errorf("error al transformar logs: %w", err)
	}

	// Imprimir el resultado transformado para depuración
	fmt.Printf("Transformed Logs JSON: %s\n", string(data))
	return data, nil
}

// Transforma OTel Logs -> Atenea JSON
func (m *monitoringExporter) transformLogs(ld plog.Logs, cfg transformCfg) ([]byte, []string, error) {
	type transformedLog struct {
		MrId         string                 `json:"mrid"`
		Level        string                 `json:"level"`
		Message      string                 `json:"message"`
		CreationDate int64                  `json:"creationDate"`
		SpanId       string                 `json:"spanId"`
		TraceId      string                 `json:"traceId"`
		Properties   map[string]interface{} `json:"properties"`
		//CreateUrl    string                 `json:"CreateUrl,omitempty"`
	}

	var transformedLogs []transformedLog
	var createUrls []string

	// Iterar sobre los logs para transformarlos
	resourceLogs := ld.ResourceLogs()
	for i := 0; i < resourceLogs.Len(); i++ {
		resourceLog := resourceLogs.At(i)
		resourceAttrs := resourceLog.Resource().Attributes().AsRaw()

		scopeLogs := resourceLog.ScopeLogs()
		for j := 0; j < scopeLogs.Len(); j++ {
			scopeLog := scopeLogs.At(j)
			logRecords := scopeLog.LogRecords()
			for k := 0; k < logRecords.Len(); k++ {
				logRecord := logRecords.At(k)
				mrID := getAttrString(logRecord.Attributes(), "mrid")
				if mrID == "" {
					mrID = getAttrString(resourceLog.Resource().Attributes(), "mrid") //"service.name")
				}
				if mrID == "" {
					mrID = m.mrid // usar el namespace como fallback
				}
				// capturamos parentSpan
				parentSpanAtt := getAttrString(logRecord.Attributes(), "parentspan")
				if parentSpanAtt == "" {
					parentSpanAtt = getAttrString(resourceLog.Resource().Attributes(), "parentspan")
				}
				if parentSpanAtt == "" {
					parentSpanAtt = m.mrid // usar mrid de config default si no viene por ningun sitio este dato
				}
				// capturamos el namespace
				nsAtt := getAttrString(logRecord.Attributes(), "ns")
				if nsAtt == "" {
					nsAtt = getAttrString(resourceLog.Resource().Attributes(), "ns")
				}
				if nsAtt == "" {
					nsAtt = m.ns // usar namespace de config default
				}
				// capturamos region
				regionAtt := getAttrString(logRecord.Attributes(), "region")
				if regionAtt == "" {
					regionAtt = getAttrString(resourceLog.Resource().Attributes(), "region")
				}
				if regionAtt == "" {
					regionAtt = m.region // usar region de config default
				}

				// Si quisieras añadir attrs del resource:

				// Crear un mapa para las propiedades
				properties := make(map[string]interface{})
				for k, v := range resourceAttrs {
					cleanKey := sanitizeName(k)
					properties[cleanKey] = v
				}
				logRecord.Attributes().Range(func(k string, v pcommon.Value) bool {
					cleanKey := sanitizeName(k)
					properties[cleanKey] = v.AsRaw()
					return true
				})
				delete(properties, "mrid")
				delete(properties, "parentspan")
				delete(properties, "ns")
				delete(properties, "region")

				// Crear el log transformado
				transformedLog := transformedLog{
					MrId:         mrID, // Usar el namespace como MrId
					Level:        logRecord.SeverityText(),
					Message:      logRecord.Body().AsString(),
					CreationDate: logRecord.Timestamp().AsTime().UnixNano(),
					SpanId:       spanHexToUUID(logRecord.SpanID().String()),
					TraceId:      spanHexToUUID(logRecord.TraceID().String()),
					Properties:   properties,
				}

				// Generar CreateUrl si es necesario
				if regionAtt != "" && regionAtt != "unknown" && nsAtt != "" && nsAtt != "unknown" {
					createUrls = append(createUrls, fmt.Sprintf("https://omega.%s/v1/ns/%s/logs", regionAtt, nsAtt))

				}

				transformedLogs = append(transformedLogs, transformedLog)
			}
		}
	}

	// Serializar los logs transformados a JSON
	data, err := json.MarshalIndent(transformedLogs, "", "  ")
	if err != nil {
		return nil, nil, fmt.Errorf("error serializing transformed logs: %w", err)
	}

	return data, createUrls, nil
}


func (m *monitoringExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	// if m.logsURL == "" {
	// 	return nil
	// }
	if !m.logs { // Verificar si el envío de logs está habilitado
		m.logger.Sugar().Warnln("El envío de logs está deshabilitado, no se realizará el POST.")
		return nil
	}
	// Transformar los logs al formato requerido
	out, createUrls, err := m.transformLogs(ld, transformCfg{UserNamespace: m.mrid})
	if err != nil {
		fmt.Printf("transformLogs ERROR: %v\n", err)
		return err
	}

	// Deserializar `out` (JSON) a un slice de `transformedLog`
	var logs []transformedLog
	if err := json.Unmarshal(out, &logs); err != nil {
		return fmt.Errorf("error unmarshaling transformed logs: %w", err)
	}
	urlToBody := make(map[string][]transformedLog)
	for i, url := range createUrls {
		urlToBody[url] = append(urlToBody[url], logs[i])
	}

	for url, logs := range urlToBody {
		body, err := json.Marshal(logs)
		if err != nil {
			return fmt.Errorf("error marshaling logs for URL %s: %w", url, err)
		}
		// Log claro del JSON que realmente enviamos
		//fmt.Printf("Custom Logs JSON to send >>> %s\n %s", string(body), url)
		// Enviar los datos a la URL
		if err := m.postJSON(ctx, url, body); err != nil {
			return fmt.Errorf("error sending data to URL %s: %w", url, err)
		}
		// Enviar los datos transformados

	}
	return nil
}


func (m *monitoringExporter) postJSON(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		m.logFailedRequest(err, url, body)
		return err
	}


	req.Header.Set("Content-Type", "application/json")
	for k, v := range m.headers {
		req.Header.Set(k, v)
	}

	resp, err := m.client.Do(req)
	if err != nil {
		m.logFailedRequest(err, url, body)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err = fmt.Errorf("monitoring exporter: %s -> HTTP %d", url, resp.StatusCode)
		m.logFailedRequest(err, url, body)
		return err
	}

	m.logger.Debug("monitoring/exporter POST OK",
		zap.String("url", url),
		zap.Int("status", resp.StatusCode),
		zap.Int("bytes", len(body)),
	)
	return nil
}

// logFailedRequest guarda errores y cuerpos fallidos en un archivo rotativo con límite de líneas
func (m *monitoringExporter) logFailedRequest(err error, url string, body []byte) {
	// Truncar body si excede el límite
	truncatedBody := body
	if len(body) > m.MaxBodyBytes {
		truncatedBody = append(body[:m.MaxBodyBytes], []byte("...<truncated>")...)
	}

	entry := fmt.Sprintf(
		"[%s] ERROR: %v\nURL: %s\nBODY: %s\n\n",
		time.Now().Format(time.RFC3339),
		err,
		url,
		string(truncatedBody),
	)

	// Contar líneas actuales
	data, _ := os.ReadFile(m.LogFile)
	lines := bytes.Count(data, []byte{'\n'})

	// Si supera el máximo, truncamos el archivo (lo vaciamos)
	if lines >= m.MaxLines {
		if err := os.Truncate(m.LogFile, 0); err != nil {
			m.logger.Error("no se pudo truncar el log local", zap.Error(err))
			return
		}
	}

	// Escribir nueva entrada
	f, ferr := os.OpenFile(m.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if ferr != nil {
		m.logger.Error("no se pudo escribir log local", zap.Error(ferr))
		return
	}
	defer f.Close()

	_, _ = f.WriteString(entry)
}
