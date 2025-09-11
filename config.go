package opentelemetryexportermonitoring

import "time"

type Config struct {
	// Opción global (opcional). Si la pones y no defines los específicos,
	// se usará <Endpoint>/v1/{traces|metrics|logs}.
	Endpoint string `mapstructure:"endpoint"`

	// Endpoints completos por señal (recomendado).
	TracesEndpoint  string `mapstructure:"traces_endpoint"`
	MetricsEndpoint string `mapstructure:"metrics_endpoint"`
	LogsEndpoint    string `mapstructure:"logs_endpoint"`

	Timeout time.Duration     `mapstructure:"timeout"`
	Headers map[string]string `mapstructure:"headers"`
}
