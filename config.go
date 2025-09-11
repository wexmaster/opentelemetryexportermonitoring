package opentelemetryexportermonitoring

import "time"

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
