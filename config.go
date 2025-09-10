package opentelemetryexportermonitoring

import "time"

type Config struct {
	Endpoint string        `mapstructure:"endpoint"`
	Timeout  time.Duration `mapstructure:"timeout"`
}
