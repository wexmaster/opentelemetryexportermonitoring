package opentelemetryexportermonitoring

import (
	"time"

	"go.opentelemetry.io/collector/exporter"
)

type Config struct {
	exporter.Settings `mapstructure:",squash"`

	Endpoint string        `mapstructure:"endpoint"`
	Timeout  time.Duration `mapstructure:"timeout"`
}
