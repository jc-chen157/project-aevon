package config

import (
	"fmt"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// Config represents the top-level configuration for Aevon.
type Config struct {
	Server      ServerConfig      `koanf:"server"`
	Database    DatabaseConfig    `koanf:"database"`
	Schema      SchemaConfig      `koanf:"schema"`
	Aggregation AggregationConfig `koanf:"aggregation"`
}

// ServerConfig holds the HTTP server configuration.
type ServerConfig struct {
	Port          int    `koanf:"port"`
	Host          string `koanf:"host"`
	MaxBodySizeMB int    `koanf:"max_body_size_mb"`
	Mode          string `koanf:"mode"` // "debug" or "release"
}

// DatabaseConfig holds the database connection settings.
type DatabaseConfig struct {
	Type         string `koanf:"type"`
	DSN          string `koanf:"dsn"`
	MaxOpenConns int    `koanf:"max_open_conns"`
	MaxIdleConns int    `koanf:"max_idle_conns"`
	AutoMigrate  bool   `koanf:"auto_migrate"`
}

// SchemaConfig holds settings for schema management.
type SchemaConfig struct {
	SourceType string `koanf:"source_type"`
	Path       string `koanf:"path"`
}

// AggregationConfig holds settings for event aggregation.
type AggregationConfig struct {
	ConfigDir         string `koanf:"config_dir"`
	Enabled           bool   `koanf:"enabled"`
	CronInterval      string `koanf:"cron_interval"`  // parsed as time.Duration in main
	SweepInterval     string `koanf:"sweep_interval"` // legacy alias for cron_interval
	BatchSize         int    `koanf:"batch_size"`
	WorkerCount       int    `koanf:"worker_count"`
	ChannelBufferSize int    `koanf:"channel_buffer_size"` // buffered chan capacity
}

// EffectiveCronInterval returns the active cron interval while keeping
// backward compatibility with the legacy sweep_interval key.
func (c AggregationConfig) EffectiveCronInterval() string {
	if c.CronInterval != "" {
		return c.CronInterval
	}
	if c.SweepInterval != "" {
		return c.SweepInterval
	}
	return "2m"
}

// Load loads the configuration from the given file path and environment variables.
func Load(configPath string) (*Config, error) {
	k := koanf.New(".")

	// 1. Defaults
	defaults := map[string]interface{}{
		"server.port":                     8080,
		"server.host":                     "0.0.0.0",
		"server.max_body_size_mb":         1,
		"server.mode":                     "release",
		"database.type":                   "postgres",
		"database.dsn":                    "aevon.db",
		"database.max_open_conns":         25,
		"database.max_idle_conns":         25,
		"database.auto_migrate":           true,
		"schema.source_type":              "filesystem",
		"schema.path":                     "./schemas",
		"aggregation.config_dir":          "./config/aggregations",
		"aggregation.enabled":             true,
		"aggregation.cron_interval":       "2m",
		"aggregation.sweep_interval":      "",
		"aggregation.batch_size":          50000,
		"aggregation.worker_count":        10,
		"aggregation.channel_buffer_size": 1024,
	}
	for key, value := range defaults {
		k.Set(key, value)
	}

	// 2. Load from file
	if configPath != "" {
		if err := k.Load(file.Provider(configPath), yaml.Parser()); err != nil {
			// If file is specified but fails, we might want to log or error.
			// However, for simplified UX, if it's the default "aevon.yaml" and strictly required?
			// The user requirement says "core aevon.yaml config" is expected.
			// We return error to be safe.
			return nil, fmt.Errorf("failed to load config file: %w", err)
		}
	}

	// 3. Load from Environment Variables
	// AEVON_SERVER__PORT=9090 overrides server.port
	if err := k.Load(env.Provider("AEVON_", ".", func(s string) string {
		return strings.Replace(strings.ToLower(
			strings.TrimPrefix(s, "AEVON_")), "__", ".", -1)
	}), nil); err != nil {
		return nil, fmt.Errorf("failed to load env vars: %w", err)
	}

	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}
