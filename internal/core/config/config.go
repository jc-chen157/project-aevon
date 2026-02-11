package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	coreagg "github.com/aevon-lab/project-aevon/internal/core/aggregation"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// Config represents the top-level application config plus resolved rule-loading config.
type Config struct {
	Server      ServerConfig      `koanf:"server"`
	Database    DatabaseConfig    `koanf:"database"`
	Schema      SchemaConfig      `koanf:"schema"`
	Aggregation AggregationConfig `koanf:"aggregation"`

	// RuleLoading is populated by Load after parsing rule files.
	RuleLoading RuleLoadingConfig `koanf:"-"`
}

type ServerConfig struct {
	Port          int    `koanf:"port"`
	Host          string `koanf:"host"`
	MaxBodySizeMB int    `koanf:"max_body_size_mb"`
	Mode          string `koanf:"mode"` // debug | release
}

type DatabaseConfig struct {
	Type         string `koanf:"type"`
	DSN          string `koanf:"dsn"`
	MaxOpenConns int    `koanf:"max_open_conns"`
	MaxIdleConns int    `koanf:"max_idle_conns"`
	AutoMigrate  bool   `koanf:"auto_migrate"`
}

type SchemaConfig struct {
	SourceType string `koanf:"source_type"`
	Path       string `koanf:"path"`
}

type AggregationConfig struct {
	ConfigDir         string `koanf:"config_dir"`
	RequireRules      bool   `koanf:"require_rules"`
	Enabled           bool   `koanf:"enabled"`
	CronInterval      string `koanf:"cron_interval"`  // parsed and validated on startup
	SweepInterval     string `koanf:"sweep_interval"` // legacy alias for cron_interval
	BatchSize         int    `koanf:"batch_size"`
	WorkerCount       int    `koanf:"worker_count"`
	ChannelBufferSize int    `koanf:"channel_buffer_size"`
}

type RuleLoadingConfig struct {
	ConfigDir string
	Rules     []coreagg.AggregationRule
}

func (c AggregationConfig) EffectiveCronInterval() string {
	if c.CronInterval != "" {
		return c.CronInterval
	}
	if c.SweepInterval != "" {
		return c.SweepInterval
	}
	return "2m"
}

func (c *Config) Validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server.port %d (must be 1-65535)", c.Server.Port)
	}
	if strings.TrimSpace(c.Server.Host) == "" {
		return fmt.Errorf("server.host is required")
	}
	if c.Server.MaxBodySizeMB <= 0 {
		return fmt.Errorf("server.max_body_size_mb must be > 0")
	}
	if c.Server.Mode != "debug" && c.Server.Mode != "release" {
		return fmt.Errorf("invalid server.mode %q (must be debug or release)", c.Server.Mode)
	}

	if strings.TrimSpace(c.Database.DSN) == "" {
		return fmt.Errorf("database.dsn is required")
	}
	if c.Database.MaxOpenConns <= 0 {
		return fmt.Errorf("database.max_open_conns must be > 0")
	}
	if c.Database.MaxIdleConns <= 0 {
		return fmt.Errorf("database.max_idle_conns must be > 0")
	}
	if c.Database.Type != "" && c.Database.Type != "postgres" {
		return fmt.Errorf("unsupported database.type %q", c.Database.Type)
	}

	if c.Schema.SourceType != "filesystem" {
		return fmt.Errorf("unsupported schema.source_type %q", c.Schema.SourceType)
	}
	if strings.TrimSpace(c.Schema.Path) == "" {
		return fmt.Errorf("schema.path is required")
	}
	if _, err := os.Stat(c.Schema.Path); err != nil {
		return fmt.Errorf("schema.path %q is not accessible: %w", c.Schema.Path, err)
	}

	if strings.TrimSpace(c.Aggregation.ConfigDir) == "" {
		return fmt.Errorf("aggregation.config_dir is required")
	}
	interval, err := time.ParseDuration(c.Aggregation.EffectiveCronInterval())
	if err != nil {
		return fmt.Errorf("invalid aggregation cron interval %q: %w", c.Aggregation.EffectiveCronInterval(), err)
	}
	if interval <= 0 {
		return fmt.Errorf("aggregation cron interval must be > 0")
	}
	if c.Aggregation.BatchSize <= 0 {
		return fmt.Errorf("aggregation.batch_size must be > 0")
	}
	if c.Aggregation.WorkerCount <= 0 {
		return fmt.Errorf("aggregation.worker_count must be > 0")
	}
	if c.Aggregation.ChannelBufferSize < 0 {
		return fmt.Errorf("aggregation.channel_buffer_size must be >= 0")
	}

	return nil
}

// Load parses config from file + env, validates it, then loads and validates aggregation rules.
func Load(configPath string) (*Config, error) {
	k := koanf.New(".")

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
		"aggregation.require_rules":       true,
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

	if configPath != "" {
		if err := k.Load(file.Provider(configPath), yaml.Parser()); err != nil {
			return nil, fmt.Errorf("failed to load config file: %w", err)
		}
	}

	if err := k.Load(env.Provider("AEVON_", ".", func(s string) string {
		return strings.Replace(strings.ToLower(strings.TrimPrefix(s, "AEVON_")), "__", ".", -1)
	}), nil); err != nil {
		return nil, fmt.Errorf("failed to load env vars: %w", err)
	}

	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	repo, err := coreagg.NewFileSystemRuleRepository(cfg.Aggregation.ConfigDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load aggregation rules: %w", err)
	}
	rules := repo.GetRules()
	if cfg.Aggregation.Enabled && cfg.Aggregation.RequireRules && len(rules) == 0 {
		return nil, fmt.Errorf("no aggregation rules found in %q", cfg.Aggregation.ConfigDir)
	}

	cfg.RuleLoading = RuleLoadingConfig{
		ConfigDir: cfg.Aggregation.ConfigDir,
		Rules:     rules,
	}

	return &cfg, nil
}
