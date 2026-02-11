package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoad_ValidConfigAndRules(t *testing.T) {
	root := t.TempDir()
	schemaDir := filepath.Join(root, "schemas")
	rulesDir := filepath.Join(root, "rules")
	requireNoError(t, os.MkdirAll(schemaDir, 0o755))
	requireNoError(t, os.MkdirAll(rulesDir, 0o755))

	requireNoError(t, os.WriteFile(filepath.Join(rulesDir, "count_requests.yaml"), []byte(`
name: "count_requests"
source_event: "api.request"
operator: "count"
`), 0o644))

	cfgPath := filepath.Join(root, "aevon.yaml")
	requireNoError(t, os.WriteFile(cfgPath, []byte(fmt.Sprintf(`
server:
  port: 8080
  host: "127.0.0.1"
  mode: "release"
database:
  type: "postgres"
  dsn: "postgres://dev:dev@localhost:5432/aevon?sslmode=disable"
schema:
  source_type: "filesystem"
  path: "%s"
aggregation:
  config_dir: "%s"
  require_rules: true
  enabled: true
  cron_interval: "2m"
  batch_size: 1000
  worker_count: 2
`, schemaDir, rulesDir)), 0o644))

	cfg, err := Load(cfgPath)
	requireNoError(t, err)
	if len(cfg.RuleLoading.Rules) != 1 {
		t.Fatalf("expected 1 loaded rule, got %d", len(cfg.RuleLoading.Rules))
	}
}

func TestLoad_InvalidCronIntervalFailsStartup(t *testing.T) {
	root := t.TempDir()
	schemaDir := filepath.Join(root, "schemas")
	rulesDir := filepath.Join(root, "rules")
	requireNoError(t, os.MkdirAll(schemaDir, 0o755))
	requireNoError(t, os.MkdirAll(rulesDir, 0o755))
	requireNoError(t, os.WriteFile(filepath.Join(rulesDir, "count_requests.yaml"), []byte(`
name: "count_requests"
source_event: "api.request"
operator: "count"
`), 0o644))

	cfgPath := filepath.Join(root, "aevon.yaml")
	requireNoError(t, os.WriteFile(cfgPath, []byte(fmt.Sprintf(`
database:
  dsn: "postgres://dev:dev@localhost:5432/aevon?sslmode=disable"
schema:
  source_type: "filesystem"
  path: "%s"
aggregation:
  config_dir: "%s"
  cron_interval: "nope"
`, schemaDir, rulesDir)), 0o644))

	_, err := Load(cfgPath)
	if err == nil || !strings.Contains(err.Error(), "invalid aggregation cron interval") {
		t.Fatalf("expected invalid cron interval error, got %v", err)
	}
}

func TestLoad_EnabledAggregationWithoutRulesFailsStartup(t *testing.T) {
	root := t.TempDir()
	schemaDir := filepath.Join(root, "schemas")
	rulesDir := filepath.Join(root, "rules")
	requireNoError(t, os.MkdirAll(schemaDir, 0o755))
	requireNoError(t, os.MkdirAll(rulesDir, 0o755))

	cfgPath := filepath.Join(root, "aevon.yaml")
	requireNoError(t, os.WriteFile(cfgPath, []byte(fmt.Sprintf(`
database:
  dsn: "postgres://dev:dev@localhost:5432/aevon?sslmode=disable"
schema:
  source_type: "filesystem"
  path: "%s"
aggregation:
  config_dir: "%s"
  enabled: true
  require_rules: true
`, schemaDir, rulesDir)), 0o644))

	_, err := Load(cfgPath)
	if err == nil || !strings.Contains(err.Error(), "no aggregation rules found") {
		t.Fatalf("expected no rules error, got %v", err)
	}
}

func TestLoad_InvalidRuleFileFailsStartup(t *testing.T) {
	root := t.TempDir()
	schemaDir := filepath.Join(root, "schemas")
	rulesDir := filepath.Join(root, "rules")
	requireNoError(t, os.MkdirAll(schemaDir, 0o755))
	requireNoError(t, os.MkdirAll(rulesDir, 0o755))

	requireNoError(t, os.WriteFile(filepath.Join(rulesDir, "bad.yaml"), []byte(`
name: "bad_rule"
source_event: "api.request"
operator: "average"
`), 0o644))

	cfgPath := filepath.Join(root, "aevon.yaml")
	requireNoError(t, os.WriteFile(cfgPath, []byte(fmt.Sprintf(`
database:
  dsn: "postgres://dev:dev@localhost:5432/aevon?sslmode=disable"
schema:
  source_type: "filesystem"
  path: "%s"
aggregation:
  config_dir: "%s"
`, schemaDir, rulesDir)), 0o644))

	_, err := Load(cfgPath)
	if err == nil || !strings.Contains(err.Error(), "failed to load aggregation rules") {
		t.Fatalf("expected rule load error, got %v", err)
	}
}

func TestLoad_InvalidServerPortFailsStartup(t *testing.T) {
	root := t.TempDir()
	schemaDir := filepath.Join(root, "schemas")
	rulesDir := filepath.Join(root, "rules")
	requireNoError(t, os.MkdirAll(schemaDir, 0o755))
	requireNoError(t, os.MkdirAll(rulesDir, 0o755))
	requireNoError(t, os.WriteFile(filepath.Join(rulesDir, "count_requests.yaml"), []byte(`
name: "count_requests"
source_event: "api.request"
operator: "count"
`), 0o644))

	cfgPath := filepath.Join(root, "aevon.yaml")
	requireNoError(t, os.WriteFile(cfgPath, []byte(fmt.Sprintf(`
server:
  port: -1
database:
  dsn: "postgres://dev:dev@localhost:5432/aevon?sslmode=disable"
schema:
  source_type: "filesystem"
  path: "%s"
aggregation:
  config_dir: "%s"
`, schemaDir, rulesDir)), 0o644))

	_, err := Load(cfgPath)
	if err == nil || !strings.Contains(err.Error(), "invalid server.port") {
		t.Fatalf("expected invalid server.port error, got %v", err)
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
