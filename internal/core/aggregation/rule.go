package aggregation

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// AggregationRule defines a single aggregation rule.
// Rules are loaded at startup from YAML files and fingerprinted for staleness detection.
// No CEL or dynamic evaluation in MVP — rules match on SourceEvent only.
type AggregationRule struct {
	Name        string        `yaml:"name"`
	SourceEvent string        `yaml:"source_event"`
	WindowSize  time.Duration // Fixed to 1m in MVP
	Operator    string        `yaml:"operator"` // count, sum, min, max
	Field       string        `yaml:"field"`    // event data field to aggregate; empty for count
	Fingerprint string        // SHA-256 of the raw YAML file; computed at load time
}

// rawRule is the on-disk YAML shape.
// window_size is optional and locked to "1m" in MVP.
type rawRule struct {
	Name        string `yaml:"name"`
	SourceEvent string `yaml:"source_event"`
	WindowSize  string `yaml:"window_size"` // optional; must be "1m" if provided
	Operator    string `yaml:"operator"`
	Field       string `yaml:"field"`
}

// RuleRepository defines the interface for loading aggregation rules.
type RuleRepository interface {
	// Get returns the rule with the given name, or an error if not found.
	Get(ctx context.Context, name string) (*AggregationRule, error)

	// List returns all loaded rules, optionally filtered by source event type.
	List(ctx context.Context, sourceEvent string) ([]AggregationRule, error)

	// GetRules returns all rules as a slice (for batch processing).
	GetRules() []AggregationRule
}

// FileSystemRuleRepository loads aggregation rules from *.yaml files in a directory.
// Each file contains exactly one rule at the top level. Rules are loaded once at
// startup and cached in memory — no hot reload in MVP.
type FileSystemRuleRepository struct {
	dir   string
	rules map[string]AggregationRule // keyed by Name
}

// NewFileSystemRuleRepository creates a new repository and eagerly loads all rules
// from dir. Returns an error if any rule file is malformed or invalid.
func NewFileSystemRuleRepository(dir string) (*FileSystemRuleRepository, error) {
	repo := &FileSystemRuleRepository{
		dir:   dir,
		rules: make(map[string]AggregationRule),
	}
	if err := repo.load(); err != nil {
		return nil, err
	}
	return repo, nil
}

func (r *FileSystemRuleRepository) load() error {
	info, err := os.Stat(r.dir)
	if os.IsNotExist(err) {
		return nil // no rules directory — valid (zero rules configured)
	}
	if err != nil {
		return fmt.Errorf("aggregation rule dir: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("aggregation rule path %q is not a directory", r.dir)
	}

	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return fmt.Errorf("reading aggregation rule dir: %w", err)
	}

	for _, e := range entries {
		if e.IsDir() || (!strings.HasSuffix(e.Name(), ".yaml") && !strings.HasSuffix(e.Name(), ".yml")) {
			continue
		}

		path := filepath.Join(r.dir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("reading rule file %s: %w", path, err)
		}

		var raw rawRule
		if err := yaml.Unmarshal(data, &raw); err != nil {
			return fmt.Errorf("parsing rule file %s: %w", path, err)
		}
		if raw.Name == "" {
			continue // skip empty / comment-only files
		}

		if raw.SourceEvent == "" {
			return fmt.Errorf("rule %q: source_event must not be empty", raw.Name)
		}

		if !ValidOperator(raw.Operator) {
			return fmt.Errorf("rule %q: unsupported operator %q", raw.Name, raw.Operator)
		}

		if raw.WindowSize != "" && raw.WindowSize != "1m" {
			return fmt.Errorf("rule %q: window_size customization is disabled in MVP (use 1m)", raw.Name)
		}

		fingerprint := fmt.Sprintf("%x", sha256.Sum256(data))

		if _, exists := r.rules[raw.Name]; exists {
			return fmt.Errorf("rule %q: duplicate rule name (check multiple YAML files)", raw.Name)
		}

		r.rules[raw.Name] = AggregationRule{
			Name:        raw.Name,
			SourceEvent: raw.SourceEvent,
			WindowSize:  time.Minute,
			Operator:    raw.Operator,
			Field:       raw.Field,
			Fingerprint: fingerprint,
		}
	}
	return nil
}

// Get returns the rule with the given name, or an error if not found.
func (r *FileSystemRuleRepository) Get(_ context.Context, name string) (*AggregationRule, error) {
	rule, ok := r.rules[name]
	if !ok {
		return nil, fmt.Errorf("aggregation rule %q not found", name)
	}
	return &rule, nil
}

// List returns all loaded rules, optionally filtered by source event type.
func (r *FileSystemRuleRepository) List(_ context.Context, sourceEvent string) ([]AggregationRule, error) {
	var out []AggregationRule
	for _, rule := range r.rules {
		if sourceEvent != "" && rule.SourceEvent != sourceEvent {
			continue
		}
		out = append(out, rule)
	}
	return out, nil
}

// GetRules returns all rules as a slice (for batch processing).
func (r *FileSystemRuleRepository) GetRules() []AggregationRule {
	rules := make([]AggregationRule, 0, len(r.rules))
	for _, rule := range r.rules {
		rules = append(rules, rule)
	}
	return rules
}
