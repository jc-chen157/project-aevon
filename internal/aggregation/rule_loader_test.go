package aggregation

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	coreagg "github.com/aevon-lab/project-aevon/internal/core/aggregation"
)

// writeRule is a test helper that writes a single rule YAML file into dir.
func writeRule(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestFileSystemRuleRepository_LoadAndList(t *testing.T) {
	dir := t.TempDir()
	writeRule(t, dir, "count_requests.yaml", `
name: "count_requests"
source_event: "api.request"
operator: "count"
`)
	writeRule(t, dir, "sum_latency.yaml", `
name: "sum_latency"
source_event: "api.request"
operator: "sum"
field: "latency"
`)

	repo, err := coreagg.NewFileSystemRuleRepository(dir)
	if err != nil {
		t.Fatalf("NewFileSystemRuleRepository: %v", err)
	}

	// List all
	all, err := repo.List(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("List all: got %d rules, want 2", len(all))
	}

	// List filtered by source event
	filtered, err := repo.List(context.Background(), "api.request")
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 2 {
		t.Errorf("List api.request: got %d, want 2", len(filtered))
	}

	noMatch, err := repo.List(context.Background(), "invoice.created")
	if err != nil {
		t.Fatal(err)
	}
	if len(noMatch) != 0 {
		t.Errorf("List invoice.created: got %d, want 0", len(noMatch))
	}
}

func TestFileSystemRuleRepository_Get(t *testing.T) {
	dir := t.TempDir()
	writeRule(t, dir, "my_rule.yaml", `
name: "my_rule"
source_event: "order.placed"
operator: "max"
field: "amount"
`)

	repo, err := coreagg.NewFileSystemRuleRepository(dir)
	if err != nil {
		t.Fatal(err)
	}

	rule, err := repo.Get(context.Background(), "my_rule")
	if err != nil {
		t.Fatal(err)
	}
	if rule.Name != "my_rule" {
		t.Errorf("Name = %q, want %q", rule.Name, "my_rule")
	}
	if rule.SourceEvent != "order.placed" {
		t.Errorf("SourceEvent = %q", rule.SourceEvent)
	}
	if rule.WindowSize != time.Minute {
		t.Errorf("WindowSize = %v, want 1m", rule.WindowSize)
	}
	if rule.Operator != "max" {
		t.Errorf("Operator = %q", rule.Operator)
	}
	if rule.Field != "amount" {
		t.Errorf("Field = %q", rule.Field)
	}
	if rule.Fingerprint == "" {
		t.Error("Fingerprint is empty")
	}

	// Not found
	_, err = repo.Get(context.Background(), "nonexistent")
	if err == nil {
		t.Error("Get nonexistent: expected error, got nil")
	}
}

func TestFileSystemRuleRepository_Fingerprint_Changes(t *testing.T) {
	dir := t.TempDir()
	content := "name: \"fp_rule\"\nsource_event: \"x\"\noperator: \"count\"\n"
	writeRule(t, dir, "fp_rule.yaml", content)

	repo1, err := coreagg.NewFileSystemRuleRepository(dir)
	if err != nil {
		t.Fatal(err)
	}
	r1, _ := repo1.Get(context.Background(), "fp_rule")

	// Modify the file content
	writeRule(t, dir, "fp_rule.yaml", content+"# comment\n")

	repo2, err := coreagg.NewFileSystemRuleRepository(dir)
	if err != nil {
		t.Fatal(err)
	}
	r2, _ := repo2.Get(context.Background(), "fp_rule")

	if r1.Fingerprint == r2.Fingerprint {
		t.Error("Fingerprint did not change after file modification")
	}
}

func TestFileSystemRuleRepository_InvalidOperator(t *testing.T) {
	dir := t.TempDir()
	writeRule(t, dir, "bad.yaml", `
name: "bad_rule"
source_event: "x"
operator: "average"
`)

	_, err := coreagg.NewFileSystemRuleRepository(dir)
	if err == nil {
		t.Fatal("expected error for unsupported operator, got nil")
	}
}

func TestFileSystemRuleRepository_WindowSizeCustomizationDisabled(t *testing.T) {
	dir := t.TempDir()
	writeRule(t, dir, "bad_window.yaml", `
name: "bad_window"
source_event: "x"
window_size: "5m"
operator: "count"
`)

	_, err := coreagg.NewFileSystemRuleRepository(dir)
	if err == nil {
		t.Fatal("expected error for window_size customization, got nil")
	}
}

func TestFileSystemRuleRepository_MissingDir(t *testing.T) {
	// Non-existent directory is valid — zero rules.
	repo, err := coreagg.NewFileSystemRuleRepository("/tmp/does-not-exist-aevon-test")
	if err != nil {
		t.Fatalf("unexpected error for missing dir: %v", err)
	}
	rules, _ := repo.List(context.Background(), "")
	if len(rules) != 0 {
		t.Errorf("expected 0 rules from missing dir, got %d", len(rules))
	}
}

func TestFileSystemRuleRepository_SkipsEmptyFiles(t *testing.T) {
	dir := t.TempDir()
	writeRule(t, dir, "empty.yaml", "")
	writeRule(t, dir, "comment_only.yaml", "# just a comment\n")
	writeRule(t, dir, "real.yaml", `
name: "real"
source_event: "x"
operator: "count"
`)

	repo, err := coreagg.NewFileSystemRuleRepository(dir)
	if err != nil {
		t.Fatal(err)
	}
	rules, _ := repo.List(context.Background(), "")
	if len(rules) != 1 {
		t.Errorf("expected 1 rule (skipping empty/comment files), got %d", len(rules))
	}
}

func TestFileSystemRuleRepository_DuplicateRuleName(t *testing.T) {
	dir := t.TempDir()
	writeRule(t, dir, "first.yaml", `
name: "dup_rule"
source_event: "x"
operator: "count"
`)
	writeRule(t, dir, "second.yaml", `
name: "dup_rule"
source_event: "y"
operator: "sum"
field: "amount"
`)

	_, err := coreagg.NewFileSystemRuleRepository(dir)
	if err == nil {
		t.Fatal("expected error for duplicate rule name, got nil")
	}
}

func TestFileSystemRuleRepository_MissingSourceEvent(t *testing.T) {
	dir := t.TempDir()
	writeRule(t, dir, "no_source.yaml", `
name: "no_source"
operator: "count"
`)

	_, err := coreagg.NewFileSystemRuleRepository(dir)
	if err == nil {
		t.Fatal("expected error for missing source_event, got nil")
	}
}
