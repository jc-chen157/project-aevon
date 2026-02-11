package storage

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aevon-lab/project-aevon/internal/schema"
)

// FileSystemRepository implements Repository using the local file system.
// It expects a directory structure: root/{tenant_id}/{event_type}/v{version}.[yaml|proto]
// YAML files take precedence over protobuf files if both exist.
type FileSystemRepository struct {
	rootDir string
}

// NewFileSystemRepository creates a new file system backed repository.
func NewFileSystemRepository(rootDir string) *FileSystemRepository {
	return &FileSystemRepository{
		rootDir: rootDir,
	}
}

// Create is not supported in read-only file system mode.
// Developers should add .yaml or .proto files directly to the disk.
func (r *FileSystemRepository) Create(ctx context.Context, s *schema.Schema) error {
	ext := ".yaml"
	if s.Format == schema.FormatProtobuf {
		ext = ".proto"
	}
	return fmt.Errorf("create not supported in filesystem mode: please add %s file directly to %s/%s/%s/v%d%s",
		ext, r.rootDir, s.TenantID, s.Type, s.Version, ext)
}

// Get retrieves a schema from the file system.
// YAML files take precedence over protobuf files.
// Warns if both formats exist for the same version.
func (r *FileSystemRepository) Get(ctx context.Context, key schema.Key) (*schema.Schema, error) {
	yamlPath := filepath.Join(r.rootDir, key.TenantID, key.Type, fmt.Sprintf("v%d.yaml", key.Version))
	protoPath := filepath.Join(r.rootDir, key.TenantID, key.Type, fmt.Sprintf("v%d.proto", key.Version))

	// Check which files exist
	yamlExists := fileExists(yamlPath)
	protoExists := fileExists(protoPath)

	// CRITICAL FIX: Warn if both exist (format conflict)
	if yamlExists && protoExists {
		slog.Warn("Both .yaml and .proto exist for schema - using .yaml (precedence rule)",
			"tenant_id", key.TenantID, "type", key.Type, "version", key.Version)
	}

	// Try YAML first (platform-first philosophy)
	if yamlExists {
		content, err := os.ReadFile(yamlPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read YAML schema: %w", err)
		}
		return r.buildSchema(key, content, schema.FormatYaml), nil
	}

	// Fallback to protobuf
	if protoExists {
		content, err := os.ReadFile(protoPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read protobuf schema: %w", err)
		}
		return r.buildSchema(key, content, schema.FormatProtobuf), nil
	}

	return nil, schema.ErrNotFound
}

// buildSchema constructs a schema.Schema object from file content.
func (r *FileSystemRepository) buildSchema(key schema.Key, content []byte, format schema.Format) *schema.Schema {
	return &schema.Schema{
		ID:          fmt.Sprintf("%s-%s-%d", key.TenantID, key.Type, key.Version),
		TenantID:    key.TenantID,
		Type:        key.Type,
		Version:     key.Version,
		Format:      format,
		Definition:  content,
		Fingerprint: schema.ComputeFingerprint(content),
		State:       schema.StateActive, // Files on disk are always considered active
		StrictMode:  true,               // Default to strict for file-based schemas
		CreatedAt:   time.Now(),         // Synthetic
	}
}

// fileExists checks if a file exists at the given path.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// List scans the directory for schemas matching the criteria.
func (r *FileSystemRepository) List(ctx context.Context, tenantID string, eventType string) ([]*schema.Schema, error) {
	var result []*schema.Schema

	tenantDir := filepath.Join(r.rootDir, tenantID)

	// If eventType is provided, we only check that specific directory
	if eventType != "" {
		typeDir := filepath.Join(tenantDir, eventType)
		schemas, err := r.scanTypeDir(tenantID, eventType, typeDir)
		if err != nil {
			return nil, err
		}
		result = append(result, schemas...)
		return result, nil
	}

	// Otherwise, walk the tenant directory to find all event types
	entries, err := os.ReadDir(tenantDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []*schema.Schema{}, nil
		}
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			eType := entry.Name()
			typeDir := filepath.Join(tenantDir, eType)
			schemas, err := r.scanTypeDir(tenantID, eType, typeDir)
			if err != nil {
				return nil, err
			}
			result = append(result, schemas...)
		}
	}

	return result, nil
}

func (r *FileSystemRepository) scanTypeDir(tenantID, eventType, dirPath string) ([]*schema.Schema, error) {
	var schemas []*schema.Schema
	seenVersions := make(map[int]bool) // Track versions to avoid duplicates

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []*schema.Schema{}, nil
		}
		return nil, err
	}

	// Scan for both .yaml and .proto files
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "v") {
			continue
		}

		var version int
		var ext string

		// Check for .yaml extension
		if strings.HasSuffix(entry.Name(), ".yaml") {
			ext = ".yaml"
			versionStr := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "v"), ext)
			v, err := strconv.Atoi(versionStr)
			if err != nil {
				continue // Skip invalid filenames
			}
			version = v
		} else if strings.HasSuffix(entry.Name(), ".proto") {
			ext = ".proto"
			versionStr := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "v"), ext)
			v, err := strconv.Atoi(versionStr)
			if err != nil {
				continue // Skip invalid filenames
			}
			version = v
		} else {
			continue // Not a schema file
		}

		// Skip if we've already loaded this version (YAML takes precedence)
		if seenVersions[version] {
			continue
		}

		key := schema.Key{TenantID: tenantID, Type: eventType, Version: version}
		// Reuse Get logic to read file and build object (handles YAML precedence)
		s, err := r.Get(context.Background(), key)
		if err == nil {
			schemas = append(schemas, s)
			seenVersions[version] = true
		}
	}
	return schemas, nil
}

// UpdateState is not supported in read-only mode.
func (r *FileSystemRepository) UpdateState(ctx context.Context, key schema.Key, state schema.State) error {
	return fmt.Errorf("update state not supported in filesystem mode")
}

// Delete is not supported in read-only mode.
func (r *FileSystemRepository) Delete(ctx context.Context, key schema.Key) error {
	return fmt.Errorf("delete not supported in filesystem mode: please remove the file")
}
