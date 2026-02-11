package protobuf

import (
	"context"
	"fmt"
	"strings"

	"github.com/aevon-lab/project-aevon/internal/schema"
	"github.com/bufbuild/protocompile"
)

// Compiler compiles protobuf schema definitions.
type Compiler struct{}

// NewCompiler creates a new protobuf compiler.
func NewCompiler() *Compiler {
	return &Compiler{}
}

// Compile parses a .proto definition and returns the compiled schema.
// The proto must define exactly one top-level message.
func (c *Compiler) Compile(ctx context.Context, s *schema.Schema) (*schema.CompiledSchema, error) {
	// Validate format
	if s.Format != schema.FormatProtobuf {
		return nil, fmt.Errorf("expected protobuf format, got %s", s.Format)
	}

	// Create a virtual file name for the proto
	fileName := fmt.Sprintf("%s_v%d.proto", strings.ReplaceAll(s.Type, ".", "_"), s.Version)

	// Create a resolver that provides the proto content
	resolver := &singleFileResolver{
		fileName: fileName,
		content:  string(s.Definition),
	}

	// Configure the compiler
	compiler := protocompile.Compiler{
		Resolver:       protocompile.WithStandardImports(resolver),
		SourceInfoMode: protocompile.SourceInfoNone,
	}

	// Compile the proto file
	files, err := compiler.Compile(ctx, fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to compile proto: %w", err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no files compiled")
	}

	// Get the file descriptor
	fd := files[0]

	// Find the message descriptor - expect exactly one top-level message
	messages := fd.Messages()
	if messages.Len() == 0 {
		return nil, fmt.Errorf("proto must define at least one message")
	}

	// Use the first message as the schema
	msgDesc := messages.Get(0)

	return &schema.CompiledSchema{
		EventType:       s.Type,
		Version:         s.Version,
		Format:          schema.FormatProtobuf,
		StrictMode:      s.StrictMode,
		ProtoDescriptor: &msgDesc,
	}, nil
}

// singleFileResolver provides proto content for compilation.
type singleFileResolver struct {
	fileName string
	content  string
}

func (r *singleFileResolver) FindFileByPath(path string) (protocompile.SearchResult, error) {
	if path == r.fileName {
		return protocompile.SearchResult{
			Source: strings.NewReader(r.content),
		}, nil
	}
	return protocompile.SearchResult{}, fmt.Errorf("file not found: %s", path)
}
