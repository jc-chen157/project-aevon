package sandbox

import (
	"context"
	"log/slog"
)

// Service handles the execution of user-defined scripts.
type Service struct {
	// dependencies (wasm runtime, etc)
}

// NewService creates a new instance of the sandbox service.
func NewService() *Service {
	return &Service{}
}

// Start prepares the sandbox environment.
func (s *Service) Start(ctx context.Context) error {
	slog.Info("Starting Sandbox Service...")
	// TODO: Initialize Wasm runtime
	<-ctx.Done()
	slog.Info("Stopping Sandbox Service...")
	return nil
}
