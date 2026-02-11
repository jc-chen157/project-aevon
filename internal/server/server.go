package server

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type Server struct {
	Engine *gin.Engine
	Addr   string
	db     *sql.DB
}

// HealthChecker is an interface for components that can report their health status.
type HealthChecker interface {
	Ping(ctx context.Context) error
}

func New(addr string, db *sql.DB, mode string) *Server {
	// Set Gin mode based on configuration
	if mode == "debug" {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.Default()

	s := &Server{
		Engine: r,
		Addr:   addr,
		db:     db,
	}

	// Health check endpoint with database connectivity verification
	r.GET("/health", s.healthHandler)

	return s
}

func (s *Server) healthHandler(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	// Check database connectivity
	if s.db != nil {
		if err := s.db.PingContext(ctx); err != nil {
			slog.Error("Health check failed: database unreachable", "error", err)
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status": "unhealthy",
				"error":  "database unreachable",
			})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"status":   "healthy",
		"database": "connected",
	})
}

func (s *Server) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:    s.Addr,
		Handler: s.Engine,
	}

	slog.Info("Starting HTTP Server...", "address", s.Addr)

	go func() {
		<-ctx.Done()
		slog.Info("Stopping HTTP Server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("HTTP Server forced to shutdown", "error", err)
		}
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
