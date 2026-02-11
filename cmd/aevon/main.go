package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aevon-lab/project-aevon/internal/aggregation"
	corecfg "github.com/aevon-lab/project-aevon/internal/core/config"
	"github.com/aevon-lab/project-aevon/internal/core/storage/postgres"
	"github.com/aevon-lab/project-aevon/internal/ingestion"
	"github.com/aevon-lab/project-aevon/internal/migrations"
	"github.com/aevon-lab/project-aevon/internal/projection"
	"github.com/aevon-lab/project-aevon/internal/schema"
	"github.com/aevon-lab/project-aevon/internal/schema/formats/protobuf"
	"github.com/aevon-lab/project-aevon/internal/schema/formats/yaml"
	schemaStorage "github.com/aevon-lab/project-aevon/internal/schema/storage"
	"github.com/aevon-lab/project-aevon/internal/server"
)

func main() {
	configPath := flag.String("config", "aevon.yaml", "Path to configuration file")
	flag.Parse()

	// 0. Initialize Logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// 1. Load Configuration
	cfg, err := corecfg.Load(*configPath)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}
	slog.Info("Loaded config", "config", cfg)

	cronInterval, err := time.ParseDuration(cfg.Aggregation.EffectiveCronInterval())
	if err != nil {
		slog.Error("Invalid aggregation interval", "value", cfg.Aggregation.EffectiveCronInterval(), "error", err)
		os.Exit(1)
	}

	// 2. Initialize Storage (PostgreSQL)
	dbAdapter, err := postgres.NewAdapter(
		cfg.Database.DSN,
		cfg.Database.MaxOpenConns,
		cfg.Database.MaxIdleConns,
	)
	if err != nil {
		slog.Error("Failed to initialize database", "error", err)
		os.Exit(1)
	}
	defer dbAdapter.Close()

	// 2.1. Run Database Migrations
	if err := migrations.RunMigrations(dbAdapter.DB(), cfg.Database.AutoMigrate); err != nil {
		slog.Error("Failed to run database migrations", "error", err)
		os.Exit(1)
	}

	// 3. Initialize Schema Registry
	var schemaRepo schemaStorage.Repository
	if cfg.Schema.SourceType == "filesystem" {
		schemaRepo = schemaStorage.NewFileSystemRepository(cfg.Schema.Path)
	} else {
		slog.Error("Unsupported schema source type", "type", cfg.Schema.SourceType)
		os.Exit(1)
	}

	registry := schema.NewRegistry(schemaRepo)

	formatRegistry := schema.NewFormatRegistry()
	formatRegistry.RegisterFormat(schema.FormatProtobuf, protobuf.NewCompiler(), protobuf.NewValidator())
	formatRegistry.RegisterFormat(schema.FormatYaml, yaml.NewCompiler(), yaml.NewValidator())

	validator := schema.NewValidator(formatRegistry)

	// 4. Initialize Aggregation (Cron-based batch processing)
	preAggStore := postgres.NewPreAggregateAdapter(dbAdapter.DB())

	// MVP: aggregation runs on fixed 1-minute buckets.
	schedulers := []*aggregation.Scheduler{
		aggregation.NewScheduler(
			cronInterval,
			dbAdapter, // EventStore
			preAggStore,
			cfg.RuleLoading.Rules,
			aggregation.BatchJobOptions{
				BatchSize:   cfg.Aggregation.BatchSize,
				WorkerCount: cfg.Aggregation.WorkerCount,
				BucketSize:  time.Minute,
				BucketLabel: "1m",
			},
		),
	}

	slog.Info("Aggregation scheduler(s) initialized",
		"interval", cronInterval,
		"enabled", cfg.Aggregation.Enabled,
		"bucket_sizes", []string{"1m"},
		"batch_size", cfg.Aggregation.BatchSize,
		"worker_count", cfg.Aggregation.WorkerCount,
	)

	// 5. Initialize Ingestion (no event channel - just write to DB)
	ingestionSvc := ingestion.NewService(registry, validator, dbAdapter, cfg.Server.MaxBodySizeMB)

	// 6. Initialize Projection (query API)
	projectionSvc := projection.NewService(preAggStore, dbAdapter, cfg.RuleLoading.Rules)

	// 7. Initialize Server
	srv := server.New(fmtAddr(cfg.Server.Host, cfg.Server.Port), dbAdapter.DB(), cfg.Server.Mode)
	ingestionSvc.RegisterRoutes(srv.Engine)
	projectionSvc.RegisterRoutes(srv.Engine)

	// 8. Start Services
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start aggregation scheduler(s) in background if enabled
	if cfg.Aggregation.Enabled {
		for _, scheduler := range schedulers {
			go func(s *aggregation.Scheduler) {
				if err := s.Start(ctx); err != nil {
					slog.Error("Scheduler stopped with error", "error", err)
				}
			}(scheduler)
		}
	} else {
		slog.Info("Aggregation scheduler disabled by config")
	}

	// Signal handler → triggers the shutdown sequence below.
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
		<-quit
		slog.Info("Signal received, shutting down...")
		cancel()
	}()

	// HTTP server blocks until ctx is cancelled.
	if err := srv.Run(ctx); err != nil {
		slog.Error("Server stopped with error", "error", err)
	}

	slog.Info("Shutdown complete")
}

func fmtAddr(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}
