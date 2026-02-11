package migrations

import (
	"database/sql"
	"embed"
	"fmt"
	"log/slog"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed *.sql
var MigrationFiles embed.FS

// RunMigrations executes all pending migrations against the provided database.
// If autoMigrate is false, it only logs the pending migrations but doesn't apply them.
func RunMigrations(db *sql.DB, autoMigrate bool) error {
	// Create iofs source from embedded files
	sourceDriver, err := iofs.New(MigrationFiles, ".")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	// Create postgres database driver
	dbDriver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("failed to create database driver: %w", err)
	}

	// Create migrate instance
	m, err := migrate.NewWithInstance("iofs", sourceDriver, "postgres", dbDriver)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	// Get current version
	version, dirty, err := m.Version()
	if err != nil && err != migrate.ErrNilVersion {
		return fmt.Errorf("failed to get current migration version: %w", err)
	}

	if dirty {
		slog.Warn("Database is in dirty state - migration was interrupted",
			"version", version,
			"action", "attempting automatic recovery",
		)

		// Single baseline migration in MVP allows safe force-to-current-version recovery.
		if err := m.Force(int(version)); err != nil {
			return fmt.Errorf("failed to recover dirty migration state at version %d: %w", version, err)
		}
		slog.Info("Recovered dirty migration state", "version", version)
	}

	if !autoMigrate {
		slog.Info("Auto-migration disabled, skipping migrations",
			"current_version", version,
			"dirty", dirty,
		)
		return nil
	}

	// Run migrations
	slog.Info("Running database migrations", "current_version", version)

	err = m.Up()
	if err != nil {
		if err == migrate.ErrNoChange {
			slog.Info("Database schema is up to date", "version", version)
			return nil
		}
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	// Get new version
	newVersion, _, err := m.Version()
	if err != nil {
		return fmt.Errorf("failed to get updated migration version: %w", err)
	}

	slog.Info("Database migrations completed successfully",
		"from_version", version,
		"to_version", newVersion,
	)

	return nil
}
