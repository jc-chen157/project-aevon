package aggregation

import (
	"context"
	"log/slog"
	"time"

	"github.com/aevon-lab/project-aevon/internal/core/aggregation"
	"github.com/aevon-lab/project-aevon/internal/core/storage"
)

// Scheduler runs batch aggregation jobs on a periodic interval.
// It is stateless: each tick independently fetches events since last checkpoint.
type Scheduler struct {
	interval    time.Duration
	eventStore  storage.EventStore
	preAggStore PreAggregateStore
	rules       []aggregation.AggregationRule
	opts        BatchJobParameter
}

// NewScheduler creates a cron scheduler for one bucket_size stream.
func NewScheduler(
	interval time.Duration,
	eventStore storage.EventStore,
	preAggStore PreAggregateStore,
	rules []aggregation.AggregationRule,
	opts BatchJobParameter,
) *Scheduler {
	return &Scheduler{
		interval:    interval,
		eventStore:  eventStore,
		preAggStore: preAggStore,
		rules:       rules,
		opts:        opts.normalized(),
	}
}

// Start begins periodic batch aggregation.
// Runs until context is cancelled.
func (s *Scheduler) Start(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	slog.Info("[Scheduler] Starting batch aggregation scheduler",
		"interval", s.interval,
		"bucket_size", s.opts.BucketLabel,
		"batch_size", s.opts.BatchSize,
		"workers", s.opts.WorkerCount,
	)

	// Run initial drain to catch up with any backlog
	s.drainBacklog(ctx)

	for {
		select {
		case <-ticker.C:
			// Drain all pending events, not just one batch
			s.drainBacklog(ctx)
		case <-ctx.Done():
			slog.Info("[Scheduler] Stopping (context cancelled)", "bucket_size", s.opts.BucketLabel)

			shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			slog.Info("[Scheduler] Running final drain before shutdown...", "bucket_size", s.opts.BucketLabel)
			s.drainBacklog(shutdownCtx)
			slog.Info("[Scheduler] Final drain complete", "bucket_size", s.opts.BucketLabel)

			return nil
		}
	}
}

// drainBacklog processes all pending events in batches until the backlog is empty.
// This prevents unbounded staleness during burst ingestion.
func (s *Scheduler) drainBacklog(ctx context.Context) {
	batchCount := 0
	maxConsecutiveBatches := 100 // Safety limit to prevent infinite loop

	for batchCount < maxConsecutiveBatches {
		select {
		case <-ctx.Done():
			slog.Info("[Scheduler] Drain interrupted by context cancellation",
				"bucket_size", s.opts.BucketLabel,
				"batches_processed", batchCount,
			)
			return
		default:
		}

		// Run one batch
		eventsProcessed, err := RunBatchAggregationWithOptionsReturningCount(ctx, s.eventStore, s.preAggStore, s.rules, s.opts)
		if err != nil {
			slog.Error("[Scheduler] Batch aggregation failed",
				"error", err,
				"bucket_size", s.opts.BucketLabel,
				"batch_number", batchCount+1,
			)
			return
		}

		batchCount++

		// If batch processed fewer events than batch size, backlog is drained
		if eventsProcessed < s.opts.BatchSize {
			if batchCount > 1 {
				slog.Info("[Scheduler] Backlog drained",
					"bucket_size", s.opts.BucketLabel,
					"total_batches", batchCount,
				)
			}
			return
		}

		// More events pending, continue draining
		slog.Info("[Scheduler] Backlog detected, continuing to drain",
			"bucket_size", s.opts.BucketLabel,
			"batches_so_far", batchCount,
		)
	}

	// Safety limit reached - log warning but don't error
	slog.Warn("[Scheduler] Max consecutive batches reached, pausing drain",
		"bucket_size", s.opts.BucketLabel,
		"max_batches", maxConsecutiveBatches,
		"note", "Will resume on next tick",
	)
}
