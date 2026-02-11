package aggregation

import (
	"fmt"
	"time"
)

// WindowSpec represents a parsed and validated window size.
type WindowSpec struct {
	Size time.Duration
}

// ParseWindowSize parses a duration string into a WindowSpec.
// Supports Go duration syntax (e.g., "10s", "1m", "1h") plus "Xd" for days.
func ParseWindowSize(s string) (WindowSpec, error) {
	if s == "" {
		return WindowSpec{}, fmt.Errorf("window_size must not be empty")
	}

	// Handle "d" suffix (days) — not supported by time.ParseDuration.
	if len(s) > 1 && s[len(s)-1] == 'd' {
		var days int
		if _, err := fmt.Sscanf(s, "%dd", &days); err != nil {
			return WindowSpec{}, fmt.Errorf("invalid window_size %q: %w", s, err)
		}
		if days <= 0 {
			return WindowSpec{}, fmt.Errorf("window_size must be positive, got %q", s)
		}
		return WindowSpec{Size: time.Duration(days) * 24 * time.Hour}, nil
	}

	d, err := time.ParseDuration(s)
	if err != nil {
		return WindowSpec{}, fmt.Errorf("invalid window_size %q: %w", s, err)
	}
	if d <= 0 {
		return WindowSpec{}, fmt.Errorf("window_size must be positive, got %q", s)
	}
	return WindowSpec{Size: d}, nil
}

// BucketFor truncates a timestamp to the nearest granularity boundary.
// This is the atomic unit of aggregation storage.
// Example: BucketFor(10:35:42, 1*time.Minute) → 10:35:00
func BucketFor(t time.Time, granularity time.Duration) time.Time {
	return t.Truncate(granularity)
}
