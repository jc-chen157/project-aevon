package aggregation

import (
	"testing"
	"time"

	coreagg "github.com/aevon-lab/project-aevon/internal/core/aggregation"
)

func TestParseWindowSize(t *testing.T) {
	tests := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		{"1m", time.Minute, false},
		{"5m", 5 * time.Minute, false},
		{"1h", time.Hour, false},
		{"30s", 30 * time.Second, false},
		{"1d", 24 * time.Hour, false},
		{"7d", 7 * 24 * time.Hour, false},
		{"", 0, true},    // empty
		{"0s", 0, true},  // zero duration
		{"-1m", 0, true}, // negative
		{"0d", 0, true},  // zero days
		{"-3d", 0, true}, // negative days
		{"abc", 0, true}, // garbage
		{"1x", 0, true},  // unknown unit
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			ws, err := coreagg.ParseWindowSize(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ws.Size != tt.want {
				t.Errorf("ParseWindowSize(%q).Size = %v, want %v", tt.input, ws.Size, tt.want)
			}
		})
	}
}

func TestBucketFor(t *testing.T) {
	base := time.Date(2026, 2, 3, 10, 35, 42, 123456789, time.UTC)

	tests := []struct {
		name        string
		granularity time.Duration
		want        time.Time
	}{
		{"1-minute", time.Minute, time.Date(2026, 2, 3, 10, 35, 0, 0, time.UTC)},
		{"5-minute", 5 * time.Minute, time.Date(2026, 2, 3, 10, 35, 0, 0, time.UTC)},
		{"1-hour", time.Hour, time.Date(2026, 2, 3, 10, 0, 0, 0, time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := coreagg.BucketFor(base, tt.granularity)
			if !got.Equal(tt.want) {
				t.Errorf("BucketFor(%v, %v) = %v, want %v", base, tt.granularity, got, tt.want)
			}
		})
	}
}
