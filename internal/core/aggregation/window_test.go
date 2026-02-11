package aggregation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseWindowSize(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantSize  time.Duration
		wantError bool
	}{
		{name: "minute", input: "1m", wantSize: time.Minute},
		{name: "hour", input: "2h", wantSize: 2 * time.Hour},
		{name: "days suffix", input: "3d", wantSize: 72 * time.Hour},
		{name: "empty invalid", input: "", wantError: true},
		{name: "negative invalid", input: "-1m", wantError: true},
		{name: "zero invalid", input: "0m", wantError: true},
		{name: "bad day format invalid", input: "xd", wantError: true},
		{name: "unknown unit invalid", input: "10x", wantError: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := ParseWindowSize(tc.input)
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantSize, spec.Size)
		})
	}
}

func TestBucketFor(t *testing.T) {
	ts := time.Date(2026, 2, 11, 10, 35, 42, 123456789, time.UTC)

	require.Equal(t,
		time.Date(2026, 2, 11, 10, 35, 0, 0, time.UTC),
		BucketFor(ts, time.Minute),
	)
	require.Equal(t,
		time.Date(2026, 2, 11, 10, 0, 0, 0, time.UTC),
		BucketFor(ts, time.Hour),
	)
	require.Equal(t,
		time.Date(2026, 2, 11, 0, 0, 0, 0, time.UTC),
		BucketFor(ts, 24*time.Hour),
	)
}
