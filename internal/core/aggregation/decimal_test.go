package aggregation

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestExtractDecimal(t *testing.T) {
	tests := []struct {
		name  string
		data  map[string]interface{}
		field string
		want  decimal.Decimal
	}{
		{
			name:  "empty field name",
			data:  map[string]interface{}{"value": 1},
			field: "",
			want:  decimal.Zero,
		},
		{
			name:  "missing field",
			data:  map[string]interface{}{"value": 1},
			field: "missing",
			want:  decimal.Zero,
		},
		{
			name:  "float64",
			data:  map[string]interface{}{"value": 12.5},
			field: "value",
			want:  decimal.RequireFromString("12.5"),
		},
		{
			name:  "float32",
			data:  map[string]interface{}{"value": float32(7.25)},
			field: "value",
			want:  decimal.RequireFromString("7.25"),
		},
		{
			name:  "int",
			data:  map[string]interface{}{"value": 7},
			field: "value",
			want:  decimal.NewFromInt(7),
		},
		{
			name:  "int32",
			data:  map[string]interface{}{"value": int32(8)},
			field: "value",
			want:  decimal.NewFromInt(8),
		},
		{
			name:  "int64",
			data:  map[string]interface{}{"value": int64(9)},
			field: "value",
			want:  decimal.NewFromInt(9),
		},
		{
			name:  "valid decimal string",
			data:  map[string]interface{}{"value": "42.125"},
			field: "value",
			want:  decimal.RequireFromString("42.125"),
		},
		{
			name:  "invalid string returns zero",
			data:  map[string]interface{}{"value": "not-a-number"},
			field: "value",
			want:  decimal.Zero,
		},
		{
			name:  "unsupported type returns zero",
			data:  map[string]interface{}{"value": true},
			field: "value",
			want:  decimal.Zero,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ExtractDecimal(tc.data, tc.field)
			require.True(t, tc.want.Equal(got), "want=%s got=%s", tc.want.String(), got.String())
		})
	}
}
