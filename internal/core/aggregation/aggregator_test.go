package aggregation

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestOperators_InitialAndApply(t *testing.T) {
	tests := []struct {
		name        string
		op          string
		incoming    decimal.Decimal
		current     decimal.Decimal
		next        decimal.Decimal
		wantInitial decimal.Decimal
		wantApply   decimal.Decimal
	}{
		{
			name:        "count",
			op:          OpCount,
			incoming:    decimal.NewFromInt(123),
			current:     decimal.NewFromInt(9),
			next:        decimal.NewFromInt(456),
			wantInitial: decimal.NewFromInt(1),
			wantApply:   decimal.NewFromInt(10),
		},
		{
			name:        "sum",
			op:          OpSum,
			incoming:    decimal.NewFromInt(3),
			current:     decimal.NewFromInt(9),
			next:        decimal.NewFromInt(4),
			wantInitial: decimal.NewFromInt(3),
			wantApply:   decimal.NewFromInt(13),
		},
		{
			name:        "min keeps lower",
			op:          OpMin,
			incoming:    decimal.NewFromInt(3),
			current:     decimal.NewFromInt(9),
			next:        decimal.NewFromInt(4),
			wantInitial: decimal.NewFromInt(3),
			wantApply:   decimal.NewFromInt(4),
		},
		{
			name:        "min keeps current when incoming is higher",
			op:          OpMin,
			incoming:    decimal.NewFromInt(3),
			current:     decimal.NewFromInt(4),
			next:        decimal.NewFromInt(9),
			wantInitial: decimal.NewFromInt(3),
			wantApply:   decimal.NewFromInt(4),
		},
		{
			name:        "max keeps higher",
			op:          OpMax,
			incoming:    decimal.NewFromInt(3),
			current:     decimal.NewFromInt(9),
			next:        decimal.NewFromInt(4),
			wantInitial: decimal.NewFromInt(3),
			wantApply:   decimal.NewFromInt(9),
		},
		{
			name:        "max takes incoming when incoming is higher",
			op:          OpMax,
			incoming:    decimal.NewFromInt(3),
			current:     decimal.NewFromInt(4),
			next:        decimal.NewFromInt(9),
			wantInitial: decimal.NewFromInt(3),
			wantApply:   decimal.NewFromInt(9),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			agg, ok := Operators[tc.op]
			require.True(t, ok)
			require.True(t, tc.wantInitial.Equal(agg.Initial(tc.incoming)))
			require.True(t, tc.wantApply.Equal(agg.Apply(tc.current, tc.next)))
		})
	}
}

func TestValidOperator(t *testing.T) {
	require.True(t, ValidOperator(OpCount))
	require.True(t, ValidOperator(OpSum))
	require.True(t, ValidOperator(OpMin))
	require.True(t, ValidOperator(OpMax))
	require.False(t, ValidOperator("avg"))
	require.False(t, ValidOperator(""))
}
