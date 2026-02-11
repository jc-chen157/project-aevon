package aggregation

import (
	"github.com/shopspring/decimal"
)

// Aggregator defines the reduce semantics of an aggregation operator.
// To add a new operator: implement this interface and register it in Operators.
// The worker's hot path becomes a single map lookup — no switch.
type Aggregator interface {
	// Initial returns the aggregate value after the very first event for a key.
	// count → 1; sum/min/max → the incoming value itself.
	Initial(incoming decimal.Decimal) decimal.Decimal

	// Apply folds an incoming value into an existing aggregate.
	Apply(current, incoming decimal.Decimal) decimal.Decimal
}

// Operators is the registry of all supported aggregation operators.
// To add a new operator: implement Aggregator and add an entry here.
// No switch statements need to be modified anywhere in the codebase.
var Operators = map[string]Aggregator{
	OpCount: countAgg{},
	OpSum:   sumAgg{},
	OpMin:   minAgg{},
	OpMax:   maxAgg{},
}

// ValidOperator reports whether op is a registered aggregation operator.
func ValidOperator(op string) bool {
	_, ok := Operators[op]
	return ok
}

// countAgg increments by 1 per event. The incoming value is ignored.
type countAgg struct{}

func (countAgg) Initial(_ decimal.Decimal) decimal.Decimal    { return decimal.NewFromInt(1) }
func (countAgg) Apply(cur, _ decimal.Decimal) decimal.Decimal { return cur.Add(decimal.NewFromInt(1)) }

// sumAgg accumulates the sum of incoming values.
type sumAgg struct{}

func (sumAgg) Initial(v decimal.Decimal) decimal.Decimal      { return v }
func (sumAgg) Apply(cur, inc decimal.Decimal) decimal.Decimal { return cur.Add(inc) }

// minAgg tracks the minimum value seen.
type minAgg struct{}

func (minAgg) Initial(v decimal.Decimal) decimal.Decimal { return v }
func (minAgg) Apply(cur, inc decimal.Decimal) decimal.Decimal {
	if inc.LessThan(cur) {
		return inc
	}
	return cur
}

// maxAgg tracks the maximum value seen.
type maxAgg struct{}

func (maxAgg) Initial(v decimal.Decimal) decimal.Decimal { return v }
func (maxAgg) Apply(cur, inc decimal.Decimal) decimal.Decimal {
	if inc.GreaterThan(cur) {
		return inc
	}
	return cur
}
