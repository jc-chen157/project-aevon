package aggregation

import "github.com/shopspring/decimal"

// ExtractDecimal pulls a numeric value from the event's Data map by field name.
// Returns decimal.Zero if the field is missing, empty, or not a recognized numeric type.
// JSON numbers unmarshal to float64 in Go — that's the common path; NewFromFloat
// converts it to an exact decimal representation.
func ExtractDecimal(data map[string]interface{}, field string) decimal.Decimal {
	if field == "" {
		return decimal.Zero
	}
	v, ok := data[field]
	if !ok {
		return decimal.Zero
	}
	switch val := v.(type) {
	case float64:
		return decimal.NewFromFloat(val)
	case float32:
		return decimal.NewFromFloat(float64(val))
	case int:
		return decimal.NewFromInt(int64(val))
	case int64:
		return decimal.NewFromInt(val)
	case int32:
		return decimal.NewFromInt(int64(val))
	case string:
		d, err := decimal.NewFromString(val)
		if err == nil {
			return d
		}
	}
	return decimal.Zero
}
