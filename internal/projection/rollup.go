package projection

import (
	"time"

	"github.com/aevon-lab/project-aevon/internal/core/aggregation"
	"github.com/shopspring/decimal"
)

// rollupTotal sums all aggregates into a single value for the entire range.
// For count/sum: adds all values together.
// For min/max: finds the global min/max across all buckets.
func (s *Service) rollupTotal(
	aggregates []aggregation.AggregateState,
	start, end time.Time,
) []AggregateValue {
	if len(aggregates) == 0 {
		return []AggregateValue{{
			WindowStart: start,
			WindowEnd:   end,
			Value:       decimal.Zero,
			EventCount:  0,
		}}
	}

	operator := aggregates[0].Operator
	total := decimal.Zero
	totalEvents := int64(0)
	initialized := false

	for _, agg := range aggregates {
		switch operator {
		case "count", "sum":
			total = total.Add(agg.Value)
		case "min":
			if !initialized || agg.Value.LessThan(total) {
				total = agg.Value
				initialized = true
			}
		case "max":
			if !initialized || agg.Value.GreaterThan(total) {
				total = agg.Value
				initialized = true
			}
		}
		totalEvents += agg.EventCount
	}

	return []AggregateValue{{
		WindowStart: start,
		WindowEnd:   end,
		Value:       total,
		EventCount:  totalEvents,
	}}
}

// convertToValues converts buckets to AggregateValue format using caller-provided bucket duration.
func (s *Service) convertToValues(aggregates []aggregation.AggregateState, bucketDuration time.Duration) []AggregateValue {
	if bucketDuration <= 0 {
		bucketDuration = time.Minute
	}

	values := make([]AggregateValue, 0, len(aggregates))

	for _, agg := range aggregates {
		values = append(values, AggregateValue{
			WindowStart: agg.WindowStart,
			WindowEnd:   agg.WindowStart.Add(bucketDuration),
			Value:       agg.Value,
			EventCount:  agg.EventCount,
		})
	}

	return values
}

// rollupToHour groups 1-minute buckets into hourly buckets.
// For count/sum: adds values within each hour.
// For min/max: finds min/max within each hour.
func (s *Service) rollupToHour(
	aggregates []aggregation.AggregateState,
	start, end time.Time,
) []AggregateValue {
	if len(aggregates) == 0 {
		return s.emptyHourlyBuckets(start, end)
	}

	operator := aggregates[0].Operator

	// Group aggregates by hour
	hourlyBuckets := make(map[time.Time][]aggregation.AggregateState)
	for _, agg := range aggregates {
		hourStart := agg.WindowStart.Truncate(time.Hour)
		hourlyBuckets[hourStart] = append(hourlyBuckets[hourStart], agg)
	}

	// Generate results for each hour in the range
	var results []AggregateValue
	currentHour := start.Truncate(time.Hour)
	for currentHour.Before(end) {
		bucket := hourlyBuckets[currentHour]

		value := decimal.Zero
		eventCount := int64(0)
		initialized := false

		if len(bucket) > 0 {
			for _, agg := range bucket {
				switch operator {
				case "count", "sum":
					value = value.Add(agg.Value)
				case "min":
					if !initialized || agg.Value.LessThan(value) {
						value = agg.Value
						initialized = true
					}
				case "max":
					if !initialized || agg.Value.GreaterThan(value) {
						value = agg.Value
						initialized = true
					}
				}
				eventCount += agg.EventCount
			}
		}

		results = append(results, AggregateValue{
			WindowStart: currentHour,
			WindowEnd:   currentHour.Add(time.Hour),
			Value:       value,
			EventCount:  eventCount,
		})

		currentHour = currentHour.Add(time.Hour)
	}

	return results
}

// rollupToDay groups 1-minute buckets into daily buckets.
// For count/sum: adds values within each day.
// For min/max: finds min/max within each day.
func (s *Service) rollupToDay(
	aggregates []aggregation.AggregateState,
	start, end time.Time,
) []AggregateValue {
	if len(aggregates) == 0 {
		return s.emptyDailyBuckets(start, end)
	}

	operator := aggregates[0].Operator

	// Group aggregates by day
	dailyBuckets := make(map[time.Time][]aggregation.AggregateState)
	for _, agg := range aggregates {
		dayStart := truncateToDay(agg.WindowStart)
		dailyBuckets[dayStart] = append(dailyBuckets[dayStart], agg)
	}

	// Generate results for each day in the range
	var results []AggregateValue
	currentDay := truncateToDay(start)
	endDayExclusive := truncateToDay(end)
	if end.After(endDayExclusive) {
		endDayExclusive = endDayExclusive.Add(24 * time.Hour)
	}

	for currentDay.Before(endDayExclusive) {
		bucket := dailyBuckets[currentDay]

		value := decimal.Zero
		eventCount := int64(0)
		initialized := false

		if len(bucket) > 0 {
			for _, agg := range bucket {
				switch operator {
				case "count", "sum":
					value = value.Add(agg.Value)
				case "min":
					if !initialized || agg.Value.LessThan(value) {
						value = agg.Value
						initialized = true
					}
				case "max":
					if !initialized || agg.Value.GreaterThan(value) {
						value = agg.Value
						initialized = true
					}
				}
				eventCount += agg.EventCount
			}
		}

		results = append(results, AggregateValue{
			WindowStart: currentDay,
			WindowEnd:   currentDay.Add(24 * time.Hour),
			Value:       value,
			EventCount:  eventCount,
		})

		currentDay = currentDay.Add(24 * time.Hour)
	}

	return results
}

// emptyHourlyBuckets creates zero-valued hourly buckets for a time range.
func (s *Service) emptyHourlyBuckets(start, end time.Time) []AggregateValue {
	var results []AggregateValue
	currentHour := start.Truncate(time.Hour)
	for currentHour.Before(end) {
		results = append(results, AggregateValue{
			WindowStart: currentHour,
			WindowEnd:   currentHour.Add(time.Hour),
			Value:       decimal.Zero,
			EventCount:  0,
		})
		currentHour = currentHour.Add(time.Hour)
	}
	return results
}

// emptyDailyBuckets creates zero-valued daily buckets for a time range.
func (s *Service) emptyDailyBuckets(start, end time.Time) []AggregateValue {
	var results []AggregateValue
	currentDay := truncateToDay(start)
	endDayExclusive := truncateToDay(end)
	if end.After(endDayExclusive) {
		endDayExclusive = endDayExclusive.Add(24 * time.Hour)
	}

	for currentDay.Before(endDayExclusive) {
		results = append(results, AggregateValue{
			WindowStart: currentDay,
			WindowEnd:   currentDay.Add(24 * time.Hour),
			Value:       decimal.Zero,
			EventCount:  0,
		})
		currentDay = currentDay.Add(24 * time.Hour)
	}
	return results
}

// truncateToDay truncates a timestamp to the start of the day (00:00:00 UTC).
func truncateToDay(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, t.Location())
}
