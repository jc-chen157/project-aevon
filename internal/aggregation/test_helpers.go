package aggregation

import (
	"context"
	"fmt"

	coreagg "github.com/aevon-lab/project-aevon/internal/core/aggregation"
)

// InMemoryRuleRepository is a test helper that implements RuleRepository.
type InMemoryRuleRepository struct {
	rules map[string]*coreagg.AggregationRule
}

// NewInMemoryRuleRepository creates a new in-memory rule repository for testing.
func NewInMemoryRuleRepository(rules []*coreagg.AggregationRule) *InMemoryRuleRepository {
	repo := &InMemoryRuleRepository{
		rules: make(map[string]*coreagg.AggregationRule),
	}
	for _, rule := range rules {
		repo.rules[rule.Name] = rule
	}
	return repo
}

func (r *InMemoryRuleRepository) Get(ctx context.Context, name string) (*coreagg.AggregationRule, error) {
	if rule, ok := r.rules[name]; ok {
		return rule, nil
	}
	return nil, fmt.Errorf("rule not found: %s", name)
}

func (r *InMemoryRuleRepository) List(ctx context.Context, sourceEvent string) ([]coreagg.AggregationRule, error) {
	var result []coreagg.AggregationRule
	for _, rule := range r.rules {
		if sourceEvent == "" || rule.SourceEvent == sourceEvent {
			result = append(result, *rule)
		}
	}
	return result, nil
}

func (r *InMemoryRuleRepository) GetRules() []coreagg.AggregationRule {
	result := make([]coreagg.AggregationRule, 0, len(r.rules))
	for _, rule := range r.rules {
		result = append(result, *rule)
	}
	return result
}
