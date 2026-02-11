package aggregation

import (
	"context"
	"fmt"
)

// InMemoryRuleRepository is a test helper that implements RuleRepository.
type InMemoryRuleRepository struct {
	rules map[string]*AggregationRule
}

// NewInMemoryRuleRepository creates a new in-memory rule repository for testing.
func NewInMemoryRuleRepository(rules []*AggregationRule) *InMemoryRuleRepository {
	repo := &InMemoryRuleRepository{
		rules: make(map[string]*AggregationRule),
	}
	for _, rule := range rules {
		repo.rules[rule.Name] = rule
	}
	return repo
}

func (r *InMemoryRuleRepository) Get(ctx context.Context, name string) (*AggregationRule, error) {
	if rule, ok := r.rules[name]; ok {
		return rule, nil
	}
	return nil, fmt.Errorf("rule not found: %s", name)
}

func (r *InMemoryRuleRepository) List(ctx context.Context, sourceEvent string) ([]AggregationRule, error) {
	var result []AggregationRule
	for _, rule := range r.rules {
		if sourceEvent == "" || rule.SourceEvent == sourceEvent {
			result = append(result, *rule)
		}
	}
	return result, nil
}

func (r *InMemoryRuleRepository) GetRules() []AggregationRule {
	result := make([]AggregationRule, 0, len(r.rules))
	for _, rule := range r.rules {
		result = append(result, *rule)
	}
	return result
}
