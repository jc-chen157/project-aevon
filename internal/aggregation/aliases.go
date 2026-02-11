package aggregation

import core "github.com/aevon-lab/project-aevon/internal/core/aggregation"

// Re-export core aggregation types for package-level compatibility.
type Aggregator = core.Aggregator
type AggregateKey = core.AggregateKey
type AggregateState = core.AggregateState
type AggregationRule = core.AggregationRule
type WindowSpec = core.WindowSpec
type RuleRepository = core.RuleRepository

var (
	Operators                   = core.Operators
	ValidOperator               = core.ValidOperator
	OpCount                     = core.OpCount
	OpSum                       = core.OpSum
	OpMin                       = core.OpMin
	OpMax                       = core.OpMax
	BucketFor                   = core.BucketFor
	ParseWindowSize             = core.ParseWindowSize
	NewFileSystemRuleRepository = core.NewFileSystemRuleRepository
)
