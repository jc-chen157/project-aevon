package partition

import "hash/fnv"

// Count is the fixed number of logical partitions.
// Never changes after initial deployment — it's a capacity decision, not a scaling decision.
const Count = 256

// For returns the partition ID for a given tenant ID.
// Stable and deterministic: same tenantID always maps to the same partition.
// Uses FNV-32a (stdlib, fast, well-distributed).
func For(tenantID string) int {
	h := fnv.New32a()
	h.Write([]byte(tenantID))
	return int(h.Sum32()) % Count
}
