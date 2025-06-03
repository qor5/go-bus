package bus

import "github.com/qor5/go-que"

// ExecutionStatus represents the execution status of a subscription.
type ExecutionStatus string

const (
	// ExecutionStatusExecuted indicates the subscription was successfully executed and a job was created.
	ExecutionStatusExecuted ExecutionStatus = "EXECUTED"

	// ExecutionStatusSkippedOverlap indicates the subscription was skipped due to overlapping patterns in the same queue.
	ExecutionStatusSkippedOverlap ExecutionStatus = "SKIPPED_OVERLAP"

	// ExecutionStatusSkippedConflict indicates the subscription was skipped due to unique constraint conflict (que.SkippedID).
	ExecutionStatusSkippedConflict ExecutionStatus = "SKIPPED_CONFLICT"
)

// SubscriptionExecution represents the execution details of a single subscription.
type SubscriptionExecution struct {
	// Subscription contains the matched subscription information.
	Subscription Subscription

	// Status indicates the execution status.
	Status ExecutionStatus

	// Plan contains the plan used for this subscription.
	Plan *que.Plan

	// JobID contains the created job ID when Status is ExecutionStatusExecuted.
	// For all skipped statuses, this will be nil.
	JobID *int64
}

// Dispatch represents the result of a publish or dispatch operation.
type Dispatch struct {
	// Executions contains detailed execution information for all matched subscriptions.
	Executions []*SubscriptionExecution
}

// MatchedCount returns the total number of matched subscriptions.
func (d *Dispatch) MatchedCount() int {
	return len(d.Executions)
}

// ExecutedCount returns the number of subscriptions that were successfully executed.
func (d *Dispatch) ExecutedCount() int {
	count := 0
	for _, exec := range d.Executions {
		if exec.Status == ExecutionStatusExecuted {
			count++
		}
	}
	return count
}

// JobIDs returns all successfully executed job IDs for backward compatibility.
func (d *Dispatch) JobIDs() []int64 {
	var ids []int64
	for _, exec := range d.Executions {
		if exec.Status == ExecutionStatusExecuted && exec.JobID != nil {
			ids = append(ids, *exec.JobID)
		}
	}
	return ids
}

// SkippedByOverlap returns subscriptions that were skipped due to overlapping patterns.
func (d *Dispatch) SkippedByOverlap() []*SubscriptionExecution {
	var skipped []*SubscriptionExecution
	for _, exec := range d.Executions {
		if exec.Status == ExecutionStatusSkippedOverlap {
			skipped = append(skipped, exec)
		}
	}
	return skipped
}

// SkippedByConflict returns subscriptions that were skipped due to unique constraint conflicts.
func (d *Dispatch) SkippedByConflict() []*SubscriptionExecution {
	var skipped []*SubscriptionExecution
	for _, exec := range d.Executions {
		if exec.Status == ExecutionStatusSkippedConflict {
			skipped = append(skipped, exec)
		}
	}
	return skipped
}
