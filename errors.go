package bus

import "errors"

// Common errors returned by the bus package.
var (
	// ErrInvalidQueue indicates that the queue name is invalid.
	ErrInvalidQueue = errors.New("queue is invalid")

	// ErrInvalidMessage indicates that the message is invalid.
	ErrInvalidMessage = errors.New("message is invalid")

	// ErrInvalidSubject indicates that the subject is invalid.
	ErrInvalidSubject = errors.New("subject is invalid")

	// ErrInvalidPattern indicates that the pattern is invalid.
	ErrInvalidPattern = errors.New("pattern is invalid")

	// ErrSubscriptionNotFound is returned when no subscription is found.
	ErrSubscriptionNotFound = errors.New("subscription not found")

	// ErrOverlappingPatterns is returned when a queue has multiple patterns matching the same subject.
	ErrOverlappingPatterns = errors.New("queue has overlapping patterns which may cause unexpected behavior; only the first subscription will be triggered")
)
