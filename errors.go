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

	// ErrQueueClosed indicates an operation was attempted on a closed queue.
	ErrQueueClosed = errors.New("queue is closed")

	// ErrBusClosed indicates an operation was attempted on a closed bus.
	ErrBusClosed = errors.New("bus is closed")

	// ErrSubscriptionNotFound is returned when no subscription is found.
	ErrSubscriptionNotFound = errors.New("subscription not found")

	// ErrWorkerAlreadyRunning is returned when a worker is already processing messages.
	ErrWorkerAlreadyRunning = errors.New("worker already running for this queue")

	// ErrOverlappingPatterns is returned when a queue has multiple patterns matching the same subject.
	ErrOverlappingPatterns = errors.New("queue has overlapping patterns which may cause unexpected behavior; only the first subscription will be triggered")
)
