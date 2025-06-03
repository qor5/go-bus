# PubSub System Enhancements TODO

This document outlines the planned enhancements for the PubSub system.

## 1. Enhanced SubscribeOptions

### 1.1 Scheduled Cleanup Worker

- Implement a background worker for periodic cleanup operations
- Handle expired subscription soft deletion automatically
- Perform AutoDrain operations for subscriptions with enabled autoDrain option
- Clean up orphaned jobs that fall outside subscription boundaries
- Add configurable cleanup intervals and batch processing
- Implement proper error handling and retry mechanisms for cleanup operations
- Add metrics and monitoring for cleanup worker performance
- Add configuration options for cleanup worker behavior (intervals, batch sizes, etc.)
- Add comprehensive tests for all cleanup scenarios including edge cases

### 1.2 Automatic Heartbeat Interval

- Add `WithAutoHeartbeatInterval(duration)` option to SubscribeOptions
- Implement automatic heartbeat mechanism that runs in the background
- **Note: This configuration is client-side only and should NOT be stored in pgbus database tables**
- The heartbeat interval is a runtime behavior setting, not subscription metadata
- Add configuration for heartbeat interval and failure handling
- Ensure heartbeat stops when subscription is unsubscribed
- Add metrics for heartbeat success/failure rates
- Add documentation for heartbeat configuration and monitoring
- Add tests for heartbeat functionality including edge cases
- Ensure proper separation between client-side heartbeat config and server-side subscription data

## Implementation Priority

1. Automatic Heartbeat Interval (improves reliability and operational simplicity)
2. Scheduled Cleanup Worker (improves system maintenance and resource management)

## Notes

- All implementations should maintain backward compatibility
- Performance benchmarks should be conducted before and after each implementation
- Code review required for each enhancement before merging
- Consider the interaction between auto-heartbeat and cleanup worker features
- Ensure cleanup worker properly handles TTL calculations and heartbeat-based expiration
- Design cleanup worker to be configurable and observable for production environments
