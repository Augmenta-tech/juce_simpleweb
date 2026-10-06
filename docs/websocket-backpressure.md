# WebSocket outbound backpressure

Every connection now has finite outbound limits: **64 MiB of queued payload/header bytes and 256 messages**, including the in-flight message. Both limits are configurable before the server starts (`config.max_send_queue_bytes`, `config.max_send_queue_messages`). Zero rejects all actual WebSocket frames; it is not unlimited. The limits apply to WS, WSS and HTTP-upgraded sockets, for reliable sends, poll responses, latest-only frames and control frames alike.

This bounds queue retention, not whole-process RSS: serialization/compression buffers, allocator capacity, socket buffers, connection count and other application allocations are separate.

## Delivery and overload

`send` remains ordered while the connection is healthy. `send_latest` retains the current write and at most one pending replaceable snapshot. All latest-only sends on a connection share that slot; callers must use it only for mutually superseding complete snapshots. It is not safe for unrelated streams or partial updates without application-level aggregation.

At either limit, the connection rejects the new message with `no_buffer_space`, becomes terminal for further sends, logs the remote endpoint/queued bytes/message count/rejected payload size, and posts one socket shutdown. It does not put another close frame behind the congested data. The active async-write buffer remains alive until its completion handler runs; queued callbacks are then failed and buffers released. A rejected-send callback can execute synchronously, outside the queue lock.

Reliable does not mean lossless across a disconnection: applications must reconnect and resynchronize. Silently dropping discrete events would be worse than explicitly terminating the overloaded connection. Healthy clients have independent queues and are not disconnected by another client's overflow.

`Connection::get_send_queue_stats()` exposes bytes, messages, replacement count and terminal state. `get_send_error()` preserves the overload cause for the endpoint error handler.

## Tests

`tests/send_queue_test.cpp` covers 100,000 snapshot replacements with payload-lifetime checks, reliable ordering, byte/count limits, oversized frames, terminal rejection, arithmetic overflow, drain accounting and independent clients. It uses no JUCE/Asio dependencies.

`tests/websocket_transport_test.cpp` uses real loopback sockets and HTTP upgrade to test actual Connection code, overload cancellation, callbacks, endpoint cleanup and isolation of a healthy second client. It also instantiates the TLS send template, but is not an end-to-end TLS test.

The workflow builds/runs both with normal builds and AddressSanitizer/UndefinedBehaviorSanitizer. This does not replace a complete Pleiades/JUCE build or a prolonged test using the actual ThreeJS + web-interface configuration.

## Server qualification

Keep compression, sensors, point-cloud requests and both real clients unchanged. Confirm the running binary includes the pinned fork commit before interpreting a repeat failure as a regression of this fix. Record Augmenta process RSS/RssAnon, cgroup memory, per-client TCP Send-Q and the overload log. Exercise a deliberately non-reading client and verify bounded queued bytes or explicit disconnection, while the other client continues receiving. Check reconnection, polling, one-shot zone events and application shutdown/restart. Inspect kernel/service logs to distinguish OOM kills from other crashes.
