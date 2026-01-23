# Query Cancellation Architecture

This document describes the query cancellation mechanism for `dd_read_arrow` table function.

## Overview

When a user interrupts a query (e.g., Ctrl+C), the extension sends an HTTP cancel request to the remote DazzleDuck server to stop the server-side query execution. This prevents unnecessary resource consumption on the server.

## Components

### 1. GlobalCancelMonitor (Singleton)

**File:** `src/include/http/cancel_monitor.hpp`, `src/http/cancel_monitor.cpp`

A singleton that manages a background thread monitoring all active queries for interruption.

```
┌─────────────────────────────────────────────────────┐
│              GlobalCancelMonitor                     │
├─────────────────────────────────────────────────────┤
│ - active_queries: map<handle, QueryInfo>            │
│ - monitor_thread: polls every 50ms                  │
│ - running: atomic<bool>                             │
├─────────────────────────────────────────────────────┤
│ + Get(): GlobalCancelMonitor&     (singleton)       │
│ + Register(context, url, token, query_id): handle   │
│ + Unregister(handle)                                │
│ + Shutdown()                                        │
└─────────────────────────────────────────────────────┘
```

**Behavior:**
- Starts monitor thread when first query is registered
- Polls `context.interrupted` every 50ms for all registered queries
- Sends cancel request when interruption is detected
- Stops thread when no queries are registered

### 2. QueryCancelGuard (RAII)

**File:** `src/include/http/cancel_monitor.hpp`, `src/http/cancel_monitor.cpp`

An RAII wrapper that ensures cancel requests are sent when queries don't complete normally.

```
┌─────────────────────────────────────────────────────┐
│              QueryCancelGuard                        │
├─────────────────────────────────────────────────────┤
│ - handles: vector<uint64_t>                         │
│ - query_infos: vector<QueryInfo>                    │
│ - completed: bool                                   │
├─────────────────────────────────────────────────────┤
│ + AddQuery(context, url, token, query_id)           │
│ + MarkCompleted()                                   │
│ + ~QueryCancelGuard() → sends cancel if !completed  │
└─────────────────────────────────────────────────────┘
```

**Behavior:**
- Stores query information when `AddQuery()` is called
- On destruction, if not marked as completed, sends cancel requests synchronously
- Ensures cleanup even when exceptions occur

### 3. ArrowHttpClient::CancelQuery

**File:** `src/http/arrow_http_client.cpp`

Sends HTTP GET request to cancel a query on the server.

```
GET /v1/cancel?q=&id={query_id}
Headers:
  Accept: text/plain
  Authorization: Bearer {token}  (if provided)
```

## Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Query Execution                              │
└─────────────────────────────────────────────────────────────────────┘

1. Query Start (InitGlobal)
   ┌──────────────────┐
   │ Generate random  │
   │ query_id         │
   └────────┬─────────┘
            │
            ▼
   ┌──────────────────┐     ┌─────────────────────┐
   │ cancel_guard.    │────▶│ GlobalCancelMonitor │
   │ AddQuery(...)    │     │ .Register(...)      │
   └────────┬─────────┘     └─────────────────────┘
            │
            ▼
   ┌──────────────────┐
   │ HTTP Request     │
   │ /v1/query?id=X   │
   └────────┬─────────┘
            │
            ▼
   ┌──────────────────┐
   │ Process Results  │
   └────────┬─────────┘

2a. Normal Completion
            │
            ▼
   ┌──────────────────┐     ┌─────────────────────┐
   │ cancel_guard.    │────▶│ GlobalCancelMonitor │
   │ MarkCompleted()  │     │ .Unregister(...)    │
   └──────────────────┘     └─────────────────────┘
   (No cancel sent)

2b. Interruption (Ctrl+C)
            │
            ▼
   ┌──────────────────┐
   │ DuckDB sets      │
   │ context.         │
   │ interrupted=true │
   └────────┬─────────┘
            │
            ▼
   ┌──────────────────┐
   │ Exception thrown │
   │ stack unwinds    │
   └────────┬─────────┘
            │
            ▼
   ┌──────────────────┐     ┌─────────────────────┐
   │ ~QueryCancelGuard│────▶│ CancelInterrupted   │
   │ (destructor)     │     │ Queries()           │
   └──────────────────┘     └─────────┬───────────┘
                                      │
                                      ▼
                            ┌─────────────────────┐
                            │ ArrowHttpClient::   │
                            │ CancelQuery(...)    │
                            │                     │
                            │ GET /v1/cancel?     │
                            │     q=&id={id}      │
                            └─────────────────────┘
```

## Query ID Generation

Query IDs are generated client-side using random 64-bit positive integers:

```cpp
std::random_device rd;
std::mt19937_64 gen(rd());
std::uniform_int_distribution<int64_t> dis(1, std::numeric_limits<int64_t>::max());
int64_t query_id = dis(gen);
```

The query ID is:
1. Passed to the server via `&id=` parameter in `/v1/query` requests
2. Stored in the cancel guard for potential cancellation
3. Used in `/v1/cancel?id=` request to identify which query to cancel

## Split Mode vs Non-Split Mode

### Non-Split Mode
- Single query ID generated in `Bind()`
- Registered with cancel guard in `InitGlobal()`
- One cancel request sent if interrupted

### Split Mode
- Multiple query IDs generated (one per split) in `InitGlobal()`
- All registered with cancel guard
- Multiple cancel requests sent if interrupted (one per active split)

## Thread Safety

- `GlobalCancelMonitor` uses mutex for query registration/unregistration
- `QueryCancelGuard` is not thread-safe but is owned by global state
- Cancel requests are sent synchronously in destructor to ensure delivery

## Error Handling

- Cancel requests are best-effort (errors are silently ignored)
- If context is invalid during destruction, cancel may fail silently
- Server returns success even if query was already completed/cancelled

## Server Integration

The DazzleDuck server:
- Tracks running queries by statement ID
- Exposes `/v1/cancel?q=&id={id}` endpoint
- Updates "Cancelled Statements" counter in `/v1/ui/api/metrics`
- May count client-disconnected queries as "Completed Prepared Statements"
