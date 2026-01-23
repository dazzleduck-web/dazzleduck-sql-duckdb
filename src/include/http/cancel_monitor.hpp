//===----------------------------------------------------------------------===//
//                         DuckDB - DazzleDuck
//
// http/cancel_monitor.hpp
//
// Global Cancel Monitor for tracking and cancelling active queries
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"

#include <condition_variable>
#include <thread>

namespace duckdb {
namespace ext_nanoarrow {

//===----------------------------------------------------------------------===//
// GlobalCancelMonitor - Singleton that monitors all active queries
//===----------------------------------------------------------------------===//

class GlobalCancelMonitor {
public:
  //! Get the singleton instance
  static GlobalCancelMonitor& Get();

  //! Register a query for cancellation monitoring
  //! @return Handle for unregistering
  uint64_t Register(ClientContext* context, const string& url,
                    const string& auth_token, int64_t query_id);

  //! Unregister a query (on completion or cleanup)
  void Unregister(uint64_t handle);

  //! Shutdown the monitor (called on extension unload)
  static void Shutdown();

private:
  GlobalCancelMonitor() = default;
  ~GlobalCancelMonitor();

  // Non-copyable
  GlobalCancelMonitor(const GlobalCancelMonitor&) = delete;
  GlobalCancelMonitor& operator=(const GlobalCancelMonitor&) = delete;

  //! Information about a registered query
  struct QueryInfo {
    ClientContext* context;
    string url;
    string auth_token;
    int64_t query_id;
  };

  //! Start the monitor thread (called when first query registered)
  void StartMonitor();

  //! Stop the monitor thread (called when last query unregistered)
  void StopMonitor();

  //! Check all registered queries for interrupts
  void CheckAllQueries();

  //! Monitor thread function
  void MonitorLoop();

  //! Singleton instance
  static GlobalCancelMonitor* instance;
  static mutex instance_mutex;

  //! Registered queries: handle -> query info
  unordered_map<uint64_t, QueryInfo> active_queries;
  uint64_t next_handle = 0;

  //! Mutex for query registration
  mutex queries_mutex;

  //! Monitor thread
  std::thread monitor_thread;
  atomic<bool> running{false};
  std::condition_variable cv;
  mutex cv_mutex;
};

//===----------------------------------------------------------------------===//
// QueryCancelGuard - RAII guard for automatic query cancellation
//===----------------------------------------------------------------------===//

class QueryCancelGuard {
public:
  QueryCancelGuard() = default;
  ~QueryCancelGuard();

  // Non-copyable but movable
  QueryCancelGuard(const QueryCancelGuard&) = delete;
  QueryCancelGuard& operator=(const QueryCancelGuard&) = delete;
  QueryCancelGuard(QueryCancelGuard&& other) noexcept;
  QueryCancelGuard& operator=(QueryCancelGuard&& other) noexcept;

  //! Add a query to be monitored
  //! @param context Client context (for interrupt detection)
  //! @param url Server URL for cancel request
  //! @param auth_token Auth token for cancel request
  //! @param query_id Query ID to cancel
  void AddQuery(ClientContext* context, const string& url,
                const string& auth_token, int64_t query_id);

  //! Mark all queries as completed (prevents cancellation)
  void MarkCompleted();

  //! Check if any queries are registered
  bool HasQueries() const { return !handles.empty(); }

private:
  //! Information about a registered query (for sync cancel on destruction)
  struct QueryInfo {
    ClientContext* context;
    string url;
    string auth_token;
    int64_t query_id;
  };

  //! Unregister all queries from the monitor
  void UnregisterAll();

  //! Send cancel requests for interrupted queries
  void CancelInterruptedQueries();

  //! Registration handles from GlobalCancelMonitor
  vector<uint64_t> handles;

  //! Query info for synchronous cancellation on destruction
  vector<QueryInfo> query_infos;

  //! Whether queries completed successfully
  bool completed = false;
};

}  // namespace ext_nanoarrow
}  // namespace duckdb
