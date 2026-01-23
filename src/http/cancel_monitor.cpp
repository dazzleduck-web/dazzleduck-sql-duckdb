#include "http/cancel_monitor.hpp"

#include "http/arrow_http_client.hpp"

#include <chrono>

namespace duckdb {
namespace ext_nanoarrow {

//===----------------------------------------------------------------------===//
// GlobalCancelMonitor Implementation
//===----------------------------------------------------------------------===//

// Static member initialization
GlobalCancelMonitor* GlobalCancelMonitor::instance = nullptr;
mutex GlobalCancelMonitor::instance_mutex;

GlobalCancelMonitor& GlobalCancelMonitor::Get() {
  lock_guard<mutex> lock(instance_mutex);
  if (!instance) {
    instance = new GlobalCancelMonitor();
  }
  return *instance;
}

void GlobalCancelMonitor::Shutdown() {
  GlobalCancelMonitor* inst = nullptr;
  {
    lock_guard<mutex> lock(instance_mutex);
    inst = instance;
    instance = nullptr;
  }
  if (inst) {
    delete inst;
  }
}

GlobalCancelMonitor::~GlobalCancelMonitor() {
  StopMonitor();
}

uint64_t GlobalCancelMonitor::Register(ClientContext* context, const string& url,
                                       const string& auth_token, int64_t query_id) {
  unique_lock<mutex> lock(queries_mutex);

  uint64_t handle = next_handle++;
  active_queries[handle] = {context, url, auth_token, query_id};

  // Start monitor if this is the first query
  if (active_queries.size() == 1 && !running.load()) {
    // Need to release lock before starting thread to avoid deadlock
    lock.unlock();
    StartMonitor();
  }

  return handle;
}

void GlobalCancelMonitor::Unregister(uint64_t handle) {
  bool should_stop = false;

  {
    lock_guard<mutex> lock(queries_mutex);
    active_queries.erase(handle);
    should_stop = active_queries.empty();
  }

  // Signal monitor to stop if no more queries
  if (should_stop) {
    lock_guard<mutex> cv_lock(cv_mutex);
    // Don't set running = false here, let the monitor thread exit naturally
    // when it finds no more queries
    cv.notify_all();
  }
}

void GlobalCancelMonitor::StartMonitor() {
  // Check if already running
  bool expected = false;
  if (!running.compare_exchange_strong(expected, true)) {
    return;  // Already running
  }

  // If there's an old thread that finished, join it first
  if (monitor_thread.joinable()) {
    monitor_thread.join();
  }

  monitor_thread = std::thread([this]() {
    MonitorLoop();
  });
}

void GlobalCancelMonitor::StopMonitor() {
  running = false;
  cv.notify_all();

  if (monitor_thread.joinable()) {
    monitor_thread.join();
  }
}

void GlobalCancelMonitor::MonitorLoop() {
  while (running.load()) {
    CheckAllQueries();

    // Wait for 50ms or until notified to stop
    std::unique_lock<mutex> lock(cv_mutex);
    cv.wait_for(lock, std::chrono::milliseconds(50), [this]() {
      return !running.load();
    });
  }

  // Thread is exiting, mark as not running
  running = false;
}

void GlobalCancelMonitor::CheckAllQueries() {
  // Collect queries to cancel (avoid holding lock during HTTP calls)
  vector<QueryInfo> to_cancel;
  bool no_more_queries = false;

  {
    lock_guard<mutex> lock(queries_mutex);

    for (auto it = active_queries.begin(); it != active_queries.end(); ) {
      auto& info = it->second;

      // Check if context is still valid and interrupted
      if (info.context && info.context->interrupted.load(std::memory_order_relaxed)) {
        to_cancel.push_back(info);
        it = active_queries.erase(it);
      } else {
        ++it;
      }
    }

    no_more_queries = active_queries.empty();
  }

  // Stop monitor if no more queries
  if (no_more_queries) {
    running = false;
  }

  // Cancel queries outside of lock
  for (auto& info : to_cancel) {
    try {
      if (info.context) {
        ArrowHttpClient::CancelQuery(*info.context, info.url,
                                     info.query_id, info.auth_token);
      }
    } catch (...) {
      // Best effort - ignore errors during cancellation
    }
  }
}

//===----------------------------------------------------------------------===//
// QueryCancelGuard Implementation
//===----------------------------------------------------------------------===//

QueryCancelGuard::~QueryCancelGuard() {
  UnregisterAll();
}

QueryCancelGuard::QueryCancelGuard(QueryCancelGuard&& other) noexcept
    : handles(std::move(other.handles)), completed(other.completed) {
  other.completed = true;  // Prevent other from unregistering
}

QueryCancelGuard& QueryCancelGuard::operator=(QueryCancelGuard&& other) noexcept {
  if (this != &other) {
    UnregisterAll();
    handles = std::move(other.handles);
    completed = other.completed;
    other.completed = true;
  }
  return *this;
}

void QueryCancelGuard::AddQuery(ClientContext* context, const string& url,
                                const string& auth_token, int64_t query_id) {
  if (query_id < 0) {
    return;  // Invalid query ID
  }

  auto handle = GlobalCancelMonitor::Get().Register(context, url, auth_token, query_id);
  handles.push_back(handle);
}

void QueryCancelGuard::MarkCompleted() {
  completed = true;
  UnregisterAll();
}

void QueryCancelGuard::UnregisterAll() {
  for (auto handle : handles) {
    GlobalCancelMonitor::Get().Unregister(handle);
  }
  handles.clear();
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
