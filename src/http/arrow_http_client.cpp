#include "http/arrow_http_client.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//! URL-encode a string for use in query parameters
static string UrlEncode(const string& input) {
  string encoded;
  for (char c : input) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      encoded += c;
    } else if (c == ' ') {
      encoded += '+';
    } else {
      char hex[4];
      snprintf(hex, sizeof(hex), "%%%02X", static_cast<unsigned char>(c));
      encoded += hex;
    }
  }
  return encoded;
}

//! Build base URL ensuring no trailing slash
static string BuildBaseUrl(const string& url) {
  string base_url = url;
  if (!base_url.empty() && base_url.back() == '/') {
    base_url.pop_back();
  }
  return base_url;
}

string ArrowHttpClient::FetchArrowStream(ClientContext& context, const string& url,
                                         const string& query, const string& auth_token,
                                         int64_t query_id) {
  // Get the HTTP utility from the database
  auto& db = DatabaseInstance::GetDatabase(context);
  auto& http_util = HTTPUtil::Get(db);

  // URL-encode the query parameter
  string encoded_query = UrlEncode(query);

  // Build the full URL with query endpoint
  string full_url = BuildBaseUrl(url) + "/v1/query?q=" + encoded_query;

  // Add query ID if specified (for tracking/cancellation)
  if (query_id >= 0) {
    full_url += "&id=" + std::to_string(query_id);
  }

  // Initialize HTTP parameters (uses httpfs settings for timeout, auth, etc.)
  auto http_params = http_util.InitializeParameters(context, full_url);

  // Build request headers
  HTTPHeaders headers(db);
  headers.Insert("Accept", "application/vnd.apache.arrow.stream");

  // Add Authorization header if auth token provided
  if (!auth_token.empty()) {
    headers.Insert("Authorization", "Bearer " + auth_token);
  }

  // Create GET request without handlers - body will be collected in response->body
  GetRequestInfo request_info(full_url, headers, *http_params, nullptr, nullptr);

  // Make the request
  auto response = http_util.Request(request_info);

  // Handle errors
  if (!response->Success()) {
    if (response->HasRequestError()) {
      throw IOException("HTTP request to %s failed: %s", url, response->GetRequestError());
    }
    throw IOException("HTTP request to %s failed: %s", url, response->GetError());
  }

  // Check HTTP status code
  auto status_code = static_cast<int>(response->status);
  if (status_code >= 400 && status_code < 500) {
    throw InvalidInputException("HTTP %d error from %s: %s", status_code, url,
                                response->body);
  }
  if (status_code >= 500) {
    throw IOException("HTTP %d server error from %s: %s", status_code, url,
                      response->body);
  }
  if (status_code < 200 || status_code >= 300) {
    throw IOException("Unexpected HTTP %d response from %s", status_code, url);
  }

  // Validate content type (optional, but good practice)
  if (response->HasHeader("Content-Type")) {
    auto content_type = response->GetHeaderValue("Content-Type");
    if (content_type.find("application/vnd.apache.arrow.stream") == string::npos &&
        content_type.find("application/octet-stream") == string::npos) {
      throw IOException(
          "Unexpected Content-Type from %s: %s (expected application/vnd.apache.arrow.stream)",
          url, content_type);
    }
  }

  return response->body;
}

string ArrowHttpClient::FetchPlanJson(ClientContext& context, const string& url,
                                      const string& query, const string& auth_token,
                                      int64_t split_size) {
  // Get the HTTP utility from the database
  auto& db = DatabaseInstance::GetDatabase(context);
  auto& http_util = HTTPUtil::Get(db);

  // URL-encode the query parameter
  string encoded_query = UrlEncode(query);

  // Build the full URL with plan endpoint
  string full_url = BuildBaseUrl(url) + "/v1/plan?q=" + encoded_query;

  // Initialize HTTP parameters
  auto http_params = http_util.InitializeParameters(context, full_url);

  // Build request headers for JSON response
  HTTPHeaders headers(db);
  headers.Insert("Accept", "application/json");

  // Add Authorization header if auth token provided
  if (!auth_token.empty()) {
    headers.Insert("Authorization", "Bearer " + auth_token);
  }

  // Add split size header if specified
  if (split_size > 0) {
    headers.Insert("x-dd-split-size", std::to_string(split_size));
  }

  // Create GET request
  GetRequestInfo request_info(full_url, headers, *http_params, nullptr, nullptr);

  // Make the request
  auto response = http_util.Request(request_info);

  // Handle errors
  if (!response->Success()) {
    if (response->HasRequestError()) {
      throw IOException("HTTP request to %s/v1/plan failed: %s", url, response->GetRequestError());
    }
    throw IOException("HTTP request to %s/v1/plan failed: %s", url, response->GetError());
  }

  // Check HTTP status code
  auto status_code = static_cast<int>(response->status);
  if (status_code >= 400 && status_code < 500) {
    throw InvalidInputException("HTTP %d error from %s/v1/plan: %s", status_code, url,
                                response->body);
  }
  if (status_code >= 500) {
    throw IOException("HTTP %d server error from %s/v1/plan: %s", status_code, url,
                      response->body);
  }
  if (status_code < 200 || status_code >= 300) {
    throw IOException("Unexpected HTTP %d response from %s/v1/plan", status_code, url);
  }

  return response->body;
}

void ArrowHttpClient::CancelQuery(ClientContext& context, const string& url,
                                  int64_t query_id, const string& auth_token) {
  // Get the HTTP utility from the database
  auto& db = DatabaseInstance::GetDatabase(context);
  auto& http_util = HTTPUtil::Get(db);

  // Build the full URL with cancel endpoint
  // Note: q parameter is required by AbstractQueryBasedService even if empty
  string full_url = BuildBaseUrl(url) + "/v1/cancel?q=&id=" + std::to_string(query_id);

  // Initialize HTTP parameters
  auto http_params = http_util.InitializeParameters(context, full_url);

  // Build request headers
  HTTPHeaders headers(db);
  headers.Insert("Accept", "text/plain");

  // Add Authorization header if auth token provided
  if (!auth_token.empty()) {
    headers.Insert("Authorization", "Bearer " + auth_token);
  }

  // Create GET request
  GetRequestInfo request_info(full_url, headers, *http_params, nullptr, nullptr);

  // Make the request - ignore response (best effort cancellation)
  try {
    http_util.Request(request_info);
  } catch (...) {
    // Ignore errors - cancellation is best effort
  }
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
