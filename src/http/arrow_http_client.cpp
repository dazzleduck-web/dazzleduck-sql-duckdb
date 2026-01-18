#include "http/arrow_http_client.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {
namespace ext_nanoarrow {

string ArrowHttpClient::FetchArrowStream(ClientContext& context, const string& url,
                                         const string& query) {
  // Get the HTTP utility from the database
  auto& db = DatabaseInstance::GetDatabase(context);
  auto& http_util = HTTPUtil::Get(db);

  // URL-encode the query parameter
  string encoded_query;
  for (char c : query) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      encoded_query += c;
    } else if (c == ' ') {
      encoded_query += '+';
    } else {
      char hex[4];
      snprintf(hex, sizeof(hex), "%%%02X", static_cast<unsigned char>(c));
      encoded_query += hex;
    }
  }

  // Build the full URL with query endpoint
  // Ensure base URL doesn't have trailing slash for consistent path building
  string base_url = url;
  if (!base_url.empty() && base_url.back() == '/') {
    base_url.pop_back();
  }
  string full_url = base_url + "/v1/query?q=" + encoded_query;

  // Initialize HTTP parameters (uses httpfs settings for timeout, auth, etc.)
  auto http_params = http_util.InitializeParameters(context, full_url);

  // Build request headers
  HTTPHeaders headers(db);
  headers.Insert("Accept", "application/vnd.apache.arrow.stream");

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

}  // namespace ext_nanoarrow
}  // namespace duckdb
