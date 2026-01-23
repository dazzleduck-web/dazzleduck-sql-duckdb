#include "http/split_info.hpp"

#include "duckdb/common/exception.hpp"
#include "http/arrow_http_client.hpp"
#include "yyjson.hpp"

namespace duckdb {
namespace ext_nanoarrow {

using namespace duckdb_yyjson;

vector<PlanResponse> FetchPlanSplits(ClientContext& context, const string& url,
                                     const string& query, const string& auth_token,
                                     int64_t split_size) {
  // Fetch JSON from plan endpoint
  string json_response = ArrowHttpClient::FetchPlanJson(context, url, query, auth_token, split_size);

  // Parse JSON
  auto doc = yyjson_read(json_response.c_str(), json_response.size(), 0);
  if (!doc) {
    throw IOException("Failed to parse JSON response from plan API");
  }

  vector<PlanResponse> splits;

  auto root = yyjson_doc_get_root(doc);
  if (!yyjson_is_arr(root)) {
    yyjson_doc_free(doc);
    throw IOException("Plan API response is not an array");
  }

  // Iterate over splits array
  yyjson_arr_iter iter;
  yyjson_arr_iter_init(root, &iter);
  yyjson_val* plan_obj;

  while ((plan_obj = yyjson_arr_iter_next(&iter))) {
    if (!yyjson_is_obj(plan_obj)) {
      continue;
    }

    PlanResponse plan_response;

    // Extract endpoints array
    auto endpoints_val = yyjson_obj_get(plan_obj, "endpoints");
    if (yyjson_is_arr(endpoints_val)) {
      yyjson_arr_iter ep_iter;
      yyjson_arr_iter_init(endpoints_val, &ep_iter);
      yyjson_val* ep_val;
      while ((ep_val = yyjson_arr_iter_next(&ep_iter))) {
        if (yyjson_is_str(ep_val)) {
          plan_response.endpoints.push_back(yyjson_get_str(ep_val));
        }
      }
    }

    // Navigate to descriptor.statementHandle
    auto descriptor_val = yyjson_obj_get(plan_obj, "descriptor");
    if (!yyjson_is_obj(descriptor_val)) {
      continue;
    }

    auto statement_handle_val = yyjson_obj_get(descriptor_val, "statementHandle");
    if (!yyjson_is_obj(statement_handle_val)) {
      continue;
    }

    // Extract query from statementHandle
    auto query_val = yyjson_obj_get(statement_handle_val, "query");
    if (yyjson_is_str(query_val)) {
      plan_response.descriptor.statement_handle.query = yyjson_get_str(query_val);
    } else {
      // Skip splits without a query
      continue;
    }

    // Extract producerId from statementHandle
    auto producer_id_val = yyjson_obj_get(statement_handle_val, "producerId");
    if (yyjson_is_str(producer_id_val)) {
      plan_response.descriptor.statement_handle.producer_id = yyjson_get_str(producer_id_val);
    }

    // Extract queryId from statementHandle
    auto query_id_val = yyjson_obj_get(statement_handle_val, "queryId");
    if (yyjson_is_int(query_id_val)) {
      plan_response.descriptor.statement_handle.query_id = yyjson_get_int(query_id_val);
    }

    // Extract splitSize from statementHandle
    auto split_size_val = yyjson_obj_get(statement_handle_val, "splitSize");
    if (yyjson_is_int(split_size_val)) {
      plan_response.descriptor.statement_handle.split_size = yyjson_get_int(split_size_val);
    }

    // Extract queryChecksum from statementHandle
    auto query_checksum_val = yyjson_obj_get(statement_handle_val, "queryChecksum");
    if (yyjson_is_str(query_checksum_val)) {
      plan_response.descriptor.statement_handle.query_checksum = yyjson_get_str(query_checksum_val);
    }

    splits.push_back(std::move(plan_response));
  }

  yyjson_doc_free(doc);
  return splits;
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
