#include "http/split_info.hpp"

#include "duckdb/common/exception.hpp"
#include "http/arrow_http_client.hpp"
#include "yyjson.hpp"

namespace duckdb {
namespace ext_nanoarrow {

using namespace duckdb_yyjson;

vector<SplitInfo> FetchPlanSplits(ClientContext& context, const string& url,
                                  const string& query, const string& auth_token,
                                  int64_t split_size) {
  // Fetch JSON from plan endpoint
  string json_response = ArrowHttpClient::FetchPlanJson(context, url, query, auth_token, split_size);

  // Parse JSON
  auto doc = yyjson_read(json_response.c_str(), json_response.size(), 0);
  if (!doc) {
    throw IOException("Failed to parse JSON response from plan API");
  }

  vector<SplitInfo> splits;

  auto root = yyjson_doc_get_root(doc);
  if (!yyjson_is_arr(root)) {
    yyjson_doc_free(doc);
    throw IOException("Plan API response is not an array");
  }

  // Iterate over splits array
  yyjson_arr_iter iter;
  yyjson_arr_iter_init(root, &iter);
  yyjson_val* split_obj;

  while ((split_obj = yyjson_arr_iter_next(&iter))) {
    if (!yyjson_is_obj(split_obj)) {
      continue;
    }

    SplitInfo split;

    // Extract id
    auto id_val = yyjson_obj_get(split_obj, "id");
    if (yyjson_is_str(id_val)) {
      split.id = yyjson_get_str(id_val);
    }

    // Extract query
    auto query_val = yyjson_obj_get(split_obj, "query");
    if (yyjson_is_str(query_val)) {
      split.query = yyjson_get_str(query_val);
    } else {
      // Skip splits without a query
      continue;
    }

    // Extract producerId
    auto producer_id_val = yyjson_obj_get(split_obj, "producerId");
    if (yyjson_is_str(producer_id_val)) {
      split.producer_id = yyjson_get_str(producer_id_val);
    }

    // Extract queryId
    auto query_id_val = yyjson_obj_get(split_obj, "queryId");
    if (yyjson_is_int(query_id_val)) {
      split.query_id = yyjson_get_int(query_id_val);
    }

    // Extract splitSize
    auto split_size_val = yyjson_obj_get(split_obj, "splitSize");
    if (yyjson_is_int(split_size_val)) {
      split.split_size = yyjson_get_int(split_size_val);
    }

    splits.push_back(std::move(split));
  }

  yyjson_doc_free(doc);
  return splits;
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
