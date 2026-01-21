#include "scalar_function/dd_login.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
// Include httplib directly from DuckDB's third_party
#include "../../../duckdb/third_party/httplib/httplib.hpp"
#include "yyjson.hpp"

namespace duckdb {
namespace ext_nanoarrow {

using namespace duckdb_yyjson;

namespace {

//! Build JSON body for login request
//! Claims should be a JSON string like '{"database":"demo_db","schema":"main","table":"demo"}'
static string BuildLoginJson(const string& username, const string& password, const string& claims) {
  auto doc = yyjson_mut_doc_new(nullptr);
  auto root = yyjson_mut_obj(doc);
  yyjson_mut_doc_set_root(doc, root);

  yyjson_mut_obj_add_str(doc, root, "username", username.c_str());
  yyjson_mut_obj_add_str(doc, root, "password", password.c_str());

  if (!claims.empty()) {
    // Parse claims as JSON and nest under "claims" key
    auto claims_doc = yyjson_read(claims.c_str(), claims.size(), 0);
    if (claims_doc) {
      auto claims_root = yyjson_doc_get_root(claims_doc);
      if (yyjson_is_obj(claims_root)) {
        // Create a mutable claims object
        auto claims_obj = yyjson_mut_obj(doc);

        // Extract known claim fields: database, schema, table
        // Use yyjson_mut_obj_add_strcpy to copy strings into the mutable doc
        auto db_val = yyjson_obj_get(claims_root, "database");
        if (yyjson_is_str(db_val)) {
          yyjson_mut_obj_add_strcpy(doc, claims_obj, "database", yyjson_get_str(db_val));
        }
        auto schema_val = yyjson_obj_get(claims_root, "schema");
        if (yyjson_is_str(schema_val)) {
          yyjson_mut_obj_add_strcpy(doc, claims_obj, "schema", yyjson_get_str(schema_val));
        }
        auto table_val = yyjson_obj_get(claims_root, "table");
        if (yyjson_is_str(table_val)) {
          yyjson_mut_obj_add_strcpy(doc, claims_obj, "table", yyjson_get_str(table_val));
        }

        // Add claims object to root
        yyjson_mut_obj_add_val(doc, root, "claims", claims_obj);
      }
      yyjson_doc_free(claims_doc);
    }
  }

  auto json_str = yyjson_mut_write(doc, 0, nullptr);
  string result(json_str);
  free(json_str);
  yyjson_mut_doc_free(doc);

  return result;
}

//! Extract accessToken from JSON response
static string ParseLoginResponse(const string& json_response) {
  auto doc = yyjson_read(json_response.c_str(), json_response.size(), 0);
  if (!doc) {
    throw IOException("dd_login: Failed to parse login response JSON. Response length=%d, body='%s'",
                      json_response.size(), json_response.empty() ? "(empty)" : json_response.substr(0, 200).c_str());
  }

  auto root = yyjson_doc_get_root(doc);
  if (!yyjson_is_obj(root)) {
    yyjson_doc_free(doc);
    throw IOException("dd_login: Expected JSON object in response");
  }

  auto token_val = yyjson_obj_get(root, "accessToken");
  if (!yyjson_is_str(token_val)) {
    yyjson_doc_free(doc);
    throw IOException("dd_login: No accessToken found in response");
  }

  string token = yyjson_get_str(token_val);
  yyjson_doc_free(doc);
  return token;
}

//! Make login HTTP POST request using httplib directly
static string MakeLoginRequest(const string& url, const string& json_body) {
  // Parse the URL to extract host and path
  string base_url = url;
  if (!base_url.empty() && base_url.back() == '/') {
    base_url.pop_back();
  }

  // Find the protocol separator
  string proto_host_port;
  string path = "/v1/login";

  size_t proto_end = base_url.find("://");
  if (proto_end != string::npos) {
    size_t path_start = base_url.find('/', proto_end + 3);
    if (path_start != string::npos) {
      proto_host_port = base_url.substr(0, path_start);
    } else {
      proto_host_port = base_url;
    }
  } else {
    // No protocol, assume http
    proto_host_port = "http://" + base_url;
    size_t path_start = base_url.find('/');
    if (path_start != string::npos) {
      proto_host_port = "http://" + base_url.substr(0, path_start);
    }
  }

  // Create httplib client
  duckdb_httplib::Client client(proto_host_port);
  client.set_connection_timeout(30);
  client.set_read_timeout(30);
  client.set_write_timeout(30);

  // Make POST request
  auto res = client.Post(path, json_body, "application/json");

  if (!res) {
    throw IOException("dd_login: HTTP POST request to %s%s failed: %s", proto_host_port, path,
                      to_string(res.error()).c_str());
  }

  // Check HTTP status code
  if (res->status >= 400 && res->status < 500) {
    throw InvalidInputException("dd_login: HTTP %d error from %s%s: %s", res->status, proto_host_port, path,
                                res->body.c_str());
  }
  if (res->status >= 500) {
    throw IOException("dd_login: HTTP %d server error from %s%s: %s", res->status, proto_host_port, path,
                      res->body.c_str());
  }
  if (res->status < 200 || res->status >= 300) {
    throw IOException("dd_login: Unexpected HTTP %d response from %s%s", res->status, proto_host_port, path);
  }

  return res->body;
}

//! dd_login function - authenticates with a dazzleduck server and returns JWT token
//! Arguments: url, username, password, [claims]
static void DDLoginFunction(DataChunk& args, ExpressionState& state, Vector& result) {
  auto& url_vec = args.data[0];
  auto& username_vec = args.data[1];
  auto& password_vec = args.data[2];

  bool has_claims = args.ColumnCount() >= 4;

  auto count = args.size();

  UnaryExecutor::Execute<string_t, string_t>(url_vec, result, count, [&](string_t url) {
    // For each row, get the values
    auto url_str = url.GetString();

    // Get username value (assuming constant or flat vector for simplicity)
    string username_str;
    if (username_vec.GetVectorType() == VectorType::CONSTANT_VECTOR) {
      if (ConstantVector::IsNull(username_vec)) {
        throw InvalidInputException("dd_login: username cannot be NULL");
      }
      username_str = ConstantVector::GetData<string_t>(username_vec)->GetString();
    } else {
      auto username_data = FlatVector::GetData<string_t>(username_vec);
      username_str = username_data[0].GetString();
    }

    // Get password value
    string password_str;
    if (password_vec.GetVectorType() == VectorType::CONSTANT_VECTOR) {
      if (ConstantVector::IsNull(password_vec)) {
        throw InvalidInputException("dd_login: password cannot be NULL");
      }
      password_str = ConstantVector::GetData<string_t>(password_vec)->GetString();
    } else {
      auto password_data = FlatVector::GetData<string_t>(password_vec);
      password_str = password_data[0].GetString();
    }

    // Get optional claims value
    string claims_str;
    if (has_claims) {
      auto& claims_vec = args.data[3];
      if (claims_vec.GetVectorType() == VectorType::CONSTANT_VECTOR) {
        if (!ConstantVector::IsNull(claims_vec)) {
          claims_str = ConstantVector::GetData<string_t>(claims_vec)->GetString();
        }
      } else {
        auto claims_data = FlatVector::GetData<string_t>(claims_vec);
        claims_str = claims_data[0].GetString();
      }
    }

    // Build JSON body
    auto json_body = BuildLoginJson(username_str, password_str, claims_str);

    // Make login request
    auto response_body = MakeLoginRequest(url_str, json_body);

    // Parse response and extract token
    auto token = ParseLoginResponse(response_body);

    return StringVector::AddString(result, token);
  });
}

}  // namespace

void RegisterDDLogin(ExtensionLoader& loader) {
  // dd_login(url, username, password) -> token
  auto func3 = ScalarFunction("dd_login", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                              LogicalType::VARCHAR, DDLoginFunction);
  loader.RegisterFunction(func3);

  // dd_login(url, username, password, claims) -> token
  auto func4 =
      ScalarFunction("dd_login",
                     {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                     LogicalType::VARCHAR, DDLoginFunction);
  loader.RegisterFunction(func4);
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
