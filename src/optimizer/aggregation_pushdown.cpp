#include "optimizer/aggregation_pushdown.hpp"

#include "table_function/read_arrow_dd.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {
namespace ext_nanoarrow {

//===----------------------------------------------------------------------===//
// Expression-to-SQL Serializer
//===----------------------------------------------------------------------===//

//! Escape double-quotes in an identifier for safe SQL quoting
static string EscapeIdentifier(const string& name) {
  string escaped;
  for (char c : name) {
    if (c == '"') {
      escaped += "\"\"";  // SQL standard: double the quote
    } else {
      escaped += c;
    }
  }
  return escaped;
}

//! Map ExpressionType comparison operators to SQL operator strings
static string ComparisonTypeToSQL(ExpressionType type) {
  switch (type) {
    case ExpressionType::COMPARE_EQUAL: return "=";
    case ExpressionType::COMPARE_NOTEQUAL: return "!=";
    case ExpressionType::COMPARE_LESSTHAN: return "<";
    case ExpressionType::COMPARE_GREATERTHAN: return ">";
    case ExpressionType::COMPARE_LESSTHANOREQUALTO: return "<=";
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO: return ">=";
    default: return "";
  }
}

//! Serialize a DuckDB Expression to a SQL string fragment.
//! Returns empty string on unsupported expressions (signals bail-out).
static string ExpressionToSQL(const Expression& expr, const vector<string>& column_names) {
  switch (expr.expression_class) {
    case ExpressionClass::BOUND_REF: {
      auto& ref = expr.Cast<BoundReferenceExpression>();
      if (ref.index < column_names.size()) {
        return "\"" + EscapeIdentifier(column_names[ref.index]) + "\"";
      }
      return "";
    }
    case ExpressionClass::BOUND_COLUMN_REF: {
      auto& col_ref = expr.Cast<BoundColumnRefExpression>();
      auto col_idx = col_ref.binding.column_index;
      if (col_idx < column_names.size()) {
        return "\"" + EscapeIdentifier(column_names[col_idx]) + "\"";
      }
      return "";
    }
    case ExpressionClass::BOUND_CONSTANT: {
      auto& constant = expr.Cast<BoundConstantExpression>();
      if (constant.value.IsNull()) {
        return "NULL";
      }
      // Quote string/varchar values with single quotes (escape embedded quotes)
      if (constant.value.type().id() == LogicalTypeId::VARCHAR) {
        string str_val = constant.value.ToString();
        string escaped;
        for (char c : str_val) {
          if (c == '\'') {
            escaped += "''";
          } else {
            escaped += c;
          }
        }
        return "'" + escaped + "'";
      }
      return constant.value.ToString();
    }
    case ExpressionClass::BOUND_COMPARISON: {
      auto& comp = expr.Cast<BoundComparisonExpression>();
      string op = ComparisonTypeToSQL(comp.type);
      if (op.empty()) {
        return "";  // Unsupported comparison type
      }
      string left_sql = ExpressionToSQL(*comp.left, column_names);
      if (left_sql.empty()) return "";
      string right_sql = ExpressionToSQL(*comp.right, column_names);
      if (right_sql.empty()) return "";
      return "(" + left_sql + " " + op + " " + right_sql + ")";
    }
    case ExpressionClass::BOUND_CONJUNCTION: {
      auto& conj = expr.Cast<BoundConjunctionExpression>();
      string conjunction_op;
      if (expr.type == ExpressionType::CONJUNCTION_AND) {
        conjunction_op = " AND ";
      } else if (expr.type == ExpressionType::CONJUNCTION_OR) {
        conjunction_op = " OR ";
      } else {
        return "";
      }
      string result = "(";
      for (idx_t i = 0; i < conj.children.size(); i++) {
        if (i > 0) {
          result += conjunction_op;
        }
        string child_sql = ExpressionToSQL(*conj.children[i], column_names);
        if (child_sql.empty()) return "";
        result += child_sql;
      }
      result += ")";
      return result;
    }
    case ExpressionClass::BOUND_CAST: {
      auto& cast = expr.Cast<BoundCastExpression>();
      string child_sql = ExpressionToSQL(*cast.child, column_names);
      if (child_sql.empty()) return "";
      return "CAST(" + child_sql + " AS " + cast.return_type.ToString() + ")";
    }
    case ExpressionClass::BOUND_FUNCTION: {
      auto& func = expr.Cast<BoundFunctionExpression>();
      // Serialize scalar function calls (e.g., abs, length, etc.)
      string args;
      for (idx_t i = 0; i < func.children.size(); i++) {
        if (i > 0) args += ", ";
        string child_sql = ExpressionToSQL(*func.children[i], column_names);
        if (child_sql.empty()) return "";
        args += child_sql;
      }
      return func.function.name + "(" + args + ")";
    }
    default:
      // Unsupported expression type - bail out
      return "";
  }
}

//! Serialize an aggregate expression to SQL (e.g., "count(*)", "sum(\"col\")")
static string AggregateToSQL(const BoundAggregateExpression& agg,
                              const vector<string>& column_names) {
  string func_name = agg.function.name;

  // Handle count_star specially
  if (func_name == "count_star") {
    return "count(*)";
  }

  string distinct_prefix = agg.IsDistinct() ? "DISTINCT " : "";

  if (agg.children.empty()) {
    // No arguments (shouldn't happen for non-count_star, but handle gracefully)
    return func_name + "()";
  }

  // Serialize arguments
  string args;
  for (idx_t i = 0; i < agg.children.size(); i++) {
    if (i > 0) {
      args += ", ";
    }
    string child_sql = ExpressionToSQL(*agg.children[i], column_names);
    if (child_sql.empty()) {
      return "";  // Bail out - unsupported child expression
    }
    args += child_sql;
  }

  return func_name + "(" + distinct_prefix + args + ")";
}

//===----------------------------------------------------------------------===//
// Split-Safety Checker
//===----------------------------------------------------------------------===//

//! Check whether an aggregate function is safe to push down in split mode.
//! In split mode, results from multiple splits must be mergeable.
static bool IsSplitSafe(const BoundAggregateExpression& agg) {
  string name = agg.function.name;
  if (agg.IsDistinct()) {
    return false;  // COUNT DISTINCT not split-safe
  }
  if (name == "sum" || name == "count" || name == "count_star" || name == "min" || name == "max") {
    return true;
  }
  return false;  // avg, stddev, etc. not split-safe
}

//===----------------------------------------------------------------------===//
// Supported Aggregate Check
//===----------------------------------------------------------------------===//

//! Check whether an aggregate function is supported for pushdown at all
static bool IsSupportedAggregate(const string& name) {
  return name == "sum" || name == "count" || name == "count_star" || name == "min" ||
         name == "max" || name == "avg";
}

//===----------------------------------------------------------------------===//
// Pattern Matching & Plan Rewriting
//===----------------------------------------------------------------------===//

//! Find dd_read_arrow LogicalGet through optional intermediate nodes
//! Returns pointer to the LogicalGet if pattern matches, nullptr otherwise
static LogicalGet* FindDDReadArrowGet(LogicalOperator& op) {
  if (op.type == LogicalOperatorType::LOGICAL_GET) {
    auto& get = op.Cast<LogicalGet>();
    if (get.function.name == "dd_read_arrow") {
      return &get;
    }
  }
  return nullptr;
}

//! Try to extract filter expressions from a LogicalFilter node
static vector<string> ExtractFilterSQL(LogicalOperator& filter_op,
                                        const vector<string>& column_names) {
  vector<string> conditions;
  for (auto& expr : filter_op.expressions) {
    string sql = ExpressionToSQL(*expr, column_names);
    if (sql.empty()) {
      // Unsupported filter expression - return empty to signal bail-out
      return {};
    }
    conditions.push_back(sql);
  }
  return conditions;
}

//! Attempt to rewrite an aggregation node that sits above dd_read_arrow.
//! Only pushes down aggregations WITHOUT GROUP BY (the most valuable case:
//! SELECT count(*), sum(x) FROM dd_read_arrow(...) returns 1 row instead of all).
//! GROUP BY queries are left for local execution to avoid column binding complexity.
//! Returns true if the rewrite was performed.
static bool TryRewriteAggregation(unique_ptr<LogicalOperator>& op) {
  if (op->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    return false;
  }

  auto& aggregate = op->Cast<LogicalAggregate>();

  // Only handle aggregates without GROUP BY
  if (!aggregate.groups.empty()) {
    return false;
  }

  if (aggregate.children.size() != 1) {
    return false;
  }

  // Navigate through the subtree to find dd_read_arrow GET
  // Track both the raw pointer and the unique_ptr owner for later extraction
  // Pattern 1: AGG -> GET
  // Pattern 2: AGG -> FILTER -> GET
  // Pattern 3: AGG -> PROJECTION -> GET
  // Pattern 4: AGG -> PROJECTION -> FILTER -> GET
  LogicalOperator* child = aggregate.children[0].get();
  LogicalOperator* filter_node = nullptr;
  LogicalGet* get_node = nullptr;
  unique_ptr<LogicalOperator>* get_owner = nullptr;

  if (child->type == LogicalOperatorType::LOGICAL_GET) {
    get_node = FindDDReadArrowGet(*child);
    if (get_node) {
      get_owner = &aggregate.children[0];
    }
  } else if (child->type == LogicalOperatorType::LOGICAL_FILTER) {
    filter_node = child;
    if (child->children.size() == 1) {
      get_node = FindDDReadArrowGet(*child->children[0]);
      if (get_node) {
        get_owner = &child->children[0];
      }
    }
  } else if (child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
    if (child->children.size() == 1) {
      if (child->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
        get_node = FindDDReadArrowGet(*child->children[0]);
        if (get_node) {
          get_owner = &child->children[0];
        }
      } else if (child->children[0]->type == LogicalOperatorType::LOGICAL_FILTER) {
        filter_node = child->children[0].get();
        if (filter_node->children.size() == 1) {
          get_node = FindDDReadArrowGet(*filter_node->children[0]);
          if (get_node) {
            get_owner = &filter_node->children[0];
          }
        }
      }
    }
  }

  if (!get_node || !get_owner) {
    return false;
  }

  // Get the function data
  auto& func_data = get_node->bind_data->Cast<ReadArrowDDFunctionData>();
  const auto& original_column_names = func_data.column_names;

  // Build effective column names for the aggregate's reference indices.
  // The BoundColumnRefExpression.binding.column_index refers to the position
  // in the child operator's output (i.e., the GET's projected columns via column_ids),
  // NOT the position in the original table schema. We must resolve through
  // column_ids to get actual column names.
  vector<string> column_names;
  auto& col_ids = get_node->GetColumnIds();
  for (auto& cid : col_ids) {
    auto primary_idx = cid.GetPrimaryIndex();
    if (primary_idx < original_column_names.size()) {
      column_names.push_back(original_column_names[primary_idx]);
    } else {
      // Skip COLUMN_IDENTIFIER_ROW_ID or invalid indices
      column_names.push_back("");
    }
  }
  bool split_mode = func_data.split_mode;

  // Validate all aggregate expressions
  for (auto& expr : aggregate.expressions) {
    if (expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
      return false;
    }
    auto& agg = expr->Cast<BoundAggregateExpression>();
    if (!IsSupportedAggregate(agg.function.name)) {
      return false;
    }
    if (split_mode && !IsSplitSafe(agg)) {
      return false;
    }
  }

  // Serialize aggregate expressions to SQL
  vector<string> agg_sql_parts;
  vector<LogicalType> new_types;
  vector<string> new_column_names;
  for (auto& expr : aggregate.expressions) {
    auto& agg = expr->Cast<BoundAggregateExpression>();
    string sql = AggregateToSQL(agg, column_names);
    if (sql.empty()) {
      return false;
    }
    agg_sql_parts.push_back(sql);
    new_column_names.push_back(sql);
    new_types.push_back(expr->return_type);
  }

  // Build SELECT clause
  string select_clause;
  for (idx_t i = 0; i < agg_sql_parts.size(); i++) {
    if (i > 0) {
      select_clause += ", ";
    }
    select_clause += agg_sql_parts[i];
  }

  // Extract filter conditions if present
  string where_clause;
  if (filter_node) {
    vector<string> filter_conditions = ExtractFilterSQL(*filter_node, column_names);
    if (filter_conditions.empty() && !filter_node->expressions.empty()) {
      return false;
    }
    if (!filter_conditions.empty()) {
      where_clause = " WHERE ";
      for (idx_t i = 0; i < filter_conditions.size(); i++) {
        if (i > 0) {
          where_clause += " AND ";
        }
        where_clause += filter_conditions[i];
      }
    }
  }

  // Build the new query: SELECT <aggregates> FROM (<original>) AS _subq [WHERE ...]
  string new_query = "SELECT " + select_clause + " FROM (" + func_data.original_query +
                     ") AS _subq" + where_clause;

  // Store the aggregation pushdown query in the bind data
  func_data.aggregation_pushdown_query = new_query;

  // Update the bind data types/names to match the aggregate output
  func_data.all_types = new_types;
  func_data.column_names = new_column_names;

  // Update the LogicalGet to reflect the aggregate output schema
  get_node->returned_types = new_types;
  get_node->names = new_column_names;

  // Clear column IDs so the planner rebuilds them for the new schema
  get_node->ClearColumnIds();
  for (idx_t i = 0; i < new_types.size(); i++) {
    get_node->AddColumnId(i);
  }

  // Set the GET's table_index to match the aggregate's aggregate_index
  // so that parent operators' column bindings still resolve correctly.
  // (Aggregate without GROUP BY produces bindings at {aggregate_index, i})
  get_node->table_index = aggregate.aggregate_index;

  // Replace the entire aggregate subtree with just the GET
  op = std::move(*get_owner);

  return true;
}

//! Recursively walk the plan tree and attempt aggregation pushdown
static void OptimizeNode(unique_ptr<LogicalOperator>& op) {
  if (!op) {
    return;
  }

  // Try to rewrite this node
  if (TryRewriteAggregation(op)) {
    // After rewriting, continue optimizing in case there are nested patterns
    OptimizeNode(op);
    return;
  }

  // Recurse into children
  for (auto& child : op->children) {
    OptimizeNode(child);
  }
}

//! Top-level optimizer callback
static void OptimizeAggregationPushdown(OptimizerExtensionInput& input,
                                         unique_ptr<LogicalOperator>& plan) {
  OptimizeNode(plan);
}

//===----------------------------------------------------------------------===//
// Registration
//===----------------------------------------------------------------------===//

void RegisterAggregationPushdown(ExtensionLoader& loader) {
  auto& db = loader.GetDatabaseInstance();
  auto& config = DBConfig::GetConfig(db);

  OptimizerExtension ext;
  ext.pre_optimize_function = OptimizeAggregationPushdown;
  config.optimizer_extensions.push_back(std::move(ext));
}

}  // namespace ext_nanoarrow
}  // namespace duckdb
