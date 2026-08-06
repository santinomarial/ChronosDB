#include "chronos/query/executor.hpp"

#include "chronos/common/status.hpp"
#include "chronos/query/evaluator.hpp"
#include "chronos/query/literal.hpp"
#include "query/decimal_internal.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

struct ExecutionFailure {
  SqlDiagnostic diagnostic;
};

struct JoinedRow {
  std::vector<const ScalarInputRow*> sources;
};

struct ProjectedRow {
  std::vector<ScalarValue> values;
  std::vector<ScalarValue> order_values;
  std::vector<ScalarValue> aggregate_values;
  std::vector<ScalarValue> group_key;
  JoinedRow input;
  bool aggregated{};
};

template <typename Value>
[[nodiscard]] const Value* optional_pointer(const std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

[[nodiscard]] SqlDiagnostic diagnostic(const SourceSpan span, const common::StatusCode status,
                                       const std::string_view message) {
  return SqlDiagnostic{SqlDiagnosticCode::kExecutionFailure, span,
                       common::Status{status, std::string{message}}};
}

[[noreturn]] void fail(const SourceSpan span, const common::StatusCode status,
                       const std::string_view message) {
  throw ExecutionFailure{diagnostic(span, status, message)};
}

[[nodiscard]] const SqlExpression* find_expression(const SqlExpression& expression,
                                                   const SourceSpan span) noexcept {
  if (expression.span() == span)
    return std::addressof(expression);
  for (const SqlExpression& child : expression.children()) {
    if (const SqlExpression* found = find_expression(child, span); found != nullptr)
      return found;
  }
  return nullptr;
}

[[nodiscard]] int compare_values(const ScalarValue& left, const ScalarValue& right,
                                 const ScalarNullPlacement nulls, const SourceSpan span) {
  const common::Result<int> result = compare_scalar_values(left, right, nulls);
  if (!result.has_value())
    fail(span, result.error().code(), result.error().message());
  return *result;
}

[[nodiscard]] bool keys_equal(const std::span<const ScalarValue> left,
                              const std::span<const ScalarValue> right, const SourceSpan span) {
  if (left.size() != right.size())
    return false;
  for (std::size_t index = 0U; index < left.size(); ++index) {
    if (compare_values(left[index], right[index], ScalarNullPlacement::kLast, span) != 0)
      return false;
  }
  return true;
}

[[nodiscard]] int compare_uuid(const common::Uuid& left, const common::Uuid& right) noexcept {
  if (left == right)
    return 0;
  return left < right ? -1 : 1;
}

[[nodiscard]] int compare_version_identity(const ScalarInputRow& left,
                                           const ScalarInputRow& right) noexcept {
  const int wal = compare_uuid(left.wal_id, right.wal_id);
  if (wal != 0)
    return wal;
  if (left.record_sequence != right.record_sequence)
    return left.record_sequence < right.record_sequence ? -1 : 1;
  if (left.row_ordinal != right.row_ordinal)
    return left.row_ordinal < right.row_ordinal ? -1 : 1;
  return 0;
}

[[nodiscard]] int compare_physical(const ScalarInputRow& left, const ScalarInputRow& right,
                                   const schema::TableSchema& schema, const SourceSpan span) {
  for (const schema::ColumnId column : schema.physical_ordering_key()) {
    const std::optional<std::size_t> ordinal = schema.column_ordinal(column);
    if (!ordinal.has_value())
      fail(span, common::StatusCode::kInternal, "Physical ordering key has no schema ordinal");
    const int comparison = compare_values(left.columns[*ordinal], right.columns[*ordinal],
                                          ScalarNullPlacement::kLast, span);
    if (comparison != 0)
      return comparison;
  }
  return compare_version_identity(left, right);
}

class Executor {
public:
  Executor(const BoundSqlSelect& plan, const ScalarSnapshotProvider& provider,
           const ScalarQueryLimits limits, ScalarQueryMetrics* metrics,
           const bool allow_explain_analyze) noexcept
      : plan_(plan), provider_(provider), limits_(limits), metrics_(metrics),
        allow_explain_analyze_(allow_explain_analyze) {}

  [[nodiscard]] SqlResult<ScalarQueryResult> run() {
    try {
      validate_limits_and_mode();
      resolve_snapshots();
      std::vector<JoinedRow> rows = latest_rows();
      if (metrics_ != nullptr)
        metrics_->rows_after_latest = rows.size();
      apply_asof_joins(rows);
      apply_where(rows);
      if (metrics_ != nullptr)
        metrics_->rows_after_where = rows.size();
      std::vector<ProjectedRow> projected =
          aggregate_query() ? aggregate_project(rows) : project(rows);
      apply_order(projected);
      apply_limit(projected);
      std::vector<std::vector<ScalarValue>> output;
      output.reserve(projected.size());
      for (ProjectedRow& row : projected)
        output.push_back(std::move(row.values));
      if (metrics_ != nullptr)
        metrics_->output_rows = output.size();
      std::vector<ScalarResultColumn> columns;
      columns.reserve(plan_.outputs().size());
      for (const BoundOutputColumn& column : plan_.outputs()) {
        columns.push_back(ScalarResultColumn{.name = column.name,
                                             .name_quoted = column.name_quoted,
                                             .type = column.type,
                                             .nullable = column.nullable});
      }
      return ScalarQueryResult{std::move(columns), std::move(output)};
    } catch (ExecutionFailure& failure) {
      return std::unexpected(std::move(failure.diagnostic));
    } catch (const std::bad_alloc&) {
      return std::unexpected(diagnostic(plan_.syntax().span(),
                                        common::StatusCode::kResourceExhausted,
                                        "Scalar query allocation failed"));
    } catch (const std::length_error&) {
      return std::unexpected(diagnostic(plan_.syntax().span(),
                                        common::StatusCode::kResourceExhausted,
                                        "Scalar query container limit was exceeded"));
    }
  }

private:
  void validate_limits_and_mode() const {
    if (limits_.maximum_rows_per_source == 0U || limits_.maximum_intermediate_rows == 0U ||
        limits_.maximum_output_rows == 0U || limits_.maximum_groups == 0U) {
      fail(plan_.syntax().span(), common::StatusCode::kInvalidArgument,
           "Scalar query limits must be nonzero");
    }
    const bool accepted =
        plan_.syntax().mode() == SqlSelectMode::kSelect ||
        (allow_explain_analyze_ && plan_.syntax().mode() == SqlSelectMode::kExplainAnalyze);
    if (!accepted) {
      fail(plan_.syntax().span(), common::StatusCode::kNotSupported,
           "This scalar execution entry point accepts SELECT only");
    }
  }

  [[nodiscard]] bool aggregate_query() const noexcept {
    return !plan_.syntax().group_by().empty() ||
           std::ranges::any_of(plan_.outputs(), &BoundOutputColumn::contains_aggregate);
  }

  void resolve_snapshots() {
    snapshots_.reserve(plan_.sources().size());
    const std::string* system_time = optional_pointer(plan_.syntax().system_time());
    for (std::size_t source = 0U; source < plan_.sources().size(); ++source) {
      std::optional<std::int64_t> as_of;
      if (source == 0U && system_time != nullptr) {
        const common::Result<std::int64_t> parsed = parse_sql_timestamp_ns_literal(*system_time);
        if (!parsed.has_value())
          fail(plan_.syntax().span(), common::StatusCode::kInternal,
               "Bound system-time literal is invalid");
        as_of = *parsed;
      }
      common::Result<std::shared_ptr<const ScalarTableSnapshot>> resolved =
          provider_.resolve(plan_.sources()[source].schema_ptr(), as_of);
      if (!resolved.has_value())
        fail(plan_.syntax().span(), resolved.error().code(), resolved.error().message());
      if (*resolved == nullptr || (*resolved)->schema_ptr() == nullptr ||
          *(*resolved)->schema_ptr() != *plan_.sources()[source].schema_ptr()) {
        fail(plan_.syntax().span(), common::StatusCode::kInvalidArgument,
             "Snapshot provider returned a different schema than the bound plan");
      }
      if ((*resolved)->rows().size() > limits_.maximum_rows_per_source)
        fail(plan_.syntax().span(), common::StatusCode::kResourceExhausted,
             "Scalar query source row count exceeds the limit");
      snapshots_.push_back(std::move(*resolved));
      if (metrics_ != nullptr)
        metrics_->source_rows += snapshots_.back()->rows().size();
    }
    null_rows_.resize(plan_.sources().size());
    for (std::size_t source = 0U; source < null_rows_.size(); ++source) {
      for (const schema::ColumnDefinition& column : plan_.sources()[source].schema_ptr()->columns())
        null_rows_[source].push_back(ScalarValue::null(column.type()));
    }
  }

  [[nodiscard]] std::vector<ScalarValue> latest_key(const ScalarInputRow& row) const {
    std::vector<ScalarValue> key;
    const BoundLatestBy* latest = optional_pointer(plan_.latest_by());
    if (latest == nullptr)
      fail(plan_.syntax().span(), common::StatusCode::kInternal,
           "LATEST key requested without a bound plan");
    key.reserve(latest->key_column_ordinals.size());
    for (const std::size_t ordinal : latest->key_column_ordinals)
      key.push_back(row.columns[ordinal]);
    return key;
  }

  [[nodiscard]] ScalarEvaluationContext
  context(const JoinedRow& row, const std::span<const ScalarValue> outputs = {},
          const std::span<const ScalarExpressionOverride> overrides = {}) {
    context_rows_.clear();
    context_rows_.reserve(row.sources.size());
    for (std::size_t source = 0U; source < row.sources.size(); ++source) {
      const ScalarInputRow* input = row.sources[source];
      context_rows_.push_back(ScalarSourceRow{input == nullptr
                                                  ? std::span<const ScalarValue>{null_rows_[source]}
                                                  : std::span<const ScalarValue>{input->columns}});
    }
    return ScalarEvaluationContext{
        .sources = context_rows_, .projected_outputs = outputs, .overrides = overrides};
  }

  [[nodiscard]] ScalarValue
  evaluate(const SqlExpression& expression, const JoinedRow& row,
           const std::span<const ScalarValue> outputs = {},
           const std::span<const ScalarExpressionOverride> overrides = {}) {
    SqlResult<ScalarValue> value =
        evaluate_sql_v1_expression(plan_, expression, context(row, outputs, overrides));
    if (!value.has_value())
      throw ExecutionFailure{value.error()};
    return std::move(*value);
  }

  [[nodiscard]] SqlTruthValue predicate(const SqlExpression& expression, const JoinedRow& row) {
    SqlResult<SqlTruthValue> value = evaluate_sql_v1_predicate(plan_, expression, context(row));
    if (!value.has_value())
      throw ExecutionFailure{value.error()};
    return *value;
  }

  [[nodiscard]] bool later_row(const ScalarInputRow& candidate, const ScalarInputRow& current,
                               const ScalarValue& candidate_time, const ScalarValue& current_time,
                               const std::size_t source, const SourceSpan span) {
    const int timestamp =
        compare_values(candidate_time, current_time, ScalarNullPlacement::kFirst, span);
    if (timestamp != 0)
      return timestamp > 0;
    return compare_physical(candidate, current, *plan_.sources()[source].schema_ptr(), span) > 0;
  }

  [[nodiscard]] std::vector<JoinedRow> latest_rows() {
    std::vector<JoinedRow> rows;
    const auto primary = snapshots_.front()->rows();
    const BoundLatestBy* bound_latest = optional_pointer(plan_.latest_by());
    const SqlLatestBy* latest = optional_pointer(plan_.syntax().latest_by());
    if (bound_latest == nullptr && latest == nullptr) {
      rows.reserve(primary.size());
      for (const ScalarInputRow& row : primary)
        rows.push_back(JoinedRow{.sources = {std::addressof(row)}});
      return rows;
    }
    if (bound_latest == nullptr || latest == nullptr)
      fail(plan_.syntax().span(), common::StatusCode::kInternal,
           "LATEST syntax and bound plan disagree");
    struct Winner {
      std::vector<ScalarValue> key;
      const ScalarInputRow* row{};
      ScalarValue timestamp;
    };
    std::vector<Winner> winners;
    for (const ScalarInputRow& row : primary) {
      std::vector<ScalarValue> key = latest_key(row);
      JoinedRow joined{.sources = {std::addressof(row)}};
      ScalarValue timestamp = evaluate(latest->timestamp, joined);
      auto winner = std::ranges::find_if(winners, [&](const Winner& candidate) {
        return keys_equal(candidate.key, key, latest->timestamp.span());
      });
      if (winner == winners.end()) {
        if (winners.size() >= limits_.maximum_groups)
          fail(latest->timestamp.span(), common::StatusCode::kResourceExhausted,
               "LATEST BY group count exceeds the limit");
        winners.push_back(
            Winner{.key = std::move(key), .row = std::addressof(row), .timestamp = timestamp});
      } else if (later_row(row, *winner->row, timestamp, winner->timestamp, 0U,
                           latest->timestamp.span())) {
        winner->row = std::addressof(row);
        winner->timestamp = std::move(timestamp);
      }
    }
    rows.reserve(winners.size());
    for (const Winner& winner : winners)
      rows.push_back(JoinedRow{.sources = {winner.row}});
    return rows;
  }

  void apply_asof_joins(std::vector<JoinedRow>& rows) {
    for (std::size_t join_index = 0U; join_index < plan_.asof_joins().size(); ++join_index) {
      const BoundAsofJoin& bound = plan_.asof_joins()[join_index];
      const SqlAsofJoin& syntax = plan_.syntax().asof_joins()[join_index];
      const SqlExpression* right_time =
          find_expression(syntax.condition, bound.right_timestamp_expression_span);
      if (right_time == nullptr)
        fail(syntax.condition.span(), common::StatusCode::kInternal,
             "Bound ASOF right timestamp expression is absent");
      std::vector<JoinedRow> joined_rows;
      joined_rows.reserve(rows.size());
      for (const JoinedRow& left : rows) {
        const ScalarInputRow* winner = nullptr;
        std::optional<ScalarValue> winner_time;
        for (const ScalarInputRow& candidate : snapshots_[bound.right_source_ordinal]->rows()) {
          if (metrics_ != nullptr)
            ++metrics_->asof_candidate_comparisons;
          JoinedRow combined = left;
          combined.sources.push_back(std::addressof(candidate));
          if (predicate(syntax.condition, combined) != SqlTruthValue::kTrue)
            continue;
          ScalarValue candidate_time = evaluate(*right_time, combined);
          const ScalarValue* previous_time = optional_pointer(winner_time);
          if (winner != nullptr && previous_time == nullptr)
            fail(right_time->span(), common::StatusCode::kInternal, "ASOF winner has no timestamp");
          bool choose = winner == nullptr;
          if (!choose)
            choose = later_row(candidate, *winner, candidate_time, *previous_time,
                               bound.right_source_ordinal, right_time->span());
          if (choose) {
            winner = std::addressof(candidate);
            winner_time = std::move(candidate_time);
          }
        }
        if (winner != nullptr || bound.left) {
          JoinedRow combined = left;
          combined.sources.push_back(winner);
          joined_rows.push_back(std::move(combined));
        }
      }
      rows = std::move(joined_rows);
      if (rows.size() > limits_.maximum_intermediate_rows)
        fail(syntax.condition.span(), common::StatusCode::kResourceExhausted,
             "ASOF intermediate row count exceeds the limit");
    }
  }

  void apply_where(std::vector<JoinedRow>& rows) {
    if (plan_.syntax().where() == nullptr)
      return;
    std::erase_if(rows, [&](const JoinedRow& row) {
      return predicate(*plan_.syntax().where(), row) != SqlTruthValue::kTrue;
    });
  }

  [[nodiscard]] std::vector<ProjectedRow> project(const std::vector<JoinedRow>& rows) {
    if (rows.size() > limits_.maximum_output_rows)
      fail(plan_.syntax().span(), common::StatusCode::kResourceExhausted,
           "Scalar query output row count exceeds the limit");
    std::vector<ProjectedRow> projected;
    projected.reserve(rows.size());
    for (const JoinedRow& row : rows) {
      ProjectedRow output{.values = {},
                          .order_values = {},
                          .aggregate_values = {},
                          .group_key = {},
                          .input = row,
                          .aggregated = false};
      output.values.reserve(plan_.outputs().size());
      for (const BoundOutputColumn& column : plan_.outputs()) {
        const SourceSpan* expression_span = optional_pointer(column.expression_span);
        if (expression_span != nullptr) {
          const SqlExpression* expression = nullptr;
          for (const SqlSelectItem& item : plan_.syntax().items()) {
            if (item.expression() != nullptr && item.expression()->span() == *expression_span)
              expression = item.expression();
          }
          if (expression == nullptr)
            fail(plan_.syntax().span(), common::StatusCode::kInternal,
                 "Bound output expression is absent");
          output.values.push_back(evaluate(*expression, row));
        } else {
          if (!column.source_ordinal.has_value() || !column.column_ordinal.has_value())
            fail(plan_.syntax().span(), common::StatusCode::kInternal,
                 "Bound star output has no source column");
          const ScalarInputRow* source = row.sources[*column.source_ordinal];
          output.values.push_back(source == nullptr
                                      ? null_rows_[*column.source_ordinal][*column.column_ordinal]
                                      : source->columns[*column.column_ordinal]);
        }
      }
      projected.push_back(std::move(output));
    }
    return projected;
  }

  [[nodiscard]] static bool aggregate_function(const SqlExpression& expression) noexcept {
    if (expression.kind() != SqlExpressionKind::kFunction)
      return false;
    const std::string& name = expression.text();
    return name == "count" || name == "sum" || name == "avg" || name == "min" || name == "max" ||
           name == "var_pop" || name == "var_samp";
  }

  void collect_aggregates(const SqlExpression& expression) {
    if (aggregate_function(expression)) {
      const auto found = std::ranges::find_if(aggregate_expressions_, [&](const auto* candidate) {
        return candidate->span() == expression.span();
      });
      if (found == aggregate_expressions_.end())
        aggregate_expressions_.push_back(std::addressof(expression));
      return;
    }
    for (const SqlExpression& child : expression.children())
      collect_aggregates(child);
  }

  [[nodiscard]] const schema::LogicalType& expression_type(const SqlExpression& expression) const {
    const BoundExpressionInfo* information = plan_.find_expression(expression.span());
    if (information == nullptr || !information->type.has_value())
      fail(expression.span(), common::StatusCode::kInternal,
           "Aggregate expression has no bound result type");
    return information->type.value();
  }

  [[nodiscard]] static double numeric_double(const ScalarValue& value, const SourceSpan span) {
    if (const auto* signed_value = std::get_if<std::int64_t>(&value.storage());
        signed_value != nullptr)
      return static_cast<double>(*signed_value);
    if (const auto* unsigned_value = std::get_if<std::uint64_t>(&value.storage());
        unsigned_value != nullptr)
      return static_cast<double>(*unsigned_value);
    if (const auto* float_value = std::get_if<float>(&value.storage()); float_value != nullptr)
      return static_cast<double>(*float_value);
    if (const auto* double_value = std::get_if<double>(&value.storage()); double_value != nullptr)
      return *double_value;
    if (const auto* decimal_value = std::get_if<Decimal128Value>(&value.storage());
        decimal_value != nullptr) {
      const schema::LogicalType* value_type = optional_pointer(value.type());
      if (value_type == nullptr)
        fail(span, common::StatusCode::kInternal, "DECIMAL aggregate input is untyped");
      const common::Result<double> converted =
          detail::decimal_to_double(*decimal_value, *value_type);
      if (!converted.has_value())
        fail(span, converted.error().code(), converted.error().message());
      return *converted;
    }
    fail(span, common::StatusCode::kInternal, "Aggregate numeric input has invalid storage");
  }

  [[nodiscard]] ScalarValue exact_sum(const SqlExpression& aggregate,
                                      const std::vector<ScalarValue>& values) const {
    const schema::LogicalType& result_type = expression_type(aggregate);
    if (result_type.kind() == schema::LogicalTypeKind::kFloat32) {
      float sum = 0.0F;
      for (const ScalarValue& value : values)
        sum += *std::get_if<float>(&value.storage());
      return ScalarValue::float32(sum).value();
    }
    if (result_type.kind() == schema::LogicalTypeKind::kFloat64) {
      double sum = 0.0;
      for (const ScalarValue& value : values)
        sum += *std::get_if<double>(&value.storage());
      return ScalarValue::float64(sum).value();
    }
    detail::ExactNumericAccumulator accumulator;
    for (const ScalarValue& value : values) {
      common::Result<void> added;
      if (const auto* signed_value = std::get_if<std::int64_t>(&value.storage());
          signed_value != nullptr) {
        added = accumulator.add_signed(*signed_value);
      } else if (const auto* unsigned_value = std::get_if<std::uint64_t>(&value.storage());
                 unsigned_value != nullptr) {
        added = accumulator.add_unsigned(*unsigned_value);
      } else {
        const auto* decimal_value = std::get_if<Decimal128Value>(&value.storage());
        if (decimal_value == nullptr)
          fail(aggregate.span(), common::StatusCode::kInternal,
               "Exact aggregate input has invalid storage");
        added = accumulator.add_decimal(*decimal_value);
      }
      if (!added.has_value())
        fail(aggregate.span(), added.error().code(), added.error().message());
    }
    if (result_type.is_decimal()) {
      const common::Result<Decimal128Value> result = accumulator.decimal_result(result_type);
      if (!result.has_value())
        fail(aggregate.span(), result.error().code(), result.error().message());
      return ScalarValue::decimal(result_type, *result).value();
    }
    if (result_type.kind() >= schema::LogicalTypeKind::kInt8 &&
        result_type.kind() <= schema::LogicalTypeKind::kInt64) {
      const common::Result<std::int64_t> result = accumulator.signed_result();
      if (!result.has_value())
        fail(aggregate.span(), result.error().code(), result.error().message());
      const common::Result<ScalarValue> scalar = ScalarValue::signed_value(result_type, *result);
      if (!scalar.has_value())
        fail(aggregate.span(), scalar.error().code(), scalar.error().message());
      return *scalar;
    }
    const common::Result<std::uint64_t> result = accumulator.unsigned_result();
    if (!result.has_value())
      fail(aggregate.span(), result.error().code(), result.error().message());
    const common::Result<ScalarValue> scalar = ScalarValue::unsigned_value(result_type, *result);
    if (!scalar.has_value())
      fail(aggregate.span(), scalar.error().code(), scalar.error().message());
    return *scalar;
  }

  [[nodiscard]] ScalarValue evaluate_aggregate(const SqlExpression& aggregate,
                                               const std::span<const JoinedRow* const> rows) {
    if (aggregate.text() == "count") {
      std::int64_t count = 0;
      const bool star = aggregate.children().front().kind() == SqlExpressionKind::kStar;
      for (const JoinedRow* row : rows) {
        if (star || !evaluate(aggregate.children().front(), *row).is_null())
          ++count;
      }
      return ScalarValue::signed_value(expression_type(aggregate), count).value();
    }
    std::vector<ScalarValue> values;
    values.reserve(rows.size());
    for (const JoinedRow* row : rows) {
      ScalarValue value = evaluate(aggregate.children().front(), *row);
      if (!value.is_null())
        values.push_back(std::move(value));
    }
    if (values.empty())
      return ScalarValue::null(expression_type(aggregate));
    if (aggregate.text() == "sum")
      return exact_sum(aggregate, values);
    if (aggregate.text() == "min" || aggregate.text() == "max") {
      ScalarValue result = values.front();
      for (std::size_t index = 1U; index < values.size(); ++index) {
        const int comparison =
            compare_values(values[index], result, ScalarNullPlacement::kLast, aggregate.span());
        if ((aggregate.text() == "min" && comparison < 0) ||
            (aggregate.text() == "max" && comparison > 0))
          result = values[index];
      }
      return result;
    }
    if (aggregate.text() == "avg") {
      double sum = 0.0;
      for (const ScalarValue& value : values)
        sum += numeric_double(value, aggregate.span());
      return ScalarValue::float64(sum / static_cast<double>(values.size())).value();
    }
    double mean = 0.0;
    double squared_distance = 0.0;
    std::size_t count = 0U;
    for (const ScalarValue& value : values) {
      ++count;
      const double number = numeric_double(value, aggregate.span());
      const double delta = number - mean;
      mean += delta / static_cast<double>(count);
      squared_distance += delta * (number - mean);
    }
    if (aggregate.text() == "var_samp" && count < 2U)
      return ScalarValue::null(expression_type(aggregate));
    const std::size_t divisor = aggregate.text() == "var_samp" ? count - 1U : count;
    return ScalarValue::float64(squared_distance / static_cast<double>(divisor)).value();
  }

  struct Group {
    std::vector<ScalarValue> key;
    std::vector<const JoinedRow*> rows;
  };

  [[nodiscard]] std::vector<Group> group_rows(const std::vector<JoinedRow>& rows) {
    std::vector<Group> groups;
    if (plan_.syntax().group_by().empty()) {
      Group group;
      group.rows.reserve(rows.size());
      for (const JoinedRow& row : rows)
        group.rows.push_back(std::addressof(row));
      groups.push_back(std::move(group));
      return groups;
    }
    for (const JoinedRow& row : rows) {
      std::vector<ScalarValue> key;
      key.reserve(plan_.syntax().group_by().size());
      for (const SqlExpression& expression : plan_.syntax().group_by())
        key.push_back(evaluate(expression, row));
      auto group = std::ranges::find_if(groups, [&](const Group& candidate) {
        return keys_equal(candidate.key, key, plan_.syntax().span());
      });
      if (group == groups.end()) {
        if (groups.size() >= limits_.maximum_groups)
          fail(plan_.syntax().span(), common::StatusCode::kResourceExhausted,
               "Scalar aggregate group count exceeds the limit");
        groups.push_back(Group{.key = std::move(key), .rows = {std::addressof(row)}});
      } else {
        group->rows.push_back(std::addressof(row));
      }
    }
    return groups;
  }

  [[nodiscard]] std::vector<ProjectedRow> aggregate_project(const std::vector<JoinedRow>& rows) {
    aggregate_expressions_.clear();
    for (const SqlSelectItem& item : plan_.syntax().items())
      collect_aggregates(*item.expression());
    for (const SqlOrderItem& order : plan_.syntax().order_by())
      collect_aggregates(order.expression);
    std::vector<Group> groups = group_rows(rows);
    if (metrics_ != nullptr)
      metrics_->groups = groups.size();
    if (groups.size() > limits_.maximum_output_rows)
      fail(plan_.syntax().span(), common::StatusCode::kResourceExhausted,
           "Scalar aggregate output exceeds the row limit");
    std::vector<ProjectedRow> projected;
    projected.reserve(groups.size());
    for (Group& group : groups) {
      JoinedRow empty;
      empty.sources.resize(plan_.sources().size(), nullptr);
      const JoinedRow& representative = group.rows.empty() ? empty : *group.rows.front();
      ProjectedRow output{.values = {},
                          .order_values = {},
                          .aggregate_values = {},
                          .group_key = group.key,
                          .input = representative,
                          .aggregated = true};
      output.aggregate_values.reserve(aggregate_expressions_.size());
      for (const SqlExpression* aggregate : aggregate_expressions_)
        output.aggregate_values.push_back(evaluate_aggregate(*aggregate, group.rows));
      std::vector<ScalarExpressionOverride> overrides;
      overrides.reserve(aggregate_expressions_.size());
      for (std::size_t index = 0U; index < aggregate_expressions_.size(); ++index) {
        overrides.push_back(
            ScalarExpressionOverride{.expression_span = aggregate_expressions_[index]->span(),
                                     .value = std::addressof(output.aggregate_values[index])});
      }
      output.values.reserve(plan_.outputs().size());
      for (const SqlSelectItem& item : plan_.syntax().items())
        output.values.push_back(evaluate(*item.expression(), representative, {}, overrides));
      projected.push_back(std::move(output));
    }
    return projected;
  }

  [[nodiscard]] int compare_logical_identity(const JoinedRow& left, const JoinedRow& right,
                                             const SourceSpan span) {
    for (std::size_t source = 0U; source < left.sources.size(); ++source) {
      const ScalarInputRow* lhs = left.sources[source];
      const ScalarInputRow* rhs = right.sources[source];
      if (lhs == nullptr || rhs == nullptr) {
        if (lhs != rhs)
          return lhs == nullptr ? -1 : 1;
        continue;
      }
      const schema::TableSchema& schema = *plan_.sources()[source].schema_ptr();
      if (schema.deduplication_key().empty()) {
        if (lhs->generated_logical_identity != rhs->generated_logical_identity)
          return lhs->generated_logical_identity < rhs->generated_logical_identity ? -1 : 1;
      } else {
        for (const schema::ColumnId key : schema.deduplication_key()) {
          const std::optional<std::size_t> key_ordinal = schema.column_ordinal(key);
          const std::size_t* ordinal = optional_pointer(key_ordinal);
          if (ordinal == nullptr)
            fail(span, common::StatusCode::kInternal, "Deduplication key has no schema ordinal");
          const int comparison = compare_values(lhs->columns[*ordinal], rhs->columns[*ordinal],
                                                ScalarNullPlacement::kLast, span);
          if (comparison != 0)
            return comparison;
        }
      }
    }
    for (std::size_t source = 0U; source < left.sources.size(); ++source) {
      const ScalarInputRow* lhs = left.sources[source];
      const ScalarInputRow* rhs = right.sources[source];
      if (lhs == nullptr || rhs == nullptr)
        continue;
      if (lhs->system_commit_position != rhs->system_commit_position)
        return lhs->system_commit_position < rhs->system_commit_position ? -1 : 1;
      if (lhs->row_ordinal != rhs->row_ordinal)
        return lhs->row_ordinal < rhs->row_ordinal ? -1 : 1;
    }
    return 0;
  }

  void apply_order(std::vector<ProjectedRow>& rows) {
    if (plan_.syntax().order_by().empty())
      return;
    for (ProjectedRow& row : rows) {
      std::vector<ScalarExpressionOverride> overrides;
      if (row.aggregated) {
        overrides.reserve(aggregate_expressions_.size());
        for (std::size_t index = 0U; index < aggregate_expressions_.size(); ++index) {
          overrides.push_back(
              ScalarExpressionOverride{.expression_span = aggregate_expressions_[index]->span(),
                                       .value = std::addressof(row.aggregate_values[index])});
        }
      }
      row.order_values.reserve(plan_.syntax().order_by().size());
      for (const SqlOrderItem& order : plan_.syntax().order_by())
        row.order_values.push_back(evaluate(order.expression, row.input, row.values, overrides));
    }
    std::ranges::sort(rows, [&](const ProjectedRow& left, const ProjectedRow& right) {
      for (std::size_t index = 0U; index < plan_.syntax().order_by().size(); ++index) {
        const SqlOrderItem& order = plan_.syntax().order_by()[index];
        ScalarNullPlacement nulls = order.direction == SqlOrderDirection::kAscending
                                        ? ScalarNullPlacement::kLast
                                        : ScalarNullPlacement::kFirst;
        if (order.null_order == SqlNullOrder::kFirst)
          nulls = ScalarNullPlacement::kFirst;
        else if (order.null_order == SqlNullOrder::kLast)
          nulls = ScalarNullPlacement::kLast;
        const bool includes_null =
            left.order_values[index].is_null() || right.order_values[index].is_null();
        int comparison = compare_values(left.order_values[index], right.order_values[index], nulls,
                                        order.expression.span());
        if (!includes_null && order.direction == SqlOrderDirection::kDescending)
          comparison = -comparison;
        if (comparison != 0)
          return comparison < 0;
      }
      if (left.aggregated) {
        for (std::size_t index = 0U; index < left.group_key.size(); ++index) {
          const int comparison = compare_values(left.group_key[index], right.group_key[index],
                                                ScalarNullPlacement::kLast, plan_.syntax().span());
          if (comparison != 0)
            return comparison < 0;
        }
        return false;
      }
      return compare_logical_identity(left.input, right.input, plan_.syntax().span()) < 0;
    });
  }

  void apply_limit(std::vector<ProjectedRow>& rows) const {
    const std::uint64_t* limit = optional_pointer(plan_.syntax().limit());
    if (limit == nullptr)
      return;
    if (*limit < rows.size())
      rows.resize(static_cast<std::size_t>(*limit));
  }

  const BoundSqlSelect& plan_;
  const ScalarSnapshotProvider& provider_;
  ScalarQueryLimits limits_;
  std::vector<std::shared_ptr<const ScalarTableSnapshot>> snapshots_;
  std::vector<std::vector<ScalarValue>> null_rows_;
  std::vector<ScalarSourceRow> context_rows_;
  std::vector<const SqlExpression*> aggregate_expressions_;
  ScalarQueryMetrics* metrics_{};
  bool allow_explain_analyze_{};
};

} // namespace

ScalarQueryResult::ScalarQueryResult(std::vector<ScalarResultColumn> columns,
                                     std::vector<std::vector<ScalarValue>> rows) noexcept
    : columns_(std::move(columns)), rows_(std::move(rows)) {}

std::span<const ScalarResultColumn> ScalarQueryResult::columns() const noexcept {
  return columns_;
}

std::span<const std::vector<ScalarValue>> ScalarQueryResult::rows() const noexcept {
  return rows_;
}

SqlResult<ScalarQueryResult> execute_sql_v1_select(const BoundSqlSelect& plan,
                                                   const ScalarSnapshotProvider& provider,
                                                   const ScalarQueryLimits limits) {
  return Executor{plan, provider, limits, nullptr, false}.run();
}

SqlResult<ScalarQueryExecution>
execute_sql_v1_select_measured(const BoundSqlSelect& plan, const ScalarSnapshotProvider& provider,
                               const ScalarQueryLimits limits) {
  ScalarQueryMetrics metrics;
  SqlResult<ScalarQueryResult> result =
      Executor{plan, provider, limits, std::addressof(metrics), true}.run();
  if (!result.has_value())
    return std::unexpected(result.error());
  return ScalarQueryExecution{.result = std::move(*result), .metrics = metrics};
}

} // namespace chronos::query
