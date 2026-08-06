#include "chronos/query/explain.hpp"

#include "chronos/common/status.hpp"
#include "chronos/query/literal.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <cstdint>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>

namespace chronos::query {
namespace {

[[nodiscard]] SqlDiagnostic diagnostic(const BoundSqlSelect& plan, const common::StatusCode status,
                                       const std::string_view message) {
  return SqlDiagnostic{SqlDiagnosticCode::kExecutionFailure, plan.syntax().span(),
                       common::Status{status, std::string{message}}};
}

void append_hex(std::string& output, const std::span<const std::byte> bytes) {
  constexpr std::string_view kDigits = "0123456789abcdef";
  for (const std::byte byte : bytes) {
    const std::uint8_t value = std::to_integer<std::uint8_t>(byte);
    output.push_back(kDigits[value >> 4U]);
    output.push_back(kDigits[value & 0x0fU]);
  }
}

void append_hex(std::string& output, const std::string_view text) {
  append_hex(output, std::as_bytes(std::span{text}));
}

void append_type(std::string& output, const schema::LogicalType type) {
  output.append(schema::logical_type_kind_name(type.kind()));
  if (type.is_decimal()) {
    output.push_back('(');
    output.append(std::to_string(type.parameter_0()));
    output.push_back(',');
    output.append(std::to_string(type.parameter_1()));
    output.push_back(')');
  }
}

[[nodiscard]] std::string_view mode_name(const SqlSelectMode mode) noexcept {
  switch (mode) {
  case SqlSelectMode::kSelect:
    return "select";
  case SqlSelectMode::kExplain:
    return "explain";
  case SqlSelectMode::kExplainAnalyze:
    return "explain_analyze";
  case SqlSelectMode::kSubscribe:
    return "subscribe";
  }
  return "unknown";
}

void append_operators(std::string& output, const BoundSqlSelect& plan) {
  output.append("physical.operators=scan");
  if (plan.latest_by().has_value())
    output.append(",latest");
  for ([[maybe_unused]] const BoundAsofJoin& join : plan.asof_joins())
    output.append(",asof_join");
  if (plan.syntax().where() != nullptr)
    output.append(",filter");
  if (plan.aggregate_query())
    output.append(",aggregate");
  output.append(",project");
  if (!plan.syntax().order_by().empty())
    output.append(",sort");
  if (plan.syntax().limit().has_value())
    output.append(",limit");
  output.push_back('\n');
}

} // namespace

SqlResult<std::string> explain_sql_v1_select(const BoundSqlSelect& plan) {
  if (plan.syntax().mode() == SqlSelectMode::kSubscribe) {
    return std::unexpected(diagnostic(plan, common::StatusCode::kNotSupported,
                                      "SUBSCRIBE plans are not executable in Phase 8"));
  }
  try {
    std::string output;
    output.reserve(512U + plan.sources().size() * 128U + plan.outputs().size() * 96U);
    output.append("chronos_sql_v1_plan=1\nmode=");
    output.append(mode_name(plan.syntax().mode()));
    output.append("\ncatalog_generation=");
    output.append(std::to_string(plan.catalog()->generation()));
    output.append("\nlogical.sources=");
    output.append(std::to_string(plan.sources().size()));
    output.push_back('\n');
    for (std::size_t index = 0U; index < plan.sources().size(); ++index) {
      const BoundSqlSource& source = plan.sources()[index];
      output.append("logical.source.");
      output.append(std::to_string(index));
      output.append("=name_hex:");
      append_hex(output, source.exposed_name());
      output.append(",quoted:");
      output.push_back(source.exposed_name_quoted() ? '1' : '0');
      output.append(",table:");
      append_hex(output, source.schema_ptr()->table_id().bytes());
      output.append(",schema:");
      append_hex(output, source.schema_ptr()->schema_id().bytes());
      output.append(",version:");
      output.append(std::to_string(source.schema_ptr()->version().value()));
      output.push_back('\n');
    }
    output.append("logical.system_time_ns=");
    if (plan.syntax().system_time().has_value()) {
      const common::Result<std::int64_t> timestamp =
          parse_sql_timestamp_ns_literal(*plan.syntax().system_time());
      if (!timestamp.has_value()) {
        return std::unexpected(diagnostic(plan, common::StatusCode::kInternal,
                                          "Bound system-time literal is invalid"));
      }
      output.append(std::to_string(*timestamp));
    } else {
      output.append("current");
    }
    output.append("\nlogical.latest_keys=");
    output.append(std::to_string(
        plan.latest_by().has_value() ? plan.latest_by()->key_column_ordinals.size() : 0U));
    output.append("\nlogical.asof_joins=");
    output.append(std::to_string(plan.asof_joins().size()));
    output.append("\nlogical.filter=");
    output.push_back(plan.syntax().where() == nullptr ? '0' : '1');
    output.append("\nlogical.aggregate=");
    output.push_back(plan.aggregate_query() ? '1' : '0');
    output.append("\nlogical.group_keys=");
    output.append(std::to_string(plan.syntax().group_by().size()));
    output.append("\nlogical.outputs=");
    output.append(std::to_string(plan.outputs().size()));
    output.push_back('\n');
    for (std::size_t index = 0U; index < plan.outputs().size(); ++index) {
      const BoundOutputColumn& column = plan.outputs()[index];
      output.append("logical.output.");
      output.append(std::to_string(index));
      output.append("=name_hex:");
      append_hex(output, column.name);
      output.append(",quoted:");
      output.push_back(column.name_quoted ? '1' : '0');
      output.append(",type:");
      append_type(output, column.type);
      output.append(",nullable:");
      output.push_back(column.nullable ? '1' : '0');
      output.append(",aggregate:");
      output.push_back(column.contains_aggregate ? '1' : '0');
      output.push_back('\n');
    }
    output.append("logical.order_keys=");
    output.append(std::to_string(plan.syntax().order_by().size()));
    output.append("\nlogical.limit=");
    if (plan.syntax().limit().has_value())
      output.append(std::to_string(*plan.syntax().limit()));
    else
      output.append("none");
    output.append("\nphysical.engine=scalar_reference\n");
    append_operators(output, plan);
    return output;
  } catch (const std::bad_alloc&) {
    return std::unexpected(
        diagnostic(plan, common::StatusCode::kResourceExhausted, "SQL EXPLAIN allocation failed"));
  } catch (const std::length_error&) {
    return std::unexpected(diagnostic(plan, common::StatusCode::kResourceExhausted,
                                      "SQL EXPLAIN output exceeds container limits"));
  }
}

SqlResult<ScalarExplainAnalyzeResult>
execute_sql_v1_explain_analyze(const BoundSqlSelect& plan, const ScalarSnapshotProvider& provider,
                               const ScalarQueryLimits limits) {
  if (plan.syntax().mode() != SqlSelectMode::kExplainAnalyze) {
    return std::unexpected(diagnostic(plan, common::StatusCode::kInvalidArgument,
                                      "EXPLAIN ANALYZE execution requires that statement mode"));
  }
  SqlResult<std::string> explained = explain_sql_v1_select(plan);
  if (!explained.has_value())
    return std::unexpected(explained.error());
  SqlResult<ScalarQueryExecution> execution =
      execute_sql_v1_select_measured(plan, provider, limits);
  if (!execution.has_value())
    return std::unexpected(execution.error());
  return ScalarExplainAnalyzeResult{.plan = std::move(*explained),
                                    .execution = std::move(*execution)};
}

} // namespace chronos::query
