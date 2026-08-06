#include "chronos/query/snapshot.hpp"

#include "chronos/common/status.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

template <typename Value>
[[nodiscard]] const Value* optional_pointer(const std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(value.value()) : nullptr;
}

[[nodiscard]] common::Status validate_row(const schema::TableSchema& schema,
                                          const std::uint64_t committed_position,
                                          const ScalarInputRow& row) {
  if (row.columns.size() != schema.columns().size())
    return invalid("scalar snapshot row column count does not match its schema");
  for (std::size_t ordinal = 0U; ordinal < row.columns.size(); ++ordinal) {
    const ScalarValue& value = row.columns[ordinal];
    const schema::LogicalType* value_type = optional_pointer(value.type());
    if (value_type == nullptr || *value_type != schema.columns()[ordinal].type())
      return invalid("scalar snapshot row column type does not match its schema");
    if (value.is_null() && !schema.columns()[ordinal].nullable())
      return invalid("scalar snapshot row contains NULL in a NOT NULL column");
  }
  if (schema.deduplication_key().empty() != !row.generated_logical_identity.empty()) {
    return invalid(
        "scalar snapshot generated logical identity does not match schema identity mode");
  }
  if (row.wal_id.is_nil())
    return invalid("scalar snapshot row WAL identity is nil");
  if (row.record_sequence == 0U || row.system_commit_position == 0U ||
      row.system_commit_position > committed_position) {
    return invalid("scalar snapshot row commit metadata is outside the snapshot boundary");
  }
  return common::Status::ok();
}

} // namespace

ScalarTableSnapshot::ScalarTableSnapshot(std::shared_ptr<const schema::TableSchema> schema,
                                         const std::uint64_t committed_position,
                                         std::vector<ScalarInputRow> rows) noexcept
    : schema_(std::move(schema)), committed_position_(committed_position), rows_(std::move(rows)) {}

common::Result<ScalarTableSnapshot>
ScalarTableSnapshot::create(std::shared_ptr<const schema::TableSchema> schema,
                            const std::uint64_t committed_position,
                            std::vector<ScalarInputRow> rows) {
  if (schema == nullptr || committed_position == 0U)
    return common::make_unexpected(invalid("scalar snapshot requires schema and commit boundary"));
  for (const ScalarInputRow& row : rows) {
    const common::Status status = validate_row(*schema, committed_position, row);
    if (!status.is_ok())
      return common::make_unexpected(status);
  }
  return ScalarTableSnapshot{std::move(schema), committed_position, std::move(rows)};
}

const std::shared_ptr<const schema::TableSchema>& ScalarTableSnapshot::schema_ptr() const noexcept {
  return schema_;
}

std::uint64_t ScalarTableSnapshot::committed_position() const noexcept {
  return committed_position_;
}

std::span<const ScalarInputRow> ScalarTableSnapshot::rows() const noexcept {
  return rows_;
}

} // namespace chronos::query
