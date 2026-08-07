#include "chronos/query/latest.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"
#include "chronos/query/row_version.hpp"
#include "chronos/query/value.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] common::Status exhausted(const std::string_view message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::string{message}};
}

[[nodiscard]] common::Result<void> validate_definition(const VectorLatestByDefinition& definition,
                                                       const LatestByLimits& limits) {
  if (limits.maximum_group_keys == 0U || limits.maximum_physical_ordering_keys == 0U)
    return common::make_unexpected(invalid("LATEST BY limits must be nonzero"));
  if (definition.key_column_ordinals.empty())
    return common::make_unexpected(invalid("LATEST BY requires a grouping key"));
  if (definition.physical_ordering_key_ordinals.empty())
    return common::make_unexpected(invalid("LATEST BY requires a physical ordering key"));
  if (definition.key_column_ordinals.size() > limits.maximum_group_keys ||
      definition.key_column_ordinals.capacity() > limits.maximum_group_keys ||
      definition.physical_ordering_key_ordinals.size() > limits.maximum_physical_ordering_keys ||
      definition.physical_ordering_key_ordinals.capacity() >
          limits.maximum_physical_ordering_keys) {
    return common::make_unexpected(exhausted("LATEST BY key configuration exceeds its limit"));
  }
  common::Result<VectorRowVersionLayout> suffix =
      vector_row_version_layout(definition.row_version_first_column_ordinal);
  if (!suffix.has_value())
    return common::make_unexpected(suffix.error());
  const std::optional<std::size_t> first = common::checked_add(
      definition.key_column_ordinals.size(), definition.physical_ordering_key_ordinals.size());
  if (!first.has_value()) {
    return common::make_unexpected(exhausted("LATEST BY sort key count overflowed"));
  }
  const std::optional<std::size_t> total = common::checked_add(*first, std::size_t{4U});
  if (!total.has_value())
    return common::make_unexpected(exhausted("LATEST BY sort key count overflowed"));
  if (*total > limits.sort_limits.maximum_keys)
    return common::make_unexpected(exhausted("LATEST BY sort key count exceeds sort limit"));
  return {};
}

// The two row ordinals are the conventional operands of one comparison.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
[[nodiscard]] common::Result<bool> same_group(const VectorChunk& chunk,
                                              const std::span<const std::size_t> keys,
                                              const std::size_t left, const std::size_t right) {
  for (const std::size_t key : keys) {
    const columnar::PhysicalColumnView* column = chunk.column(key);
    if (column == nullptr)
      return common::make_unexpected(invalid("LATEST BY key column is absent"));
    common::Result<columnar::ColumnCellView> left_cell = chunk.cell({key, left});
    if (!left_cell.has_value())
      return common::make_unexpected(left_cell.error());
    common::Result<columnar::ColumnCellView> right_cell = chunk.cell({key, right});
    if (!right_cell.has_value())
      return common::make_unexpected(right_cell.error());
    common::Result<int> comparison =
        compare_physical_cells(column->type(), *left_cell, *right_cell, ScalarNullPlacement::kLast);
    if (!comparison.has_value())
      return common::make_unexpected(comparison.error());
    if (*comparison != 0)
      return false;
  }
  return true;
}
// NOLINTEND(bugprone-easily-swappable-parameters)

} // namespace

common::Result<void> LatestByOperator::compact_groups(AccountedVectorChunk& chunk,
                                                      const std::span<const std::size_t> keys,
                                                      const QueryResourceContext& resources) {
  VectorChunk& vector_chunk = chunk.chunk_;
  std::vector<std::uint32_t>& indices = vector_chunk.selection_.indices_;
  if (indices.empty())
    return {};
  std::size_t output = 1U;
  for (std::size_t row = 1U; row < indices.size(); ++row) {
    if ((row & 255U) == 0U) {
      common::Result<void> active = resources.check_cancelled();
      if (!active.has_value())
        return common::make_unexpected(active.error());
    }
    common::Result<bool> equal = same_group(vector_chunk, keys, output - 1U, row);
    if (!equal.has_value())
      return common::make_unexpected(equal.error());
    if (!*equal) {
      indices[output] = indices[row];
      ++output;
    }
  }
  const std::size_t old_bytes = vector_chunk.selection_.buffer_bytes();
  indices.resize(output);
  vector_chunk.selection_.identity_ = output == vector_chunk.selection_.physical_row_count_;
  vector_chunk.buffer_bytes_ -= old_bytes;
  vector_chunk.buffer_bytes_ += vector_chunk.selection_.buffer_bytes();
  return {};
}

LatestByOperator::LatestByOperator(std::unique_ptr<PhysicalOperator> sorted,
                                   VectorLatestByDefinition definition) noexcept
    : sorted_(std::move(sorted)), definition_(std::move(definition)) {}

common::Result<std::unique_ptr<PhysicalOperator>>
LatestByOperator::create(std::unique_ptr<PhysicalOperator> input,
                         VectorLatestByDefinition definition, const LatestByLimits limits) {
  if (input == nullptr)
    return common::make_unexpected(invalid("LATEST BY input must be non-null"));
  common::Result<void> valid = validate_definition(definition, limits);
  if (!valid.has_value())
    return common::make_unexpected(valid.error());
  try {
    std::vector<VectorSortKey> keys;
    keys.reserve(definition.key_column_ordinals.size() +
                 definition.physical_ordering_key_ordinals.size() + 4U);
    for (const std::size_t ordinal : definition.key_column_ordinals) {
      keys.push_back({.column_ordinal = ordinal,
                      .direction = PhysicalSortDirection::kAscending,
                      .null_placement = ScalarNullPlacement::kLast});
    }
    keys.push_back({.column_ordinal = definition.timestamp_column_ordinal,
                    .direction = PhysicalSortDirection::kDescending,
                    .null_placement = ScalarNullPlacement::kLast});
    for (const std::size_t ordinal : definition.physical_ordering_key_ordinals) {
      keys.push_back({.column_ordinal = ordinal,
                      .direction = PhysicalSortDirection::kDescending,
                      .null_placement = ScalarNullPlacement::kFirst});
    }
    const VectorRowVersionLayout suffix =
        vector_row_version_layout(definition.row_version_first_column_ordinal).value();
    for (const std::size_t ordinal :
         {suffix.wal_id_column_ordinal(), suffix.record_sequence_column_ordinal(),
          suffix.row_ordinal_column_ordinal()}) {
      keys.push_back({.column_ordinal = ordinal,
                      .direction = PhysicalSortDirection::kDescending,
                      .null_placement = ScalarNullPlacement::kLast});
    }
    common::Result<std::unique_ptr<PhysicalOperator>> sorted =
        SortOperator::create(std::move(input), std::move(keys), limits.sort_limits);
    if (!sorted.has_value())
      return common::make_unexpected(sorted.error());
    return std::unique_ptr<PhysicalOperator>{
        new LatestByOperator{std::move(*sorted), std::move(definition)}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("LATEST BY operator allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("LATEST BY configuration exceeds container limits"));
  }
}

common::Result<PhysicalOperatorStep> LatestByOperator::next(const QueryResourceContext& resources) {
  if (ended_)
    return PhysicalOperatorStep::end();
  common::Result<void> active = resources.check_cancelled();
  if (!active.has_value())
    return common::make_unexpected(active.error());
  common::Result<PhysicalOperatorStep> step = sorted_->next(resources);
  if (!step.has_value()) {
    static_cast<void>(resources.request_cancel());
    sorted_.reset();
    ended_ = true;
    return common::make_unexpected(step.error());
  }
  if (step->kind() == PhysicalOperatorStepKind::kEnd) {
    sorted_.reset();
    ended_ = true;
    return PhysicalOperatorStep::end();
  }
  common::Result<AccountedVectorChunk> chunk = std::move(*step).take_chunk();
  if (!chunk.has_value()) {
    static_cast<void>(resources.request_cancel());
    sorted_.reset();
    ended_ = true;
    return common::make_unexpected(chunk.error());
  }
  VectorChunk& vector_chunk = chunk->chunk_;
  if (definition_.timestamp_column_ordinal >= vector_chunk.column_count()) {
    static_cast<void>(resources.request_cancel());
    sorted_.reset();
    ended_ = true;
    return common::make_unexpected(invalid("LATEST BY timestamp column is absent"));
  }
  const columnar::PhysicalColumnView* timestamp =
      vector_chunk.column(definition_.timestamp_column_ordinal);
  if (timestamp == nullptr || timestamp->type().kind() != schema::LogicalTypeKind::kTimestampNs) {
    static_cast<void>(resources.request_cancel());
    sorted_.reset();
    ended_ = true;
    return common::make_unexpected(invalid("LATEST BY timestamp must have TIMESTAMP_NS type"));
  }
  const VectorRowVersionLayout suffix =
      vector_row_version_layout(definition_.row_version_first_column_ordinal).value();
  const std::array<std::pair<std::size_t, VectorRowVersionColumnKind>, 4U> suffix_columns{{
      {suffix.wal_id_column_ordinal(), VectorRowVersionColumnKind::kWalId},
      {suffix.record_sequence_column_ordinal(), VectorRowVersionColumnKind::kRecordSequence},
      {suffix.row_ordinal_column_ordinal(), VectorRowVersionColumnKind::kRowOrdinal},
      {suffix.operation_column_ordinal(), VectorRowVersionColumnKind::kOperation},
  }};
  for (const auto& [ordinal, kind] : suffix_columns) {
    const columnar::PhysicalColumnView* actual = vector_chunk.column(ordinal);
    common::Result<schema::LogicalType> expected = vector_row_version_column_type(kind);
    if (actual == nullptr || !expected.has_value() || actual->type() != *expected ||
        actual->nullable()) {
      static_cast<void>(resources.request_cancel());
      sorted_.reset();
      ended_ = true;
      return common::make_unexpected(invalid("LATEST BY row-version suffix is invalid"));
    }
  }
  common::Result<void> compacted =
      compact_groups(*chunk, definition_.key_column_ordinals, resources);
  if (!compacted.has_value()) {
    static_cast<void>(resources.request_cancel());
    sorted_.reset();
    ended_ = true;
    return common::make_unexpected(compacted.error());
  }
  return PhysicalOperatorStep::chunk(std::move(*chunk));
}

} // namespace chronos::query
