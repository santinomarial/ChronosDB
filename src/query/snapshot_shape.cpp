#include "chronos/query/snapshot_shape.hpp"

#include "chronos/common/status.hpp"

#include <array>
#include <cstddef>
#include <string>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

} // namespace

common::Result<RowVersionScanMode>
validate_snapshot_pipeline_input_shape(const std::span<const PhysicalColumnShape> input,
                                       const schema::TableSchema& destination_schema) {
  const std::span<const schema::ColumnDefinition> columns = destination_schema.columns();
  auto appended_count = scan_output_column_count(columns.size(), RowVersionScanMode::kAppend);
  if (!appended_count.has_value())
    return common::make_unexpected(appended_count.error());
  if (input.size() != columns.size() && input.size() != *appended_count) {
    return common::make_unexpected(invalid(
        "snapshot pipeline input must be the destination schema with an optional row-version "
        "suffix"));
  }
  for (std::size_t ordinal = 0U; ordinal < columns.size(); ++ordinal) {
    const PhysicalColumnShape expected{.type = columns[ordinal].type(),
                                       .nullable = columns[ordinal].nullable()};
    if (input[ordinal] != expected) {
      return common::make_unexpected(
          invalid("snapshot pipeline user-column shape disagrees with the destination schema"));
    }
  }
  if (input.size() == columns.size())
    return RowVersionScanMode::kOmit;

  constexpr std::array<VectorRowVersionColumnKind, kVectorRowVersionColumnCount> kSuffixKinds{
      VectorRowVersionColumnKind::kWalId, VectorRowVersionColumnKind::kRecordSequence,
      VectorRowVersionColumnKind::kRowOrdinal, VectorRowVersionColumnKind::kOperation};
  for (std::size_t suffix = 0U; suffix < kSuffixKinds.size(); ++suffix) {
    auto expected_type = vector_row_version_column_type(kSuffixKinds[suffix]);
    if (!expected_type.has_value())
      return common::make_unexpected(expected_type.error());
    const PhysicalColumnShape& actual = input[columns.size() + suffix];
    if (actual.nullable || actual.type != *expected_type) {
      return common::make_unexpected(
          invalid("snapshot pipeline row-version suffix has an invalid physical shape"));
    }
  }
  return RowVersionScanMode::kAppend;
}

} // namespace chronos::query
