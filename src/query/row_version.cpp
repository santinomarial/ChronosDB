#include "chronos/query/row_version.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/status.hpp"

#include <cstddef>
#include <string>
#include <string_view>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] common::Status exhausted(const std::string_view message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::string{message}};
}

} // namespace

common::Result<VectorRowVersionLayout>
vector_row_version_layout(const std::size_t user_column_count) {
  if (!common::checked_add(user_column_count, kVectorRowVersionColumnCount).has_value()) {
    return common::make_unexpected(exhausted("vector row-version column count overflowed"));
  }
  return VectorRowVersionLayout{user_column_count};
}

common::Result<std::size_t> scan_output_column_count(const std::size_t user_column_count,
                                                     const RowVersionScanMode mode) {
  switch (mode) {
  case RowVersionScanMode::kOmit:
    return user_column_count;
  case RowVersionScanMode::kAppend: {
    common::Result<VectorRowVersionLayout> layout = vector_row_version_layout(user_column_count);
    if (!layout.has_value())
      return common::make_unexpected(layout.error());
    return layout->total_column_count();
  }
  }
  return common::make_unexpected(invalid("row-version scan mode is invalid"));
}

common::Result<schema::LogicalType>
vector_row_version_column_type(const VectorRowVersionColumnKind kind) {
  using schema::LogicalTypeKind;
  switch (kind) {
  case VectorRowVersionColumnKind::kWalId:
    return schema::LogicalType::create(LogicalTypeKind::kUuid);
  case VectorRowVersionColumnKind::kRecordSequence:
    return schema::LogicalType::create(LogicalTypeKind::kUInt64);
  case VectorRowVersionColumnKind::kRowOrdinal:
    return schema::LogicalType::create(LogicalTypeKind::kUInt32);
  case VectorRowVersionColumnKind::kOperation:
    return schema::LogicalType::create(LogicalTypeKind::kUInt8);
  }
  return common::make_unexpected(invalid("vector row-version column kind is invalid"));
}

} // namespace chronos::query
