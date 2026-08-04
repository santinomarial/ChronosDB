#include "chronos/columnar/columnar_batch.hpp"

#include "chronos/common/checked_math.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace chronos::columnar {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

} // namespace

OwnedColumnarBatch::OwnedColumnarBatch(std::shared_ptr<const schema::TableSchema> schema,
                                       std::vector<OwnedColumnVector> columns,
                                       const Accounting accounting) noexcept
    : schema_(std::move(schema)), columns_(std::move(columns)), row_count_(accounting.row_count),
      buffer_bytes_(accounting.buffer_bytes),
      retained_buffer_bytes_(accounting.retained_buffer_bytes) {}

common::Result<OwnedColumnarBatch>
OwnedColumnarBatch::create(std::shared_ptr<const schema::TableSchema> schema,
                           std::vector<OwnedColumnVector> columns,
                           const ColumnarBatchLimits limits) {
  if (schema == nullptr) {
    return common::make_unexpected(invalid("columnar batch requires an owning schema pointer"));
  }
  if (limits.max_rows == 0U || limits.max_columns == 0U || limits.max_buffer_bytes == 0U ||
      limits.max_retained_buffer_bytes == 0U) {
    return common::make_unexpected(invalid("columnar batch limits must be nonzero"));
  }
  if (limits.max_columns > schema::kMaximumSchemaColumnCount ||
      limits.max_buffer_bytes > kMaximumV1BatchLength ||
      limits.max_retained_buffer_bytes > kMaximumV1BatchLength) {
    return common::make_unexpected(invalid("columnar batch limits exceed the accepted v1 bounds"));
  }
  if (columns.size() != schema->columns().size()) {
    return common::make_unexpected(
        invalid("columnar batch must contain every schema column exactly once"));
  }
  if (columns.size() > limits.max_columns) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "columnar batch exceeds the column limit"});
  }

  const std::uint32_t row_count = columns.front().row_count();
  if (row_count > limits.max_rows) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "columnar batch exceeds the row limit"});
  }
  std::size_t buffer_bytes = 0U;
  std::size_t retained_buffer_bytes = 0U;
  for (std::size_t ordinal = 0; ordinal < columns.size(); ++ordinal) {
    const schema::ColumnDefinition& definition = schema->columns()[ordinal];
    const OwnedColumnVector& vector = columns[ordinal];
    if (vector.column_id() != definition.id()) {
      return common::make_unexpected(
          invalid("columnar batch columns are not in exact schema ordinal order"));
    }
    if (vector.type() != definition.type() || vector.nullable() != definition.nullable()) {
      return common::make_unexpected(
          invalid("columnar batch type or nullability does not match its schema"));
    }
    if (vector.row_count() != row_count) {
      return common::make_unexpected(
          invalid("all columnar batch vectors must have the same row count"));
    }
    const auto next = common::checked_add(buffer_bytes, vector.buffer_bytes());
    if (!next.has_value()) {
      return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                    "columnar batch buffer accounting overflowed"});
    }
    buffer_bytes = *next;
    if (buffer_bytes > limits.max_buffer_bytes) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kResourceExhausted, "columnar batch exceeds the buffer-byte limit"});
    }
    const auto next_retained =
        common::checked_add(retained_buffer_bytes, vector.retained_buffer_bytes());
    if (!next_retained.has_value()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kResourceExhausted,
                         "columnar batch retained-buffer accounting overflowed"});
    }
    retained_buffer_bytes = *next_retained;
    if (retained_buffer_bytes > limits.max_retained_buffer_bytes) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kResourceExhausted,
                         "columnar batch exceeds the retained-buffer-byte limit"});
    }
  }

  return OwnedColumnarBatch{std::move(schema), std::move(columns),
                            Accounting{.row_count = row_count,
                                       .buffer_bytes = buffer_bytes,
                                       .retained_buffer_bytes = retained_buffer_bytes}};
}

const std::shared_ptr<const schema::TableSchema>& OwnedColumnarBatch::schema_ptr() const noexcept {
  return schema_;
}

const schema::TableSchema& OwnedColumnarBatch::schema() const noexcept {
  return *schema_;
}

std::uint32_t OwnedColumnarBatch::row_count() const noexcept {
  return row_count_;
}

std::span<const OwnedColumnVector> OwnedColumnarBatch::columns() const noexcept {
  return columns_;
}

const OwnedColumnVector* OwnedColumnarBatch::column(const std::size_t ordinal) const noexcept {
  if (ordinal >= columns_.size()) {
    return nullptr;
  }
  return &columns_[ordinal];
}

common::Result<ColumnCellView> OwnedColumnarBatch::cell(const BatchCellPosition position) const {
  const OwnedColumnVector* vector = column(position.column_ordinal);
  if (vector == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kOutOfRange, "batch column ordinal is out of range"});
  }
  return vector->cell(position.row);
}

std::size_t OwnedColumnarBatch::buffer_bytes() const noexcept {
  return buffer_bytes_;
}

std::size_t OwnedColumnarBatch::retained_buffer_bytes() const noexcept {
  return retained_buffer_bytes_;
}

} // namespace chronos::columnar
