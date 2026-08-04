#ifndef CHRONOS_COLUMNAR_COLUMNAR_BATCH_HPP_
#define CHRONOS_COLUMNAR_COLUMNAR_BATCH_HPP_

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/result.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <vector>

namespace chronos::columnar {

inline constexpr std::size_t kMaximumV1BatchLength = 16'776'992;

// Limits are checked before an OwnedColumnarBatch retains the supplied vectors. Logical bytes count
// exact buffer sizes; retained bytes count vector capacities so spare allocations cannot evade the
// bound. C++ object/allocator overhead and future serialized metadata remain separate domains.
struct ColumnarBatchLimits {
  std::uint32_t max_rows{std::numeric_limits<std::uint32_t>::max()};
  std::size_t max_columns{schema::kMaximumSchemaColumnCount};
  std::size_t max_buffer_bytes{kMaximumV1BatchLength};
  std::size_t max_retained_buffer_bytes{kMaximumV1BatchLength};
};

struct BatchCellPosition {
  std::size_t column_ordinal;
  std::uint32_t row;
};

// An immutable schema-shaped owner. The pinned schema and all vectors remain stable for the batch
// lifetime, so views and cells may be used concurrently when the batch itself remains alive.
class OwnedColumnarBatch {
public:
  OwnedColumnarBatch() = delete;
  OwnedColumnarBatch(const OwnedColumnarBatch&) = delete;
  OwnedColumnarBatch& operator=(const OwnedColumnarBatch&) = delete;
  OwnedColumnarBatch(OwnedColumnarBatch&&) noexcept = default;
  OwnedColumnarBatch& operator=(OwnedColumnarBatch&&) noexcept = default;

  [[nodiscard]] static common::Result<OwnedColumnarBatch>
  create(std::shared_ptr<const schema::TableSchema> schema, std::vector<OwnedColumnVector> columns,
         ColumnarBatchLimits limits = {});

  [[nodiscard]] const std::shared_ptr<const schema::TableSchema>& schema_ptr() const noexcept;
  [[nodiscard]] const schema::TableSchema& schema() const noexcept;
  [[nodiscard]] std::uint32_t row_count() const noexcept;
  [[nodiscard]] std::span<const OwnedColumnVector> columns() const noexcept;
  [[nodiscard]] const OwnedColumnVector* column(std::size_t ordinal) const noexcept;
  [[nodiscard]] common::Result<ColumnCellView> cell(BatchCellPosition position) const;
  [[nodiscard]] std::size_t buffer_bytes() const noexcept;
  [[nodiscard]] std::size_t retained_buffer_bytes() const noexcept;

private:
  struct Accounting {
    std::uint32_t row_count;
    std::size_t buffer_bytes;
    std::size_t retained_buffer_bytes;
  };

  OwnedColumnarBatch(std::shared_ptr<const schema::TableSchema> schema,
                     std::vector<OwnedColumnVector> columns, Accounting accounting) noexcept;

  std::shared_ptr<const schema::TableSchema> schema_;
  std::vector<OwnedColumnVector> columns_;
  std::uint32_t row_count_;
  std::size_t buffer_bytes_;
  std::size_t retained_buffer_bytes_;
};

} // namespace chronos::columnar

#endif // CHRONOS_COLUMNAR_COLUMNAR_BATCH_HPP_
