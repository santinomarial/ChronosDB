#ifndef CHRONOS_QUERY_VECTOR_CHUNK_HPP_
#define CHRONOS_QUERY_VECTOR_CHUNK_HPP_

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/result.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace chronos::query {

inline constexpr std::uint32_t kDefaultVectorChunkRowLimit = 2'048U;
inline constexpr std::size_t kDefaultVectorChunkColumnLimit = 4'096U;
inline constexpr std::size_t kDefaultVectorChunkMemoryLimit = std::size_t{32U} * 1024U * 1024U;

// One order-preserving selection over a nonempty physical row domain. Selected row ordinals are
// unique and strictly increasing. An empty selection is valid after a predicate removes every row.
class VectorSelection {
public:
  VectorSelection() = delete;
  VectorSelection(const VectorSelection&) = delete;
  VectorSelection& operator=(const VectorSelection&) = delete;
  VectorSelection(VectorSelection&&) noexcept = default;
  VectorSelection& operator=(VectorSelection&&) noexcept = default;

  [[nodiscard]] static common::Result<VectorSelection> all(std::uint32_t physical_row_count);
  [[nodiscard]] static common::Result<VectorSelection>
  from_indices(std::uint32_t physical_row_count, std::vector<std::uint32_t> indices);

  [[nodiscard]] std::uint32_t physical_row_count() const noexcept;
  [[nodiscard]] std::span<const std::uint32_t> indices() const noexcept;
  [[nodiscard]] std::size_t selected_row_count() const noexcept;
  [[nodiscard]] bool is_identity() const noexcept;
  [[nodiscard]] std::size_t buffer_bytes() const noexcept;
  [[nodiscard]] std::size_t retained_buffer_bytes() const noexcept;
  [[nodiscard]] common::Result<std::uint32_t> physical_row(std::size_t selected_row) const;

  // Consumes and compacts this selection to rows whose Boolean predicate is TRUE. FALSE and NULL
  // are removed according to SQL WHERE semantics. The existing index allocation is reused.
  [[nodiscard]] static common::Result<VectorSelection>
  where_true(VectorSelection selection, const columnar::OwnedPhysicalColumn& predicate);
  // Stable truncation used by LIMIT. Retained capacity is unchanged and an oversized maximum is a
  // no-op.
  [[nodiscard]] static VectorSelection take_first(VectorSelection selection,
                                                  std::size_t maximum_selected_rows);

private:
  VectorSelection(std::uint32_t physical_row_count, std::vector<std::uint32_t> indices,
                  bool identity) noexcept;

  std::uint32_t physical_row_count_;
  std::vector<std::uint32_t> indices_;
  bool identity_;
};

// All limits are finite and checked before a VectorChunk retains its columns and selection.
// Retained bytes include vector capacities, preventing spare allocation from evading admission.
struct VectorChunkLimits {
  std::uint32_t maximum_rows{kDefaultVectorChunkRowLimit};
  std::size_t maximum_columns{kDefaultVectorChunkColumnLimit};
  std::size_t maximum_buffer_bytes{kDefaultVectorChunkMemoryLimit};
  std::size_t maximum_retained_buffer_bytes{kDefaultVectorChunkMemoryLimit};
};

struct SelectedVectorCell {
  std::size_t column_ordinal;
  std::size_t selected_row;
};

// A move-only immutable physical chunk. Columns use canonical physical buffers without durable
// column identities, so computed expressions never fabricate catalog identity. The explicit
// selection preserves input order and maps logical selected rows to physical vector rows.
class VectorChunk {
public:
  VectorChunk() = delete;
  VectorChunk(const VectorChunk&) = delete;
  VectorChunk& operator=(const VectorChunk&) = delete;
  VectorChunk(VectorChunk&&) noexcept = default;
  VectorChunk& operator=(VectorChunk&&) noexcept = default;

  [[nodiscard]] static common::Result<VectorChunk>
  create(std::vector<columnar::OwnedPhysicalColumn> columns, VectorSelection selection,
         VectorChunkLimits limits = {});

  [[nodiscard]] std::uint32_t physical_row_count() const noexcept;
  [[nodiscard]] std::size_t selected_row_count() const noexcept;
  [[nodiscard]] std::span<const columnar::OwnedPhysicalColumn> columns() const noexcept;
  [[nodiscard]] const columnar::OwnedPhysicalColumn* column(std::size_t ordinal) const noexcept;
  [[nodiscard]] const VectorSelection& selection() const noexcept;
  [[nodiscard]] common::Result<columnar::ColumnCellView> cell(SelectedVectorCell position) const;
  [[nodiscard]] std::size_t buffer_bytes() const noexcept;
  [[nodiscard]] std::size_t retained_buffer_bytes() const noexcept;

  // Consumes a chunk and replaces its selection with the allocation-free SQL Boolean filter.
  [[nodiscard]] static common::Result<VectorChunk> where_true(VectorChunk chunk,
                                                              std::size_t predicate_column);
  // Stable-compacts an ordered unique subset without allocating. An empty subset preserves row
  // cardinality for operators such as COUNT(*); duplicate or reordered outputs require a builder.
  [[nodiscard]] static common::Result<VectorChunk>
  project_columns(VectorChunk chunk, std::span<const std::size_t> column_ordinals);
  [[nodiscard]] static VectorChunk take_first(VectorChunk chunk, std::size_t maximum_selected_rows);

private:
  struct Accounting {
    std::size_t buffer_bytes;
    std::size_t retained_buffer_bytes;
  };

  VectorChunk(std::vector<columnar::OwnedPhysicalColumn> columns, VectorSelection selection,
              Accounting accounting) noexcept;

  std::vector<columnar::OwnedPhysicalColumn> columns_;
  VectorSelection selection_;
  std::size_t buffer_bytes_;
  std::size_t retained_buffer_bytes_;
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_VECTOR_CHUNK_HPP_
