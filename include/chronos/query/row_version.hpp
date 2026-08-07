#ifndef CHRONOS_QUERY_ROW_VERSION_HPP_
#define CHRONOS_QUERY_ROW_VERSION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <cstdint>

namespace chronos::query {

inline constexpr std::size_t kVectorRowVersionColumnCount = 4U;

// User columns always precede this fixed non-null suffix. The order matches CSEG v1's frozen
// system-column order and is also materialized from mutable-head row metadata.
enum class VectorRowVersionColumnKind : std::uint8_t {
  kWalId,
  kRecordSequence,
  kRowOrdinal,
  kOperation,
};

enum class RowVersionScanMode : std::uint8_t { kOmit, kAppend };

class VectorRowVersionLayout {
public:
  VectorRowVersionLayout() = delete;

  [[nodiscard]] constexpr std::size_t first_column_ordinal() const noexcept {
    return first_column_ordinal_;
  }
  [[nodiscard]] constexpr std::size_t wal_id_column_ordinal() const noexcept {
    return first_column_ordinal_;
  }
  [[nodiscard]] constexpr std::size_t record_sequence_column_ordinal() const noexcept {
    return first_column_ordinal_ + 1U;
  }
  [[nodiscard]] constexpr std::size_t row_ordinal_column_ordinal() const noexcept {
    return first_column_ordinal_ + 2U;
  }
  [[nodiscard]] constexpr std::size_t operation_column_ordinal() const noexcept {
    return first_column_ordinal_ + 3U;
  }
  [[nodiscard]] constexpr std::size_t total_column_count() const noexcept {
    return first_column_ordinal_ + kVectorRowVersionColumnCount;
  }

private:
  explicit constexpr VectorRowVersionLayout(const std::size_t first_column_ordinal) noexcept
      : first_column_ordinal_(first_column_ordinal) {}

  std::size_t first_column_ordinal_;

  friend common::Result<VectorRowVersionLayout>
  vector_row_version_layout(std::size_t user_column_count);
};

// Checked layout for one output whose caller-visible user columns precede the row-version suffix.
[[nodiscard]] common::Result<VectorRowVersionLayout>
vector_row_version_layout(std::size_t user_column_count);

// Returns user_column_count in omit mode and the checked suffix total in append mode. Invalid enum
// values are rejected instead of silently selecting a representation.
[[nodiscard]] common::Result<std::size_t> scan_output_column_count(std::size_t user_column_count,
                                                                   RowVersionScanMode mode);

[[nodiscard]] common::Result<schema::LogicalType>
vector_row_version_column_type(VectorRowVersionColumnKind kind);

} // namespace chronos::query

#endif // CHRONOS_QUERY_ROW_VERSION_HPP_
