#include "system_rows_internal.hpp"

#include "chronos/common/result.hpp"
#include "chronos/cseg/format.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace chronos::cseg::detail {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const std::string_view message) {
  return common::Status{code, std::string{message}};
}

[[nodiscard]] common::Status corruption(const std::string_view message) {
  return status(common::StatusCode::kCorruption, message);
}

template <typename Unsigned>
[[nodiscard]] common::Result<Unsigned> load_little_endian(const common::ByteView bytes) {
  if (bytes.size() != sizeof(Unsigned)) {
    return common::make_unexpected(corruption("CSEG system cell has an invalid fixed width"));
  }
  Unsigned value = 0U;
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    const Unsigned byte = static_cast<Unsigned>(std::to_integer<std::uint8_t>(bytes[index]));
    const Unsigned shifted = static_cast<Unsigned>(byte << (index * 8U));
    value = static_cast<Unsigned>(value | shifted);
  }
  return value;
}

[[nodiscard]] common::Result<common::ByteView>
cell_bytes(const columnar::PhysicalColumnView& column, const std::uint32_t row) {
  const common::Result<columnar::ColumnCellView> cell = column.cell(row);
  if (!cell.has_value() || cell->is_null()) {
    return common::make_unexpected(corruption("CSEG system cell is inaccessible or null"));
  }
  const common::Result<common::ByteView> bytes = cell->bytes();
  if (!bytes.has_value()) {
    return common::make_unexpected(corruption("CSEG system cell is not byte-valued"));
  }
  return *bytes;
}

} // namespace

common::Status validate_cseg_v1_system_rows(const CsegSystemColumns& columns,
                                            const std::uint32_t row_count) {
  if (columns.wal_id.row_count() != row_count || columns.record_sequence.row_count() != row_count ||
      columns.row_ordinal.row_count() != row_count || columns.operation.row_count() != row_count) {
    return corruption("CSEG system pages disagree on their row count");
  }
  for (std::uint32_t row = 0U; row < row_count; ++row) {
    const common::Result<common::ByteView> wal_id = cell_bytes(columns.wal_id, row);
    const common::Result<common::ByteView> sequence = cell_bytes(columns.record_sequence, row);
    const common::Result<common::ByteView> ordinal = cell_bytes(columns.row_ordinal, row);
    const common::Result<common::ByteView> operation = cell_bytes(columns.operation, row);
    if (!wal_id.has_value() || !sequence.has_value() || !ordinal.has_value() ||
        !operation.has_value()) {
      return corruption("validated CSEG system cell is inaccessible");
    }
    if (wal_id->size() != 16U ||
        std::ranges::all_of(*wal_id, [](const std::byte value) { return value == std::byte{0}; })) {
      return corruption("CSEG WAL_ID system value is zero or malformed");
    }
    const common::Result<std::uint64_t> sequence_value =
        load_little_endian<std::uint64_t>(*sequence);
    if (!sequence_value.has_value() || *sequence_value == 0U) {
      return corruption("CSEG RECORD_SEQUENCE system value is zero or malformed");
    }
    if (!load_little_endian<std::uint32_t>(*ordinal).has_value()) {
      return corruption("CSEG ROW_ORDINAL system value is malformed");
    }
    const common::Result<std::uint8_t> operation_value =
        load_little_endian<std::uint8_t>(*operation);
    if (!operation_value.has_value() || *operation_value == 0U) {
      return corruption("CSEG OPERATION system value zero is invalid");
    }
    if (*operation_value != format::kAppendRowsOperation) {
      return status(common::StatusCode::kNotSupported,
                    "CSEG OPERATION system value is unsupported");
    }
  }
  return common::Status::ok();
}

} // namespace chronos::cseg::detail
