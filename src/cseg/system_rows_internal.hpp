#ifndef CHRONOS_CSEG_SYSTEM_ROWS_INTERNAL_HPP_
#define CHRONOS_CSEG_SYSTEM_ROWS_INTERNAL_HPP_

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/status.hpp"

#include <cstdint>

namespace chronos::cseg::detail {

struct CsegSystemColumns {
  const columnar::PhysicalColumnView& wal_id;
  const columnar::PhysicalColumnView& record_sequence;
  const columnar::PhysicalColumnView& row_ordinal;
  const columnar::PhysicalColumnView& operation;
};

[[nodiscard]] common::Status validate_cseg_v1_system_rows(const CsegSystemColumns& columns,
                                                          std::uint32_t row_count);

} // namespace chronos::cseg::detail

#endif // CHRONOS_CSEG_SYSTEM_ROWS_INTERNAL_HPP_
