#ifndef CHRONOS_CSEG_SORT_ORDER_INTERNAL_HPP_
#define CHRONOS_CSEG_SORT_ORDER_INTERNAL_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstdint>

namespace chronos::cseg::detail {

struct SortCellView {
  bool is_null{};
  bool is_boolean{};
  bool boolean{};
  common::ByteView bytes;
};

// The single physical ordering definition shared by sealed-head conversion and CSEG validation.
// NULL sorts last, floating NaNs sort after all non-NaNs, and equal NaN payloads compare equal.
[[nodiscard]] common::Result<int> compare_sort_cells(schema::LogicalTypeKind kind,
                                                     SortCellView left, SortCellView right);

} // namespace chronos::cseg::detail

#endif // CHRONOS_CSEG_SORT_ORDER_INTERNAL_HPP_
