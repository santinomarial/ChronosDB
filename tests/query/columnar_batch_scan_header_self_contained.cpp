#include "chronos/query/columnar_batch_scan.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::ColumnarBatchScanLimits>);
