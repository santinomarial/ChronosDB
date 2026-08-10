#include "chronos/query/scalar_snapshot_scan.hpp"

namespace {

[[maybe_unused]] constexpr auto kDefaultRows =
    chronos::query::ScalarSnapshotScanLimits{}.maximum_rows_per_chunk;

} // namespace
