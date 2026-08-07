#include "chronos/query/database_cseg_scan.hpp"

#include <type_traits>

static_assert(std::is_same_v<decltype(&chronos::query::pin_snapshot_cseg_part),
                             chronos::common::Result<chronos::query::CsegPartPin> (*)(
                                 std::shared_ptr<const chronos::manifest::SnapshotPartImage>)>);
