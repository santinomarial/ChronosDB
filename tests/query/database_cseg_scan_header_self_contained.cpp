#include "chronos/query/database_cseg_scan.hpp"

#include <type_traits>

static_assert(std::is_same_v<decltype(&chronos::query::pin_snapshot_cseg_part),
                             chronos::common::Result<chronos::query::CsegPartPin> (*)(
                                 std::shared_ptr<const chronos::manifest::SnapshotPartImage>)>);
static_assert(!std::is_copy_constructible_v<chronos::query::SnapshotCsegPartScanPlan>);
static_assert(std::is_nothrow_move_constructible_v<chronos::query::SnapshotCsegPartScanPlan>);
static_assert(std::is_aggregate_v<chronos::query::SnapshotTabletScanLimits>);
static_assert(std::is_same_v<decltype(&chronos::query::plan_snapshot_cseg_part_scan),
                             chronos::common::Result<chronos::query::SnapshotCsegPartScanPlan> (*)(
                                 const chronos::manifest::DatabaseStorageSnapshot&,
                                 const chronos::schema::TabletId&,
                                 const std::optional<chronos::cseg::EventTimePredicate>&,
                                 chronos::query::SnapshotCsegPartScanPlanLimits)>);
