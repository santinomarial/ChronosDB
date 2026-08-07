#include "chronos/query/head_scan.hpp"

#include <type_traits>

static_assert(
    std::is_base_of_v<chronos::query::PhysicalOperator, chronos::query::HeadScanOperator>);
static_assert(std::is_aggregate_v<chronos::query::HeadScanLimits>);
using ExactHeadScanFactory =
    chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
        const chronos::query::QueryResourceContext&, chronos::head::HeadSnapshot,
        const chronos::schema::SchemaLineage&, chronos::schema::SchemaId,
        const chronos::schema::TabletId&, std::vector<std::uint32_t>,
        chronos::query::TimestampRangePredicate, chronos::query::HeadScanLimits);
static_assert(
    std::is_same_v<decltype(&chronos::query::HeadScanOperator::create_event_time_filtered),
                   ExactHeadScanFactory>);
