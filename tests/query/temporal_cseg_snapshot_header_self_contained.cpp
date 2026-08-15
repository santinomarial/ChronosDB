#include "chronos/query/temporal_cseg_snapshot.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::TemporalCsegResolutionLimits>);

using SchemaOwner = std::shared_ptr<const chronos::schema::TableSchema>;
using ResolveFunction =
    chronos::common::Result<std::shared_ptr<const chronos::query::ScalarTableSnapshot>> (*)(
        const SchemaOwner&, std::span<const chronos::cseg::ProjectedCsegGranule* const>,
        chronos::query::TemporalCsegSourceLineage, std::optional<std::int64_t>,
        chronos::query::TemporalCsegResolutionLimits);
using ManifestResolveFunction =
    chronos::common::Result<std::shared_ptr<const chronos::query::ScalarTableSnapshot>> (*)(
        const SchemaOwner&, const chronos::schema::SchemaLineage&,
        const chronos::manifest::TemporalTabletDescriptor&,
        std::span<const chronos::query::TemporalManifestCsegPartView>,
        chronos::query::TemporalCsegSourceLineage, std::optional<std::int64_t>,
        chronos::query::TemporalManifestCsegResolutionLimits);
using RestoreFunction =
    chronos::common::Result<std::unique_ptr<chronos::query::TemporalSnapshotProvider>> (*)(
        const SchemaOwner&, const chronos::schema::SchemaLineage&,
        const chronos::manifest::TemporalTabletDescriptor&,
        std::span<const chronos::query::TemporalManifestCsegPartView>,
        chronos::query::TemporalCsegSourceLineage, std::int64_t,
        chronos::query::TemporalStoreLimits, chronos::query::TemporalManifestCsegResolutionLimits);

static_assert(
    std::is_same_v<decltype(&chronos::query::resolve_cseg_v2_temporal_snapshot), ResolveFunction>);
static_assert(
    std::is_same_v<decltype(&chronos::query::resolve_manifest_v2_temporal_tablet_snapshot),
                   ManifestResolveFunction>);
static_assert(std::is_same_v<decltype(&chronos::query::restore_manifest_v2_temporal_tablet_history),
                             RestoreFunction>);
