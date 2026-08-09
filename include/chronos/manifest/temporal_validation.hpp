#ifndef CHRONOS_MANIFEST_TEMPORAL_VALIDATION_HPP_
#define CHRONOS_MANIFEST_TEMPORAL_VALIDATION_HPP_

#include "chronos/common/status.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/validation.hpp"

#include <span>

namespace chronos::manifest {

struct TemporalTabletSourceBinding {
  schema::TabletId tablet_id;
  ManifestCommitSource commit_source{ManifestCommitSource::kWal};
  common::Uuid source_id;
  friend bool operator==(const TemporalTabletSourceBinding&,
                         const TemporalTabletSourceBinding&) = default;
};

// Requires exactly one retained schema lineage per temporal tablet in canonical tablet order.
// Every part schema must be an ancestor of the tablet's recovery schema.
[[nodiscard]] common::Status
validate_manifest_v2_temporal_schema_binding(const DecodedTemporalManifestView& manifest,
                                             std::span<const TabletSchemaBinding> bindings);

// Binds every tablet to the caller's exact configured WAL or Raft owner. Recovery cannot infer or
// silently create source ownership from untrusted durable bytes.
[[nodiscard]] common::Status
validate_manifest_v2_temporal_source_binding(const DecodedTemporalManifestView& manifest,
                                             std::span<const TemporalTabletSourceBinding> bindings);

// Validates an add-only Manifest v2 successor. Source identities are immutable, tablet application
// and reclaim boundaries are monotonic within their source namespaces, schema recovery moves only
// forward, and every protected part and retry descriptor is retained exactly. Authorized temporal
// compaction/retention replacement is intentionally a separate future proof boundary.
[[nodiscard]] common::Status
validate_manifest_v2_temporal_transition(const DecodedTemporalManifestView& predecessor,
                                         const DecodedTemporalManifestView& next,
                                         std::span<const TabletSchemaBinding> bindings);

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_TEMPORAL_VALIDATION_HPP_
