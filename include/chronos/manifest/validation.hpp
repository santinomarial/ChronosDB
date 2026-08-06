#ifndef CHRONOS_MANIFEST_VALIDATION_HPP_
#define CHRONOS_MANIFEST_VALIDATION_HPP_

#include "chronos/common/status.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <functional>
#include <span>

namespace chronos::manifest {

struct TabletSchemaBinding {
  schema::TabletId tablet_id;
  std::reference_wrapper<const schema::SchemaLineage> lineage;
};

// Borrows the exact canonical part-identity sets authorized by one append-only compaction plan.
// Both spans are nonempty and strictly sorted. Input identities name predecessor parts removed
// from tablet_id; output identities name fresh successor parts installed only after independent
// full-row equivalence validation.
struct ManifestCompactionReplacement {
  schema::TabletId tablet_id;
  std::span<const cseg::PartId> input_part_ids;
  std::span<const cseg::PartId> output_part_ids;
};

// Requires exactly one binding per tablet in the same canonical TabletId order. Every recovery and
// part schema must bind by table, SchemaId, SchemaVersion, and ancestor relationship.
[[nodiscard]] common::Status
validate_manifest_v1_schema_binding(const DecodedManifestView& manifest,
                                    std::span<const TabletSchemaBinding> bindings);

// Validates the accepted Phase 6 add-only transition. Bindings exactly describe next.tablets();
// they must also retain every predecessor tablet lineage. WAL coverage proof and installed CSEG
// content validation require external bytes and remain separate installation checks.
[[nodiscard]] common::Status
validate_manifest_v1_transition(const DecodedManifestView& predecessor,
                                const DecodedManifestView& next,
                                std::span<const TabletSchemaBinding> bindings);

// Validates the accepted append-only Phase 7 replacement transition. This is a structural
// authorization check, not an input/output row-equivalence proof: callers must complete that proof
// and durably install every output before using the resulting generation.
[[nodiscard]] common::Status
validate_manifest_v1_compaction_transition(const DecodedManifestView& predecessor,
                                           const DecodedManifestView& next,
                                           std::span<const TabletSchemaBinding> bindings,
                                           const ManifestCompactionReplacement& replacement);

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_VALIDATION_HPP_
