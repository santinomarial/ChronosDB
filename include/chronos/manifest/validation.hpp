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

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_VALIDATION_HPP_
