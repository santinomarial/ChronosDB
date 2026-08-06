#ifndef CHRONOS_MANIFEST_GENERATION_BUILDER_HPP_
#define CHRONOS_MANIFEST_GENERATION_BUILDER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/part_validation.hpp"
#include "chronos/manifest/sealed_head_flush.hpp"
#include "chronos/manifest/types.hpp"
#include "chronos/manifest/validation.hpp"

#include <functional>
#include <span>

namespace chronos::manifest {

struct SealedHeadManifestBuildInput {
  std::reference_wrapper<const DecodedManifestView> predecessor;
  std::reference_wrapper<const EncodedSealedHeadPart> sealed_part;
  std::span<const RetryDescriptor> new_retries;
  std::span<const TabletSchemaBinding> schema_bindings;
  ReferencedPartValidationLimits part_validation_limits;
};

// Builds exactly the next add-only Manifest v1 generation around one already materialized sealed
// head. The part bytes, schema binding, tablet boundary, and one retry outcome per represented WAL
// record are checked before encoding. Input retry order is immaterial; output is canonical. The
// predecessor reclaim checkpoint is deliberately preserved because WAL coverage proof and durable
// publication belong to the installation coordinator, not this pure in-memory builder.
[[nodiscard]] common::Result<EncodedManifest>
build_manifest_v1_for_sealed_head(const SealedHeadManifestBuildInput& input);

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_GENERATION_BUILDER_HPP_
