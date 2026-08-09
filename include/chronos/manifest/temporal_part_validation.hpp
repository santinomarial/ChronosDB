#ifndef CHRONOS_MANIFEST_TEMPORAL_PART_VALIDATION_HPP_
#define CHRONOS_MANIFEST_TEMPORAL_PART_VALIDATION_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/cseg/metadata_codec.hpp"
#include "chronos/cseg/validator.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/schema/table_schema.hpp"

namespace chronos::manifest {

struct TemporalPartValidationLimits {
  cseg::CsegMetadataDecodeLimits decode;
  cseg::CsegValidationLimits contents;
};

// Derives the only Manifest v2 descriptor that can truthfully describe one exact CSEG v2 image.
// The image is decoded exactly, fully validated against its physical schema and target tablet, and
// hashed in its entirety. Every temporal row must use the supplied source lineage. The returned
// descriptor recomputes commit-position and system-time extrema rather than trusting metadata.
[[nodiscard]] common::Result<TemporalPartDescriptor> describe_manifest_v2_temporal_part_image(
    common::ByteView image, const schema::TableSchema& schema,
    const schema::TabletId& target_tablet, ManifestCommitSource commit_source,
    const common::Uuid& source_id, TemporalPartValidationLimits limits = {});

// Completes the descriptor-to-object trust boundary for one decoded Manifest v2 part. Owner and
// descriptor disagreements are corruption because both are durable manifest state; invalid schema
// or tablet call context retains the CSEG validator's invalid-argument classification.
[[nodiscard]] common::Status
validate_manifest_v2_temporal_part_image(const TemporalPartDescriptor& descriptor,
                                         const TemporalTabletDescriptor& owner,
                                         common::ByteView image, const schema::TableSchema& schema,
                                         TemporalPartValidationLimits limits = {});

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_TEMPORAL_PART_VALIDATION_HPP_
