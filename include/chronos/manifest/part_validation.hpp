#ifndef CHRONOS_MANIFEST_PART_VALIDATION_HPP_
#define CHRONOS_MANIFEST_PART_VALIDATION_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/status.hpp"
#include "chronos/cseg/metadata_codec.hpp"
#include "chronos/cseg/validator.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/validation.hpp"

#include <span>
#include <string_view>

namespace chronos::manifest {

// Borrows one already-read regular-file image for the duration of validation. Callers provide
// exactly one image per manifest part in descriptor order; filesystem opening and file-type checks
// belong to the installation owner.
struct ReferencedPartImage {
  std::string_view file_name;
  common::ByteView bytes;
};

struct ReferencedPartValidationLimits {
  cseg::CsegMetadataDecodeLimits decode;
  cseg::CsegValidationLimits contents;
};

// Validates one supplied image against one descriptor, WAL identity, and exact physical schema.
// This is the shared pre-install/readback primitive used by the filesystem owner and by complete
// manifest referenced-part validation.
[[nodiscard]] common::Status
validate_manifest_v1_part_image(const PartDescriptor& descriptor, const wal::WalId& wal_id,
                                const schema::TableSchema& schema, const ReferencedPartImage& image,
                                ReferencedPartValidationLimits limits = {});

// Completes the in-memory referenced-part trust boundary. It first validates exact schema lineage
// bindings, then for every descriptor checks the canonical filename, exact file length, complete
// CSEG integrity/content/schema validation, duplicate header metadata, manifest WAL identity on
// every row, and recomputed RECORD_SEQUENCE extrema. Any disagreement between valid manifest state
// and supplied installed bytes is corruption; invalid catalog bindings remain invalid arguments.
[[nodiscard]] common::Status validate_manifest_v1_referenced_parts(
    const DecodedManifestView& manifest, std::span<const TabletSchemaBinding> bindings,
    std::span<const ReferencedPartImage> images, ReferencedPartValidationLimits limits = {});

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_PART_VALIDATION_HPP_
