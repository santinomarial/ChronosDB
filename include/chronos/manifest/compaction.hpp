#ifndef CHRONOS_MANIFEST_COMPACTION_HPP_
#define CHRONOS_MANIFEST_COMPACTION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/cseg/compression.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/compaction_equivalence.hpp"
#include "chronos/manifest/part_validation.hpp"
#include "chronos/manifest/types.hpp"
#include "chronos/manifest/validation.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/table_schema.hpp"
#include "chronos/wal/types.hpp"

#include <cstdint>
#include <functional>
#include <span>
#include <utility>

namespace chronos::manifest {

struct AppendOnlyCompactionLimits {
  CompactionEquivalenceLimits equivalence;
  std::uint64_t max_rows{16ULL * 1'024ULL * 1'024ULL};
  std::uint64_t max_materialized_page_bytes{512ULL * 1'024ULL * 1'024ULL};
};

struct AppendOnlyCompactionRequest {
  std::span<const CompactionPartImage> inputs;
  std::reference_wrapper<const schema::TableSchema> schema;
  schema::TabletId tablet_id;
  wal::WalId wal_id;
  cseg::PartId output_part_id;
  cseg::PageCompression compression{cseg::PageCompression::kNone};
  AppendOnlyCompactionLimits limits;
};

struct EncodedCompactionPart {
  PartDescriptor descriptor;
  wal::WalId wal_id;
  cseg::EncodedCsegPart encoded_part;

  EncodedCompactionPart(PartDescriptor descriptor_value, wal::WalId wal_id_value,
                        cseg::EncodedCsegPart encoded_part_value) noexcept
      : descriptor(descriptor_value), wal_id(wal_id_value),
        encoded_part(std::move(encoded_part_value)) {}
  EncodedCompactionPart(const EncodedCompactionPart&) = delete;
  EncodedCompactionPart& operator=(const EncodedCompactionPart&) = delete;
  EncodedCompactionPart(EncodedCompactionPart&&) noexcept = default;
  EncodedCompactionPart& operator=(EncodedCompactionPart&&) noexcept = default;
};

struct AppendOnlyCompactionManifestBuildInput {
  std::reference_wrapper<const DecodedManifestView> predecessor;
  std::span<const CompactionPartImage> inputs;
  std::reference_wrapper<const EncodedCompactionPart> output;
  std::reference_wrapper<const schema::TableSchema> schema;
  std::span<const TabletSchemaBinding> schema_bindings;
  CompactionEquivalenceLimits equivalence_limits;
  ReferencedPartValidationLimits part_validation_limits;
};

// Reference append-only merger for one nonempty, strictly PartId-sorted input set and one fresh
// output identity. It fully validates and schema/WAL-binds every input, stably merges by the frozen
// CSEG physical tuple, rejects duplicate tuples, emits one canonical CSEG v1 part, and runs the
// independent full-row equivalence oracle before returning owned bytes. It performs no filesystem,
// Manifest, publication, or reclamation operation.
[[nodiscard]] common::Result<EncodedCompactionPart>
merge_append_only_cseg_v1(const AppendOnlyCompactionRequest& request);

// Builds one exact next-generation Manifest snapshot only after independently revalidating the
// supplied input/output images and their complete append-only row equivalence. The predecessor's
// tablet, checkpoint, and retry state are preserved; exactly the input identities are replaced by
// the one fresh output descriptor. No filesystem or publication operation is performed.
[[nodiscard]] common::Result<EncodedManifest>
build_manifest_v1_for_append_only_compaction(const AppendOnlyCompactionManifestBuildInput& input);

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_COMPACTION_HPP_
