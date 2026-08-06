#ifndef CHRONOS_MANIFEST_COMPACTION_EQUIVALENCE_HPP_
#define CHRONOS_MANIFEST_COMPACTION_EQUIVALENCE_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/status.hpp"
#include "chronos/cseg/metadata_codec.hpp"
#include "chronos/cseg/types.hpp"
#include "chronos/cseg/validator.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/table_schema.hpp"
#include "chronos/wal/types.hpp"

#include <cstddef>
#include <cstdint>
#include <span>

namespace chronos::manifest {

// Borrows one complete immutable CSEG v1 image and its independently expected final identity.
struct CompactionPartImage {
  cseg::PartId part_id;
  common::ByteView bytes;
};

struct CompactionEquivalenceLimits {
  cseg::CsegMetadataDecodeLimits decode;
  cseg::CsegValidationLimits validation;
  std::size_t max_parts_per_side{1'024U};
  std::uint64_t max_rows_per_side{cseg::format::kMaximumRowCount};
  // Bounds the sum of every active input/output cursor's largest uncompressed granule.
  std::uint64_t max_resident_page_bytes{512U * 1'024U * 1'024U};
};

// Independently proves exact append-only row equivalence between two nonempty, strictly
// PartId-sorted CSEG v1 image sets. Every part is fully decoded, content/schema validated, and
// bound to the expected tablet and WAL. The two globally merged streams must agree cell-for-cell,
// including nulls, floating bits, variable bytes, WAL ID, record sequence, row ordinal, and
// operation. Duplicate physical ordering tuples within either set are corruption. Output PartIds
// must be fresh relative to every input. Bytes are borrowed only for the call.
[[nodiscard]] common::Status validate_append_only_cseg_v1_equivalence(
    std::span<const CompactionPartImage> inputs, std::span<const CompactionPartImage> outputs,
    const schema::TableSchema& schema, const schema::TabletId& tablet_id, const wal::WalId& wal_id,
    CompactionEquivalenceLimits limits = {});

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_COMPACTION_EQUIVALENCE_HPP_
