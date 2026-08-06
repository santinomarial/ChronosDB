#ifndef CHRONOS_MANIFEST_SEALED_HEAD_FLUSH_HPP_
#define CHRONOS_MANIFEST_SEALED_HEAD_FLUSH_HPP_

#include "chronos/common/result.hpp"
#include "chronos/cseg/compression.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/head/mutable_head.hpp"
#include "chronos/manifest/types.hpp"

#include <functional>
#include <utility>

namespace chronos::manifest {

struct SealedHeadFlushRequest {
  std::reference_wrapper<const head::HeadSnapshot> snapshot;
  cseg::PartId part_id;
  cseg::PageCompression compression{cseg::PageCompression::kNone};
};

struct EncodedSealedHeadPart {
  PartDescriptor descriptor;
  wal::WalId wal_id;
  cseg::EncodedCsegPart encoded_part;

  EncodedSealedHeadPart(PartDescriptor descriptor_value, wal::WalId wal_id_value,
                        cseg::EncodedCsegPart encoded_part_value) noexcept
      : descriptor(descriptor_value), wal_id(wal_id_value),
        encoded_part(std::move(encoded_part_value)) {}
  EncodedSealedHeadPart(const EncodedSealedHeadPart&) = delete;
  EncodedSealedHeadPart& operator=(const EncodedSealedHeadPart&) = delete;
  EncodedSealedHeadPart(EncodedSealedHeadPart&&) noexcept = default;
  EncodedSealedHeadPart& operator=(EncodedSealedHeadPart&&) noexcept = default;
};

// Converts one nonempty sealed generation without changing its row multiset. Rows are sorted by
// the schema physical key and CSEG system identity, canonical granules/pages are encoded using the
// requested v1 compression policy, and the complete result is schema/content validated.
[[nodiscard]] common::Result<EncodedSealedHeadPart>
encode_sealed_head_v1(const SealedHeadFlushRequest& request);

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_SEALED_HEAD_FLUSH_HPP_
