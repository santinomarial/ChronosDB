#ifndef CHRONOS_CSEG_LAYOUT_HPP_
#define CHRONOS_CSEG_LAYOUT_HPP_

#include "chronos/common/result.hpp"

#include <cstdint>
#include <span>

namespace chronos::cseg {

struct CsegMetadataLayoutInput {
  std::uint32_t user_column_count{};
  std::uint32_t granule_count{};
};

struct CsegMetadataLayout {
  std::uint32_t stored_column_count{};
  std::uint32_t page_count{};
  std::uint64_t columns_offset{};
  std::uint64_t granules_offset{};
  std::uint64_t pages_offset{};
  std::uint64_t metadata_trailer_offset{};
  std::uint64_t metadata_length{};

  friend bool operator==(const CsegMetadataLayout&, const CsegMetadataLayout&) = default;
};

struct CsegFileLayout {
  CsegMetadataLayout metadata;
  std::uint64_t total_length{};

  friend bool operator==(const CsegFileLayout&, const CsegFileLayout&) = default;
};

struct CsegPageLayout {
  std::uint64_t offset{};
  std::uint64_t stored_length{};
  std::uint64_t padding_length{};
  std::uint64_t next_offset{};

  friend bool operator==(const CsegPageLayout&, const CsegPageLayout&) = default;
};

// Computes the exact descriptor arrays and metadata trailer without allocating or trusting a
// caller-provided product. Counts outside the frozen format limits are rejected.
[[nodiscard]] common::Result<CsegMetadataLayout>
plan_cseg_v1_metadata_layout(CsegMetadataLayoutInput input);

// Places one nonempty stored page at an already canonical aligned offset and returns the minimum
// zero padding plus the next aligned offset. This primitive does not allocate.
[[nodiscard]] common::Result<CsegPageLayout> plan_cseg_v1_page_layout(std::uint64_t current_offset,
                                                                      std::uint64_t stored_length);

// Computes the canonical aligned file end for exactly one stored length per page. The span order
// is granule-major then stored-column-major. Page offsets are recovered by iterating from
// metadata.metadata_length with plan_cseg_v1_page_layout().
[[nodiscard]] common::Result<CsegFileLayout>
plan_cseg_v1_layout(CsegMetadataLayoutInput input,
                    std::span<const std::uint64_t> stored_page_lengths);

} // namespace chronos::cseg

#endif // CHRONOS_CSEG_LAYOUT_HPP_
