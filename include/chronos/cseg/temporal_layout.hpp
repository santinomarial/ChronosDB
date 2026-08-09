#ifndef CHRONOS_CSEG_TEMPORAL_LAYOUT_HPP_
#define CHRONOS_CSEG_TEMPORAL_LAYOUT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/cseg/layout.hpp"

#include <cstdint>
#include <span>

namespace chronos::cseg {

// CSEG v2 preserves the v1 fixed outer header, descriptor sizes, alignment, and page limits while
// expanding the system suffix to the temporal registry. These planners write no bytes.
[[nodiscard]] common::Result<CsegMetadataLayout>
plan_cseg_v2_temporal_metadata_layout(CsegMetadataLayoutInput input);

[[nodiscard]] common::Result<CsegPageLayout>
plan_cseg_v2_temporal_page_layout(std::uint64_t current_offset, std::uint64_t stored_length);

[[nodiscard]] common::Result<CsegFileLayout>
plan_cseg_v2_temporal_layout(CsegMetadataLayoutInput input,
                             std::span<const std::uint64_t> stored_page_lengths);

} // namespace chronos::cseg

#endif // CHRONOS_CSEG_TEMPORAL_LAYOUT_HPP_
