#ifndef CHRONOS_MANIFEST_LAYOUT_HPP_
#define CHRONOS_MANIFEST_LAYOUT_HPP_

#include "chronos/common/result.hpp"

#include <cstdint>

namespace chronos::manifest {

struct ManifestLayoutInput {
  std::uint64_t tablet_count{};
  std::uint64_t part_count{};
  std::uint64_t retry_count{};
};

struct ManifestLayout {
  std::uint64_t tablets_offset{};
  std::uint64_t parts_offset{};
  std::uint64_t retries_offset{};
  std::uint64_t trailer_offset{};
  std::uint64_t total_length{};

  friend bool operator==(const ManifestLayout&, const ManifestLayout&) = default;
};

// Computes the one canonical Manifest v1 descriptor layout without allocating. Each count and the
// combined byte length are checked against the frozen format limits.
[[nodiscard]] common::Result<ManifestLayout> plan_manifest_v1_layout(ManifestLayoutInput input);

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_LAYOUT_HPP_
