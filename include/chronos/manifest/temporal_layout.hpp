#ifndef CHRONOS_MANIFEST_TEMPORAL_LAYOUT_HPP_
#define CHRONOS_MANIFEST_TEMPORAL_LAYOUT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/manifest/layout.hpp"

namespace chronos::manifest {

[[nodiscard]] common::Result<ManifestLayout>
plan_manifest_v2_temporal_layout(ManifestLayoutInput input);

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_TEMPORAL_LAYOUT_HPP_
