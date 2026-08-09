#include "chronos/manifest/temporal_validation.hpp"

namespace {
using TemporalTransitionFunction =
    chronos::common::Status (*)(const chronos::manifest::DecodedTemporalManifestView&,
                                const chronos::manifest::DecodedTemporalManifestView&,
                                std::span<const chronos::manifest::TabletSchemaBinding>);
[[maybe_unused]] constexpr TemporalTransitionFunction kTransition =
    &chronos::manifest::validate_manifest_v2_temporal_transition;
} // namespace
