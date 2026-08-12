#include "chronos/query/tablet_state_pipeline.hpp"

namespace {
[[maybe_unused]] constexpr auto kSourceLimit =
    chronos::query::TabletStatePipelineLimits{}.maximum_source_configuration_bytes;
}
