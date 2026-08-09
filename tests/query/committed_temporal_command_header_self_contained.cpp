#include "chronos/query/committed_temporal_command.hpp"

namespace {
[[maybe_unused]] constexpr auto* kApplyCommittedTemporalCommand =
    &chronos::query::apply_committed_temporal_command;
[[maybe_unused]] constexpr auto* kVerifyRetainedTemporalCommand =
    &chronos::query::verify_retained_temporal_command;
} // namespace
