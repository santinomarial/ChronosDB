#include "chronos/query/temporal_snapshot.hpp"

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained =
    chronos::query::TemporalMutationKind::kCorrection;
[[maybe_unused]] constexpr auto kRetainedHistoryIsDeclared =
    sizeof(chronos::query::RetainedTemporalVersion);
} // namespace
