#include "chronos/query/temporal_recovery.hpp"

namespace {
[[maybe_unused]] constexpr auto* kRecoverTemporalWal = &chronos::query::recover_temporal_wal;
[[maybe_unused]] constexpr auto* kRecoverManifestTemporalWal =
    &chronos::query::recover_manifest_temporal_wal;
} // namespace
