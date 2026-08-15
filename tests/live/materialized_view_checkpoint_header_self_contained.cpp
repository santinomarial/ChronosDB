#include "chronos/live/materialized_view_checkpoint.hpp"

namespace {
[[maybe_unused]] constexpr std::size_t kHeader =
    chronos::live::kMaterializedViewCheckpointHeaderSize;
static_assert(chronos::live::kMaximumMaterializedViewCheckpointSize == (std::size_t{1U} << 30U));
static_assert(chronos::live::MaterializedViewCheckpointCodecLimits{}.maximum_checkpoint_bytes ==
              (std::size_t{256U} << 20U));
} // namespace
