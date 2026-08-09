#include "chronos/query/distributed.hpp"

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained =
    chronos::query::DistributedReadConsistency::kLeaderLinearizable;
}
