#include "chronos/raft/metadata.hpp"

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained =
    &chronos::raft::MetadataLimits::maximum_nodes;
}
