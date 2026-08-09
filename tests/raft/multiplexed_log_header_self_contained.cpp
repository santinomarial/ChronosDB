#include "chronos/raft/multiplexed_log.hpp"

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained = chronos::raft::kMultiplexedLogFormatMajor;
}
