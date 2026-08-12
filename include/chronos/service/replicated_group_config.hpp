#ifndef CHRONOS_SERVICE_REPLICATED_GROUP_CONFIG_HPP_
#define CHRONOS_SERVICE_REPLICATED_GROUP_CONFIG_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/durable_runtime.hpp"

#include <cstddef>
#include <string_view>
#include <vector>

namespace chronos::service {

inline constexpr std::string_view kReplicatedGroupConfigV1Magic{"CHRONOSDB_REPLICATED_GROUPS_V1"};

struct ReplicatedGroupConfigLimits {
  std::size_t maximum_bytes{1024U * 1024U};
  std::size_t maximum_groups{4096U};
  std::size_t maximum_voters_per_group{9U};
};

// Parses strict deployment text. The first line is kReplicatedGroupConfigV1Magic. Each remaining
// line is one lowercase canonical UUID, '=', and a strictly increasing comma-separated list of
// positive decimal node IDs. A final LF is optional; blank lines, comments, spaces, and CR are not
// accepted. The returned groups are sorted by UUID.
[[nodiscard]] common::Result<std::vector<raft::RaftGroupConfiguration>>
parse_replicated_group_config(std::string_view text, ReplicatedGroupConfigLimits limits = {});

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_GROUP_CONFIG_HPP_
