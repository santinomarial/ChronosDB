#ifndef CHRONOS_RAFT_PERSISTENT_STATE_BUDGET_HPP_
#define CHRONOS_RAFT_PERSISTENT_STATE_BUDGET_HPP_

#include "chronos/common/checked_math.hpp"
#include "chronos/common/result.hpp"
#include "chronos/raft/types.hpp"

#include <cstddef>
#include <span>

namespace chronos::raft {

inline constexpr std::size_t kRaftPersistentStateFixedSizeV1 = 112U;
inline constexpr std::size_t kRaftPersistentLogEntryFixedSizeV1 = 32U;
inline constexpr std::size_t kMaximumRaftPersistentStatePayloadSize =
    std::size_t{16U} * 1024U * 1024U - 64U - 4U;

[[nodiscard]] inline common::Result<std::size_t>
raft_persistent_state_payload_size(const std::size_t snapshot_voter_count,
                                   const std::span<const LogEntry> log) noexcept {
  auto voter_bytes = common::checked_multiply(snapshot_voter_count, sizeof(NodeId));
  if (!voter_bytes.has_value()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kOutOfRange, "Raft snapshot voter size overflows"});
  }
  auto size = common::checked_add(kRaftPersistentStateFixedSizeV1, *voter_bytes);
  if (!size.has_value()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kOutOfRange, "Raft persistent state size overflows"});
  }
  for (const LogEntry& entry : log) {
    size = common::checked_add(*size, kRaftPersistentLogEntryFixedSizeV1);
    if (size.has_value())
      size = common::checked_add(*size, entry.payload.size());
    if (!size.has_value()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kOutOfRange, "Raft persistent state size overflows"});
    }
  }
  return *size;
}

} // namespace chronos::raft

#endif // CHRONOS_RAFT_PERSISTENT_STATE_BUDGET_HPP_
