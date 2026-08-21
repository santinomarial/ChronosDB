#ifndef CHRONOS_RAFT_MEMBERSHIP_HPP_
#define CHRONOS_RAFT_MEMBERSHIP_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/raft/types.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <variant>
#include <vector>

namespace chronos::raft {

inline constexpr std::uint8_t kLeaderNoopEntryType = 253U;
inline constexpr std::uint8_t kJointMembershipEntryType = 254U;
inline constexpr std::uint8_t kFinalMembershipEntryType = 255U;
inline constexpr std::size_t kMembershipCommandHeaderSize = 32U;
inline constexpr std::size_t kMembershipCommandTrailerSize = 4U;
inline constexpr std::size_t kMaximumMembershipVoters = std::numeric_limits<std::uint16_t>::max();

struct JointMembershipCommand {
  std::vector<NodeId> old_voters;
  std::vector<NodeId> new_voters;

  friend bool operator==(const JointMembershipCommand&, const JointMembershipCommand&) = default;
};

struct FinalMembershipCommand {
  LogIndex joint_index{};
  std::vector<NodeId> new_voters;

  friend bool operator==(const FinalMembershipCommand&, const FinalMembershipCommand&) = default;
};

using MembershipCommand = std::variant<JointMembershipCommand, FinalMembershipCommand>;

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_membership_command_v1(MembershipCommand command, std::size_t maximum_voters = 31U);
[[nodiscard]] common::Result<MembershipCommand>
decode_membership_command_v1(common::ByteView bytes, std::size_t maximum_voters = 31U);

[[nodiscard]] constexpr bool is_membership_entry_type(const std::uint8_t type) noexcept {
  return type == kJointMembershipEntryType || type == kFinalMembershipEntryType;
}

[[nodiscard]] constexpr bool is_internal_raft_entry_type(const std::uint8_t type) noexcept {
  return type == kLeaderNoopEntryType || is_membership_entry_type(type);
}

} // namespace chronos::raft

#endif // CHRONOS_RAFT_MEMBERSHIP_HPP_
