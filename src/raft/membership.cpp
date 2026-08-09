#include "chronos/raft/membership.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::raft {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                           std::byte{'N'}, std::byte{'M'}, std::byte{'B'},
                                           std::byte{'C'}, std::byte{0U}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;

enum class Kind : std::uint8_t { kJoint = 1U, kFinal = 2U };

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status unsupported(std::string message) {
  return common::Status{common::StatusCode::kNotSupported, std::move(message)};
}

[[nodiscard]] bool canonical(const std::vector<NodeId>& voters, const std::size_t maximum_voters) {
  return !voters.empty() && voters.size() <= maximum_voters && voters.front() != 0U &&
         std::ranges::is_sorted(voters) &&
         std::adjacent_find(voters.begin(), voters.end()) == voters.end();
}

[[nodiscard]] common::Status canonicalize(std::vector<NodeId>& voters,
                                          const std::size_t maximum_voters) {
  std::ranges::sort(voters);
  return canonical(voters, maximum_voters)
             ? common::Status::ok()
             : invalid("membership voters must be bounded, unique, and nonzero");
}

[[nodiscard]] bool union_fits(const std::vector<NodeId>& old_voters,
                              const std::vector<NodeId>& new_voters,
                              const std::size_t maximum_voters) {
  std::size_t count = 0U;
  auto old = old_voters.begin();
  auto next = new_voters.begin();
  while (old != old_voters.end() || next != new_voters.end()) {
    if (next == new_voters.end() || (old != old_voters.end() && *old < *next)) {
      ++old;
    } else if (old == old_voters.end() || *next < *old) {
      ++next;
    } else {
      ++old;
      ++next;
    }
    if (++count > maximum_voters)
      return false;
  }
  return true;
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes, const std::size_t offset) {
  std::uint32_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  }
  return value;
}

} // namespace

common::Result<std::vector<std::byte>>
encode_membership_command_v1(MembershipCommand command, const std::size_t maximum_voters) {
  if (maximum_voters == 0U || maximum_voters > std::numeric_limits<std::uint16_t>::max()) {
    return common::make_unexpected(invalid("membership voter limit is invalid"));
  }
  Kind kind = Kind::kJoint;
  LogIndex joint_index{};
  std::vector<NodeId>* old_voters = nullptr;
  std::vector<NodeId>* new_voters = nullptr;
  common::Status status = std::visit(
      [&](auto& value) -> common::Status {
        using T = std::remove_cvref_t<decltype(value)>;
        if constexpr (std::is_same_v<T, JointMembershipCommand>) {
          kind = Kind::kJoint;
          old_voters = &value.old_voters;
          new_voters = &value.new_voters;
        } else {
          kind = Kind::kFinal;
          joint_index = value.joint_index;
          new_voters = &value.new_voters;
          if (joint_index == 0U)
            return invalid("final membership command has no joint index");
        }
        if (old_voters != nullptr) {
          if (auto result = canonicalize(*old_voters, maximum_voters); !result.is_ok())
            return result;
        }
        return canonicalize(*new_voters, maximum_voters);
      },
      command);
  if (!status.is_ok())
    return common::make_unexpected(status);
  if (old_voters != nullptr && !union_fits(*old_voters, *new_voters, maximum_voters))
    return common::make_unexpected(invalid("joint membership union exceeds voter limit"));
  const std::size_t old_count = old_voters == nullptr ? 0U : old_voters->size();
  const std::size_t new_count = new_voters->size();
  const std::size_t total_size = kMembershipCommandHeaderSize +
                                 (old_count + new_count) * sizeof(NodeId) +
                                 kMembershipCommandTrailerSize;
  std::vector<std::byte> bytes(total_size, std::byte{0U});
  common::ByteWriter writer{bytes};
  for (const common::Status& result :
       {writer.write_exact(kMagic), writer.write_u16_le(kMajor), writer.write_u16_le(kMinor),
        writer.write_u8(static_cast<std::uint8_t>(kind)), writer.zero_fill(3U),
        writer.write_u32_le(static_cast<std::uint32_t>(total_size)),
        writer.write_u64_le(joint_index),
        writer.write_u16_le(static_cast<std::uint16_t>(old_count)),
        writer.write_u16_le(static_cast<std::uint16_t>(new_count))}) {
    if (!result.is_ok())
      return common::make_unexpected(result);
  }
  if (old_voters != nullptr) {
    for (const NodeId voter : *old_voters) {
      if (status = writer.write_u64_le(voter); !status.is_ok())
        return common::make_unexpected(status);
    }
  }
  for (const NodeId voter : *new_voters) {
    if (status = writer.write_u64_le(voter); !status.is_ok())
      return common::make_unexpected(status);
  }
  if (status = writer.write_u32_le(common::crc32c(
          common::ByteView{bytes}.first(total_size - kMembershipCommandTrailerSize)));
      !status.is_ok() || !writer.full()) {
    return common::make_unexpected(status.is_ok() ? corruption("membership encoding size mismatch")
                                                  : status);
  }
  return bytes;
}

common::Result<MembershipCommand> decode_membership_command_v1(const common::ByteView bytes,
                                                               const std::size_t maximum_voters) {
  if (maximum_voters == 0U || maximum_voters > std::numeric_limits<std::uint16_t>::max())
    return common::make_unexpected(invalid("membership voter limit is invalid"));
  if (bytes.size() < kMembershipCommandHeaderSize + kMembershipCommandTrailerSize)
    return common::make_unexpected(corruption("membership command is shorter than framing"));
  if (common::crc32c(bytes.first(bytes.size() - kMembershipCommandTrailerSize)) !=
      load_u32(bytes, bytes.size() - kMembershipCommandTrailerSize)) {
    return common::make_unexpected(corruption("membership command checksum mismatch"));
  }
  common::ByteReader reader{bytes};
  auto magic = reader.read_exact(kMagic.size());
  auto major = reader.read_u16_le();
  auto minor = reader.read_u16_le();
  auto kind = reader.read_u8();
  auto reserved = reader.read_exact(3U);
  auto total_size = reader.read_u32_le();
  auto joint_index = reader.read_u64_le();
  auto old_count = reader.read_u16_le();
  auto new_count = reader.read_u16_le();
  if (!magic.has_value() || !major.has_value() || !minor.has_value() || !kind.has_value() ||
      !reserved.has_value() || !total_size.has_value() || !joint_index.has_value() ||
      !old_count.has_value() || !new_count.has_value()) {
    return common::make_unexpected(corruption("membership command header is incomplete"));
  }
  if (!std::ranges::equal(*magic, kMagic) || *major != kMajor)
    return common::make_unexpected(unsupported("membership command magic or major unknown"));
  if (*minor != kMinor)
    return common::make_unexpected(unsupported("membership command minor version unknown"));
  if (std::ranges::any_of(*reserved,
                          [](const std::byte value) { return value != std::byte{0U}; }) ||
      *total_size != bytes.size() || *old_count > maximum_voters || *new_count == 0U ||
      *new_count > maximum_voters ||
      reader.remaining() != (static_cast<std::size_t>(*old_count) + *new_count) * sizeof(NodeId) +
                                kMembershipCommandTrailerSize) {
    return common::make_unexpected(corruption("membership command header relationships invalid"));
  }
  std::vector<NodeId> old_voters;
  std::vector<NodeId> new_voters;
  old_voters.reserve(*old_count);
  new_voters.reserve(*new_count);
  for (std::size_t index = 0U; index < *old_count; ++index) {
    auto voter = reader.read_u64_le();
    if (!voter.has_value())
      return common::make_unexpected(voter.error());
    old_voters.push_back(*voter);
  }
  for (std::size_t index = 0U; index < *new_count; ++index) {
    auto voter = reader.read_u64_le();
    if (!voter.has_value())
      return common::make_unexpected(voter.error());
    new_voters.push_back(*voter);
  }
  if (!canonical(new_voters, maximum_voters) ||
      (*kind == static_cast<std::uint8_t>(Kind::kJoint)
           ? (*joint_index != 0U || !canonical(old_voters, maximum_voters) ||
              !union_fits(old_voters, new_voters, maximum_voters))
           : (*kind != static_cast<std::uint8_t>(Kind::kFinal) || *joint_index == 0U ||
              !old_voters.empty()))) {
    return common::make_unexpected(corruption("membership command semantics are noncanonical"));
  }
  if (auto trailer = reader.read_u32_le(); !trailer.has_value() || !reader.empty())
    return common::make_unexpected(corruption("membership command trailer is inaccessible"));
  if (*kind == static_cast<std::uint8_t>(Kind::kJoint))
    return MembershipCommand{JointMembershipCommand{std::move(old_voters), std::move(new_voters)}};
  return MembershipCommand{FinalMembershipCommand{*joint_index, std::move(new_voters)}};
}

} // namespace chronos::raft
