#include "chronos/raft/tablet_reconfiguration_action_codec.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/raft/metadata_codec.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::raft {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                           std::byte{'R'}, std::byte{'A'}, std::byte{'C'},
                                           std::byte{'T'}, std::byte{0U}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::size_t kHeaderCrcOffset = 84U;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status unsupported(const char* message) {
  return {common::StatusCode::kNotSupported, message};
}

[[nodiscard]] bool valid_limits(const TabletReconfigurationActionCodecLimits& limits) {
  constexpr std::size_t kFixedFraming =
      kTabletReconfigurationActionHeaderSize + kTabletReconfigurationActionTrailerSize;
  return limits.maximum_action_bytes >= kFixedFraming + 8U + sizeof(NodeId) &&
         limits.maximum_action_bytes <= kMaximumTabletReconfigurationActionSize &&
         limits.maximum_voters > 0U &&
         limits.maximum_voters <= std::numeric_limits<std::uint32_t>::max() &&
         limits.maximum_voters <=
             (limits.maximum_action_bytes - kFixedFraming - 8U) / sizeof(NodeId);
}

[[nodiscard]] bool canonical_voters(const std::vector<NodeId>& voters,
                                    const std::size_t maximum_voters) {
  return !voters.empty() && voters.size() <= maximum_voters && voters.front() != 0U &&
         std::ranges::is_sorted(voters) &&
         std::adjacent_find(voters.begin(), voters.end()) == voters.end();
}

void store_u32(std::span<std::byte> bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t ordinal = 0U; ordinal < sizeof(value); ++ordinal)
    bytes[offset + ordinal] = static_cast<std::byte>(value >> (ordinal * 8U));
}

[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes, const std::size_t offset) {
  return static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset])) |
         static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset + 1U]) << 8U);
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes, const std::size_t offset) {
  std::uint32_t value{};
  for (std::size_t ordinal = 0U; ordinal < sizeof(value); ++ordinal) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + ordinal]))
             << (ordinal * 8U);
  }
  return value;
}

[[nodiscard]] std::uint64_t load_u64(const common::ByteView bytes, const std::size_t offset) {
  std::uint64_t value{};
  for (std::size_t ordinal = 0U; ordinal < sizeof(value); ++ordinal) {
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[offset + ordinal]))
             << (ordinal * 8U);
  }
  return value;
}

[[nodiscard]] std::uint32_t header_crc(const common::ByteView header) {
  std::array<std::byte, kTabletReconfigurationActionHeaderSize> copy{};
  std::ranges::copy(header, copy.begin());
  std::fill_n(copy.begin() + static_cast<std::ptrdiff_t>(kHeaderCrcOffset), sizeof(std::uint32_t),
              std::byte{0U});
  return common::crc32c(copy);
}

[[nodiscard]] common::Result<schema::TabletId> read_tablet_id(common::ByteReader& reader) {
  auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::Uuid::Bytes owned{};
  std::ranges::copy(*bytes, owned.begin());
  return schema::TabletId::from_bytes(owned);
}

[[nodiscard]] common::Result<GroupId> read_group_id(common::ByteReader& reader) {
  auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::Uuid::Bytes owned{};
  std::ranges::copy(*bytes, owned.begin());
  return GroupId{owned};
}

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_operation(const TabletReconfigurationAction& action,
                 const TabletReconfigurationActionCodecLimits& limits) {
  if (action.id.tablet_id.uuid().is_nil() || action.id.movement_epoch == 0U ||
      action.id.kind != action.kind || action.request.group_id.is_nil()) {
    return common::make_unexpected(invalid("reconfiguration action identity is invalid"));
  }
  return std::visit(
      [&](const auto& operation) -> common::Result<std::vector<std::byte>> {
        using T = std::remove_cvref_t<decltype(operation)>;
        if constexpr (std::is_same_v<T, BeginMembershipChangeOperation>) {
          if (action.kind != TabletReconfigurationActionKind::kBeginJointMembership ||
              !canonical_voters(operation.new_voters, limits.maximum_voters)) {
            return common::make_unexpected(invalid("begin-joint action is inconsistent"));
          }
          std::vector<std::byte> payload(8U + operation.new_voters.size() * sizeof(NodeId),
                                         std::byte{0U});
          common::ByteWriter writer{payload};
          common::Status status =
              writer.write_u32_le(static_cast<std::uint32_t>(operation.new_voters.size()));
          if (status.is_ok())
            status = writer.zero_fill(4U);
          for (const NodeId voter : operation.new_voters) {
            if (status.is_ok())
              status = writer.write_u64_le(voter);
          }
          if (!status.is_ok() || !writer.full())
            return common::make_unexpected(status.is_ok() ? corruption("action size mismatch")
                                                          : status);
          return payload;
        } else if constexpr (std::is_same_v<T, FinalizeMembershipChangeOperation>) {
          if (action.kind != TabletReconfigurationActionKind::kFinalizeJointMembership)
            return common::make_unexpected(invalid("finalize-joint action is inconsistent"));
          return std::vector<std::byte>{};
        } else if constexpr (std::is_same_v<T, ProposeOperation>) {
          if (action.kind != TabletReconfigurationActionKind::kPublishPlacement ||
              operation.type != kRaftMetadataCommandEntryType || operation.payload.empty()) {
            return common::make_unexpected(invalid("placement action is inconsistent"));
          }
          auto command = decode_metadata_command_v1(operation.payload);
          if (!command.has_value() || !std::holds_alternative<TabletPlacementMetadata>(*command)) {
            return common::make_unexpected(invalid("placement action payload is invalid"));
          }
          const auto& placement = std::get<TabletPlacementMetadata>(*command);
          if (action.id.movement_epoch == std::numeric_limits<std::uint64_t>::max() ||
              placement.tablet_id != action.id.tablet_id ||
              placement.placement_epoch != action.id.movement_epoch + 1U) {
            return common::make_unexpected(
                invalid("placement action payload disagrees with its identity"));
          }
          std::vector<std::byte> payload(8U + operation.payload.size(), std::byte{0U});
          common::ByteWriter writer{payload};
          common::Status status = writer.write_u8(operation.type);
          if (status.is_ok())
            status = writer.zero_fill(7U);
          if (status.is_ok())
            status = writer.write_exact(operation.payload);
          if (!status.is_ok() || !writer.full())
            return common::make_unexpected(status.is_ok() ? corruption("action size mismatch")
                                                          : status);
          return payload;
        } else {
          return common::make_unexpected(invalid("Raft operation is not a reconfiguration action"));
        }
      },
      action.request.operation);
}

} // namespace

common::Result<std::vector<std::byte>>
encode_tablet_reconfiguration_action_v1(const TabletReconfigurationAction& action,
                                        const TabletReconfigurationActionCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("reconfiguration action codec limits are invalid"));
  auto payload = encode_operation(action, limits);
  if (!payload.has_value())
    return common::make_unexpected(payload.error());
  if (payload->size() > limits.maximum_action_bytes - kTabletReconfigurationActionHeaderSize -
                            kTabletReconfigurationActionTrailerSize) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "reconfiguration action exceeds the configured size limit"});
  }
  const std::size_t total_size = kTabletReconfigurationActionHeaderSize + payload->size() +
                                 kTabletReconfigurationActionTrailerSize;
  std::vector<std::byte> output(total_size, std::byte{0U});
  common::ByteWriter header{
      std::span<std::byte>{output}.first(kTabletReconfigurationActionHeaderSize)};
  for (const common::Status& status :
       {header.write_exact(kMagic), header.write_u16_le(kMajor), header.write_u16_le(kMinor),
        header.write_u32_le(kTabletReconfigurationActionHeaderSize),
        header.write_u64_le(total_size), header.write_exact(action.id.tablet_id.bytes()),
        header.write_u64_le(action.id.movement_epoch),
        header.write_u8(static_cast<std::uint8_t>(action.kind)), header.zero_fill(7U),
        header.write_exact(action.request.group_id.bytes()), header.write_u64_le(payload->size()),
        header.write_u32_le(common::crc32c(*payload)), header.write_u32_le(0U),
        header.zero_fill(8U)}) {
    if (!status.is_ok())
      return common::make_unexpected(status);
  }
  std::ranges::copy(*payload, output.begin() + static_cast<std::ptrdiff_t>(
                                                   kTabletReconfigurationActionHeaderSize));
  store_u32(output, kHeaderCrcOffset,
            header_crc(common::ByteView{output}.first(kTabletReconfigurationActionHeaderSize)));
  store_u32(output, total_size - kTabletReconfigurationActionTrailerSize,
            common::crc32c(common::ByteView{output}.first(
                total_size - kTabletReconfigurationActionTrailerSize)));
  return output;
}

common::Result<TabletReconfigurationAction>
decode_tablet_reconfiguration_action_v1(const common::ByteView bytes,
                                        const TabletReconfigurationActionCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("reconfiguration action codec limits are invalid"));
  if (bytes.size() <
          kTabletReconfigurationActionHeaderSize + kTabletReconfigurationActionTrailerSize ||
      bytes.size() > limits.maximum_action_bytes) {
    return common::make_unexpected(corruption("reconfiguration action size is invalid"));
  }
  const common::ByteView header = bytes.first(kTabletReconfigurationActionHeaderSize);
  if (header_crc(header) != load_u32(header, kHeaderCrcOffset))
    return common::make_unexpected(corruption("reconfiguration action header checksum mismatch"));
  if (!std::ranges::equal(header.first(kMagic.size()), kMagic) || load_u16(header, 8U) != kMajor ||
      load_u16(header, 10U) != kMinor) {
    return common::make_unexpected(unsupported("reconfiguration action version is unsupported"));
  }
  common::ByteReader reader{header.subspan(24U)};
  auto tablet_id = read_tablet_id(reader);
  auto epoch = reader.read_u64_le();
  auto kind_byte = reader.read_u8();
  auto kind_reserved = reader.read_exact(7U);
  auto group_id = read_group_id(reader);
  auto payload_size = reader.read_u64_le();
  auto payload_crc = reader.read_u32_le();
  auto header_crc_field = reader.read_u32_le();
  auto tail_reserved = reader.read_exact(8U);
  if (load_u32(header, 12U) != kTabletReconfigurationActionHeaderSize ||
      load_u64(header, 16U) != bytes.size() || !tablet_id.has_value() || !epoch.has_value() ||
      !kind_byte.has_value() || !kind_reserved.has_value() || !group_id.has_value() ||
      !payload_size.has_value() || !payload_crc.has_value() || !header_crc_field.has_value() ||
      !tail_reserved.has_value() || !reader.empty() || tablet_id->uuid().is_nil() || *epoch == 0U ||
      group_id->is_nil() || *header_crc_field != load_u32(header, kHeaderCrcOffset) ||
      *payload_size != bytes.size() - kTabletReconfigurationActionHeaderSize -
                           kTabletReconfigurationActionTrailerSize ||
      std::ranges::any_of(*kind_reserved,
                          [](const std::byte value) { return value != std::byte{0U}; }) ||
      std::ranges::any_of(*tail_reserved,
                          [](const std::byte value) { return value != std::byte{0U}; })) {
    return common::make_unexpected(corruption("reconfiguration action header is invalid"));
  }
  const common::ByteView payload = bytes.subspan(kTabletReconfigurationActionHeaderSize,
                                                 static_cast<std::size_t>(*payload_size));
  if (common::crc32c(payload) != *payload_crc ||
      common::crc32c(bytes.first(bytes.size() - kTabletReconfigurationActionTrailerSize)) !=
          load_u32(bytes, bytes.size() - kTabletReconfigurationActionTrailerSize)) {
    return common::make_unexpected(corruption("reconfiguration action checksum mismatch"));
  }
  const auto kind = static_cast<TabletReconfigurationActionKind>(*kind_byte);
  DurableRaftOperation operation;
  common::ByteReader payload_reader{payload};
  if (kind == TabletReconfigurationActionKind::kBeginJointMembership) {
    auto count = payload_reader.read_u32_le();
    auto reserved = payload_reader.read_u32_le();
    if (!count.has_value() || !reserved.has_value() || *count == 0U ||
        *count > limits.maximum_voters || *reserved != 0U ||
        payload_reader.remaining() != static_cast<std::size_t>(*count) * sizeof(NodeId)) {
      return common::make_unexpected(corruption("begin-joint action payload is invalid"));
    }
    std::vector<NodeId> voters;
    voters.reserve(*count);
    for (std::uint32_t ordinal = 0U; ordinal < *count; ++ordinal) {
      auto voter = payload_reader.read_u64_le();
      if (!voter.has_value())
        return common::make_unexpected(voter.error());
      voters.push_back(*voter);
    }
    if (!canonical_voters(voters, limits.maximum_voters))
      return common::make_unexpected(corruption("begin-joint voters are noncanonical"));
    operation = BeginMembershipChangeOperation{std::move(voters)};
  } else if (kind == TabletReconfigurationActionKind::kFinalizeJointMembership) {
    if (!payload.empty())
      return common::make_unexpected(corruption("finalize-joint action payload is not empty"));
    operation = FinalizeMembershipChangeOperation{};
  } else if (kind == TabletReconfigurationActionKind::kPublishPlacement) {
    auto type = payload_reader.read_u8();
    auto reserved = payload_reader.read_exact(7U);
    if (!type.has_value() || !reserved.has_value() || *type != kRaftMetadataCommandEntryType ||
        std::ranges::any_of(*reserved,
                            [](const std::byte value) { return value != std::byte{0U}; }) ||
        payload_reader.empty()) {
      return common::make_unexpected(corruption("placement action payload is invalid"));
    }
    auto command_bytes = payload_reader.read_exact(payload_reader.remaining());
    if (!command_bytes.has_value())
      return common::make_unexpected(command_bytes.error());
    auto command = decode_metadata_command_v1(*command_bytes);
    if (!command.has_value() || !std::holds_alternative<TabletPlacementMetadata>(*command))
      return common::make_unexpected(corruption("placement metadata command is invalid"));
    const auto& placement = std::get<TabletPlacementMetadata>(*command);
    if (*epoch == std::numeric_limits<std::uint64_t>::max() || placement.tablet_id != *tablet_id ||
        placement.placement_epoch != *epoch + 1U) {
      return common::make_unexpected(
          corruption("placement metadata command disagrees with action identity"));
    }
    operation = ProposeOperation{
        *type, std::vector<std::byte>{command_bytes->begin(), command_bytes->end()}};
  } else {
    return common::make_unexpected(corruption("reconfiguration action kind is unknown"));
  }
  return TabletReconfigurationAction{TabletReconfigurationActionId{*tablet_id, *epoch, kind}, kind,
                                     DurableRaftRequest{*group_id, std::move(operation)}};
}

} // namespace chronos::raft
