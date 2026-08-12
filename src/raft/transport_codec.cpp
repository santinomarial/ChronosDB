#include "chronos/raft/transport_codec.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/raft/membership.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <ranges>
#include <span>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::raft {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                           std::byte{'N'}, std::byte{'R'}, std::byte{'T'},
                                           std::byte{'W'}, std::byte{0U}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::size_t kHeaderCrcOffset = 76U;
constexpr std::size_t kMaximumEntryHeaderSize = 24U;

enum class MessageKind : std::uint8_t {
  kRequestVoteRequest = 1U,
  kRequestVoteResponse = 2U,
  kAppendEntriesRequest = 3U,
  kAppendEntriesResponse = 4U,
  kInstallSnapshotRequest = 5U,
  kInstallSnapshotResponse = 6U,
  kReadBarrierRequest = 7U,
  kReadBarrierResponse = 8U,
};

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status unsupported(const char* message) {
  return {common::StatusCode::kNotSupported, message};
}

[[nodiscard]] bool valid_limits(const RaftTransportCodecLimits& limits) noexcept {
  constexpr std::size_t kFraming = kRaftTransportHeaderSize + kRaftTransportTrailerSize;
  return limits.maximum_frame_bytes >= kFraming + 16U &&
         limits.maximum_frame_bytes <= kMaximumRaftTransportFrameSize &&
         limits.maximum_append_entries != 0U &&
         limits.maximum_append_entries <= std::numeric_limits<std::uint32_t>::max() &&
         limits.maximum_entry_bytes != 0U &&
         limits.maximum_entry_bytes <= limits.maximum_frame_bytes &&
         limits.maximum_snapshot_voters != 0U &&
         limits.maximum_snapshot_voters <= std::numeric_limits<std::uint32_t>::max();
}

[[nodiscard]] bool canonical_voters(const std::vector<NodeId>& voters,
                                    const std::size_t maximum) noexcept {
  return voters.size() <= maximum &&
         (voters.empty() || (voters.front() != 0U && std::ranges::is_sorted(voters) &&
                             std::adjacent_find(voters.begin(), voters.end()) == voters.end()));
}

[[nodiscard]] bool zero_bytes(const common::ByteView bytes) noexcept {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{0U}; });
}

void store_u32(const std::span<std::byte> bytes, const std::size_t offset,
               const std::uint32_t value) noexcept {
  for (std::size_t ordinal = 0U; ordinal < sizeof(value); ++ordinal)
    bytes[offset + ordinal] = static_cast<std::byte>(value >> (ordinal * 8U));
}

[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  return static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset])) |
         static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset + 1U]) << 8U);
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  std::uint32_t value{};
  for (std::size_t ordinal = 0U; ordinal < sizeof(value); ++ordinal) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + ordinal]))
             << (ordinal * 8U);
  }
  return value;
}

[[nodiscard]] std::uint64_t load_u64(const common::ByteView bytes,
                                     const std::size_t offset) noexcept {
  std::uint64_t value{};
  for (std::size_t ordinal = 0U; ordinal < sizeof(value); ++ordinal) {
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[offset + ordinal]))
             << (ordinal * 8U);
  }
  return value;
}

[[nodiscard]] std::uint32_t header_crc(const common::ByteView header) noexcept {
  std::array<std::byte, kRaftTransportHeaderSize> copy{};
  std::ranges::copy(header, copy.begin());
  std::fill_n(copy.begin() + static_cast<std::ptrdiff_t>(kHeaderCrcOffset), sizeof(std::uint32_t),
              std::byte{0U});
  return common::crc32c(copy);
}

[[nodiscard]] MessageKind message_kind(const Message& message) noexcept {
  return std::visit(
      [](const auto& value) {
        using T = std::remove_cvref_t<decltype(value)>;
        if constexpr (std::is_same_v<T, RequestVoteRequest>)
          return MessageKind::kRequestVoteRequest;
        if constexpr (std::is_same_v<T, RequestVoteResponse>)
          return MessageKind::kRequestVoteResponse;
        if constexpr (std::is_same_v<T, AppendEntriesRequest>)
          return MessageKind::kAppendEntriesRequest;
        if constexpr (std::is_same_v<T, AppendEntriesResponse>)
          return MessageKind::kAppendEntriesResponse;
        if constexpr (std::is_same_v<T, InstallSnapshotRequest>)
          return MessageKind::kInstallSnapshotRequest;
        if constexpr (std::is_same_v<T, InstallSnapshotResponse>)
          return MessageKind::kInstallSnapshotResponse;
        if constexpr (std::is_same_v<T, ReadBarrierRequest>)
          return MessageKind::kReadBarrierRequest;
        return MessageKind::kReadBarrierResponse;
      },
      message);
}

[[nodiscard]] common::Status validate_snapshot(const SnapshotMetadata& snapshot,
                                               const std::size_t maximum_voters) {
  if (snapshot.last_included_index == 0U || snapshot.last_included_term == 0U ||
      snapshot.manifest_generation == 0U ||
      snapshot.configuration_index > snapshot.last_included_index || snapshot.voters.empty() ||
      !canonical_voters(snapshot.voters, maximum_voters)) {
    return invalid("Raft transport snapshot metadata is inconsistent");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_message(const RaftTransportEnvelope& envelope,
                                              const RaftTransportCodecLimits& limits) {
  if (envelope.group_id.is_nil() || envelope.source == 0U || envelope.destination == 0U ||
      envelope.source == envelope.destination)
    return invalid("Raft transport route identity is invalid");
  return std::visit(
      [&](const auto& value) -> common::Status {
        using T = std::remove_cvref_t<decltype(value)>;
        if (value.term == 0U)
          return invalid("Raft transport message term is zero");
        if constexpr (std::is_same_v<T, RequestVoteRequest>) {
          if (value.candidate_id != envelope.source)
            return invalid("Raft vote candidate disagrees with the transport source");
        } else if constexpr (std::is_same_v<T, AppendEntriesRequest>) {
          if (value.leader_id != envelope.source ||
              value.entries.size() > limits.maximum_append_entries)
            return invalid("Raft append request source or entry count is invalid");
          LogIndex expected = value.previous_log_index;
          for (const LogEntry& entry : value.entries) {
            if (expected == std::numeric_limits<LogIndex>::max() || entry.index != expected + 1U ||
                entry.index == std::numeric_limits<LogIndex>::max() || entry.term == 0U ||
                entry.term > value.term || entry.type == 0U ||
                entry.payload.size() > limits.maximum_entry_bytes ||
                (entry.type == kLeaderNoopEntryType && !entry.payload.empty()))
              return invalid("Raft append entry is noncanonical or exceeds limits");
            expected = entry.index;
          }
        } else if constexpr (std::is_same_v<T, AppendEntriesResponse>) {
          if (value.success && (value.conflict_term.has_value() || value.conflict_index != 0U))
            return invalid("successful Raft append response carries conflict state");
          if (value.conflict_term.has_value() && *value.conflict_term == 0U)
            return invalid("Raft append response conflict term is zero");
        } else if constexpr (std::is_same_v<T, InstallSnapshotRequest>) {
          if (value.leader_id != envelope.source)
            return invalid("Raft snapshot leader disagrees with the transport source");
          common::Status snapshot =
              validate_snapshot(value.snapshot, limits.maximum_snapshot_voters);
          if (!snapshot.is_ok())
            return snapshot;
        } else if constexpr (std::is_same_v<T, ReadBarrierRequest>) {
          if (value.leader_id != envelope.source || value.context == 0U)
            return invalid("Raft read barrier request identity is invalid");
        } else if constexpr (std::is_same_v<T, ReadBarrierResponse>) {
          if (value.context == 0U)
            return invalid("Raft read barrier response context is zero");
        }
        return common::Status::ok();
      },
      envelope.message);
}

[[nodiscard]] std::size_t snapshot_payload_size(const SnapshotMetadata& snapshot) noexcept {
  return 72U + snapshot.voters.size() * sizeof(NodeId);
}

[[nodiscard]] common::Result<std::size_t>
message_payload_size(const Message& message, const RaftTransportCodecLimits& limits) {
  return std::visit(
      [&](const auto& value) -> common::Result<std::size_t> {
        using T = std::remove_cvref_t<decltype(value)>;
        if constexpr (std::is_same_v<T, RequestVoteRequest>) {
          return 32U;
        } else if constexpr (std::is_same_v<T, RequestVoteResponse>) {
          return 16U;
        } else if constexpr (std::is_same_v<T, AppendEntriesRequest>) {
          std::size_t size = 48U;
          for (const LogEntry& entry : value.entries) {
            if (entry.payload.size() > limits.maximum_entry_bytes ||
                size > limits.maximum_frame_bytes - kMaximumEntryHeaderSize ||
                entry.payload.size() > limits.maximum_frame_bytes - size - kMaximumEntryHeaderSize)
              return common::make_unexpected(exhausted("Raft append payload exceeds frame limits"));
            size += kMaximumEntryHeaderSize + entry.payload.size();
          }
          return size;
        } else if constexpr (std::is_same_v<T, AppendEntriesResponse>) {
          return 40U;
        } else if constexpr (std::is_same_v<T, InstallSnapshotRequest>) {
          return 16U + snapshot_payload_size(value.snapshot);
        } else if constexpr (std::is_same_v<T, InstallSnapshotResponse>) {
          return 24U;
        } else if constexpr (std::is_same_v<T, ReadBarrierRequest>) {
          return 24U;
        } else {
          return 24U;
        }
      },
      message);
}

void advance(common::Status& status, common::Status next) {
  if (status.is_ok())
    status = std::move(next);
}

void write_snapshot(common::ByteWriter& writer, common::Status& status,
                    const SnapshotMetadata& snapshot) {
  advance(status, writer.write_u64_le(snapshot.last_included_index));
  advance(status, writer.write_u64_le(snapshot.last_included_term));
  advance(status, writer.write_u64_le(snapshot.manifest_generation));
  advance(status, writer.write_exact(snapshot.part_set_checksum));
  advance(status, writer.write_u64_le(snapshot.configuration_index));
  advance(status, writer.write_u32_le(static_cast<std::uint32_t>(snapshot.voters.size())));
  advance(status, writer.zero_fill(4U));
  for (const NodeId voter : snapshot.voters)
    advance(status, writer.write_u64_le(voter));
}

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_payload(const Message& message, const std::size_t payload_size) {
  std::vector<std::byte> payload(payload_size, std::byte{0U});
  common::ByteWriter writer{payload};
  common::Status status = common::Status::ok();
  std::visit(
      [&](const auto& value) {
        using T = std::remove_cvref_t<decltype(value)>;
        advance(status, writer.write_u64_le(value.term));
        if constexpr (std::is_same_v<T, RequestVoteRequest>) {
          advance(status, writer.write_u64_le(value.candidate_id));
          advance(status, writer.write_u64_le(value.last_log_index));
          advance(status, writer.write_u64_le(value.last_log_term));
        } else if constexpr (std::is_same_v<T, RequestVoteResponse>) {
          advance(status, writer.write_u8(value.granted ? 1U : 0U));
          advance(status, writer.zero_fill(7U));
        } else if constexpr (std::is_same_v<T, AppendEntriesRequest>) {
          advance(status, writer.write_u64_le(value.leader_id));
          advance(status, writer.write_u64_le(value.previous_log_index));
          advance(status, writer.write_u64_le(value.previous_log_term));
          advance(status, writer.write_u64_le(value.leader_commit));
          advance(status, writer.write_u32_le(static_cast<std::uint32_t>(value.entries.size())));
          advance(status, writer.zero_fill(4U));
          for (const LogEntry& entry : value.entries) {
            advance(status, writer.write_u64_le(entry.index));
            advance(status, writer.write_u64_le(entry.term));
            advance(status, writer.write_u8(entry.type));
            advance(status, writer.zero_fill(3U));
            advance(status, writer.write_u32_le(static_cast<std::uint32_t>(entry.payload.size())));
            advance(status, writer.write_exact(entry.payload));
          }
        } else if constexpr (std::is_same_v<T, AppendEntriesResponse>) {
          advance(status, writer.write_u8(value.success ? 1U : 0U));
          advance(status, writer.write_u8(value.conflict_term.has_value() ? 1U : 0U));
          advance(status, writer.zero_fill(6U));
          advance(status, writer.write_u64_le(value.match_index));
          advance(status, writer.write_u64_le(value.conflict_term.value_or(0U)));
          advance(status, writer.write_u64_le(value.conflict_index));
        } else if constexpr (std::is_same_v<T, InstallSnapshotRequest>) {
          advance(status, writer.write_u64_le(value.leader_id));
          write_snapshot(writer, status, value.snapshot);
        } else if constexpr (std::is_same_v<T, InstallSnapshotResponse>) {
          advance(status, writer.write_u8(value.success ? 1U : 0U));
          advance(status, writer.zero_fill(7U));
          advance(status, writer.write_u64_le(value.last_included_index));
        } else if constexpr (std::is_same_v<T, ReadBarrierRequest>) {
          advance(status, writer.write_u64_le(value.leader_id));
          advance(status, writer.write_u64_le(value.context));
        } else if constexpr (std::is_same_v<T, ReadBarrierResponse>) {
          advance(status, writer.write_u64_le(value.context));
          advance(status, writer.write_u8(value.accepted ? 1U : 0U));
          advance(status, writer.zero_fill(7U));
        }
      },
      message);
  if (!status.is_ok() || !writer.full())
    return common::make_unexpected(corruption("Raft transport payload size calculation disagrees"));
  return payload;
}

[[nodiscard]] common::Result<common::Uuid> read_uuid(common::ByteReader& reader) {
  auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value())
    return common::make_unexpected(corruption("Raft transport UUID is truncated"));
  common::Uuid::Bytes owned{};
  std::ranges::copy(*bytes, owned.begin());
  return common::Uuid{owned};
}

[[nodiscard]] common::Result<SnapshotMetadata>
decode_snapshot(common::ByteReader& reader, const RaftTransportCodecLimits& limits) {
  auto index = reader.read_u64_le();
  auto term = reader.read_u64_le();
  auto generation = reader.read_u64_le();
  auto checksum = reader.read_exact(32U);
  auto configuration = reader.read_u64_le();
  auto voter_count = reader.read_u32_le();
  auto reserved = reader.read_exact(4U);
  if (!index.has_value() || !term.has_value() || !generation.has_value() || !checksum.has_value() ||
      !configuration.has_value() || !voter_count.has_value() || !reserved.has_value() ||
      !zero_bytes(*reserved))
    return common::make_unexpected(corruption("Raft transport snapshot header is invalid"));
  if (*voter_count > limits.maximum_snapshot_voters ||
      *voter_count > reader.remaining() / sizeof(NodeId))
    return common::make_unexpected(exhausted("Raft transport snapshot voter count exceeds limits"));
  SnapshotMetadata snapshot;
  snapshot.last_included_index = *index;
  snapshot.last_included_term = *term;
  snapshot.manifest_generation = *generation;
  std::ranges::copy(*checksum, snapshot.part_set_checksum.begin());
  snapshot.configuration_index = *configuration;
  snapshot.voters.reserve(*voter_count);
  for (std::uint32_t ordinal = 0U; ordinal < *voter_count; ++ordinal) {
    auto voter = reader.read_u64_le();
    if (!voter.has_value())
      return common::make_unexpected(corruption("Raft transport snapshot voters are truncated"));
    snapshot.voters.push_back(*voter);
  }
  common::Status valid = validate_snapshot(snapshot, limits.maximum_snapshot_voters);
  if (!valid.is_ok())
    return common::make_unexpected(corruption(valid.message().c_str()));
  return snapshot;
}

[[nodiscard]] common::Result<Message> decode_payload(const MessageKind kind,
                                                     const common::ByteView payload,
                                                     const RaftTransportCodecLimits& limits) {
  common::ByteReader reader{payload};
  auto term = reader.read_u64_le();
  if (!term.has_value())
    return common::make_unexpected(corruption("Raft transport message term is truncated"));
  Message message;
  switch (kind) {
  case MessageKind::kRequestVoteRequest: {
    auto candidate = reader.read_u64_le();
    auto index = reader.read_u64_le();
    auto last_term = reader.read_u64_le();
    if (!candidate.has_value() || !index.has_value() || !last_term.has_value())
      return common::make_unexpected(corruption("Raft vote request is truncated"));
    message = RequestVoteRequest{*term, *candidate, *index, *last_term};
    break;
  }
  case MessageKind::kRequestVoteResponse: {
    auto granted = reader.read_u8();
    auto reserved = reader.read_exact(7U);
    if (!granted.has_value() || *granted > 1U || !reserved.has_value() || !zero_bytes(*reserved))
      return common::make_unexpected(corruption("Raft vote response is noncanonical"));
    message = RequestVoteResponse{*term, *granted == 1U};
    break;
  }
  case MessageKind::kAppendEntriesRequest: {
    auto leader = reader.read_u64_le();
    auto previous_index = reader.read_u64_le();
    auto previous_term = reader.read_u64_le();
    auto commit = reader.read_u64_le();
    auto count = reader.read_u32_le();
    auto reserved = reader.read_exact(4U);
    if (!leader.has_value() || !previous_index.has_value() || !previous_term.has_value() ||
        !commit.has_value() || !count.has_value() || !reserved.has_value() ||
        !zero_bytes(*reserved))
      return common::make_unexpected(corruption("Raft append request header is invalid"));
    if (*count > limits.maximum_append_entries ||
        *count > reader.remaining() / kMaximumEntryHeaderSize)
      return common::make_unexpected(exhausted("Raft append entry count exceeds limits"));
    std::vector<LogEntry> entries;
    entries.reserve(*count);
    for (std::uint32_t ordinal = 0U; ordinal < *count; ++ordinal) {
      auto index = reader.read_u64_le();
      auto entry_term = reader.read_u64_le();
      auto type = reader.read_u8();
      auto entry_reserved = reader.read_exact(3U);
      auto size = reader.read_u32_le();
      if (!index.has_value() || !entry_term.has_value() || !type.has_value() ||
          !entry_reserved.has_value() || !zero_bytes(*entry_reserved) || !size.has_value())
        return common::make_unexpected(corruption("Raft append entry header is invalid"));
      if (*size > limits.maximum_entry_bytes || *size > reader.remaining())
        return common::make_unexpected(exhausted("Raft append entry payload exceeds limits"));
      auto bytes = reader.read_exact(*size);
      if (!bytes.has_value())
        return common::make_unexpected(corruption("Raft append entry payload is truncated"));
      entries.push_back({*index, *entry_term, *type, {bytes->begin(), bytes->end()}});
    }
    message = AppendEntriesRequest{
        *term, *leader, *previous_index, *previous_term, std::move(entries), *commit};
    break;
  }
  case MessageKind::kAppendEntriesResponse: {
    auto success = reader.read_u8();
    auto has_conflict = reader.read_u8();
    auto reserved = reader.read_exact(6U);
    auto match = reader.read_u64_le();
    auto conflict_term = reader.read_u64_le();
    auto conflict_index = reader.read_u64_le();
    if (!success.has_value() || *success > 1U || !has_conflict.has_value() || *has_conflict > 1U ||
        !reserved.has_value() || !zero_bytes(*reserved) || !match.has_value() ||
        !conflict_term.has_value() || !conflict_index.has_value() ||
        (*has_conflict == 0U && *conflict_term != 0U))
      return common::make_unexpected(corruption("Raft append response is noncanonical"));
    message = AppendEntriesResponse{
        *term, *success == 1U, *match,
        *has_conflict == 1U ? std::optional<Term>{*conflict_term} : std::nullopt, *conflict_index};
    break;
  }
  case MessageKind::kInstallSnapshotRequest: {
    auto leader = reader.read_u64_le();
    if (!leader.has_value())
      return common::make_unexpected(corruption("Raft snapshot request leader is truncated"));
    auto snapshot = decode_snapshot(reader, limits);
    if (!snapshot.has_value())
      return common::make_unexpected(snapshot.error());
    message = InstallSnapshotRequest{*term, *leader, std::move(*snapshot)};
    break;
  }
  case MessageKind::kInstallSnapshotResponse: {
    auto success = reader.read_u8();
    auto reserved = reader.read_exact(7U);
    auto index = reader.read_u64_le();
    if (!success.has_value() || *success > 1U || !reserved.has_value() || !zero_bytes(*reserved) ||
        !index.has_value())
      return common::make_unexpected(corruption("Raft snapshot response is noncanonical"));
    message = InstallSnapshotResponse{*term, *success == 1U, *index};
    break;
  }
  case MessageKind::kReadBarrierRequest: {
    auto leader = reader.read_u64_le();
    auto context = reader.read_u64_le();
    if (!leader.has_value() || !context.has_value())
      return common::make_unexpected(corruption("Raft read barrier request is truncated"));
    message = ReadBarrierRequest{*term, *leader, *context};
    break;
  }
  case MessageKind::kReadBarrierResponse: {
    auto context = reader.read_u64_le();
    auto accepted = reader.read_u8();
    auto reserved = reader.read_exact(7U);
    if (!context.has_value() || !accepted.has_value() || *accepted > 1U || !reserved.has_value() ||
        !zero_bytes(*reserved))
      return common::make_unexpected(corruption("Raft read barrier response is noncanonical"));
    message = ReadBarrierResponse{*term, *context, *accepted == 1U};
    break;
  }
  }
  if (!reader.empty())
    return common::make_unexpected(corruption("Raft transport message has trailing bytes"));
  return message;
}

} // namespace

common::Result<std::vector<std::byte>>
encode_raft_transport_envelope_v1(const RaftTransportEnvelope& envelope,
                                  const RaftTransportCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("Raft transport codec limits are invalid"));
  common::Status valid = validate_message(envelope, limits);
  if (!valid.is_ok())
    return common::make_unexpected(std::move(valid));
  try {
    auto size = message_payload_size(envelope.message, limits);
    if (!size.has_value())
      return common::make_unexpected(size.error());
    constexpr std::size_t kFraming = kRaftTransportHeaderSize + kRaftTransportTrailerSize;
    if (*size > limits.maximum_frame_bytes - kFraming)
      return common::make_unexpected(exhausted("Raft transport frame exceeds its byte limit"));
    auto payload = encode_payload(envelope.message, *size);
    if (!payload.has_value())
      return common::make_unexpected(payload.error());
    std::vector<std::byte> frame(kFraming + payload->size(), std::byte{0U});
    common::ByteWriter writer{frame};
    common::Status status = common::Status::ok();
    advance(status, writer.write_exact(kMagic));
    advance(status, writer.write_u16_le(kMajor));
    advance(status, writer.write_u16_le(kMinor));
    advance(status, writer.write_u32_le(kRaftTransportHeaderSize));
    advance(status, writer.write_u64_le(frame.size()));
    advance(status, writer.write_exact(envelope.group_id.bytes()));
    advance(status, writer.write_u64_le(envelope.source));
    advance(status, writer.write_u64_le(envelope.destination));
    advance(status, writer.write_u8(static_cast<std::uint8_t>(message_kind(envelope.message))));
    advance(status, writer.write_u8(0U));
    advance(status, writer.zero_fill(6U));
    advance(status, writer.write_u64_le(payload->size()));
    advance(status, writer.write_u32_le(common::crc32c(*payload)));
    advance(status, writer.write_u32_le(0U));
    advance(status, writer.zero_fill(16U));
    if (!status.is_ok() || writer.offset() != kRaftTransportHeaderSize)
      return common::make_unexpected(
          corruption("Raft transport header size calculation disagrees"));
    store_u32(frame, kHeaderCrcOffset,
              header_crc(common::ByteView{frame}.first(kRaftTransportHeaderSize)));
    std::ranges::copy(*payload,
                      frame.begin() + static_cast<std::ptrdiff_t>(kRaftTransportHeaderSize));
    store_u32(
        frame, frame.size() - kRaftTransportTrailerSize,
        common::crc32c(common::ByteView{frame}.first(frame.size() - kRaftTransportTrailerSize)));
    return frame;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft transport encoding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft transport encoding exceeds container limits"));
  }
}

common::Result<RaftTransportEnvelope>
decode_raft_transport_envelope_v1(const common::ByteView bytes,
                                  const RaftTransportCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("Raft transport codec limits are invalid"));
  constexpr std::size_t kFraming = kRaftTransportHeaderSize + kRaftTransportTrailerSize;
  if (bytes.size() < kFraming)
    return common::make_unexpected(corruption("Raft transport frame is truncated"));
  if (bytes.size() > limits.maximum_frame_bytes)
    return common::make_unexpected(exhausted("Raft transport frame exceeds its byte limit"));
  const common::ByteView header = bytes.first(kRaftTransportHeaderSize);
  if (!std::ranges::equal(header.first(kMagic.size()), kMagic))
    return common::make_unexpected(corruption("Raft transport magic is invalid"));
  if (load_u32(header, kHeaderCrcOffset) != header_crc(header))
    return common::make_unexpected(corruption("Raft transport header checksum mismatch"));
  if (load_u16(header, 8U) != kMajor || load_u16(header, 10U) != kMinor)
    return common::make_unexpected(unsupported("Raft transport version is unsupported"));
  if (load_u32(header, 12U) != kRaftTransportHeaderSize || load_u64(header, 16U) != bytes.size())
    return common::make_unexpected(corruption("Raft transport frame lengths are inconsistent"));
  if (header[57U] != std::byte{0U} || !zero_bytes(header.subspan(58U, 6U)) ||
      !zero_bytes(header.subspan(80U, 16U)))
    return common::make_unexpected(corruption("Raft transport reserved bytes are nonzero"));
  const std::uint8_t raw_kind = std::to_integer<std::uint8_t>(header[56U]);
  if (raw_kind < static_cast<std::uint8_t>(MessageKind::kRequestVoteRequest) ||
      raw_kind > static_cast<std::uint8_t>(MessageKind::kReadBarrierResponse))
    return common::make_unexpected(unsupported("Raft transport message kind is unsupported"));
  const std::uint64_t declared_payload = load_u64(header, 64U);
  if (declared_payload != bytes.size() - kFraming)
    return common::make_unexpected(corruption("Raft transport payload length is inconsistent"));
  const common::ByteView payload =
      bytes.subspan(kRaftTransportHeaderSize, static_cast<std::size_t>(declared_payload));
  if (load_u32(header, 72U) != common::crc32c(payload) ||
      load_u32(bytes, bytes.size() - kRaftTransportTrailerSize) !=
          common::crc32c(bytes.first(bytes.size() - kRaftTransportTrailerSize)))
    return common::make_unexpected(corruption("Raft transport frame checksum mismatch"));
  try {
    common::ByteReader header_reader{header.subspan(24U, 32U)};
    auto group_id = read_uuid(header_reader);
    auto source = header_reader.read_u64_le();
    auto destination = header_reader.read_u64_le();
    if (!group_id.has_value() || !source.has_value() || !destination.has_value() ||
        !header_reader.empty())
      return common::make_unexpected(corruption("Raft transport route is invalid"));
    auto message = decode_payload(static_cast<MessageKind>(raw_kind), payload, limits);
    if (!message.has_value())
      return common::make_unexpected(message.error());
    RaftTransportEnvelope envelope{*group_id, *source, *destination, std::move(*message)};
    common::Status valid = validate_message(envelope, limits);
    if (!valid.is_ok())
      return common::make_unexpected(corruption(valid.message().c_str()));
    return envelope;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft transport decoding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft transport decoding exceeds container limits"));
  }
}

} // namespace chronos::raft
