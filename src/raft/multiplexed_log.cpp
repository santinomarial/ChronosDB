#include "chronos/raft/multiplexed_log.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/raft/membership.hpp"
#include "chronos/raft/persistent_state_budget.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{
    std::byte{0x43}, std::byte{0x48}, std::byte{0x52}, std::byte{0x4e},
    std::byte{0x4d}, std::byte{0x52}, std::byte{0x4c}, std::byte{0x00},
};
inline constexpr std::size_t kStateFixedSizeV1_0 = 96U;
inline constexpr std::size_t kStateFixedSizeV1_1 = kRaftPersistentStateFixedSizeV1;
inline constexpr std::size_t kEntryFixedSize = kRaftPersistentLogEntryFixedSizeV1;
static_assert(kMaximumRaftPersistentStatePayloadSize == kMaximumMultiplexedLogRecordSize -
                                                            kMultiplexedLogHeaderSize -
                                                            kMultiplexedLogTrailerSize);

[[nodiscard]] common::Status corrupt(const char* message) {
  return common::Status{common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Result<std::size_t> payload_size(const PersistentState& state) {
  return raft_persistent_state_payload_size(state.snapshot.voters.size(), state.log);
}

[[nodiscard]] common::Status write_state(common::ByteWriter& writer, const PersistentState& state) {
  common::Status status = writer.write_u64_le(state.current_term);
  if (status.is_ok())
    status = writer.write_u64_le(state.voted_for.value_or(0U));
  if (status.is_ok())
    status = writer.write_u64_le(state.commit_index);
  if (status.is_ok())
    status = writer.write_u64_le(state.applied_index);
  if (status.is_ok())
    status = writer.write_u64_le(state.snapshot.last_included_index);
  if (status.is_ok())
    status = writer.write_u64_le(state.snapshot.last_included_term);
  if (status.is_ok())
    status = writer.write_u64_le(state.snapshot.manifest_generation);
  if (status.is_ok())
    status = writer.write_exact(state.snapshot.part_set_checksum);
  if (status.is_ok())
    status = writer.write_u64_le(state.snapshot.configuration_index);
  if (status.is_ok())
    status = writer.write_u32_le(static_cast<std::uint32_t>(state.snapshot.voters.size()));
  if (status.is_ok())
    status = writer.write_u32_le(0U);
  for (const NodeId voter : state.snapshot.voters) {
    if (status.is_ok())
      status = writer.write_u64_le(voter);
  }
  if (status.is_ok())
    status = writer.write_u32_le(static_cast<std::uint32_t>(state.log.size()));
  if (status.is_ok())
    status = writer.write_u32_le(0U);
  for (const LogEntry& entry : state.log) {
    if (!status.is_ok())
      break;
    status = writer.write_u64_le(entry.index);
    if (status.is_ok())
      status = writer.write_u64_le(entry.term);
    if (status.is_ok())
      status = writer.write_u8(entry.type);
    if (status.is_ok())
      status = writer.zero_fill(7U);
    if (status.is_ok())
      status = writer.write_u32_le(static_cast<std::uint32_t>(entry.payload.size()));
    if (status.is_ok())
      status = writer.write_u32_le(0U);
    if (status.is_ok())
      status = writer.write_exact(entry.payload);
  }
  return status;
}

} // namespace

common::Result<MultiplexedLogRecordHeader>
inspect_multiplexed_log_record_header_v1(const common::ByteView encoded_header) {
  if (encoded_header.size() != kMultiplexedLogHeaderSize) {
    return common::make_unexpected(corrupt("multiplexed log header size is invalid"));
  }
  std::array<std::byte, kMultiplexedLogHeaderSize> checked_header{};
  std::copy(encoded_header.begin(), encoded_header.end(), checked_header.begin());
  common::ByteReader crc_reader{common::ByteView{checked_header}.subspan(52U, 4U)};
  auto stored_header_crc = crc_reader.read_u32_le();
  std::fill(checked_header.begin() + 52, checked_header.begin() + 56, std::byte{0});
  if (!stored_header_crc.has_value() || common::crc32c(checked_header) != *stored_header_crc) {
    return common::make_unexpected(corrupt("multiplexed log header checksum mismatch"));
  }

  common::ByteReader header{encoded_header};
  auto magic = header.read_exact(8U);
  auto major = header.read_u16_le();
  auto minor = header.read_u16_le();
  auto header_size = header.read_u32_le();
  auto total_size = header.read_u32_le();
  auto payload_length = header.read_u32_le();
  auto physical_sequence = header.read_u64_le();
  auto group_bytes = header.read_exact(common::Uuid::kSize);
  auto payload_crc = header.read_u32_le();
  auto ignored_header_crc = header.read_u32_le();
  auto reserved = header.read_exact(8U);
  static_cast<void>(payload_crc);
  static_cast<void>(ignored_header_crc);
  if (!magic || !major || !minor || !header_size || !total_size || !payload_length ||
      !physical_sequence || !group_bytes || !reserved || !std::ranges::equal(*magic, kMagic)) {
    return common::make_unexpected(corrupt("multiplexed log header is invalid"));
  }
  if (*major != kMultiplexedLogFormatMajor || *minor > kMultiplexedLogFormatMinor) {
    return common::make_unexpected(common::Status{common::StatusCode::kNotSupported,
                                                  "multiplexed log version is unsupported"});
  }
  const std::uint64_t minimum_size = kMultiplexedLogHeaderSize + kMultiplexedLogTrailerSize;
  if (*header_size != kMultiplexedLogHeaderSize || *physical_sequence == 0U ||
      *total_size < minimum_size || *total_size > kMaximumMultiplexedLogRecordSize ||
      *payload_length != *total_size - minimum_size ||
      std::ranges::any_of(*reserved, [](const std::byte value) { return value != std::byte{0}; })) {
    return common::make_unexpected(corrupt("multiplexed log layout or reserved bytes invalid"));
  }
  common::Uuid::Bytes group_array{};
  std::copy(group_bytes->begin(), group_bytes->end(), group_array.begin());
  const GroupId group_id{group_array};
  if (group_id.is_nil()) {
    return common::make_unexpected(corrupt("multiplexed log group is nil"));
  }
  return MultiplexedLogRecordHeader{*total_size, *payload_length, *physical_sequence, group_id,
                                    *minor};
}

common::Result<std::vector<std::byte>>
encode_multiplexed_log_record_v1(const GroupPersistentState& persistent) {
  if (persistent.group_id.is_nil() || persistent.physical_sequence == 0U ||
      persistent.state.log.size() > std::numeric_limits<std::uint32_t>::max() ||
      persistent.state.snapshot.voters.size() > kMaximumMembershipVoters) {
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                  "multiplexed log identity or state is invalid"});
  }
  auto payload = payload_size(persistent.state);
  if (!payload.has_value() || *payload > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(payload.has_value()
                                       ? common::Status{common::StatusCode::kResourceExhausted,
                                                        "multiplexed log payload is too large"}
                                       : payload.error());
  }
  auto total = common::checked_add(kMultiplexedLogHeaderSize, *payload);
  if (total.has_value())
    total = common::checked_add(*total, kMultiplexedLogTrailerSize);
  if (!total.has_value() || *total > kMaximumMultiplexedLogRecordSize ||
      *total > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "multiplexed log record is too large"});
  }

  std::vector<std::byte> encoded(*total);
  common::ByteWriter payload_writer{
      common::MutableByteView{encoded.data() + kMultiplexedLogHeaderSize, *payload}};
  common::Status status = write_state(payload_writer, persistent.state);
  if (!status.is_ok() || !payload_writer.full()) {
    return common::make_unexpected(status.is_ok()
                                       ? common::Status{common::StatusCode::kInternal,
                                                        "multiplexed log payload layout mismatch"}
                                       : std::move(status));
  }
  const std::uint32_t payload_crc =
      common::crc32c(common::ByteView{encoded.data() + kMultiplexedLogHeaderSize, *payload});

  common::ByteWriter header{common::MutableByteView{encoded.data(), kMultiplexedLogHeaderSize}};
  status = header.write_exact(kMagic);
  if (status.is_ok())
    status = header.write_u16_le(kMultiplexedLogFormatMajor);
  if (status.is_ok())
    status = header.write_u16_le(kMultiplexedLogFormatMinor);
  if (status.is_ok())
    status = header.write_u32_le(static_cast<std::uint32_t>(kMultiplexedLogHeaderSize));
  if (status.is_ok())
    status = header.write_u32_le(static_cast<std::uint32_t>(*total));
  if (status.is_ok())
    status = header.write_u32_le(static_cast<std::uint32_t>(*payload));
  if (status.is_ok())
    status = header.write_u64_le(persistent.physical_sequence);
  if (status.is_ok())
    status = header.write_exact(persistent.group_id.bytes());
  if (status.is_ok())
    status = header.write_u32_le(payload_crc);
  if (status.is_ok())
    status = header.write_u32_le(0U);
  if (status.is_ok())
    status = header.zero_fill(8U);
  if (!status.is_ok() || !header.full()) {
    return common::make_unexpected(status.is_ok()
                                       ? common::Status{common::StatusCode::kInternal,
                                                        "multiplexed log header layout mismatch"}
                                       : std::move(status));
  }
  const std::uint32_t header_crc = common::crc32c(common::ByteView{encoded.data(), 64U});
  common::ByteWriter header_crc_writer{common::MutableByteView{encoded.data() + 52U, 4U}};
  status = header_crc_writer.write_u32_le(header_crc);
  if (!status.is_ok())
    return common::make_unexpected(status);
  const std::uint32_t record_crc =
      common::crc32c(common::ByteView{encoded.data(), kMultiplexedLogHeaderSize + *payload});
  common::ByteWriter trailer{common::MutableByteView{
      encoded.data() + kMultiplexedLogHeaderSize + *payload, kMultiplexedLogTrailerSize}};
  status = trailer.write_u32_le(record_crc);
  if (!status.is_ok())
    return common::make_unexpected(status);
  return encoded;
}

common::Result<DecodedGroupPersistentState>
decode_multiplexed_log_record_v1(const common::ByteView encoded) {
  if (encoded.size() < kMultiplexedLogHeaderSize + kMultiplexedLogTrailerSize) {
    return common::make_unexpected(corrupt("multiplexed log record is truncated"));
  }
  auto inspected =
      inspect_multiplexed_log_record_header_v1(encoded.first(kMultiplexedLogHeaderSize));
  if (!inspected.has_value()) {
    return common::make_unexpected(inspected.error());
  }
  if (inspected->encoded_size != encoded.size()) {
    return common::make_unexpected(corrupt("multiplexed log record size does not match header"));
  }
  common::ByteReader crc_fields{encoded.subspan(48U, 4U)};
  auto payload_crc = crc_fields.read_u32_le();
  if (!payload_crc.has_value()) {
    return common::make_unexpected(corrupt("multiplexed log payload checksum is truncated"));
  }
  const common::ByteView payload =
      encoded.subspan(kMultiplexedLogHeaderSize, inspected->payload_size);
  const std::size_t minimum_payload =
      inspected->format_minor == 0U ? kStateFixedSizeV1_0 : kStateFixedSizeV1_1;
  if (payload.size() < minimum_payload)
    return common::make_unexpected(corrupt("multiplexed log state prefix is truncated"));
  if (common::crc32c(payload) != *payload_crc) {
    return common::make_unexpected(corrupt("multiplexed log payload checksum mismatch"));
  }
  common::ByteReader trailer{encoded.last(kMultiplexedLogTrailerSize)};
  auto record_crc = trailer.read_u32_le();
  if (!record_crc ||
      common::crc32c(encoded.first(encoded.size() - kMultiplexedLogTrailerSize)) != *record_crc) {
    return common::make_unexpected(corrupt("multiplexed log record checksum mismatch"));
  }

  common::ByteReader reader{payload};
  auto term = reader.read_u64_le();
  auto voted = reader.read_u64_le();
  auto commit = reader.read_u64_le();
  auto applied = reader.read_u64_le();
  auto snapshot_index = reader.read_u64_le();
  auto snapshot_term = reader.read_u64_le();
  auto manifest_generation = reader.read_u64_le();
  auto checksum = reader.read_exact(32U);
  std::uint64_t configuration_index = 0U;
  std::uint32_t voter_count = 0U;
  if (inspected->format_minor >= 1U) {
    auto decoded_configuration_index = reader.read_u64_le();
    auto decoded_voter_count = reader.read_u32_le();
    auto snapshot_reserved = reader.read_u32_le();
    if (!decoded_configuration_index || !decoded_voter_count || !snapshot_reserved ||
        *snapshot_reserved != 0U) {
      return common::make_unexpected(corrupt("multiplexed log snapshot checkpoint is invalid"));
    }
    configuration_index = *decoded_configuration_index;
    voter_count = *decoded_voter_count;
  }
  if (voter_count > kMaximumMembershipVoters || voter_count > reader.remaining() / sizeof(NodeId)) {
    return common::make_unexpected(corrupt("multiplexed log snapshot voter count is invalid"));
  }
  std::vector<NodeId> snapshot_voters;
  snapshot_voters.reserve(voter_count);
  for (std::uint32_t index = 0U; index < voter_count; ++index) {
    auto voter = reader.read_u64_le();
    if (!voter)
      return common::make_unexpected(corrupt("multiplexed log snapshot voters are truncated"));
    snapshot_voters.push_back(*voter);
  }
  auto log_count = reader.read_u32_le();
  auto state_reserved = reader.read_u32_le();
  if (!term || !voted || !commit || !applied || !snapshot_index || !snapshot_term ||
      !manifest_generation || !checksum || !log_count || !state_reserved || *state_reserved != 0U) {
    return common::make_unexpected(corrupt("multiplexed log persistent state is truncated"));
  }
  PersistentState state{};
  state.current_term = *term;
  if (*voted != 0U)
    state.voted_for = *voted;
  state.commit_index = *commit;
  state.applied_index = *applied;
  state.snapshot.last_included_index = *snapshot_index;
  state.snapshot.last_included_term = *snapshot_term;
  state.snapshot.manifest_generation = *manifest_generation;
  std::copy(checksum->begin(), checksum->end(), state.snapshot.part_set_checksum.begin());
  state.snapshot.configuration_index = configuration_index;
  state.snapshot.voters = std::move(snapshot_voters);
  if (*log_count > reader.remaining() / kEntryFixedSize) {
    return common::make_unexpected(corrupt("multiplexed log entry count exceeds payload"));
  }
  state.log.reserve(*log_count);
  for (std::uint32_t index = 0U; index < *log_count; ++index) {
    auto log_index = reader.read_u64_le();
    auto log_term = reader.read_u64_le();
    auto type = reader.read_u8();
    auto entry_reserved = reader.read_exact(7U);
    auto payload_size = reader.read_u32_le();
    auto payload_reserved = reader.read_u32_le();
    if (!log_index || !log_term || !type || !entry_reserved || !payload_size || !payload_reserved ||
        *payload_reserved != 0U || std::ranges::any_of(*entry_reserved, [](const std::byte value) {
          return value != std::byte{0};
        })) {
      return common::make_unexpected(corrupt("multiplexed log entry header is invalid"));
    }
    auto entry_payload = reader.read_exact(*payload_size);
    if (!entry_payload)
      return common::make_unexpected(corrupt("multiplexed log entry is truncated"));
    state.log.push_back(
        LogEntry{*log_index, *log_term, *type,
                 std::vector<std::byte>{entry_payload->begin(), entry_payload->end()}});
  }
  if (!reader.empty())
    return common::make_unexpected(corrupt("multiplexed log has trailing payload"));
  return DecodedGroupPersistentState{
      GroupPersistentState{inspected->group_id, inspected->physical_sequence, std::move(state)},
      encoded.size()};
}

} // namespace chronos::raft
