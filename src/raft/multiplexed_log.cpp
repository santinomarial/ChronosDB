#include "chronos/raft/multiplexed_log.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

inline constexpr std::array<std::byte, 8U> kMagic{
    std::byte{0x43}, std::byte{0x48}, std::byte{0x52}, std::byte{0x4e},
    std::byte{0x4d}, std::byte{0x52}, std::byte{0x4c}, std::byte{0x00},
};
inline constexpr std::size_t kStateFixedSize = 96U;
inline constexpr std::size_t kEntryFixedSize = 32U;

[[nodiscard]] common::Status corrupt(const char* message) {
  return common::Status{common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Result<std::size_t> payload_size(const PersistentState& state) {
  std::optional<std::size_t> size{kStateFixedSize};
  for (const LogEntry& entry : state.log) {
    size = common::checked_add(*size, kEntryFixedSize);
    if (size.has_value()) {
      size = common::checked_add(*size, entry.payload.size());
    }
    if (!size.has_value()) {
      return common::make_unexpected(common::Status{common::StatusCode::kOutOfRange,
                                                     "multiplexed log payload size overflows"});
    }
  }
  return *size;
}

[[nodiscard]] common::Status write_state(common::ByteWriter& writer,
                                         const PersistentState& state) {
  common::Status status = writer.write_u64_le(state.current_term);
  if (status.is_ok()) status = writer.write_u64_le(state.voted_for.value_or(0U));
  if (status.is_ok()) status = writer.write_u64_le(state.commit_index);
  if (status.is_ok()) status = writer.write_u64_le(state.applied_index);
  if (status.is_ok()) status = writer.write_u64_le(state.snapshot.last_included_index);
  if (status.is_ok()) status = writer.write_u64_le(state.snapshot.last_included_term);
  if (status.is_ok()) status = writer.write_u64_le(state.snapshot.manifest_generation);
  if (status.is_ok()) status = writer.write_exact(state.snapshot.part_set_checksum);
  if (status.is_ok()) status = writer.write_u32_le(static_cast<std::uint32_t>(state.log.size()));
  if (status.is_ok()) status = writer.write_u32_le(0U);
  for (const LogEntry& entry : state.log) {
    if (!status.is_ok()) break;
    status = writer.write_u64_le(entry.index);
    if (status.is_ok()) status = writer.write_u64_le(entry.term);
    if (status.is_ok()) status = writer.write_u8(entry.type);
    if (status.is_ok()) status = writer.zero_fill(7U);
    if (status.is_ok()) status = writer.write_u32_le(static_cast<std::uint32_t>(entry.payload.size()));
    if (status.is_ok()) status = writer.write_u32_le(0U);
    if (status.is_ok()) status = writer.write_exact(entry.payload);
  }
  return status;
}

} // namespace

common::Result<std::vector<std::byte>>
encode_multiplexed_log_record_v1(const GroupPersistentState& persistent) {
  if (persistent.group_id.is_nil() || persistent.physical_sequence == 0U ||
      persistent.state.log.size() > std::numeric_limits<std::uint32_t>::max()) {
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
  if (total.has_value()) total = common::checked_add(*total, kMultiplexedLogTrailerSize);
  if (!total.has_value() || *total > kMaximumMultiplexedLogRecordSize ||
      *total > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                   "multiplexed log record is too large"});
  }

  std::vector<std::byte> encoded(*total);
  common::ByteWriter payload_writer{common::MutableByteView{
      encoded.data() + kMultiplexedLogHeaderSize, *payload}};
  common::Status status = write_state(payload_writer, persistent.state);
  if (!status.is_ok() || !payload_writer.full()) {
    return common::make_unexpected(status.is_ok()
                                       ? common::Status{common::StatusCode::kInternal,
                                                        "multiplexed log payload layout mismatch"}
                                       : std::move(status));
  }
  const std::uint32_t payload_crc = common::crc32c(common::ByteView{
      encoded.data() + kMultiplexedLogHeaderSize, *payload});

  common::ByteWriter header{common::MutableByteView{encoded.data(), kMultiplexedLogHeaderSize}};
  status = header.write_exact(kMagic);
  if (status.is_ok()) status = header.write_u16_le(kMultiplexedLogFormatMajor);
  if (status.is_ok()) status = header.write_u16_le(kMultiplexedLogFormatMinor);
  if (status.is_ok()) status = header.write_u32_le(static_cast<std::uint32_t>(kMultiplexedLogHeaderSize));
  if (status.is_ok()) status = header.write_u32_le(static_cast<std::uint32_t>(*total));
  if (status.is_ok()) status = header.write_u32_le(static_cast<std::uint32_t>(*payload));
  if (status.is_ok()) status = header.write_u64_le(persistent.physical_sequence);
  if (status.is_ok()) status = header.write_exact(persistent.group_id.bytes());
  if (status.is_ok()) status = header.write_u32_le(payload_crc);
  if (status.is_ok()) status = header.write_u32_le(0U);
  if (status.is_ok()) status = header.zero_fill(8U);
  if (!status.is_ok() || !header.full()) {
    return common::make_unexpected(status.is_ok()
                                       ? common::Status{common::StatusCode::kInternal,
                                                        "multiplexed log header layout mismatch"}
                                       : std::move(status));
  }
  const std::uint32_t header_crc = common::crc32c(common::ByteView{encoded.data(), 64U});
  common::ByteWriter header_crc_writer{common::MutableByteView{encoded.data() + 52U, 4U}};
  status = header_crc_writer.write_u32_le(header_crc);
  if (!status.is_ok()) return common::make_unexpected(status);
  const std::uint32_t record_crc =
      common::crc32c(common::ByteView{encoded.data(), kMultiplexedLogHeaderSize + *payload});
  common::ByteWriter trailer{common::MutableByteView{
      encoded.data() + kMultiplexedLogHeaderSize + *payload, kMultiplexedLogTrailerSize}};
  status = trailer.write_u32_le(record_crc);
  if (!status.is_ok()) return common::make_unexpected(status);
  return encoded;
}

common::Result<DecodedGroupPersistentState>
decode_multiplexed_log_record_v1(const common::ByteView encoded) {
  if (encoded.size() < kMultiplexedLogHeaderSize + kMultiplexedLogTrailerSize) {
    return common::make_unexpected(corrupt("multiplexed log record is truncated"));
  }
  std::array<std::byte, kMultiplexedLogHeaderSize> checked_header{};
  std::copy_n(encoded.begin(), checked_header.size(), checked_header.begin());
  common::ByteReader crc_reader{common::ByteView{checked_header}.subspan(52U, 4U)};
  auto stored_header_crc = crc_reader.read_u32_le();
  std::fill(checked_header.begin() + 52, checked_header.begin() + 56, std::byte{0});
  if (!stored_header_crc.has_value() || common::crc32c(checked_header) != *stored_header_crc) {
    return common::make_unexpected(corrupt("multiplexed log header checksum mismatch"));
  }

  common::ByteReader header{encoded.first(kMultiplexedLogHeaderSize)};
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
  static_cast<void>(ignored_header_crc);
  if (!magic || !major || !minor || !header_size || !total_size || !payload_length ||
      !physical_sequence || !group_bytes || !payload_crc || !reserved ||
      !std::ranges::equal(*magic, kMagic)) {
    return common::make_unexpected(corrupt("multiplexed log header is invalid"));
  }
  if (*major != kMultiplexedLogFormatMajor || *minor > kMultiplexedLogFormatMinor) {
    return common::make_unexpected(common::Status{common::StatusCode::kNotSupported,
                                                   "multiplexed log version is unsupported"});
  }
  if (*header_size != kMultiplexedLogHeaderSize || *physical_sequence == 0U ||
      *total_size != encoded.size() || *total_size > kMaximumMultiplexedLogRecordSize ||
      *payload_length != encoded.size() - kMultiplexedLogHeaderSize - kMultiplexedLogTrailerSize ||
      std::ranges::any_of(*reserved, [](const std::byte value) { return value != std::byte{0}; })) {
    return common::make_unexpected(corrupt("multiplexed log layout or reserved bytes invalid"));
  }
  const common::ByteView payload = encoded.subspan(kMultiplexedLogHeaderSize, *payload_length);
  if (common::crc32c(payload) != *payload_crc) {
    return common::make_unexpected(corrupt("multiplexed log payload checksum mismatch"));
  }
  common::ByteReader trailer{encoded.last(kMultiplexedLogTrailerSize)};
  auto record_crc = trailer.read_u32_le();
  if (!record_crc ||
      common::crc32c(encoded.first(encoded.size() - kMultiplexedLogTrailerSize)) != *record_crc) {
    return common::make_unexpected(corrupt("multiplexed log record checksum mismatch"));
  }

  common::Uuid::Bytes group_array{};
  std::copy(group_bytes->begin(), group_bytes->end(), group_array.begin());
  const GroupId group_id{group_array};
  if (group_id.is_nil()) return common::make_unexpected(corrupt("multiplexed log group is nil"));
  common::ByteReader reader{payload};
  auto term = reader.read_u64_le();
  auto voted = reader.read_u64_le();
  auto commit = reader.read_u64_le();
  auto applied = reader.read_u64_le();
  auto snapshot_index = reader.read_u64_le();
  auto snapshot_term = reader.read_u64_le();
  auto manifest_generation = reader.read_u64_le();
  auto checksum = reader.read_exact(32U);
  auto log_count = reader.read_u32_le();
  auto state_reserved = reader.read_u32_le();
  if (!term || !voted || !commit || !applied || !snapshot_index || !snapshot_term ||
      !manifest_generation || !checksum || !log_count || !state_reserved || *state_reserved != 0U) {
    return common::make_unexpected(corrupt("multiplexed log persistent state is truncated"));
  }
  PersistentState state{};
  state.current_term = *term;
  if (*voted != 0U) state.voted_for = *voted;
  state.commit_index = *commit;
  state.applied_index = *applied;
  state.snapshot.last_included_index = *snapshot_index;
  state.snapshot.last_included_term = *snapshot_term;
  state.snapshot.manifest_generation = *manifest_generation;
  std::copy(checksum->begin(), checksum->end(), state.snapshot.part_set_checksum.begin());
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
        *payload_reserved != 0U ||
        std::ranges::any_of(*entry_reserved,
                            [](const std::byte value) { return value != std::byte{0}; })) {
      return common::make_unexpected(corrupt("multiplexed log entry header is invalid"));
    }
    auto entry_payload = reader.read_exact(*payload_size);
    if (!entry_payload) return common::make_unexpected(corrupt("multiplexed log entry is truncated"));
    state.log.push_back(LogEntry{*log_index, *log_term, *type,
                                 std::vector<std::byte>{entry_payload->begin(), entry_payload->end()}});
  }
  if (!reader.empty()) return common::make_unexpected(corrupt("multiplexed log has trailing payload"));
  return DecodedGroupPersistentState{
      GroupPersistentState{group_id, *physical_sequence, std::move(state)}, encoded.size()};
}

} // namespace chronos::raft
