#include "chronos/common/crc32c.hpp"
#include "chronos/raft/transport_codec.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ranges>
#include <span>
#include <utility>
#include <vector>

namespace {

constexpr std::size_t kHeaderCrcOffset = 76U;
constexpr std::size_t kPayloadCrcOffset = 72U;
constexpr std::size_t kFuzzFrameLimit = 64U * 1024U;

[[nodiscard]] chronos::raft::RaftTransportCodecLimits limits() {
  return {.maximum_frame_bytes = kFuzzFrameLimit,
          .maximum_append_entries = 64U,
          .maximum_entry_bytes = 16U * 1024U,
          .maximum_snapshot_voters = 31U};
}

[[nodiscard]] std::uint8_t input_byte(const chronos::common::ByteView bytes,
                                      const std::size_t offset, const std::uint8_t fallback = 0U) {
  return offset < bytes.size() ? std::to_integer<std::uint8_t>(bytes[offset]) : fallback;
}

[[nodiscard]] chronos::raft::GroupId group_id() {
  chronos::common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{9U};
  return chronos::raft::GroupId{bytes};
}

[[nodiscard]] chronos::raft::SnapshotMetadata snapshot(const chronos::common::ByteView input) {
  chronos::raft::SnapshotMetadata value;
  value.last_included_index = 8U;
  value.last_included_term = 3U;
  value.manifest_generation = 11U;
  value.part_set_checksum.fill(std::byte{0x5a});
  value.configuration_index = 7U;
  const std::size_t voter_count = 1U + static_cast<std::size_t>(input_byte(input, 1U)) % 8U;
  value.voters.reserve(voter_count);
  for (std::size_t ordinal = 0U; ordinal < voter_count; ++ordinal)
    value.voters.push_back(ordinal + 1U);
  return value;
}

[[nodiscard]] chronos::raft::AppendEntriesRequest
append_request(const chronos::common::ByteView input) {
  std::vector<chronos::raft::LogEntry> entries;
  const std::size_t entry_count = static_cast<std::size_t>(input_byte(input, 1U)) % 5U;
  entries.reserve(entry_count);
  for (std::size_t ordinal = 0U; ordinal < entry_count; ++ordinal) {
    const std::size_t payload_size =
        static_cast<std::size_t>(input_byte(input, 2U + ordinal)) % 65U;
    std::vector<std::byte> payload(payload_size, static_cast<std::byte>(0x11U + ordinal));
    entries.push_back({8U + ordinal, 4U, 1U, std::move(payload)});
  }
  return {4U, 1U, 7U, 3U, std::move(entries), 7U};
}

[[nodiscard]] chronos::raft::Message message(const chronos::common::ByteView input) {
  using namespace chronos::raft;
  switch (input_byte(input, 0U) % 8U) {
  case 0U:
    return RequestVoteRequest{4U, 1U, 7U, 3U};
  case 1U:
    return RequestVoteResponse{4U, (input_byte(input, 1U) & 1U) != 0U};
  case 2U:
    return append_request(input);
  case 3U:
    return AppendEntriesResponse{4U, false, 7U, 3U, 8U};
  case 4U:
    return InstallSnapshotRequest{4U, 1U, snapshot(input)};
  case 5U:
    return InstallSnapshotResponse{4U, true, 8U};
  case 6U:
    return ReadBarrierRequest{4U, 1U, 19U};
  case 7U:
    return ReadBarrierResponse{4U, 19U, (input_byte(input, 1U) & 1U) != 0U};
  }
  std::abort();
}

[[nodiscard]] chronos::raft::RaftTransportEnvelope envelope(const chronos::common::ByteView input) {
  return {group_id(), 1U, 2U, message(input)};
}

void store_u32(const std::span<std::byte> bytes, const std::size_t offset,
               const std::uint32_t value) {
  for (std::size_t ordinal = 0U; ordinal < sizeof(value); ++ordinal)
    bytes[offset + ordinal] = static_cast<std::byte>(value >> (ordinal * 8U));
}

void repair_header_crc(std::vector<std::byte>& frame) {
  if (frame.size() < chronos::raft::kRaftTransportHeaderSize)
    return;
  std::array<std::byte, chronos::raft::kRaftTransportHeaderSize> header{};
  std::ranges::copy_n(frame.begin(), header.size(), header.begin());
  std::fill_n(header.begin() + static_cast<std::ptrdiff_t>(kHeaderCrcOffset), 4U, std::byte{0U});
  store_u32(frame, kHeaderCrcOffset, chronos::common::crc32c(header));
}

void repair_frame_crc(std::vector<std::byte>& frame) {
  if (frame.size() < chronos::raft::kRaftTransportTrailerSize)
    return;
  store_u32(frame, frame.size() - chronos::raft::kRaftTransportTrailerSize,
            chronos::common::crc32c(chronos::common::ByteView{frame}.first(
                frame.size() - chronos::raft::kRaftTransportTrailerSize)));
}

void repair_all_checksums(std::vector<std::byte>& frame) {
  constexpr std::size_t kFraming =
      chronos::raft::kRaftTransportHeaderSize + chronos::raft::kRaftTransportTrailerSize;
  if (frame.size() < kFraming)
    return;
  store_u32(frame, kPayloadCrcOffset,
            chronos::common::crc32c(chronos::common::ByteView{frame}.subspan(
                chronos::raft::kRaftTransportHeaderSize, frame.size() - kFraming)));
  repair_header_crc(frame);
  repair_frame_crc(frame);
}

void verify_envelope_prefix(const chronos::raft::RaftTransportEnvelope& decoded,
                            const chronos::common::ByteView prefix) {
  const auto encoded = chronos::raft::encode_raft_transport_envelope_v1(decoded, limits());
  if (!encoded.has_value() || !std::ranges::equal(*encoded, prefix))
    std::abort();
}

void verify_sticky_failure(chronos::raft::RaftTransportFrameReader& reader,
                           const chronos::common::Status& failure) {
  const auto sticky = reader.consume({});
  if (sticky.has_value() || sticky.error() != failure)
    std::abort();
}

void exercise_reader(const chronos::common::ByteView bytes, const std::size_t split) {
  auto created = chronos::raft::RaftTransportFrameReader::create(limits());
  if (!created.has_value())
    std::abort();
  chronos::raft::RaftTransportFrameReader reader = std::move(*created);
  const auto first = reader.consume(bytes.first(split));
  if (!first.has_value()) {
    verify_sticky_failure(reader, first.error());
    return;
  }
  if (first->envelope.has_value()) {
    if (first->consumed_bytes > split)
      std::abort();
    verify_envelope_prefix(*first->envelope, bytes.first(first->consumed_bytes));
    return;
  }
  if (first->consumed_bytes != split)
    std::abort();

  const auto second = reader.consume(bytes.subspan(split));
  if (!second.has_value()) {
    verify_sticky_failure(reader, second.error());
    return;
  }
  if (second->envelope.has_value()) {
    const std::size_t consumed = split + second->consumed_bytes;
    if (consumed > bytes.size())
      std::abort();
    verify_envelope_prefix(*second->envelope, bytes.first(consumed));
  }
}

void exercise(const chronos::common::ByteView bytes, const std::uint8_t split_selector) {
  const auto decoded = chronos::raft::decode_raft_transport_envelope_v1(bytes, limits());
  if (decoded.has_value()) {
    verify_envelope_prefix(*decoded, bytes);
    if (bytes.size() < chronos::raft::kRaftTransportHeaderSize)
      std::abort();
    const auto frame_length = chronos::raft::raft_transport_frame_length_v1(
        bytes.first(chronos::raft::kRaftTransportHeaderSize), limits());
    if (!frame_length.has_value() || *frame_length != bytes.size())
      std::abort();
    std::vector<std::byte> owned{bytes.begin(), bytes.end()};
    const auto cursor =
        chronos::raft::RaftTransportFrameWriteCursor::create(std::move(owned), limits());
    if (!cursor.has_value() || !std::ranges::equal(cursor->pending_write(), bytes))
      std::abort();
  }

  if (bytes.size() >= chronos::raft::kRaftTransportHeaderSize)
    static_cast<void>(chronos::raft::raft_transport_frame_length_v1(
        bytes.first(chronos::raft::kRaftTransportHeaderSize), limits()));
  const std::size_t split =
      bytes.empty() ? 0U : static_cast<std::size_t>(split_selector) % (bytes.size() + 1U);
  exercise_reader(bytes, split);
}

void mutate(std::vector<std::byte>& candidate, const chronos::common::ByteView input) {
  if (candidate.empty() || input.empty())
    return;
  const std::size_t selector = static_cast<std::size_t>(input_byte(input, 3U)) |
                               (static_cast<std::size_t>(input_byte(input, 4U)) << 8U);
  const std::size_t offset = selector % candidate.size();
  const std::uint8_t supplied_mask = input_byte(input, 5U, 1U);
  candidate[offset] ^= static_cast<std::byte>(supplied_mask == 0U ? 1U : supplied_mask);

  switch (input_byte(input, 6U) & 3U) {
  case 0U:
    break;
  case 1U:
    repair_frame_crc(candidate);
    break;
  case 2U:
    repair_header_crc(candidate);
    repair_frame_crc(candidate);
    break;
  case 3U:
    repair_all_checksums(candidate);
    break;
  }

  if ((input_byte(input, 7U) & 1U) != 0U) {
    const std::size_t new_size =
        static_cast<std::size_t>(input_byte(input, 8U)) * candidate.size() / 255U;
    candidate.resize(new_size);
  }
}

void exercise_structured(const chronos::common::ByteView input) {
  const chronos::raft::RaftTransportEnvelope expected = envelope(input);
  const auto encoded = chronos::raft::encode_raft_transport_envelope_v1(expected, limits());
  if (!encoded.has_value())
    std::abort();
  exercise(*encoded, input_byte(input, 9U));

  std::vector<std::byte> candidate = *encoded;
  mutate(candidate, input);
  exercise(candidate, input_byte(input, 10U));
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const chronos::common::ByteView input = std::as_bytes(std::span{data, size});
  exercise(input, input_byte(input, 0U));
  exercise_structured(input);
  return 0;
}
