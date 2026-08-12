#include "chronos/common/crc32c.hpp"
#include "chronos/raft/transport_codec.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <span>
#include <variant>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] GroupId group_id() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{9U};
  return GroupId{bytes};
}

[[nodiscard]] SnapshotMetadata snapshot() {
  SnapshotMetadata value;
  value.last_included_index = 8U;
  value.last_included_term = 3U;
  value.manifest_generation = 11U;
  value.part_set_checksum.fill(std::byte{0x5a});
  value.configuration_index = 0U;
  value.voters = {1U, 2U, 3U};
  return value;
}

[[nodiscard]] RaftTransportEnvelope envelope(Message message) {
  return {group_id(), 1U, 2U, std::move(message)};
}

void store_u32(const std::span<std::byte> bytes, const std::size_t offset,
               const std::uint32_t value) {
  for (std::size_t ordinal = 0U; ordinal < sizeof(value); ++ordinal)
    bytes[offset + ordinal] = static_cast<std::byte>(value >> (ordinal * 8U));
}

void repair_checksums(std::vector<std::byte>& frame) {
  constexpr std::size_t kHeaderCrcOffset = 76U;
  std::array<std::byte, kRaftTransportHeaderSize> header{};
  std::copy_n(frame.begin(), header.size(), header.begin());
  std::fill_n(header.begin() + static_cast<std::ptrdiff_t>(kHeaderCrcOffset), 4U, std::byte{0U});
  store_u32(frame, kHeaderCrcOffset, common::crc32c(header));
  store_u32(
      frame, frame.size() - kRaftTransportTrailerSize,
      common::crc32c(common::ByteView{frame}.first(frame.size() - kRaftTransportTrailerSize)));
}

TEST(RaftTransportCodecTest, RoundTripsEveryCurrentMessageWithExactRouteIdentity) {
  std::vector<Message> messages;
  messages.emplace_back(RequestVoteRequest{4U, 1U, 12U, 3U});
  messages.emplace_back(RequestVoteResponse{4U, true});
  messages.emplace_back(
      AppendEntriesRequest{4U,
                           1U,
                           12U,
                           3U,
                           {{13U, 4U, 1U, {std::byte{0x11}, std::byte{0x22}}}, {14U, 4U, 253U, {}}},
                           11U});
  messages.emplace_back(AppendEntriesResponse{4U, false, 12U, 3U, 7U});
  messages.emplace_back(InstallSnapshotRequest{4U, 1U, snapshot()});
  messages.emplace_back(InstallSnapshotResponse{4U, true, 8U});
  messages.emplace_back(ReadBarrierRequest{4U, 1U, 19U});
  messages.emplace_back(ReadBarrierResponse{4U, 19U, true});

  for (Message& message : messages) {
    const RaftTransportEnvelope expected = envelope(std::move(message));
    auto encoded = encode_raft_transport_envelope_v1(expected);
    ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
    ASSERT_GE(encoded->size(), kRaftTransportHeaderSize + kRaftTransportTrailerSize);
    EXPECT_EQ((*encoded)[0], std::byte{'C'});
    EXPECT_EQ((*encoded)[56],
              static_cast<std::byte>(static_cast<std::uint8_t>(expected.message.index() + 1U)));
    auto decoded = decode_raft_transport_envelope_v1(*encoded);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
    EXPECT_EQ(*decoded, expected);
  }
}

TEST(RaftTransportCodecTest, RejectsDamageUnknownKindsAndNoncanonicalRoutes) {
  auto encoded =
      encode_raft_transport_envelope_v1(envelope(AppendEntriesRequest{4U, 1U, 0U, 0U, {}, 0U}))
          .value();
  encoded[kRaftTransportHeaderSize + 1U] ^= std::byte{1U};
  auto damaged = decode_raft_transport_envelope_v1(encoded);
  ASSERT_FALSE(damaged.has_value());
  EXPECT_EQ(damaged.error().code(), common::StatusCode::kCorruption);

  encoded = encode_raft_transport_envelope_v1(envelope(RequestVoteResponse{4U, false})).value();
  encoded[56U] = std::byte{99U};
  repair_checksums(encoded);
  auto unknown = decode_raft_transport_envelope_v1(encoded);
  ASSERT_FALSE(unknown.has_value());
  EXPECT_EQ(unknown.error().code(), common::StatusCode::kNotSupported);

  encoded = encode_raft_transport_envelope_v1(envelope(RequestVoteResponse{4U, false})).value();
  encoded[8U] = std::byte{2U};
  repair_checksums(encoded);
  auto version = decode_raft_transport_envelope_v1(encoded);
  ASSERT_FALSE(version.has_value());
  EXPECT_EQ(version.error().code(), common::StatusCode::kNotSupported);

  RaftTransportEnvelope mismatched = envelope(RequestVoteRequest{4U, 2U, 0U, 0U});
  auto invalid_route = encode_raft_transport_envelope_v1(mismatched);
  ASSERT_FALSE(invalid_route.has_value());
  EXPECT_EQ(invalid_route.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(RaftTransportCodecTest, CarriesConflictRepairResponseProducedByTheCore) {
  auto follower = RaftNode::create(2U, {1U, 2U, 3U});
  ASSERT_TRUE(follower.has_value()) << follower.error().to_string();
  auto transition = follower->receive(1U, AppendEntriesRequest{1U, 1U, 5U, 1U, {}, 0U});
  ASSERT_TRUE(transition.has_value()) << transition.error().to_string();
  ASSERT_EQ(transition->outbound.size(), 1U);
  const OutboundMessage& outbound = transition->outbound.front();
  ASSERT_TRUE(std::holds_alternative<AppendEntriesResponse>(outbound.message));
  const AppendEntriesResponse& response = std::get<AppendEntriesResponse>(outbound.message);
  EXPECT_FALSE(response.success);
  EXPECT_EQ(response.conflict_index, 1U);

  RaftTransportEnvelope expected{group_id(), 2U, outbound.destination, outbound.message};
  auto encoded = encode_raft_transport_envelope_v1(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded = decode_raft_transport_envelope_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, expected);
}

TEST(RaftTransportCodecTest, ReadsFragmentedAndCoalescedFramesWithBoundedState) {
  const RaftTransportEnvelope first = envelope(RequestVoteResponse{4U, true});
  const RaftTransportEnvelope second = envelope(ReadBarrierResponse{4U, 19U, true});
  const auto first_bytes = encode_raft_transport_envelope_v1(first).value();
  const auto second_bytes = encode_raft_transport_envelope_v1(second).value();

  auto fragmented = RaftTransportFrameReader::create();
  ASSERT_TRUE(fragmented.has_value());
  std::optional<RaftTransportEnvelope> received;
  for (const std::byte value : first_bytes) {
    const std::array one{value};
    auto step = fragmented->consume(one);
    ASSERT_TRUE(step.has_value()) << step.error().to_string();
    EXPECT_EQ(step->consumed_bytes, 1U);
    if (step->envelope.has_value())
      received = std::move(step->envelope);
  }
  ASSERT_TRUE(received.has_value());
  EXPECT_EQ(*received, first);
  EXPECT_EQ(fragmented->buffered_bytes(), 0U);
  EXPECT_FALSE(fragmented->expected_frame_bytes().has_value());

  std::vector<std::byte> coalesced = first_bytes;
  coalesced.insert(coalesced.end(), second_bytes.begin(), second_bytes.end());
  auto reader = RaftTransportFrameReader::create();
  ASSERT_TRUE(reader.has_value());
  auto one = reader->consume(coalesced);
  ASSERT_TRUE(one.has_value()) << one.error().to_string();
  ASSERT_TRUE(one->envelope.has_value());
  EXPECT_EQ(*one->envelope, first);
  EXPECT_EQ(one->consumed_bytes, first_bytes.size());
  auto two = reader->consume(common::ByteView{coalesced}.subspan(one->consumed_bytes));
  ASSERT_TRUE(two.has_value()) << two.error().to_string();
  ASSERT_TRUE(two->envelope.has_value());
  EXPECT_EQ(*two->envelope, second);

  std::vector<std::byte> damaged = first_bytes;
  damaged[24U] ^= std::byte{1U};
  auto sticky = RaftTransportFrameReader::create();
  ASSERT_TRUE(sticky.has_value());
  auto rejected = sticky->consume(damaged);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_TRUE(sticky->failed());
  EXPECT_EQ(sticky->consume(first_bytes).error(), rejected.error());
}

TEST(RaftTransportCodecTest, OwnsOneValidatedFrameAcrossShortWritesAndMoves) {
  const auto encoded =
      encode_raft_transport_envelope_v1(envelope(RequestVoteResponse{4U, true})).value();
  auto cursor = RaftTransportFrameWriteCursor::create(encoded);
  ASSERT_TRUE(cursor.has_value()) << cursor.error().to_string();
  EXPECT_EQ(cursor->pending_write().size(), encoded.size());
  ASSERT_TRUE(cursor->consume_written(7U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 7U);

  RaftTransportFrameWriteCursor moved{std::move(*cursor)};
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(std::ranges::equal(moved.pending_write(), common::ByteView{encoded}.subspan(7U)));
  EXPECT_FALSE(moved.consume_written(encoded.size()).is_ok());
  ASSERT_TRUE(moved.consume_written(moved.pending_write().size()).is_ok());
  EXPECT_TRUE(moved.complete());
  EXPECT_TRUE(moved.pending_write().empty());

  std::vector<std::byte> damaged = encoded;
  damaged.back() ^= std::byte{1U};
  EXPECT_EQ(RaftTransportFrameWriteCursor::create(std::move(damaged)).error().code(),
            common::StatusCode::kCorruption);
}

TEST(RaftTransportCodecTest, EnforcesFrameEntryAndSnapshotBoundsBeforeAllocation) {
  RaftTransportCodecLimits small;
  small.maximum_frame_bytes = 120U;
  small.maximum_entry_bytes = 64U;
  auto oversized = encode_raft_transport_envelope_v1(
      envelope(AppendEntriesRequest{
          4U, 1U, 0U, 0U, {{1U, 4U, 1U, std::vector<std::byte>(32U, std::byte{1U})}}, 0U}),
      small);
  ASSERT_FALSE(oversized.has_value());
  EXPECT_EQ(oversized.error().code(), common::StatusCode::kResourceExhausted);

  RaftTransportCodecLimits one_entry;
  one_entry.maximum_append_entries = 1U;
  auto too_many = encode_raft_transport_envelope_v1(
      envelope(AppendEntriesRequest{4U, 1U, 0U, 0U, {{1U, 4U, 1U, {}}, {2U, 4U, 1U, {}}}, 0U}),
      one_entry);
  ASSERT_FALSE(too_many.has_value());
  EXPECT_EQ(too_many.error().code(), common::StatusCode::kInvalidArgument);

  SnapshotMetadata too_many_voters = snapshot();
  too_many_voters.voters.push_back(4U);
  RaftTransportCodecLimits three_voters;
  three_voters.maximum_snapshot_voters = 3U;
  auto snapshot_limit = encode_raft_transport_envelope_v1(
      envelope(InstallSnapshotRequest{4U, 1U, std::move(too_many_voters)}), three_voters);
  ASSERT_FALSE(snapshot_limit.has_value());
  EXPECT_EQ(snapshot_limit.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::raft
