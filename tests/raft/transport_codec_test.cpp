#include "chronos/common/crc32c.hpp"
#include "chronos/raft/transport_codec.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
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

void store_u64(const std::span<std::byte> bytes, const std::size_t offset,
               const std::uint64_t value) {
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

void repair_payload_and_checksums(std::vector<std::byte>& frame) {
  constexpr std::size_t kPayloadCrcOffset = 72U;
  const common::ByteView bytes{frame};
  store_u32(frame, kPayloadCrcOffset,
            common::crc32c(bytes.subspan(kRaftTransportHeaderSize, frame.size() -
                                                                       kRaftTransportHeaderSize -
                                                                       kRaftTransportTrailerSize)));
  repair_checksums(frame);
}

void expect_repaired_header_rejection(std::vector<std::byte> frame,
                                      const common::StatusCode expected) {
  repair_checksums(frame);
  const common::ByteView bytes{frame};
  const auto header = raft_transport_frame_length_v1(bytes.first(kRaftTransportHeaderSize));
  ASSERT_FALSE(header.has_value());
  EXPECT_EQ(header.error().code(), expected);
  const auto decoded = decode_raft_transport_envelope_v1(bytes);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), expected);
}

void expect_repaired_payload_rejection(std::vector<std::byte> frame,
                                       const common::StatusCode expected) {
  repair_payload_and_checksums(frame);
  const auto decoded = decode_raft_transport_envelope_v1(frame);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), expected);
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

  for (std::size_t ordinal = 0U; ordinal < messages.size(); ++ordinal) {
    Message& message = messages[ordinal];
    const RaftTransportEnvelope expected = envelope(std::move(message));
    auto encoded = encode_raft_transport_envelope_v1(expected);
    ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
    ASSERT_GE(encoded->size(), kRaftTransportHeaderSize + kRaftTransportTrailerSize);
    EXPECT_EQ((*encoded)[0], std::byte{'C'});
    EXPECT_EQ((*encoded)[56], static_cast<std::byte>(ordinal + 1U));
    auto decoded = decode_raft_transport_envelope_v1(*encoded);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
    EXPECT_EQ(*decoded, expected);
  }
}

TEST(RaftTransportCodecTest, RejectsNoncanonicalAppendResponseState) {
  const std::vector<Message> malformed{
      AppendEntriesResponse{4U, false, 12U, 5U, 7U},
      AppendEntriesResponse{4U, false, 12U, std::nullopt, 0U},
  };
  for (const Message& message : malformed) {
    auto encoded = encode_raft_transport_envelope_v1(envelope(message));
    ASSERT_FALSE(encoded.has_value());
    EXPECT_EQ(encoded.error().code(), common::StatusCode::kInvalidArgument);
  }

  const auto canonical =
      encode_raft_transport_envelope_v1(envelope(AppendEntriesResponse{4U, false, 12U, 3U, 7U}))
          .value();
  std::vector<std::byte> candidate = canonical;
  store_u64(candidate, 120U, 5U);
  expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kCorruption);
  candidate = canonical;
  store_u64(candidate, 128U, 0U);
  expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kCorruption);
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

TEST(RaftTransportCodecTest, RejectsChecksumRepairedHostileHeaderFields) {
  const std::vector<std::byte> canonical =
      encode_raft_transport_envelope_v1(envelope(RequestVoteResponse{4U, false})).value();

  std::vector<std::byte> candidate = canonical;
  std::fill_n(candidate.begin() + 24, common::Uuid::kSize, std::byte{0U});
  expect_repaired_header_rejection(std::move(candidate), common::StatusCode::kCorruption);

  candidate = canonical;
  store_u64(candidate, 40U, 0U);
  expect_repaired_header_rejection(std::move(candidate), common::StatusCode::kCorruption);
  candidate = canonical;
  store_u64(candidate, 48U, 1U);
  expect_repaired_header_rejection(std::move(candidate), common::StatusCode::kCorruption);

  for (const std::size_t offset : {57U, 58U, 59U, 60U, 61U, 62U, 63U, 80U, 81U, 82U, 83U, 84U,
                                   85U, 86U, 87U, 88U, 89U, 90U, 91U, 92U, 93U, 94U, 95U}) {
    SCOPED_TRACE(offset);
    candidate = canonical;
    candidate[offset] = std::byte{1U};
    expect_repaired_header_rejection(std::move(candidate), common::StatusCode::kCorruption);
  }

  for (const std::byte kind : {std::byte{0U}, std::byte{9U}, std::byte{255U}}) {
    candidate = canonical;
    candidate[56U] = kind;
    expect_repaired_header_rejection(std::move(candidate), common::StatusCode::kNotSupported);
  }
  candidate = canonical;
  candidate[8U] = std::byte{2U};
  expect_repaired_header_rejection(std::move(candidate), common::StatusCode::kNotSupported);
  candidate = canonical;
  candidate[10U] = std::byte{1U};
  expect_repaired_header_rejection(std::move(candidate), common::StatusCode::kNotSupported);

  candidate = canonical;
  store_u64(candidate, 16U, canonical.size() - 1U);
  expect_repaired_header_rejection(std::move(candidate), common::StatusCode::kCorruption);
  candidate = canonical;
  store_u64(candidate, 16U, canonical.size() + 1U);
  expect_repaired_header_rejection(std::move(candidate), common::StatusCode::kCorruption);
  candidate = canonical;
  store_u64(candidate, 64U, 17U);
  expect_repaired_header_rejection(std::move(candidate), common::StatusCode::kCorruption);
}

TEST(RaftTransportCodecTest, RejectsChecksumRepairedHostilePayloadFields) {
  const std::vector<std::byte> vote_request =
      encode_raft_transport_envelope_v1(envelope(RequestVoteRequest{4U, 1U, 7U, 3U})).value();

  std::vector<std::byte> candidate = vote_request;
  store_u64(candidate, 112U, std::numeric_limits<LogIndex>::max());
  expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kCorruption);

  const std::vector<std::byte> append =
      encode_raft_transport_envelope_v1(
          envelope(AppendEntriesRequest{
              4U, 1U, 7U, 3U, {{8U, 4U, 1U, {std::byte{0x11}, std::byte{0x22}}}}, 7U}))
          .value();

  candidate = append;
  store_u32(candidate, 136U, std::numeric_limits<std::uint32_t>::max());
  expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kResourceExhausted);
  candidate = append;
  store_u32(candidate, 164U, std::numeric_limits<std::uint32_t>::max());
  expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kResourceExhausted);

  const std::vector<std::byte> heartbeat =
      encode_raft_transport_envelope_v1(envelope(AppendEntriesRequest{4U, 1U, 7U, 3U, {}, 7U}))
          .value();
  candidate = heartbeat;
  store_u64(candidate, 112U, std::numeric_limits<LogIndex>::max());
  expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kCorruption);
  candidate = heartbeat;
  store_u64(candidate, 128U, std::numeric_limits<LogIndex>::max());
  expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kCorruption);

  for (const std::size_t offset : {140U, 141U, 142U, 143U, 161U, 162U, 163U}) {
    SCOPED_TRACE(offset);
    candidate = append;
    candidate[offset] = std::byte{1U};
    expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kCorruption);
  }

  candidate = append;
  candidate.insert(candidate.end() - static_cast<std::ptrdiff_t>(kRaftTransportTrailerSize),
                   std::byte{0U});
  store_u64(candidate, 16U, candidate.size());
  store_u64(candidate, 64U,
            candidate.size() - kRaftTransportHeaderSize - kRaftTransportTrailerSize);
  expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kCorruption);

  const std::vector<std::byte> snapshot_frame =
      encode_raft_transport_envelope_v1(envelope(InstallSnapshotRequest{4U, 1U, snapshot()}))
          .value();
  candidate = snapshot_frame;
  store_u64(candidate, 112U, std::numeric_limits<LogIndex>::max());
  expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kCorruption);
  candidate = snapshot_frame;
  store_u64(candidate, 120U, 5U);
  expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kCorruption);
  candidate = snapshot_frame;
  store_u32(candidate, 176U, std::numeric_limits<std::uint32_t>::max());
  expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kResourceExhausted);
  for (const std::size_t offset : {180U, 181U, 182U, 183U}) {
    SCOPED_TRACE(offset);
    candidate = snapshot_frame;
    candidate[offset] = std::byte{1U};
    expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kCorruption);
  }
  candidate = snapshot_frame;
  store_u64(candidate, 184U, 0U);
  expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kCorruption);
  candidate = snapshot_frame;
  store_u64(candidate, 192U, 1U);
  expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kCorruption);

  const std::vector<std::byte> vote =
      encode_raft_transport_envelope_v1(envelope(RequestVoteResponse{4U, false})).value();
  candidate = vote;
  candidate[104U] = std::byte{2U};
  expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kCorruption);
  for (const std::size_t offset : {105U, 106U, 107U, 108U, 109U, 110U, 111U}) {
    SCOPED_TRACE(offset);
    candidate = vote;
    candidate[offset] = std::byte{1U};
    expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kCorruption);
  }
  candidate = vote;
  store_u64(candidate, 96U, 0U);
  expect_repaired_payload_rejection(std::move(candidate), common::StatusCode::kCorruption);
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
  EXPECT_EQ(received, first);
  EXPECT_EQ(fragmented->buffered_bytes(), 0U);
  EXPECT_FALSE(fragmented->expected_frame_bytes().has_value());

  std::vector<std::byte> coalesced = first_bytes;
  coalesced.insert(coalesced.end(), second_bytes.begin(), second_bytes.end());
  auto reader = RaftTransportFrameReader::create();
  ASSERT_TRUE(reader.has_value());
  auto one = reader->consume(coalesced);
  ASSERT_TRUE(one.has_value()) << one.error().to_string();
  ASSERT_TRUE(one->envelope.has_value());
  EXPECT_EQ(one->envelope, first);
  EXPECT_EQ(one->consumed_bytes, first_bytes.size());
  auto two = reader->consume(common::ByteView{coalesced}.subspan(one->consumed_bytes));
  ASSERT_TRUE(two.has_value()) << two.error().to_string();
  ASSERT_TRUE(two->envelope.has_value());
  EXPECT_EQ(two->envelope, second);

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

TEST(RaftTransportCodecTest, EnforcesFrameEntrySnapshotAndPositionBoundsBeforeAllocation) {
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

  SnapshotMetadata future_term_snapshot = snapshot();
  future_term_snapshot.last_included_term = 5U;
  auto future_term = encode_raft_transport_envelope_v1(
      envelope(InstallSnapshotRequest{4U, 1U, std::move(future_term_snapshot)}));
  ASSERT_FALSE(future_term.has_value());
  EXPECT_EQ(future_term.error().code(), common::StatusCode::kInvalidArgument);

  SnapshotMetadata exhausted_snapshot = snapshot();
  exhausted_snapshot.last_included_index = std::numeric_limits<LogIndex>::max();
  auto exhausted = encode_raft_transport_envelope_v1(
      envelope(InstallSnapshotRequest{4U, 1U, std::move(exhausted_snapshot)}));
  ASSERT_FALSE(exhausted.has_value());
  EXPECT_EQ(exhausted.error().code(), common::StatusCode::kInvalidArgument);

  auto exhausted_vote = encode_raft_transport_envelope_v1(
      envelope(RequestVoteRequest{4U, 1U, std::numeric_limits<LogIndex>::max(), 4U}));
  ASSERT_FALSE(exhausted_vote.has_value());
  EXPECT_EQ(exhausted_vote.error().code(), common::StatusCode::kInvalidArgument);

  auto exhausted_predecessor = encode_raft_transport_envelope_v1(
      envelope(AppendEntriesRequest{4U, 1U, std::numeric_limits<LogIndex>::max(), 4U, {}, 0U}));
  ASSERT_FALSE(exhausted_predecessor.has_value());
  EXPECT_EQ(exhausted_predecessor.error().code(), common::StatusCode::kInvalidArgument);

  auto exhausted_commit = encode_raft_transport_envelope_v1(
      envelope(AppendEntriesRequest{4U, 1U, 0U, 0U, {}, std::numeric_limits<LogIndex>::max()}));
  ASSERT_FALSE(exhausted_commit.has_value());
  EXPECT_EQ(exhausted_commit.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::raft
