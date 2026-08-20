#include "chronos/common/status.hpp"
#include "chronos/raft/transport_codec.hpp"
#include "support/failing_allocator.hpp"

#include <algorithm>
#include <cstddef>
#include <gtest/gtest.h>
#include <optional>
#include <utility>
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
  value.configuration_index = 7U;
  value.voters = {1U, 2U, 3U, 4U};
  return value;
}

[[nodiscard]] RaftTransportEnvelope envelope(Message message) {
  return {group_id(), 1U, 2U, std::move(message)};
}

[[nodiscard]] RaftTransportEnvelope append_envelope() {
  std::vector<std::byte> first_payload(64U, std::byte{0x11});
  std::vector<std::byte> second_payload(96U, std::byte{0x22});
  return envelope(AppendEntriesRequest{
      4U,
      1U,
      7U,
      3U,
      {{8U, 4U, 1U, std::move(first_payload)}, {9U, 4U, 2U, std::move(second_payload)}},
      7U,
  });
}

[[nodiscard]] RaftTransportEnvelope snapshot_envelope() {
  return envelope(InstallSnapshotRequest{4U, 1U, snapshot()});
}

template <typename Operation>
[[nodiscard]] auto run_with_allocation_failure(const std::size_t fail_after, std::size_t& observed,
                                               Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    observed = failure.observed_allocations();
    failure.disable();
  }
  return std::move(*result);
}

template <typename Operation, typename VerifySuccess>
[[nodiscard]] std::size_t sweep_allocation_failures(Operation&& operation,
                                                    VerifySuccess&& verify_success) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::size_t observed = 0U;
    auto result = run_with_allocation_failure(fail_after, observed, operation);
    if (result.has_value()) {
      verify_success(*result);
      reached_success = true;
      break;
    }
    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
  return failure_count;
}

void sweep_reader(const std::vector<std::byte>& frame, const RaftTransportEnvelope& expected) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    auto created = RaftTransportFrameReader::create();
    ASSERT_TRUE(created.has_value());
    RaftTransportFrameReader reader = std::move(*created);

    const common::ByteView bytes{frame};
    auto prefix = reader.consume(bytes.first(kRaftTransportHeaderSize - 1U));
    ASSERT_TRUE(prefix.has_value());
    EXPECT_EQ(prefix->consumed_bytes, kRaftTransportHeaderSize - 1U);
    EXPECT_FALSE(prefix->envelope.has_value());

    std::size_t observed = 0U;
    auto step = run_with_allocation_failure(fail_after, observed, [&] {
      return reader.consume(bytes.subspan(kRaftTransportHeaderSize - 1U));
    });
    if (step.has_value()) {
      EXPECT_EQ(step->envelope, std::optional<RaftTransportEnvelope>{expected});
      EXPECT_EQ(step->consumed_bytes, frame.size() - (kRaftTransportHeaderSize - 1U));
      EXPECT_EQ(reader.buffered_bytes(), 0U);
      EXPECT_FALSE(reader.failed());
      reached_success = true;
      break;
    }

    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(step.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(reader.failed());
    auto sticky = reader.consume(frame);
    ASSERT_FALSE(sticky.has_value());
    EXPECT_EQ(sticky.error(), step.error());
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

void sweep_write_cursor(const std::vector<std::byte>& frame) {
  std::size_t failure_count = 0U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::vector<std::byte> owned = frame;
    std::size_t observed = 0U;
    auto cursor = run_with_allocation_failure(fail_after, observed, [&] {
      return RaftTransportFrameWriteCursor::create(std::move(owned));
    });
    if (cursor.has_value()) {
      EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), frame));
      reached_success = true;
      break;
    }
    ++failure_count;
    EXPECT_GT(observed, 0U);
    EXPECT_EQ(cursor.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_GT(failure_count, 0U);
  EXPECT_TRUE(reached_success);
}

TEST(RaftTransportCodecAllocationFailureTest, EncodesEveryMessageKindFailClosed) {
  std::vector<RaftTransportEnvelope> envelopes;
  envelopes.emplace_back(envelope(RequestVoteRequest{4U, 1U, 7U, 3U}));
  envelopes.emplace_back(envelope(RequestVoteResponse{4U, true}));
  envelopes.emplace_back(append_envelope());
  envelopes.emplace_back(envelope(AppendEntriesResponse{4U, false, 7U, 3U, 8U}));
  envelopes.emplace_back(snapshot_envelope());
  envelopes.emplace_back(envelope(InstallSnapshotResponse{4U, true, 8U}));
  envelopes.emplace_back(envelope(ReadBarrierRequest{4U, 1U, 19U}));
  envelopes.emplace_back(envelope(ReadBarrierResponse{4U, 19U, true}));

  for (std::size_t ordinal = 0U; ordinal < envelopes.size(); ++ordinal) {
    SCOPED_TRACE(ordinal);
    const RaftTransportEnvelope& expected = envelopes[ordinal];
    const std::size_t failures =
        sweep_allocation_failures([&] { return encode_raft_transport_envelope_v1(expected); },
                                  [&](const std::vector<std::byte>& bytes) {
                                    auto decoded = decode_raft_transport_envelope_v1(bytes);
                                    ASSERT_TRUE(decoded.has_value());
                                    EXPECT_EQ(*decoded, expected);
                                  });
    EXPECT_GT(failures, 0U);
  }
}

TEST(RaftTransportCodecAllocationFailureTest, DecodesEveryOwnedVariableMessageAllocation) {
  const std::vector<RaftTransportEnvelope> envelopes{append_envelope(), snapshot_envelope()};
  for (std::size_t ordinal = 0U; ordinal < envelopes.size(); ++ordinal) {
    SCOPED_TRACE(ordinal);
    const RaftTransportEnvelope& expected = envelopes[ordinal];
    const auto encoded = encode_raft_transport_envelope_v1(expected);
    ASSERT_TRUE(encoded.has_value());
    const std::size_t failures = sweep_allocation_failures(
        [&] { return decode_raft_transport_envelope_v1(*encoded); },
        [&](const RaftTransportEnvelope& decoded) { EXPECT_EQ(decoded, expected); });
    EXPECT_GT(failures, 0U);
  }
}

TEST(RaftTransportCodecAllocationFailureTest, ReaderFailureIsResourceExhaustedAndSticky) {
  const std::vector<RaftTransportEnvelope> envelopes{append_envelope(), snapshot_envelope()};
  for (const RaftTransportEnvelope& expected : envelopes) {
    const auto encoded = encode_raft_transport_envelope_v1(expected);
    ASSERT_TRUE(encoded.has_value());
    sweep_reader(*encoded, expected);
  }
}

TEST(RaftTransportCodecAllocationFailureTest,
     WriteCursorValidationFailsBeforeOwnershipPublication) {
  const std::vector<RaftTransportEnvelope> envelopes{append_envelope(), snapshot_envelope()};
  for (const RaftTransportEnvelope& expected : envelopes) {
    const auto encoded = encode_raft_transport_envelope_v1(expected);
    ASSERT_TRUE(encoded.has_value());
    sweep_write_cursor(*encoded);
  }
}

} // namespace
} // namespace chronos::raft
