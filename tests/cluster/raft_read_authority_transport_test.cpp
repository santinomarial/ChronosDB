#include "chronos/cluster/raft_read_authority_transport.hpp"
#include "chronos/common/crc32c.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] raft::GroupId group(const std::uint8_t seed = 7U) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return raft::GroupId{bytes};
}

[[nodiscard]] RaftReadAuthority authority() {
  return {
      .barrier = {.group_id = group(), .barrier = {.term = 7U, .context = 9U, .read_index = 11U}},
      .observation = {.group_id = group(),
                      .node_id = 2U,
                      .role = raft::Role::kLeader,
                      .current_term = 7U,
                      .leader_id = 2U,
                      .last_log_index = 12U,
                      .commit_index = 11U,
                      .applied_index = 10U,
                      .voters = {1U, 2U, 3U},
                      .committed_voters = {1U, 2U, 3U}}};
}

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void store_u64(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint64_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void rewrite_request_checksums(std::vector<std::byte>& bytes) {
  store_u32(bytes, 76U, common::crc32c(common::ByteView{bytes}.first(76U)));
  store_u32(bytes, 80U, common::crc32c(common::ByteView{bytes}.first(80U)));
}

void rewrite_response_checksums(std::vector<std::byte>& bytes) {
  store_u32(bytes, 124U, common::crc32c(common::ByteView{bytes}.first(124U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

void rewrite_nested_observation_checksums(std::vector<std::byte>& bytes) {
  constexpr std::size_t kNestedOffset = kRaftReadAuthorityResponseHeaderSize;
  const std::size_t nested_size = bytes.size() - kNestedOffset - 4U;
  store_u32(bytes, kNestedOffset + 92U,
            common::crc32c(common::ByteView{bytes}.subspan(kNestedOffset, 92U)));
  store_u32(bytes, kNestedOffset + nested_size - 4U,
            common::crc32c(common::ByteView{bytes}.subspan(kNestedOffset, nested_size - 4U)));
}

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId claimed_node_id) const override {
    ++calls;
    return principal_id == 91U && claimed_node_id == 1U;
  }

  mutable std::size_t calls{};
};

class AuthorityService final : public RaftReadAuthorityService {
public:
  common::Result<RaftReadAuthority> acquire(const raft::GroupId& requested_group) override {
    ++calls;
    last_group = requested_group;
    if (throw_failure)
      throw std::runtime_error{"authority failure"};
    if (failure.has_value())
      return common::make_unexpected(*failure);
    return result;
  }

  std::size_t calls{};
  raft::GroupId last_group;
  RaftReadAuthority result{authority()};
  std::optional<common::Status> failure;
  bool throw_failure{};
};

TEST(RaftReadAuthorityTransportCodecTest, RoundTripsCanonicalSuccessAndFailureFrames) {
  const RaftReadAuthorityRequest request{1U, 2U, group(), 19U};
  auto request_bytes = encode_raft_read_authority_request_v1(request);
  ASSERT_TRUE(request_bytes.has_value()) << request_bytes.error().to_string();
  EXPECT_EQ(request_bytes->size(), kRaftReadAuthorityRequestSize);
  auto decoded_request = decode_raft_read_authority_request_v1(*request_bytes);
  ASSERT_TRUE(decoded_request.has_value()) << decoded_request.error().to_string();
  EXPECT_EQ(*decoded_request, request);

  const RaftReadAuthorityResponse success{.source_node_id = 2U,
                                          .target_node_id = 1U,
                                          .group_id = group(),
                                          .correlation_id = 19U,
                                          .status_code = common::StatusCode::kOk,
                                          .authority = authority()};
  auto response_bytes = encode_raft_read_authority_response_v1(success);
  ASSERT_TRUE(response_bytes.has_value()) << response_bytes.error().to_string();
  EXPECT_EQ(response_bytes->size(), 352U);
  EXPECT_EQ((*response_bytes)[64U], std::byte{0U});
  EXPECT_EQ((*response_bytes)[65U], std::byte{1U});
  auto decoded_response = decode_raft_read_authority_response_v1(*response_bytes);
  ASSERT_TRUE(decoded_response.has_value()) << decoded_response.error().to_string();
  EXPECT_EQ(*decoded_response, success);

  const RaftReadAuthorityResponse failure{.source_node_id = 2U,
                                          .target_node_id = 1U,
                                          .group_id = group(),
                                          .correlation_id = 20U,
                                          .status_code = common::StatusCode::kUnavailable};
  auto failure_bytes = encode_raft_read_authority_response_v1(failure);
  ASSERT_TRUE(failure_bytes.has_value()) << failure_bytes.error().to_string();
  EXPECT_EQ(failure_bytes->size(), 132U);
  auto decoded_failure = decode_raft_read_authority_response_v1(*failure_bytes);
  ASSERT_TRUE(decoded_failure.has_value()) << decoded_failure.error().to_string();
  EXPECT_EQ(*decoded_failure, failure);
}

TEST(RaftReadAuthorityTransportCodecTest, RejectsDamageAndUncorrelatedAuthorityProofs) {
  auto request = encode_raft_read_authority_request_v1({1U, 2U, group(), 19U}).value();
  request[40U] ^= std::byte{1U};
  EXPECT_EQ(decode_raft_read_authority_request_v1(request).error().code(),
            common::StatusCode::kCorruption);
  request = encode_raft_read_authority_request_v1({1U, 2U, group(), 19U}).value();
  store_u16(request, 8U, 2U);
  rewrite_request_checksums(request);
  EXPECT_EQ(decode_raft_read_authority_request_v1(request).error().code(),
            common::StatusCode::kNotSupported);

  RaftReadAuthorityResponse success{.source_node_id = 2U,
                                    .target_node_id = 1U,
                                    .group_id = group(),
                                    .correlation_id = 19U,
                                    .status_code = common::StatusCode::kOk,
                                    .authority = authority()};
  auto response = encode_raft_read_authority_response_v1(success).value();
  response.back() ^= std::byte{1U};
  EXPECT_EQ(decode_raft_read_authority_response_v1(response).error().code(),
            common::StatusCode::kCorruption);

  response = encode_raft_read_authority_response_v1(success).value();
  response[128U + 56U] ^= std::byte{1U};
  rewrite_nested_observation_checksums(response);
  store_u32(response, 104U,
            common::crc32c(common::ByteView{response}.subspan(128U, response.size() - 132U)));
  rewrite_response_checksums(response);
  EXPECT_EQ(decode_raft_read_authority_response_v1(response).error().code(),
            common::StatusCode::kCorruption);

  success.authority->observation.current_term = 8U;
  EXPECT_EQ(encode_raft_read_authority_response_v1(success).error().code(),
            common::StatusCode::kInvalidArgument);
  success.authority = authority();
  success.authority->observation.joint_membership_active = true;
  success.authority->observation.joint_old_voters = {1U, 2U, 3U};
  success.authority->observation.joint_new_voters = {2U, 3U};
  EXPECT_EQ(encode_raft_read_authority_response_v1(success).error().code(),
            common::StatusCode::kInvalidArgument);
  success.authority.reset();
  EXPECT_EQ(encode_raft_read_authority_response_v1(success).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(RaftReadAuthorityStreamTest, OwnsEverySplitCoalescedSuffixAndShortWrite) {
  const auto request = encode_raft_read_authority_request_v1({1U, 2U, group(), 19U}).value();
  for (std::size_t split = 0U; split <= request.size(); ++split) {
    RaftReadAuthorityRequestReader reader;
    auto first = reader.consume(common::ByteView{request}.first(split));
    ASSERT_TRUE(first.has_value()) << "request split " << split;
    EXPECT_EQ(first->consumed_bytes, split);
    if (split == request.size()) {
      EXPECT_TRUE(first->request.has_value());
      continue;
    }
    EXPECT_FALSE(first->request.has_value());
    auto second = reader.consume(common::ByteView{request}.subspan(split));
    ASSERT_TRUE(second.has_value()) << "request split " << split;
    const auto& decoded_request = second->request;
    if (!decoded_request.has_value()) {
      ADD_FAILURE() << "complete request split produced no value";
      return;
    }
    EXPECT_EQ(decoded_request->correlation_id, 19U);
  }

  const RaftReadAuthorityResponse success{.source_node_id = 2U,
                                          .target_node_id = 1U,
                                          .group_id = group(),
                                          .correlation_id = 19U,
                                          .status_code = common::StatusCode::kOk,
                                          .authority = authority()};
  const RaftReadAuthorityResponse failure{.source_node_id = 2U,
                                          .target_node_id = 1U,
                                          .group_id = group(),
                                          .correlation_id = 20U,
                                          .status_code = common::StatusCode::kUnavailable};
  const std::vector<std::vector<std::byte>> responses{
      encode_raft_read_authority_response_v1(success).value(),
      encode_raft_read_authority_response_v1(failure).value()};
  for (const auto& response : responses) {
    for (std::size_t split = 0U; split <= response.size(); ++split) {
      auto reader = RaftReadAuthorityResponseReader::create().value();
      auto first = reader.consume(common::ByteView{response}.first(split));
      ASSERT_TRUE(first.has_value()) << "response size " << response.size() << " split " << split;
      if (split == response.size()) {
        EXPECT_TRUE(first->response.has_value());
        continue;
      }
      EXPECT_FALSE(first->response.has_value());
      auto second = reader.consume(common::ByteView{response}.subspan(split));
      ASSERT_TRUE(second.has_value()) << "response size " << response.size() << " split " << split;
      const auto& decoded_response = second->response;
      if (!decoded_response.has_value()) {
        ADD_FAILURE() << "complete response split produced no value";
        return;
      }
      EXPECT_EQ(decoded_response->source_node_id, 2U);
    }
  }

  std::vector<std::byte> coalesced = responses.front();
  coalesced.insert(coalesced.end(), responses.back().begin(), responses.back().end());
  auto reader = RaftReadAuthorityResponseReader::create().value();
  auto first = reader.consume(coalesced);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->response.has_value());
  EXPECT_EQ(first->consumed_bytes, responses.front().size());
  auto second = reader.consume(common::ByteView{coalesced}.subspan(first->consumed_bytes));
  ASSERT_TRUE(second.has_value());
  const auto& decoded_response = second->response;
  if (!decoded_response.has_value()) {
    FAIL() << "complete coalesced response produced no value";
  }
  EXPECT_EQ(decoded_response->status_code, common::StatusCode::kUnavailable);

  auto cursor = RaftReadAuthorityFrameWriteCursor::create(request);
  ASSERT_TRUE(cursor.has_value()) << cursor.error().to_string();
  EXPECT_EQ(cursor->pending_write().size(), request.size());
  ASSERT_TRUE(cursor->consume_written(17U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 17U);
  EXPECT_EQ(cursor->consume_written(cursor->pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  RaftReadAuthorityFrameWriteCursor moved = std::move(*cursor);
  EXPECT_TRUE(cursor->complete());
  ASSERT_TRUE(moved.consume_written(moved.pending_write().size()).is_ok());
  EXPECT_TRUE(moved.complete());
  EXPECT_TRUE(RaftReadAuthorityFrameWriteCursor::create(responses.front()).has_value());
}

TEST(RaftReadAuthorityStreamTest, RejectsHostileHeadersBeforeAllocationAndFailsSticky) {
  auto damaged = encode_raft_read_authority_request_v1({1U, 2U, group(), 19U}).value();
  damaged[40U] ^= std::byte{1U};
  RaftReadAuthorityRequestReader request_reader;
  auto rejected = request_reader.consume(damaged);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(request_reader.failed());
  EXPECT_EQ(request_reader.consume({}).error(), rejected.error());

  const RaftReadAuthorityResponse success{.source_node_id = 2U,
                                          .target_node_id = 1U,
                                          .group_id = group(),
                                          .correlation_id = 19U,
                                          .status_code = common::StatusCode::kOk,
                                          .authority = authority()};
  auto oversized = encode_raft_read_authority_response_v1(success).value();
  constexpr std::uint64_t kOversizedDefaultResponse = 1297U;
  store_u64(oversized, 16U, kOversizedDefaultResponse);
  store_u64(oversized, 96U,
            kOversizedDefaultResponse - kRaftReadAuthorityResponseHeaderSize -
                kRaftReadAuthorityFrameTrailerSize);
  store_u32(oversized, 124U, common::crc32c(common::ByteView{oversized}.first(124U)));
  auto response_reader = RaftReadAuthorityResponseReader::create().value();
  auto response_rejected = response_reader.consume(
      common::ByteView{oversized}.first(kRaftReadAuthorityResponseHeaderSize));
  ASSERT_FALSE(response_rejected.has_value());
  EXPECT_EQ(response_rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(response_reader.buffered_bytes(), kRaftReadAuthorityResponseHeaderSize);
  EXPECT_FALSE(response_reader.expected_frame_bytes().has_value());
  EXPECT_TRUE(response_reader.failed());

  auto valid = encode_raft_read_authority_response_v1(success).value();
  auto partial_reader = RaftReadAuthorityResponseReader::create().value();
  auto partial = partial_reader.consume(common::ByteView{valid}.first(151U));
  ASSERT_TRUE(partial.has_value());
  EXPECT_FALSE(partial->response.has_value());
  RaftReadAuthorityResponseReader moved_reader = std::move(partial_reader);
  // The moved-from reader is deliberately reusable and empty.
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
  EXPECT_EQ(partial_reader.buffered_bytes(), 0U);
  auto completed = moved_reader.consume(common::ByteView{valid}.subspan(151U));
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  EXPECT_TRUE(completed->response.has_value());

  auto corrupt = valid;
  corrupt.back() ^= std::byte{1U};
  EXPECT_EQ(RaftReadAuthorityFrameWriteCursor::create(std::move(corrupt)).error().code(),
            common::StatusCode::kCorruption);
}

TEST(RaftReadAuthorityReceiverTest, AuthenticatesAuthorizesCorrelatesAndContainsFailures) {
  Authorizer authorizer;
  AuthorityService service;
  auto receiver = RaftReadAuthorityReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .service = &service});
  ASSERT_TRUE(receiver.has_value()) << receiver.error().to_string();
  const auto request = encode_raft_read_authority_request_v1({1U, 2U, group(), 19U}).value();

  EXPECT_EQ(receiver->receive(request, {}).error().code(), common::StatusCode::kUnauthenticated);
  EXPECT_EQ(authorizer.calls, 0U);
  EXPECT_EQ(service.calls, 0U);

  const auto foreign = encode_raft_read_authority_request_v1({3U, 2U, group(), 20U}).value();
  EXPECT_EQ(receiver->receive(foreign, {.authorized = true, .principal_id = 91U}).error().code(),
            common::StatusCode::kUnauthenticated);
  EXPECT_EQ(service.calls, 0U);

  const auto wrong_target = encode_raft_read_authority_request_v1({1U, 3U, group(), 21U}).value();
  EXPECT_EQ(
      receiver->receive(wrong_target, {.authorized = true, .principal_id = 91U}).error().code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(service.calls, 0U);

  auto response = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(response.has_value()) << response.error().to_string();
  EXPECT_EQ(service.calls, 1U);
  EXPECT_EQ(service.last_group, group());
  auto decoded = decode_raft_read_authority_response_v1(*response);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->source_node_id, 2U);
  EXPECT_EQ(decoded->target_node_id, 1U);
  EXPECT_EQ(decoded->correlation_id, 19U);
  EXPECT_EQ(decoded->authority, authority());

  service.failure = common::Status{common::StatusCode::kUnavailable, "not leader"};
  response = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(response.has_value()) << response.error().to_string();
  decoded = decode_raft_read_authority_response_v1(*response);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->status_code, common::StatusCode::kUnavailable);
  EXPECT_FALSE(decoded->authority.has_value());

  service.failure.reset();
  service.result.barrier.group_id = group(9U);
  EXPECT_EQ(receiver->receive(request, {.authorized = true, .principal_id = 91U}).error().code(),
            common::StatusCode::kInvalidArgument);

  service.result = authority();
  service.throw_failure = true;
  response = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(response.has_value()) << response.error().to_string();
  decoded = decode_raft_read_authority_response_v1(*response);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->status_code, common::StatusCode::kInternal);
}

} // namespace
} // namespace chronos::cluster
