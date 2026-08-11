#include "chronos/cluster/distributed_query_transport.hpp"
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

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] query::DistributedAggregateFragmentDispatch dispatch() {
  return {.raft_group_id = uuid(9U),
          .fragment = {
              .query_id = uuid(1U),
              .database_id = id<manifest::DatabaseId>(2U),
              .table_id = id<schema::TableId>(3U),
              .tablet_id = id<schema::TabletId>(4U),
              .destination_schema_id = id<schema::SchemaId>(5U),
              .snapshot_generation = 6U,
              .serving_node = 2U,
              .applied_position = 10U,
              .observed_leader_commit_position = 10U,
              .placement_epoch = 8U,
              .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable,
                              .maximum_staleness_positions = std::nullopt},
              .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
              .destination_column_ordinals = {1U},
              .aggregate_input_index = 0U,
              .event_time_predicate = std::nullopt}};
}

[[nodiscard]] query::ExchangeMessage message() {
  query::MergeableAggregateState partial;
  EXPECT_TRUE(partial.add(2.5).is_ok());
  return {.query_id = uuid(1U),
          .tablet_id = id<schema::TabletId>(4U),
          .sequence = 1U,
          .partial = partial,
          .terminal = true};
}

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId claimed_node_id) const override {
    return principal_id == 91U && claimed_node_id == 1U;
  }
};

class Worker final : public DistributedQueryWorkerService {
public:
  common::Result<query::ExchangeMessage>
  execute(const query::DistributedAggregateFragmentDispatch& received) override {
    ++calls;
    last_query = received.fragment.query_id;
    if (throw_failure)
      throw std::runtime_error{"worker failure"};
    if (failure.has_value())
      return common::make_unexpected(*failure);
    auto response = message();
    if (return_wrong_query)
      response.query_id = uuid(7U);
    return response;
  }

  std::size_t calls{};
  common::Uuid last_query;
  std::optional<common::Status> failure;
  bool return_wrong_query{};
  bool throw_failure{};
};

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void rewrite_request_checksums(std::vector<std::byte>& bytes) {
  const common::ByteView payload = common::ByteView{bytes}.subspan(
      kDistributedQueryRequestHeaderSize, bytes.size() - kDistributedQueryRequestHeaderSize - 4U);
  store_u32(bytes, 48U, common::crc32c(payload));
  store_u32(bytes, 76U, common::crc32c(common::ByteView{bytes}.first(76U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

void rewrite_response_checksums(std::vector<std::byte>& bytes) {
  store_u32(bytes, 108U, common::crc32c(common::ByteView{bytes}.first(108U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

TEST(DistributedQueryTransportCodecTest, RoundTripsCorrelatedRequestAndResponses) {
  const DistributedQueryRequest request{1U, 2U, dispatch()};
  const auto encoded_request = encode_distributed_query_request_v1(request);
  ASSERT_TRUE(encoded_request.has_value()) << encoded_request.error().to_string();
  EXPECT_EQ(encoded_request->size(), 392U);
  const auto decoded_request = decode_distributed_query_request_v1(*encoded_request);
  ASSERT_TRUE(decoded_request.has_value()) << decoded_request.error().to_string();
  EXPECT_EQ(decoded_request->source_node_id, 1U);
  EXPECT_EQ(decoded_request->target_node_id, 2U);
  EXPECT_EQ(decoded_request->dispatch, request.dispatch);

  const DistributedQueryResponse success{.source_node_id = 2U,
                                         .target_node_id = 1U,
                                         .query_id = uuid(1U),
                                         .tablet_id = id<schema::TabletId>(4U),
                                         .status_code = common::StatusCode::kOk,
                                         .message = message(),
                                         .leader_hint = DistributedQueryLeaderHint{2U, 8U}};
  const auto encoded_success = encode_distributed_query_response_v1(success);
  ASSERT_TRUE(encoded_success.has_value()) << encoded_success.error().to_string();
  EXPECT_EQ(encoded_success->size(), kMaximumDistributedQueryResponseSize);
  const auto decoded_success = decode_distributed_query_response_v1(*encoded_success);
  ASSERT_TRUE(decoded_success.has_value()) << decoded_success.error().to_string();
  ASSERT_TRUE(decoded_success->message.has_value());
  EXPECT_EQ(decoded_success->message->partial.count, 1U);
  EXPECT_EQ(decoded_success->message->partial.sum, 2.5);
  EXPECT_EQ(decoded_success->leader_hint, success.leader_hint);

  const DistributedQueryResponse failure{.source_node_id = 2U,
                                         .target_node_id = 1U,
                                         .query_id = uuid(1U),
                                         .tablet_id = id<schema::TabletId>(4U),
                                         .status_code = common::StatusCode::kUnavailable};
  const auto encoded_failure = encode_distributed_query_response_v1(failure);
  ASSERT_TRUE(encoded_failure.has_value()) << encoded_failure.error().to_string();
  EXPECT_EQ(encoded_failure->size(), kDistributedQueryResponseHeaderSize + 4U);
  const auto decoded_failure = decode_distributed_query_response_v1(*encoded_failure);
  ASSERT_TRUE(decoded_failure.has_value());
  EXPECT_EQ(decoded_failure->status_code, common::StatusCode::kUnavailable);
  EXPECT_FALSE(decoded_failure->message.has_value());
}

TEST(DistributedQueryTransportCodecTest, RejectsDamageAndUncorrelatedResults) {
  auto encoded = encode_distributed_query_request_v1({1U, 2U, dispatch()}).value();
  encoded[24U] ^= std::byte{1U};
  EXPECT_EQ(decode_distributed_query_request_v1(encoded).error().code(),
            common::StatusCode::kCorruption);

  auto damaged_inner = encode_distributed_query_request_v1({1U, 2U, dispatch()}).value();
  damaged_inner[kDistributedQueryRequestHeaderSize + 24U] ^= std::byte{1U};
  rewrite_request_checksums(damaged_inner);
  EXPECT_EQ(decode_distributed_query_request_v1(damaged_inner).error().code(),
            common::StatusCode::kCorruption);

  auto future_request = encode_distributed_query_request_v1({1U, 2U, dispatch()}).value();
  store_u16(future_request, 8U, 2U);
  rewrite_request_checksums(future_request);
  EXPECT_EQ(decode_distributed_query_request_v1(future_request).error().code(),
            common::StatusCode::kNotSupported);

  DistributedQueryResponse wrong{.source_node_id = 2U,
                                 .target_node_id = 1U,
                                 .query_id = uuid(7U),
                                 .tablet_id = id<schema::TabletId>(4U),
                                 .status_code = common::StatusCode::kOk,
                                 .message = message()};
  EXPECT_EQ(encode_distributed_query_response_v1(wrong).error().code(),
            common::StatusCode::kInvalidArgument);
  wrong.query_id = uuid(1U);
  wrong.message.reset();
  EXPECT_EQ(encode_distributed_query_response_v1(wrong).error().code(),
            common::StatusCode::kInvalidArgument);

  auto future_response =
      encode_distributed_query_response_v1({.source_node_id = 2U,
                                            .target_node_id = 1U,
                                            .query_id = uuid(1U),
                                            .tablet_id = id<schema::TabletId>(4U),
                                            .status_code = common::StatusCode::kUnavailable})
          .value();
  store_u16(future_response, 8U, 2U);
  rewrite_response_checksums(future_response);
  EXPECT_EQ(decode_distributed_query_response_v1(future_response).error().code(),
            common::StatusCode::kNotSupported);
}

TEST(DistributedQueryReceiverTest, AuthenticatesSourceAndCorrelatesWorkerOutcome) {
  Authorizer authorizer;
  Worker worker;
  auto receiver = DistributedQueryReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value()) << receiver.error().to_string();
  const auto request = encode_distributed_query_request_v1({1U, 2U, dispatch()}).value();

  EXPECT_EQ(receiver->receive(request, {.authorized = false, .principal_id = 91U}).error().code(),
            common::StatusCode::kUnauthenticated);
  EXPECT_EQ(receiver->receive(request, {.authorized = true, .principal_id = 92U}).error().code(),
            common::StatusCode::kUnauthenticated);
  EXPECT_EQ(worker.calls, 0U);

  const auto misrouted = encode_distributed_query_request_v1({1U, 3U, dispatch()}).value();
  EXPECT_EQ(receiver->receive(misrouted, {.authorized = true, .principal_id = 91U}).error().code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(worker.calls, 0U);

  const auto response = receiver->receive(request, {.authorized = true, .principal_id = 91U},
                                          DistributedQueryLeaderHint{2U, 8U});
  ASSERT_TRUE(response.has_value()) << response.error().to_string();
  const auto decoded = decode_distributed_query_response_v1(*response);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->source_node_id, 2U);
  EXPECT_EQ(decoded->target_node_id, 1U);
  EXPECT_EQ(decoded->query_id, uuid(1U));
  EXPECT_EQ(decoded->status_code, common::StatusCode::kOk);
  EXPECT_EQ(worker.calls, 1U);

  worker.failure = common::Status{common::StatusCode::kUnavailable, "placement changed"};
  const auto failed = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(failed.has_value());
  EXPECT_EQ(decode_distributed_query_response_v1(*failed)->status_code,
            common::StatusCode::kUnavailable);
  EXPECT_EQ(worker.calls, 2U);

  worker.failure.reset();
  worker.return_wrong_query = true;
  EXPECT_EQ(receiver->receive(request, {.authorized = true, .principal_id = 91U}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(worker.calls, 3U);

  worker.return_wrong_query = false;
  worker.throw_failure = true;
  const auto threw = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(threw.has_value());
  EXPECT_EQ(decode_distributed_query_response_v1(*threw)->status_code,
            common::StatusCode::kInternal);
  EXPECT_EQ(worker.calls, 4U);
}

} // namespace
} // namespace chronos::cluster
