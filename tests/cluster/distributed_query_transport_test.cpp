#include "chronos/cluster/distributed_query_transport.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <chrono>
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

class LeaderHintProvider final : public DistributedQueryLeaderHintProvider {
public:
  common::Result<std::optional<DistributedQueryLeaderHint>>
  current_leader_hint(const schema::TabletId& tablet_id,
                      const raft::GroupId& group_id) const override {
    ++calls;
    last_tablet = tablet_id;
    last_group = group_id;
    if (failure.has_value())
      return common::make_unexpected(*failure);
    return hint;
  }

  mutable std::size_t calls{};
  mutable std::optional<schema::TabletId> last_tablet;
  mutable std::optional<raft::GroupId> last_group;
  std::optional<DistributedQueryLeaderHint> hint{DistributedQueryLeaderHint{3U, 9U}};
  std::optional<common::Status> failure;
};

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

  constexpr std::array status_codes{common::StatusCode::kOk,
                                    common::StatusCode::kCancelled,
                                    common::StatusCode::kInvalidArgument,
                                    common::StatusCode::kOutOfRange,
                                    common::StatusCode::kNotFound,
                                    common::StatusCode::kAlreadyExists,
                                    common::StatusCode::kCorruption,
                                    common::StatusCode::kIoError,
                                    common::StatusCode::kResourceExhausted,
                                    common::StatusCode::kUnavailable,
                                    common::StatusCode::kNotSupported,
                                    common::StatusCode::kUnauthenticated,
                                    common::StatusCode::kInternal};
  for (std::size_t ordinal = 0U; ordinal < status_codes.size(); ++ordinal) {
    const bool success_status = status_codes[ordinal] == common::StatusCode::kOk;
    auto bytes = encode_distributed_query_response_v1(
        {.source_node_id = 2U,
         .target_node_id = 1U,
         .query_id = uuid(1U),
         .tablet_id = id<schema::TabletId>(4U),
         .status_code = status_codes[ordinal],
         .message =
             success_status ? std::optional<query::ExchangeMessage>{message()} : std::nullopt});
    ASSERT_TRUE(bytes.has_value()) << ordinal;
    EXPECT_EQ(std::to_integer<std::uint8_t>((*bytes)[72U]), ordinal);
    const auto decoded_status = decode_distributed_query_response_v1(*bytes);
    ASSERT_TRUE(decoded_status.has_value()) << ordinal;
    EXPECT_EQ(decoded_status->status_code, status_codes[ordinal]);
  }
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
  LeaderHintProvider hint_provider;
  auto receiver = DistributedQueryReceiver::create({.local_node_id = 2U,
                                                    .authorizer = &authorizer,
                                                    .worker = &worker,
                                                    .leader_hint_provider = &hint_provider});
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
  const auto decoded_failure = decode_distributed_query_response_v1(*failed);
  ASSERT_TRUE(decoded_failure.has_value());
  EXPECT_EQ(decoded_failure->status_code, common::StatusCode::kUnavailable);
  EXPECT_EQ(decoded_failure->leader_hint, DistributedQueryLeaderHint(3U, 9U));
  EXPECT_EQ(hint_provider.calls, 1U);
  EXPECT_EQ(hint_provider.last_tablet, id<schema::TabletId>(4U));
  EXPECT_EQ(hint_provider.last_group, uuid(9U));
  EXPECT_EQ(worker.calls, 2U);

  hint_provider.failure =
      common::Status{common::StatusCode::kUnavailable, "metadata view unavailable"};
  EXPECT_EQ(receiver->receive(request, {.authorized = true, .principal_id = 91U}).error(),
            *hint_provider.failure);
  EXPECT_EQ(worker.calls, 3U);
  hint_provider.failure.reset();

  worker.failure.reset();
  worker.return_wrong_query = true;
  EXPECT_EQ(receiver->receive(request, {.authorized = true, .principal_id = 91U}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(worker.calls, 4U);

  worker.return_wrong_query = false;
  worker.throw_failure = true;
  const auto threw = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(threw.has_value());
  EXPECT_EQ(decode_distributed_query_response_v1(*threw)->status_code,
            common::StatusCode::kInternal);
  EXPECT_EQ(worker.calls, 5U);
}

TEST(DistributedQueryStreamTest, RequestReaderHandlesEverySplitAndCoalescedFrames) {
  const auto encoded = encode_distributed_query_request_v1({1U, 2U, dispatch()}).value();
  for (std::size_t split = 0U; split <= encoded.size(); ++split) {
    DistributedQueryRequestReader reader;
    const auto first = reader.consume(common::ByteView{encoded}.first(split));
    ASSERT_TRUE(first.has_value()) << "split " << split;
    EXPECT_EQ(first->consumed_bytes, split);
    if (split == encoded.size()) {
      ASSERT_TRUE(first->request.has_value());
      EXPECT_EQ(first->request->dispatch.fragment.query_id, uuid(1U));
      continue;
    }
    EXPECT_FALSE(first->request.has_value());
    const auto second = reader.consume(common::ByteView{encoded}.subspan(split));
    ASSERT_TRUE(second.has_value()) << "split " << split;
    ASSERT_TRUE(second->request.has_value());
    EXPECT_EQ(second->consumed_bytes, encoded.size() - split);
    EXPECT_EQ(second->request->dispatch.raft_group_id, uuid(9U));
  }

  std::vector<std::byte> coalesced = encoded;
  coalesced.insert(coalesced.end(), encoded.begin(), encoded.end());
  DistributedQueryRequestReader reader;
  const auto first = reader.consume(coalesced);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->request.has_value());
  EXPECT_EQ(first->consumed_bytes, encoded.size());
  const auto second = reader.consume(common::ByteView{coalesced}.subspan(first->consumed_bytes));
  ASSERT_TRUE(second.has_value());
  EXPECT_TRUE(second->request.has_value());
  EXPECT_EQ(second->consumed_bytes, encoded.size());
}

TEST(DistributedQueryStreamTest, ResponseReaderHandlesBothLengthsEverySplit) {
  const std::vector<std::vector<std::byte>> frames{
      encode_distributed_query_response_v1({.source_node_id = 2U,
                                            .target_node_id = 1U,
                                            .query_id = uuid(1U),
                                            .tablet_id = id<schema::TabletId>(4U),
                                            .status_code = common::StatusCode::kOk,
                                            .message = message()})
          .value(),
      encode_distributed_query_response_v1({.source_node_id = 2U,
                                            .target_node_id = 1U,
                                            .query_id = uuid(1U),
                                            .tablet_id = id<schema::TabletId>(4U),
                                            .status_code = common::StatusCode::kUnavailable})
          .value()};
  for (const auto& frame : frames) {
    for (std::size_t split = 0U; split <= frame.size(); ++split) {
      DistributedQueryResponseReader reader;
      const auto first = reader.consume(common::ByteView{frame}.first(split));
      ASSERT_TRUE(first.has_value()) << "size " << frame.size() << " split " << split;
      if (split == frame.size()) {
        EXPECT_TRUE(first->response.has_value());
        continue;
      }
      EXPECT_FALSE(first->response.has_value());
      const auto second = reader.consume(common::ByteView{frame}.subspan(split));
      ASSERT_TRUE(second.has_value()) << "size " << frame.size() << " split " << split;
      ASSERT_TRUE(second->response.has_value());
      EXPECT_EQ(second->response->query_id, uuid(1U));
    }
  }

  std::vector<std::byte> coalesced = frames.front();
  coalesced.insert(coalesced.end(), frames.back().begin(), frames.back().end());
  DistributedQueryResponseReader reader;
  const auto first = reader.consume(coalesced);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->response.has_value());
  EXPECT_EQ(first->consumed_bytes, frames.front().size());
  const auto second = reader.consume(common::ByteView{coalesced}.subspan(first->consumed_bytes));
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(second->response.has_value());
  EXPECT_EQ(second->response->status_code, common::StatusCode::kUnavailable);
}

TEST(DistributedQueryStreamTest, ReaderFailuresAreStickyAndWriteCursorOwnsOneFrame) {
  auto damaged = encode_distributed_query_request_v1({1U, 2U, dispatch()}).value();
  damaged[24U] ^= std::byte{1U};
  DistributedQueryRequestReader reader;
  const auto rejected = reader.consume(damaged);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(reader.failed());
  const auto retry =
      reader.consume(encode_distributed_query_request_v1({1U, 2U, dispatch()}).value());
  ASSERT_FALSE(retry.has_value());
  EXPECT_EQ(retry.error(), rejected.error());

  auto oversized = encode_distributed_query_request_v1({1U, 2U, dispatch()}).value();
  store_u64(oversized, 16U, kMaximumDistributedQueryRequestSize + 1U);
  store_u32(oversized, 76U, common::crc32c(common::ByteView{oversized}.first(76U)));
  DistributedQueryRequestReader oversized_reader;
  const auto oversized_result = oversized_reader.consume(
      common::ByteView{oversized}.first(kDistributedQueryRequestHeaderSize));
  ASSERT_FALSE(oversized_result.has_value());
  EXPECT_EQ(oversized_result.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(oversized_reader.buffered_bytes(), kDistributedQueryRequestHeaderSize);
  EXPECT_FALSE(oversized_reader.expected_frame_bytes().has_value());

  const auto expected = encode_distributed_query_request_v1({1U, 2U, dispatch()}).value();
  auto cursor = DistributedQueryFrameWriteCursor::create(expected);
  ASSERT_TRUE(cursor.has_value()) << cursor.error().to_string();
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), expected));
  ASSERT_TRUE(cursor->consume_written(17U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 17U);
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), common::ByteView{expected}.subspan(17U)));
  EXPECT_EQ(cursor->consume_written(cursor->pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(cursor->written_bytes(), 17U);
  DistributedQueryFrameWriteCursor moved = std::move(*cursor);
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(cursor->pending_write().empty());
  ASSERT_TRUE(moved.consume_written(moved.pending_write().size()).is_ok());
  EXPECT_TRUE(moved.complete());

  auto corrupt = expected;
  corrupt.back() ^= std::byte{1U};
  EXPECT_EQ(DistributedQueryFrameWriteCursor::create(std::move(corrupt)).error().code(),
            common::StatusCode::kCorruption);

  auto response =
      encode_distributed_query_response_v1({.source_node_id = 2U,
                                            .target_node_id = 1U,
                                            .query_id = uuid(1U),
                                            .tablet_id = id<schema::TabletId>(4U),
                                            .status_code = common::StatusCode::kUnavailable})
          .value();
  auto response_cursor = DistributedQueryFrameWriteCursor::create(response);
  ASSERT_TRUE(response_cursor.has_value());
  EXPECT_TRUE(std::ranges::equal(response_cursor->pending_write(), response));
}

TEST(DistributedQuerySenderTest, RetriesTheImmutableDispatchAndCorrelatesTerminalResult) {
  auto sender = DistributedQuerySender::create(1U, dispatch(),
                                               {.maximum_attempts = 3U,
                                                .initial_backoff = std::chrono::milliseconds{10},
                                                .maximum_backoff = std::chrono::milliseconds{20}});
  ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
  const auto start = DistributedQuerySender::TimePoint{};
  const auto first = sender->begin_attempt(start);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  EXPECT_EQ(first->attempt_number, 1U);
  EXPECT_EQ(first->target_node_id, 2U);
  const auto first_request = decode_distributed_query_request_v1(first->request_bytes);
  ASSERT_TRUE(first_request.has_value());
  EXPECT_EQ(first_request->dispatch.fragment.snapshot_generation, 6U);
  EXPECT_EQ(sender->begin_attempt(start).error().code(), common::StatusCode::kUnavailable);

  const auto wrong =
      encode_distributed_query_response_v1({.source_node_id = 3U,
                                            .target_node_id = 1U,
                                            .query_id = uuid(1U),
                                            .tablet_id = id<schema::TabletId>(4U),
                                            .status_code = common::StatusCode::kUnavailable})
          .value();
  EXPECT_EQ(sender->accept_response(wrong, start).code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kWaitingForResponse);

  const DistributedQueryLeaderHint hint{3U, 9U};
  const auto retryable =
      encode_distributed_query_response_v1({.source_node_id = 2U,
                                            .target_node_id = 1U,
                                            .query_id = uuid(1U),
                                            .tablet_id = id<schema::TabletId>(4U),
                                            .status_code = common::StatusCode::kUnavailable,
                                            .leader_hint = hint})
          .value();
  ASSERT_TRUE(sender->accept_response(retryable, start).is_ok());
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kBackoff);
  EXPECT_EQ(sender->suggested_leader(), hint);
  EXPECT_EQ(sender->next_attempt_not_before(), start + std::chrono::milliseconds{10});
  EXPECT_EQ(sender->begin_attempt(start + std::chrono::milliseconds{9}).error().code(),
            common::StatusCode::kUnavailable);

  const auto second = sender->begin_attempt(start + std::chrono::milliseconds{10});
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(second->target_node_id, 2U);
  const auto second_request = decode_distributed_query_request_v1(second->request_bytes);
  ASSERT_TRUE(second_request.has_value());
  EXPECT_EQ(second_request->dispatch, first_request->dispatch);
  ASSERT_TRUE(sender
                  ->record_transport_failure(common::StatusCode::kIoError,
                                             start + std::chrono::milliseconds{20})
                  .is_ok());
  EXPECT_EQ(sender->next_attempt_not_before(), start + std::chrono::milliseconds{40});

  ASSERT_TRUE(sender->begin_attempt(start + std::chrono::milliseconds{40}).has_value());
  const auto success = encode_distributed_query_response_v1({.source_node_id = 2U,
                                                             .target_node_id = 1U,
                                                             .query_id = uuid(1U),
                                                             .tablet_id = id<schema::TabletId>(4U),
                                                             .status_code = common::StatusCode::kOk,
                                                             .message = message()})
                           .value();
  ASSERT_TRUE(sender->accept_response(success, start).is_ok());
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kSucceeded);
  ASSERT_TRUE(sender->result().has_value());
  EXPECT_EQ(sender->result()->partial.sum, 2.5);
  EXPECT_EQ(sender->attempts_started(), 3U);
  EXPECT_EQ(sender->last_status_code(), common::StatusCode::kOk);
}

TEST(DistributedQuerySenderTest, StopsOnTerminalStatusAndExhaustedRetryBudget) {
  auto terminal = DistributedQuerySender::create(1U, dispatch());
  auto exhausted =
      DistributedQuerySender::create(1U, dispatch(),
                                     {.maximum_attempts = 1U,
                                      .initial_backoff = std::chrono::milliseconds{1},
                                      .maximum_backoff = std::chrono::milliseconds{1}});
  ASSERT_TRUE(terminal.has_value());
  ASSERT_TRUE(exhausted.has_value());
  const auto now = DistributedQuerySender::TimePoint{};

  ASSERT_TRUE(terminal->begin_attempt(now).has_value());
  ASSERT_TRUE(
      terminal->record_transport_failure(common::StatusCode::kUnauthenticated, now).is_ok());
  EXPECT_EQ(terminal->state(), DistributedQuerySenderState::kFailed);
  EXPECT_EQ(terminal->last_status_code(), common::StatusCode::kUnauthenticated);

  ASSERT_TRUE(exhausted->begin_attempt(now).has_value());
  ASSERT_TRUE(exhausted->record_transport_failure(common::StatusCode::kUnavailable, now).is_ok());
  EXPECT_EQ(exhausted->state(), DistributedQuerySenderState::kFailed);
  EXPECT_EQ(exhausted->attempts_started(), 1U);

  auto invalid_dispatch = dispatch();
  invalid_dispatch.fragment.serving_node = 1U;
  EXPECT_EQ(DistributedQuerySender::create(1U, std::move(invalid_dispatch)).error().code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
