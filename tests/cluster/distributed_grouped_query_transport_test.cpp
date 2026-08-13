#include "chronos/cluster/distributed_grouped_query_transport.hpp"
#include "chronos/cluster/distributed_query_transport.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <variant>
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

[[nodiscard]] query::DistributedGroupedFloat64FragmentDispatch dispatch() {
  return {
      .raft_group_id = uuid(9U),
      .fragment = {
          .aggregate = {.query_id = uuid(1U),
                        .database_id = id<manifest::DatabaseId>(2U),
                        .table_id = id<schema::TableId>(3U),
                        .tablet_id = id<schema::TabletId>(4U),
                        .destination_schema_id = id<schema::SchemaId>(5U),
                        .snapshot_generation = 6U,
                        .serving_node = 2U,
                        .applied_position = 10U,
                        .observed_leader_commit_position = 10U,
                        .placement_epoch = 8U,
                        .read_policy = {.consistency =
                                            query::DistributedReadConsistency::kLeaderLinearizable,
                                        .maximum_staleness_positions = std::nullopt},
                        .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
                        .destination_column_ordinals = {1U},
                        .aggregate_input_index = 0U,
                        .event_time_predicate = std::nullopt},
          .group_key_input_index = 0U}};
}

[[nodiscard]] query::GroupedFloat64ExchangeMessage partial() {
  query::MergeableAggregateState aggregate;
  EXPECT_TRUE(aggregate.add(2.5).is_ok());
  return {.query_id = uuid(1U),
          .tablet_id = id<schema::TabletId>(4U),
          .sequence = 2U,
          .group_key = -0.0,
          .partial = aggregate,
          .terminal = true};
}

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId claimed_node_id) const override {
    return principal_id == 91U && claimed_node_id == 1U;
  }
};

class GroupedWorker final : public DistributedGroupedQueryWorkerService {
public:
  common::Result<query::DistributedGroupedFloat64WorkerResult>
  execute(const query::DistributedGroupedFloat64FragmentDispatch&) override {
    ++calls;
    if (throw_failure)
      throw std::runtime_error{"grouped worker failure"};
    if (failure.has_value())
      return common::make_unexpected(*failure);
    if (terminal_only) {
      return query::DistributedGroupedFloat64WorkerResult{query::GroupedExchangeTerminalMessage{
          .query_id = uuid(1U), .tablet_id = id<schema::TabletId>(4U), .sequence = 1U}};
    }
    auto first = partial();
    first.sequence = wrong_sequence ? 3U : 1U;
    first.terminal = false;
    auto second = partial();
    return query::DistributedGroupedFloat64WorkerResult{
        std::vector<query::GroupedFloat64ExchangeMessage>{first, second}};
  }

  std::size_t calls{};
  std::optional<common::Status> failure;
  bool terminal_only{};
  bool wrong_sequence{};
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
    return DistributedQueryLeaderHint{3U, 9U};
  }

  mutable std::size_t calls{};
  mutable std::optional<schema::TabletId> last_tablet;
  mutable std::optional<raft::GroupId> last_group;
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
      kDistributedGroupedQueryRequestHeaderSize,
      bytes.size() - kDistributedGroupedQueryRequestHeaderSize - 4U);
  store_u32(bytes, 48U, common::crc32c(payload));
  store_u32(bytes, 76U, common::crc32c(common::ByteView{bytes}.first(76U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

void rewrite_response_checksums(std::vector<std::byte>& bytes) {
  const common::ByteView payload = common::ByteView{bytes}.subspan(
      kDistributedGroupedQueryResponseHeaderSize,
      bytes.size() - kDistributedGroupedQueryResponseHeaderSize - 4U);
  store_u32(bytes, 80U, payload.empty() ? 0U : common::crc32c(payload));
  store_u32(bytes, 108U, common::crc32c(common::ByteView{bytes}.first(108U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

TEST(DistributedGroupedQueryTransportCodecTest, RoundTripsDistinctRequestAndEveryResponseKind) {
  const DistributedGroupedQueryRequest request{1U, 2U, dispatch()};
  const auto encoded_request = encode_distributed_grouped_query_request_v1(request);
  ASSERT_TRUE(encoded_request.has_value()) << encoded_request.error().to_string();
  EXPECT_EQ(encoded_request->size(), 436U);
  const auto decoded_request = decode_distributed_grouped_query_request_v1(*encoded_request);
  ASSERT_TRUE(decoded_request.has_value()) << decoded_request.error().to_string();
  EXPECT_EQ(decoded_request->source_node_id, 1U);
  EXPECT_EQ(decoded_request->target_node_id, 2U);
  EXPECT_EQ(decoded_request->dispatch, request.dispatch);
  EXPECT_EQ(decode_distributed_query_request_v1(*encoded_request).error().code(),
            common::StatusCode::kCorruption);
  const auto ungrouped_request = encode_distributed_query_request_v1(
      {1U, 2U,
       query::DistributedAggregateFragmentDispatch{.raft_group_id = uuid(9U),
                                                   .fragment = dispatch().fragment.aggregate}});
  ASSERT_TRUE(ungrouped_request.has_value()) << ungrouped_request.error().to_string();
  EXPECT_EQ(decode_distributed_grouped_query_request_v1(*ungrouped_request).error().code(),
            common::StatusCode::kCorruption);

  const DistributedGroupedQueryResponse grouped{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = id<schema::TabletId>(4U),
      .status_code = common::StatusCode::kOk,
      .payload = DistributedGroupedQueryResponsePayload{partial()},
      .leader_hint = DistributedQueryLeaderHint{2U, 8U}};
  const auto encoded_grouped = encode_distributed_grouped_query_response_v1(grouped);
  ASSERT_TRUE(encoded_grouped.has_value()) << encoded_grouped.error().to_string();
  EXPECT_EQ(encoded_grouped->size(), kMaximumDistributedGroupedQueryResponseSize);
  const auto decoded_grouped = decode_distributed_grouped_query_response_v1(*encoded_grouped);
  ASSERT_TRUE(decoded_grouped.has_value()) << decoded_grouped.error().to_string();
  EXPECT_EQ(decode_distributed_query_response_v1(*encoded_grouped).error().code(),
            common::StatusCode::kCorruption);
  const auto* decoded_partial =
      std::get_if<query::GroupedFloat64ExchangeMessage>(&*decoded_grouped->payload);
  ASSERT_NE(decoded_partial, nullptr);
  EXPECT_EQ(decoded_partial->sequence, 2U);
  EXPECT_EQ(decoded_partial->group_key, 0.0);
  EXPECT_EQ(decoded_partial->partial.sum, 2.5);
  EXPECT_EQ(decoded_grouped->leader_hint, grouped.leader_hint);

  const DistributedGroupedQueryResponse terminal{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = id<schema::TabletId>(4U),
      .status_code = common::StatusCode::kOk,
      .payload = DistributedGroupedQueryResponsePayload{query::GroupedExchangeTerminalMessage{
          .query_id = uuid(1U), .tablet_id = id<schema::TabletId>(4U), .sequence = 1U}}};
  const auto encoded_terminal = encode_distributed_grouped_query_response_v1(terminal);
  ASSERT_TRUE(encoded_terminal.has_value()) << encoded_terminal.error().to_string();
  EXPECT_EQ(encoded_terminal->size(), kDistributedGroupedQueryResponseHeaderSize + 64U + 4U);
  const auto decoded_terminal = decode_distributed_grouped_query_response_v1(*encoded_terminal);
  ASSERT_TRUE(decoded_terminal.has_value()) << decoded_terminal.error().to_string();
  EXPECT_TRUE(
      std::holds_alternative<query::GroupedExchangeTerminalMessage>(*decoded_terminal->payload));

  const DistributedGroupedQueryResponse failure{.source_node_id = 2U,
                                                .target_node_id = 1U,
                                                .query_id = uuid(1U),
                                                .tablet_id = id<schema::TabletId>(4U),
                                                .status_code = common::StatusCode::kUnavailable};
  const auto encoded_failure = encode_distributed_grouped_query_response_v1(failure);
  ASSERT_TRUE(encoded_failure.has_value()) << encoded_failure.error().to_string();
  EXPECT_EQ(encoded_failure->size(), kDistributedGroupedQueryResponseHeaderSize + 4U);
  const auto decoded_failure = decode_distributed_grouped_query_response_v1(*encoded_failure);
  ASSERT_TRUE(decoded_failure.has_value());
  EXPECT_EQ(decoded_failure->status_code, common::StatusCode::kUnavailable);
  EXPECT_FALSE(decoded_failure->payload.has_value());
  const auto ungrouped_failure =
      encode_distributed_query_response_v1({.source_node_id = 2U,
                                            .target_node_id = 1U,
                                            .query_id = uuid(1U),
                                            .tablet_id = id<schema::TabletId>(4U),
                                            .status_code = common::StatusCode::kUnavailable});
  ASSERT_TRUE(ungrouped_failure.has_value()) << ungrouped_failure.error().to_string();
  EXPECT_EQ(decode_distributed_grouped_query_response_v1(*ungrouped_failure).error().code(),
            common::StatusCode::kCorruption);

  constexpr std::array status_codes{
      common::StatusCode::kCancelled,       common::StatusCode::kInvalidArgument,
      common::StatusCode::kOutOfRange,      common::StatusCode::kNotFound,
      common::StatusCode::kAlreadyExists,   common::StatusCode::kCorruption,
      common::StatusCode::kIoError,         common::StatusCode::kResourceExhausted,
      common::StatusCode::kUnavailable,     common::StatusCode::kNotSupported,
      common::StatusCode::kUnauthenticated, common::StatusCode::kInternal};
  for (std::size_t index = 0U; index < status_codes.size(); ++index) {
    auto encoded =
        encode_distributed_grouped_query_response_v1({.source_node_id = 2U,
                                                      .target_node_id = 1U,
                                                      .query_id = uuid(1U),
                                                      .tablet_id = id<schema::TabletId>(4U),
                                                      .status_code = status_codes[index]});
    ASSERT_TRUE(encoded.has_value()) << index;
    EXPECT_EQ(std::to_integer<std::uint8_t>((*encoded)[72U]), index + 1U);
    EXPECT_EQ(decode_distributed_grouped_query_response_v1(*encoded)->status_code,
              status_codes[index]);
  }
}

TEST(DistributedGroupedQueryTransportCodecTest, RejectsDamageTypeConfusionAndMiscalculation) {
  auto damaged_request = encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()}).value();
  damaged_request[kDistributedGroupedQueryRequestHeaderSize + 24U] ^= std::byte{1U};
  rewrite_request_checksums(damaged_request);
  EXPECT_EQ(decode_distributed_grouped_query_request_v1(damaged_request).error().code(),
            common::StatusCode::kCorruption);

  auto future_request = encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()}).value();
  store_u16(future_request, 8U, 2U);
  rewrite_request_checksums(future_request);
  EXPECT_EQ(decode_distributed_grouped_query_request_v1(future_request).error().code(),
            common::StatusCode::kNotSupported);

  DistributedGroupedQueryResponse wrong{.source_node_id = 2U,
                                        .target_node_id = 1U,
                                        .query_id = uuid(7U),
                                        .tablet_id = id<schema::TabletId>(4U),
                                        .status_code = common::StatusCode::kOk,
                                        .payload =
                                            DistributedGroupedQueryResponsePayload{partial()}};
  EXPECT_EQ(encode_distributed_grouped_query_response_v1(wrong).error().code(),
            common::StatusCode::kInvalidArgument);

  auto wrong_kind = encode_distributed_grouped_query_response_v1(
                        {.source_node_id = 2U,
                         .target_node_id = 1U,
                         .query_id = uuid(1U),
                         .tablet_id = id<schema::TabletId>(4U),
                         .status_code = common::StatusCode::kOk,
                         .payload = DistributedGroupedQueryResponsePayload{partial()}})
                        .value();
  wrong_kind[73U] = std::byte{2U};
  rewrite_response_checksums(wrong_kind);
  EXPECT_EQ(decode_distributed_grouped_query_response_v1(wrong_kind).error().code(),
            common::StatusCode::kCorruption);

  auto damaged_payload = encode_distributed_grouped_query_response_v1(
                             {.source_node_id = 2U,
                              .target_node_id = 1U,
                              .query_id = uuid(1U),
                              .tablet_id = id<schema::TabletId>(4U),
                              .status_code = common::StatusCode::kOk,
                              .payload = DistributedGroupedQueryResponsePayload{partial()}})
                             .value();
  damaged_payload[kDistributedGroupedQueryResponseHeaderSize + 56U] ^= std::byte{1U};
  rewrite_response_checksums(damaged_payload);
  EXPECT_EQ(decode_distributed_grouped_query_response_v1(damaged_payload).error().code(),
            common::StatusCode::kCorruption);

  auto future_response = encode_distributed_grouped_query_response_v1(
                             {.source_node_id = 2U,
                              .target_node_id = 1U,
                              .query_id = uuid(1U),
                              .tablet_id = id<schema::TabletId>(4U),
                              .status_code = common::StatusCode::kUnavailable})
                             .value();
  store_u16(future_response, 8U, 2U);
  rewrite_response_checksums(future_response);
  EXPECT_EQ(decode_distributed_grouped_query_response_v1(future_response).error().code(),
            common::StatusCode::kNotSupported);
}

TEST(DistributedGroupedQueryStreamTest, RequestReaderOwnsEverySplitAndOneCoalescedFrame) {
  const auto encoded = encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()}).value();
  for (std::size_t split = 0U; split <= encoded.size(); ++split) {
    DistributedGroupedQueryRequestReader reader;
    const auto first = reader.consume(common::ByteView{encoded}.first(split));
    ASSERT_TRUE(first.has_value()) << "split " << split;
    EXPECT_EQ(first->consumed_bytes, split);
    if (split == encoded.size()) {
      ASSERT_TRUE(first->request.has_value());
      EXPECT_EQ(first->request->dispatch.fragment.aggregate.query_id, uuid(1U));
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
  DistributedGroupedQueryRequestReader reader;
  const auto first = reader.consume(coalesced);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->request.has_value());
  EXPECT_EQ(first->consumed_bytes, encoded.size());
  const auto second = reader.consume(common::ByteView{coalesced}.subspan(first->consumed_bytes));
  ASSERT_TRUE(second.has_value());
  EXPECT_TRUE(second->request.has_value());
  EXPECT_EQ(second->consumed_bytes, encoded.size());
}

TEST(DistributedGroupedQueryStreamTest, ResponseReaderOwnsAllThreeLengthsAndCoalescing) {
  const std::vector<std::vector<std::byte>> frames{
      encode_distributed_grouped_query_response_v1(
          {.source_node_id = 2U,
           .target_node_id = 1U,
           .query_id = uuid(1U),
           .tablet_id = id<schema::TabletId>(4U),
           .status_code = common::StatusCode::kOk,
           .payload = DistributedGroupedQueryResponsePayload{partial()}})
          .value(),
      encode_distributed_grouped_query_response_v1(
          {.source_node_id = 2U,
           .target_node_id = 1U,
           .query_id = uuid(1U),
           .tablet_id = id<schema::TabletId>(4U),
           .status_code = common::StatusCode::kOk,
           .payload = DistributedGroupedQueryResponsePayload{query::GroupedExchangeTerminalMessage{
               .query_id = uuid(1U), .tablet_id = id<schema::TabletId>(4U), .sequence = 1U}}})
          .value(),
      encode_distributed_grouped_query_response_v1(
          {.source_node_id = 2U,
           .target_node_id = 1U,
           .query_id = uuid(1U),
           .tablet_id = id<schema::TabletId>(4U),
           .status_code = common::StatusCode::kUnavailable})
          .value()};
  for (const auto& frame : frames) {
    for (std::size_t split = 0U; split <= frame.size(); ++split) {
      DistributedGroupedQueryResponseReader reader;
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

  std::vector<std::byte> coalesced;
  for (const auto& frame : frames)
    coalesced.insert(coalesced.end(), frame.begin(), frame.end());
  DistributedGroupedQueryResponseReader reader;
  std::size_t consumed = 0U;
  for (const auto& frame : frames) {
    const auto step = reader.consume(common::ByteView{coalesced}.subspan(consumed));
    ASSERT_TRUE(step.has_value());
    ASSERT_TRUE(step->response.has_value());
    EXPECT_EQ(step->consumed_bytes, frame.size());
    consumed += step->consumed_bytes;
  }
  EXPECT_EQ(consumed, coalesced.size());
}

TEST(DistributedGroupedQueryStreamTest, FailureIsStickyAndWriteCursorOwnsOneFrame) {
  auto damaged = encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()}).value();
  damaged[24U] ^= std::byte{1U};
  DistributedGroupedQueryRequestReader reader;
  const auto rejected = reader.consume(damaged);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(reader.failed());
  const auto retry =
      reader.consume(encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()}).value());
  ASSERT_FALSE(retry.has_value());
  EXPECT_EQ(retry.error(), rejected.error());

  auto oversized = encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()}).value();
  store_u64(oversized, 16U, kMaximumDistributedGroupedQueryRequestSize + 1U);
  store_u32(oversized, 76U, common::crc32c(common::ByteView{oversized}.first(76U)));
  DistributedGroupedQueryRequestReader oversized_reader;
  const auto oversized_result = oversized_reader.consume(
      common::ByteView{oversized}.first(kDistributedGroupedQueryRequestHeaderSize));
  ASSERT_FALSE(oversized_result.has_value());
  EXPECT_EQ(oversized_result.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(oversized_reader.buffered_bytes(), kDistributedGroupedQueryRequestHeaderSize);
  EXPECT_FALSE(oversized_reader.expected_frame_bytes().has_value());

  const auto expected = encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()}).value();
  auto cursor = DistributedGroupedQueryFrameWriteCursor::create(expected);
  ASSERT_TRUE(cursor.has_value()) << cursor.error().to_string();
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), expected));
  ASSERT_TRUE(cursor->consume_written(17U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 17U);
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), common::ByteView{expected}.subspan(17U)));
  EXPECT_EQ(cursor->consume_written(cursor->pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(cursor->written_bytes(), 17U);
  DistributedGroupedQueryFrameWriteCursor moved = std::move(*cursor);
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(cursor->pending_write().empty());
  ASSERT_TRUE(moved.consume_written(moved.pending_write().size()).is_ok());
  EXPECT_TRUE(moved.complete());

  auto corrupt = expected;
  corrupt.back() ^= std::byte{1U};
  EXPECT_EQ(DistributedGroupedQueryFrameWriteCursor::create(std::move(corrupt)).error().code(),
            common::StatusCode::kCorruption);

  auto response = encode_distributed_grouped_query_response_v1(
                      {.source_node_id = 2U,
                       .target_node_id = 1U,
                       .query_id = uuid(1U),
                       .tablet_id = id<schema::TabletId>(4U),
                       .status_code = common::StatusCode::kUnavailable})
                      .value();
  auto response_cursor = DistributedGroupedQueryFrameWriteCursor::create(response);
  ASSERT_TRUE(response_cursor.has_value());
  EXPECT_TRUE(std::ranges::equal(response_cursor->pending_write(), response));
}

TEST(DistributedGroupedQueryReceiverTest, AuthenticatesAndPublishesOnlyCompleteWorkerStreams) {
  Authorizer authorizer;
  GroupedWorker worker;
  LeaderHintProvider hint_provider;
  auto receiver = DistributedGroupedQueryReceiver::create({.local_node_id = 2U,
                                                           .authorizer = &authorizer,
                                                           .worker = &worker,
                                                           .leader_hint_provider = &hint_provider});
  ASSERT_TRUE(receiver.has_value()) << receiver.error().to_string();
  const auto request = encode_distributed_grouped_query_request_v1({1U, 2U, dispatch()}).value();

  EXPECT_EQ(receiver->receive(request, {.authorized = false, .principal_id = 91U}).error().code(),
            common::StatusCode::kUnauthenticated);
  EXPECT_EQ(receiver->receive(request, {.authorized = true, .principal_id = 92U}).error().code(),
            common::StatusCode::kUnauthenticated);
  const auto misrouted = encode_distributed_grouped_query_request_v1({1U, 3U, dispatch()}).value();
  EXPECT_EQ(receiver->receive(misrouted, {.authorized = true, .principal_id = 91U}).error().code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(worker.calls, 0U);

  const auto success = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(success.has_value()) << success.error().to_string();
  ASSERT_EQ(success->size(), 2U);
  for (std::size_t index = 0U; index < success->size(); ++index) {
    const auto decoded = decode_distributed_grouped_query_response_v1((*success)[index]);
    ASSERT_TRUE(decoded.has_value()) << index;
    const auto* message = std::get_if<query::GroupedFloat64ExchangeMessage>(&*decoded->payload);
    ASSERT_NE(message, nullptr);
    EXPECT_EQ(message->sequence, index + 1U);
    EXPECT_EQ(message->terminal, index + 1U == success->size());
  }

  worker.terminal_only = true;
  const auto terminal = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(terminal.has_value());
  ASSERT_EQ(terminal->size(), 1U);
  const auto decoded_terminal = decode_distributed_grouped_query_response_v1(terminal->front());
  ASSERT_TRUE(decoded_terminal.has_value());
  EXPECT_TRUE(
      std::holds_alternative<query::GroupedExchangeTerminalMessage>(*decoded_terminal->payload));

  worker.terminal_only = false;
  worker.failure = common::Status{common::StatusCode::kUnavailable, "placement changed"};
  const auto failed = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(failed.has_value());
  ASSERT_EQ(failed->size(), 1U);
  const auto decoded_failure = decode_distributed_grouped_query_response_v1(failed->front());
  ASSERT_TRUE(decoded_failure.has_value());
  EXPECT_EQ(decoded_failure->status_code, common::StatusCode::kUnavailable);
  EXPECT_EQ(decoded_failure->leader_hint, DistributedQueryLeaderHint(3U, 9U));
  EXPECT_EQ(hint_provider.calls, 1U);
  EXPECT_EQ(hint_provider.last_tablet, id<schema::TabletId>(4U));
  EXPECT_EQ(hint_provider.last_group, uuid(9U));

  worker.failure.reset();
  worker.wrong_sequence = true;
  EXPECT_EQ(receiver->receive(request, {.authorized = true, .principal_id = 91U}).error().code(),
            common::StatusCode::kInvalidArgument);
  worker.wrong_sequence = false;
  worker.throw_failure = true;
  const auto threw = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(threw.has_value());
  EXPECT_EQ(decode_distributed_grouped_query_response_v1(threw->front())->status_code,
            common::StatusCode::kInternal);

  worker.throw_failure = false;
  auto bounded = DistributedGroupedQueryReceiver::create({.local_node_id = 2U,
                                                          .authorizer = &authorizer,
                                                          .worker = &worker,
                                                          .maximum_response_frames = 1U});
  ASSERT_TRUE(bounded.has_value());
  const auto exhausted = bounded->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(exhausted.has_value());
  ASSERT_EQ(exhausted->size(), 1U);
  EXPECT_EQ(decode_distributed_grouped_query_response_v1(exhausted->front())->status_code,
            common::StatusCode::kResourceExhausted);
}

TEST(DistributedGroupedQuerySenderTest, RetainsOnlyCompleteCorrelatedTerminalStreams) {
  auto sender = DistributedGroupedQuerySender::create(1U, dispatch());
  ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
  const auto now = DistributedGroupedQuerySender::TimePoint{};
  auto attempt = sender->begin_attempt(now);
  ASSERT_TRUE(attempt.has_value()) << attempt.error().to_string();
  EXPECT_EQ(attempt->attempt_number, 1U);
  EXPECT_EQ(attempt->target_node_id, 2U);
  auto decoded = decode_distributed_grouped_query_request_v1(attempt->request_bytes);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->dispatch.raft_group_id, uuid(9U));

  auto first = partial();
  first.sequence = 1U;
  first.terminal = false;
  const auto second = partial();
  const std::array responses{
      DistributedGroupedQueryResponse{.source_node_id = 2U,
                                      .target_node_id = 1U,
                                      .query_id = uuid(1U),
                                      .tablet_id = id<schema::TabletId>(4U),
                                      .status_code = common::StatusCode::kOk,
                                      .payload = DistributedGroupedQueryResponsePayload{first}},
      DistributedGroupedQueryResponse{.source_node_id = 2U,
                                      .target_node_id = 1U,
                                      .query_id = uuid(1U),
                                      .tablet_id = id<schema::TabletId>(4U),
                                      .status_code = common::StatusCode::kOk,
                                      .payload = DistributedGroupedQueryResponsePayload{second}}};
  auto wrong_payload = responses;
  std::get<query::GroupedFloat64ExchangeMessage>(*wrong_payload[1].payload).query_id = uuid(8U);
  EXPECT_EQ(sender->accept_responses(wrong_payload, now).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kWaitingForResponse);
  EXPECT_FALSE(sender->result().has_value());
  EXPECT_TRUE(sender->accept_responses(responses, now).is_ok());
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kSucceeded);
  ASSERT_TRUE(sender->result().has_value());
  ASSERT_EQ(sender->result()->size(), 2U);
  EXPECT_EQ(std::get<query::GroupedFloat64ExchangeMessage>(sender->result()->front()).sequence, 1U);
  EXPECT_TRUE(std::get<query::GroupedFloat64ExchangeMessage>(sender->result()->back()).terminal);
  EXPECT_FALSE(sender->begin_attempt(now).has_value());

  auto empty_sender = DistributedGroupedQuerySender::create(1U, dispatch());
  ASSERT_TRUE(empty_sender.has_value());
  ASSERT_TRUE(empty_sender->begin_attempt(now).has_value());
  const std::array empty_response{DistributedGroupedQueryResponse{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = id<schema::TabletId>(4U),
      .status_code = common::StatusCode::kOk,
      .payload = DistributedGroupedQueryResponsePayload{query::GroupedExchangeTerminalMessage{
          .query_id = uuid(1U), .tablet_id = id<schema::TabletId>(4U), .sequence = 1U}}}};
  EXPECT_TRUE(empty_sender->accept_responses(empty_response, now).is_ok());
  ASSERT_TRUE(empty_sender->result().has_value());
  EXPECT_TRUE(std::holds_alternative<query::GroupedExchangeTerminalMessage>(
      empty_sender->result()->front()));
}

TEST(DistributedGroupedQuerySenderTest, RetriesWholeAttemptsWithoutPartialPublication) {
  auto sender =
      DistributedGroupedQuerySender::create(1U, dispatch(),
                                            {.maximum_attempts = 3U,
                                             .initial_backoff = std::chrono::milliseconds{10},
                                             .maximum_backoff = std::chrono::milliseconds{20}});
  ASSERT_TRUE(sender.has_value());
  const auto start = DistributedGroupedQuerySender::TimePoint{};
  ASSERT_TRUE(sender->begin_attempt(start).has_value());
  const DistributedGroupedQueryResponse unavailable_response{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = id<schema::TabletId>(4U),
      .status_code = common::StatusCode::kUnavailable,
      .leader_hint = DistributedQueryLeaderHint{3U, 9U}};
  auto mismatched = unavailable_response;
  mismatched.source_node_id = 3U;
  EXPECT_EQ(sender->accept_responses(std::span{&mismatched, 1U}, start).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kWaitingForResponse);
  EXPECT_TRUE(sender->accept_responses(std::span{&unavailable_response, 1U}, start).is_ok());
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kBackoff);
  ASSERT_TRUE(sender->suggested_leader().has_value());
  EXPECT_EQ(sender->suggested_leader()->node_id, 3U);
  EXPECT_EQ(*sender->next_attempt_not_before(), start + std::chrono::milliseconds{10});
  EXPECT_FALSE(sender->result().has_value());
  EXPECT_FALSE(sender->begin_attempt(start + std::chrono::milliseconds{9}).has_value());
  ASSERT_TRUE(sender->begin_attempt(start + std::chrono::milliseconds{10}).has_value());
  EXPECT_TRUE(sender
                  ->record_transport_failure(common::StatusCode::kIoError,
                                             start + std::chrono::milliseconds{10})
                  .is_ok());
  EXPECT_EQ(*sender->next_attempt_not_before(), start + std::chrono::milliseconds{30});
  ASSERT_TRUE(sender->begin_attempt(start + std::chrono::milliseconds{30}).has_value());
  EXPECT_TRUE(sender
                  ->record_transport_failure(common::StatusCode::kInvalidArgument,
                                             start + std::chrono::milliseconds{30})
                  .is_ok());
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kFailed);
  EXPECT_EQ(sender->attempts_started(), 3U);
  EXPECT_FALSE(sender->result().has_value());
}

} // namespace
} // namespace chronos::cluster
