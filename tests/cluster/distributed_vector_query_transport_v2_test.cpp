#include "chronos/cluster/distributed_vector_query_transport.hpp"
#include "chronos/cluster/distributed_vector_query_transport_v2.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <ranges>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {
      .columns = {
          {"key", schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), false},
          {"total", schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value(), true}}};
}

[[nodiscard]] query::DistributedVectorFragmentDispatchV2 dispatch_v2() {
  return {.dispatch = {.query_id = uuid(1U),
                       .database_id = id<manifest::DatabaseId>(2U),
                       .table_id = id<schema::TableId>(3U),
                       .tablet_id = id<schema::TabletId>(4U),
                       .destination_schema_id = id<schema::SchemaId>(5U),
                       .raft_group_id = uuid(9U),
                       .snapshot_generation = 6U,
                       .serving_node = 2U,
                       .applied_position = 10U,
                       .observed_leader_commit_position = 10U,
                       .placement_epoch = 8U,
                       .read_policy = {.consistency =
                                           query::DistributedReadConsistency::kLeaderLinearizable},
                       .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
                       .destination_column_ordinals = {0U, 1U},
                       .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
                                .group_key_input_indices = {0U},
                                .aggregates = {{.operation = query::VectorAggregateOperation::kSum,
                                                .input_index = 1U}},
                                .order_keys = {{.output_index = 1U,
                                                .direction =
                                                    query::PhysicalSortDirection::kDescending}},
                                .limit = 3U}},
          .result_schema = result_schema()};
}

[[nodiscard]] std::vector<std::byte> zero_row_batch() {
  const query::DistributedVectorResultSchema schema_value = result_schema();
  const std::array<network::QueryResultColumn, 2U> columns{
      network::QueryResultColumn{schema_value.columns[0].name, schema_value.columns[0].type,
                                 schema_value.columns[0].nullable},
      network::QueryResultColumn{schema_value.columns[1].name, schema_value.columns[1].type,
                                 schema_value.columns[1].nullable}};
  return network::encode_query_result_batch(0U, columns, {}).value();
}

[[nodiscard]] std::vector<std::byte> wrong_schema_batch() {
  const std::array<network::QueryResultColumn, 1U> columns{network::QueryResultColumn{
      "wrong", schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), false}};
  return network::encode_query_result_batch(0U, columns, {}).value();
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

class VectorWorkerV2 final : public DistributedVectorQueryWorkerServiceV2 {
public:
  common::Result<std::vector<DistributedVectorResultExchangeMessage>>
  execute(const query::DistributedVectorFragmentDispatchV2&) override {
    ++calls;
    if (throw_failure)
      throw std::runtime_error{"vector worker failure"};
    if (failure.has_value())
      return common::make_unexpected(*failure);
    if (empty_stream)
      return std::vector<DistributedVectorResultExchangeMessage>{};
    if (terminal_only) {
      return std::vector<DistributedVectorResultExchangeMessage>{
          {.query_id = uuid(1U),
           .tablet_id = id<schema::TabletId>(4U),
           .sequence = 1U,
           .terminal = true}};
    }
    return std::vector<DistributedVectorResultExchangeMessage>{
        {.query_id = uuid(1U),
         .tablet_id = id<schema::TabletId>(4U),
         .sequence = wrong_sequence ? 2U : 1U,
         .terminal = false,
         .encoded_result_batch = wrong_schema ? wrong_schema_batch() : zero_row_batch()},
        {.query_id = uuid(1U),
         .tablet_id = id<schema::TabletId>(4U),
         .sequence = 2U,
         .terminal = true,
         .encoded_result_batch = zero_row_batch()}};
  }

  std::size_t calls{};
  std::optional<common::Status> failure;
  bool terminal_only{};
  bool empty_stream{};
  bool wrong_sequence{};
  bool wrong_schema{};
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

void rewrite_request_checksums(std::vector<std::byte>& bytes) {
  const common::ByteView payload =
      common::ByteView{bytes}.subspan(kDistributedVectorQueryRequestV2HeaderSize,
                                      bytes.size() - kDistributedVectorQueryRequestV2HeaderSize -
                                          kDistributedVectorQueryRequestV2TrailerSize);
  store_u32(bytes, 48U, common::crc32c(payload));
  store_u32(bytes, 76U, common::crc32c(common::ByteView{bytes}.first(76U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

void rewrite_response_checksums(std::vector<std::byte>& bytes) {
  const common::ByteView payload =
      common::ByteView{bytes}.subspan(kDistributedVectorQueryResponseV2HeaderSize,
                                      bytes.size() - kDistributedVectorQueryResponseV2HeaderSize -
                                          kDistributedVectorQueryResponseV2TrailerSize);
  store_u32(bytes, 80U, payload.empty() ? 0U : common::crc32c(payload));
  store_u32(bytes, 108U, common::crc32c(common::ByteView{bytes}.first(108U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

TEST(DistributedVectorQueryTransportV2Test, RoundTripsSchemaBoundRequestResponseAndFailure) {
  const DistributedVectorQueryRequestV2 request{1U, 2U, dispatch_v2()};
  const auto encoded_request = encode_distributed_vector_query_request_v2(request);
  ASSERT_TRUE(encoded_request.has_value()) << encoded_request.error().to_string();
  const auto decoded_request = decode_distributed_vector_query_request_v2_exact(*encoded_request);
  ASSERT_TRUE(decoded_request.has_value()) << decoded_request.error().to_string();
  EXPECT_EQ(*decoded_request, request);
  EXPECT_EQ(decode_distributed_vector_query_request_v1(*encoded_request).error().code(),
            common::StatusCode::kCorruption);
  const auto encoded_v1_request =
      encode_distributed_vector_query_request_v1({1U, 2U, dispatch_v2().dispatch});
  ASSERT_TRUE(encoded_v1_request.has_value());
  EXPECT_EQ(decode_distributed_vector_query_request_v2_exact(*encoded_v1_request).error().code(),
            common::StatusCode::kCorruption);

  const query::DistributedVectorResultSchema schema_value = result_schema();
  const DistributedVectorQueryResponseV2 response{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = id<schema::TabletId>(4U),
      .status_code = common::StatusCode::kOk,
      .payload = DistributedVectorResultExchangeMessage{.query_id = uuid(1U),
                                                        .tablet_id = id<schema::TabletId>(4U),
                                                        .sequence = 1U,
                                                        .terminal = true,
                                                        .encoded_result_batch = zero_row_batch()}};
  const auto encoded_response = encode_distributed_vector_query_response_v2(response, schema_value);
  ASSERT_TRUE(encoded_response.has_value()) << encoded_response.error().to_string();
  const auto decoded_response =
      decode_distributed_vector_query_response_v2_exact(*encoded_response, schema_value);
  ASSERT_TRUE(decoded_response.has_value()) << decoded_response.error().to_string();
  ASSERT_TRUE(decoded_response->payload.has_value());
  // Guarded by the payload assertion above.
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  EXPECT_EQ(decoded_response->payload->encoded_result_batch,
            response.payload->encoded_result_batch);
  EXPECT_TRUE(decoded_response->payload->terminal);
  // NOLINTEND(bugprone-unchecked-optional-access)
  EXPECT_EQ(decode_distributed_vector_query_response_v1(*encoded_response).error().code(),
            common::StatusCode::kCorruption);
  const auto encoded_v1_response = encode_distributed_vector_query_response_v1(
      {.source_node_id = 2U,
       .target_node_id = 1U,
       .query_id = uuid(1U),
       .tablet_id = id<schema::TabletId>(4U),
       .status_code = common::StatusCode::kUnavailable});
  ASSERT_TRUE(encoded_v1_response.has_value());
  EXPECT_EQ(decode_distributed_vector_query_response_v2_exact(*encoded_v1_response, schema_value)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  const DistributedVectorQueryResponseV2 failure{.source_node_id = 2U,
                                                 .target_node_id = 1U,
                                                 .query_id = uuid(1U),
                                                 .tablet_id = id<schema::TabletId>(4U),
                                                 .status_code = common::StatusCode::kUnavailable,
                                                 .leader_hint = DistributedQueryLeaderHint{3U, 9U}};
  const auto encoded_failure = encode_distributed_vector_query_response_v2(failure, schema_value);
  ASSERT_TRUE(encoded_failure.has_value());
  const auto decoded_failure =
      decode_distributed_vector_query_response_v2_exact(*encoded_failure, schema_value);
  ASSERT_TRUE(decoded_failure.has_value());
  EXPECT_FALSE(decoded_failure->payload.has_value());
  EXPECT_EQ(decoded_failure->leader_hint, failure.leader_hint);
}

TEST(DistributedVectorQueryTransportV2Test, RejectsSchemaMismatchDamageAndVersionConfusion) {
  const auto encoded_request = encode_distributed_vector_query_request_v2({1U, 2U, dispatch_v2()});
  ASSERT_TRUE(encoded_request.has_value());
  EXPECT_EQ(decode_distributed_vector_query_request_v2_exact(
                common::ByteView{*encoded_request}.first(encoded_request->size() - 1U))
                .error()
                .code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> future_request = *encoded_request;
  store_u16(future_request, 8U, 3U);
  rewrite_request_checksums(future_request);
  EXPECT_EQ(decode_distributed_vector_query_request_v2_exact(future_request).error().code(),
            common::StatusCode::kNotSupported);
  std::vector<std::byte> nested_request = *encoded_request;
  nested_request[kDistributedVectorQueryRequestV2HeaderSize] ^= std::byte{1U};
  rewrite_request_checksums(nested_request);
  EXPECT_EQ(decode_distributed_vector_query_request_v2_exact(nested_request).error().code(),
            common::StatusCode::kCorruption);

  const query::DistributedVectorResultSchema schema_value = result_schema();
  const DistributedVectorQueryResponseV2 response{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = id<schema::TabletId>(4U),
      .status_code = common::StatusCode::kOk,
      .payload = DistributedVectorResultExchangeMessage{.query_id = uuid(1U),
                                                        .tablet_id = id<schema::TabletId>(4U),
                                                        .sequence = 1U,
                                                        .terminal = true,
                                                        .encoded_result_batch = zero_row_batch()}};
  const auto encoded_response = encode_distributed_vector_query_response_v2(response, schema_value);
  ASSERT_TRUE(encoded_response.has_value());
  query::DistributedVectorResultSchema mismatch = schema_value;
  mismatch.columns.front().name = "other";
  EXPECT_EQ(
      decode_distributed_vector_query_response_v2_exact(*encoded_response, mismatch).error().code(),
      common::StatusCode::kCorruption);
  EXPECT_EQ(encode_distributed_vector_query_response_v2(response, mismatch).error().code(),
            common::StatusCode::kInvalidArgument);

  std::vector<std::byte> correlation = *encoded_response;
  correlation[40U] ^= std::byte{1U};
  rewrite_response_checksums(correlation);
  EXPECT_EQ(
      decode_distributed_vector_query_response_v2_exact(correlation, schema_value).error().code(),
      common::StatusCode::kCorruption);
  std::vector<std::byte> future_response = *encoded_response;
  store_u16(future_response, 8U, 3U);
  rewrite_response_checksums(future_response);
  EXPECT_EQ(decode_distributed_vector_query_response_v2_exact(future_response, schema_value)
                .error()
                .code(),
            common::StatusCode::kNotSupported);
}

TEST(DistributedVectorQueryTransportV2Test, OwnsFragmentedFramesAndTypedShortWrites) {
  const DistributedVectorQueryRequestV2 request{1U, 2U, dispatch_v2()};
  const auto encoded_request = encode_distributed_vector_query_request_v2(request);
  ASSERT_TRUE(encoded_request.has_value());
  for (std::size_t split = 0U; split <= encoded_request->size(); ++split) {
    DistributedVectorQueryRequestV2Reader reader;
    const auto prefix = reader.consume(common::ByteView{*encoded_request}.first(split));
    ASSERT_TRUE(prefix.has_value()) << "split=" << split;
    const auto suffix = reader.consume(common::ByteView{*encoded_request}.subspan(split));
    ASSERT_TRUE(suffix.has_value()) << "split=" << split;
    ASSERT_TRUE(prefix->request.has_value() || suffix->request.has_value()) << "split=" << split;
    EXPECT_EQ(prefix->consumed_bytes + suffix->consumed_bytes, encoded_request->size())
        << "split=" << split;
  }

  const query::DistributedVectorResultSchema schema_value = result_schema();
  const DistributedVectorQueryResponseV2 response{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = id<schema::TabletId>(4U),
      .status_code = common::StatusCode::kOk,
      .payload = DistributedVectorResultExchangeMessage{.query_id = uuid(1U),
                                                        .tablet_id = id<schema::TabletId>(4U),
                                                        .sequence = 1U,
                                                        .terminal = true}};
  const auto encoded_response = encode_distributed_vector_query_response_v2(response, schema_value);
  ASSERT_TRUE(encoded_response.has_value());
  for (std::size_t split = 0U; split <= encoded_response->size(); ++split) {
    DistributedVectorQueryResponseV2Reader reader{result_schema()};
    const auto prefix = reader.consume(common::ByteView{*encoded_response}.first(split));
    ASSERT_TRUE(prefix.has_value()) << "split=" << split;
    const auto suffix = reader.consume(common::ByteView{*encoded_response}.subspan(split));
    ASSERT_TRUE(suffix.has_value()) << "split=" << split;
    ASSERT_TRUE(prefix->response.has_value() || suffix->response.has_value()) << "split=" << split;
    EXPECT_EQ(prefix->consumed_bytes + suffix->consumed_bytes, encoded_response->size())
        << "split=" << split;
  }

  std::vector<std::byte> coalesced(encoded_request->begin(), encoded_request->end());
  coalesced.insert(coalesced.end(), encoded_request->begin(), encoded_request->end());
  DistributedVectorQueryRequestV2Reader coalesced_reader;
  const auto first = coalesced_reader.consume(coalesced);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->request.has_value());
  EXPECT_EQ(first->consumed_bytes, encoded_request->size());
  const auto second =
      coalesced_reader.consume(common::ByteView{coalesced}.subspan(first->consumed_bytes));
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(second->request.has_value());

  std::vector<std::byte> coalesced_responses(encoded_response->begin(), encoded_response->end());
  coalesced_responses.insert(coalesced_responses.end(), encoded_response->begin(),
                             encoded_response->end());
  DistributedVectorQueryResponseV2Reader coalesced_response_reader{result_schema()};
  const auto first_response = coalesced_response_reader.consume(coalesced_responses);
  ASSERT_TRUE(first_response.has_value());
  ASSERT_TRUE(first_response->response.has_value());
  EXPECT_EQ(first_response->consumed_bytes, encoded_response->size());
  const auto second_response = coalesced_response_reader.consume(
      common::ByteView{coalesced_responses}.subspan(first_response->consumed_bytes));
  ASSERT_TRUE(second_response.has_value());
  ASSERT_TRUE(second_response->response.has_value());

  std::vector<std::byte> corrupt = *encoded_request;
  corrupt.front() ^= std::byte{1U};
  DistributedVectorQueryRequestV2Reader failed_reader;
  const auto rejected = failed_reader.consume(corrupt);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_TRUE(failed_reader.failed());
  EXPECT_EQ(failed_reader.consume(*encoded_request).error(), rejected.error());
  std::vector<std::byte> corrupt_response = *encoded_response;
  corrupt_response.front() ^= std::byte{1U};
  DistributedVectorQueryResponseV2Reader failed_response_reader{result_schema()};
  const auto rejected_response = failed_response_reader.consume(corrupt_response);
  ASSERT_FALSE(rejected_response.has_value());
  EXPECT_TRUE(failed_response_reader.failed());
  EXPECT_EQ(failed_response_reader.consume(*encoded_response).error(), rejected_response.error());
  DistributedVectorQueryRequestV2Reader limited_request(encoded_request->size() - 1U);
  EXPECT_EQ(limited_request.consume(*encoded_request).error().code(),
            common::StatusCode::kResourceExhausted);
  DistributedVectorQueryResponseV2Reader limited_response{result_schema(),
                                                          encoded_response->size() - 1U};
  EXPECT_EQ(limited_response.consume(*encoded_response).error().code(),
            common::StatusCode::kResourceExhausted);

  auto request_cursor = DistributedVectorQueryFrameV2WriteCursor::create_request(request);
  ASSERT_TRUE(request_cursor.has_value());
  EXPECT_TRUE(std::ranges::equal(request_cursor->pending_write(), *encoded_request));
  ASSERT_TRUE(request_cursor->consume_written(29U).is_ok());
  EXPECT_EQ(request_cursor->consume_written(request_cursor->pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  DistributedVectorQueryFrameV2WriteCursor moved = std::move(*request_cursor);
  EXPECT_TRUE(request_cursor->complete());
  ASSERT_TRUE(moved.consume_written(moved.pending_write().size()).is_ok());
  EXPECT_TRUE(moved.complete());
  auto response_cursor =
      DistributedVectorQueryFrameV2WriteCursor::create_response(response, schema_value);
  ASSERT_TRUE(response_cursor.has_value());
  EXPECT_TRUE(std::ranges::equal(response_cursor->pending_write(), *encoded_response));
}

TEST(DistributedVectorQueryReceiverV2Test,
     AuthenticatesAndPublishesOnlyBoundedSchemaValidTerminalStreams) {
  Authorizer authorizer;
  VectorWorkerV2 worker;
  LeaderHintProvider hint_provider;
  auto receiver =
      DistributedVectorQueryReceiverV2::create({.local_node_id = 2U,
                                                .authorizer = &authorizer,
                                                .worker = &worker,
                                                .leader_hint_provider = &hint_provider});
  ASSERT_TRUE(receiver.has_value()) << receiver.error().to_string();
  const auto request = encode_distributed_vector_query_request_v2({1U, 2U, dispatch_v2()}).value();

  EXPECT_EQ(
      receiver
          ->receive(common::ByteView{request}.first(1U), {.authorized = false, .principal_id = 91U})
          .error()
          .code(),
      common::StatusCode::kUnauthenticated);
  EXPECT_EQ(receiver->receive(request, {.authorized = true, .principal_id = 92U}).error().code(),
            common::StatusCode::kUnauthenticated);
  const auto misrouted =
      encode_distributed_vector_query_request_v2({1U, 3U, dispatch_v2()}).value();
  EXPECT_EQ(receiver->receive(misrouted, {.authorized = true, .principal_id = 91U}).error().code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(worker.calls, 0U);

  const auto success = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(success.has_value()) << success.error().to_string();
  ASSERT_EQ(success->size(), 2U);
  for (std::size_t index = 0U; index < success->size(); ++index) {
    const auto decoded =
        decode_distributed_vector_query_response_v2_exact((*success)[index], result_schema());
    ASSERT_TRUE(decoded.has_value()) << index << ": " << decoded.error().to_string();
    ASSERT_TRUE(decoded->payload.has_value());
    // Guarded by the payload assertion above.
    // NOLINTBEGIN(bugprone-unchecked-optional-access)
    EXPECT_EQ(decoded->payload->sequence, index + 1U);
    EXPECT_EQ(decoded->payload->terminal, index + 1U == success->size());
    // NOLINTEND(bugprone-unchecked-optional-access)
  }

  worker.terminal_only = true;
  const auto terminal = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(terminal.has_value());
  ASSERT_EQ(terminal->size(), 1U);
  const auto decoded_terminal =
      decode_distributed_vector_query_response_v2_exact(terminal->front(), result_schema());
  ASSERT_TRUE(decoded_terminal.has_value());
  ASSERT_TRUE(decoded_terminal->payload.has_value());
  // Guarded by the payload assertion above.
  // NOLINTBEGIN(bugprone-unchecked-optional-access)
  EXPECT_TRUE(decoded_terminal->payload->terminal);
  EXPECT_TRUE(decoded_terminal->payload->encoded_result_batch.empty());
  // NOLINTEND(bugprone-unchecked-optional-access)

  worker.terminal_only = false;
  worker.failure = common::Status{common::StatusCode::kUnavailable, "placement changed"};
  const auto failed = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(failed.has_value());
  ASSERT_EQ(failed->size(), 1U);
  const auto decoded_failure =
      decode_distributed_vector_query_response_v2_exact(failed->front(), result_schema());
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
  worker.wrong_schema = true;
  EXPECT_EQ(receiver->receive(request, {.authorized = true, .principal_id = 91U}).error().code(),
            common::StatusCode::kInvalidArgument);
  worker.wrong_schema = false;
  worker.empty_stream = true;
  EXPECT_EQ(receiver->receive(request, {.authorized = true, .principal_id = 91U}).error().code(),
            common::StatusCode::kInvalidArgument);
  worker.empty_stream = false;
  worker.throw_failure = true;
  const auto threw = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(threw.has_value());
  EXPECT_EQ(decode_distributed_vector_query_response_v2_exact(threw->front(), result_schema())
                ->status_code,
            common::StatusCode::kInternal);

  worker.throw_failure = false;
  auto frame_bounded = DistributedVectorQueryReceiverV2::create({.local_node_id = 2U,
                                                                 .authorizer = &authorizer,
                                                                 .worker = &worker,
                                                                 .maximum_response_frames = 1U});
  ASSERT_TRUE(frame_bounded.has_value());
  const auto frame_exhausted =
      frame_bounded->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(frame_exhausted.has_value());
  EXPECT_EQ(
      decode_distributed_vector_query_response_v2_exact(frame_exhausted->front(), result_schema())
          ->status_code,
      common::StatusCode::kResourceExhausted);

  auto byte_bounded = DistributedVectorQueryReceiverV2::create({.local_node_id = 2U,
                                                                .authorizer = &authorizer,
                                                                .worker = &worker,
                                                                .maximum_response_bytes = 199U});
  ASSERT_TRUE(byte_bounded.has_value());
  const auto byte_exhausted =
      byte_bounded->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(byte_exhausted.has_value());
  EXPECT_EQ(
      decode_distributed_vector_query_response_v2_exact(byte_exhausted->front(), result_schema())
          ->status_code,
      common::StatusCode::kResourceExhausted);
  worker.terminal_only = true;
  auto exact_byte_bound =
      DistributedVectorQueryReceiverV2::create({.local_node_id = 2U,
                                                .authorizer = &authorizer,
                                                .worker = &worker,
                                                .maximum_response_bytes = 200U});
  ASSERT_TRUE(exact_byte_bound.has_value());
  const auto exact_sized =
      exact_byte_bound->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(exact_sized.has_value());
  ASSERT_EQ(exact_sized->size(), 1U);
  EXPECT_EQ(exact_sized->front().size(), 200U);
  EXPECT_EQ(decode_distributed_vector_query_response_v2_exact(exact_sized->front(), result_schema())
                ->status_code,
            common::StatusCode::kOk);
  EXPECT_FALSE(DistributedVectorQueryReceiverV2::create({.local_node_id = 2U,
                                                         .authorizer = &authorizer,
                                                         .worker = &worker,
                                                         .maximum_response_bytes = 115U})
                   .has_value());
}

// Optional values in these sender cases are asserted or constructed present before access.
// NOLINTBEGIN(bugprone-unchecked-optional-access)
TEST(DistributedVectorQuerySenderV2Test, RetainsOnlyCompleteSchemaBoundTerminalStreams) {
  auto sender = DistributedVectorQuerySenderV2::create(1U, dispatch_v2());
  ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
  const auto now = DistributedVectorQuerySenderV2::TimePoint{};
  auto attempt = sender->begin_attempt(now);
  ASSERT_TRUE(attempt.has_value()) << attempt.error().to_string();
  EXPECT_EQ(attempt->attempt_number, 1U);
  EXPECT_EQ(attempt->target_node_id, 2U);
  const auto decoded = decode_distributed_vector_query_request_v2_exact(attempt->request_bytes);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->dispatch, dispatch_v2());

  std::array responses{
      DistributedVectorQueryResponseV2{
          .source_node_id = 2U,
          .target_node_id = 1U,
          .query_id = uuid(1U),
          .tablet_id = id<schema::TabletId>(4U),
          .status_code = common::StatusCode::kOk,
          .payload =
              DistributedVectorResultExchangeMessage{.query_id = uuid(1U),
                                                     .tablet_id = id<schema::TabletId>(4U),
                                                     .sequence = 1U,
                                                     .terminal = false,
                                                     .encoded_result_batch = zero_row_batch()}},
      DistributedVectorQueryResponseV2{.source_node_id = 2U,
                                       .target_node_id = 1U,
                                       .query_id = uuid(1U),
                                       .tablet_id = id<schema::TabletId>(4U),
                                       .status_code = common::StatusCode::kOk,
                                       .payload = DistributedVectorResultExchangeMessage{
                                           .query_id = uuid(1U),
                                           .tablet_id = id<schema::TabletId>(4U),
                                           .sequence = 2U,
                                           .terminal = true,
                                           .encoded_result_batch = zero_row_batch()}}};
  auto wrong_schema = responses;
  wrong_schema[1].payload->encoded_result_batch = wrong_schema_batch();
  EXPECT_EQ(sender->accept_responses(wrong_schema, now).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kWaitingForResponse);
  EXPECT_FALSE(sender->result().has_value());
  auto wrong_sequence = responses;
  wrong_sequence[1].payload->sequence = 3U;
  EXPECT_EQ(sender->accept_responses(wrong_sequence, now).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kWaitingForResponse);

  const auto first_encoded =
      encode_distributed_vector_query_response_v2(responses[0], result_schema());
  const auto second_encoded =
      encode_distributed_vector_query_response_v2(responses[1], result_schema());
  ASSERT_TRUE(first_encoded.has_value());
  ASSERT_TRUE(second_encoded.has_value());
  auto bounded = DistributedVectorQuerySenderV2::create(
      1U, dispatch_v2(),
      {.maximum_response_frames = 2U,
       .maximum_response_bytes = first_encoded->size() + second_encoded->size() - 1U});
  ASSERT_TRUE(bounded.has_value());
  ASSERT_TRUE(bounded->begin_attempt(now).has_value());
  EXPECT_EQ(bounded->accept_responses(responses, now).code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(bounded->state(), DistributedQuerySenderState::kWaitingForResponse);
  EXPECT_FALSE(bounded->result().has_value());
  auto frame_bounded =
      DistributedVectorQuerySenderV2::create(1U, dispatch_v2(), {.maximum_response_frames = 1U});
  ASSERT_TRUE(frame_bounded.has_value());
  ASSERT_TRUE(frame_bounded->begin_attempt(now).has_value());
  EXPECT_EQ(frame_bounded->accept_responses(responses, now).code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(frame_bounded->state(), DistributedQuerySenderState::kWaitingForResponse);

  EXPECT_TRUE(sender->accept_responses(responses, now).is_ok());
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kSucceeded);
  ASSERT_TRUE(sender->result().has_value());
  ASSERT_EQ(sender->result()->size(), 2U);
  EXPECT_EQ(sender->result()->front().sequence, 1U);
  EXPECT_TRUE(sender->result()->back().terminal);
  responses[0].payload->encoded_result_batch.clear();
  EXPECT_FALSE(sender->result()->front().encoded_result_batch.empty());
  EXPECT_FALSE(sender->begin_attempt(now).has_value());

  auto empty_sender = DistributedVectorQuerySenderV2::create(1U, dispatch_v2());
  ASSERT_TRUE(empty_sender.has_value());
  ASSERT_TRUE(empty_sender->begin_attempt(now).has_value());
  const std::array empty_response{DistributedVectorQueryResponseV2{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = id<schema::TabletId>(4U),
      .status_code = common::StatusCode::kOk,
      .payload = DistributedVectorResultExchangeMessage{.query_id = uuid(1U),
                                                        .tablet_id = id<schema::TabletId>(4U),
                                                        .sequence = 1U,
                                                        .terminal = true}}};
  EXPECT_TRUE(empty_sender->accept_responses(empty_response, now).is_ok());
  ASSERT_TRUE(empty_sender->result().has_value());
  EXPECT_TRUE(empty_sender->result()->front().encoded_result_batch.empty());
}

TEST(DistributedVectorQuerySenderV2Test, RetriesWholeAttemptsWithoutRebindingAuthority) {
  auto sender = DistributedVectorQuerySenderV2::create(
      1U, dispatch_v2(),
      {.retry = {.maximum_attempts = 3U,
                 .initial_backoff = std::chrono::milliseconds{10},
                 .maximum_backoff = std::chrono::milliseconds{20}},
       .maximum_response_frames = 4U,
       .maximum_response_bytes = 1024U});
  ASSERT_TRUE(sender.has_value());
  const auto start = DistributedVectorQuerySenderV2::TimePoint{};
  auto first_attempt = sender->begin_attempt(start);
  ASSERT_TRUE(first_attempt.has_value());
  const DistributedVectorQueryResponseV2 unavailable_response{
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
  auto second_attempt = sender->begin_attempt(start + std::chrono::milliseconds{10});
  ASSERT_TRUE(second_attempt.has_value());
  EXPECT_EQ(second_attempt->target_node_id, 2U);
  EXPECT_EQ(second_attempt->request_bytes, first_attempt->request_bytes);
  EXPECT_TRUE(sender
                  ->record_transport_failure(common::StatusCode::kIoError,
                                             start + std::chrono::milliseconds{10})
                  .is_ok());
  EXPECT_EQ(*sender->next_attempt_not_before(), start + std::chrono::milliseconds{30});
  auto third_attempt = sender->begin_attempt(start + std::chrono::milliseconds{30});
  ASSERT_TRUE(third_attempt.has_value());
  EXPECT_EQ(third_attempt->target_node_id, 2U);
  EXPECT_TRUE(sender
                  ->record_transport_failure(common::StatusCode::kInvalidArgument,
                                             start + std::chrono::milliseconds{30})
                  .is_ok());
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kFailed);
  EXPECT_EQ(sender->attempts_started(), 3U);
  EXPECT_FALSE(sender->result().has_value());
  EXPECT_FALSE(
      DistributedVectorQuerySenderV2::create(1U, dispatch_v2(), {.maximum_response_bytes = 115U})
          .has_value());
}
// NOLINTEND(bugprone-unchecked-optional-access)

} // namespace
} // namespace chronos::cluster
