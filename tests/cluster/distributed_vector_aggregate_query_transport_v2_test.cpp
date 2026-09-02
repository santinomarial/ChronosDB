#include "chronos/cluster/distributed_vector_aggregate_query_transport_v2.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <ranges>
#include <span>
#include <stdexcept>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet(const std::uint8_t seed) {
  return schema::TabletId::from_uuid(uuid(seed)).value();
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> definitions() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> receiver_definitions() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt},
          {.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] query::DistributedVectorFragmentDispatchV2 receiver_dispatch() {
  const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {
      .dispatch =
          {.query_id = uuid(11U),
           .database_id = manifest::DatabaseId::from_uuid(uuid(12U)).value(),
           .table_id = schema::TableId::from_uuid(uuid(13U)).value(),
           .tablet_id = tablet(14U),
           .destination_schema_id = schema::SchemaId::from_uuid(uuid(15U)).value(),
           .raft_group_id = uuid(16U),
           .snapshot_generation = 2U,
           .serving_node = 2U,
           .placement_epoch = 3U,
           .read_policy = {.consistency = query::DistributedReadConsistency::kLocalEventual},
           .destination_column_ordinals = {0U},
           .plan = {.mode = query::DistributedVectorPlanMode::kUngroupedAggregate,
                    .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar},
                                   {.operation = query::VectorAggregateOperation::kCountStar}}}},
      .result_schema = {.columns = {{.name = "first", .type = int64, .nullable = false},
                                    {.name = "second", .type = int64, .nullable = false}}}};
}

[[nodiscard]] query::DistributedVectorAggregateExchangeMessage message() {
  const auto expected = definitions();
  auto state = query::MergeableVectorAggregateState::create(expected.front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  return {{.query_id = uuid(1U),
           .tablet_id = tablet(2U),
           .sequence = 1U,
           .aggregate_ordinal = 0U,
           .terminal = true},
          std::move(state)};
}

[[nodiscard]] DistributedVectorAggregateQueryResponseV2 response() {
  return {.source_node_id = 2U,
          .target_node_id = 1U,
          .query_id = uuid(1U),
          .tablet_id = tablet(2U),
          .status_code = common::StatusCode::kOk,
          .payload = message(),
          .leader_hint = DistributedQueryLeaderHint{3U, 9U}};
}

[[nodiscard]] std::vector<DistributedVectorAggregateQueryResponseV2> sender_responses() {
  const auto expected = receiver_definitions();
  std::vector<DistributedVectorAggregateQueryResponseV2> responses;
  responses.reserve(expected.size());
  for (std::size_t ordinal = 0U; ordinal < expected.size(); ++ordinal) {
    auto state = query::MergeableVectorAggregateState::create(expected[ordinal]).value();
    for (std::size_t count = 0U; count <= ordinal; ++count)
      EXPECT_TRUE(state.accumulate_count_star().has_value());
    responses.push_back({.source_node_id = 2U,
                         .target_node_id = 1U,
                         .query_id = uuid(11U),
                         .tablet_id = tablet(14U),
                         .status_code = common::StatusCode::kOk,
                         .payload = query::DistributedVectorAggregateExchangeMessage{
                             {.query_id = uuid(11U),
                              .tablet_id = tablet(14U),
                              .sequence = ordinal + 1U,
                              .aggregate_ordinal = static_cast<std::uint32_t>(ordinal),
                              .terminal = ordinal + 1U == expected.size()},
                             std::move(state)}});
  }
  return responses;
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

class AggregateWorker final : public DistributedVectorAggregateQueryWorkerServiceV2 {
public:
  common::Result<std::vector<query::VectorAggregateDefinition>>
  bind_definitions(const query::DistributedVectorFragmentDispatchV2&) override {
    ++bind_calls;
    if (throw_bind)
      throw std::runtime_error{"bind failure"};
    if (bind_failure.has_value())
      return common::make_unexpected(*bind_failure);
    auto result = receiver_definitions();
    if (wrong_bound_definition)
      result[1] = {.operation = query::VectorAggregateOperation::kCount,
                   .input = query::VectorAggregateInput{
                       .column_ordinal = 0U,
                       .type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
                       .nullable = false}};
    return result;
  }

  common::Result<query::DistributedVectorAggregateWorkerResultV2>
  execute(const query::DistributedVectorFragmentDispatchV2&) override {
    ++execute_calls;
    if (throw_execute)
      throw std::runtime_error{"execute failure"};
    if (execute_failure.has_value())
      return common::make_unexpected(*execute_failure);
    auto expected = receiver_definitions();
    query::DistributedVectorAggregateWorkerResultV2 result{.definitions = expected,
                                                           .input_rows = 3U};
    for (std::size_t ordinal = 0U; ordinal < expected.size(); ++ordinal) {
      auto state = query::MergeableVectorAggregateState::create(expected[ordinal]).value();
      for (std::size_t count = 0U; count <= ordinal; ++count)
        EXPECT_TRUE(state.accumulate_count_star().has_value());
      result.messages.emplace_back(
          query::DistributedVectorAggregateExchangePosition{
              .query_id = uuid(11U),
              .tablet_id = tablet(14U),
              .sequence = wrong_sequence && ordinal == 1U ? 7U : ordinal + 1U,
              .aggregate_ordinal = static_cast<std::uint32_t>(ordinal),
              .terminal = ordinal + 1U == expected.size()},
          std::move(state));
    }
    if (changed_definitions)
      result.definitions.pop_back();
    if (incomplete_messages)
      result.messages.pop_back();
    return result;
  }

  std::size_t bind_calls{};
  std::size_t execute_calls{};
  std::optional<common::Status> bind_failure;
  std::optional<common::Status> execute_failure;
  bool wrong_bound_definition{};
  bool changed_definitions{};
  bool incomplete_messages{};
  bool wrong_sequence{};
  bool throw_bind{};
  bool throw_execute{};
};

class LeaderHintProvider final : public DistributedQueryLeaderHintProvider {
public:
  common::Result<std::optional<DistributedQueryLeaderHint>>
  current_leader_hint(const schema::TabletId& tablet_id,
                      const raft::GroupId& group_id) const override {
    ++calls;
    last_tablet = tablet_id;
    last_group = group_id;
    return DistributedQueryLeaderHint{3U, 17U};
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

void rewrite_checksums(std::vector<std::byte>& bytes) {
  const common::ByteView payload = common::ByteView{bytes}.subspan(
      kDistributedVectorAggregateQueryResponseV2HeaderSize,
      bytes.size() - kDistributedVectorAggregateQueryResponseV2HeaderSize -
          kDistributedVectorAggregateQueryResponseV2TrailerSize);
  store_u32(bytes, 80U, payload.empty() ? 0U : common::crc32c(payload));
  store_u32(bytes, 108U, common::crc32c(common::ByteView{bytes}.first(108U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

TEST(DistributedVectorAggregateQueryTransportV2Test,
     RoundTripsDefinitionBoundStateAndCorrelatedFailure) {
  const auto expected = definitions();
  const auto encoded = encode_distributed_vector_aggregate_query_response_v2(response(), expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const std::array<std::byte, 8U> magic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                        std::byte{'V'}, std::byte{'A'}, std::byte{'R'},
                                        std::byte{'P'}, std::byte{'2'}};
  EXPECT_TRUE(std::ranges::equal(common::ByteView{*encoded}.first(magic.size()), magic));
  query::QueryResourceContext resources = query::QueryResourceContext::create(1U << 20U).value();
  auto decoded =
      decode_distributed_vector_aggregate_query_response_v2_exact(*encoded, expected, resources);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->source_node_id, 2U);
  EXPECT_EQ(decoded->target_node_id, 1U);
  ASSERT_TRUE(decoded->payload.has_value());
  auto& decoded_payload = decoded->payload;
  if (!decoded_payload.has_value()) {
    ADD_FAILURE() << "expected a definition-bound aggregate payload";
    return;
  }
  auto result = std::move(decoded_payload->state).take_result();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(std::get<std::int64_t>(result->storage()), 2);
  ASSERT_TRUE(decoded->leader_hint.has_value());
  const auto& leader_hint = decoded->leader_hint;
  if (!leader_hint.has_value()) {
    ADD_FAILURE() << "expected an aggregate leader hint";
    return;
  }
  EXPECT_EQ(leader_hint->node_id, 3U);

  const DistributedVectorAggregateQueryResponseV2 failure{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = tablet(2U),
      .status_code = common::StatusCode::kUnavailable,
      .leader_hint = DistributedQueryLeaderHint{4U, 10U}};
  const auto encoded_failure =
      encode_distributed_vector_aggregate_query_response_v2(failure, expected);
  ASSERT_TRUE(encoded_failure.has_value());
  const auto decoded_failure = decode_distributed_vector_aggregate_query_response_v2_exact(
      *encoded_failure, expected, resources);
  ASSERT_TRUE(decoded_failure.has_value());
  EXPECT_EQ(decoded_failure->status_code, common::StatusCode::kUnavailable);
  EXPECT_FALSE(decoded_failure->payload.has_value());
}

TEST(DistributedVectorAggregateQueryTransportV2Test,
     RejectsDamageVersionConfusionCorrelationAndLowerBounds) {
  const auto expected = definitions();
  const auto encoded =
      encode_distributed_vector_aggregate_query_response_v2(response(), expected).value();
  query::QueryResourceContext resources = query::QueryResourceContext::create(1U << 20U).value();
  const query::DistributedVectorResultSchema row_schema{
      .columns = {{.name = "count",
                   .type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
                   .nullable = false}}};
  EXPECT_EQ(decode_distributed_vector_query_response_v2_exact(encoded, row_schema).error().code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> future = encoded;
  store_u16(future, 8U, 3U);
  rewrite_checksums(future);
  EXPECT_EQ(decode_distributed_vector_aggregate_query_response_v2_exact(future, expected, resources)
                .error()
                .code(),
            common::StatusCode::kNotSupported);

  std::vector<std::byte> nested_damage = encoded;
  nested_damage[kDistributedVectorAggregateQueryResponseV2HeaderSize] ^= std::byte{1U};
  rewrite_checksums(nested_damage);
  EXPECT_EQ(decode_distributed_vector_aggregate_query_response_v2_exact(nested_damage, expected,
                                                                        resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  auto wrong = expected;
  wrong.front().operation = query::VectorAggregateOperation::kMinimum;
  wrong.front().input = query::VectorAggregateInput{
      .column_ordinal = 0U,
      .type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
      .nullable = false};
  EXPECT_EQ(decode_distributed_vector_aggregate_query_response_v2_exact(encoded, wrong, resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  auto lower_definitions = definitions();
  DistributedVectorAggregateQueryResponseV2Reader lower_reader{
      std::move(lower_definitions), query::QueryResourceContext::create(1U << 20U).value(),
      encoded.size() - 1U};
  EXPECT_EQ(lower_reader.consume(encoded).error().code(), common::StatusCode::kResourceExhausted);

  auto uncorrelated = response();
  uncorrelated.query_id = uuid(9U);
  EXPECT_EQ(
      encode_distributed_vector_aggregate_query_response_v2(uncorrelated, expected).error().code(),
      common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorAggregateQueryTransportV2Test,
     ReaderOwnsEverySplitLeavesSuccessorAndFailsSticky) {
  const auto expected = definitions();
  const auto encoded =
      encode_distributed_vector_aggregate_query_response_v2(response(), expected).value();
  for (std::size_t split = 0U; split <= encoded.size(); ++split) {
    auto owned_definitions = definitions();
    DistributedVectorAggregateQueryResponseV2Reader reader{
        std::move(owned_definitions), query::QueryResourceContext::create(1U << 20U).value()};
    const auto first = reader.consume(common::ByteView{encoded}.first(split));
    ASSERT_TRUE(first.has_value()) << first.error().to_string();
    EXPECT_EQ(first->consumed_bytes, split);
    if (split != encoded.size()) {
      EXPECT_FALSE(first->response.has_value());
    }
    std::array<std::byte, 3U> successor{std::byte{1U}, std::byte{2U}, std::byte{3U}};
    std::vector<std::byte> tail(common::ByteView{encoded}.subspan(split).begin(),
                                common::ByteView{encoded}.subspan(split).end());
    tail.insert(tail.end(), successor.begin(), successor.end());
    if (split == encoded.size()) {
      ASSERT_TRUE(first->response.has_value());
      continue;
    }
    const auto second = reader.consume(tail);
    ASSERT_TRUE(second.has_value()) << second.error().to_string();
    ASSERT_TRUE(second->response.has_value());
    EXPECT_EQ(second->consumed_bytes, encoded.size() - split);
  }

  auto damaged = encoded;
  damaged.back() ^= std::byte{1U};
  auto owned_definitions = definitions();
  DistributedVectorAggregateQueryResponseV2Reader reader{
      std::move(owned_definitions), query::QueryResourceContext::create(1U << 20U).value()};
  const auto failed = reader.consume(damaged);
  ASSERT_FALSE(failed.has_value());
  EXPECT_TRUE(reader.failed());
  EXPECT_EQ(reader.consume({}).error().code(), failed.error().code());
}

TEST(DistributedVectorAggregateQueryTransportV2Test, CursorOwnsValidatedShortWriteProgress) {
  const auto expected = definitions();
  auto cursor = DistributedVectorAggregateQueryResponseV2WriteCursor::create(response(), expected);
  ASSERT_TRUE(cursor.has_value()) << cursor.error().to_string();
  const std::size_t size = cursor->pending_write().size();
  ASSERT_GT(size, 2U);
  EXPECT_TRUE(cursor->consume_written(1U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 1U);
  EXPECT_EQ(cursor->consume_written(size).code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(cursor->written_bytes(), 1U);
  DistributedVectorAggregateQueryResponseV2WriteCursor moved{std::move(*cursor)};
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(moved.consume_written(size - 1U).is_ok());
  EXPECT_TRUE(moved.complete());
}

TEST(DistributedVectorAggregateQueryReceiverV2Test,
     AuthenticatesBindsAndPublishesOnlyACompleteStateVector) {
  Authorizer authorizer;
  AggregateWorker worker;
  auto receiver = DistributedVectorAggregateQueryReceiverV2::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value()) << receiver.error().to_string();
  const auto encoded_request = encode_distributed_vector_query_request_v2(
      {.source_node_id = 1U, .target_node_id = 2U, .dispatch = receiver_dispatch()});
  ASSERT_TRUE(encoded_request.has_value());

  const auto missing_auth = receiver->receive(*encoded_request, {});
  ASSERT_FALSE(missing_auth.has_value());
  EXPECT_EQ(missing_auth.error().code(), common::StatusCode::kUnauthenticated);
  EXPECT_EQ(authorizer.calls, 0U);
  EXPECT_EQ(worker.bind_calls, 0U);

  const auto unauthorized =
      receiver->receive(*encoded_request, {.authorized = true, .principal_id = 92U});
  ASSERT_FALSE(unauthorized.has_value());
  EXPECT_EQ(unauthorized.error().code(), common::StatusCode::kUnauthenticated);
  EXPECT_EQ(authorizer.calls, 1U);
  EXPECT_EQ(worker.bind_calls, 0U);

  const auto result =
      receiver->receive(*encoded_request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->size(), 2U);
  EXPECT_EQ(worker.bind_calls, 1U);
  EXPECT_EQ(worker.execute_calls, 1U);
  const auto expected = receiver_definitions();
  for (std::size_t ordinal = 0U; ordinal < result->size(); ++ordinal) {
    auto resources = query::QueryResourceContext::create(1U << 20U).value();
    auto decoded = decode_distributed_vector_aggregate_query_response_v2_exact((*result)[ordinal],
                                                                               expected, resources);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
    EXPECT_EQ(decoded->status_code, common::StatusCode::kOk);
    ASSERT_TRUE(decoded->payload.has_value());
    auto& decoded_payload = decoded->payload;
    if (!decoded_payload.has_value()) {
      ADD_FAILURE() << "expected aggregate payload " << ordinal;
      return;
    }
    EXPECT_EQ(decoded_payload->sequence, ordinal + 1U);
    EXPECT_EQ(decoded_payload->aggregate_ordinal, ordinal);
    EXPECT_EQ(decoded_payload->terminal, ordinal + 1U == result->size());
    auto scalar = std::move(decoded_payload->state).take_result();
    ASSERT_TRUE(scalar.has_value());
    EXPECT_EQ(std::get<std::int64_t>(scalar->storage()), static_cast<std::int64_t>(ordinal + 1U));
  }
}

TEST(DistributedVectorAggregateQueryReceiverV2Test,
     RejectsRouteModeAndDefinitionFailuresBeforeExecution) {
  Authorizer authorizer;
  AggregateWorker worker;
  EXPECT_EQ(DistributedVectorAggregateQueryReceiverV2::create({}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(DistributedVectorAggregateQueryReceiverV2::create(
                {.local_node_id = 2U,
                 .authorizer = &authorizer,
                 .worker = &worker,
                 .maximum_response_frames = query::kMaximumUngroupedAggregateWidth + 1U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  auto receiver = DistributedVectorAggregateQueryReceiverV2::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());

  const auto wrong_target = encode_distributed_vector_query_request_v2(
      {.source_node_id = 1U, .target_node_id = 3U, .dispatch = receiver_dispatch()});
  ASSERT_TRUE(wrong_target.has_value());
  EXPECT_EQ(
      receiver->receive(*wrong_target, {.authorized = true, .principal_id = 91U}).error().code(),
      common::StatusCode::kUnavailable);
  EXPECT_EQ(worker.bind_calls, 0U);

  auto row_dispatch = receiver_dispatch();
  row_dispatch.dispatch.plan = {.mode = query::DistributedVectorPlanMode::kRows,
                                .row_output_indices = {0U}};
  row_dispatch.result_schema.columns.erase(row_dispatch.result_schema.columns.begin() + 1,
                                           row_dispatch.result_schema.columns.end());
  const auto row_request = encode_distributed_vector_query_request_v2(
      {.source_node_id = 1U, .target_node_id = 2U, .dispatch = row_dispatch});
  ASSERT_TRUE(row_request.has_value()) << row_request.error().to_string();
  EXPECT_EQ(
      receiver->receive(*row_request, {.authorized = true, .principal_id = 91U}).error().code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(worker.bind_calls, 0U);

  worker.wrong_bound_definition = true;
  const auto aggregate_request = encode_distributed_vector_query_request_v2(
      {.source_node_id = 1U, .target_node_id = 2U, .dispatch = receiver_dispatch()});
  ASSERT_TRUE(aggregate_request.has_value());
  EXPECT_EQ(receiver->receive(*aggregate_request, {.authorized = true, .principal_id = 91U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(worker.execute_calls, 0U);

  worker.wrong_bound_definition = false;
  worker.throw_bind = true;
  EXPECT_EQ(receiver->receive(*aggregate_request, {.authorized = true, .principal_id = 91U})
                .error()
                .code(),
            common::StatusCode::kInternal);
  EXPECT_EQ(worker.execute_calls, 0U);
}

TEST(DistributedVectorAggregateQueryReceiverV2Test,
     ReturnsCorrelatedFailureHintAndEnforcesPublicationBounds) {
  Authorizer authorizer;
  LeaderHintProvider hint_provider;
  const auto encoded_request = encode_distributed_vector_query_request_v2(
      {.source_node_id = 1U, .target_node_id = 2U, .dispatch = receiver_dispatch()});
  ASSERT_TRUE(encoded_request.has_value());
  const auto expected = receiver_definitions();

  AggregateWorker unavailable_worker;
  unavailable_worker.execute_failure =
      common::Status{common::StatusCode::kUnavailable, "leadership changed"};
  auto unavailable_receiver =
      DistributedVectorAggregateQueryReceiverV2::create({.local_node_id = 2U,
                                                         .authorizer = &authorizer,
                                                         .worker = &unavailable_worker,
                                                         .leader_hint_provider = &hint_provider});
  ASSERT_TRUE(unavailable_receiver.has_value());
  auto unavailable =
      unavailable_receiver->receive(*encoded_request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(unavailable.has_value());
  ASSERT_EQ(unavailable->size(), 1U);
  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  auto decoded = decode_distributed_vector_aggregate_query_response_v2_exact(unavailable->front(),
                                                                             expected, resources);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->status_code, common::StatusCode::kUnavailable);
  ASSERT_TRUE(decoded->leader_hint.has_value());
  const auto& leader_hint = decoded->leader_hint;
  if (!leader_hint.has_value()) {
    ADD_FAILURE() << "expected an unavailable leader hint";
    return;
  }
  EXPECT_EQ(leader_hint->node_id, 3U);
  EXPECT_EQ(hint_provider.last_tablet, tablet(14U));
  EXPECT_EQ(hint_provider.last_group, uuid(16U));

  AggregateWorker frame_worker;
  auto frame_receiver =
      DistributedVectorAggregateQueryReceiverV2::create({.local_node_id = 2U,
                                                         .authorizer = &authorizer,
                                                         .worker = &frame_worker,
                                                         .maximum_response_frames = 1U});
  ASSERT_TRUE(frame_receiver.has_value());
  auto frame_limited =
      frame_receiver->receive(*encoded_request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(frame_limited.has_value());
  ASSERT_EQ(frame_limited->size(), 1U);
  decoded = decode_distributed_vector_aggregate_query_response_v2_exact(frame_limited->front(),
                                                                        expected, resources);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->status_code, common::StatusCode::kResourceExhausted);

  AggregateWorker byte_worker;
  auto byte_receiver = DistributedVectorAggregateQueryReceiverV2::create(
      {.local_node_id = 2U,
       .authorizer = &authorizer,
       .worker = &byte_worker,
       .maximum_response_bytes = kDistributedVectorAggregateQueryResponseV2HeaderSize +
                                 kDistributedVectorAggregateQueryResponseV2TrailerSize});
  ASSERT_TRUE(byte_receiver.has_value());
  auto byte_limited =
      byte_receiver->receive(*encoded_request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(byte_limited.has_value());
  ASSERT_EQ(byte_limited->size(), 1U);
  decoded = decode_distributed_vector_aggregate_query_response_v2_exact(byte_limited->front(),
                                                                        expected, resources);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->status_code, common::StatusCode::kResourceExhausted);
}

TEST(DistributedVectorAggregateQueryReceiverV2Test,
     RejectsWorkerContractViolationsWithoutPublishingPrefixes) {
  Authorizer authorizer;
  const auto encoded_request = encode_distributed_vector_query_request_v2(
      {.source_node_id = 1U, .target_node_id = 2U, .dispatch = receiver_dispatch()});
  ASSERT_TRUE(encoded_request.has_value());

  const auto verify = [&](AggregateWorker& worker) {
    auto receiver = DistributedVectorAggregateQueryReceiverV2::create(
        {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
    ASSERT_TRUE(receiver.has_value());
    const auto result =
        receiver->receive(*encoded_request, {.authorized = true, .principal_id = 91U});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), common::StatusCode::kInvalidArgument);
  };

  AggregateWorker changed;
  changed.changed_definitions = true;
  verify(changed);
  AggregateWorker incomplete;
  incomplete.incomplete_messages = true;
  verify(incomplete);
  AggregateWorker wrong_sequence;
  wrong_sequence.wrong_sequence = true;
  verify(wrong_sequence);

  AggregateWorker threw;
  threw.throw_execute = true;
  auto receiver = DistributedVectorAggregateQueryReceiverV2::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &threw});
  ASSERT_TRUE(receiver.has_value());
  const auto internal =
      receiver->receive(*encoded_request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(internal.has_value());
  const auto expected = receiver_definitions();
  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  const auto decoded = decode_distributed_vector_aggregate_query_response_v2_exact(
      internal->front(), expected, resources);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->status_code, common::StatusCode::kInternal);
}

// Optional values below are asserted present before access.
// NOLINTBEGIN(bugprone-unchecked-optional-access)
TEST(DistributedVectorAggregateQuerySenderV2Test,
     ReconstructsAndPublishesOnlyTheCompleteDefinitionBoundVector) {
  auto expected = receiver_definitions();
  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  auto sender = DistributedVectorAggregateQuerySenderV2::create(1U, receiver_dispatch(),
                                                                std::move(expected), resources);
  ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
  const auto now = DistributedVectorAggregateQuerySenderV2::TimePoint{};
  auto attempt = sender->begin_attempt(now);
  ASSERT_TRUE(attempt.has_value()) << attempt.error().to_string();
  EXPECT_EQ(attempt->attempt_number, 1U);
  EXPECT_EQ(attempt->target_node_id, 2U);
  auto request = decode_distributed_vector_query_request_v2_exact(attempt->request_bytes);
  ASSERT_TRUE(request.has_value());
  EXPECT_EQ(request->dispatch, receiver_dispatch());

  auto wrong_sequence = sender_responses();
  wrong_sequence.back().payload->sequence = 7U;
  EXPECT_EQ(sender->accept_responses(wrong_sequence, now).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kWaitingForResponse);
  EXPECT_FALSE(sender->result().has_value());

  auto incomplete = sender_responses();
  incomplete.pop_back();
  EXPECT_EQ(sender->accept_responses(incomplete, now).code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kWaitingForResponse);

  auto responses = sender_responses();
  std::size_t encoded_bytes{};
  for (const auto& response_value : responses) {
    auto encoded = encode_distributed_vector_aggregate_query_response_v2(response_value,
                                                                         sender->definitions());
    ASSERT_TRUE(encoded.has_value());
    encoded_bytes += encoded->size();
  }
  auto bounded_definitions = receiver_definitions();
  auto bounded = DistributedVectorAggregateQuerySenderV2::create(
      1U, receiver_dispatch(), std::move(bounded_definitions), resources,
      {.maximum_response_frames = 2U, .maximum_response_bytes = encoded_bytes - 1U});
  ASSERT_TRUE(bounded.has_value()) << bounded.error().to_string();
  ASSERT_TRUE(bounded->begin_attempt(now).has_value());
  EXPECT_EQ(bounded->accept_responses(responses, now).code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(bounded->state(), DistributedQuerySenderState::kWaitingForResponse);
  EXPECT_FALSE(bounded->result().has_value());

  EXPECT_TRUE(sender->accept_responses(responses, now).is_ok());
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kSucceeded);
  ASSERT_TRUE(sender->result().has_value());
  ASSERT_EQ(sender->result()->size(), 2U);
  EXPECT_EQ(sender->result()->front().aggregate_ordinal, 0U);
  EXPECT_TRUE(sender->result()->back().terminal);
  EXPECT_EQ(sender->result()->back().state.definition(), sender->definitions().back());
  EXPECT_FALSE(sender->begin_attempt(now).has_value());

  auto too_narrow_definitions = receiver_definitions();
  EXPECT_FALSE(DistributedVectorAggregateQuerySenderV2::create(
                   1U, receiver_dispatch(), std::move(too_narrow_definitions), resources,
                   {.maximum_response_frames = 1U})
                   .has_value());
}

TEST(DistributedVectorAggregateQuerySenderV2Test,
     RetriesWholeImmutableAttemptsAndKeepsHintsAdvisory) {
  auto expected = receiver_definitions();
  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  auto sender = DistributedVectorAggregateQuerySenderV2::create(
      1U, receiver_dispatch(), std::move(expected), resources,
      {.retry = {.maximum_attempts = 3U,
                 .initial_backoff = std::chrono::milliseconds{10},
                 .maximum_backoff = std::chrono::milliseconds{20}},
       .maximum_response_frames = 2U,
       .maximum_response_bytes = 4096U});
  ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
  const auto start = DistributedVectorAggregateQuerySenderV2::TimePoint{};
  auto first = sender->begin_attempt(start);
  ASSERT_TRUE(first.has_value());
  const DistributedVectorAggregateQueryResponseV2 unavailable_response{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(11U),
      .tablet_id = tablet(14U),
      .status_code = common::StatusCode::kUnavailable,
      .leader_hint = DistributedQueryLeaderHint{3U, 9U}};
  auto mismatched =
      DistributedVectorAggregateQueryResponseV2{.source_node_id = 3U,
                                                .target_node_id = 1U,
                                                .query_id = uuid(11U),
                                                .tablet_id = tablet(14U),
                                                .status_code = common::StatusCode::kUnavailable};
  EXPECT_EQ(sender->accept_responses(std::span{&mismatched, 1U}, start).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kWaitingForResponse);
  EXPECT_TRUE(sender->accept_responses(std::span{&unavailable_response, 1U}, start).is_ok());
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kBackoff);
  ASSERT_TRUE(sender->suggested_leader().has_value());
  EXPECT_EQ(sender->suggested_leader()->node_id, 3U);
  EXPECT_EQ(*sender->next_attempt_not_before(), start + std::chrono::milliseconds{10});
  EXPECT_FALSE(sender->begin_attempt(start + std::chrono::milliseconds{9}).has_value());
  auto second = sender->begin_attempt(start + std::chrono::milliseconds{10});
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(second->target_node_id, 2U);
  EXPECT_EQ(second->request_bytes, first->request_bytes);
  EXPECT_TRUE(sender
                  ->record_transport_failure(common::StatusCode::kIoError,
                                             start + std::chrono::milliseconds{10})
                  .is_ok());
  EXPECT_EQ(*sender->next_attempt_not_before(), start + std::chrono::milliseconds{30});
  auto third = sender->begin_attempt(start + std::chrono::milliseconds{30});
  ASSERT_TRUE(third.has_value());
  EXPECT_EQ(third->request_bytes, first->request_bytes);
  EXPECT_TRUE(sender
                  ->record_transport_failure(common::StatusCode::kInvalidArgument,
                                             start + std::chrono::milliseconds{30})
                  .is_ok());
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kFailed);
  EXPECT_EQ(sender->attempts_started(), 3U);
  EXPECT_FALSE(sender->result().has_value());
}
// NOLINTEND(bugprone-unchecked-optional-access)

} // namespace
} // namespace chronos::cluster
