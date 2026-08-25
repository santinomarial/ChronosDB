#include "chronos/cluster/distributed_vector_aggregate_query_transport_v2.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_query_transport_v2.hpp"
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
#include <string>
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

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = string_type(), .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] query::DistributedVectorFragmentDispatchV2 grouped_dispatch() {
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
           .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
                    .group_key_input_indices = {0U},
                    .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}}}},
      .result_schema = {.columns = {{.name = "region", .type = string_type(), .nullable = false},
                                    {.name = "count", .type = int64, .nullable = false}}}};
}

[[nodiscard]] query::DistributedVectorGroupedAggregateExchangeMessage
grouped_message(const std::size_t ordinal, const std::size_t group_count,
                const std::optional<std::uint64_t> sequence = std::nullopt) {
  const auto expected_aggregates = aggregates();
  auto state = query::MergeableVectorAggregateState::create(expected_aggregates.front()).value();
  for (std::size_t count = 0U; count <= ordinal; ++count)
    EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(
      query::ScalarValue::text(string_type(), ordinal == 0U ? "east" : "west").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  return {{.query_id = uuid(11U),
           .tablet_id = tablet(14U),
           .sequence = sequence.value_or(ordinal + 1U),
           .group_ordinal = static_cast<std::uint32_t>(ordinal),
           .group_count = static_cast<std::uint32_t>(group_count),
           .terminal = ordinal + 1U == group_count,
           .empty = false},
          std::move(values),
          std::move(states)};
}

[[nodiscard]] std::vector<DistributedVectorGroupedAggregateQueryResponseV2> grouped_responses() {
  std::vector<DistributedVectorGroupedAggregateQueryResponseV2> result;
  for (std::size_t ordinal = 0U; ordinal < 2U; ++ordinal) {
    result.push_back({.source_node_id = 2U,
                      .target_node_id = 1U,
                      .query_id = uuid(11U),
                      .tablet_id = tablet(14U),
                      .status_code = common::StatusCode::kOk,
                      .payload = grouped_message(ordinal, 2U)});
  }
  return result;
}

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId claimed_node_id) const override {
    ++calls;
    return common::Result<bool>{principal_id == 91U && claimed_node_id == 1U};
  }

  mutable std::size_t calls{};
};

class GroupedWorker final : public DistributedVectorGroupedAggregateQueryWorkerServiceV2 {
public:
  common::Result<query::DistributedVectorGroupedAggregateAuthority>
  bind_authority(const query::DistributedVectorFragmentDispatchV2&) override {
    ++bind_calls;
    if (throw_bind)
      throw std::runtime_error{"bind failure"};
    auto bound_keys = keys();
    if (wrong_bound_key)
      bound_keys.front().nullable = true;
    return query::DistributedVectorGroupedAggregateAuthority{std::move(bound_keys), aggregates()};
  }

  common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
  execute(const query::DistributedVectorFragmentDispatchV2&) override {
    ++execute_calls;
    if (throw_execute)
      throw std::runtime_error{"execute failure"};
    if (execute_failure.has_value())
      return common::make_unexpected(*execute_failure);
    query::DistributedVectorGroupedAggregateWorkerResultV2 result{
        .authority = {.keys = keys(), .aggregates = aggregates()},
        .input_rows = 3U,
        .group_count = 2U};
    for (std::size_t ordinal = 0U; ordinal < 2U; ++ordinal) {
      auto message = grouped_message(
          ordinal, 2U,
          wrong_sequence && ordinal == 1U ? std::optional<std::uint64_t>{7U} : std::nullopt);
      auto encoded = query::encode_distributed_vector_grouped_aggregate_exchange_message(
          message, result.authority.keys, result.authority.aggregates);
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      result.encoded_bytes += encoded->bytes().size();
      result.messages.push_back(std::move(*encoded));
    }
    if (changed_authority)
      result.authority.keys.front().nullable = true;
    if (incomplete_messages)
      result.messages.pop_back();
    return result;
  }

  std::size_t bind_calls{};
  std::size_t execute_calls{};
  std::optional<common::Status> execute_failure;
  bool wrong_bound_key{};
  bool changed_authority{};
  bool incomplete_messages{};
  bool wrong_sequence{};
  bool throw_bind{};
  bool throw_execute{};
};

class LeaderHintProvider final : public DistributedQueryLeaderHintProvider {
public:
  common::Result<std::optional<DistributedQueryLeaderHint>>
  current_leader_hint(const schema::TabletId&, const raft::GroupId&) const override {
    ++calls;
    return DistributedQueryLeaderHint{3U, 17U};
  }

  mutable std::size_t calls{};
};

[[nodiscard]] DistributedVectorGroupedAggregateQueryResponseV2 response() {
  const auto expected_aggregates = aggregates();
  auto state = query::MergeableVectorAggregateState::create(expected_aggregates.front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(string_type(), "west").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  return {.source_node_id = 2U,
          .target_node_id = 1U,
          .query_id = uuid(1U),
          .tablet_id = tablet(2U),
          .status_code = common::StatusCode::kOk,
          .payload =
              query::DistributedVectorGroupedAggregateExchangeMessage{{.query_id = uuid(1U),
                                                                       .tablet_id = tablet(2U),
                                                                       .sequence = 1U,
                                                                       .group_ordinal = 0U,
                                                                       .group_count = 1U,
                                                                       .terminal = true,
                                                                       .empty = false},
                                                                      std::move(values),
                                                                      std::move(states)},
          .leader_hint = DistributedQueryLeaderHint{3U, 9U}};
}

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
      kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize,
      bytes.size() - kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize -
          kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize);
  store_u32(bytes, 80U, payload.empty() ? 0U : common::crc32c(payload));
  store_u32(bytes, 108U, common::crc32c(common::ByteView{bytes}.first(108U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

TEST(DistributedVectorGroupedAggregateQueryTransportV2Test,
     RoundTripsAuthorityBoundStateAndCorrelatedFailure) {
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  const auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(
      response(), expected_keys, expected_aggregates);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const std::array<std::byte, 8U> magic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                        std::byte{'V'}, std::byte{'G'}, std::byte{'R'},
                                        std::byte{'P'}, std::byte{'2'}};
  EXPECT_TRUE(std::ranges::equal(common::ByteView{*encoded}.first(magic.size()), magic));

  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  auto decoded = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
      *encoded, expected_keys, expected_aggregates, resources);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->source_node_id, 2U);
  EXPECT_EQ(decoded->target_node_id, 1U);
  ASSERT_TRUE(decoded->payload.has_value());
  EXPECT_EQ(std::get<std::string>(decoded->payload->keys().front().storage()), "west");
  auto states = std::move(*decoded->payload).take_states();
  ASSERT_EQ(states.size(), 1U);
  auto value = std::move(states.front()).take_result();
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(std::get<std::int64_t>(value->storage()), 2);
  ASSERT_TRUE(decoded->leader_hint.has_value());
  EXPECT_EQ(decoded->leader_hint->node_id, 3U);

  const DistributedVectorGroupedAggregateQueryResponseV2 failure{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = tablet(2U),
      .status_code = common::StatusCode::kUnavailable,
      .leader_hint = DistributedQueryLeaderHint{4U, 10U}};
  const auto encoded_failure = encode_distributed_vector_grouped_aggregate_query_response_v2(
      failure, expected_keys, expected_aggregates);
  ASSERT_TRUE(encoded_failure.has_value());
  const auto decoded_failure = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
      *encoded_failure, expected_keys, expected_aggregates, resources);
  ASSERT_TRUE(decoded_failure.has_value());
  EXPECT_EQ(decoded_failure->status_code, common::StatusCode::kUnavailable);
  EXPECT_FALSE(decoded_failure->payload.has_value());
}

TEST(DistributedVectorGroupedAggregateQueryTransportV2Test,
     RejectsDamageVersionTypeConfusionCorrelationAndLowerBounds) {
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  const auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(
                           response(), expected_keys, expected_aggregates)
                           .value();
  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  EXPECT_EQ(decode_distributed_vector_aggregate_query_response_v2_exact(
                encoded, expected_aggregates, resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  std::vector<std::byte> future = encoded;
  store_u16(future, 8U, 3U);
  rewrite_checksums(future);
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
                future, expected_keys, expected_aggregates, resources)
                .error()
                .code(),
            common::StatusCode::kNotSupported);

  std::vector<std::byte> nested_damage = encoded;
  nested_damage[kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize] ^= std::byte{1U};
  rewrite_checksums(nested_damage);
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
                nested_damage, expected_keys, expected_aggregates, resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  auto wrong_keys = keys();
  wrong_keys.front().nullable = true;
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
                encoded, wrong_keys, expected_aggregates, resources)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  auto owned_keys = keys();
  auto owned_aggregates = aggregates();
  DistributedVectorGroupedAggregateQueryResponseV2Reader lower_reader{
      std::move(owned_keys), std::move(owned_aggregates),
      query::QueryResourceContext::create(1U << 20U).value(), encoded.size() - 1U};
  EXPECT_EQ(lower_reader.consume(encoded).error().code(), common::StatusCode::kResourceExhausted);

  auto uncorrelated = response();
  uncorrelated.query_id = uuid(9U);
  EXPECT_EQ(encode_distributed_vector_grouped_aggregate_query_response_v2(
                uncorrelated, expected_keys, expected_aggregates)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorGroupedAggregateQueryTransportV2Test,
     ReaderOwnsEverySplitLeavesSuccessorAndFailsSticky) {
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  const auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(
                           response(), expected_keys, expected_aggregates)
                           .value();
  for (std::size_t split = 0U; split <= encoded.size(); ++split) {
    auto owned_keys = keys();
    auto owned_aggregates = aggregates();
    DistributedVectorGroupedAggregateQueryResponseV2Reader reader{
        std::move(owned_keys), std::move(owned_aggregates),
        query::QueryResourceContext::create(1U << 20U).value()};
    const auto first = reader.consume(common::ByteView{encoded}.first(split));
    ASSERT_TRUE(first.has_value()) << first.error().to_string();
    EXPECT_EQ(first->consumed_bytes, split);
    if (split != encoded.size())
      EXPECT_FALSE(first->response.has_value());
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
  auto owned_keys = keys();
  auto owned_aggregates = aggregates();
  DistributedVectorGroupedAggregateQueryResponseV2Reader reader{
      std::move(owned_keys), std::move(owned_aggregates),
      query::QueryResourceContext::create(1U << 20U).value()};
  const auto failed = reader.consume(damaged);
  ASSERT_FALSE(failed.has_value());
  EXPECT_TRUE(reader.failed());
  EXPECT_EQ(reader.consume({}).error(), failed.error());
}

TEST(DistributedVectorGroupedAggregateQueryTransportV2Test, CursorOwnsValidatedShortWriteProgress) {
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  auto cursor = DistributedVectorGroupedAggregateQueryResponseV2WriteCursor::create(
      response(), expected_keys, expected_aggregates);
  ASSERT_TRUE(cursor.has_value()) << cursor.error().to_string();
  const std::size_t size = cursor->pending_write().size();
  ASSERT_GT(size, 2U);
  EXPECT_TRUE(cursor->consume_written(1U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 1U);
  EXPECT_EQ(cursor->consume_written(size).code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(cursor->written_bytes(), 1U);
  DistributedVectorGroupedAggregateQueryResponseV2WriteCursor moved{std::move(*cursor)};
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(moved.consume_written(size - 1U).is_ok());
  EXPECT_TRUE(moved.complete());
}

TEST(DistributedVectorGroupedAggregateQueryReceiverV2Test,
     AuthenticatesBindsAndPublishesOnlyACompleteGroupedStream) {
  Authorizer authorizer;
  GroupedWorker worker;
  auto receiver = DistributedVectorGroupedAggregateQueryReceiverV2::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value()) << receiver.error().to_string();
  const auto request = encode_distributed_vector_query_request_v2(
      {.source_node_id = 1U, .target_node_id = 2U, .dispatch = grouped_dispatch()});
  ASSERT_TRUE(request.has_value());

  EXPECT_EQ(receiver->receive(*request, {}).error().code(), common::StatusCode::kUnauthenticated);
  EXPECT_EQ(authorizer.calls, 0U);
  EXPECT_EQ(worker.bind_calls, 0U);
  EXPECT_EQ(receiver->receive(*request, {.authorized = true, .principal_id = 92U}).error().code(),
            common::StatusCode::kUnauthenticated);
  EXPECT_EQ(worker.bind_calls, 0U);

  auto result = receiver->receive_bound(*request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->encoded_responses.size(), 2U);
  EXPECT_EQ(worker.bind_calls, 1U);
  EXPECT_EQ(worker.execute_calls, 1U);
  for (std::size_t ordinal = 0U; ordinal < result->encoded_responses.size(); ++ordinal) {
    auto resources = query::QueryResourceContext::create(1U << 20U).value();
    auto decoded = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
        result->encoded_responses[ordinal], result->authority.keys, result->authority.aggregates,
        resources);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
    ASSERT_TRUE(decoded->payload.has_value());
    EXPECT_EQ(decoded->payload->position().group_ordinal, ordinal);
    EXPECT_EQ(decoded->payload->position().group_count, 2U);
    EXPECT_EQ(decoded->payload->position().terminal, ordinal == 1U);
  }
}

TEST(DistributedVectorGroupedAggregateQueryReceiverV2Test,
     RejectsRouteModeAndAuthorityFailuresBeforeExecution) {
  Authorizer authorizer;
  GroupedWorker worker;
  EXPECT_EQ(DistributedVectorGroupedAggregateQueryReceiverV2::create({}).error().code(),
            common::StatusCode::kInvalidArgument);
  auto receiver = DistributedVectorGroupedAggregateQueryReceiverV2::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
  ASSERT_TRUE(receiver.has_value());

  const auto wrong_target = encode_distributed_vector_query_request_v2(
      {.source_node_id = 1U, .target_node_id = 3U, .dispatch = grouped_dispatch()});
  ASSERT_TRUE(wrong_target.has_value());
  EXPECT_EQ(
      receiver->receive(*wrong_target, {.authorized = true, .principal_id = 91U}).error().code(),
      common::StatusCode::kUnavailable);
  EXPECT_EQ(worker.bind_calls, 0U);

  auto rows = grouped_dispatch();
  rows.dispatch.plan = {.mode = query::DistributedVectorPlanMode::kRows,
                        .row_output_indices = {0U}};
  rows.result_schema.columns.pop_back();
  const auto row_request = encode_distributed_vector_query_request_v2(
      {.source_node_id = 1U, .target_node_id = 2U, .dispatch = rows});
  ASSERT_TRUE(row_request.has_value());
  EXPECT_EQ(
      receiver->receive(*row_request, {.authorized = true, .principal_id = 91U}).error().code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(worker.bind_calls, 0U);

  const auto request = encode_distributed_vector_query_request_v2(
      {.source_node_id = 1U, .target_node_id = 2U, .dispatch = grouped_dispatch()});
  ASSERT_TRUE(request.has_value());
  worker.wrong_bound_key = true;
  EXPECT_EQ(receiver->receive(*request, {.authorized = true, .principal_id = 91U}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(worker.execute_calls, 0U);
  worker.wrong_bound_key = false;
  worker.throw_bind = true;
  EXPECT_EQ(receiver->receive(*request, {.authorized = true, .principal_id = 91U}).error().code(),
            common::StatusCode::kInternal);
  EXPECT_EQ(worker.execute_calls, 0U);
}

TEST(DistributedVectorGroupedAggregateQueryReceiverV2Test,
     ReturnsCorrelatedFailuresAndRejectsContractViolationsWithoutPrefixes) {
  Authorizer authorizer;
  LeaderHintProvider hints;
  const auto request = encode_distributed_vector_query_request_v2(
      {.source_node_id = 1U, .target_node_id = 2U, .dispatch = grouped_dispatch()});
  ASSERT_TRUE(request.has_value());

  GroupedWorker unavailable_worker;
  unavailable_worker.execute_failure =
      common::Status{common::StatusCode::kUnavailable, "leadership changed"};
  auto unavailable_receiver =
      DistributedVectorGroupedAggregateQueryReceiverV2::create({.local_node_id = 2U,
                                                                .authorizer = &authorizer,
                                                                .worker = &unavailable_worker,
                                                                .leader_hint_provider = &hints});
  ASSERT_TRUE(unavailable_receiver.has_value());
  auto unavailable =
      unavailable_receiver->receive(*request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(unavailable.has_value());
  ASSERT_EQ(unavailable->size(), 1U);
  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  auto decoded = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
      unavailable->front(), keys(), aggregates(), resources);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->status_code, common::StatusCode::kUnavailable);
  ASSERT_TRUE(decoded->leader_hint.has_value());
  EXPECT_EQ(decoded->leader_hint->node_id, 3U);

  const auto verify_invalid = [&](GroupedWorker& worker, const char* label) {
    SCOPED_TRACE(label);
    auto bounded = DistributedVectorGroupedAggregateQueryReceiverV2::create(
        {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
    ASSERT_TRUE(bounded.has_value());
    const auto rejected = bounded->receive(*request, {.authorized = true, .principal_id = 91U});
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
  };
  GroupedWorker changed;
  changed.changed_authority = true;
  verify_invalid(changed, "changed authority");
  GroupedWorker incomplete;
  incomplete.incomplete_messages = true;
  verify_invalid(incomplete, "incomplete stream");
  GroupedWorker wrong_sequence;
  wrong_sequence.wrong_sequence = true;
  auto invalid_worker_response = DistributedVectorGroupedAggregateQueryReceiverV2::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &wrong_sequence});
  ASSERT_TRUE(invalid_worker_response.has_value());
  auto invalid_worker =
      invalid_worker_response->receive(*request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(invalid_worker.has_value());
  ASSERT_EQ(invalid_worker->size(), 1U);
  decoded = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
      invalid_worker->front(), keys(), aggregates(), resources);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->status_code, common::StatusCode::kInvalidArgument);

  GroupedWorker byte_worker;
  auto byte_receiver = DistributedVectorGroupedAggregateQueryReceiverV2::create(
      {.local_node_id = 2U,
       .authorizer = &authorizer,
       .worker = &byte_worker,
       .maximum_response_bytes = kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
                                 kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize});
  ASSERT_TRUE(byte_receiver.has_value());
  auto limited = byte_receiver->receive(*request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(limited.has_value());
  ASSERT_EQ(limited->size(), 1U);
  decoded = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
      limited->front(), keys(), aggregates(), resources);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->status_code, common::StatusCode::kResourceExhausted);

  GroupedWorker throwing;
  throwing.throw_execute = true;
  auto throwing_receiver = DistributedVectorGroupedAggregateQueryReceiverV2::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .worker = &throwing});
  ASSERT_TRUE(throwing_receiver.has_value());
  auto internal = throwing_receiver->receive(*request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(internal.has_value());
  decoded = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
      internal->front(), keys(), aggregates(), resources);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->status_code, common::StatusCode::kInternal);
}

// Optional values below are asserted present before access.
// NOLINTBEGIN(bugprone-unchecked-optional-access)
TEST(DistributedVectorGroupedAggregateQuerySenderV2Test,
     ReconstructsOnlyACompleteCanonicalGroupedStream) {
  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  auto sender = DistributedVectorGroupedAggregateQuerySenderV2::create(
      1U, grouped_dispatch(), keys(), aggregates(), resources);
  ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
  const auto now = DistributedVectorGroupedAggregateQuerySenderV2::TimePoint{};
  auto attempt = sender->begin_attempt(now);
  ASSERT_TRUE(attempt.has_value());
  auto request = decode_distributed_vector_query_request_v2_exact(attempt->request_bytes);
  ASSERT_TRUE(request.has_value());
  EXPECT_EQ(request->dispatch, grouped_dispatch());

  auto incomplete = grouped_responses();
  incomplete.pop_back();
  EXPECT_EQ(sender->accept_responses(incomplete, now).code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kWaitingForResponse);
  EXPECT_FALSE(sender->result().has_value());

  auto wrong = grouped_responses();
  wrong[1] = {.source_node_id = 2U,
              .target_node_id = 1U,
              .query_id = uuid(11U),
              .tablet_id = tablet(14U),
              .status_code = common::StatusCode::kOk,
              .payload = grouped_message(1U, 2U, 7U)};
  EXPECT_EQ(sender->accept_responses(wrong, now).code(), common::StatusCode::kInvalidArgument);

  auto responses = grouped_responses();
  EXPECT_TRUE(sender->accept_responses(responses, now).is_ok());
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kSucceeded);
  ASSERT_TRUE(sender->result().has_value());
  ASSERT_EQ(sender->result()->size(), 2U);
  for (std::size_t ordinal = 0U; ordinal < sender->result()->size(); ++ordinal) {
    auto decode_resources = query::QueryResourceContext::create(1U << 20U).value();
    auto message = query::decode_distributed_vector_grouped_aggregate_exchange_message_exact(
        (*sender->result())[ordinal].bytes(), sender->keys(), sender->aggregates(),
        decode_resources);
    ASSERT_TRUE(message.has_value()) << message.error().to_string();
    EXPECT_EQ(message->position().group_ordinal, ordinal);
    EXPECT_EQ(message->position().terminal, ordinal == 1U);
  }
  EXPECT_FALSE(sender->begin_attempt(now).has_value());
}

TEST(DistributedVectorGroupedAggregateQuerySenderV2Test,
     RetriesWholeImmutableAttemptsAndKeepsHintsAdvisory) {
  auto sender = DistributedVectorGroupedAggregateQuerySenderV2::create(
      1U, grouped_dispatch(), keys(), aggregates(),
      query::QueryResourceContext::create(1U << 20U).value(),
      {.retry = {.maximum_attempts = 3U,
                 .initial_backoff = std::chrono::milliseconds{10},
                 .maximum_backoff = std::chrono::milliseconds{20}},
       .maximum_response_frames = 2U,
       .maximum_response_bytes = 4096U});
  ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
  const auto start = DistributedVectorGroupedAggregateQuerySenderV2::TimePoint{};
  auto first = sender->begin_attempt(start);
  ASSERT_TRUE(first.has_value());
  const DistributedVectorGroupedAggregateQueryResponseV2 unavailable{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(11U),
      .tablet_id = tablet(14U),
      .status_code = common::StatusCode::kUnavailable,
      .leader_hint = DistributedQueryLeaderHint{3U, 9U}};
  EXPECT_TRUE(sender->accept_responses(std::span{&unavailable, 1U}, start).is_ok());
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kBackoff);
  ASSERT_TRUE(sender->suggested_leader().has_value());
  EXPECT_EQ(sender->suggested_leader()->node_id, 3U);
  EXPECT_FALSE(sender->begin_attempt(start + std::chrono::milliseconds{9}).has_value());
  auto second = sender->begin_attempt(start + std::chrono::milliseconds{10});
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(second->request_bytes, first->request_bytes);
  EXPECT_TRUE(sender
                  ->record_transport_failure(common::StatusCode::kIoError,
                                             start + std::chrono::milliseconds{10})
                  .is_ok());
  auto third = sender->begin_attempt(start + std::chrono::milliseconds{30});
  ASSERT_TRUE(third.has_value());
  EXPECT_EQ(third->request_bytes, first->request_bytes);
  EXPECT_TRUE(sender
                  ->record_transport_failure(common::StatusCode::kInvalidArgument,
                                             start + std::chrono::milliseconds{30})
                  .is_ok());
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kFailed);
  EXPECT_FALSE(sender->result().has_value());
}
// NOLINTEND(bugprone-unchecked-optional-access)

} // namespace
} // namespace chronos::cluster
