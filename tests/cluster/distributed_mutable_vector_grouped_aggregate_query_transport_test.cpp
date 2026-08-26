#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_transport.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
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

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
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

[[nodiscard]] query::DistributedMutableVectorFragment fragment() {
  const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  std::vector<query::VectorExpressionInstruction> instructions{
      query::VectorInputExpression{0U, string_type(), false},
      query::VectorUnaryExpression{query::VectorUnaryOperation::kUpperAscii, 0U}};
  return {
      .query_id = uuid(1U),
      .database_id = id<manifest::DatabaseId>(2U),
      .table_id = id<schema::TableId>(3U),
      .tablet_id = id<schema::TabletId>(4U),
      .destination_schema_id = id<schema::SchemaId>(5U),
      .raft_group_id = uuid(6U),
      .serving_node = 2U,
      .applied_position = 10U,
      .observed_leader_commit_position = 10U,
      .placement_epoch = 7U,
      .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
      .linearizable_barrier = raft::ReadBarrier{.term = 2U, .context = 3U, .read_index = 10U},
      .destination_column_ordinals = {0U},
      .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
               .group_key_input_indices = {0U},
               .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}}},
      .result_schema = {.columns = {{.name = "region", .type = string_type(), .nullable = false},
                                    {.name = "count", .type = int64, .nullable = false}}},
      .pre_group_program = query::DistributedVectorPreGroupProgram{
          .outputs = {query::VectorExpression::create(std::move(instructions)).value()}}};
}

[[nodiscard]] query::DistributedVectorGroupedAggregateExchangeMessage
message(const std::size_t ordinal, const std::size_t group_count,
        const std::optional<std::uint64_t> sequence = std::nullopt) {
  const auto definitions = aggregates();
  auto state = query::MergeableVectorAggregateState::create(definitions.front()).value();
  for (std::size_t count = 0U; count <= ordinal; ++count)
    EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(
      query::ScalarValue::text(string_type(), ordinal == 0U ? "east" : "west").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  return {{.query_id = uuid(1U),
           .tablet_id = id<schema::TabletId>(4U),
           .sequence = sequence.value_or(ordinal + 1U),
           .group_ordinal = static_cast<std::uint32_t>(ordinal),
           .group_count = static_cast<std::uint32_t>(group_count),
           .terminal = ordinal + 1U == group_count,
           .empty = false},
          std::move(values),
          std::move(states)};
}

[[nodiscard]] std::vector<DistributedVectorGroupedAggregateQueryResponseV2> responses() {
  std::vector<DistributedVectorGroupedAggregateQueryResponseV2> result;
  for (std::size_t ordinal = 0U; ordinal < 2U; ++ordinal) {
    result.push_back({.source_node_id = 2U,
                      .target_node_id = 1U,
                      .query_id = uuid(1U),
                      .tablet_id = id<schema::TabletId>(4U),
                      .status_code = common::StatusCode::kOk,
                      .payload = message(ordinal, 2U)});
  }
  return result;
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

class Worker final : public DistributedMutableVectorGroupedAggregateQueryWorkerService {
public:
  common::Result<query::DistributedVectorGroupedAggregateAuthority>
  bind_authority(const query::DistributedMutableVectorFragment& received) override {
    ++bind_calls;
    last_bound = received;
    if (throw_bind)
      throw std::runtime_error{"bind failure"};
    auto bound_keys = keys();
    if (wrong_bound_key)
      bound_keys.front().nullable = true;
    return query::DistributedVectorGroupedAggregateAuthority{std::move(bound_keys), aggregates()};
  }

  common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
  execute(const query::DistributedMutableVectorFragment& received) override {
    ++execute_calls;
    last_executed = received;
    if (throw_execute)
      throw std::runtime_error{"execute failure"};
    if (failure.has_value())
      return common::make_unexpected(*failure);
    query::DistributedVectorGroupedAggregateWorkerResultV2 result{
        .authority = {.keys = keys(), .aggregates = aggregates()},
        .input_rows = 3U,
        .group_count = 2U};
    for (std::size_t ordinal = 0U; ordinal < 2U; ++ordinal) {
      auto current = message(ordinal, 2U,
                             wrong_sequence && ordinal == 1U ? std::optional<std::uint64_t>{9U}
                                                             : std::nullopt);
      auto encoded = query::encode_distributed_vector_grouped_aggregate_exchange_message(
          current, result.authority.keys, result.authority.aggregates);
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      result.encoded_bytes += encoded->bytes().size();
      result.messages.push_back(std::move(*encoded));
    }
    if (changed_authority)
      result.authority.keys.front().nullable = true;
    if (incomplete)
      result.messages.pop_back();
    return result;
  }

  std::size_t bind_calls{};
  std::size_t execute_calls{};
  std::optional<query::DistributedMutableVectorFragment> last_bound;
  std::optional<query::DistributedMutableVectorFragment> last_executed;
  std::optional<common::Status> failure;
  bool wrong_bound_key{};
  bool changed_authority{};
  bool incomplete{};
  bool wrong_sequence{};
  bool throw_bind{};
  bool throw_execute{};
};

class HintProvider final : public DistributedQueryLeaderHintProvider {
public:
  common::Result<std::optional<DistributedQueryLeaderHint>>
  current_leader_hint(const schema::TabletId&, const raft::GroupId&) const override {
    ++calls;
    return DistributedQueryLeaderHint{3U, 17U};
  }

  mutable std::size_t calls{};
};

TEST(DistributedMutableVectorGroupedAggregateQueryReceiverTest,
     AuthenticatesBindsAndPublishesOnlyACompleteCanonicalStream) {
  Authorizer authorizer;
  Worker worker;
  HintProvider hints;
  auto receiver = DistributedMutableVectorGroupedAggregateQueryReceiver::create(
      {.local_node_id = 2U,
       .authorizer = &authorizer,
       .worker = &worker,
       .leader_hint_provider = &hints});
  ASSERT_TRUE(receiver.has_value()) << receiver.error().to_string();
  const auto request =
      encode_distributed_mutable_vector_query_request({1U, 2U, fragment()}).value();

  EXPECT_EQ(receiver->receive(request, {.authorized = false, .principal_id = 91U}).error().code(),
            common::StatusCode::kUnauthenticated);
  EXPECT_EQ(authorizer.calls, 0U);
  EXPECT_EQ(worker.bind_calls, 0U);
  EXPECT_EQ(receiver->receive(request, {.authorized = true, .principal_id = 92U}).error().code(),
            common::StatusCode::kUnauthenticated);
  EXPECT_EQ(authorizer.calls, 1U);
  EXPECT_EQ(worker.bind_calls, 0U);

  auto bound = receiver->receive_bound(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(bound.has_value()) << bound.error().to_string();
  EXPECT_EQ(worker.bind_calls, 1U);
  EXPECT_EQ(worker.execute_calls, 1U);
  ASSERT_EQ(bound->authority.keys.size(), 1U);
  EXPECT_EQ(bound->authority.keys.front().column_ordinal, 0U);
  EXPECT_EQ(bound->authority.keys.front().type, string_type());
  EXPECT_FALSE(bound->authority.keys.front().nullable);
  EXPECT_EQ(bound->encoded_responses.size(), 2U);
  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  for (std::size_t ordinal = 0U; ordinal < bound->encoded_responses.size(); ++ordinal) {
    auto decoded = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
        bound->encoded_responses[ordinal], bound->authority.keys, bound->authority.aggregates,
        resources);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
    ASSERT_TRUE(decoded->payload.has_value());
    EXPECT_EQ(decoded->payload->position().sequence, ordinal + 1U);
    EXPECT_EQ(decoded->payload->position().terminal, ordinal + 1U == 2U);
  }

  worker.wrong_bound_key = true;
  EXPECT_EQ(receiver->receive(request, {.authorized = true, .principal_id = 91U}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(worker.execute_calls, 1U);
  worker.wrong_bound_key = false;
  worker.incomplete = true;
  EXPECT_EQ(receiver->receive(request, {.authorized = true, .principal_id = 91U}).error().code(),
            common::StatusCode::kInvalidArgument);
  worker.incomplete = false;
  worker.wrong_sequence = true;
  auto invalid_worker = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(invalid_worker.has_value());
  ASSERT_EQ(invalid_worker->size(), 1U);
  auto invalid_response = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
      invalid_worker->front(), bound->authority.keys, bound->authority.aggregates, resources);
  ASSERT_TRUE(invalid_response.has_value());
  EXPECT_EQ(invalid_response->status_code, common::StatusCode::kInvalidArgument);
  worker.wrong_sequence = false;
  worker.changed_authority = true;
  EXPECT_EQ(receiver->receive(request, {.authorized = true, .principal_id = 91U}).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedMutableVectorGroupedAggregateQueryReceiverTest,
     EncodesWorkerFailureWithFreshLeaderHint) {
  Authorizer authorizer;
  Worker worker;
  HintProvider hints;
  worker.failure = common::Status{common::StatusCode::kUnavailable, "not leader"};
  auto receiver = DistributedMutableVectorGroupedAggregateQueryReceiver::create(
                      {.local_node_id = 2U,
                       .authorizer = &authorizer,
                       .worker = &worker,
                       .leader_hint_provider = &hints})
                      .value();
  const auto request =
      encode_distributed_mutable_vector_query_request({1U, 2U, fragment()}).value();
  auto encoded = receiver.receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  ASSERT_EQ(encoded->size(), 1U);
  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  auto decoded = decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
      encoded->front(), expected_keys, expected_aggregates, resources);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->status_code, common::StatusCode::kUnavailable);
  ASSERT_TRUE(decoded->leader_hint.has_value());
  EXPECT_EQ(decoded->leader_hint->node_id, 3U);
  EXPECT_EQ(hints.calls, 1U);
}

TEST(DistributedMutableVectorGroupedAggregateQuerySenderTest,
     RetriesByteIdenticalMutableAuthorityAndAcceptsOnlyCompleteResponses) {
  auto resources = query::QueryResourceContext::create(1U << 20U).value();
  auto sender = DistributedMutableVectorGroupedAggregateQuerySender::create(
      1U, fragment(), keys(), aggregates(), std::move(resources));
  ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
  const auto now = DistributedMutableVectorGroupedAggregateQuerySender::TimePoint{};
  auto first = sender->begin_attempt(now);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  auto decoded_request =
      decode_distributed_mutable_vector_query_request_exact(first->request_bytes);
  ASSERT_TRUE(decoded_request.has_value()) << decoded_request.error().to_string();
  EXPECT_EQ(decoded_request->fragment, fragment());

  ASSERT_TRUE(sender->record_transport_failure(common::StatusCode::kIoError, now).is_ok());
  ASSERT_TRUE(sender->next_attempt_not_before().has_value());
  auto second = sender->begin_attempt(*sender->next_attempt_not_before());
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  EXPECT_EQ(second->request_bytes, first->request_bytes);

  auto incomplete = responses();
  incomplete.pop_back();
  EXPECT_EQ(sender->accept_responses(incomplete, now).code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kWaitingForResponse);
  auto complete = responses();
  EXPECT_TRUE(sender->accept_responses(complete, now).is_ok());
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kSucceeded);
  ASSERT_TRUE(sender->result().has_value());
  EXPECT_EQ(sender->result()->size(), 2U);
}

} // namespace
} // namespace chronos::cluster
