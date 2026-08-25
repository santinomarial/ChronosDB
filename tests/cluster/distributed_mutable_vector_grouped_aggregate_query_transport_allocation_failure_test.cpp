#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_transport.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    test::ScopedAllocationFailure failure{fail_after};
    try {
      result.emplace(operation());
    } catch (...) {
      failure.disable();
      throw;
    }
    failure.disable();
  }
  return std::move(*result);
}

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
                                    {.name = "count", .type = int64, .nullable = false}}}};
}

[[nodiscard]] DistributedVectorGroupedAggregateQueryResponseV2 response() {
  const auto expected = aggregates();
  auto state = query::MergeableVectorAggregateState::create(expected.front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(
                       string_type(), "a mutable grouped response key larger than inline storage")
                       .value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  return {.source_node_id = 2U,
          .target_node_id = 1U,
          .query_id = uuid(1U),
          .tablet_id = id<schema::TabletId>(4U),
          .status_code = common::StatusCode::kOk,
          .payload = query::DistributedVectorGroupedAggregateExchangeMessage{
              {.query_id = uuid(1U),
               .tablet_id = id<schema::TabletId>(4U),
               .sequence = 1U,
               .group_ordinal = 0U,
               .group_count = 1U,
               .terminal = true,
               .empty = false},
              std::move(values),
              std::move(states)}};
}

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId claimed_node_id) const override {
    return principal_id == 91U && claimed_node_id == 1U;
  }
};

class Worker final : public DistributedMutableVectorGroupedAggregateQueryWorkerService {
public:
  common::Result<query::DistributedVectorGroupedAggregateAuthority>
  bind_authority(const query::DistributedMutableVectorFragment&) override {
    return query::DistributedVectorGroupedAggregateAuthority{keys(), aggregates()};
  }

  common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
  execute(const query::DistributedMutableVectorFragment&) override {
    auto value = response();
    auto authority = query::DistributedVectorGroupedAggregateAuthority{keys(), aggregates()};
    auto encoded = query::encode_distributed_vector_grouped_aggregate_exchange_message(
        *value.payload, authority.keys, authority.aggregates);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    query::DistributedVectorGroupedAggregateWorkerResultV2 result{
        .authority = std::move(authority), .input_rows = 1U, .group_count = 1U};
    result.encoded_bytes = encoded->bytes().size();
    result.messages.push_back(std::move(*encoded));
    return result;
  }
};

TEST(DistributedMutableVectorGroupedAggregateQueryReceiverAllocationFailureTest,
     ClassifiesAuthorityExecutionAndAtomicPublicationAllocations) {
  Authorizer authorizer;
  const auto request =
      encode_distributed_mutable_vector_query_request({1U, 2U, fragment()}).value();
  bool success{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    Worker worker;
    auto receiver = DistributedMutableVectorGroupedAggregateQueryReceiver::create(
        {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
    ASSERT_TRUE(receiver.has_value());
    auto result = run_failure(fail_after, [&] {
      return receiver->receive(request, {.authorized = true, .principal_id = 91U});
    });
    if (result.has_value()) {
      ASSERT_EQ(result->size(), 1U);
      success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(success);
}

TEST(DistributedMutableVectorGroupedAggregateQuerySenderAllocationFailureTest,
     ClassifiesCanonicalReconstructionAndReleasesDecodedKeyCredit) {
  const auto response_value = response();
  bool success{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    auto resources = query::QueryResourceContext::create(4U << 20U).value();
    {
      auto sender = DistributedMutableVectorGroupedAggregateQuerySender::create(
          1U, fragment(), keys(), aggregates(), resources);
      ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
      ASSERT_TRUE(sender->begin_attempt({}).has_value());
      const common::Status status = run_failure(
          fail_after, [&] { return sender->accept_responses(std::span{&response_value, 1U}, {}); });
      if (status.is_ok()) {
        ASSERT_TRUE(sender->result().has_value());
        EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
        success = true;
      } else {
        EXPECT_EQ(status.code(), common::StatusCode::kResourceExhausted);
        EXPECT_EQ(sender->state(), DistributedQuerySenderState::kWaitingForResponse);
        EXPECT_FALSE(sender->result().has_value());
        EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      }
    }
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
    if (success)
      break;
  }
  EXPECT_TRUE(success);
}

} // namespace
} // namespace chronos::cluster
