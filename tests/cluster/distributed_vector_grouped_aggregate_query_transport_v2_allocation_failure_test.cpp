#include "chronos/cluster/distributed_vector_grouped_aggregate_query_transport_v2.hpp"
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

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U,
           .type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
           .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] query::DistributedVectorFragmentDispatchV2 dispatch_v2() {
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {.dispatch = {.query_id = uuid(1U),
                       .database_id = manifest::DatabaseId::from_uuid(uuid(3U)).value(),
                       .table_id = schema::TableId::from_uuid(uuid(4U)).value(),
                       .tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(),
                       .destination_schema_id = schema::SchemaId::from_uuid(uuid(5U)).value(),
                       .raft_group_id = uuid(6U),
                       .snapshot_generation = 1U,
                       .serving_node = 2U,
                       .placement_epoch = 1U,
                       .read_policy = {.consistency =
                                           query::DistributedReadConsistency::kLocalEventual},
                       .destination_column_ordinals = {0U},
                       .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
                                .group_key_input_indices = {0U},
                                .aggregates = {{.operation =
                                                    query::VectorAggregateOperation::kCountStar}}}},
          .result_schema = {.columns = {{.name = "key", .type = string, .nullable = false},
                                        {.name = "count", .type = int64, .nullable = false}}}};
}

[[nodiscard]] DistributedVectorGroupedAggregateQueryResponseV2 response() {
  const auto expected = aggregates();
  auto state = query::MergeableVectorAggregateState::create(expected.front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(
                       schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
                       "a grouped response key larger than short string storage")
                       .value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  return {.source_node_id = 2U,
          .target_node_id = 1U,
          .query_id = uuid(1U),
          .tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(),
          .status_code = common::StatusCode::kOk,
          .payload = query::DistributedVectorGroupedAggregateExchangeMessage{
              {.query_id = uuid(1U),
               .tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(),
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
    return common::Result<bool>{principal_id == 91U && claimed_node_id == 1U};
  }
};

class AllocationWorker final : public DistributedVectorGroupedAggregateQueryWorkerServiceV2 {
public:
  common::Result<query::DistributedVectorGroupedAggregateAuthority>
  bind_authority(const query::DistributedVectorFragmentDispatchV2&) override {
    return query::DistributedVectorGroupedAggregateAuthority{keys(), aggregates()};
  }

  common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
  execute(const query::DistributedVectorFragmentDispatchV2&) override {
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

TEST(DistributedVectorGroupedAggregateQueryTransportV2AllocationFailureTest,
     ClassifiesOwnedFrameAllocationsAndReleasesDecodedKeyCredit) {
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  const auto value = response();
  bool encode_success{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return encode_distributed_vector_grouped_aggregate_query_response_v2(value, expected_keys,
                                                                           expected_aggregates);
    });
    if (result.has_value()) {
      encode_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  ASSERT_TRUE(encode_success);
  const auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(
      value, expected_keys, expected_aggregates);
  ASSERT_TRUE(encoded.has_value());

  bool decode_success{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto resources = query::QueryResourceContext::create(4U << 20U).value();
    {
      auto result = run_failure(fail_after, [&] {
        return decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
            *encoded, expected_keys, expected_aggregates, resources);
      });
      if (result.has_value())
        decode_success = true;
      else
        EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
    }
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
    if (decode_success)
      break;
  }
  EXPECT_TRUE(decode_success);

  bool reader_success{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto resources = query::QueryResourceContext::create(4U << 20U).value();
    {
      auto owned_keys = keys();
      auto owned_aggregates = aggregates();
      DistributedVectorGroupedAggregateQueryResponseV2Reader reader{
          std::move(owned_keys), std::move(owned_aggregates), resources};
      auto result = run_failure(fail_after, [&] { return reader.consume(*encoded); });
      if (result.has_value())
        reader_success = true;
      else
        EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
    }
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
    if (reader_success)
      break;
  }
  EXPECT_TRUE(reader_success);
}

TEST(DistributedVectorGroupedAggregateQueryReceiverV2AllocationFailureTest,
     ClassifiesOwnedAuthorityExecutionAndAtomicPublicationAllocations) {
  Authorizer authorizer;
  const auto request = encode_distributed_vector_query_request_v2(
      {.source_node_id = 1U, .target_node_id = 2U, .dispatch = dispatch_v2()});
  ASSERT_TRUE(request.has_value());
  bool success{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    AllocationWorker worker;
    auto receiver = DistributedVectorGroupedAggregateQueryReceiverV2::create(
        {.local_node_id = 2U, .authorizer = &authorizer, .worker = &worker});
    ASSERT_TRUE(receiver.has_value());
    auto result = run_failure(fail_after, [&] {
      return receiver->receive(*request, {.authorized = true, .principal_id = 91U});
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

TEST(DistributedVectorGroupedAggregateQuerySenderV2AllocationFailureTest,
     ClassifiesCanonicalReconstructionAndReleasesTemporaryDecodedKeyCredit) {
  const auto response_value = response();
  bool success{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    auto resources = query::QueryResourceContext::create(4U << 20U).value();
    {
      auto sender = DistributedVectorGroupedAggregateQuerySenderV2::create(
          1U, dispatch_v2(), keys(), aggregates(), resources);
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
