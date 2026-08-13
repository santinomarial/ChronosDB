#include "chronos/cluster/distributed_vector_aggregate_query_transport_v2.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string_view>
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

[[nodiscard]] std::vector<query::VectorAggregateDefinition> definitions() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] query::DistributedVectorFragmentDispatchV2 dispatch_v2() {
  return {
      .dispatch =
          {.query_id = uuid(1U),
           .database_id = manifest::DatabaseId::from_uuid(uuid(3U)).value(),
           .table_id = schema::TableId::from_uuid(uuid(4U)).value(),
           .tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(),
           .destination_schema_id = schema::SchemaId::from_uuid(uuid(5U)).value(),
           .raft_group_id = uuid(6U),
           .snapshot_generation = 1U,
           .serving_node = 2U,
           .placement_epoch = 1U,
           .read_policy = {.consistency = query::DistributedReadConsistency::kLocalEventual},
           .destination_column_ordinals = {0U},
           .plan = {.mode = query::DistributedVectorPlanMode::kUngroupedAggregate,
                    .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}}}},
      .result_schema = {
          .columns = {{.name = "count",
                       .type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
                       .nullable = false}}}};
}

void append_u32(std::vector<std::byte>& output, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    output.push_back(static_cast<std::byte>((value >> (index * 8U)) & 0xffU));
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> variable_definitions() {
  return {{.operation = query::VectorAggregateOperation::kMaximum,
           .input = query::VectorAggregateInput{
               .column_ordinal = 0U,
               .type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
               .nullable = false}}};
}

[[nodiscard]] query::DistributedVectorFragmentDispatchV2 variable_dispatch_v2() {
  auto dispatch = dispatch_v2();
  dispatch.dispatch.plan.aggregates = {
      {.operation = query::VectorAggregateOperation::kMaximum, .input_index = 0U}};
  dispatch.result_schema.columns = {
      {.name = "maximum",
       .type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
       .nullable = true}};
  return dispatch;
}

[[nodiscard]] DistributedVectorAggregateQueryResponseV2
variable_response(const query::QueryResourceContext& source_resources) {
  columnar::ColumnVectorBuffers buffers;
  append_u32(buffers.offsets, 0U);
  constexpr std::string_view kValue = "a variable sender extremum larger than SSO";
  for (const char byte : kValue)
    buffers.values.push_back(static_cast<std::byte>(byte));
  append_u32(buffers.offsets, static_cast<std::uint32_t>(buffers.values.size()));
  auto column = columnar::OwnedPhysicalColumn::create(
                    {.type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
                     .nullable = false,
                     .row_count = 1U,
                     .null_count = 0U},
                    std::move(buffers))
                    .value();
  auto expected = variable_definitions();
  auto state = query::MergeableVectorAggregateState::create(expected.front()).value();
  EXPECT_TRUE(state.accumulate_cell(column.cell(0U).value(), source_resources).has_value());
  return {.source_node_id = 2U,
          .target_node_id = 1U,
          .query_id = uuid(1U),
          .tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(),
          .status_code = common::StatusCode::kOk,
          .payload = query::DistributedVectorAggregateExchangeMessage{
              {.query_id = uuid(1U),
               .tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(),
               .sequence = 1U,
               .aggregate_ordinal = 0U,
               .terminal = true},
              std::move(state)}};
}

[[nodiscard]] DistributedVectorAggregateQueryResponseV2 response() {
  const auto expected = definitions();
  auto state = query::MergeableVectorAggregateState::create(expected.front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  return {.source_node_id = 2U,
          .target_node_id = 1U,
          .query_id = uuid(1U),
          .tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(),
          .status_code = common::StatusCode::kOk,
          .payload = query::DistributedVectorAggregateExchangeMessage{
              {.query_id = uuid(1U),
               .tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(),
               .sequence = 1U,
               .aggregate_ordinal = 0U,
               .terminal = true},
              std::move(state)}};
}

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId claimed_node_id) const override {
    return principal_id == 91U && claimed_node_id == 1U;
  }
};

class AllocationWorker final : public DistributedVectorAggregateQueryWorkerServiceV2 {
public:
  common::Result<std::vector<query::VectorAggregateDefinition>>
  bind_definitions(const query::DistributedVectorFragmentDispatchV2&) override {
    return definitions();
  }

  common::Result<query::DistributedVectorAggregateWorkerResultV2>
  execute(const query::DistributedVectorFragmentDispatchV2&) override {
    auto expected = definitions();
    auto state = query::MergeableVectorAggregateState::create(expected.front()).value();
    const auto accumulated = state.accumulate_count_star();
    if (!accumulated.has_value())
      return common::make_unexpected(accumulated.error());
    query::DistributedVectorAggregateWorkerResultV2 result{.definitions = expected};
    result.messages.emplace_back(
        query::DistributedVectorAggregateExchangePosition{
            .query_id = uuid(1U),
            .tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(),
            .sequence = 1U,
            .aggregate_ordinal = 0U,
            .terminal = true},
        std::move(state));
    return result;
  }
};

TEST(DistributedVectorAggregateQueryTransportV2AllocationFailureTest,
     ClassifiesEveryOwnedFrameAllocation) {
  bool encode_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    const auto expected = definitions();
    const auto value = response();
    const auto result = run_failure(fail_after, [&] {
      return encode_distributed_vector_aggregate_query_response_v2(value, expected);
    });
    if (result.has_value()) {
      encode_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(encode_success);

  const auto expected = definitions();
  const auto encoded =
      encode_distributed_vector_aggregate_query_response_v2(response(), expected).value();
  bool decode_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto resources = query::QueryResourceContext::create(1U << 20U).value();
    const auto result = run_failure(fail_after, [&] {
      return decode_distributed_vector_aggregate_query_response_v2_exact(encoded, expected,
                                                                         resources);
    });
    if (result.has_value()) {
      decode_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(decode_success);

  bool reader_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto owned = definitions();
    auto resources = query::QueryResourceContext::create(1U << 20U).value();
    const auto result = run_failure(fail_after, [&] {
      DistributedVectorAggregateQueryResponseV2Reader reader{std::move(owned),
                                                             std::move(resources)};
      return reader.consume(encoded);
    });
    if (result.has_value()) {
      reader_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reader_success);
}

TEST(DistributedVectorAggregateQueryReceiverV2AllocationFailureTest,
     ClassifiesEveryOwnedDefinitionExecutionAndPublicationAllocation) {
  Authorizer authorizer;
  const auto request = encode_distributed_vector_query_request_v2(
      {.source_node_id = 1U, .target_node_id = 2U, .dispatch = dispatch_v2()});
  ASSERT_TRUE(request.has_value());
  bool success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    AllocationWorker worker;
    auto receiver = DistributedVectorAggregateQueryReceiverV2::create(
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

TEST(DistributedVectorAggregateQuerySenderV2AllocationFailureTest,
     ClassifiesEveryCanonicalReconstructionAllocationAndReleasesQueryCredit) {
  auto source_resources = query::QueryResourceContext::create(4U << 20U).value();
  const auto response_value = variable_response(source_resources);
  bool success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    auto sender_resources = query::QueryResourceContext::create(4U << 20U).value();
    {
      auto expected = variable_definitions();
      auto sender = DistributedVectorAggregateQuerySenderV2::create(
          1U, variable_dispatch_v2(), std::move(expected), sender_resources);
      ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
      ASSERT_TRUE(sender->begin_attempt({}).has_value());
      const common::Status status = run_failure(
          fail_after, [&] { return sender->accept_responses(std::span{&response_value, 1U}, {}); });
      if (status.is_ok()) {
        ASSERT_TRUE(sender->result().has_value());
        EXPECT_GT(sender_resources.reserved_memory_bytes(), 0U);
        success = true;
      } else {
        EXPECT_EQ(status.code(), common::StatusCode::kResourceExhausted);
        EXPECT_EQ(sender->state(), DistributedQuerySenderState::kWaitingForResponse);
        EXPECT_FALSE(sender->result().has_value());
        EXPECT_EQ(sender_resources.reserved_memory_bytes(), 0U);
      }
    }
    EXPECT_EQ(sender_resources.reserved_memory_bytes(), 0U);
    if (success)
      break;
  }
  EXPECT_TRUE(success);
}

} // namespace
} // namespace chronos::cluster
