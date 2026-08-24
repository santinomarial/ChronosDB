#include "chronos/cluster/distributed_vector_physical_rows_finalization_v2.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/aggregate.hpp"
#include "chronos/query/physical_plan.hpp"
#include "support/failing_allocator.hpp"

#include <array>
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

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    failure.disable();
  }
  return std::move(*result);
}

TEST(DistributedVectorPhysicalRowsFinalizationV2AllocationFailureTest,
     ClassifiesEveryOwnedPipelineAllocation) {
  const schema::LogicalType string_type =
      schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const schema::LogicalType int64_type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  query::PhysicalPipelinePlan pipeline =
      query::PhysicalPipelinePlan::create(
          {{.type = string_type, .nullable = true}},
          {query::GroupedAggregateStage{
              .keys = {{.column_ordinal = 0U, .type = string_type, .nullable = true}},
              .definitions = {{.operation = query::VectorAggregateOperation::kCountStar,
                               .input = std::nullopt}}}})
          .value();
  const query::DistributedVectorResultSchema input_schema{
      .columns = {{.name = "key", .type = string_type, .nullable = true}}};
  const query::DistributedVectorResultSchema output_schema{
      .columns = {{.name = "key", .type = string_type, .nullable = true},
                  {.name = "rows", .type = int64_type, .nullable = false}}};
  const query::DistributedVectorPlanIntent input_plan{
      .mode = query::DistributedVectorPlanMode::kRows, .row_output_indices = {0U}};
  const std::string value = "allocation-owned distributed grouped key larger than SSO";
  const std::array columns{
      network::QueryResultColumn{.name = "key", .type = string_type, .nullable = true}};
  const std::array cells{
      network::QueryResultCell{.value = std::as_bytes(std::span{value.data(), value.size()})},
      network::QueryResultCell{.value = std::as_bytes(std::span{value.data(), value.size()})}};
  const std::vector<std::byte> batch =
      network::encode_query_result_batch(2U, columns, cells).value();

  bool succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 1024U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    DistributedVectorQueryExecutionResultV2 input{
        .plan = input_plan,
        .result = {.result_schema = input_schema,
                   .messages = {{.query_id = uuid(20U),
                                 .tablet_id = id<schema::TabletId>(30U),
                                 .sequence = 1U,
                                 .terminal = true,
                                 .encoded_result_batch = batch}}}};
    query::DistributedVectorResultSchema output = output_schema;
    auto result = run_failure(fail_after, [&] {
      return finalize_distributed_vector_physical_rows_v2(std::move(input), pipeline,
                                                          std::move(output));
    });
    if (result.has_value()) {
      succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(succeeded);
}

} // namespace
} // namespace chronos::cluster
