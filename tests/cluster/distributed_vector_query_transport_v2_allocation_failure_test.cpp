#include "chronos/cluster/distributed_vector_query_transport_v2.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <new>
#include <optional>
#include <utility>

namespace chronos::cluster {
namespace {

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
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

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {
      .columns = {
          {"value", schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), false}}};
}

[[nodiscard]] query::DistributedVectorFragmentDispatchV2 dispatch_v2() {
  return {.dispatch = {.query_id = uuid(1U),
                       .database_id = manifest::DatabaseId::from_uuid(uuid(2U)).value(),
                       .table_id = schema::TableId::from_uuid(uuid(3U)).value(),
                       .tablet_id = schema::TabletId::from_uuid(uuid(4U)).value(),
                       .destination_schema_id = schema::SchemaId::from_uuid(uuid(5U)).value(),
                       .raft_group_id = uuid(6U),
                       .snapshot_generation = 1U,
                       .serving_node = 7U,
                       .placement_epoch = 1U,
                       .read_policy = {.consistency =
                                           query::DistributedReadConsistency::kLocalEventual},
                       .destination_column_ordinals = {0U},
                       .plan = {.mode = query::DistributedVectorPlanMode::kRows,
                                .row_output_indices = {0U}}},
          .result_schema = result_schema()};
}

template <typename Operation>
void expect_eventual_success(const char* label, Operation&& operation) {
  SCOPED_TRACE(label);
  bool success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto result = run_failure(fail_after, operation);
    if (result.has_value()) {
      success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(success);
}

TEST(DistributedVectorQueryTransportV2AllocationFailureTest, ClassifiesOwnedFrameAllocations) {
  const DistributedVectorQueryRequestV2 request{1U, 2U, dispatch_v2()};
  expect_eventual_success("request encode",
                          [&] { return encode_distributed_vector_query_request_v2(request); });
  const auto encoded_request = encode_distributed_vector_query_request_v2(request);
  ASSERT_TRUE(encoded_request.has_value());
  expect_eventual_success("request decode", [&] {
    return decode_distributed_vector_query_request_v2_exact(*encoded_request);
  });
  expect_eventual_success("request reader", [&] {
    DistributedVectorQueryRequestV2Reader reader;
    return reader.consume(*encoded_request);
  });

  const query::DistributedVectorResultSchema schema_value = result_schema();
  const DistributedVectorQueryResponseV2 response{
      .source_node_id = 2U,
      .target_node_id = 1U,
      .query_id = uuid(1U),
      .tablet_id = schema::TabletId::from_uuid(uuid(4U)).value(),
      .status_code = common::StatusCode::kOk,
      .payload = DistributedVectorResultExchangeMessage{
          .query_id = uuid(1U),
          .tablet_id = schema::TabletId::from_uuid(uuid(4U)).value(),
          .sequence = 1U,
          .terminal = true}};
  expect_eventual_success("response encode", [&] {
    return encode_distributed_vector_query_response_v2(response, schema_value);
  });
  const auto encoded_response = encode_distributed_vector_query_response_v2(response, schema_value);
  ASSERT_TRUE(encoded_response.has_value());
  expect_eventual_success("response decode", [&] {
    return decode_distributed_vector_query_response_v2_exact(*encoded_response, schema_value);
  });
  bool reader_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    query::DistributedVectorResultSchema reader_schema = result_schema();
    auto result = run_failure(fail_after, [&] {
      DistributedVectorQueryResponseV2Reader reader{std::move(reader_schema)};
      return reader.consume(*encoded_response);
    });
    if (result.has_value()) {
      reader_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reader_success);
}

} // namespace
} // namespace chronos::cluster
