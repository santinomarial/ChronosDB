#include "chronos/query/distributed_vector_grouped_aggregate_exchange.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

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

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

TEST(DistributedVectorGroupedAggregateExchangeAllocationFailureTest,
     ClassifiesOwnedEncodingAndDecodingAllocationsAndReleasesCredit) {
  const schema::LogicalType string =
      schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const std::vector<VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = string, .nullable = false}};
  const std::vector<VectorAggregateDefinition> aggregates{
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  std::vector<ScalarValue> values;
  values.push_back(
      ScalarValue::text(string, "a variable grouped key larger than short string storage").value());
  std::vector<MergeableVectorAggregateState> states;
  states.push_back(MergeableVectorAggregateState::create(aggregates.front()).value());
  EXPECT_TRUE(states.front().accumulate_count_star().has_value());
  DistributedVectorGroupedAggregateExchangeMessage message{
      {.query_id = uuid(1U),
       .tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(),
       .sequence = 1U,
       .group_ordinal = 0U,
       .group_count = 1U,
       .terminal = true,
       .empty = false},
      std::move(values),
      std::move(states)};

  bool encoded_success{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return encode_distributed_vector_grouped_aggregate_exchange_message(message, keys,
                                                                          aggregates);
    });
    if (result.has_value()) {
      encoded_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  ASSERT_TRUE(encoded_success);
  const auto encoded =
      encode_distributed_vector_grouped_aggregate_exchange_message(message, keys, aggregates);
  ASSERT_TRUE(encoded.has_value());

  bool decoded_success{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
    {
      auto result = run_failure(fail_after, [&] {
        return decode_distributed_vector_grouped_aggregate_exchange_message_exact(
            encoded->bytes(), keys, aggregates, resources);
      });
      if (result.has_value()) {
        decoded_success = true;
      } else {
        EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
      }
    }
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
    if (decoded_success)
      break;
  }
  EXPECT_TRUE(decoded_success);

  bool reader_success{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
    {
      DistributedVectorGroupedAggregateExchangeReader reader{
          std::vector<VectorGroupKeyDefinition>{keys.begin(), keys.end()},
          std::vector<VectorAggregateDefinition>{aggregates.begin(), aggregates.end()}, resources};
      auto result = run_failure(fail_after, [&] { return reader.consume(encoded->bytes()); });
      if (result.has_value()) {
        ASSERT_TRUE(result->message.has_value());
        reader_success = true;
      } else {
        EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
      }
    }
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
    if (reader_success)
      break;
  }
  EXPECT_TRUE(reader_success);
}

} // namespace
} // namespace chronos::query
