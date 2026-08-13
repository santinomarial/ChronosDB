#include "chronos/query/distributed_vector_aggregate_exchange.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string_view>
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

void append_u32(std::vector<std::byte>& output, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    output.push_back(static_cast<std::byte>((value >> (index * 8U)) & 0xffU));
}

[[nodiscard]] std::vector<VectorAggregateDefinition> definitions() {
  return {{.operation = VectorAggregateOperation::kMaximum,
           .input = VectorAggregateInput{
               .column_ordinal = 0U,
               .type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
               .nullable = false}}};
}

[[nodiscard]] MergeableVectorAggregateState
variable_state(const VectorAggregateDefinition& definition, const QueryResourceContext& resources) {
  columnar::ColumnVectorBuffers buffers;
  append_u32(buffers.offsets, 0U);
  constexpr std::string_view kValue = "a variable exchange extremum larger than SSO";
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
  auto state = MergeableVectorAggregateState::create(definition).value();
  EXPECT_TRUE(state.accumulate_cell(column.cell(0U).value(), resources).has_value());
  return state;
}

TEST(DistributedVectorAggregateExchangeAllocationFailureTest,
     ClassifiesEveryOwnedAllocationAndReleasesQueryCredit) {
  QueryResourceContext source_resources = QueryResourceContext::create(4U << 20U).value();
  const auto expected = definitions();
  auto state = variable_state(expected[0], source_resources);
  DistributedVectorAggregateExchangeMessage message{
      {.query_id = uuid(1U),
       .tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(),
       .sequence = 1U,
       .aggregate_ordinal = 0U,
       .terminal = true},
      std::move(state)};

  bool encoded_success{};
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return encode_distributed_vector_aggregate_exchange_message(message, expected);
    });
    if (result.has_value()) {
      encoded_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  ASSERT_TRUE(encoded_success);
  const auto encoded = encode_distributed_vector_aggregate_exchange_message(message, expected);
  ASSERT_TRUE(encoded.has_value());

  bool decoded_success{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
    {
      auto result = run_failure(fail_after, [&] {
        return decode_distributed_vector_aggregate_exchange_message_exact(encoded->bytes(),
                                                                          expected, resources);
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
  ASSERT_TRUE(decoded_success);

  bool reader_success{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
    {
      DistributedVectorAggregateExchangeReader reader{
          std::vector<VectorAggregateDefinition>{expected.begin(), expected.end()}, resources};
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
