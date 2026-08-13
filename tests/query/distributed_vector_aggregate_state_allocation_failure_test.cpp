#include "chronos/query/distributed_vector_aggregate_state.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
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

void append_u32(std::vector<std::byte>& output, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    output.push_back(static_cast<std::byte>((value >> (index * 8U)) & 0xffU));
}

[[nodiscard]] MergeableVectorAggregateState variable_state(const QueryResourceContext& resources) {
  const schema::LogicalType string =
      schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  columnar::ColumnVectorBuffers buffers;
  append_u32(buffers.offsets, 0U);
  constexpr std::string_view kValue = "a variable aggregate extremum larger than SSO";
  for (const char byte : kValue)
    buffers.values.push_back(static_cast<std::byte>(byte));
  append_u32(buffers.offsets, static_cast<std::uint32_t>(buffers.values.size()));
  auto column = columnar::OwnedPhysicalColumn::create(
                    {.type = string, .nullable = false, .row_count = 1U, .null_count = 0U},
                    std::move(buffers))
                    .value();
  auto state =
      MergeableVectorAggregateState::create(
          {.operation = VectorAggregateOperation::kMinimum,
           .input = VectorAggregateInput{.column_ordinal = 0U, .type = string, .nullable = false}})
          .value();
  EXPECT_TRUE(state.accumulate_cell(column.cell(0U).value(), resources).has_value());
  return state;
}

TEST(MergeableVectorAggregateStateCodecAllocationFailureTest,
     ClassifiesEveryOwnedCodecAllocationAndReleasesCredit) {
  QueryResourceContext source_resources = QueryResourceContext::create(1U << 20U).value();
  auto state = variable_state(source_resources);
  bool encoded_success{};
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    auto result =
        run_failure(fail_after, [&] { return encode_mergeable_vector_aggregate_state(state); });
    if (result.has_value()) {
      encoded_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  ASSERT_TRUE(encoded_success);
  const auto encoded = encode_mergeable_vector_aggregate_state(state);
  ASSERT_TRUE(encoded.has_value());

  bool decoded_success{};
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    {
      auto result = run_failure(fail_after, [&] {
        return decode_mergeable_vector_aggregate_state_exact(encoded->bytes(), resources);
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
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    {
      auto result = run_failure(fail_after, [&] {
        MergeableVectorAggregateStateReader reader{resources};
        return reader.consume(encoded->bytes());
      });
      if (result.has_value()) {
        ASSERT_TRUE(result->state.has_value());
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
