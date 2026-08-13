#include "chronos/cluster/distributed_vector_aggregate_query_transport_v2.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <optional>
#include <utility>

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

} // namespace
} // namespace chronos::cluster
