#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_collector.hpp"
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

[[nodiscard]] DistributedVectorGroupedAggregateShuffleCompleteResultStream
complete(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         const query::DistributedVectorResultSchema& schema, const schema::LogicalType& type) {
  const std::string value{"allocation-collector-result-larger-than-SSO"};
  const std::array columns{network::QueryResultColumn{"region", type, false}};
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{value})}};
  std::vector<std::vector<std::byte>> batches;
  batches.push_back(network::encode_query_result_batch(1U, columns, cells).value());
  auto sender = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
                    authority, schema, 0U, 3U, 9U, batches)
                    .value();
  return {.query_id = authority.query_id(),
          .source_node_id = 3U,
          .target_node_id = 9U,
          .partition_id = 0U,
          .encoded_result_batches = std::move(batches),
          .frame_count = static_cast<std::uint32_t>(sender.frame_count()),
          .encoded_bytes = sender.encoded_bytes()};
}

TEST(DistributedVectorGroupedAggregateShuffleResultCollectorAllocationFailureTest,
     ClassifiesConstructionValidationAndAtomicPublication) {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  const auto type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const std::vector<query::VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = type, .nullable = false}};
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                       uuid(1U), {{.tablet_id = tablet, .node_id = 2U}},
                       {{.partition_id = 0U, .node_id = 3U}}, keys, {})
                       .value();
  query::DistributedVectorResultSchema schema{.columns = {{"region", type, false}}};

  auto create_failure = run_failure(0U, [&] {
    return DistributedVectorGroupedAggregateShuffleResultCollector::create(authority, schema, 9U);
  });
  ASSERT_FALSE(create_failure.has_value());
  EXPECT_EQ(create_failure.error().code(), common::StatusCode::kResourceExhausted);

  auto collector =
      DistributedVectorGroupedAggregateShuffleResultCollector::create(authority, schema, 9U)
          .value();
  bool saw_accept_failure{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto stream = complete(authority, schema, type);
    const auto encoded_bytes = stream.encoded_bytes;
    const common::Status accepted =
        run_failure(fail_after, [&] { return collector.accept_stream_preserving(stream); });
    if (!accepted.is_ok()) {
      saw_accept_failure = true;
      EXPECT_EQ(accepted.code(), common::StatusCode::kResourceExhausted);
      EXPECT_EQ(collector.metrics().accepted_partitions, 0U);
      ASSERT_EQ(stream.encoded_result_batches.size(), 1U);
      EXPECT_EQ(stream.encoded_bytes, encoded_bytes);
      EXPECT_EQ(stream.partition_id, 0U);
      continue;
    }
    break;
  }
  EXPECT_TRUE(saw_accept_failure);
  ASSERT_TRUE(collector.ready());

  auto publication_failure = run_failure(0U, [&] { return collector.take_complete_streams(); });
  ASSERT_FALSE(publication_failure.has_value());
  EXPECT_EQ(publication_failure.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(collector.ready());
  EXPECT_TRUE(collector.take_complete_streams().has_value());
}

} // namespace
} // namespace chronos::cluster
