#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_stream.hpp"
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
    test::ScopedAllocationFailure failure{fail_after};
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

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(std::uint64_t, raft::NodeId) const override {
    return true;
  }
};

TEST(DistributedVectorGroupedAggregateShuffleResultStreamAllocationFailureTest,
     ClassifiesSenderReceiverAndAtomicRetentionAllocations) {
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                       uuid(1U), {{tablet, 2U}}, {{0U, 3U}}, {{0U, string, false}},
                       {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
                       .value();
  const query::DistributedVectorResultSchema schema{.columns = {{"region", string, false}}};
  const std::array columns{network::QueryResultColumn{"region", string, false}};
  const std::string label = "allocation-owned-result";
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{label})}};
  const std::vector<std::vector<std::byte>> batches{
      network::encode_query_result_batch(1U, columns, cells).value()};

  bool sender_success{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
          authority, schema, 0U, 3U, 9U, batches);
    });
    if (result.has_value()) {
      sender_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(sender_success);
  auto sender = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
                    authority, schema, 0U, 3U, 9U, batches)
                    .value();
  std::vector<std::byte> encoded(sender.pending_write().begin(), sender.pending_write().end());
  Authorizer authorizer;
  bool receiver_success{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleResultStreamReceiver::create(
          authority, schema, 9U, authorizer, {.authorized = true, .principal_id = 91U});
    });
    if (result.has_value()) {
      receiver_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(receiver_success);
  bool retention_success{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto receiver =
        DistributedVectorGroupedAggregateShuffleResultStreamReceiver::create(
            authority, schema, 9U, authorizer, {.authorized = true, .principal_id = 91U})
            .value();
    auto result = run_failure(fail_after, [&] { return receiver.consume(encoded); });
    if (result.has_value()) {
      retention_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(receiver.failed());
  }
  EXPECT_TRUE(retention_success);
}

} // namespace
} // namespace chronos::cluster
