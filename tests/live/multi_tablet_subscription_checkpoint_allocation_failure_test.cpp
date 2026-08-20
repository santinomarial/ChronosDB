#include "chronos/live/multi_tablet_subscription_checkpoint.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdlib>
#include <gtest/gtest.h>
#include <optional>
#include <utility>

namespace chronos::live {
namespace {

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier identifier(const std::byte seed) {
  auto result = Identifier::from_uuid(uuid(seed));
  if (!result.has_value())
    std::abort();
  return *result;
}

[[nodiscard]] wal::WalId wal_id(const std::byte seed) {
  wal::WalId value{};
  value.bytes.fill(seed);
  return value;
}

[[nodiscard]] MultiTabletSubscriptionCheckpoint wal_checkpoint() {
  const schema::TabletId tablet_a = identifier<schema::TabletId>(std::byte{1});
  const schema::TabletId tablet_b = identifier<schema::TabletId>(std::byte{2});
  const wal::WalId wal_a = wal_id(std::byte{3});
  const wal::WalId wal_b = wal_id(std::byte{4});
  const schema::SchemaId schema_id = identifier<schema::SchemaId>(std::byte{5});
  PlanFingerprint plan{};
  plan.fill(std::byte{6});
  return {uuid(std::byte{7}),
          identifier<schema::TableId>(std::byte{8}),
          plan,
          schema_id,
          schema::SchemaVersion::initial(),
          {{{tablet_a, wal_a, 2U}, 0U}, {{tablet_b, wal_b, 1U}, 0U}},
          {{{tablet_a, wal_a, 1U},
            schema_id,
            schema::SchemaVersion::initial(),
            LogicalChangeOperation::kUpsert,
            {std::byte{9}},
            {std::byte{10}}},
           {{tablet_b, wal_b, 1U},
            schema_id,
            schema::SchemaVersion::initial(),
            LogicalChangeOperation::kDelete,
            {std::byte{11}},
            {}},
           {{tablet_a, wal_a, 2U},
            schema_id,
            schema::SchemaVersion::initial(),
            LogicalChangeOperation::kUpsert,
            {std::byte{12}, std::byte{13}},
            {std::byte{14}}}}};
}

[[nodiscard]] MultiTabletSubscriptionCheckpoint mixed_checkpoint() {
  MultiTabletSubscriptionCheckpoint checkpoint = wal_checkpoint();
  const common::Uuid raft_group = uuid(std::byte{4});
  checkpoint.sources[1].latest_position =
      SourcePosition::raft(checkpoint.sources[1].latest_position.tablet_id, raft_group, 1U);
  checkpoint.retained_changes[1].position =
      SourcePosition::raft(checkpoint.retained_changes[1].position.tablet_id, raft_group, 1U);
  return checkpoint;
}

template <typename Operation>
[[nodiscard]] auto run_with_allocation_failure(const std::size_t fail_after, std::size_t& observed,
                                               Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    observed = failure.observed_allocations();
    failure.disable();
  }
  return std::move(*result);
}

template <typename Expected, typename Operation>
void expect_every_allocation_is_classified(const Expected& expected, Operation&& operation) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::size_t observed = 0U;
    auto result = run_with_allocation_failure(fail_after, observed, operation);
    EXPECT_GT(observed, 0U);
    if (result.has_value()) {
      EXPECT_EQ(*result, expected);
      reached_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(MultiTabletSubscriptionCheckpointAllocationFailureTest, EncodersClassifyEveryOwnedAllocation) {
  const MultiTabletSubscriptionCheckpoint v1 = wal_checkpoint();
  const MultiTabletSubscriptionCheckpoint v2 = mixed_checkpoint();
  const auto v1_bytes = encode_multi_tablet_subscription_checkpoint_v1(v1).value();
  const auto v2_bytes = encode_multi_tablet_subscription_checkpoint_v2(v2).value();
  const BoundMultiTabletSubscriptionCheckpoint bound_v1{3U, v1};
  const BoundMultiTabletSubscriptionCheckpoint bound_v2{4U, v2};
  const auto bound_v1_bytes =
      encode_bound_multi_tablet_subscription_checkpoint_v1(bound_v1).value();
  const auto bound_v2_bytes =
      encode_bound_multi_tablet_subscription_checkpoint_v2(bound_v2).value();

  {
    SCOPED_TRACE("checkpoint v1");
    expect_every_allocation_is_classified(
        v1_bytes, [&] { return encode_multi_tablet_subscription_checkpoint_v1(v1); });
  }
  {
    SCOPED_TRACE("checkpoint v2");
    expect_every_allocation_is_classified(
        v2_bytes, [&] { return encode_multi_tablet_subscription_checkpoint_v2(v2); });
  }
  {
    SCOPED_TRACE("bound checkpoint v1");
    expect_every_allocation_is_classified(bound_v1_bytes, [&] {
      return encode_bound_multi_tablet_subscription_checkpoint_v1(bound_v1);
    });
  }
  {
    SCOPED_TRACE("bound checkpoint v2");
    expect_every_allocation_is_classified(bound_v2_bytes, [&] {
      return encode_bound_multi_tablet_subscription_checkpoint_v2(bound_v2);
    });
  }
}

TEST(MultiTabletSubscriptionCheckpointAllocationFailureTest, DecodersClassifyEveryOwnedAllocation) {
  const MultiTabletSubscriptionCheckpoint v1 = wal_checkpoint();
  const MultiTabletSubscriptionCheckpoint v2 = mixed_checkpoint();
  const auto v1_bytes = encode_multi_tablet_subscription_checkpoint_v1(v1).value();
  const auto v2_bytes = encode_multi_tablet_subscription_checkpoint_v2(v2).value();
  const BoundMultiTabletSubscriptionCheckpoint bound_v1{3U, v1};
  const BoundMultiTabletSubscriptionCheckpoint bound_v2{4U, v2};
  const auto bound_v1_bytes =
      encode_bound_multi_tablet_subscription_checkpoint_v1(bound_v1).value();
  const auto bound_v2_bytes =
      encode_bound_multi_tablet_subscription_checkpoint_v2(bound_v2).value();

  {
    SCOPED_TRACE("checkpoint v1");
    expect_every_allocation_is_classified(
        v1, [&] { return decode_multi_tablet_subscription_checkpoint_v1(v1_bytes); });
  }
  {
    SCOPED_TRACE("checkpoint v2");
    expect_every_allocation_is_classified(
        v2, [&] { return decode_multi_tablet_subscription_checkpoint_v2(v2_bytes); });
  }
  {
    SCOPED_TRACE("bound checkpoint v1");
    expect_every_allocation_is_classified(bound_v1, [&] {
      return decode_bound_multi_tablet_subscription_checkpoint_v1(bound_v1_bytes);
    });
  }
  {
    SCOPED_TRACE("bound checkpoint v2");
    expect_every_allocation_is_classified(bound_v2, [&] {
      return decode_bound_multi_tablet_subscription_checkpoint_v2(bound_v2_bytes);
    });
  }
}

} // namespace
} // namespace chronos::live
