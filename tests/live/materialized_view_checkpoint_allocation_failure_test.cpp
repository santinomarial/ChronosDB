#include "chronos/live/materialized_view_checkpoint.hpp"
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

[[nodiscard]] WindowedMaterializedViewCheckpoint checkpoint() {
  const schema::TabletId tablet_id = identifier<schema::TabletId>(std::byte{1});
  const wal::WalId wal = wal_id(std::byte{2});
  auto view = WindowedMaterializedView::create(tablet_id, wal, {10, 5, 2, 16U, 16U});
  if (!view.has_value())
    std::abort();
  if (!view->apply_committed(SourcePosition{tablet_id, wal, 1U},
                             MaterializedViewInput{{1U, 1, 1U, 10.0, 1.0}, false})
           .has_value() ||
      !view->apply_committed(SourcePosition{tablet_id, wal, 2U},
                             MaterializedViewInput{{2U, 6, 2U, 20.0, 2.0}, false})
           .has_value() ||
      !view->advance_watermark(12).has_value()) {
    std::abort();
  }
  auto result = view->checkpoint();
  if (!result.has_value())
    std::abort();
  return std::move(*result);
}

[[nodiscard]] BoundMaterializedViewCheckpoint bound_checkpoint() {
  PlanFingerprint plan{};
  plan.fill(std::byte{7});
  return {.identity = {.database_id = uuid(std::byte{3}),
                       .view_id = uuid(std::byte{4}),
                       .table_id = identifier<schema::TableId>(std::byte{5}),
                       .schema_id = identifier<schema::SchemaId>(std::byte{6}),
                       .schema_version = schema::SchemaVersion::initial(),
                       .plan_fingerprint = plan},
          .checkpoint_generation = 2U,
          .state = checkpoint()};
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

TEST(MaterializedViewCheckpointAllocationFailureTest, EncodersClassifyEveryOwnedAllocation) {
  const WindowedMaterializedViewCheckpoint state = checkpoint();
  const BoundMaterializedViewCheckpoint bound = bound_checkpoint();
  const auto state_bytes = encode_windowed_materialized_view_checkpoint_v1(state).value();
  const auto bound_bytes = encode_bound_materialized_view_checkpoint_v1(bound).value();

  {
    SCOPED_TRACE("checkpoint");
    expect_every_allocation_is_classified(
        state_bytes, [&] { return encode_windowed_materialized_view_checkpoint_v1(state); });
  }
  {
    SCOPED_TRACE("bound checkpoint");
    expect_every_allocation_is_classified(
        bound_bytes, [&] { return encode_bound_materialized_view_checkpoint_v1(bound); });
  }
}

TEST(MaterializedViewCheckpointAllocationFailureTest, DecodersClassifyEveryOwnedAllocation) {
  const WindowedMaterializedViewCheckpoint state = checkpoint();
  const BoundMaterializedViewCheckpoint bound = bound_checkpoint();
  const auto state_bytes = encode_windowed_materialized_view_checkpoint_v1(state).value();
  const auto bound_bytes = encode_bound_materialized_view_checkpoint_v1(bound).value();

  {
    SCOPED_TRACE("checkpoint");
    expect_every_allocation_is_classified(
        state, [&] { return decode_windowed_materialized_view_checkpoint_v1(state_bytes); });
  }
  {
    SCOPED_TRACE("bound checkpoint");
    expect_every_allocation_is_classified(
        bound, [&] { return decode_bound_materialized_view_checkpoint_v1(bound_bytes); });
  }
}

} // namespace
} // namespace chronos::live
