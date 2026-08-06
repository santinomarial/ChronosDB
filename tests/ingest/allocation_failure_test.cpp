#include "chronos/head/mutable_head.hpp"
#include "chronos/ingest/retry_directory.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

constexpr std::size_t kMaximumInjectedAllocation = 128U;

[[nodiscard]] schema::TabletId tablet_id() {
  return columnar::test::id<schema::TabletId>(70U);
}

[[nodiscard]] RetryIdentity retry_identity(const std::uint8_t seed) {
  return RetryIdentity{.client_id = test::request_id<ClientId>(seed),
                       .client_batch_id =
                           test::request_id<ClientBatchId>(static_cast<std::uint8_t>(seed + 32U))};
}

[[nodiscard]] ColumnarAppendMutationIdentity
mutation(const std::shared_ptr<const schema::TableSchema>& schema, const std::uint8_t seed) {
  Sha256Digest::Bytes digest{};
  digest.back() = static_cast<std::byte>(seed);
  return ColumnarAppendMutationIdentity{.table_id = schema->table_id(),
                                        .tablet_id = tablet_id(),
                                        .request_digest = Sha256Digest{digest}};
}

[[nodiscard]] wal::WalId wal_id() {
  wal::WalId id;
  id.bytes.back() = std::byte{1U};
  return id;
}

[[nodiscard]] head::HeadCommitPosition position(const std::uint64_t sequence) {
  return head::HeadCommitPosition{.wal_id = wal_id(), .record_sequence = sequence};
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
batch(std::shared_ptr<const schema::TableSchema> schema = columnar::test::batch_schema()) {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(std::move(schema), columnar::test::batch_columns())
          .value());
}

[[nodiscard]] std::shared_ptr<const schema::TableSchema> deduplication_schema() {
  const std::shared_ptr<const schema::TableSchema> base = columnar::test::batch_schema();
  std::vector<schema::ColumnDefinition> columns{base->columns().begin(), base->columns().end()};
  const schema::ColumnId timestamp = columnar::test::id<schema::ColumnId>(1U);
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(
          columnar::test::id<schema::TableId>(60U), columnar::test::id<schema::SchemaId>(61U),
          schema::SchemaVersion::initial(), std::nullopt, std::move(columns),
          schema::TableSchemaRoles{.event_time_column = timestamp,
                                   .physical_ordering_key = {timestamp},
                                   .partition_columns = {timestamp},
                                   .shard_key = {timestamp},
                                   .deduplication_key = {timestamp}})
          .value());
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
deduplication_batch(const std::shared_ptr<const schema::TableSchema>& schema) {
  std::vector<columnar::OwnedColumnVector> columns = columnar::test::batch_columns();
  std::vector<std::byte> timestamps(16U);
  timestamps[8U] = std::byte{1U};
  columns[0U] =
      columnar::test::fixed_vector(1U, columnar::test::type(schema::LogicalTypeKind::kTimestampNs),
                                   false, 2U, {}, 0U, std::move(timestamps));
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(schema, std::move(columns)).value());
}

[[nodiscard]] head::MutableHead make_head() {
  return head::MutableHead::create(
             columnar::test::batch_schema(), tablet_id(), 1U,
             head::MutableHeadCapacity{.row_capacity = 4U, .variable_value_bytes = {0U, 2U, 0U}})
      .value();
}

[[nodiscard]] TabletState make_tablet(const std::shared_ptr<const schema::TableSchema>& schema,
                                      const std::uint32_t row_capacity = 4U) {
  return TabletState::create(
             schema, tablet_id(),
             TabletStateConfig{.head_capacity =
                                   head::MutableHeadCapacity{.row_capacity = row_capacity,
                                                             .variable_value_bytes = {0U, 2U, 0U}},
                               .maximum_schema_versions = 1U,
                               .maximum_sealed_generations = 2U,
                               .maximum_retry_entries = 8U})
      .value();
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

TEST(AllocationFailureTest, RetryDirectoryConstructionReportsEveryAllocationFailure) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < kMaximumInjectedAllocation; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::size_t observed = 0U;
    auto created = run_with_allocation_failure(fail_after, observed, [] {
      return RetryDirectory::create(RetryDirectoryConfig{.maximum_entries = 8U});
    });
    EXPECT_GT(observed, 0U);
    if (created.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(created.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(AllocationFailureTest, RetryReservationRollsBackEveryAllocationFailure) {
  bool reached_success = false;
  const auto schema = columnar::test::batch_schema();
  for (std::size_t fail_after = 0U; fail_after < kMaximumInjectedAllocation; ++fail_after) {
    SCOPED_TRACE(fail_after);
    RetryDirectory directory =
        RetryDirectory::create(RetryDirectoryConfig{.maximum_entries = 8U}).value();
    std::size_t observed = 0U;
    auto decision = run_with_allocation_failure(fail_after, observed, [&] {
      return directory.try_reserve(retry_identity(1U), mutation(schema, 1U));
    });
    EXPECT_GT(observed, 0U);
    if (decision.has_value()) {
      ASSERT_EQ(decision->kind(), RetryDecisionKind::kReserved);
      ASSERT_NE(decision->reservation(), nullptr);
      EXPECT_TRUE(decision->reservation()->cancel_before_wal().is_ok());
      reached_success = true;
      break;
    }
    EXPECT_EQ(decision.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(directory.metrics().entries, 0U);
    auto retry = directory.try_reserve(retry_identity(1U), mutation(schema, 1U));
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    ASSERT_NE(retry->reservation(), nullptr);
    EXPECT_TRUE(retry->reservation()->cancel_before_wal().is_ok());
  }
  EXPECT_TRUE(reached_success);
}

TEST(AllocationFailureTest, MutableHeadPreparationIsRollbackSafeAtEveryAllocation) {
  bool reached_success = false;
  const auto input = batch();
  for (std::size_t fail_after = 0U; fail_after < kMaximumInjectedAllocation; ++fail_after) {
    SCOPED_TRACE(fail_after);
    head::MutableHead target = make_head();
    std::size_t observed = 0U;
    auto prepared = run_with_allocation_failure(fail_after, observed,
                                                [&] { return target.prepare_append(input); });
    EXPECT_GT(observed, 0U);
    if (prepared.has_value()) {
      EXPECT_TRUE(prepared->cancel_before_wal().is_ok());
      reached_success = true;
      break;
    }
    EXPECT_EQ(prepared.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(target.snapshot()->row_count(), 0U);
    EXPECT_FALSE(target.metrics().failed);
    auto retry = target.prepare_append(input);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_TRUE(retry->cancel_before_wal().is_ok());
  }
  EXPECT_TRUE(reached_success);
}

TEST(AllocationFailureTest, TabletPreparationIsRollbackSafeAtEveryAllocation) {
  bool reached_success = false;
  const auto schema = deduplication_schema();
  const auto input = deduplication_batch(schema);
  for (std::size_t fail_after = 0U; fail_after < kMaximumInjectedAllocation; ++fail_after) {
    SCOPED_TRACE(fail_after);
    TabletState target = make_tablet(schema);
    std::size_t observed = 0U;
    auto prepared = run_with_allocation_failure(fail_after, observed, [&] {
      return target.prepare_append(retry_identity(1U), mutation(schema, 1U), input);
    });
    EXPECT_GT(observed, 0U);
    if (prepared.has_value()) {
      EXPECT_TRUE(prepared->cancel_before_wal().is_ok());
      reached_success = true;
      break;
    }
    EXPECT_EQ(prepared.error().code(), common::StatusCode::kResourceExhausted);
    const TabletSnapshot snapshot = target.snapshot().value();
    EXPECT_EQ(snapshot.visible_row_count(), 0U);
    EXPECT_FALSE(snapshot.applied_position().has_value());
    EXPECT_EQ(snapshot.retry_entry_count(), 0U);
    EXPECT_FALSE(target.metrics().failed);
    auto retry = target.prepare_append(retry_identity(1U), mutation(schema, 1U), input);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_TRUE(retry->cancel_before_wal().is_ok());
  }
  EXPECT_TRUE(reached_success);
}

TEST(AllocationFailureTest, TabletRotationPreservesThePriorEpochAtEveryAllocation) {
  bool reached_success = false;
  const auto schema = columnar::test::batch_schema();
  const auto input = batch(schema);
  for (std::size_t fail_after = 0U; fail_after < kMaximumInjectedAllocation; ++fail_after) {
    SCOPED_TRACE(fail_after);
    TabletState target = make_tablet(schema, 2U);
    auto first = target.prepare_append(retry_identity(1U), mutation(schema, 1U), input);
    ASSERT_TRUE(first.has_value()) << first.error().to_string();
    ASSERT_TRUE(first->mark_wal_started().is_ok());
    ASSERT_TRUE(first->publish(position(1U)).has_value());

    std::size_t observed = 0U;
    auto second = run_with_allocation_failure(fail_after, observed, [&] {
      return target.prepare_append(retry_identity(2U), mutation(schema, 2U), input);
    });
    EXPECT_GT(observed, 0U);
    if (second.has_value()) {
      EXPECT_TRUE(second->cancel_before_wal().is_ok());
      reached_success = true;
      break;
    }
    EXPECT_EQ(second.error().code(), common::StatusCode::kResourceExhausted);
    const TabletSnapshot snapshot = target.snapshot().value();
    EXPECT_EQ(snapshot.visible_row_count(), 2U);
    EXPECT_EQ(snapshot.applied_position().value_or(head::HeadCommitPosition{}), position(1U));
    EXPECT_EQ(snapshot.retry_entry_count(), 1U);
    EXPECT_FALSE(target.metrics().failed);
    auto retry = target.prepare_append(retry_identity(2U), mutation(schema, 2U), input);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_TRUE(retry->cancel_before_wal().is_ok());
  }
  EXPECT_TRUE(reached_success);
}

TEST(AllocationFailureTest, ExpectedPostWalPublicationAndRetryCommitAllocateNothing) {
  const auto schema = columnar::test::batch_schema();
  const auto input = batch(schema);
  RetryDirectory directory =
      RetryDirectory::create(RetryDirectoryConfig{.maximum_entries = 8U}).value();
  auto decision = directory.try_reserve(retry_identity(1U), mutation(schema, 1U));
  ASSERT_TRUE(decision.has_value()) << decision.error().to_string();
  ASSERT_NE(decision->reservation(), nullptr);
  RetryReservation reservation = std::move(*decision->reservation());
  TabletState target = make_tablet(schema);
  auto prepared = target.prepare_append(retry_identity(1U), mutation(schema, 1U), input);
  ASSERT_TRUE(prepared.has_value()) << prepared.error().to_string();
  ASSERT_TRUE(reservation.mark_wal_started().is_ok());
  ASSERT_TRUE(prepared->mark_wal_started().is_ok());

  std::optional<common::Result<TabletAppendResult>> published;
  std::optional<common::Result<std::shared_ptr<const ColumnarAppendRetryOutcome>>> committed;
  std::size_t observed = 0U;
  {
    ::chronos::test::ScopedAllocationFailure failure{0U};
    published.emplace(prepared->publish(position(1U)));
    if (published->has_value()) {
      committed.emplace(reservation.commit_published((*published)->outcome));
    }
    observed = failure.observed_allocations();
    failure.disable();
  }

  EXPECT_EQ(observed, 0U);
  if (!published.has_value()) {
    FAIL() << "tablet publication result was not captured";
  }
  common::Result<TabletAppendResult>& published_result = *published;
  if (!published_result.has_value()) {
    FAIL() << published_result.error().to_string();
  }
  if (!committed.has_value()) {
    FAIL() << "retry commit result was not captured";
  }
  common::Result<std::shared_ptr<const ColumnarAppendRetryOutcome>>& committed_result = *committed;
  if (!committed_result.has_value()) {
    FAIL() << committed_result.error().to_string();
  }
  EXPECT_EQ(committed_result->get(), published_result->outcome.get());
  EXPECT_EQ(target.snapshot()->visible_row_count(), 2U);
  EXPECT_EQ(directory.metrics().committed_entries, 1U);
}

} // namespace
} // namespace chronos::ingest
