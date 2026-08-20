#include "chronos/live/durable_multi_tablet_subscription.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-durable-subscription-alloc-XXXXXX")
            .string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier identifier(const std::byte seed) {
  return Identifier::from_uuid(uuid(seed)).value();
}

[[nodiscard]] wal::WalId wal_id(const std::byte seed) {
  wal::WalId value{};
  value.bytes.fill(seed);
  return value;
}

struct Fixture {
  common::Uuid database_id{uuid(std::byte{1})};
  schema::TableId table_id{identifier<schema::TableId>(std::byte{2})};
  schema::TabletId tablet_a{identifier<schema::TabletId>(std::byte{3})};
  schema::TabletId tablet_b{identifier<schema::TabletId>(std::byte{4})};
  wal::WalId wal_a{wal_id(std::byte{5})};
  wal::WalId wal_b{wal_id(std::byte{6})};
  schema::SchemaId schema_id{identifier<schema::SchemaId>(std::byte{7})};
  common::Uuid subscription_id{uuid(std::byte{8})};
  PlanFingerprint plan{};
  ResumeTokenMacKey key{};

  Fixture() {
    plan.fill(std::byte{9});
    key.fill(std::byte{10});
  }

  [[nodiscard]] DurableMultiTabletSubscriptionConfig
  config(const std::filesystem::path& directory) const {
    SubscriptionLimits limits;
    limits.maximum_retained_changes = 2U;
    return {.storage = {.directory_path = directory.string(),
                        .identity = {database_id,
                                     table_id,
                                     plan,
                                     schema_id,
                                     schema::SchemaVersion::initial(),
                                     {{tablet_a, wal_a}, {tablet_b, wal_b}}}},
            .source = {.database_id = database_id,
                       .table_id = table_id,
                       .plan_fingerprint = plan,
                       .schema_id = schema_id,
                       .schema_version = schema::SchemaVersion::initial(),
                       .members = {{tablet_b, wal_b, 0U}, {tablet_a, wal_a, 0U}},
                       .token_key = key},
            .limits = limits};
  }

  [[nodiscard]] SubscriptionRequest request() const {
    return {subscription_id, plan, schema_id, schema::SchemaVersion::initial()};
  }

  [[nodiscard]] CommittedChange change(const schema::TabletId& tablet, const wal::WalId& wal,
                                       const std::uint64_t sequence) const {
    return {.position = {tablet, wal, sequence},
            .schema_id = schema_id,
            .schema_version = schema::SchemaVersion::initial(),
            .operation = LogicalChangeOperation::kUpsert,
            .result_key = {std::byte{static_cast<unsigned char>(sequence)}},
            .payload = {std::byte{static_cast<unsigned char>(sequence + 16U)}}};
  }
};

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

[[nodiscard]] std::vector<SourcePosition> frontiers(const DurableMultiTabletSubscription& owner) {
  const auto durable = owner.durable_retention_frontiers();
  EXPECT_TRUE(durable.has_value()) << durable.error().to_string();
  if (!durable.has_value())
    return {};
  EXPECT_TRUE(durable->has_value());
  return durable->value_or(std::vector<SourcePosition>{});
}

struct PublishBatch {
  std::uint64_t source_sequence{};
  std::uint64_t delivery_sequence{};
};

void publish_and_acknowledge(DurableMultiTabletSubscription& owner, const Fixture& fixture,
                             const PublishBatch batch) {
  ASSERT_TRUE(
      owner
          .publish_committed(fixture.change(fixture.tablet_a, fixture.wal_a, batch.source_sequence))
          .is_ok());
  ASSERT_TRUE(
      owner
          .publish_committed(fixture.change(fixture.tablet_b, fixture.wal_b, batch.source_sequence))
          .is_ok());
  const auto delivered = owner.poll(fixture.subscription_id, 2U);
  ASSERT_TRUE(delivered.has_value()) << delivered.error().to_string();
  ASSERT_EQ(delivered->size(), 2U);
  ASSERT_TRUE(owner.acknowledge(fixture.subscription_id, batch.delivery_sequence).has_value());
}

TEST(DurableMultiTabletSubscriptionAllocationFailureTest,
     CheckpointNeverPublishesAnUninstalledRetentionFrontier) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    TemporaryDirectory directory;
    ASSERT_FALSE(directory.path().empty());
    const Fixture fixture;
    auto owner = DurableMultiTabletSubscription::create_new(fixture.config(directory.path()));
    ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
    ASSERT_TRUE(owner->register_subscription(fixture.request()).has_value());
    ASSERT_TRUE(owner->complete_snapshot(fixture.subscription_id).is_ok());

    publish_and_acknowledge(*owner, fixture, {.source_sequence = 1U, .delivery_sequence = 2U});
    publish_and_acknowledge(*owner, fixture, {.source_sequence = 2U, .delivery_sequence = 4U});
    const auto first = owner->checkpoint();
    ASSERT_TRUE(first.has_value()) << first.error().to_string();
    ASSERT_EQ(owner->checkpoint_generation(), 1U);
    const std::vector<SourcePosition> first_frontiers = frontiers(*owner);
    ASSERT_EQ(first_frontiers.size(), 2U);
    EXPECT_EQ(first_frontiers[0].record_sequence, 1U);
    EXPECT_EQ(first_frontiers[1].record_sequence, 1U);

    publish_and_acknowledge(*owner, fixture, {.source_sequence = 3U, .delivery_sequence = 6U});
    ASSERT_TRUE(owner->has_uncheckpointed_changes());
    std::size_t observed = 0U;
    auto second =
        run_with_allocation_failure(fail_after, observed, [&] { return owner->checkpoint(); });
    EXPECT_GT(observed, 0U);
    if (second.has_value()) {
      EXPECT_EQ(second->checkpoint_generation, 2U);
      EXPECT_FALSE(owner->has_uncheckpointed_changes());
      const std::vector<SourcePosition> second_frontiers = frontiers(*owner);
      ASSERT_EQ(second_frontiers.size(), 2U);
      EXPECT_EQ(second_frontiers[0].record_sequence, 2U);
      EXPECT_EQ(second_frontiers[1].record_sequence, 2U);
      reached_success = true;
      break;
    }

    EXPECT_EQ(second.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(owner->checkpoint_generation(), 1U);
    EXPECT_TRUE(owner->has_uncheckpointed_changes());
    EXPECT_EQ(frontiers(*owner), first_frontiers);

    const auto recovered = owner->checkpoint();
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    EXPECT_EQ(recovered->checkpoint_generation, 2U);
    EXPECT_FALSE(owner->has_uncheckpointed_changes());
    const std::vector<SourcePosition> recovered_frontiers = frontiers(*owner);
    ASSERT_EQ(recovered_frontiers.size(), 2U);
    EXPECT_EQ(recovered_frontiers[0].record_sequence, 2U);
    EXPECT_EQ(recovered_frontiers[1].record_sequence, 2U);
  }
  EXPECT_TRUE(reached_success);
}

} // namespace
} // namespace chronos::live
