#include "chronos/live/subscription_retention.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace chronos::live {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-subscription-retention-XXXXXX").string();
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

class CapturingReclaimer final : public SubscriptionSourceReclaimer {
public:
  common::Status reclaim(const std::span<const SubscriptionSourceReclamation> requests) override {
    ++calls;
    captured.assign(requests.begin(), requests.end());
    return result;
  }

  std::size_t calls{};
  std::vector<SubscriptionSourceReclamation> captured;
  common::Status result;
};

TEST(SubscriptionRetentionTest, IntersectsDurablePlansAndRejectsPlacementDriftBeforeDeletion) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const common::Uuid database = uuid(std::byte{1});
  const schema::TableId table = identifier<schema::TableId>(std::byte{2});
  const schema::TabletId tablet_a = identifier<schema::TabletId>(std::byte{3});
  const schema::TabletId tablet_b = identifier<schema::TabletId>(std::byte{4});
  const schema::SchemaId schema_id = identifier<schema::SchemaId>(std::byte{5});
  const wal::WalId wal_a = wal_id(std::byte{6});
  const wal::WalId wal_b = wal_id(std::byte{7});
  PlanFingerprint plan{};
  plan.fill(std::byte{8});
  ResumeTokenMacKey key{};
  key.fill(std::byte{9});
  SubscriptionLimits limits;
  limits.maximum_retained_changes = 1U;
  DurableMultiTabletSubscriptionConfig owner_config{
      .storage = {.directory_path = directory.path().string(),
                  .identity = {database,
                               table,
                               plan,
                               schema_id,
                               schema::SchemaVersion::initial(),
                               {{tablet_a, wal_a}, {tablet_b, wal_b}}}},
      .source = {.database_id = database,
                 .table_id = table,
                 .plan_fingerprint = plan,
                 .schema_id = schema_id,
                 .schema_version = schema::SchemaVersion::initial(),
                 .members = {{tablet_b, wal_b, 0U}, {tablet_a, wal_a, 0U}},
                 .token_key = key},
      .limits = limits};
  auto owner = DurableMultiTabletSubscription::create_new(std::move(owner_config));
  ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
  const auto change = [&](const schema::TabletId tablet_id, const wal::WalId wal,
                          const std::uint64_t sequence) {
    return CommittedChange{.position = {tablet_id, wal, sequence},
                           .schema_id = schema_id,
                           .schema_version = schema::SchemaVersion::initial(),
                           .operation = LogicalChangeOperation::kUpsert,
                           .result_key = {std::byte{1}},
                           .payload = {std::byte{2}}};
  };
  auto metadata = raft::MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(
      metadata
          ->apply_committed(1U, raft::TabletPlacementMetadata{table, tablet_a, 11U, {1U, 2U}, 1U})
          .is_ok());
  ASSERT_TRUE(
      metadata
          ->apply_committed(2U, raft::TabletPlacementMetadata{table, tablet_b, 12U, {1U, 3U}, 1U})
          .is_ok());
  auto retention = SubscriptionRetentionCoordinator::create(
      {.database_id = database,
       .table_id = table,
       .local_node_id = 1U,
       .members = {{tablet_b, wal_b, 12U}, {tablet_a, wal_a, 11U}},
       .subscription_owners = {&*owner}});
  ASSERT_TRUE(retention.has_value()) << retention.error().to_string();
  const std::vector<SourcePosition> storage_safe{{tablet_a, wal_a, 2U}, {tablet_b, wal_b, 1U}};
  CapturingReclaimer reclaimer;
  const auto blocked = retention->advance(*metadata, storage_safe, reclaimer);
  ASSERT_TRUE(blocked.has_value()) << blocked.error().to_string();
  EXPECT_TRUE(blocked->blocked_on_subscription_checkpoint);
  EXPECT_FALSE(blocked->advanced);
  EXPECT_EQ(reclaimer.calls, 0U);

  ASSERT_TRUE(owner->publish_committed(change(tablet_a, wal_a, 1U)).is_ok());
  ASSERT_TRUE(owner->publish_committed(change(tablet_b, wal_b, 1U)).is_ok());
  ASSERT_TRUE(owner->publish_committed(change(tablet_a, wal_a, 2U)).is_ok());
  ASSERT_TRUE(owner->checkpoint().has_value());
  const auto advanced = retention->advance(*metadata, storage_safe, reclaimer);
  ASSERT_TRUE(advanced.has_value()) << advanced.error().to_string();
  EXPECT_TRUE(advanced->advanced);
  ASSERT_EQ(reclaimer.calls, 1U);
  ASSERT_EQ(reclaimer.captured.size(), 2U);
  EXPECT_EQ(reclaimer.captured[0].reclaim_through.record_sequence, 1U);
  EXPECT_EQ(reclaimer.captured[1].reclaim_through.record_sequence, 1U);
  EXPECT_EQ(reclaimer.captured[0].metadata_index, 2U);

  ASSERT_TRUE(
      metadata
          ->apply_committed(3U, raft::TabletPlacementMetadata{table, tablet_a, 13U, {1U, 2U}, 1U})
          .is_ok());
  const auto changed = retention->advance(*metadata, storage_safe, reclaimer);
  ASSERT_FALSE(changed.has_value());
  EXPECT_EQ(changed.error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(reclaimer.calls, 1U);
}

TEST(SubscriptionRetentionTest, AdvancesToStorageSafeFrontierWhenNoDurablePlansExist) {
  const common::Uuid database = uuid(std::byte{21});
  const schema::TableId table = identifier<schema::TableId>(std::byte{22});
  const schema::TabletId tablet = identifier<schema::TabletId>(std::byte{23});
  const wal::WalId wal = wal_id(std::byte{24});
  auto metadata = raft::MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(
      metadata->apply_committed(1U, raft::TabletPlacementMetadata{table, tablet, 7U, {4U, 5U}, 4U})
          .is_ok());
  auto retention = SubscriptionRetentionCoordinator::create({.database_id = database,
                                                             .table_id = table,
                                                             .local_node_id = 4U,
                                                             .members = {{tablet, wal, 7U}}});
  ASSERT_TRUE(retention.has_value()) << retention.error().to_string();
  const std::vector<SourcePosition> storage_safe{{tablet, wal, 19U}};
  CapturingReclaimer reclaimer;

  const auto advanced = retention->advance(*metadata, storage_safe, reclaimer);

  ASSERT_TRUE(advanced.has_value()) << advanced.error().to_string();
  EXPECT_TRUE(advanced->advanced);
  ASSERT_EQ(reclaimer.calls, 1U);
  ASSERT_EQ(reclaimer.captured.size(), 1U);
  EXPECT_EQ(reclaimer.captured.front().reclaim_through.record_sequence, 19U);
}

} // namespace
} // namespace chronos::live
