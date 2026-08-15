#include "chronos/live/wal_subscription_reclaimer.hpp"
#include "chronos/wal/wal_log_id_generator.hpp"

#include <cstddef>
#include <cstdint>
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
  explicit TemporaryDirectory(const char* label) {
    std::string pattern =
        (std::filesystem::temp_directory_path() / (std::string{label} + "-XXXXXX")).string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
    }
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

[[nodiscard]] schema::TabletId tablet_id(const std::byte seed) {
  return schema::TabletId::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::TableId table_id(const std::byte seed) {
  return schema::TableId::from_uuid(uuid(seed)).value();
}

[[nodiscard]] wal::WalId make_wal_id(const std::uint8_t first_byte) {
  wal::WalId id;
  id.bytes[0] = static_cast<std::byte>(first_byte);
  for (std::size_t index = 1U; index < id.bytes.size(); ++index) {
    id.bytes[index] = static_cast<std::byte>(index);
  }
  return id;
}

class FixedWalIdGenerator final : public wal::WalLogIdGenerator {
public:
  explicit FixedWalIdGenerator(const wal::WalId id) : id_(id) {}

  [[nodiscard]] common::Result<wal::WalId> generate() override {
    return id_;
  }

private:
  wal::WalId id_;
};

[[nodiscard]] std::vector<std::byte> application_payload() {
  std::vector<std::byte> payload(wal::kApplicationEnvelopeSize);
  payload[0] = std::byte{1};
  payload[4] = std::byte{1};
  return payload;
}

[[nodiscard]] common::Result<wal::WalWriter> make_writer(const std::filesystem::path& path,
                                                         const wal::WalId id,
                                                         const std::size_t record_count) {
  FixedWalIdGenerator generator{id};
  auto writer =
      wal::WalWriter::create_new({.directory_path = path.string(),
                                  .target_segment_size = wal::kSegmentHeaderSize + 64U,
                                  .maximum_application_payload = wal::kApplicationEnvelopeSize},
                                 generator);
  if (!writer.has_value()) {
    return common::make_unexpected(writer.error());
  }
  for (std::size_t index = 0U; index < record_count; ++index) {
    auto appended = writer->append_application_entry(application_payload());
    if (!appended.has_value()) {
      return common::make_unexpected(appended.error());
    }
  }
  auto synchronized = writer->synchronize();
  if (!synchronized.has_value()) {
    return common::make_unexpected(synchronized.error());
  }
  return writer;
}

[[nodiscard]] std::filesystem::path segment_path(const std::filesystem::path& directory,
                                                 const std::uint64_t number) {
  std::string digits = std::to_string(number);
  digits.insert(digits.begin(), 20U - digits.size(), '0');
  return directory / ("wal-" + digits + ".cwal");
}

TEST(WalSubscriptionSourceReclaimerTest,
     MapsCompleteBatchAndUsesMinimumFrontierForTabletsSharingOneWal) {
  TemporaryDirectory directory{"chronos-live-wal-reclaimer-shared"};
  ASSERT_FALSE(directory.path().empty());
  const wal::WalId wal_id = make_wal_id(4U);
  auto writer = make_writer(directory.path(), wal_id, 5U);
  ASSERT_TRUE(writer.has_value()) << writer.error().to_string();
  const schema::TabletId tablet_a = tablet_id(std::byte{1});
  const schema::TabletId tablet_b = tablet_id(std::byte{2});
  auto reclaimer = WalSubscriptionSourceReclaimer::create(
      {.sources = {{tablet_b, 12U, &*writer}, {tablet_a, 11U, &*writer}}});
  ASSERT_TRUE(reclaimer.has_value()) << reclaimer.error().to_string();
  const std::vector<SubscriptionSourceReclamation> requests{{{tablet_a, wal_id, 3U}, 11U, 7U},
                                                            {{tablet_b, wal_id, 4U}, 12U, 7U}};

  const common::Status reclaimed = reclaimer->reclaim(requests);

  ASSERT_TRUE(reclaimed.is_ok()) << reclaimed.to_string();
  EXPECT_FALSE(std::filesystem::exists(segment_path(directory.path(), 1U)));
  EXPECT_FALSE(std::filesystem::exists(segment_path(directory.path(), 2U)));
  EXPECT_FALSE(std::filesystem::exists(segment_path(directory.path(), 3U)));
  EXPECT_TRUE(std::filesystem::exists(segment_path(directory.path(), 4U)));
  EXPECT_TRUE(std::filesystem::exists(segment_path(directory.path(), 5U)));
  EXPECT_TRUE(reclaimer->reclaim(requests).is_ok());
  EXPECT_TRUE(writer->close().is_ok());
}

TEST(WalSubscriptionSourceReclaimerTest, PrevalidatesEveryWalBeforeFirstDeletion) {
  TemporaryDirectory directory_a{"chronos-live-wal-reclaimer-a"};
  TemporaryDirectory directory_b{"chronos-live-wal-reclaimer-b"};
  ASSERT_FALSE(directory_a.path().empty());
  ASSERT_FALSE(directory_b.path().empty());
  const wal::WalId wal_a = make_wal_id(5U);
  const wal::WalId wal_b = make_wal_id(6U);
  auto writer_a = make_writer(directory_a.path(), wal_a, 5U);
  auto writer_b = make_writer(directory_b.path(), wal_b, 2U);
  ASSERT_TRUE(writer_a.has_value()) << writer_a.error().to_string();
  ASSERT_TRUE(writer_b.has_value()) << writer_b.error().to_string();
  const schema::TabletId tablet_a = tablet_id(std::byte{3});
  const schema::TabletId tablet_b = tablet_id(std::byte{4});
  auto reclaimer = WalSubscriptionSourceReclaimer::create(
      {.sources = {{tablet_a, 21U, &*writer_a}, {tablet_b, 22U, &*writer_b}}});
  ASSERT_TRUE(reclaimer.has_value()) << reclaimer.error().to_string();
  const std::vector<SubscriptionSourceReclamation> requests{{{tablet_a, wal_a, 3U}, 21U, 9U},
                                                            {{tablet_b, wal_b, 3U}, 22U, 9U}};

  const common::Status rejected = reclaimer->reclaim(requests);

  EXPECT_EQ(rejected.code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(std::filesystem::exists(segment_path(directory_a.path(), 1U)));
  EXPECT_TRUE(std::filesystem::exists(segment_path(directory_a.path(), 2U)));
  EXPECT_TRUE(std::filesystem::exists(segment_path(directory_a.path(), 3U)));
  EXPECT_FALSE(writer_a->is_failed());
  EXPECT_FALSE(writer_b->is_failed());
  EXPECT_TRUE(writer_a->close().is_ok());
  EXPECT_TRUE(writer_b->close().is_ok());
}

TEST(WalSubscriptionSourceReclaimerTest, IntegratesTopologyAuthorityWithPhysicalWalReclamation) {
  TemporaryDirectory directory{"chronos-live-wal-reclaimer-integrated"};
  ASSERT_FALSE(directory.path().empty());
  const common::Uuid database = uuid(std::byte{7});
  const schema::TableId table = table_id(std::byte{8});
  const schema::TabletId tablet = tablet_id(std::byte{9});
  const wal::WalId wal = make_wal_id(7U);
  auto writer = make_writer(directory.path(), wal, 5U);
  ASSERT_TRUE(writer.has_value()) << writer.error().to_string();
  auto metadata = raft::MetadataStateMachine::create();
  ASSERT_TRUE(metadata.has_value()) << metadata.error().to_string();
  ASSERT_TRUE(
      metadata
          ->apply_committed(1U, raft::TabletPlacementMetadata{table, tablet, 31U, {10U, 11U}, 10U})
          .is_ok());
  auto authority = SubscriptionRetentionCoordinator::create({.database_id = database,
                                                             .table_id = table,
                                                             .local_node_id = 10U,
                                                             .members = {{tablet, wal, 31U}}});
  ASSERT_TRUE(authority.has_value()) << authority.error().to_string();
  auto reclaimer = WalSubscriptionSourceReclaimer::create({.sources = {{tablet, 31U, &*writer}}});
  ASSERT_TRUE(reclaimer.has_value()) << reclaimer.error().to_string();
  const std::vector<SourcePosition> storage_safe{{tablet, wal, 3U}};

  const auto report = authority->advance(*metadata, storage_safe, *reclaimer);

  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_TRUE(report->advanced);
  EXPECT_EQ(report->authorized_frontiers, storage_safe);
  EXPECT_FALSE(std::filesystem::exists(segment_path(directory.path(), 1U)));
  EXPECT_FALSE(std::filesystem::exists(segment_path(directory.path(), 2U)));
  EXPECT_FALSE(std::filesystem::exists(segment_path(directory.path(), 3U)));
  EXPECT_TRUE(std::filesystem::exists(segment_path(directory.path(), 4U)));
  EXPECT_TRUE(writer->close().is_ok());
}

} // namespace
} // namespace chronos::live
