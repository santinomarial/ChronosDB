#include "chronos/query/temporal_command_executor.hpp"
#include "chronos/query/temporal_recovery.hpp"
#include "columnar/columnar_test_support.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

class TemporaryDirectory {
public:
  explicit TemporaryDirectory(const char* label) {
    std::string pattern =
        (std::filesystem::temp_directory_path() / (std::string{label} + "-XXXXXX")).string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

  [[nodiscard]] bool valid() const noexcept {
    return !path_.empty();
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] std::vector<TemporalMutationDescriptor> descriptors(const TemporalMutationKind kind) {
  return {{{std::byte{1U}}, 100, 110, kind}, {{std::byte{2U}}, 200, 220, kind}};
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch> batch() {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                           columnar::test::batch_columns())
          .value());
}

[[nodiscard]] TemporalCommandExecutionInput
input(const std::shared_ptr<const columnar::OwnedColumnarBatch>& values,
      const TemporalMutationKind kind, const std::int64_t system_time,
      const wal::WalDurabilityMode durability = wal::WalDurabilityMode::kLocalSync) {
  return TemporalCommandExecutionInput{.batch = values,
                                       .mutations = descriptors(kind),
                                       .system_commit_time_ns = system_time,
                                       .durability = durability};
}

TEST(TemporalCommandExecutorTest, AcknowledgesLocalSyncThenRecoversThePublishedCommit) {
  TemporaryDirectory directory{"chronos-temporal-executor"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  auto writer = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(writer.has_value()) << writer.error().to_string();
  auto coordinator = wal::WalCommitCoordinator::start(std::move(*writer));
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  const auto values = batch();
  auto provider = TemporalSnapshotProvider::create(values->schema_ptr());
  ASSERT_TRUE(provider.has_value()) << provider.error().to_string();

  auto executed = execute_temporal_command(input(values, TemporalMutationKind::kOriginal, 1000),
                                           **provider, *coordinator);
  ASSERT_TRUE(executed.has_value()) << executed.error().to_string();
  EXPECT_EQ(executed->wal_commit.requested_durability, wal::WalDurabilityMode::kLocalSync);
  EXPECT_EQ(executed->wal_commit.effective_durability, wal::WalDurabilityMode::kLocalSync);
  ASSERT_TRUE(executed->wal_commit.durable_record_sequence.has_value());
  EXPECT_GE(*executed->wal_commit.durable_record_sequence,
            executed->wal_commit.append.record_sequence);
  EXPECT_EQ(executed->application.system_commit_position, 1U);
  EXPECT_EQ((*provider)->latest_commit_position(), 1U);
  EXPECT_FALSE((*provider)->is_failed());
  EXPECT_TRUE(coordinator->shutdown().is_ok());

  std::vector<TemporalRecoveryTableConfig> tables;
  tables.push_back({.schema = values->schema_ptr(), .store_limits = {}});
  auto recovered =
      recover_temporal_wal(writer_config, {}, {.tables = std::move(tables), .decode_limits = {}});
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  const TemporalSnapshotProvider* const replayed = recovered->provider(values->schema().table_id());
  ASSERT_NE(replayed, nullptr);
  EXPECT_EQ(replayed->latest_commit_position(), 1U);
  EXPECT_EQ(replayed->version_count(), 2U);
  auto reopened_writer = recovered->release_writer();
  ASSERT_TRUE(reopened_writer.has_value()) << reopened_writer.error().to_string();
  EXPECT_TRUE(reopened_writer->close().is_ok());
}

TEST(TemporalCommandExecutorTest, RejectsInvalidTransitionBeforeWalAdmission) {
  TemporaryDirectory directory{"chronos-temporal-precommit"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  auto writer = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(writer.has_value()) << writer.error().to_string();
  auto coordinator = wal::WalCommitCoordinator::start(std::move(*writer));
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  const auto values = batch();
  auto provider = TemporalSnapshotProvider::create(values->schema_ptr());
  ASSERT_TRUE(provider.has_value()) << provider.error().to_string();

  auto rejected = execute_temporal_command(input(values, TemporalMutationKind::kCorrection, 1000),
                                           **provider, *coordinator);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_FALSE((*provider)->is_failed());

  auto accepted = execute_temporal_command(input(values, TemporalMutationKind::kOriginal, 1000),
                                           **provider, *coordinator);
  ASSERT_TRUE(accepted.has_value()) << accepted.error().to_string();
  EXPECT_EQ(accepted->wal_commit.append.record_sequence, 1U);
  EXPECT_TRUE(coordinator->shutdown().is_ok());
}

TEST(TemporalCommandExecutorTest, FailsProviderClosedAfterPostAdmissionPositionMismatch) {
  TemporaryDirectory directory{"chronos-temporal-fail-closed"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  auto writer = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(writer.has_value()) << writer.error().to_string();
  auto coordinator = wal::WalCommitCoordinator::start(std::move(*writer));
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  const auto values = batch();
  auto provider = TemporalSnapshotProvider::create(values->schema_ptr());
  ASSERT_TRUE(provider.has_value()) << provider.error().to_string();

  auto encoded =
      encode_temporal_command_v1(*values, descriptors(TemporalMutationKind::kOriginal), 1000);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded = decode_temporal_command_v1(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  wal::WalId unrelated_wal;
  unrelated_wal.bytes.front() = std::byte{9U};
  ASSERT_TRUE(
      apply_committed_temporal_command(*decoded, values->schema(), 5U, unrelated_wal, **provider)
          .has_value());

  auto mismatched = execute_temporal_command(input(values, TemporalMutationKind::kCorrection, 2000),
                                             **provider, *coordinator);
  ASSERT_FALSE(mismatched.has_value());
  EXPECT_EQ(mismatched.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE((*provider)->is_failed());
  auto unavailable = (*provider)->resolve(values->schema_ptr(), std::nullopt);
  ASSERT_FALSE(unavailable.has_value());
  EXPECT_EQ(unavailable.error().code(), common::StatusCode::kUnavailable);
  EXPECT_TRUE(coordinator->shutdown().is_ok());
}

} // namespace
} // namespace chronos::query
