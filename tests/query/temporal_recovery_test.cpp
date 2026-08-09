#include "chronos/query/temporal_recovery.hpp"
#include "columnar/columnar_test_support.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
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

[[nodiscard]] EncodedTemporalCommand command(const columnar::OwnedColumnarBatch& batch,
                                             const TemporalMutationKind kind,
                                             const std::int64_t system_time) {
  return encode_temporal_command_v1(batch, descriptors(kind), system_time).value();
}

[[nodiscard]] wal::WalId write_history(const wal::WalWriterConfig& config,
                                       const columnar::OwnedColumnarBatch& batch,
                                       const bool begin_with_correction = false) {
  auto created = wal::WalWriter::create_new(config);
  EXPECT_TRUE(created.has_value());
  if (!created.has_value()) {
    return {};
  }
  wal::WalWriter writer = std::move(*created);
  const wal::WalId identity = writer.wal_id();
  auto first = command(batch,
                       begin_with_correction ? TemporalMutationKind::kCorrection
                                             : TemporalMutationKind::kOriginal,
                       1000);
  EXPECT_TRUE(writer.append_application_entry(first.bytes()).has_value());
  if (!begin_with_correction) {
    auto second = command(batch, TemporalMutationKind::kCorrection, 2000);
    EXPECT_TRUE(writer.append_application_entry(second.bytes()).has_value());
  }
  EXPECT_TRUE(writer.synchronize().has_value());
  EXPECT_TRUE(writer.close().is_ok());
  return identity;
}

[[nodiscard]] TemporalRecoveryConfig
recovery_config(const std::shared_ptr<const schema::TableSchema>& retained) {
  std::vector<TemporalRecoveryTableConfig> tables;
  tables.push_back({.schema = retained, .store_limits = {}});
  return TemporalRecoveryConfig{.tables = std::move(tables), .decode_limits = {}};
}

TEST(TemporalRecoveryTest, ReplaysVerifiedHistoryAndContinuesTheWalSequence) {
  TemporaryDirectory directory{"chronos-temporal-recovery"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  const std::shared_ptr<const schema::TableSchema> retained = columnar::test::batch_schema();
  auto batch = columnar::OwnedColumnarBatch::create(retained, columnar::test::batch_columns());
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  const wal::WalId expected_wal_id = write_history(writer_config, *batch);
  ASSERT_TRUE(expected_wal_id.is_valid());

  auto recovered = recover_temporal_wal(writer_config, {}, recovery_config(retained));
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  ASSERT_EQ(recovered->table_count(), 1U);
  TemporalSnapshotProvider* const provider = recovered->provider(retained->table_id());
  ASSERT_NE(provider, nullptr);
  EXPECT_EQ(provider->latest_commit_position(), 2U);
  EXPECT_EQ(provider->logical_row_count(), 2U);
  EXPECT_EQ(provider->version_count(), 4U);
  auto historical = provider->resolve(retained, 1500);
  ASSERT_TRUE(historical.has_value()) << historical.error().to_string();
  EXPECT_EQ((*historical)->committed_position(), 1U);
  ASSERT_EQ((*historical)->rows().size(), 2U);
  EXPECT_EQ((*historical)->rows()[0].wal_id, common::Uuid{expected_wal_id.bytes});

  auto writer = recovered->release_writer();
  ASSERT_TRUE(writer.has_value()) << writer.error().to_string();
  EXPECT_FALSE(recovered->release_writer().has_value());
  auto next = writer->next_record_sequence();
  ASSERT_TRUE(next.has_value());
  EXPECT_EQ(*next, 3U);
  EXPECT_TRUE(writer->close().is_ok());
}

TEST(TemporalRecoveryTest, RejectsCorrectionWithoutAnOriginalAsCommittedCorruption) {
  TemporaryDirectory directory{"chronos-temporal-invalid-history"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  const std::shared_ptr<const schema::TableSchema> retained = columnar::test::batch_schema();
  auto batch = columnar::OwnedColumnarBatch::create(retained, columnar::test::batch_columns());
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  ASSERT_TRUE(write_history(writer_config, *batch, true).is_valid());

  auto recovered = recover_temporal_wal(writer_config, {}, recovery_config(retained));
  ASSERT_FALSE(recovered.has_value());
  EXPECT_EQ(recovered.error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::query
