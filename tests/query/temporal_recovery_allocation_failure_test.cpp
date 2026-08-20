#include "chronos/query/temporal_recovery.hpp"
#include "columnar/columnar_test_support.hpp"
#include "support/failing_allocator.hpp"

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
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-temporal-allocation-XXXXXX").string();
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

[[nodiscard]] EncodedTemporalCommand command(const columnar::OwnedColumnarBatch& batch) {
  const std::vector<TemporalMutationDescriptor> descriptors{
      {{std::byte{1U}}, 100, 110, TemporalMutationKind::kOriginal},
      {{std::byte{2U}}, 200, 220, TemporalMutationKind::kOriginal}};
  return encode_temporal_command_v1(batch, descriptors, 1000).value();
}

[[nodiscard]] TemporalRecoveryConfig
recovery_config(const std::shared_ptr<const schema::TableSchema>& retained) {
  std::vector<TemporalRecoveryTableConfig> tables;
  tables.push_back({.schema = retained, .store_limits = {}});
  return {.tables = std::move(tables), .decode_limits = {}};
}

TEST(TemporalRecoveryAllocationFailureTest, UnwindsEveryAllocationAndReleasesTheWalLock) {
  TemporaryDirectory directory;
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  const std::shared_ptr<const schema::TableSchema> retained = columnar::test::batch_schema();
  const columnar::OwnedColumnarBatch batch =
      columnar::OwnedColumnarBatch::create(retained, columnar::test::batch_columns()).value();
  const EncodedTemporalCommand encoded = command(batch);
  auto created = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  wal::WalWriter writer = std::move(*created);
  ASSERT_TRUE(writer.append_application_entry(encoded.bytes()).has_value());
  ASSERT_TRUE(writer.synchronize().has_value());
  ASSERT_TRUE(writer.close().is_ok());

  bool succeeded = false;
  for (std::size_t fail_after = 0U; fail_after < 512U; ++fail_after) {
    TemporalRecoveryConfig config = recovery_config(retained);
    auto recovered = run_failure(
        fail_after, [&] { return recover_temporal_wal(writer_config, {}, std::move(config)); });
    if (!recovered.has_value()) {
      EXPECT_EQ(recovered.error().code(), common::StatusCode::kResourceExhausted)
          << recovered.error().to_string();
      continue;
    }

    const TemporalSnapshotProvider* const provider = recovered->provider(retained->table_id());
    ASSERT_NE(provider, nullptr);
    EXPECT_EQ(provider->latest_commit_position(), 1U);
    EXPECT_EQ(provider->logical_row_count(), 2U);
    EXPECT_EQ(provider->version_count(), 2U);
    auto reopened = recovered->release_writer();
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    EXPECT_TRUE(reopened->close().is_ok());
    succeeded = true;
    break;
  }
  EXPECT_TRUE(succeeded);
}

} // namespace
} // namespace chronos::query
