#include "chronos/live/materialized_view_checkpoint_storage.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace chronos::live {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-view-checkpoint-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
    }
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] MaterializedViewCheckpointIdentity identity() {
  PlanFingerprint plan{};
  plan.fill(std::byte{0x61U});
  return {.database_id = uuid(1U),
          .view_id = uuid(2U),
          .table_id = schema::TableId::from_uuid(uuid(3U)).value(),
          .schema_id = schema::SchemaId::from_uuid(uuid(4U)).value(),
          .schema_version = schema::SchemaVersion::initial(),
          .plan_fingerprint = plan};
}

[[nodiscard]] BoundMaterializedViewCheckpoint
checkpoint(const std::uint64_t sequence, const double value_bias = 0.0,
           const std::uint64_t generation = 0U,
           const std::optional<std::int64_t> watermark = std::nullopt) {
  const auto tablet = schema::TabletId::from_uuid(uuid(5U)).value();
  wal::WalId wal;
  wal.bytes.fill(std::byte{0x62U});
  auto view = WindowedMaterializedView::create(tablet, wal, WindowDefinition{10, 10, 2, 16U, 16U});
  EXPECT_TRUE(view.has_value());
  for (std::uint64_t index = 1U; index <= sequence; ++index) {
    EXPECT_TRUE(
        view->apply_committed(SourcePosition{tablet, wal, index},
                              MaterializedViewInput{{index, static_cast<std::int64_t>(index), index,
                                                     static_cast<double>(index) + value_bias, 1.0},
                                                    false})
            .has_value());
  }
  if (watermark.has_value()) {
    EXPECT_TRUE(view->advance_watermark(*watermark).has_value());
  }
  return {.identity = identity(),
          .checkpoint_generation = generation,
          .state = std::move(view->checkpoint().value())};
}

[[nodiscard]] MaterializedViewCheckpointStorageConfig config(const TemporaryDirectory& directory) {
  return {.directory_path = directory.path().string(), .identity = identity()};
}

TEST(MaterializedViewCheckpointStorageTest, InstallsIdempotentlyAndSelectsLatestAfterReopen) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  {
    auto storage = MaterializedViewCheckpointStorage::create(config(directory));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto locked = MaterializedViewCheckpointStorage::create(config(directory));
    ASSERT_FALSE(locked.has_value());
    EXPECT_EQ(locked.error().code(), common::StatusCode::kUnavailable);

    const auto first = checkpoint(1U);
    auto installed = storage->install(first);
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    EXPECT_FALSE(installed->already_present);
    EXPECT_EQ(installed->file_name, "checkpoint-00000000000000000001.mvcp");
    auto repeated = storage->install(first);
    ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
    EXPECT_TRUE(repeated->already_present);

    const auto conflicting = checkpoint(1U, 10.0);
    EXPECT_EQ(storage->install(conflicting).error().code(), common::StatusCode::kCorruption);
    ASSERT_TRUE(storage->install(checkpoint(2U)).has_value());
    const auto generated = checkpoint(2U, 0.0, 1U);
    auto installed_generation = storage->install(generated);
    ASSERT_TRUE(installed_generation.has_value()) << installed_generation.error().to_string();
    EXPECT_EQ(installed_generation->file_name, "generation-00000000000000000001.mvcg");
    const auto watermarked = checkpoint(2U, 0.0, 2U, 12);
    ASSERT_TRUE(storage->install(watermarked).has_value());
    auto stale_retry = storage->install(generated);
    ASSERT_TRUE(stale_retry.has_value()) << stale_retry.error().to_string();
    EXPECT_TRUE(stale_retry->already_present);
    EXPECT_EQ(storage->install(checkpoint(3U)).error().code(),
              common::StatusCode::kInvalidArgument);
    auto latest = storage->load_latest();
    ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
    ASSERT_TRUE(latest->has_value());
    EXPECT_EQ((*latest)->checkpoint, watermarked);
  }

  auto reopened = MaterializedViewCheckpointStorage::open_existing(config(directory));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto latest = reopened->load_latest();
  ASSERT_TRUE(latest.has_value()) << latest.error().to_string();
  ASSERT_TRUE(latest->has_value());
  EXPECT_EQ((*latest)->checkpoint, checkpoint(2U, 0.0, 2U, 12));
}

TEST(MaterializedViewCheckpointStorageTest, CleansTemporaryAndRejectsCorruptInstalledBytes) {
  TemporaryDirectory directory;
  std::filesystem::path final_path;
  {
    auto storage = MaterializedViewCheckpointStorage::create(config(directory));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto installed = storage->install(checkpoint(1U));
    ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
    final_path = directory.path() / installed->file_name;
  }

  const auto temporary = directory.path() / "checkpoint-00000000000000000002.mvcp.tmp";
  {
    std::ofstream output{temporary, std::ios::binary | std::ios::trunc};
    output.put('x');
  }
  {
    auto reopened = MaterializedViewCheckpointStorage::open_existing(config(directory));
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    EXPECT_FALSE(std::filesystem::exists(temporary));
  }

  {
    std::fstream file{final_path, std::ios::binary | std::ios::in | std::ios::out};
    ASSERT_TRUE(file.good());
    file.seekp(200);
    file.put('x');
  }
  auto reopened = MaterializedViewCheckpointStorage::open_existing(config(directory));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto latest = reopened->load_latest();
  ASSERT_FALSE(latest.has_value());
  EXPECT_EQ(latest.error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::live
